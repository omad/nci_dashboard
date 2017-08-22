"""
Store information about running jobs from the NCI in an ElasticSearch Cluster

* Create a new index for each day, named `dea-nci-*`
* Sleep for 60 seconds between each `qstat` query
* Grabs a list of users for the relevant groups for requesting job information
  - Currently `u46`, `v10`, `r78`
* Currently hard coded to connect-through the GA proxy
  - Should be replaced with IAM auth if running in lambda
  - Connects to port 80, but should use 443, haven't gotten this working yet.
"""

from threading import Thread
import time
from elasticsearch import RequestsHttpConnection
from elasticsearch_dsl.connections import connections
from elasticsearch import helpers
from datetime import datetime
import pandas as pd
from nci_dashboard.connect_and_get_qstat import NCIServer

import logging

logger = logging.getLogger(__name__)

raijin = None
relevant_users = []


def update_template(es):
    jobs_template = {'template': 'dea-nci-*',
                     'mappings': {
                         'job_status': {
                             'properties': {'cpu_efficiency': {'type': 'float'},
                                            'elap_time': {'type': 'float'},
                                            'nodes': {'type': 'integer'},
                                            'reqd_mem': {'type': 'long'},
                                            'reqd_time': {'type': 'integer'},
                                            'tasks': {'type': 'integer'},
                                            "session_id": {
                                                "type": "text",
                                                "fields": {
                                                    "keyword": {
                                                        "type": "keyword",
                                                        "ignore_above": 256
                                                    }
                                                }
                                            },
                                            '@timestamp': {'type': 'date'}}}}}
    es.indices.put_template(name='dea-jobs', body=jobs_template)


def update_jobs_list():
    while True:
        # do some blocking computation
        jobs = raijin.detailed_job_info_for_users(*relevant_users)

        jobs['resources_used.walltime'] = jobs['resources_used.walltime'].dt.seconds
        jobs['resources_used.cput'] = jobs['resources_used.cput'].dt.seconds
        jobs['reqd_time'] = jobs['reqd_time'].dt.seconds
        jobs['elap_time'] = jobs['elap_time'].dt.seconds
        logger.info('Retrieved Jobs Information')

        index_name = 'dea-nci-' + datetime.now().strftime('%Y-%m-%d')

        # JSON can't contain NaN values, so replace them with None
        actions = jobs.where((pd.notnull(jobs)), None).to_dict(orient='record')

        t = datetime.utcnow()
        for action in actions:
            action['_index'] = index_name
            action['_type'] = 'job_status'
            action['@timestamp'] = t.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        client = connections.get_connection()
        summary = helpers.bulk(client, actions)

        logging.debug(summary)

        time.sleep(60)


class ProxiedConnection(RequestsHttpConnection):
    def __init__(self, *args, **kwargs):
        proxies = kwargs.pop('proxies', {})
        super(ProxiedConnection, self).__init__(*args, **kwargs)
        self.session.proxies = proxies


HOSTS = [{'host': 'search-digitalearthaustralia-lz7w5p3eakto7wrzkmg677yebm.ap-southeast-2.es.amazonaws.com',
          'port': 443, 'use_ssl': True}]
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    connections.create_connection(
        hosts=['search-digitalearthaustralia-lz7w5p3eakto7wrzkmg677yebm.ap-southeast-2.es.amazonaws.com:80'],
        # use_ssl=True)
        connection_class=ProxiedConnection, proxies={'http': 'proxy.inno.lan:3128'})
    logging.info('connected to ES')

    client = connections.get_connection()
    update_template(client)

    raijin = NCIServer()
    logging.info('connected to NCI: %s', raijin)
    relevant_users = raijin.find_users_in_groups('v10', 'u46')

    thread = Thread(target=update_jobs_list)
    thread.start()
