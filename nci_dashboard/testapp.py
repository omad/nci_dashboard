
from threading import Thread
import time

from bokeh.models import ColumnDataSource
from bokeh.plotting import curdoc, figure
from bokeh.models.widgets import RangeSlider, Button, DataTable, TableColumn, NumberFormatter
from bokeh.layouts import column, layout
from tornado import gen
from functools import partial

import logging

logger = logging.getLogger()

from connect_and_get_qstat import NCIServer

running_cols = ['jobid', 'username', 'project', 'jobname', 'queue_x', 'nodes', 'tasks', 'reqd_mem', 'reqd_time', 'state',
                'elap_time', 'cpu_efficiency']
# this must only be modified from a Bokeh session allback
running_jobs = ColumnDataSource(data={k: [] for k in running_cols})


# See https://bokeh.github.io/blog/2017/6/29/simple_bokeh_server/ for streaming example
usage_history = ColumnDataSource({'time': [time.time() * 1000, time.time() * 1000 + 1],
                                  'active_cpus': [0, 0]})

# Also https://github.com/bokeh/bokeh/blob/master/examples/app/ohlc/main.py

usage_graph = figure(plot_width=1000, plot_height=400, x_axis_type='datetime')

usage_graph.line(source=usage_history, x='time', y='active_cpus', title='Current number of active CPUs')
usage_graph.yaxis.axis_label = "Currently active CPUs"


# This is important! Save curdoc() to make sure all threads
# see then same document.
doc = curdoc()


@gen.coroutine
def update_jobs(jobs):

    logger.debug('updated_data_jobs %s', jobs)
    updated_data = {k: jobs[k] for k in running_cols}
    running_jobs.data = updated_data
    logger.debug('updated_data %s', updated_data)

    result = {'time': [time.time() * 1000],
              'active_cpus': [jobs['resources_used.ncpus'].sum()]}
    usage_history.stream(result, 100)


def update_jobs_list():
    while True:
        # do some blocking computation
        jobs = raijin.detailed_job_info_for_users(*relevant_users)

        # but update the document from callback
        doc.add_next_tick_callback(partial(update_jobs, jobs))

        time.sleep(60)


raijin = NCIServer()
relevant_users = raijin.find_users_in_groups('v10', 'u46')


columns = [
    TableColumn(field=name, title=name) for name in running_cols
]

data_table = DataTable(source=running_jobs, columns=columns, width=1200)


def on_select_job(attr, old, new):
    selected_rows = new['1d']['indices']

    df = running_jobs.to_df()

    selected_jobs = df.iloc[selected_rows]

running_jobs.on_change('selected', on_select_job)


page = layout(
    children=[[data_table],
              # job_details,
              [usage_graph]]
)

doc.add_root(page)


thread = Thread(target=update_jobs_list)
thread.start()
