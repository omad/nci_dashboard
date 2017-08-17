import pandas as pd
from paramiko import SSHClient
from io import StringIO


def qstat_to_df(output):
    return pd.read_csv(StringIO(output), skiprows=5, delim_whitespace=True, header=None,
                       names=['jobid', 'username', 'queue', 'jobname', 'session_id', 'nodes', 'tasks',
                              'reqd_mem', 'reqd_time', 'state', 'elap_time'])


def convert_to_bytes(mem_string):
    scale = {'mb': 2 ** 20, 'gb': 2 ** 30, 'tb': 2 ** 40}
    num, units = int(mem_string[:-2]), mem_string[-2:]
    return num * scale[units]


def hhmm_to_timedelta(hhmm):
    h, m = hhmm.split(':')
    return pd.Timedelta(hours=int(h), minutes=int(m))


class NCIServer:
    def __init__(self):
        client = SSHClient()
        client.load_system_host_keys()
        self.client = client
        client.connect('raijin.nci.org.au', username='dra547')

    def execute_command(self, command):
        stdin, stdout, stderr = None, None, None
        try:
            stdin, stdout, stderr = self.client.exec_command(command)

            return stdout.read().decode('ascii')
        finally:
            if stdin:
                stdin.close()
                stdout.close()
                stderr.close()

    def print_command_output(self, command):
        output = self.execute_command(command)
        print(output)

    def find_users_in_groups(self, *groups):
        output = self.execute_command(f'getent group {" ".join(groups)}')
        users = set()
        for line in output.split('\n'):
            userlist = line.split(':')[-1]
            group_users = userlist.split(',')
            users.update(group_users)
        users.discard('')
        return users

    def find_names_of_users(self, *usernames):
        output = self.execute_command(f'getent passwd {" ".join(usernames)}')
        users = {}
        for line in output.split('\n'):
            if not line:
                continue
            username, _, _, _, full_name, homedir, shell = line.split(':')
            users[username] = full_name
        return users

    def find_jobs_for_users(self, *users):
        output = self.execute_command(f'qstat -w -u {",".join(users)}')
        df = qstat_to_df(output)

        df['reqd_mem'] = df['reqd_mem'].apply(convert_to_bytes)

        df['reqd_time'] = df['reqd_time'].apply(hhmm_to_timedelta)
        df['elap_time'] = df['elap_time'].apply(lambda x: pd.to_timedelta(x) if '-' not in x else None)
        return df

    def detailed_job_info(self, *jobids):
        qstat_f_output = self.execute_command(f'qstat -f {" ".join(jobids)}')

        outputs = self._decode_full_qstat(qstat_f_output)
        outputs = dict(self._decode_job_thing(jobt) for jobt in outputs if len(jobt) > 5)

        df = pd.DataFrame.from_dict(outputs, orient='index')

        duration_cols = ['resources_used.cput', 'resources_used.walltime']
        df[duration_cols] = df[duration_cols].apply(pd.to_timedelta)

        numeric_cols = ['resources_used.cpupercent', 'resources_used.ncpus']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

        df['cpu_efficiency'] = df['resources_used.cput'] / df['resources_used.walltime'] / df['resources_used.ncpus']
        return df

    def detailed_job_info_for_users(self, *users):
        simple_jobs_df = self.find_jobs_for_users(*users)
        running_jobs = simple_jobs_df[simple_jobs_df.state == 'R']
        extra_info = self.detailed_job_info(*running_jobs.jobid)

        extra_info.drop(['queue', 'session_id'], axis=1, inplace=True)
        merged = pd.merge(simple_jobs_df, extra_info, how='left', left_on='jobid', right_index=True)

        return merged

    @staticmethod
    def _decode_full_qstat(qstat_f_output):
        qstat_f_output = qstat_f_output.replace('\n\t', '')
        outputs = qstat_f_output.split('\n\n')

        outputs = [output.split('\n') for output in outputs]
        return outputs

    @staticmethod
    def _decode_job_thing(job_thing):
        job_id = job_thing[0].split()[-1]

        job = {}
        for line in job_thing[1:]:
            line = line.strip()
            name, val = line.split(' = ')
            job[name] = val

        return job_id, job


if __name__ == '__main__':
    raijin = NCIServer()
    relevant_users = raijin.find_users_in_groups('v10', 'u46')

    names_of_users = raijin.find_names_of_users(*relevant_users)
    print(names_of_users)
    #
    # jobs = raijin.find_jobs_for_users(*relevant_users)
    # print(jobs)
    #
    # job_details = raijin.detailed_job_info(*jobs.jobid)
    # print(job_details)
    jobs = raijin.detailed_job_info_for_users(*relevant_users)
    print(jobs)
