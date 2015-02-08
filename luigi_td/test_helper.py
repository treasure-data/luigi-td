from client import ResultProxy

import os
import shutil
import tempfile
import tdclient

class MockDatabase(object):
    def __init__(self, name):
        self.name = name

class MockTable(object):
    def __init__(self, database, name):
        self.database = database
        self.name = name
        self.schema = []

class MockJob(object):
    def __init__(self, spec):
        self.spec = spec
        self.job_id = spec['job_id']
        self.url = 'https://mock.example.com/jobs/{0}'.format(spec['job_id'])
        self._result_size = spec.get('size')
        self._hive_result_schema = spec.get('description', [])

    def _update_status(self):
        pass

    def kill(self):
        pass

    def status(self):
        return self.spec['status']

    def success(self):
        return self.status() == 'success'

    def finished(self):
        return self.status() != 'pending'

    def result(self):
        return iter(self.spec['rows'])

class MockClient(object):
    def __init__(self, databases, tables, jobs):
        self._databases = databases
        self._tables = tables
        self._jobs = jobs

    def create_database(self, name):
        self._databases.append(name)

    def databases(self):
        return [MockDatabase(name) for name in self._databases]

    def create_log_table(self, database, name):
        self._tables.append("{0}.{1}".format(database, name))

    def update_schema(self, database, name, schema):
        pass

    def table(self, database, name):
        if "{0}.{1}".format(database, name) in self._tables:
            return MockTable(database, name)
        raise tdclient.api.NotFoundError(database, name)

    def jobs(self):
        return [MockJob(job) for job in self._jobs]

    def job(self, job_id):
        for job in self.jobs():
            if job.job_id == job_id:
                return job

    def query(self, database, query, type='hive', result_url=None):
        return MockJob(self._jobs[0])

class TestConfig(object):
    def __init__(self, databases=[], tables=[], jobs=[]):
        self._databases = databases
        self._tables = tables
        self._jobs = jobs
        self.tmp_dir = None

    def setUp(self):
        if not self.tmp_dir:
            self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if self.tmp_dir:
            shutil.rmtree(self.tmp_dir)
            self.tmp_dir = None

    def get_tmp_path(self, filename):
        return os.path.join(self.tmp_dir, filename)

    def get_client(self):
        return MockClient(databases=self._databases, tables=self._tables, jobs=self._jobs)
