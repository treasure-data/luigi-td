import os
import shutil
import tempfile

class MockJob(object):
    def __init__(self, spec):
        self.spec = spec
        self.job_id = spec['job_id']
        self.url = 'https://mock.example.com/jobs/{0}'.format(spec['job_id'])
        self._result_size = spec['size']
        self._hive_result_schema = spec['description']

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

    def databases(self):
        return iter(self._databases)

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
