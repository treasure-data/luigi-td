import os
import shutil
import tempfile

class TestEnv(object):
    def __init__(self):
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
    def __init__(self, query_spec):
        self.query_spec = query_spec

    def query(self, database, query, type='hive', result_url=None):
        return MockJob(self.query_spec)
