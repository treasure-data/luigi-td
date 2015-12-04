from .client import ResultProxy

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
        self.result_size = spec.get('size')
        self.result_schema = spec.get('description', [])

    def update(self):
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

class MockBulkImport(object):
    def __init__(self, spec):
        self.spec = spec

    @property
    def upload_frozen(self):
        return self.spec['frozen']

    @property
    def status(self):
        return self.spec['status']

    @property
    def job_id(self):
        return self.spec['job_id']

class MockClient(object):
    def __init__(self, databases, tables, jobs, bulk_imports):
        self._databases = databases
        self._tables = tables
        self._jobs = jobs
        self._bulk_imports = bulk_imports

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

    def query(self, database, query, priority=None, retry_limit=None, type='hive', result_url=None):
        return MockJob(self._jobs[0])

    def bulk_import(self, session):
        for spec in self._bulk_imports:
            if spec['session'] == session:
                return MockBulkImport(spec)
        raise tdclient.api.NotFoundError(session)

    def create_bulk_import(self, session, database, table):
        return self.bulk_import(session)

    def freeze_bulk_import(self, session):
        pass

    def perform_bulk_import(self, session):
        return MockJob(self._jobs[0])

    def commit_bulk_import(self, session):
        pass

class TestConfig(object):
    def __init__(self, databases=[], tables=[], jobs=[], bulk_imports=[]):
        self._databases = databases
        self._tables = tables
        self._jobs = jobs
        self._bulk_imports = bulk_imports
        self.endpoint = 'http://test.example.com'
        self.apikey = 'test-key'
        self.tmp_dir = None

    def setUp(self):
        if not self.tmp_dir:
            self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if hasattr(self, '_client'):
            del self._client
        if self.tmp_dir:
            shutil.rmtree(self.tmp_dir)
            self.tmp_dir = None

    def get_tmp_path(self, filename):
        return os.path.join(self.tmp_dir, filename)

    def get_client(self):
        if not hasattr(self, '_client'):
            self._client = MockClient(databases=self._databases, tables=self._tables, jobs=self._jobs, bulk_imports=self._bulk_imports)
        return self._client
