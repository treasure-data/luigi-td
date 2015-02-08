from task import Query
from targets import ResultTarget
from test_helper import TestEnv, MockClient

from unittest import TestCase
from nose.tools import eq_, raises

import luigi

env = TestEnv()

class TestConfig(object):
    def __init__(self, spec):
        self.query_spec = spec

    def get_client(self):
        return MockClient(self.query_spec)

success_test_config = TestConfig({
    'job_id': 1,
    'status': 'success',
    'size': 20,
    'description': [['cnt', 'int']],
    'rows': [[5000]],
})

class TestQuery(Query):
    config = success_test_config
    type = 'hive'
    database = 'sample_datasets'
    def query(self):
        return 'select count(1) cnt from www_access'

class QueryTestCase(TestCase):
    def setUp(self):
        env.setUp()

    def tearDown(self):
        env.tearDown()

    def test_simple(self):
        class SimpleTestQuery(TestQuery):
            pass
        task = SimpleTestQuery()
        task.run()

    def test_with_output(self):
        class OutputTestQuery(TestQuery):
            def output(self):
                return ResultTarget(env.get_tmp_path('{0}.job'.format(self)))
        task = OutputTestQuery()
        task.run()

    def test_with_dependency(self):
        class DependencyTestQuery(TestQuery):
            def output(self):
                return ResultTarget(env.get_tmp_path('{0}.job'.format(self)))
        class DependencyTestResult(luigi.Task):
            def requires(self):
                return DependencyTestQuery()
            def output(self):
                return LocalTarget(env.get_tmp_path('{0}.csv'.format(self)))
        task = DependencyTestResult()
        task.run()
