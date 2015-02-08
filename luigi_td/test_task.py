from test_helper import TestConfig
from task import DatabaseTask
from task import TableTask
from task import Query
from targets import ResultTarget

from unittest import TestCase
from nose.tools import eq_, raises

import luigi

test_config = TestConfig(
    jobs = [
        {
            'job_id': 1,
            'status': 'success',
            'size': 20,
            'description': [['cnt', 'int']],
            'rows': [[5000]],
        }
    ]
)

# DatabaseTask

class TestDatabaseTask(DatabaseTask):
    config = test_config

class DatabaseTaskTestCase(TestCase):
    def test_create(self):
        task = TestDatabaseTask('test_db')
        task.run()

# TableTask

class TestTableTask(TableTask):
    config = test_config

class TableTaskTestCase(TestCase):
    def test_create(self):
        task = TestTableTask('test_db', 'test_table')
        task.run()

# Query

class TestQuery(Query):
    config = test_config
    type = 'hive'
    database = 'sample_datasets'
    def query(self):
        return 'select count(1) cnt from www_access'

class QueryTestCase(TestCase):
    def setUp(self):
        test_config.setUp()

    def tearDown(self):
        test_config.tearDown()

    def test_simple(self):
        class SimpleTestQuery(TestQuery):
            pass
        task = SimpleTestQuery()
        task.run()

    def test_with_output(self):
        class OutputTestQuery(TestQuery):
            def output(self):
                return ResultTarget(test_config.get_tmp_path('{0}.job'.format(self)))
        task = OutputTestQuery()
        task.run()

    def test_with_dependency(self):
        class DependencyTestQuery(TestQuery):
            def output(self):
                return ResultTarget(test_config.get_tmp_path('{0}.job'.format(self)))
        class DependencyTestResult(luigi.Task):
            def requires(self):
                return DependencyTestQuery()
            def output(self):
                return LocalTarget(test_config.get_tmp_path('{0}.csv'.format(self)))
        task = DependencyTestResult()
        task.run()
