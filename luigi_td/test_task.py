from .test_helper import TestConfig
from .task import DatabaseTask
from .task import TableTask
from .task import Query
from .targets.result import ResultTarget

from unittest import TestCase
try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock
from nose.tools import ok_, eq_, raises

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
        client = task.config.get_client()
        client.create_log_table = MagicMock()
        client.update_schema = MagicMock()
        task.run()
        client.create_log_table.assert_called_with('test_db', 'test_table')
        client.update_schema.assert_not_called()

    def test_create_with_schema(self):
        task = TestTableTask('test_db', 'test_table', schema=['x:string', 'y:int'])
        client = task.config.get_client()
        client.create_log_table = MagicMock()
        client.update_schema = MagicMock()
        task.run()
        client.create_log_table.assert_called_with('test_db', 'test_table')
        client.update_schema.assert_called_with('test_db', 'test_table', [['x', 'string'], ['y', 'int']])

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

    def test_priority(self):
        class PriorityTestQuery(TestQuery):
            priority = 1
        task = PriorityTestQuery()
        client = task.config.get_client()
        client.query = MagicMock()
        task.run()
        client.query.assert_called_with(task.database,
                                        task.query(),
                                        priority=1,
                                        retry_limit=task.retry_limit,
                                        type=task.type,
                                        result_url=None)

    def test_retry_limit(self):
        class RetryLimitTestQuery(TestQuery):
            retry_limit = 3
        task = RetryLimitTestQuery()
        client = task.config.get_client()
        client.query = MagicMock()
        task.run()
        client.query.assert_called_with(task.database,
                                        task.query(),
                                        priority=task.priority,
                                        retry_limit=3,
                                        type=task.type,
                                        result_url=None)

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
