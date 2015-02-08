from ..test_helper import TestConfig
from td import DatabaseTarget, TableTarget, SchemaError

from unittest import TestCase
from nose.tools import eq_, raises

import urllib
import urlparse

test_config = TestConfig(
    databases = ['test_db'],
    tables = ['test_db.test_table'],
)

class DatabaseTargetTestCase(TestCase):
    def test_exists(self):
        target = DatabaseTarget('test_db', config=test_config)
        eq_(target.exists(), True)

    def test_not_exists(self):
        target = DatabaseTarget('dummy_db', config=test_config)
        eq_(target.exists(), False)

class TableTargetTestCase(TestCase):
    def test_exists(self):
        target = TableTarget('test_db', 'test_table', config=test_config)
        eq_(target.exists(), True)

    def test_table_not_exists(self):
        target = TableTarget('test_db', 'dummy_table', config=test_config)
        eq_(target.exists(), False)

    def test_database_not_exists(self):
        target = TableTarget('dummy_db', 'test_table', config=test_config)
        eq_(target.exists(), False)

    @raises(SchemaError)
    def test_schema_unmatch(self):
        target = TableTarget('test_db', 'test_table', schema=[['c', 'int']], config=test_config)
        target.exists()
