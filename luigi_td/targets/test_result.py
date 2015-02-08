from ..test_helper import TestConfig
from result import ResultTarget

from unittest import TestCase
from nose.tools import eq_, raises

test_config = TestConfig()

class ResultTargetTestCase(TestCase):
    def setUp(self):
        test_config.setUp()

    def tearDown(self):
        test_config.tearDown()

    def test_ok(self):
        target = ResultTarget(test_config.get_tmp_path('result.job'))
