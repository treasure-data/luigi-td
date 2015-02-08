from ..test_helper import TestEnv, MockJob
from result import ResultTarget

from unittest import TestCase
from nose.tools import eq_, raises

env = TestEnv()

class ResultTargetTestCase(TestCase):
    def setUp(self):
        env.setUp()

    def tearDown(self):
        env.tearDown()

    def test_ok(self):
        target = ResultTarget(env.get_tmp_path('result.job'))
