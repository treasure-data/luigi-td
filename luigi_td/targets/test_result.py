from ..test_helper import TestConfig
from ..client import ResultProxy
from result import ResultTarget

from unittest import TestCase
from nose.tools import eq_, raises

test_config = TestConfig(
    jobs = [
        {
            'job_id': 1,
            'status': 'success',
            'size': 20,
            'description': [['cnt', 'int']],
            'rows': [[5000]],
        },
        {
            'job_id': 2,
            'status': 'error',
            'size': 20,
        },
        {
            'job_id': 3,
            'status': 'killed',
            'size': 20,
        },
        {
            'job_id': 4,
            'status': 'queued',
        },
        {
            'job_id': 5,
            'status': 'running',
        },
    ]
)

class ResultTargetTestCase(TestCase):
    def setUp(self):
        test_config.setUp()
        for job in test_config.get_client().jobs():
            result = ResultProxy(job)
            target = ResultTarget(test_config.get_tmp_path('{0}.job'.format(result.status)),
                                  config=test_config)
            target.save_result_state(result)

    def tearDown(self):
        test_config.tearDown()

    def test_not_exists_when_state_file_does_not_found(self):
        target = ResultTarget(test_config.get_tmp_path('dummy.job'), config=test_config)
        eq_(target.exists(), False)

    def test_not_exists_when_status_is_not_success(self):
        for job in test_config.get_client().jobs():
            status = job.status()
            if status != 'success':
                target = ResultTarget(test_config.get_tmp_path('{0}.job'.format(status)),
                                      config=test_config)
                eq_(target.exists(), False)

    def test_exists_when_status_is_success(self):
        target = ResultTarget(test_config.get_tmp_path('success.job'), config=test_config)
        eq_(target.exists(), True)
        eq_(type(target.result), ResultProxy)
