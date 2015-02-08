from client import ResultProxy
from test_helper import MockJob

from unittest import TestCase
from nose.tools import eq_, raises

import tempfile

SUCCESS_JOB = {
    'job_id': 1,
    'status': 'success',
    'size': 20,
    'description': [['cnt', 'int']],
    'rows': [[5000]],
}

class ResultProxyTestCase(TestCase):
    def test_with_successful_job(self):
        result = ResultProxy(MockJob(SUCCESS_JOB))
        eq_(result.job_id, SUCCESS_JOB['job_id'])
        eq_(result.status, SUCCESS_JOB['status'])
        eq_(result.size, SUCCESS_JOB['size'])
        eq_(result.description, SUCCESS_JOB['description'])
        eq_(list(result), SUCCESS_JOB['rows'])
        with tempfile.NamedTemporaryFile() as f:
            result.to_csv(f.name)
            eq_(f.read(), "cnt\n5000\n")
