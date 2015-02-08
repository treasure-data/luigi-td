from ..test_helper import TestEnv, MockJob
from s3 import S3ResultTarget

from unittest import TestCase
from nose.tools import eq_, raises

import urllib
import urlparse

env = TestEnv()

class TableauServerResultTargetTestCase(TestCase):
    def setUp(self):
        env.setUp()

    def tearDown(self):
        env.tearDown()

    def test_default(self):
        target = S3ResultTarget(env.get_tmp_path('result.job'))
        target.aws_access_key_id = 'AWS_ACCESS_KEY_ID'
        target.aws_secret_access_key = 'AWS_SECRET_ACCESS_KEY'
        target.bucket = 'test-bucket'
        target.path = 'test-prefix/test.tsv'
        print target.get_result_url()
        url = urlparse.urlparse(target.get_result_url())
        params = urlparse.parse_qs(url.query)
        eq_(url.scheme, 's3')
        eq_(url.path, '/{0}/{1}'.format(target.bucket, target.path))
        eq_(urllib.unquote(url.username), target.aws_access_key_id)
        eq_(urllib.unquote(url.password), target.aws_secret_access_key)
        eq_(params.get('format'), ['tsv'])
