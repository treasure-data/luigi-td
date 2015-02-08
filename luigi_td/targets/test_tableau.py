from ..test_helper import TestConfig
from tableau import TableauServerResultTarget
from tableau import TableauOnlineResultTarget

from unittest import TestCase
from nose.tools import eq_, raises

import urllib
import urlparse

test_config = TestConfig()

class TestTableauServerResultTarget(TableauServerResultTarget):
    server = 'tableau.example.com'
    username = 'test@example.com'
    password = 'test-password'

class TableauServerResultTargetTestCase(TestCase):
    def setUp(self):
        test_config.setUp()

    def tearDown(self):
        test_config.tearDown()

    def test_default(self):
        target = TestTableauServerResultTarget(test_config.get_tmp_path('result.job'))
        target.datasource = 'test-datasource'
        url = urlparse.urlparse(target.get_result_url())
        params = urlparse.parse_qs(url.query)
        eq_(url.scheme, 'tableau')
        eq_(url.hostname, 'tableau.example.com')
        eq_(url.path, '/' + target.datasource)
        eq_(urllib.unquote(url.username), TestTableauServerResultTarget.username)
        eq_(urllib.unquote(url.password), TestTableauServerResultTarget.password)
        eq_(params.get('ssl'), ['true'])
        eq_(params.get('ssl_verify'), ['true'])
        eq_(params.get('version'), None)
        eq_(params.get('site'), None)
        eq_(params.get('project'), None)
        eq_(params.get('mode'), ['replace'])

    def test_options(self):
        target = TestTableauServerResultTarget(test_config.get_tmp_path('result.job'))
        target.version = '8.3'
        target.site = 'test-site'
        target.project = 'test-project'
        target.datasource = 'test-datasource'
        target.mode = 'append'
        url = urlparse.urlparse(target.get_result_url())
        params = urlparse.parse_qs(url.query)
        eq_(params.get('version'), [target.version])
        eq_(params.get('site'), [target.site])
        eq_(params.get('project'), [target.project])
        eq_(params.get('mode'), [target.mode])

class TableauOnlineResultTargetTestCase(TestCase):
    def setUp(self):
        test_config.setUp()

    def tearDown(self):
        test_config.tearDown()

    def test_default(self):
        target = TableauOnlineResultTarget(test_config.get_tmp_path('result.job'))
        target.username = 'test@example.com'
        target.password = 'test-password'
        target.datasource = 'test-datasource'
        url = urlparse.urlparse(target.get_result_url())
        params = urlparse.parse_qs(url.query)
        eq_(url.scheme, 'tableau')
        eq_(url.hostname, 'online.tableausoftware.com')
        eq_(url.path, '/' + target.datasource)
        eq_(urllib.unquote(url.username), target.username)
        eq_(urllib.unquote(url.password), target.password)
        eq_(params['version'], ['online'])
        eq_(params['mode'], ['replace'])
