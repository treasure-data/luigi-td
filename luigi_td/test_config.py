from config import Config, ConfigLoader, get_config, DEFAULT_ENDPOINT

from unittest import TestCase
from nose.tools import eq_, raises

import os
import luigi
import tdclient

class ConfigTestCase(TestCase):
    def test_create(self):
        config = Config('test-apikey')
        eq_(type(config.get_client()), tdclient.Client)
        eq_(config.apikey, 'test-apikey')
        eq_(config.endpoint, DEFAULT_ENDPOINT)

    def test_create_with_endpoint(self):
        config = Config('test-apikey', endpoint='https://api.example.com')
        eq_(type(config.get_client()), tdclient.Client)
        eq_(config.apikey, 'test-apikey')
        eq_(config.endpoint, 'https://api.example.com')

class ConfigLoaderTestCase(TestCase):
    def setUp(self):
        self.environ = os.environ.copy()
        # clear environment variables
        if 'TD_API_KEY' in os.environ:
            del os.environ['TD_API_KEY']

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.environ)
        if os.path.exists('client.cfg'):
            os.unlink('client.cfg')
            luigi.configuration.LuigiConfigParser._instance = None

    def _config(self, values):
        with file('client.cfg', 'w') as f:
            f.write("[td]\n")
            for key, val in values.iteritems():
                f.write("{0}: {1}\n".format(key, val))
        luigi.configuration.LuigiConfigParser._instance = None

    def _get_config(self):
        loader = ConfigLoader()
        loader.load_default()
        return loader.get_config()

    @raises(ValueError)
    def test_no_config(self):
        config = self._get_config()
        eq_(config.apikey, None)
        config.get_client()

    def test_apikey_by_environ(self):
        os.environ['TD_API_KEY'] = 'test-apikey'
        config = self._get_config()
        eq_(config.apikey, 'test-apikey')

    def test_apikey_by_luigi_config(self):
        self._config({'apikey': 'test-apikey'})
        config = self._get_config()
        eq_(config.apikey, 'test-apikey')

class GetConfigTestCase(TestCase):
    def test_default(self):
        eq_(type(get_config()), Config)
