import os
import luigi
import tdclient

import logging
logger = logging.getLogger('luigi-interface')

DEFAULT_ENDPOINT = 'https://api.treasuredata.com/'

__all__ = ['Config', 'get_config']

class Config(object):
    def __init__(self, apikey, endpoint=DEFAULT_ENDPOINT):
        self.apikey = apikey
        self.endpoint = endpoint

    def get_client(self):
        return tdclient.Client(self.apikey, endpoint=self.endpoint)

class ConfigLoader(object):
    _instance = None

    @classmethod
    def instance(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
            cls._instance.load_default()
        return cls._instance

    def __init__(self):
        self.config = None

    def get_config(self):
        return self.config

    def load_default(self):
        luigi_config = luigi.configuration.get_config()
        apikey = luigi_config.get('td', 'apikey', os.environ.get('TD_API_KEY'))
        endpoint = luigi_config.get('td', 'endpoint', DEFAULT_ENDPOINT)
        self.config = Config(apikey, endpoint=endpoint)

def get_config():
    return ConfigLoader.instance().get_config()
