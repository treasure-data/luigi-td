import os
import luigi
import tdclient

import logging
logger = logging.getLogger('luigi-interface')

class ConfigWrapper(object):
    def __init__(self):
        self._config = luigi.configuration.get_config()
        self._state_store = None

    def set(self, name, value):
        self._config[name] = value

    def get(self, name, default=None):
        return self._config.get(name, default)

    @property
    def api_key(self):
        return self._config.get('td', 'api-key', os.environ.get('TD_API_KEY'))

    @property
    def state_store(self):
        if not self._state_store:
            import luigi_td.state_store
            state_dir = self._config.get('td', 'local-state-dir')
            if state_dir:
                self._state_store = luigi_td.state_store.LocalStateStore(state_dir)
            else:
                self._state_store = luigi_td.state_store.MemoryStateStore()
        return self._state_store

    def get_client(self):
        return tdclient.Client(self.api_key)

config = ConfigWrapper()
