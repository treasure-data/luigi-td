import os
import luigi
import tdclient

import logging
logger = logging.getLogger('luigi-interface')

class ConfigWrapper(object):
    def __init__(self):
        self._config = luigi.configuration.get_config()
        self._state_store = None

    def get(self, name, default=None):
        return self._config.get('td', name, default)

    @property
    def api_key(self):
        return self.get('api-key', os.environ.get('TD_API_KEY'))

    @property
    def state_store(self):
        if not self._state_store:
            import luigi_td.state_store
            state_dir = self.get('local-state-dir')
            if state_dir:
                self._state_store = luigi_td.state_store.LocalStateStore(state_dir)
            else:
                self._state_store = luigi_td.state_store.MemoryStateStore()
        return self._state_store

    def get_client(self):
        return tdclient.Client(self.api_key)

config = ConfigWrapper()
