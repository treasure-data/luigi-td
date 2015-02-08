import luigi
from luigi_td.config import get_config
from luigi_td.client import ResultProxy

import json
import os

import logging
logger = logging.getLogger('luigi-interface')

__all__ = ['ResultTarget']

class ResultTarget(luigi.Target):
    def __init__(self, path, result_url=None, config=None):
        self.path = path
        self.result_url = result_url
        self.config = config or get_config()

    # Job result handling

    def save_result_state(self, result):
        state_dir = os.path.dirname(self.path)
        if state_dir != '' and not os.path.exists(state_dir):
            os.makedirs(state_dir)
        with file(self.path, 'w') as f:
            state = {'job_id': result.job_id, 'status': result.status}
            json.dump(state, f)

    def load_result_state(self):
        with file(self.path) as f:
            return json.load(f)

    @property
    def job_id(self):
        return self.load_result_state()['job_id']

    @property
    def status(self):
        return self.load_result_state()['status']

    @property
    def result(self):
        if not hasattr(self, '_result'):
            client = self.config.get_client()
            self._result = ResultProxy(client.job(self.job_id))
        return self._result

    # Luigi support

    def exists(self):
        if not os.path.exists(self.path):
            return False
        if self.status in ['success']:
            return True
        if self.status in ['error', 'killed']:
            return False
        # TODO: should we wait for the end of the job?
        client = self.config.get_client()
        if client.job(self.job_id).success():
            return True
        return False

    # For subclasses

    def get_result_url(self):
        return self.result_url
