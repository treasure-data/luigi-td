import luigi
from luigi_td.config import get_config

import json
import os

import logging
logger = logging.getLogger('luigi-interface')

__all__ = ['ResultTarget']

class ResultTarget(luigi.Target):
    def __init__(self, state_path=None, result_url=None, config=None):
        self.state_path = state_path
        self.result_url = result_url
        self.config = config or get_config()

    # Job result handling

    def save_state(self, state):
        if self.state_path:
            state_dir = os.path.dirname(self.state_path)
            if not os.path.exists(state_dir):
                os.makedirs(state_dir)
            with file(self.state_path, 'w') as f:
                json.dump(state, f)

    def get_state(self):
        if self.state_path:
            with file(self.state_path) as f:
                return json.load(f)
        raise TypeError('state_path is required to store job states')

    @property
    def status(self):
        return self.get_state()['status']

    @property
    def job_id(self):
        return self.get_state()['job_id']

    @property
    def job(self):
        if not hasattr(self, '_job'):
            td = self.config.get_client()
            self._job = td.job(self.job_id)
        return self._job

    @property
    def size(self):
        return self.job._result_size

    @property
    def columns(self):
        return self.job._hive_result_schema

    def cursor(self):
        # TODO: need optimization
        return self.job.result()

    def to_csv(self, path):
        # TODO: need optimization
        with file(path, 'w') as f:
            f.write(",".join([c[0] for c in self.columns]))
            f.write("\n")
            for row in self.cursor():
                f.write(",".join([str(c) if c else '' for c in row]) + "\n")

    def to_dataframe(self):
        # TODO: need optimization
        import pandas as pd
        return pd.DataFrame(self.cursor(), columns=[c[0] for c in self.columns])

    # Luigi support

    def exists(self):
        if not self.state_path or not os.path.exists(self.state_path):
            return False
        if self.status == 'success':
            return True
        elif self.status == 'pending':
            td = self.config.get_client()
            if td.job(self.job_id).success():
                return True
        return False

    # For subclasses

    def get_result_url(self):
        return self.result_url
