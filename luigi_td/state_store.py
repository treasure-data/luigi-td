from luigi_td.config import config

import os
import json
import luigi

import logging
logger = logging.getLogger('luigi-interface')

__all__ = ['MemoryStateTarget', 'MemoryStateStore', 'LocalStateTarget', 'LocalStateStore']

class StateTarget(luigi.Target):
    @property
    def job(self):
        if not hasattr(self, '_job'):
            td = config.get_client()
            self._job = td.job(self.state['job_id'])
        return self._job

    @property
    def job_id(self):
        return self.state['job_id']

    @property
    def status(self):
        return self.state['status']

    @property
    def size(self):
        return self.job._result_size

    @property
    def columns(self):
        return self.job._hive_result_schema

    def cursor(self):
        return self.job.result()

    def dump(self, path):
        with file(path, 'w') as f:
            f.write("\t".join([c[0] for c in self.columns]))
            f.write("\n")
            for row in self.cursor():
                f.write("\t".join([str(c) for c in row]))
                f.write("\n")

# Memory state

class MemoryStateTarget(StateTarget):
    def __init__(self):
        self.state = None

    def exists(self):
        if self.state and self.state['status'] == 'success':
            return True
        return False

    def save_state(self, state):
        self.state = state

class MemoryStateStore(object):
    def __init__(self):
        self.state_cache = {}

    def get_target(self, task):
        if not self.state_cache.has_key(task):
            self.state_cache[str(task)] = MemoryStateTarget()
        return self.state_cache[str(task)]

# Local state

class LocalStateTarget(StateTarget):
    def __init__(self, path):
        self.path = path

    def exists(self):
        if not os.path.exists(self.path):
            return False
        state = self.state
        if state['status'] == 'success':
            return True
        td = config.get_client()
        job = td.job(state['job_id'])
        if job.success():
            return True
        return False

    def save_state(self, state):
        with file(self.path, 'w') as f:
            json.dump(state, f)

    @property
    def state(self):
        with file(self.path) as f:
            return json.load(f)

class LocalStateStore(object):
    def __init__(self, state_dir):
        self.state_dir = state_dir

    def get_target_path(self, task):
        if hasattr(task, 'schedule_id'):
            target_dir = os.path.join(self.state_dir, task.schedule_id())
        else:
            target_dir = self.state_dir
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        return os.path.join(target_dir, str(task))

    def get_target(self, task):
        return LocalStateTarget(self.get_target_path(task))
