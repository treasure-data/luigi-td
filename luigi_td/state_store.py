import os
import json
import luigi

import logging
logger = logging.getLogger('luigi-interface')

__all__ = ['MemoryStateTarget', 'MemoryStateStore', 'LocalStateTarget', 'LocalStateStore']

# Memory state

class MemoryStateTarget(luigi.Target):
    def __init__(self):
        self.state = None

    def exists(self):
        if self.state and self.state['status'] == 'success':
            return True
        return False

    def save(self, state):
        self.state = state

class MemoryStateStore(object):
    def __init__(self):
        self.state_cache = {}

    def get_target(self, task):
        if not self.state_cache.has_key(task):
            self.state_cache[str(task)] = MemoryStateTarget()
        return self.state_cache[str(task)]

# Local state

class LocalStateTarget(luigi.Target):
    def __init__(self, path):
        self.path = path

    def exists(self):
        if not os.path.exists(self.path):
            return False
        with file(self.path) as f:
            state = json.load(f)
        if state['status'] == 'success':
            return True
        td = _config.get_client()
        job = td.job(state['job_id'])
        if job.success():
            return True
        return False

    def save(self, state):
        with file(self.path, 'w') as f:
            json.dump(state, f)

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
