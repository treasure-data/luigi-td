__all__ = ['ResultProxy']

class ResultProxy(object):
    def __init__(self, job):
        self.job = job

    @property
    def status(self):
        return self.job.status()

    @property
    def job_id(self):
        return self.job.job_id

    @property
    def size(self):
        return self.job._result_size

    @property
    def description(self):
        return self.job._hive_result_schema

    def __iter__(self):
        return self.job.result()

    def to_csv(self, path_or_file):
        def _to_csv(f):
            # TODO: need optimization
            f.write(",".join([c[0] for c in self.description]))
            f.write("\n")
            for row in self:
                f.write(",".join([str(c) if c else '' for c in row]) + "\n")

        if type(path_or_file) in [str, unicode]:
            with file(path_or_file, 'w') as f:
                return _to_csv(f)
        else:
            return _to_csv(path_or_file)

    def to_dataframe(self):
        # TODO: need optimization
        import pandas as pd
        return pd.DataFrame(iter(self), columns=[c[0] for c in self.description])
