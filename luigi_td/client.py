import six

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
        return self.job.result_size

    @property
    def description(self):
        return self.job.result_schema

    def __iter__(self):
        return self.job.result()

    def to_csv(self, path_or_file):
        def _to_csv(f):
            # TODO: need optimization
            f.write(",".join([c[0] for c in self.description]))
            f.write("\n")
            for row in self:
                f.write(",".join([str(c) if c else '' for c in row]) + "\n")

        if isinstance(path_or_file, six.string_types):
            with open(path_or_file, 'w') as f:
                return _to_csv(f)
        else:
            return _to_csv(path_or_file)

    def to_dataframe(self):
        # TODO: need optimization
        import pandas as pd
        return pd.DataFrame(iter(self), columns=[c[0] for c in self.description])
