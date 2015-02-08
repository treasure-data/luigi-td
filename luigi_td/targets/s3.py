from luigi_td.targets.result import ResultTarget

import urllib

import logging
logger = logging.getLogger('luigi-interface')

__all__ = ['S3ResultTarget']

class S3ResultTarget(ResultTarget):
    aws_access_key_id = None
    aws_secret_access_key = None
    bucket = None
    path = None
    format = 'tsv'
    delimiter = None
    quote = None
    escape = None
    null = None
    newline = None
    header = None

    def get_result_url(self):
        reqs = {}
        # required
        reqs['access_key'] = urllib.quote(self.aws_access_key_id)
        reqs['secret_key'] = urllib.quote(self.aws_secret_access_key)
        reqs['bucket'] = self.bucket
        reqs['path'] = self.path
        # optional
        params = {}
        for name in ['format']:
            if hasattr(self, name):
                params[name] = getattr(self, name)
        reqs['params'] = urllib.urlencode([(key, params[key]) for key in params if params[key]])
        return "s3://{access_key}:{secret_key}@/{bucket}/{path}?{params}".format(**reqs)
