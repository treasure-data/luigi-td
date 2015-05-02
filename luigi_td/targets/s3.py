from luigi_td.targets.result import ResultTarget

from six.moves.urllib.parse import urlencode
from six.moves.urllib.parse import quote as url_quote

import logging
logger = logging.getLogger('luigi-interface')

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
        reqs['access_key'] = url_quote(self.aws_access_key_id)
        reqs['secret_key'] = url_quote(self.aws_secret_access_key)
        reqs['bucket'] = self.bucket
        reqs['path'] = self.path
        # optional
        params = {}
        for name in ['format']:
            if hasattr(self, name):
                params[name] = getattr(self, name)
        reqs['params'] = urlencode([(key, params[key]) for key in params if params[key]])
        return "s3://{access_key}:{secret_key}@/{bucket}/{path}?{params}".format(**reqs)
