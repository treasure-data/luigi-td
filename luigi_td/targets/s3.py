from luigi_td.targets.result import ResultTarget

import logging
logger = logging.getLogger('luigi-interface')

__all__ = ['S3ResultTarget']

class S3ResultTarget(ResultTarget):
    aws_access_key_id = None
    aws_secret_access_key = None
    s3_path = None
    s3_format = 'tsv'
    s3_delimiter = None
    s3_quote = None
    s3_escape = None
    s3_null = None
    s3_newline = None
    s3_header = None

    def get_result_url(self):
        reqs = {}
        # required
        reqs['access_key'] = urllib.quote(self.aws_access_key_id)
        reqs['secret_key'] = urllib.quote(self.aws_secret_access_key)
        reqs['path'] = self.s3_path
        # optional
        params = {}
        if self.s3_format:
            params['format'] = self.s3_format
        if self.s3_delimiter:
            params['delimiter'] = self.s3_delimiter
        if self.s3_quote:
            params['quote'] = self.s3_quote
        if self.s3_escape:
            params['escape'] = self.s3_escape
        if self.s3_null:
            params['null'] = self.s3_null
        if self.s3_newline:
            params['newline'] = self.s3_newline
        if self.s3_header:
            params['header'] = self.s3_header
        reqs['params'] = urllib.urlencode([(key, params[key]) for key in params if params[key] is not None])
        return "s3://{access_key}:{secret_key}@/{path}?{params}".format(**reqs)
