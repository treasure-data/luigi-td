import urllib

import logging
logger = logging.getLogger('luigi-interface')

__all__ = ['ResultOutput', 'S3ResultOutput', 'TableauServerResultOutput', 'TableauOnlineResultOutput']

class ResultOutput(object):
    @property
    def result_url(self):
        return None

class S3ResultOutput(ResultOutput):
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

    @property
    def result_url(self):
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

class TableauServerResultOutput(ResultOutput):
    # required
    tableau_server = None
    tableau_username = None
    tableau_password = None
    tableau_datasource = None
    # optional
    tableau_ssl = 'true'
    tableau_ssl_verify = 'true'
    tableau_version = None
    tableau_site = None
    tableau_project = None
    tableau_mode = 'replace'

    @property
    def result_url(self):
        reqs = {}
        for key in ['server', 'username', 'password', 'datasource']:
            name = 'tableau_' + key
            if getattr(self, name) is None:
                raise TypeError('missing option "{0}" for {1}'.format(name, self))
            reqs[key] = urllib.quote(getattr(self, name))
        params = {
            'ssl': self.tableau_ssl,
            'ssl_verify': self.tableau_ssl_verify,
            'version': self.tableau_version,
            'site': self.tableau_site,
            'project': self.tableau_project,
            'mode': self.tableau_mode,
        }
        reqs['params'] = urllib.urlencode([(key, params[key]) for key in params if params[key] is not None])
        return "tableau://{username}:{password}@{server}/{datasource}?{params}".format(**reqs)

class TableauOnlineResultOutput(TableauServerResultOutput):
    tableau_server = 'online.tableausoftware.com'
    tableau_version = 'online'
    tableau_ssl = 'true'
    tableau_ssl_verify = 'true'
