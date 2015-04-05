from luigi_td.targets.result import ResultTarget

import urllib

import logging
logger = logging.getLogger('luigi-interface')

class TableauServerResultTarget(ResultTarget):
    # required
    server = None
    username = None
    password = None
    datasource = None
    # optional
    ssl = 'true'
    ssl_verify = 'true'
    server_version = None
    site = None
    project = None
    mode = 'replace'

    def get_result_url(self):
        reqs = {}
        for name in ['server', 'username', 'password', 'datasource']:
            if getattr(self, name) is None:
                raise TypeError('missing option "{0}" for {1}'.format(name, self))
            reqs[name] = urllib.quote(getattr(self, name))
        params = {
            'ssl': self.ssl,
            'ssl_verify': self.ssl_verify,
            'server_version': self.server_version,
            'site': self.site,
            'project': self.project,
            'mode': self.mode,
        }
        reqs['params'] = urllib.urlencode([(key, params[key]) for key in params if params[key] is not None])
        return "tableau://{username}:{password}@{server}/{datasource}?{params}".format(**reqs)

class TableauOnlineResultTarget(TableauServerResultTarget):
    server = 'online.tableausoftware.com'
    server_version = 'online'
