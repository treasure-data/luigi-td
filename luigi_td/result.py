import urllib

import logging
logger = logging.getLogger('luigi-interface')

__all__ = ['QueryResult', 'QueryResultTableauServer', 'QueryResultTableauOnline']

class QueryResult(object):
    def get_result_url(self):
        return None

class QueryResultTableauServer(QueryResult):
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

    def get_result_url(self):
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

class QueryResultTableauOnline(QueryResultTableauServer):
    tableau_server = 'online.tableausoftware.com'
    tableau_version = 'online'
    tableau_ssl = 'true'
    tableau_ssl_verify = 'true'
