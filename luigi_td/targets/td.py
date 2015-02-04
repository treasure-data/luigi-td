from luigi_td.config import get_config

import luigi
import tdclient

import logging
logger = logging.getLogger('luigi-interface')

__all__ = ['DatabaseTarget', 'TableTarget']

class DatabaseTarget(luigi.Target):
    def __init__(self, database_name):
        self.database_name = database_name

    def exists(self):
        td = config.get_client()
        return self.database_name in [db.name for db in td.databases()]

class TableTarget(luigi.Target):
    def __init__(self, database_name, table_name, schema=[], empty=False):
        self.database_name = database_name
        self.table_name = table_name
        self.schema = schema
        self.empty = empty

    def exists(self):
        td = config.get_client()
        try:
            t = td.table(self.database_name, self.table_name)
        except tdclient.api.NotFoundError:
            return False
        if self.empty:
            if t and t.count == 0 and [':'.join(c) for c in t.schema] == list(self.schema):
                return True
            else:
                logger.debug('Deleting table: {0}.{1}'.format(self.database_name, self.table_name))
                td.delete_table(self.database_name, self.table_name)
                return False
        else:
            if t:
                table_schema = [str(':'.join(c)) for c in t.schema]
                if table_schema != list(self.schema):
                    logger.error('Current schema: {0}'.format(table_schema))
                    logger.error('Expected schema: {0}'.format(self.schema))
                    raise ValueError('table schema for {0}.{1} does not match'.format(self.database_name, self.table_name))
                return True
            else:
                return False
