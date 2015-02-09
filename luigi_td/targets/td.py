from luigi_td.config import get_config

import luigi
import tdclient

import logging
logger = logging.getLogger('luigi-interface')

__all__ = ['DatabaseTarget', 'TableTarget', 'SchemaError']

class SchemaError(Exception):
    pass

class DatabaseTarget(luigi.Target):
    def __init__(self, database_name, config=None):
        self.database_name = database_name
        self.config = config or get_config()

    def exists(self):
        client = self.config.get_client()
        return self.database_name in [db.name for db in client.databases()]

class TableTarget(luigi.Target):
    def __init__(self, database_name, table_name, schema=None, empty=False, config=None):
        self.database_name = database_name
        self.table_name = table_name
        self.schema = schema or []
        self.empty = empty
        self.config = config or get_config()

    def exists(self):
        client = self.config.get_client()
        try:
            t = client.table(self.database_name, self.table_name)
        except tdclient.api.NotFoundError:
            return False
        if self.empty:
            if t and t.count == 0 and [':'.join(c) for c in t.schema] == list(self.schema):
                return True
            else:
                logger.debug('Deleting table: {0}.{1}'.format(self.database_name, self.table_name))
                client.delete_table(self.database_name, self.table_name)
                return False
        else:
            if t:
                table_schema = [str(':'.join(c)) for c in t.schema]
                if table_schema != list(self.schema):
                    logger.error('Current schema: {0}'.format(table_schema))
                    logger.error('Expected schema: {0}'.format(self.schema))
                    raise SchemaError('table schema for {0}.{1} does not match'.format(self.database_name, self.table_name))
                return True
            else:
                return False
