from luigi_td.config import get_config
from luigi_td.client import ResultProxy
from luigi_td.targets.result import ResultTarget
from luigi_td.targets.td import DatabaseTarget
from luigi_td.targets.td import TableTarget

import os
import sys
import time
import urllib
import luigi
import jinja2

import logging
logger = logging.getLogger('luigi-interface')

__all__ = ['DatabaseTask', 'TableTask', 'Query']

class DatabaseTask(luigi.Task):
    config = get_config()
    database_name = luigi.Parameter()
    action = luigi.Parameter(default='create')

    def output(self):
        return DatabaseTarget(self.database_name)

    def run(self):
        client = self.config.get_client()
        logger.debug('{0}: creating database: {1}'.format(self, self.database_name))
        client.create_database(self.database_name)

class TableTask(luigi.Task):
    config = get_config()
    database_name = luigi.Parameter()
    table_name = luigi.Parameter()
    action = luigi.Parameter(default='create')
    schema = luigi.Parameter(is_list=True, default=[], significant=False)
    empty = luigi.BooleanParameter(default=False, significant=False)

    def requires(self):
        return DatabaseTask(self.database_name)

    def output(self):
        return TableTarget(self.database_name, self.table_name, self.schema, empty=self.empty)

    def run(self):
        client = self.config.get_client()
        logger.debug('{0}: creating table: {1}.{2}'.format(self, self.database_name, self.table_name))
        client.create_log_table(self.database_name, self.table_name)
        if self.schema != []:
            logger.debug('{0}: updating schema for {1}.{2}'.format(self, self.database_name, self.table_name))
            client.update_schema(self.database_name, self.table_name, [s.split(':') for s in self.schema])

# query

class Query(luigi.Task):
    config = get_config()
    debug = False
    type = 'hive'
    database = None
    source = None
    variables = {}

    def query(self):
        return NotImplemented()

    def load_query(self, source):
        env = jinja2.Environment(loader=jinja2.PackageLoader(self.__module__, '.'))
        template = env.get_template(self.source)
        return template.render(task=self, **self.variables)

    def run_query(self, query):
        result = self.output()
        result_url = None
        if isinstance(result, ResultTarget):
            result_url = result.get_result_url()
        client = self.config.get_client()
        job = client.query(self.database, 
                           query,
                           type = self.type,
                           result_url = result_url)
        job._update_status()
        logger.info("{task}: td.job.url: {url}".format(task=self, url=job.url))

        # wait for the result
        try:
            while not job.finished():
                time.sleep(2)
        except:
            # kill query on exceptions
            job.kill()
            raise
        job._update_status()

        logger.info("{task}: td.job.result: id={job_id} status={status}".format(
            task = self,
            job_id = job.job_id,
            status = job.status(),
        ))

        if not job.success():
            stderr = job._debug['stderr']
            if stderr:
                logger.error(stderr)
            raise RuntimeError("job {0} {1}\n\nOutput:\n{2}".format(job.job_id, job.status(), job._debug['cmdout']))

        return ResultProxy(job)

    def run(self):
        if hasattr(self, 'query_file'):
            self.source = self.query_file
        query = self.load_query(self.source) if self.source else self.query()
        result = self.run_query(query)
        target = self.output()
        if target and isinstance(target, ResultTarget):
            target.save_result_state(result)

        if self.debug:
            import pandas as pd
            TERMINAL_WIDTH = 120
            pd.options.display.width = TERMINAL_WIDTH
            print '-' * TERMINAL_WIDTH
            print 'Query result:'
            print result.to_dataframe()
            print '-' * TERMINAL_WIDTH
