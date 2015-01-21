from luigi_td.config import config
from luigi_td.result import QueryResult
from luigi_td.target import DatabaseTarget
from luigi_td.target import TableTarget

import time
import urllib
import luigi
import jinja2

import logging
logger = logging.getLogger('luigi-interface')

class CreateDatabase(luigi.Task):
    database_name = luigi.Parameter()

    def output(self):
        return DatabaseTarget(self.database_name)

    def run(self):
        td = config.get_client()
        logger.debug('Creating database: {0}'.format(self.database_name))
        td.create_database(self.database_name)

class CreateTable(luigi.Task):
    database_name = luigi.Parameter()
    table_name = luigi.Parameter()
    schema = luigi.Parameter(is_list=True, significant=False)
    empty = luigi.BooleanParameter(significant=False)

    def requires(self):
        return CreateDatabase(self.database_name)

    def output(self):
        return TableTarget(self.database_name, self.table_name, self.schema, empty=self.empty)

    def run(self):
        td = config.get_client()
        logger.debug('Creating table: {0}.{1}'.format(self.database_name, self.table_name))
        td.create_log_table(self.database_name, self.table_name)
        logger.debug('Updating schema for {0}.{1}'.format(self.database_name, self.table_name))
        td.update_schema(self.database_name, self.table_name, [s.split(':') for s in self.schema])

# query

class Scheduled(object):
    scheduled_time = luigi.Parameter()

class DailyScheduled(Scheduled):
    scheduled_time = luigi.DateParameter()

class HourlyScheduled(Scheduled):
    scheduled_time = luigi.DateHourParameter()

class Query(luigi.Task, QueryResult):
    type = 'hive'
    database = None
    query = None
    variables = {}

    @property
    def query_ext(self):
        return 'hql' if self.type == 'hive' else 'sql'

    def resolve(self, name):
        v = getattr(self, name)
        if callable(v):
            return v()
        else:
            return v

    def query_header(self):
        pass

    def query_body(self):
        return file(self['query']).read()

    def output(self):
        return config.state_store.get_target(self)

    def run(self):
        # build a query
        header = self.query_header()
        query = jinja2.Template(self.query_body()).render(task=self, **self.resolve('variables'))
        if header:
            query = header + "\n" + query
        # run the query
        td = config.get_client()
        job = td.query(self.resolve('database'), 
                       query,
                       type = self.type,
                       result_url = self.get_result_url())
        job._update_status()
        logger.info("{task}: td.job_url: {url}".format(task=self, url=job.url))
        # wait for the result
        while not job.finished():
            time.sleep(2)
        job._update_status()
        logger.info("{task}: td.job_result: id={job_id} status={status} elapsed={elapsed}".format(
            task = self,
            job_id = job.job_id,
            status = job.status(),
            elapsed = '(not implemented)',
        ))
        # output
        target = self.output()
        target.save_state({'job_id': job.job_id, 'status': job.status()})
        if not job.success():
            stderr = job._debug['stderr']
            if stderr:
                print stderr
            raise RuntimeError("job {0} {1}\n\nOutput:\n{2}".format(job.job_id, job.status(), job._debug['cmdout']))

class InsertIntoQuery(Query):
    table = luigi.Parameter()
    schema = luigi.Parameter(is_list=True, significant=False)
    overwrite = False
    empty = False

    def table_requires(self):
        name = self.resolve('table')
        database_name, table_name = name.split('.')
        if self.type == 'presto' and self.overwrite:
            logger.warning('Presto does not support "INSERT OVERWRITE".  Rebuilding the table...')
            self.empty = True
        return [CreateTable(database_name, table_name, schema=self.resolve('schema'), empty=self.empty)]
        return []

    def query_body(self):
        name = self.resolve('table')
        query = super(InsertIntoQuery, self).query_body()
        if self.type == 'hive':
            if self.overwrite:
                return "INSERT OVERWRITE TABLE {0}\n".format(name) + query
            else:
                return "INSERT INTO TABLE {0}\n".format(name) + query
        elif self.type == 'presto':
            return "INSERT INTO {0}\n".format(name) + query
        else:
            raise ValueError('Query type "{0}" does not support INSERT INTO'.format(self.type))
