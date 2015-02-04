from luigi_td.config import get_config
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

class CreateDatabase(luigi.Task):
    config = get_config()
    database_name = luigi.Parameter()

    def output(self):
        return DatabaseTarget(self.database_name)

    def run(self):
        td = self.config.get_client()
        logger.debug('Creating database: {0}'.format(self.database_name))
        td.create_database(self.database_name)

class CreateTable(luigi.Task):
    config = get_config()
    database_name = luigi.Parameter()
    table_name = luigi.Parameter()
    schema = luigi.Parameter(is_list=True, significant=False)
    empty = luigi.BooleanParameter(significant=False)

    def requires(self):
        return CreateDatabase(self.database_name)

    def output(self):
        return TableTarget(self.database_name, self.table_name, self.schema, empty=self.empty)

    def run(self):
        td = self.config.get_client()
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

class Query(luigi.Task):
    config = get_config()
    type = 'hive'
    database = None
    query_file = None
    variables = {}

    def query(self):
        pass

    def _load_query(self):
        if self.query_file:
            # query file
            env = jinja2.Environment(loader=jinja2.PackageLoader(self.__module__, '.'))
            template = env.get_template(self.query_file)
        else:
            # query string
            template = jinja2.Template(self.query())
        return template.render(task=self, **self.variables)

    def _run_query(self, query):
        result = self.output()
        td = self.config.get_client()
        job = td.query(self.database, 
                       query,
                       type = self.type,
                       result_url = result.get_result_url() if result else None)
        job._update_status()
        result.save_state({'job_id': job.job_id, 'status': job.status()})
        logger.info("{task}: td.job_url: {url}".format(task=self, url=job.url))

        # wait for the result
        try:
            while not job.finished():
                time.sleep(2)
        except:
            # kill query on exceptions
            job.kill()
        job._update_status()
        result.save_state({'job_id': job.job_id, 'status': job.status()})
        logger.info("{task}: td.job_result: id={job_id} status={status} cpu_time={cpu_time}".format(
            task = self,
            job_id = job.job_id,
            status = job.status(),
            cpu_time = job._cpu_time,
        ))
        return job

    def run(self):
        query = self._load_query()
        job = self._run_query(query)

        # output
        if not job.success():
            stderr = job._debug['stderr']
            if stderr:
                logger.error(stderr)
            raise RuntimeError("job {0} {1}\n\nOutput:\n{2}".format(job.job_id, job.status(), job._debug['cmdout']))
