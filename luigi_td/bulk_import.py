from .config import get_config

import json
import os
import subprocess
import shutil
import time
import luigi
import luigi.s3
import tdclient

from six.moves.urllib.parse import urlparse

import logging
logger = logging.getLogger('luigi-interface')

class BulkImportUploadContext(object):
    def __init__(self, env=None, log_dir='log', tmp_dir='tmp'):
        self._env = env or {}
        self._cur_dir = os.getcwd()
        self._cur_env = os.environ.copy()
        self.log_dir = log_dir
        self.tmp_dir = tmp_dir

    def __enter__(self):
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        os.chdir(self.log_dir)
        os.environ.update(self._env)
        return self

    def __exit__(self, exc_type, value, traceback):
        os.environ.clear()
        os.environ.update(self._cur_env)
        os.chdir(self._cur_dir)
        shutil.rmtree(self.tmp_dir)

class BulkImportTarget(luigi.Target):
    config = get_config()

    def __init__(self, session):
        self._session = session
        self._bulk_import = None

    def session(self):
        if not self._bulk_import:
            try:
                client = self.config.get_client()
                self._bulk_import = client.bulk_import(self._session)
            except tdclient.api.NotFoundError:
                self._bulk_import = None
        return self._bulk_import

    def exists(self):
        return self.session() != None

class BulkImportSession(luigi.Task):
    config = get_config()
    session = luigi.Parameter()
    options = luigi.Parameter(significant=False)

    def output(self):
        return BulkImportTarget(self.session)

    def run(self):
        options = json.loads(self.options)
        client = self.config.get_client()
        client.create_bulk_import(self.session, options['database'], options['table'])

class BulkImportUpload(luigi.Task):
    config = get_config()
    session = luigi.Parameter()
    options = luigi.Parameter(significant=False)

    def requires(self):
        return BulkImportSession(self.session, options=self.options)

    def complete(self):
        client = self.config.get_client()
        try:
            session = client.bulk_import(self.session)
        except tdclient.api.NotFoundError:
            return False
        else:
            return session.upload_frozen

    @property
    def log_dir(self):
        luigi_config = luigi.configuration.get_config()
        return os.path.abspath(os.path.join(luigi_config.get('td', 'log-dir', 'log'), self.session))

    @property
    def tmp_dir(self):
        luigi_config = luigi.configuration.get_config()
        return os.path.abspath(os.path.join(luigi_config.get('td', 'tmp-dir', 'tmp'), self.session))

    def full_path(self):
        options = json.loads(self.options)
        return [options['path']]

    def build_command_line(self):
        options = json.loads(self.options)
        args = [
            'td',
            '-e', self.config.endpoint,
            '-k', self.config.apikey,
            'import:upload',
            self.session,
            '--output', os.path.join(self.tmp_dir, 'parts'),
            '--error-records-output', os.path.join(self.log_dir, 'error-records'),
            '--time-column', options['time_column'],
            '--time-format', options['time_format'],
            '--encoding', 'utf-8',
            '--prepare-parallel', '8',
            '--parallel', '8',
            '--columns', ','.join([c.split(':')[0] for c in options['columns']]),
            '--column-types', ','.join([c.split(':')[1] for c in options['columns']]),
            # '--error-records-handling', 'abort',
            '--empty-as-null-if-numeric',
        ]
        if options.get('format'):
            args += ['--format', options['format']]
        if options.get('column_header'):
            args += ['--column-header']
        args += self.full_path()
        env = {}
        env['TD_TOOLBELT_JAR_UPDATE'] = '0'
        if options.get('default_timezone'):
            env['TZ'] = options['default_timezone']
        return env, args

    def run_upload(self, env, args):
        with BulkImportUploadContext(env, log_dir=self.log_dir, tmp_dir=self.tmp_dir):
            logger.debug(' '.join(args))
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            _, err = p.communicate()
            if p.returncode != 0:
                raise RuntimeError(err)

    def run(self):
        self.run_upload(*self.build_command_line())
        client = self.config.get_client()
        client.freeze_bulk_import(self.session)

class BulkImportPerform(luigi.Task):
    config = get_config()
    session = luigi.Parameter()
    options = luigi.Parameter(significant=False)

    def requires(self):
        return BulkImportUpload(self.session, options=self.options)

    def get_session(self):
        client = self.config.get_client()
        try:
            return client.bulk_import(self.session)
        except tdclient.api.NotFoundError:
            return None

    def complete(self):
        session = self.get_session()
        if session and session.status == 'ready':
            return True
        return False

    def run(self):
        client = self.config.get_client()
        session = self.get_session()
        if session.status == 'uploading':
            job = client.perform_bulk_import(self.session)
        else:
            job = client.job(session.job_id)
        logger.info("%s: performing...", self)
        while not job.finished():
            time.sleep(2)

class BulkImport(luigi.Task):
    config = get_config()
    session = luigi.Parameter()
    database = luigi.Parameter(significant=False)
    table = luigi.Parameter(significant=False)
    format = luigi.Parameter(significant=False, default='csv')
    columns = luigi.Parameter(significant=False)
    time_column = luigi.Parameter(significant=False)
    time_format = luigi.Parameter(significant=False)
    default_timezone = luigi.Parameter(significant=False, default=None)
    # CSV/TSV options
    column_header = luigi.BooleanParameter(significant=False, default=False)
    # steps
    no_commit = luigi.BooleanParameter(default=False)

    def complete(self):
        client = self.config.get_client()
        try:
            session = client.bulk_import(self.session)
        except tdclient.api.NotFoundError:
            return False
        else:
            if self.no_commit:
                return session.status == 'ready'
            else:
                return session.status == 'committed'

    def get_path(self):
        target = self.input()
        # LocalTarget
        if isinstance(target, luigi.LocalTarget):
            return os.path.abspath(target.path)
        # S3Target
        if isinstance(target, luigi.s3.S3Target):
            url = urlparse(target.path)
            return "s3://{aws_access_key_id}:{aws_secret_access_key}@/{bucket}{path}".format(
                aws_access_key_id = target.fs.s3.aws_access_key_id,
                aws_secret_access_key = target.fs.s3.aws_secret_access_key,
                bucket = url.hostname,
                path = url.path
            )
        raise ValueError('unsupported target: {0}'.format(target))

    def run(self):
        options = {
            'database': self.database,
            'table': self.table,
            'path': self.get_path(),
            'format': self.format,
            'columns': self.columns,
            'time_column': self.time_column,
            'time_format': self.time_format,
            'default_timezone': self.default_timezone,
            'column_header': self.column_header,
        }

        # upload and perform
        yield BulkImportPerform(self.session, options=json.dumps(options))

        # commit
        if not self.no_commit:
            client = self.config.get_client()
            client.commit_bulk_import(self.session)
