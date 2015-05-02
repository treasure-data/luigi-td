from .bulk_import import BulkImportUploadContext
from .bulk_import import BulkImportTarget
from .bulk_import import BulkImportSession
from .bulk_import import BulkImportUpload
from .bulk_import import BulkImportPerform
from .bulk_import import BulkImport
from .test_helper import MockJob
from .test_helper import TestConfig

from unittest import TestCase
from nose.tools import ok_, eq_, raises

import json
import os
import shutil
import tempfile
import luigi

test_config = TestConfig(
    jobs = [
        {
            'job_id': 1,
            'status': 'success',
        }
    ],
    bulk_imports = [
        {
            'session': 'session-1',
            'status': 'uploading',
            'frozen': False,
        },
        {
            'session': 'session-2',
            'status': 'uploading',
            'frozen': True,
        },
        {
            'session': 'session-3',
            'status': 'performing',
            'frozen': True,
            'job_id': 1,
        },
        {
            'session': 'session-4',
            'status': 'ready',
            'frozen': True,
            'job_id': 1,
        },
        {
            'session': 'session-5',
            'status': 'committing',
            'frozen': True,
            'job_id': 1,
        },
        {
            'session': 'session-6',
            'status': 'committed',
            'frozen': True,
            'job_id': 1,
        },
    ]
)

class BulkImportUploadContextTestCase(TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_creates_temporary_environ(self):
        env = {'TEST_ENV': 'ok'}
        eq_(os.environ.get('TEST_ENV'), None)
        with BulkImportUploadContext(env):
            eq_(os.environ.get('TEST_ENV'), 'ok')
        eq_(os.environ.get('TEST_ENV'), None)

    def test_creates_directories(self):
        cur_dir = os.getcwd()
        log_dir = os.path.realpath(os.path.join(self.test_dir, 'log'))
        tmp_dir = os.path.realpath(os.path.join(self.test_dir, 'tmp'))
        with BulkImportUploadContext(log_dir=log_dir, tmp_dir=tmp_dir):
            eq_(os.getcwd(), log_dir)
            eq_(os.path.exists(log_dir), True)
            eq_(os.path.exists(tmp_dir), True)
        eq_(os.getcwd(), cur_dir)
        eq_(os.path.exists(log_dir), True)
        eq_(os.path.exists(tmp_dir), False)

class TestBulkImportTarget(BulkImportTarget):
    config = test_config

class BulkImportTargetTestCase(TestCase):
    def test_not_exists(self):
        target = TestBulkImportTarget('none')
        eq_(target.exists(), False)

    def test_exists(self):
        target = TestBulkImportTarget('session-1')
        eq_(target.exists(), True)

class TestBulkImportSession(BulkImportSession):
    config = test_config

class BulkImportSessionTestCase(TestCase):
    test_options = {
        'database': 'test_db',
        'table': 'test_tb',
    }

    def test_output(self):
        task = TestBulkImportSession('session-1', options=json.dumps(self.test_options))
        eq_(type(task.output()), BulkImportTarget)

    def test_run(self):
        task = TestBulkImportSession('session-1', options=json.dumps(self.test_options))
        task.run()

class TestBulkImportUpload(BulkImportUpload):
    config = test_config

class BulkImportUploadTestCase(TestCase):
    test_options = {
        'database': 'test_db',
        'table': 'test_tb',
        'path': 'test_input.tsv',
        'format': 'tsv',
        'columns': ['date:string', 'x:int', 'y:int'],
        'time_column': 'date',
        'time_format': '%Y-%m-%d',
    }

    def test_requires(self):
        task = TestBulkImportUpload('session-1', options=json.dumps(self.test_options))
        eq_(type(task.requires()), BulkImportSession)

    def test_complete(self):
        task = TestBulkImportUpload('none', options=json.dumps(self.test_options))
        eq_(task.complete(), False)
        task = TestBulkImportUpload('session-1', options=json.dumps(self.test_options))
        eq_(task.complete(), False)
        task = TestBulkImportUpload('session-2', options=json.dumps(self.test_options))
        eq_(task.complete(), True)

    def test_command_line(self):
        task = TestBulkImportUpload('session-1', options=json.dumps(self.test_options))
        env, args = task.build_command_line()
        # env
        eq_(env['TD_TOOLBELT_JAR_UPDATE'], '0')
        # args
        ok_(task.config.endpoint in args)
        ok_(task.config.apikey in args)
        ok_('import:upload' in args)
        ok_('session-1' in args)
        ok_(os.path.join(task.tmp_dir, 'parts') in args)
        ok_(os.path.join(task.log_dir, 'error-records') in args)
        ok_(self.test_options['path'] in args)
        ok_(self.test_options['format'] in args)
        ok_(self.test_options['time_column'] in args)
        ok_(self.test_options['time_format'] in args)
        ok_('date,x,y' in args)
        ok_('string,int,int' in args)
        ok_('--empty-as-null-if-numeric' in args)

class TestBulkImportPerform(BulkImportPerform):
    config = test_config

class BulkImportPerformTestCase(TestCase):
    test_options = {
        'database': 'test_db',
        'table': 'test_tb',
    }

    def test_requires(self):
        task = TestBulkImportPerform('session-1', options=json.dumps(self.test_options))
        eq_(type(task.requires()), BulkImportUpload)

    def test_complete(self):
        task = TestBulkImportPerform('none', options=json.dumps(self.test_options))
        eq_(task.complete(), False)
        task = TestBulkImportPerform('session-1', options=json.dumps(self.test_options))
        eq_(task.complete(), False)
        task = TestBulkImportPerform('session-2', options=json.dumps(self.test_options))
        eq_(task.complete(), False)
        task = TestBulkImportPerform('session-3', options=json.dumps(self.test_options))
        eq_(task.complete(), False)
        task = TestBulkImportPerform('session-4', options=json.dumps(self.test_options))
        eq_(task.complete(), True)

    def test_run_2(self):
        task = TestBulkImportPerform('session-2', options=json.dumps(self.test_options))
        task.run()

    def test_run_3(self):
        task = TestBulkImportPerform('session-3', options=json.dumps(self.test_options))
        task.run()

    def test_run_4(self):
        task = TestBulkImportPerform('session-4', options=json.dumps(self.test_options))
        task.run()

class TestBulkImportInput(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget('test_input.tsv')

class TestBulkImport(BulkImport):
    config = test_config
    database = 'test_db'
    table = 'test_tb'
    format = 'tsv'
    columns = ['date:string', 'x:int', 'y:int']
    time_column = 'date'
    time_format = '%Y-%m-%d'

    def requires(self):
        return TestBulkImportInput()

class BulkImportTestCase(TestCase):
    def test_complete(self):
        task = TestBulkImport('none')
        eq_(task.complete(), False)
        task = TestBulkImport('session-1')
        eq_(task.complete(), False)
        task = TestBulkImport('session-2')
        eq_(task.complete(), False)
        task = TestBulkImport('session-3')
        eq_(task.complete(), False)
        task = TestBulkImport('session-4')
        eq_(task.complete(), False)
        task = TestBulkImport('session-5')
        eq_(task.complete(), False)
        task = TestBulkImport('session-6')
        eq_(task.complete(), True)

    def test_local_path(self):
        class TestInput(luigi.ExternalTask):
            def output(self):
                return luigi.LocalTarget('test_input.tsv')
        class TestBulkImportFromLocalTarget(TestBulkImport):
            def requires(self):
                return TestInput()
        task = TestBulkImportFromLocalTarget('session-1')
        eq_(task.get_path(), os.path.realpath('test_input.tsv'))

    def test_s3_path(self):
        class TestInput(luigi.ExternalTask):
            def output(self):
                s3client = luigi.s3.S3Client(aws_access_key_id='test-key', aws_secret_access_key='test-secret')
                return luigi.s3.S3Target('s3://test-bucket/test_input.tsv', client=s3client)
        class TestBulkImportFromS3Target(TestBulkImport):
            def requires(self):
                return TestInput()
        task = TestBulkImportFromS3Target('session-1')
        eq_(task.get_path(), 's3://test-key:test-secret@/test-bucket/test_input.tsv')

    def test_run_without_commit(self):
        task = TestBulkImport('session-4', no_commit=True)
        for req in task.run():
            eq_(type(req), BulkImportPerform)

    def test_run(self):
        task = TestBulkImport('session-4')
        for req in task.run():
            eq_(type(req), BulkImportPerform)
