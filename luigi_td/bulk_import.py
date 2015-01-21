class BulkImportTarget(luigi.Target):
    def __init__(self, session_name):
        self.session_name = session_name

    def exists(self):
        td = _config.get_client()
        for session in td.bulk_imports():
            if session.name == session_name:
                return session.status == 'commited'
        return False

class BulkImportCreate(luigi.Task):
    session_name = luigi.Parameter()
    database_name = luigi.Parameter()
    table_name = luigi.Parameter()

    def complete(self):
        td = _config.get_client()
        return session_name in td.bulk_imports()

    def run(self):
        td = _config.get_client()
        td.create_bulk_import(self.session_name, self.database_name, self.table_name)

class BulkImportUpload(luigi.Task):
    session_name = luigi.Parameter()
    database_name = luigi.Parameter()
    table_name = luigi.Parameter()

    def requires(self):
        return BulkImportCreate(self.session_name, self.database_name, self.table_name)

    def complete(self):
        td = _config.get_client()
        for session in td.bulk_imports():
            if session.name == self.session_name:
                return session.upload_frozen()
        return False

    def run(self):
        td = _config.get_client()
        print 'uploading...'
        td.freeze_bulk_import(self.session_name)

class BulkImportPerform(luigi.Task):
    session_name = luigi.Parameter()
    database_name = luigi.Parameter()
    table_name = luigi.Parameter()

    def requires(self):
        return BulkImportUpload(self.session_name, self.database_name, self.table_name)

    def complete(self):
        td = _config.get_client()
        for session in td.bulk_imports():
            if session.name == self.session_name:
                return session.status == 'ready'
        return False

    def run(self):
        td = _config.get_client()
        job = td.perform_bulk_import(self.session_name)
        print 'performing...'
        while not job.finished():
            print job
            time.sleep(2)

class BulkImport(luigi.Task):
    session_name = luigi.Parameter()
    database_name = luigi.Parameter()
    table_name = luigi.Parameter()
    schema = {}

    def requires(self):
        return [
            CreateTable(self.database_name, self.table_name),
            BulkImportPerform(self.session_name, self.database_name, self.table_name),
        ]

    def output(self):
        return BulkImportTarget(self.session_name, self.database_name, self.table_name)

    def run(self):
        td = _config.get_client()
        td.commit_bulk_import(self.session_name)
