# luigi-tasks.py

import luigi
import luigi_td

class MyQuery(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'

    def query_body(self):
        return "SELECT method, count(1) cnt FROM www_access group by 1"

class MyQueryResult(luigi.Task):
    def requires(self):
        return MyQuery()

    def run(self):
        result = self.input()
        print '===================='
        print "Job ID     : ", result.job_id
        print "Status     : ", result.status
        print "Result size:", result.size
        print "Result     :"
        print "\t".join([c[0] for c in result.columns])
        print "----"
        for row in result.cursor():
            print "\t".join([str(c) for c in row])
        print '===================='

class MyQueryResultDump(luigi.Task):
    def requires(self):
        return MyQuery()

    def output(self):
        # this task creates "result.tsv"
        return luigi.LocalTarget('result.tsv')

    def run(self):
        result = self.input()
        # dump to "result.tsv"
        result.dump('result.tsv')

class MyQueryResultProcess(luigi.Task):
    def requires(self):
        return MyQueryResultDump()

    def run(self):
        result = self.input()
        # open the result now
        with result.open() as f:
            print f.read()

class MyQueryResultOutput(luigi_td.Query, luigi_td.S3ResultOutput):
    type = 'presto'
    database = 'sample_datasets'

    aws_access_key_id = '...'
    aws_secret_access_key = '...'
    s3_path = 'my_output_bucket/luigi-td/file.csv'

    def query_body(self):
        return "SELECT count(1) cnt FROM www_access"

class MyTemplateQuery(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'

    # variables used in the template
    target_table = 'www_access'

    def query_body(self):
        # query string is rendered as a Jinja2 template
        return "SELECT count(1) cnt FROM {{ task.target_table }}"

class MyTemplateQueryWithVariables(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'
    variables = {
        'target_table': 'www_access'
    }

    def query_body(self):
        return "SELECT count(1) cnt FROM {{ target_table }}"

class MyTemplateFileQuery(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'
    query = 'templates/query.sql'

class MyQueryWithParameters(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'
    query = 'templates/query_with_time_range.sql'

    # parameters
    date = luigi.DateParameter()

if __name__ == '__main__':
    luigi.run()
