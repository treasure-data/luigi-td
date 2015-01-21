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

class MyQueryToFile(luigi_td.QueryToFile):
    type = 'presto'
    database = 'sample_datasets'
    output_file = 'result.tsv'

    def query_body(self):
        return "SELECT count(1) cnt FROM www_access"

if __name__ == '__main__':
    luigi.run()
