# luigi-tasks.py

import luigi
import luigi_td

## Running Queries

class MyQuery(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'

    def query(self):
        return "SELECT count(1) cnt FROM www_access"

## Getting Results

class MyQueryRun(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'

    def query(self):
        return "SELECT count(1) cnt FROM www_access"

    def run(self):
        result = self.run_query(self.query())
        print '===================='
        print "Job ID     :", result.job_id
        print "Result size:", result.size
        print "Result     :"
        print "\t".join([c[0] for c in result.description])
        print "----"
        for row in result:
            print "\t".join([str(c) for c in row])
        print '===================='

class MyQuerySave(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'

    def query(self):
        return "SELECT count(1) cnt FROM www_access"

    def output(self):
        return luigi.LocalTarget('MyQuerySave.csv')

    def run(self):
        result = self.run_query(self.query())
        with self.output().open('w') as f:
            result.to_csv(f)

## Building Pipelines

class MyQueryStep1(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'

    def query(self):
        return "SELECT count(1) cnt FROM www_access"

    def output(self):
        # the query state is stored by ResultTarget
        return luigi_td.ResultTarget('MyQueryStep1.job')

class MyQueryStep2(luigi.Task):
    def requires(self):
        return MyQueryStep1()

    def output(self):
        return luigi.LocalTarget('MyQueryStep2.csv')

    def run(self):
        target = self.input()
        # retrieve the result and save it as a local CSV file
        with self.output().open('w') as f:
            target.result.to_csv(f)

class MyQueryStep3(luigi.Task):
    def requires(self):
        return MyQueryStep2()

    def output(self):
        return luigi.LocalTarget('MyQueryStep3.csv')

    def run(self):
        with self.input().open() as f:
            # process the result here
            print f.read()
        with self.output().open('w') as f:
            # crate the final output
            f.write('done')

## Templating Queries

class MyQueryFromTemplate(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'
    source = 'templates/query_with_status_code.sql'

    # variables used in the template
    status_code = 200

class MyQueryWithVariables(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'
    source = 'templates/query_with_variables.sql'

    # define variables
    variables = {
        'status_code': 200,
    }

    # or use property for dynamic variables
    # @property
    # def variables(self):
    #     return {
    #         'status_code': 200,
    #     }

## Passing Parameters

class MyQueryWithParameters(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'
    source = 'templates/query_with_time_range.sql'

    # parameters
    year = luigi.IntParameter()

    def output(self):
        # create a unique name for this output using parameters
        return luigi_td.ResultTarget('MyQueryWithParameters-{0}.job'.format(self.year))

class MyQueryAggregator(luigi.Task):
    def requires(self):
        # create a list of tasks with different parameters
        return [
            MyQueryWithParameters(2010),
            MyQueryWithParameters(2011),
            MyQueryWithParameters(2012),
            MyQueryWithParameters(2013),
        ]

    def output(self):
        return luigi.LocalTarget('MyQueryAggretator.txt')

    def run(self):
        with self.output().open('w') as f:
            # repeat for each ResultTarget
            for target in self.input():
                # output results into a single file
                for row in target.result:
                    f.write(str(row) + "\n")

if __name__ == '__main__':
    luigi.run()
