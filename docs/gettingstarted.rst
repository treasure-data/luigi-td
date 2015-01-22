===============
Getting Started
===============

Installation
============

Assuming you have already installed Python, you can install Luigi-td by ``pip``::

  $ pip install luigi-td

Configuration
=============

Set ``TD_API_KEY`` to your API key::

  $ export TD_API_KEY="1/1c410625..."

If you have a configuration file for Luigi (``/etc/luigi/client.cfg`` by default), you can add a ``[td]`` section in it::

  # configuration for Luigi
  [core]
  error-email: you@example.com

  # configuration for Luigi-td
  [td]
  api-key: 1/1c410625...

Writing Queries
===============

.. note::

  The script in this tutorial is available online at http://github.com/k24d/luigi-td/example/tutorial/tutorial.py

All queries are defined as subclasses of ``luigi_td.Query``::

  # tutorial.py

  import luigi
  import luigi_td

  class MyQuery(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'

      def query_body(self):
          return "SELECT count(1) cnt FROM www_access"

  if __name__ == '__main__':
      luigi.run()

You can submit your query as a normal Python script as follows::

  $ python tutorial.py MyQuery --local-scheduler
  DEBUG: Checking if MyQuery() is complete
  INFO: Scheduled MyQuery() (PENDING)
  INFO: Done scheduling tasks
  INFO: Running Worker with 1 processes
  DEBUG: Asking scheduler for work...
  DEBUG: Pending tasks: 1
  INFO: [pid 1234] Worker Worker(salt=123456789, host=...) running   MyQuery()
  INFO: MyQuery(): td.job_url: https://console.treasuredata.com/jobs/19958264
  INFO: MyQuery(): td.job_result: id=19958264 status=success elapsed=12.34
  INFO: [pid 1234] Worker Worker(salt=123456789, host=...) done      MyQuery()

Notice that there are INFO messages "td.job_url" and "td.job_result" in the log.  The result of your query is accessible either by opening the URL or by running ``td job:show`` as follows::

  $ td job:show 19958264
  JobID       : 19958264
  Status      : success
  ...
  Result      :
  +------+
  | cnt  |
  +------+
  | 5000 |
  +------+
  1 row in set

Getting the Result
==================

You will often retrieve query results within Python for further processing.  Fortunately, you can directly access to the result object by calling ``self.input()`` from another Luigi task::

  class MyQuery(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'

      def query_body(self):
          return "SELECT count(1) cnt FROM www_access"

  class MyQueryResult(luigi.Task):
      def requires(self):
          return MyQuery()

      def run(self):
          result = self.input()
          print '===================='
          print "Job ID     : ", result.job_id
          print "Status     : ", result.status
          print "Result     :"
          print "\t".join([c[0] for c in result.columns])
          print "----"
          for row in result.cursor():
              print "\t".join([str(c) for c in row])
          print '===================='

As you can see in this example, our second task ``MyQueryResult`` requires ``MyQuery``, which is automatically executed by Luigi's scheduler.  Thus, two tasks will be performed sequencially just by running ``MyQueryResult``::

  $ python tutorial.py MyQueryResult --local-scheduler
  ...
  INFO: [pid 1234] Worker Worker(salt=123456789, host=...) running   MyQuery()
  INFO: MyQuery(): td.job_url: https://console.treasuredata.com/jobs/19958264
  INFO: MyQuery(): td.job_result: id=19958264 status=success elapsed=12.34
  INFO: [pid 1234] Worker Worker(salt=123456789, host=...) done      MyQuery()
  ...
  INFO: [pid 1234] Worker Worker(salt=123456789, host=...) running   MyQueryResult()
  ====================
  Job ID     :  19958264
  Status     :  success
  Result     :
  cnt
  ----
  5000
  ====================
  INFO: [pid 1234] Worker Worker(salt=123456789, host=...) done      MyQueryResult()

``MyQueryResult`` is executed when the query defined in ``MyQuery`` is completed.  You can start downloading the result of your query as soon as ``MyQueryResult.run()`` is called.  Note that ``MyQueryResult`` won't be executed if ``MyQuery`` got interrupted, either by a user error (like a syntax error) or by a run-time error (like network failure).  See (TODO) for details.

Processing the Result
=====================

At the time our second query ``MyQueryResult`` is called, the query result has not been downloaded yet.  Unless it is adequately small, you should save the result into a local file before processing so you can avoid downloading it twice on processing errors.  That's why we need another step in our data workflow.

Suppose you had a bug in your data processing code.  If you didn't save the result, you would run the same query again and again until you fixed the bug.  Instead, you can use the ``dump()`` method, combined with ``luigi.LocalTarget``, to save query results to local files and let Luigi to skip running the same query::

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

Outputting the Result
=====================

You might want to send query results to cloud services without saving them to your local machine.  Luigi-td supports various output classes that transfer the results to remote servers, either directly from Treasure Data or indirectly through an intermediate server.

The primitive way of using result outputs is to set ``result_url`` in your query class::

  class MyOutputQuery(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'

      # the output goes to S3
      result_url = 's3://XXX/my_output_bucket'

      # alternatively, you can define a property
      # @property
      # def result_url(self):
      #     return 's3://XXX/my_output_bucket'

      def query_body(self):
          return "SELECT count(1) cnt FROM www_access"

See `Job Result Output <http://docs.treasuredata.com/categories/result>`_ for the list of available result output targets.

Luigi-td provides several pre-defined output classes for convenience.  For example, you can mixin ``luigi_td.S3ResultOutput`` to your query and get the result output to Amazon S3::

  class MyQueryResultOutput(luigi_td.Query, luigi_td.S3ResultOutput):
      type = 'presto'
      database = 'sample_datasets'

      aws_access_key_id = '...'
      aws_secret_access_key = '...'
      s3_path = 'my_output_bucket/luigi-td/file.csv'

      def query_body(self):
          return "SELECT count(1) cnt FROM www_access"

See "Result Output" for the list of available classes.

Templating Queries
==================

Luigi-td uses `Jinja2 <http://jinja.pocoo.org/>`_ as the default template engine.  Query strings are rendered as Jinja2 templates at run time::

  class MyTemplateQuery(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'

      # variables used in the template
      target_table = 'www_access'

      def query_body(self):
          # query string is rendered as a Jinja2 template
          return "SELECT count(1) cnt FROM {{ task.target_table }}"

As you can see in this example, a single variable ``task`` is available in templates.  The value of ``task`` is the instance of your query class.  As a result, the template ``{{ task.target_table }}`` will be replaced by ``www_access`` at run time.  You can set any variables or methods in your class and access to them through ``task``.

If you prefer defining variables explicitly, set a dictionaly ``variables``::

  class MyTemplateQueryWithVariables(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'
      variables = {
          'target_table': 'www_access'
      }

      def query_body(self):
          return "SELECT count(1) cnt FROM {{ target_table }}"

You might want to store your queries in separate files instead of writing them within the script.  Just set your query file name to ``query``.  This is the most common case how you will define your queries::

  class MyTemplateFileQuery(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'
      query = 'templates/query.sql'
      variables = {
          'target_table': 'www_access'
      }

::

  -- templates/query.sql
  SELECT count(1) cnt FROM {{ target_table }}

Passing Parameters
==================

Luigi supports passing parameters as command line options or constructor arguments.  Parameters can be used to build queries dynamically::

  class MyQueryWithParameters(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'
      query = 'templates/query_with_time_range.sql'

      # parameters
      target_date = luigi.DateParameter()

::

  -- templates/query_with_time_range.sql
  SELECT count(1) cnt
  FROM www_access
  WHERE td_time_range(time, td_time_add('{{ task.target_date }}', '-1d'), '{{ task.target_date }}')

You can pass parameters as command line options::

  $ python tutorial.py MyQueryWithParameters --local-scheduler --target-date 2015-01-01
  INFO: Scheduled MyQueryWithParameters(target_date=2015-01-01) (PENDING)
  ...

The query template is rendered using parameters, and you will get the following query as a result::

  -- templates/query_with_time_range.sql
  SELECT count(1) cnt
  FROM www_access
  WHERE td_time_range(time, td_time_add('2015-01-01', '-1d'), '2015-01-01')
