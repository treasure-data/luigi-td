===============
Getting Started
===============

Installation
============

Assuming you have already installed Python, you can install luigi-td by ``pip``::

  $ pip install luigi-td

Configuration
=============

Set ``TD_API_KEY`` to your API key::

  $ export TD_API_KEY="1/1c410625..."

If you have a configuration file for Luigi (``/etc/luigi/client.cfg`` by default), you can add a ``[td]`` section in it::

  # configuration for luigi
  [core]
  error-email: luigi@example.com

  # configuration for luigi-td
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

At the time our second query ``MyQueryResult`` is called, the query result has not been downloaded yet.  Unless it is adequately small, you should save the result into a local file before processing so you can avoid downloading it again on errors.  That's why we need another step in our data workflow.

Suppose you had a bug in your data processing code.  If you didn't save the result, you would run the same query and download the result again and again until you successfully fixed the bug.  You shouldn't do that.  Instead, you can easily save the result locally and let Luigi to skip running the same query.

The ``dump()`` method, combined with ``luigi.LocalTarget``, can be used for saving query results to local files::

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

You might want to send the results of queries to some cloud services, instead of saving them to your local machine.  luigi-td supports various output classes that transfer the results to remote servers, either directly from Treasure Data or indirectly through an intermediate server.

The primitive way of using result outputs is to define ``result_url``::

  class MyOutputQuery(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'

      # the output does to S3
      result_url = 's3://XXX/my_output_bucket'

      # alternatively, you can define a property
      @property
      def result_url(self):
          return 's3://XXX/my_output_bucket'

      def query_body(self):
          return "SELECT count(1) cnt FROM www_access"

See `Job Result Output <http://docs.treasuredata.com/categories/result>`_ for the list of available result output options.

It is sometimes convenient to use pre-defined output classes.  For example, you can mixin ``luigi_td.S3ResultOutput`` to your query and get the result output to Amazon S3::

  class MyOutputQuery(luigi_td.Query, luigi_td.S3ResultOutput):
      type = 'presto'
      database = 'sample_datasets'

      aws_access_key_id = 'xxx'
      aws_secret_access_key = 'xxx'
      s3_bucket = 'my_output_bucket'
      s3_prefix = 'luigi-td/output/'

      def query_body(self):
          return "SELECT count(1) cnt FROM www_access"

See "Result Output" for the list of available classes.

Passing Parameters
==================

It is often useful to use paramters to build queries dynamically.  You can define parameters in the same way as regular Luigi tasks::

  class MyQueryWithParameters(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'

      # parameters
      foo = luigi.Parameter()

      def query_body(self):
          return "SELECT count(1) cnt FROM www_access WHERE host = '{foo}'".format(
              foo = self.foo
          )

Now you can pass parameters as command line arguments::

  $ python tutorial.py MyQueryWithParameters --local-scheduler --foo bar
  INFO: Scheduled MyQueryWithParameters(foo=bar) (PENDING)
  ...

Or you can build a task dynamically in your script::

  class AnotherTask(luigi.Task):
      def requires(self):
          return MyQueryWithParameters(foo='bar')

See "Parameters" for how to use parameters in Luigi.

Templating Queries
==================

luigi-td uses `Jinja2 <http://jinja.pocoo.org/>`_ as the default template engine.  Your quries will be rendered as Jinja2 templates at run time::

  class MyTemplateQuery(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'

      def query_body(self):
          return '''
              {# query string is a Jinja2 template #}
              SELECT count(1) cnt FROM {{ "www_access" }}
          '''

You may want to write queries in separate files from the script.  Just define the file name to ``query`` in this case::

  class MyTemplateQuery(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'
      query = 'templates/my_template_query.sql'

::

  -- templates/my_template_query.sql
  SELECT count(1) cnt FROM www_access

A single variable ``task`` is defined in the template by default.  The value of ``task`` is the instance of your query (= ``luigi_td.Query``).  Thus, this template::

  -- {{ task }}
  SELECT count(1) cnt
  FROM {{ task.database }}.www_access

will be converted as follows::

  -- MyTemplateQuery()
  SELECT count(1) cnt
  FROM sample_datasets.www_access

You can pass arbitrary parameters to the template by defining variables or methods in your query class and accessing them through ``task``.  Or if you prefer to defining variables explicitly, you can do that as follows::

  class MyTemplateQuery(luigi_td.Query):
      type = 'presto'
      database = 'sample_datasets'
      query = 'templates/my_template_query.sql'
      variables = {
          'table': 'www_access'
      }

::

  -- templates/my_template_query.sql
  SELECT count(1) cnt FROM {{ table }}
