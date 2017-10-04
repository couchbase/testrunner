import json
import math
from remote.remote_util import RemoteMachineShellConnection

from tuqquery.tuq import QueryTests

class OptionsTests(QueryTests):
    def setUp(self):
        super(OptionsTests, self).setUp()

    def suite_setUp(self):
        super(OptionsTests, self).suite_setUp()

    def tearDown(self):
        super(OptionsTests, self).tearDown()
        if hasattr(self, 'shell'):
           o = self.shell.execute_command("ps -aef| grep cbq-engine")
           if len(o):
               for cbq_engine in o[0]:
                   if cbq_engine.find('grep') == -1:
                       pid = [item for item in cbq_engine.split(' ') if item][1]
                       self.shell.execute_command("kill -9 %s" % pid)

    def suite_tearDown(self):
        super(OptionsTests, self).suite_tearDown()

    def test_metrics(self):
        self.shell.execute_command("killall cbq-engine")
        self._start_command_line_query(self.master, options='-metrics=false')
        for bucket in self.buckets:
            self.query = "SELECT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE CASE WHEN" % (bucket.name) +\
            " join_mo < 3 OR join_mo > 11 THEN 'winter' ELSE 'other' END LIKE 'win%'"
            actual_result = self.run_cbq_query()
            self.assertFalse('metrics' in actual_result, 'Metrics are shown!')

    def test_readonly(self):
        self.shell.execute_command("killall cbq-engine")
        self._start_command_line_query(self.master, options='-readonly=true')
        for bucket in self.buckets:
            self.query = 'INSERT into %s (key, value) VALUES ("%s", %s)' % (bucket.name, 'key1', 'value')
            actual_result = self.run_cbq_query()

    def test_namespace(self):
        self.shell.execute_command("killall cbq-engine")
        self._start_command_line_query(self.master, options='-namespace=default')
        for bucket in self.buckets:
            self.query = "SELECT count(name) FROM %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'], 'There are no results for namespace')

    def test_signature(self):
        self.shell.execute_command("killall cbq-engine")
        self._start_command_line_query(self.master, options='-signature=false')
        for bucket in self.buckets:
            self.query = "SELECT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE CASE WHEN" % (bucket.name) +\
            " join_mo < 3 OR join_mo > 11 THEN 'winter' ELSE 'other' END LIKE 'win%'"
            actual_result = self.run_cbq_query()
            self.assertFalse('signature' in actual_result, 'signature are shown!')
    
    def test_timeout(self):
        self.shell.execute_command("killall cbq-engine")
        self._start_command_line_query(self.master, options='-timeout=1ms')
        for bucket in self.buckets:
            self.query = "SELECT count(name) FROM %s" % (bucket.name)
            try:
                actual_result = self.run_cbq_query()
            except Exception, ex:
                self.assertTrue(str(ex).find('timeout') != -1, 'Server timeout did not work')
                self.log.info('Timeout is on')
            else:
                self.assertTrue(actual_result['status'] == 'stopped', 'Server timeout did not work')
    
    def test_http(self):
        self.shell.execute_command("killall cbq-engine")
        self.n1ql_port = 8094
        self._start_command_line_query(self.master, options='-http=:%s' % self.n1ql_port)
        for bucket in self.buckets:
            self.query = "SELECT count(name) FROM %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'], 'There are no results for port')

class OptionsRestTests(QueryTests):
    def setUp(self):
        super(OptionsRestTests, self).setUp()

    def suite_setUp(self):
        super(OptionsRestTests, self).suite_setUp()

    def tearDown(self):
        super(OptionsRestTests, self).tearDown()
        # if hasattr(self, 'shell'):
        #    o = self.shell.execute_command("ps -aef| grep cbq-engine")
        #    if len(o):
        #        for cbq_engine in o[0]:
        #            if cbq_engine.find('grep') == -1:
        #                pid = [item for item in cbq_engine.split(' ') if item][1]
        #                self.shell.execute_command("kill -9 %s" % pid)

    def suite_tearDown(self):
        super(OptionsRestTests, self).suite_tearDown()

    def test_readonly(self):
       for bucket in self.buckets:
            self.query = 'INSERT into %s (key, value) VALUES ("%s", "%s")' % (bucket.name, 'key3', 'value3')
            try:
                actual_result = self.run_cbq_query(query_params= {'readonly':True})
            except:
                pass
            else:
                self.fail('Error for ro request expected')

    def test_signature(self):
        for bucket in self.buckets:
            self.query = "SELECT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE CASE WHEN" % (bucket.name) +\
            " join_mo < 3 OR join_mo > 11 THEN 'winter' ELSE 'other' END LIKE 'win%'"
            actual_result = self.run_cbq_query(query_params= {'signature':False})
            self.assertFalse('signature' in actual_result, 'signature are shown!')

    def test_timeout(self):
        for bucket in self.buckets:
            self.query = "SELECT count(name) FROM %s" % (bucket.name)
            actual_result = self.run_cbq_query(query_params={'timeout':'0.1s'})
            self.assertEqual(actual_result['status'], 'timeout', 'Request was not timed out')

    def test_named_var(self):
        for bucket in self.buckets:
            self.query = "SELECT count(test_rate) FROM %s where test_rate>$rate" % (bucket.name)
            actual_result = self.run_cbq_query(query_params= {'$rate':3})
            self.assertTrue(actual_result['results'], 'There are no results')

    def test_args(self):
        for bucket in self.buckets:
            self.query = "SELECT count(test_rate) FROM %s where test_rate>$1" % (bucket.name)
            actual_result = self.run_cbq_query(query_params= {'args':[3]})
            self.assertTrue(actual_result['results'], 'There are no results')
            self.query = "SELECT count(test_rate) FROM %s where test_rate>?" % (bucket.name)
            actual_result = self.run_cbq_query(query_params= {'args':[3]})
            self.assertTrue(actual_result['results'], 'There are no results')

    def test_named_var_arg(self):
        for bucket in self.buckets:
            self.query = 'SELECT count($1) FROM %s where test_rate>$rate' % (bucket.name)
            actual_result = self.run_cbq_query(query_params= {'$rate':3, 'args' :'["test_rate"]'})
            self.assertTrue(actual_result['results'], 'There are no results')

    # This test is for verifying the optimized adhoc queries.
    # MB-24871 has the test case file uploaded in it.
    def test_optimized_adhoc_queries(self):
        for bucket in self.buckets:
            self.query = "CREATE INDEX `def_name` ON %s(`name`) WHERE  job_title = 'Engineer'"% (bucket.name)
            self.run_cbq_query()
            statement = 'EXPLAIN SELECT * FROM %s where job_title = "Engineer" and name=$name&$name="employee-4"'% (bucket.name)
            output = self.curl_helper(statement)

            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == 'def_name')
            # compare results
            statement = 'SELECT * FROM %s where job_title = "Engineer" and name=$name&$name="employee-4"'% (bucket.name)
            actual_result = self.curl_helper(statement)

            statement = 'SELECT * FROM %s use index(`#primary`)  where job_title = "Engineer" and name=$name&$name="employee-4"'% (bucket.name)
            expected_result = self.curl_helper(statement)

            self.assertTrue(sorted(actual_result['results'])== sorted(expected_result['results']))

            statement = 'EXPLAIN SELECT * FROM %s where job_title=$job_title and name=$name&$job_title="Engineer"&$name="employee-4"'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == 'def_name')
            # compare results
            statement = 'SELECT * FROM %s where job_title=$job_title and name=$name&$job_title="Engineer"&$name="employee-4"'% (bucket.name)
            actual_result = self.curl_helper(statement)
            statement = 'SELECT * FROM %s use index(`#primary`) where job_title=$job_title and name=$name&$job_title="Engineer"&$name="employee-4"'% (bucket.name)
            expected_result = self.curl_helper(statement)
            self.assertTrue(sorted(actual_result['results'])== sorted(expected_result['results']))

            statement = 'EXPLAIN SELECT * FROM %s where job_title=$1 and name=$2&args=["Engineer","id@mail.com"]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == 'def_name')
            # compare results
            statement = 'SELECT * FROM %s where job_title=$1 and name=$2&args=["Engineer","employee-4"]'% (bucket.name)
            actual_result = self.curl_helper(statement)
            statement = 'SELECT * FROM %s use index(`#primary`) where job_title=$1 and name=$2&args=["Engineer","employee-4"]'% (bucket.name)
            expected_result = self.curl_helper(statement)
            self.assertTrue(sorted(actual_result['results'])== sorted(expected_result['results']))

            #in clause with incorrect datatype for first argument
            statement = 'EXPLAIN SELECT * FROM %s where job_title=$job_title and name IN $name&$job_title=3&$name= ["id@mail.com", "employee-4"]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] != 'def_name')
            statement = 'SELECT * FROM %s where job_title=$job_title and name IN $name&$job_title=3&$name= ["id@mail.com", "employee-4"]'% (bucket.name)
            actual_result = self.curl_helper(statement)
            statement = 'SELECT * FROM %s use index(`#primary`) where job_title=$job_title and name IN $name&$job_title=3&$name= ["id@mail.com", "employee-4"]'% (bucket.name)
            expected_result = self.curl_helper(statement)
            self.assertTrue(sorted(actual_result['results'])== sorted(expected_result['results']))

            #in clause with correct datatype for first argument
            statement = 'EXPLAIN SELECT * FROM %s where job_title=$job_title and name IN $name&$job_title="Engineer"&$name= ["id@mail.com", "employee-4"]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == 'def_name')
            statement = 'SELECT * FROM %s where job_title=$job_title and name IN $name&$job_title="Engineer"&$name= ["id@mail.com", "employee-4"]'% (bucket.name)
            actual_result = self.curl_helper(statement)
            statement = 'SELECT * FROM %s use index(`#primary`) where job_title=$job_title and name IN $name&$job_title="Engineer"&$name= ["id@mail.com", "employee-4"]'% (bucket.name)
            expected_result = self.curl_helper(statement)
            self.assertTrue(sorted(actual_result['results'])== sorted(expected_result['results']))

            #in clause with missing first argument
            statement = 'EXPLAIN SELECT * FROM %s where job_title=$job_title and name IN $name&$job_title is missing&$name= ["id@mail.com", "employee-4"]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['errors'][0]['msg'] == 'No named argument $job_title is missing value')

            #in clause with null first argument
            statement = 'EXPLAIN SELECT * FROM %s where job_title=$job_title and name IN $name&$job_title=null&$name= ["id@mail.com", "employee-4"]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == '#primary')
            statement = 'SELECT * FROM %s where job_title=$job_title and name IN $name&$job_title=null&$name= ["id@mail.com", "employee-4"]'% (bucket.name)
            actual_result = self.curl_helper(statement)
            statement = 'SELECT * FROM %s use index(`#primary`) where job_title=$job_title and name IN $name&$job_title=null&$name= ["id@mail.com", "employee-4"]'% (bucket.name)
            expected_result = self.curl_helper(statement)
            self.assertTrue(sorted(actual_result['results'])== sorted(expected_result['results']))

            statement = 'EXPLAIN SELECT * FROM %s where job_title=$1 and name IN $2&args=["Engineer", ["id@mail.com", "employee-4"]]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == 'def_name')

            statement = 'SELECT * FROM %s where job_title=$1 and name IN $2&args=["Engineer", ["id@mail.com", "employee-4"]]'% (bucket.name)
            actual_result = self.curl_helper(statement)
            statement = 'SELECT * FROM %s use index(`#primary`) where job_title=$1 and name IN $2&args=["Engineer", ["id@mail.com", "employee-4"]]'% (bucket.name)
            expected_result = self.curl_helper(statement)
            self.assertTrue(sorted(actual_result['results'])== sorted(expected_result['results']))
            # args is empty
            statement = 'explain SELECT * FROM %s where job_title=$1 and name IN $2&args=["", ["id@mail.com", "employee-4"]]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == '#primary')

            # args is null
            statement = 'explain SELECT * FROM %s where job_title=$1 and name IN $2&args=[null, ["id@mail.com", "employee-4"]]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == '#primary')

            # prepare statement
            statement = "PREPARE p1 FROM SELECT * FROM %s where job_title=$type and name=$name"% (bucket.name)
            output = self.curl_helper(statement)
            statement = "p1"
            output = self.prepare_helper(statement)
            self.assertTrue(output['metrics']['resultCount'] == 144)

            statement = "PREPARE p2 FROM SELECT * FROM %s where job_title=$1 and name=$2"% (bucket.name)
            output = self.curl_helper(statement)
            statement = 'p2'
            output = self.prepare_helper2(statement)
            self.assertTrue(output['metrics']['resultCount'] == 144)

            statement = "PREPARE p3 FROM SELECT * FROM %s where job_title=$type and name=$name"% (bucket.name)
            output = self.curl_helper(statement)
            statement = 'p3'
            output = self.prepare_helper(statement)
            self.assertTrue(output['metrics']['resultCount'] == 144)

            statement = 'PREPARE p4 FROM SELECT * FROM %s where job_title=$type and name=$name&$type="Engineer"&$name="id@mail.com"'% (bucket.name)
            output = self.curl_helper(statement)
            statement = 'p3'
            output = self.prepare_helper(statement)
            self.assertTrue(output['metrics']['resultCount'] == 144)

            #update
            statement = 'EXPLAIN UPDATE %s set id = "1" where job_title=$type and name=$name&$type="Engineer"&$name=""'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == 'def_name')
            statement = 'UPDATE %s set id = "1" where job_title=$type and name=$name&$type="Engineer"&$name=""'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue('status' in str(output))

            statement = 'EXPLAIN UPDATE %s set id = "1" where job_title=$1 and name=$2&args=["Engineer","employee-4"]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == 'def_name')

            statement = 'UPDATE %s set id = "1" where job_title=$1 and name=$2&args=["Engineer","employee-4"]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['metrics']['mutationCount'] == 144)

            #delete
            statement = 'EXPLAIN DELETE FROM %s where job_title=$type and name=$name&$type="Engineer"&$name="employee-4"'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == 'def_name')

            statement='EXPLAIN DELETE FROM %s  where job_title=$1 and name=$2&args=["Engineer","employee-4"]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == 'def_name')

            statement = 'DELETE FROM %s where job_title=$type and name=$name&$type="Engineer"&$name="employee-4"'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['metrics']['mutationCount'] == 144)

            statement='DELETE FROM %s  where job_title=$1 and name=$2&args=["Engineer","employee-4"]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue("success" in str(output))

            #subqueries
            statement = 'EXPLAIN SELECT * FROM %s t1 WHERE job_title = "Engineer" and name IN (SELECT name FROM %s where job_title=$type and name=$name)&$type="Support"&$name="employee-4"'% (bucket.name,bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == 'def_name')
            self.query = "CREATE INDEX `def_name2` ON %s(`name`) WHERE  job_title = 'Support'"% (bucket.name)
            self.run_cbq_query()
            statement = 'SELECT * FROM %s t1 WHERE job_title = "Engineer" and name IN (SELECT name FROM %s where job_title=$type and name=$name)&$type="Support"&$name="employee-4"'% (bucket.name,bucket.name)
            actual_result = self.curl_helper(statement)
            statement = 'SELECT * FROM %s t1 use index(`#primary`) WHERE job_title = "Engineer" and name IN (SELECT name FROM %s where job_title=$type and name=$name)&$type="Support"&$name="employee-4"'% (bucket.name,bucket.name)
            expected_result = self.curl_helper(statement)
            self.assertTrue(sorted(actual_result['results'])==sorted(expected_result['results']))

            statement='EXPLAIN SELECT * FROM %s t1 WHERE job_title = "Engineer" and name in (SELECT name FROM %s where _type=$1 and name=$2)&args=["Support","employee-4"]'% (bucket.name,bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['index'] == 'def_name')

            statement='SELECT * FROM %s t1 WHERE job_title = "Engineer" and name in (SELECT name FROM %s where _type=$1 and name=$2)&args=["Support","employee-4"]'% (bucket.name,bucket.name)
            actual_result = self.curl_helper(statement)
            statement='SELECT * FROM %s t1 use index(`#primary`) WHERE job_title = "Engineer" and name in (SELECT name FROM %s where _type=$1 and name=$2)&args=["Support","employee-4"]'% (bucket.name,bucket.name)
            expected_result = self.curl_helper(statement)
            self.assertTrue(sorted(actual_result['results'])==sorted(expected_result['results']))

            statement = 'EXPLAIN SELECT * FROM (SELECT name FROM %s where job_title=$type and name=$name) as name&$type="Support"&$name="employee-4"'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['~children'][0]['index'] == 'def_name2')
            statement = 'SELECT * FROM (SELECT name FROM %s where job_title=$type and name=$name) as name&$type="Support"&$name="employee-4"'% (bucket.name)
            actual_result = self.curl_helper(statement)
            statement = 'SELECT * FROM (SELECT name FROM %s use index(`#primary`) where job_title=$type and name=$name) as name&$type="Support"&$name="employee-4"'% (bucket.name)
            expected_result = self.curl_helper(statement)
            self.assertTrue(sorted(actual_result['results'])==sorted(expected_result['results']))

            statement = 'EXPLAIN SELECT * FROM  (SELECT name FROM %s where job_title=$1 and name=$2) as name&args=["Support","employee-4"]'% (bucket.name)
            output = self.curl_helper(statement)
            self.assertTrue(output['results'][0]['plan']['~children'][0]['~children'][0]['index'] == 'def_name2')
            statement = 'SELECT * FROM  (SELECT name FROM %s where job_title=$1 and name=$2) as name&args=["Support","employee-4"]'% (bucket.name)
            actual_result = self.curl_helper(statement)
            self.assertTrue(actual_result['metrics']['resultCount'] == 144)




