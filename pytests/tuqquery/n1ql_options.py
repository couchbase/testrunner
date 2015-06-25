import math

from tuqquery.tuq import QueryTests


class OptionsTests(QueryTests):
    def setUp(self):
        super(OptionsTests, self).setUp()
        self.create_primary_index_for_3_0_and_greater()

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
        self.create_primary_index_for_3_0_and_greater()
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
        self.create_primary_index_for_3_0_and_greater()
        for bucket in self.buckets:
            self.query = "SELECT count(name) FROM %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'], 'There are no results for namespace')

    def test_signature(self):
        self.shell.execute_command("killall cbq-engine")
        self._start_command_line_query(self.master, options='-signature=false')
        self.create_primary_index_for_3_0_and_greater()
        for bucket in self.buckets:
            self.query = "SELECT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE CASE WHEN" % (bucket.name) +\
            " join_mo < 3 OR join_mo > 11 THEN 'winter' ELSE 'other' END LIKE 'win%'"
            actual_result = self.run_cbq_query()
            self.assertFalse('signature' in actual_result, 'signature are shown!')
    
    def test_timeout(self):
        self.shell.execute_command("killall cbq-engine")
        self._start_command_line_query(self.master, options='-timeout=1ms')
        self.create_primary_index_for_3_0_and_greater()
        for bucket in self.buckets:
            self.query = "SELECT count(name) FROM %s" % (bucket.name)
            try:
                actual_result = self.run_cbq_query()
                print actual_result
            except Exception, ex:
                self.assertTrue(str(ex).find('timeout') != -1, 'Server timeout did not work')
                self.log.info('Timeout is on')
            else:
                self.assertTrue(actual_result['status'] == 'stopped', 'Server timeout did not work')
    
    def test_http(self):
        self.shell.execute_command("killall cbq-engine")
        self.n1ql_port = 8094
        self._start_command_line_query(self.master, options='-http=:%s' % self.n1ql_port)
        self.create_primary_index_for_3_0_and_greater()
        for bucket in self.buckets:
            self.query = "SELECT count(name) FROM %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'], 'There are no results for port')

class OptionsRestTests(QueryTests):
    def setUp(self):
        super(OptionsRestTests, self).setUp()
        self.create_primary_index_for_3_0_and_greater()

    def suite_setUp(self):
        super(OptionsRestTests, self).suite_setUp()

    def tearDown(self):
        super(OptionsRestTests, self).tearDown()
        if hasattr(self, 'shell'):
           o = self.shell.execute_command("ps -aef| grep cbq-engine")
           if len(o):
               for cbq_engine in o[0]:
                   if cbq_engine.find('grep') == -1:
                       pid = [item for item in cbq_engine.split(' ') if item][1]
                       self.shell.execute_command("kill -9 %s" % pid)

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
        self.create_primary_index_for_3_0_and_greater()
        for bucket in self.buckets:
            self.query = "SELECT count(name) FROM %s" % (bucket.name)
            try:
                actual_result = self.run_cbq_query(query_params= {'timeout':'0.5s'})
                print actual_result
            except Exception, ex:
                self.assertTrue(str(ex).find('timeout') != -1, 'Server timeout did not work')
                self.log.info('Timeout is on')
            else:
                self.assertTrue(actual_result['status'] == 'stopped', 'Server timeout did not work')

    def test_named_var(self):
        self.create_primary_index_for_3_0_and_greater()
        for bucket in self.buckets:
            self.query = "SELECT count(test_rate) FROM %s where test_rate>$rate_min" % (bucket.name)
            actual_result = self.run_cbq_query(query_params= {'$rate_min':3})
            self.assertTrue(actual_result['results'], 'There are no results')

    def test_args(self):
        self.create_primary_index_for_3_0_and_greater()
        for bucket in self.buckets:
            self.query = "SELECT count(test_rate) FROM %s where test_rate>`$1`" % (bucket.name)
            actual_result = self.run_cbq_query(query_params= {'positional_params':[3]})
            self.assertTrue(actual_result['results'], 'There are no results')
            self.query = "SELECT count(test_rate) FROM %s where test_rate>`?`" % (bucket.name)
            actual_result = self.run_cbq_query(query_params= {'positional_params':[3]})
            self.assertTrue(actual_result['results'], 'There are no results')

    def test_named_var_arg(self):
        self.create_primary_index_for_3_0_and_greater()
        for bucket in self.buckets:
            self.query = 'SELECT count($1) FROM %s where test_rate>$rate_min' % (bucket.name)
            actual_result = self.run_cbq_query(query_params= {'$rate_min':3, 'args' :["test_rate"]})
            self.assertTrue(actual_result['results'], 'There are no results')
