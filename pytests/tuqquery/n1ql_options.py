import math

from tuqquery.tuq import QueryTests


class OptionsTests(QueryTests):
    def setUp(self):
        super(OptionsTests, self).setUp()

    def suite_setUp(self):
        super(OptionsTests, self).suite_setUp()

    def tearDown(self):
        super(OptionsTests, self).tearDown()

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
            self.query = 'INSERT into %s key "%s" VALUES "%s"' % (bucket.name, 'key1', 'value')
            try:
                actual_result = self.run_cbq_query()
            except Exception, ex:
                self.assertTrue(str(ex).find('request is read-only') != -1, 'Server is not readonly')
                self.log.info('Read only is on')
            else:
                self.fail('server is not read-only')

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
            except Exception, ex:
                self.assertTrue(str(ex).find('timeout') != -1, 'Server timeout did not work')
                self.log.info('Timeout is on')
            else:
                self.fail('Server timeout did not work')
    
    def test_http(self):
        self.shell.execute_command("killall cbq-engine")
        self.n1ql_port = 8094
        self._start_command_line_query(self.master, options='-http=:%s' % self.n1ql_port)
        self.create_primary_index_for_3_0_and_greater()
        for bucket in self.buckets:
            self.query = "SELECT count(name) FROM %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'], 'There are no results for port')
