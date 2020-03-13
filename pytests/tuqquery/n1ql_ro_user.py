from .tuq import QueryTests
from .tuq_sanity import QuerySanityTests


class ReadOnlyUserTests(QuerySanityTests, QueryTests):
    def setUp(self):
        super(ReadOnlyUserTests, self).setUp()
        self.username = self.input.param('username', 'RON1ql')
        self.password = self.input.param('password', 'RO$Pass')
        cli_cmd = "user-manage"
        output, error = self.shell.execute_couchbase_cli(cli_command=cli_cmd,
                                                         options=' --set --ro-username=%s'
                                                                 ' --ro-password=%s ' % (self.username, self.password),
                                                         cluster_host=self.master.ip, user=self.master.rest_username,
                                                         password=self.master.rest_password)
        self.log.info(output)
        self.log.error(error)
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)

    def suite_setUp(self):
        super(ReadOnlyUserTests, self).suite_setUp()

    def tearDown(self):
        super(ReadOnlyUserTests, self).tearDown()
        self._kill_all_processes_cbq()

    def suite_tearDown(self):
        super(ReadOnlyUserTests, self).suite_tearDown()

    def test_select(self):
        self._kill_all_processes_cbq()
        self._start_command_line_query(self.master, user=self.username, password=self.password)
        method_name = self.input.param('to_run', 'test_any')
        getattr(self, method_name)()

    def test_select_indx(self):
        self._kill_all_processes_cbq()
        self._start_command_line_query(self.master, user=self.username, password=self.password)
        for query_bucket in self.query_buckets:
            index_name = "my_index"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs) using %s " % (index_name, query_bucket, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
            finally:
                self.query = "DROP INDEX %s ON %s using %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()

    def test_readonly(self):
        self._kill_all_processes_cbq()
        self._start_command_line_query(self.master, user=self.username, password=self.password)
        for query_bucket in self.query_buckets:
            self.analytics = False
            self.query = 'INSERT into %s (key, value) VALUES ("%s", %s)' % (query_bucket, 'key1', 1)
            self.run_cbq_query()
