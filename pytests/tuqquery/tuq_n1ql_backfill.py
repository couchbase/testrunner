import threading

from membase.api.exception import CBQError
from membase.api.rest_client import RestHelper

from .tuq import QueryTests


class QueryN1QLBackfillTests(QueryTests):
    def setUp(self):
        super(QueryN1QLBackfillTests, self).setUp()
        self.log.info("==============  QueryN1QLBackfillTests setup has started ==============")
        self.directory_path = self.input.param("directory_path", "/opt/couchbase/var/lib/couchbase/tmp")
        self.create_directory = self.input.param("create_directory", True)
        self.tmp_size = self.input.param("tmp_size", 5120)
        self.nonint_size = self.input.param("nonint_size", False)
        self.out_of_range_size = self.input.param("out_of_range_size", False)
        self.set_backfill_directory = self.input.param("set_backfill_directory", True)
        self.change_directory = self.input.param("change_directory", False)
        self.reset_settings = self.input.param("reset_settings", False)
        self.curl_url = "http://%s:%s/settings/querySettings" % (self.master.ip, self.master.port)
        self.log.info("==============  QueryN1QLBackfillTests setup has completed ==============")
        self.log_config_info()
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)
        self.standard_bucket_name = 'standard_bucket0'

    def suite_setUp(self):
        super(QueryN1QLBackfillTests, self).suite_setUp()
        self.log.info("==============  QueryN1QLBackfillTests suite_setup has started ==============")
        self.log.info("==============  QueryN1QLBackfillTests suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryN1QLBackfillTests tearDown has started ==============")
        self.log.info("==============  QueryN1QLBackfillRests tearDown has completed ==============")
        super(QueryN1QLBackfillTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryN1QLBackfillTests suite_tearDown has started ==============")
        self.log.info("==============  QueryN1QLBackfillTests suite_tearDown has completed ==============")
        super(QueryN1QLBackfillTests, self).suite_tearDown()

    '''Test to see if anything goes into the backfill directory'''
    def test_backfill(self):
        if self.reset_settings:
            self.set_directory()
            self.set_tmpspace()
        try:
            if self.change_directory:
                self.set_directory()

            self.run_cbq_query(query="CREATE INDEX join_day on " + self.query_buckets[1] + "(join_day)")
            for query_bucket, bucket in zip(self.query_buckets, self.buckets):
                if bucket.name == self.standard_bucket_name:
                    self._wait_for_index_online(bucket, 'join_day')
            thread1 = threading.Thread(name='monitor_backfill', target=self.monitor_backfill)
            thread1.setDaemon(True)
            thread2 = threading.Thread(name='execute_query', target=self.execute_query)

            thread1.start()
            thread2.start()

            thread2.join()

            # Check to see if the monitoring thread is still alive after query completes, monitoring thread should
            # return once it sees backfill being used, if that has not happened by the time the query completes,
            # we can determine backfill was not used
            if thread1.isAlive():
                self.assertTrue(False, "The backfill thread never registered any files")
            else:
                self.log.info("The backfill directory was being used during the query")
                self.assertTrue(True)
        finally:
            self.run_cbq_query(query="DROP INDEX join_day ON " + self.query_buckets[1])

    '''Test to see what happens when the backfill data exceeds the max amount allowed by the tmpspacesize setting
        WIP'''
    def test_exceed_backfill(self):
        self.set_tmpspace()
        try:
            self.run_cbq_query(query="CREATE INDEX join_day on " + self.query_buckets[1] + "(join_day)")
            for query_bucket, bucket in zip(self.query_buckets, self.buckets):
                if bucket.name == self.standard_bucket_name:
                    self._wait_for_index_online(bucket, 'join_day')
            self.run_cbq_query(query="select * from " + self.query_buckets[0] + " d JOIN " + self.query_buckets[1] +
                                     " s on (d.join_day == s.join_day)")
        except CBQError as error:
            self.assertTrue("Thevaluemustbeinrangefrom-1toinfinity" in str(error),
                            "The error message is incorrect. It should have been %s" % error)
        finally:
            self.run_cbq_query(query="DROP INDEX join_day ON " + self.query_buckets[1])

    '''Test to set the directory to an absolute path (a directory that exists)
        Test to set directory to a path with spaces in it
        Test to set directory to a relative path (errors)
        Test to set directory to default (not set it)
        Test to set directory to an invalid path
        Test to set to a directory that does not exist'''
    def test_set_directory(self):

        # Set the directory or let the directory be the default
        if self.set_backfill_directory:
            expected_curl = self.set_directory()
            # Either the set succesfully happened, or we tried to set a directory that didn't exist
            if 'queryTmpSpaceDir' in expected_curl:
                self.log.info("Directory exists")
                if "space" in expected_curl['queryTmpSpaceDir']:
                    # For some reason the response strips out the whitespace, but inspecting the UI shows it is set
                    # correctly
                    self.assertEqual(expected_curl['queryTmpSpaceDir'], '/opt/couchbase/withspace')
                else:
                    self.assertEqual(expected_curl['queryTmpSpaceDir'], self.directory_path)
            else:
                self.log.info("Directory doesn't exist or is not valid")
                # The error message should be The value must be a valid directory
                self.assertEqual(expected_curl['errors']['queryTmpSpaceDir'], "Thevaluemustbeavaliddirectory")
        else:
            curl_output = self.shell.execute_command(f"{self.curl_path} -u Administrator:password {self.curl_url}")
            expected_curl = self.convert_list_to_json(curl_output[0])
            self.assertEqual(expected_curl['queryTmpSpaceDir'], "/opt/couchbase/var/lib/couchbase/tmp")

    '''Test setting of the tmpspacesize setting
        Test to set the size -1 (unlimited)
        Test to set the size to 5121 (>default)
        Test to set the size to 1 (between -1 and default)
        Test to set the size to 1.5 (nonint)
        Test to set the size to abc(nonnumeric)'''
    def test_tmpspace(self):
        expected_curl = self.set_tmpspace()

        # Check what kind of tmp_space value was passed in, this determines the type of check needed
        if self.nonint_size:
            # The error message should be The value must be an integer
            self.assertEqual(expected_curl['errors']['queryTmpSpaceSize'], "Thevaluemustbeaninteger")
        # This error message is currently not correct in the current implementation of tmpspacesize
        elif self.out_of_range_size:
            self.assertEqual(expected_curl['errors']['queryTmpSpaceSize'], "Thevaluemustbeinrangefrom-1to18446744073709551615(inclusive)")
        else:
            self.assertEqual(expected_curl['queryTmpSpaceSize'], self.tmp_size)

    def test_setting_propogation_rebalance_in(self):
        expected_curl = self.set_tmpspace()
        self.assertEqual(expected_curl['queryTmpSpaceSize'], self.tmp_size)
        expected_dir = self.set_directory()
        self.assertEqual(expected_dir['queryTmpSpaceDir'], self.directory_path)
        services_in = ["n1ql", "index", "data"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [self.servers[self.nodes_init]], [],
                                                 services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        curl_url = "http://%s:%s/settings/querySettings" % (self.servers[self.nodes_init].ip,
                                                            self.servers[self.nodes_init].port)
        max_try = 15
        for i in range(max_try+1):
            self.sleep(3)
            curl_output = self.shell.execute_command("%s -u Administrator:password %s"
                                                     % (self.curl_path, curl_url))
            try:
                expected_curl = self.convert_list_to_json(curl_output[0])
            except Exception as e:
                self.log.error(str(e))
            try:
                self.assertEqual(expected_curl['queryTmpSpaceSize'], self.tmp_size)
                self.assertEqual(expected_curl['queryTmpSpaceDir'], self.directory_path)
                break
            except Exception as e:
                if i == max_try:
                    self.fail(f'Assertion error: {str(e)}')
                else:
                    self.log.error(f"[{i+1}/9] Got TmpSpaceSize: {expected_curl['queryTmpSpaceSize']} and TmpSpaceDir: {expected_curl['queryTmpSpaceDir']}. Retrying ...")

    def test_setting_propogation_swap_rebalance(self):
        expected_curl = self.set_tmpspace()
        self.assertEqual(expected_curl['queryTmpSpaceSize'], self.tmp_size)
        expected_dir = self.set_directory()
        self.assertEqual(expected_dir['queryTmpSpaceDir'], self.directory_path)
        nodes_out_list = self.servers[1]
        to_add_nodes = [self.servers[self.nodes_init+1]]
        to_remove_nodes = [nodes_out_list]
        services_in = ["index", "n1ql", "data"]
        # do a swap rebalance
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add_nodes, [], services=services_in)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init + 1], [], to_remove_nodes)
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(5)
        curl_url = "http://%s:%s/settings/querySettings" % (self.servers[self.nodes_init+1].ip, self.servers[self.nodes_init+1].port)
        curl_output = self.shell.execute_command("%s -u Administrator:password %s"
                                                 % (self.curl_path, curl_url))
        expected_curl = self.convert_list_to_json(curl_output[0])
        self.assertEqual(expected_curl['queryTmpSpaceSize'], self.tmp_size)
        self.assertEqual(expected_curl['queryTmpSpaceDir'], self.directory_path)



    '''Helper method to send a curl request to couchbase server to set directory path'''
    def set_directory(self):
        # Try to create directory if it doesn't exist because backfill directories have to be created manually
        if self.create_directory:
            self.shell.create_directory(self.directory_path)
            self.shell.execute_command("chmod 777 %s" % self.directory_path)

        curl_output = self.shell.execute_command("%s -u Administrator:password -X POST -d 'queryTmpSpaceDir=%s' %s"
                                                 % (self.curl_path, self.directory_path, self.curl_url))
        expected_curl = self.convert_list_to_json(curl_output[0])
        self.log.info(expected_curl)
        return expected_curl

    '''Helper method to set the tmpspacesize setting'''
    def set_tmpspace(self):
        curl_output = self.shell.execute_command("%s -u Administrator:password -X POST -d 'queryTmpSpaceSize=%s' %s"
                                                 % (self.curl_path,  self.tmp_size, self.curl_url))
        expected_curl = self.convert_list_to_json(curl_output[0])
        self.log.info(expected_curl)
        return expected_curl

    '''Helper method that monitors the backfill directory to see when files get added to the directory'''
    def monitor_backfill(self):
        sftp = self.shell._ssh_client.open_sftp()
        no_backfill = True
        while no_backfill:
            if sftp.listdir(self.directory_path):
                no_backfill = False
        self.log.info("backfill is being used")
        return

    def execute_query(self):
        actual_results = self.run_cbq_query(query="select * from " + self.query_buckets[0] + " d JOIN " + self.query_buckets[1] + " s on "
                                                  "(d.join_day == s.join_day)")
        return actual_results
