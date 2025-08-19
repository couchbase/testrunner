from .tuq import QueryTests
import time
from membase.api.exception import CBQError
from datetime import datetime, timedelta
import threading

class QueryAWRTests(QueryTests):

    def setUp(self):
        super(QueryAWRTests, self).setUp()
        self.log.info("==============  QueryAWRTests setup has started ==============")
        system_stats = self.rest.fetch_system_stats()
        self.bucket_name = "travel-sample"
        self.rest.load_sample("travel-sample")
        init_time = time.time()
        while True:
            next_time = time.time()
            query_response = self.run_cbq_query("SELECT COUNT(*) FROM `" + self.bucket_name + "`")
            self.log.info(f"{self.bucket_name}+ count: {query_response['results'][0]['$1']}")
            if query_response['results'][0]['$1'] == 31591:
                break
            if next_time - init_time > 600:
                break
            time.sleep(2)

        self.wait_for_all_indexes_online(build_deferred=True)
        # Create collection for workload
        self.run_cbq_query("CREATE COLLECTION `travel-sample`._default.workload if not exists")
        time.sleep(1)
        
        self.log.info("==============  QueryAWRTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryAWRTests, self).suite_setUp()
        self.log.info("==============  QueryAWRTests suite_setup has started ==============")
        self.log.info("==============  QueryAWRTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryAWRTests tearDown has started ==============")
        travel_sample = self.get_bucket_from_name("travel-sample")
        if travel_sample:
            self.delete_bucket(travel_sample)
        self.log.info("==============  QueryAWRTests tearDown has completed ==============")
        super(QueryAWRTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryAWRTests suite_tearDown has started ==============")
        self.log.info("==============  QueryAWRTests suite_tearDown has completed ==============")
        super(QueryAWRTests, self).suite_tearDown()
    
    def test_awr_basic(self):
        update_awr = self.run_cbq_query("UPDATE system:awr SET location = 'travel-sample._default.workload', threshold = '1s', interval = '3m', enabled = true")
        self.assertEqual(update_awr['status'], 'success')
        time.sleep(5)
        # Create threads for first query (15000 limit)
        threads1 = []
        for i in range(5):
            t = threading.Thread(target=lambda: self.run_cbq_query(query="SELECT * FROM `travel-sample` LIMIT 15000", server=self.master))
            threads1.append(t)
            t.start()

        # Create threads for second query (25000 limit) 
        threads2 = []
        for i in range(3):
            t = threading.Thread(target=lambda: self.run_cbq_query(query="SELECT * FROM `travel-sample` LIMIT 25000",server=self.servers[1]))
            threads2.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads1:
            t.join()
        for t in threads2:
            t.join()

        time.sleep(360)

        # Get current timestamp in UTC
        current_time = time.strftime("%Y-%m-%dT%H:%M", time.gmtime())

        # Check workload collection for first query
        check_workload = self.run_cbq_query("SELECT meta().id,* FROM `travel-sample`._default.workload WHERE txt = 'SELECT * FROM `travel-sample` LIMIT 15000'")
        self.assertTrue(len(check_workload['results']) > 0, "Expected non-zero results for first query workload")
        self.assertEqual(check_workload['results'][0]['workload']['cnt'], 5, "Expected count of 5 for first query workload, please check the results {check_workload}")

        # Check workload collection for second query
        check_workload = self.run_cbq_query("SELECT meta().id,* FROM `travel-sample`._default.workload WHERE txt = 'SELECT * FROM `travel-sample` LIMIT 25000'")
        self.assertTrue(len(check_workload['results']) > 0, "Expected non-zero results for first query workload")
        self.assertEqual(check_workload['results'][0]['workload']['cnt'], 3, "Expected count of 3 for first query workload, please check the results {check_workload}")
        

    '''Set location to a keyspace that does not exist and enable awr, should be in quiescent mode'''
    def test_awr_quiescent(self):
        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET location = 'testbucket', threshold = '1s', interval = '1m', enabled = true")
            self.assertEqual(update_awr['status'], 'success')
            check_awr = self.run_cbq_query("SELECT awr FROM system:vitals")
            self.assertEqual(check_awr['results'][0]['awr'], {'state': 'quiescent'}, f"The state is not quiescent check the results {check_awr}")

            # Create the bucket so that awr leaves quiescent state
            self.run_cbq_query("create bucket testbucket with {'ramQuota':256}")
            self.log.info("Sleeping for 60 seconds to wait for the next interval")
            time.sleep(60)
            
            # Check vitals again now the awr state should be active
            check_awr = self.run_cbq_query("SELECT awr FROM system:vitals") 
            self.assertEqual(check_awr['results'][0]['awr']['state'], 'active', f"The state is not active and we are expecting it to be check the results {check_awr}")
        finally:
            # Drop test bucket
            self.run_cbq_query("DROP BUCKET testbucket")
        
    '''This test will check that all awr options for query can be changed'''
    def test_awr_enabled(self):
        update_awr = self.run_cbq_query("UPDATE system:awr SET enabled = true")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['enabled'], True, f"AWR value is not set to the expected value, please check the results {check_awr}")
        update_awr = self.run_cbq_query("UPDATE system:awr SET enabled = false")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['enabled'], False, f"AWR value is not set to the expected value, please check the results {check_awr}")

    '''This test will check that we cannot set the value of enabled to something other than true or false'''
    def test_awr_enabled_negative(self):
        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET enabled = 'true'")
            self.fail("We expect this query to error out")
        except Exception as e:
            self.assertTrue("Setting 'enabled' must be: A boolean value." in str(e), f"Error is not as expected, please check the results {e}")
    
    '''This test will check that we can set interval with seconds, minutes, hours'''
    def test_awr_interval(self):
        update_awr = self.run_cbq_query("UPDATE system:awr SET interval = '90s'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['interval'], '1m30s', f"AWR interval is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET interval = '2m30s'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['interval'], '2m30s', f"AWR interval is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET interval = '1h10m5s'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['interval'], '1h10m5s', f"AWR interval is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET interval = '600s'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['interval'], '10m0s', f"AWR interval is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET interval = '3600s'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['interval'], '1h0m0s', f"AWR interval is not set to the expected value, please check the results {check_awr}")
    
        update_awr = self.run_cbq_query("UPDATE system:awr SET interval = '90m'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['interval'], '1h30m0s', f"AWR interval is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET interval = '48h'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['interval'], '48h0m0s', f"AWR interval is not set to the expected value, please check the results {check_awr}")

    '''This test will check that we cannot set interval with something other than seconds, minutes, hours'''
    def test_awr_interval_negative(self):
        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET interval = '30s'")
            self.fail("We expect this query to error out")
        except Exception as e:
            self.assertTrue("must be: A valid duration string." in str(e), f"Error is not as expected, please check the results {e}")

        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET interval = 'invalid'")
            self.fail("We expect this query to error out")
        except Exception as e:
            self.assertTrue("must be: A valid duration string." in str(e), f"Error is not as expected, please check the results {e}")
    
    '''This will test if we can set the location to a valid keyspace path'''
    def test_awr_location(self):
        update_awr = self.run_cbq_query("UPDATE system:awr SET location = 'default'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['location'], '`default`:`default`', f"AWR location is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET location = 'default:default._default._default'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['location'], '`default`:`default`.`_default`.`_default`', f"AWR location is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET location = '`travel-sample`.inventory.airline'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['location'], '`default`:`travel-sample`.`inventory`.`airline`', f"AWR location is not set to the expected value, please check the results {check_awr}")
    
    def test_awr_location_negative(self):
        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET location = 'default:default._default'")
            self.fail("We expect this query to error out")
        except Exception as e:
            self.assertTrue("must be: A string representating a syntactically valid path to a bucket or collection. The only permitted namespace is" in str(e), f"Error is not as expected, please check the results {e}")

        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET location = 'namespace:default'")
            self.fail("We expect this query to error out") 
        except Exception as e:
            self.assertTrue("must be: A string representating a syntactically valid path to a bucket or collection. The only permitted namespace is" in str(e), f"Error is not as expected, please check the results {e}")

    '''This will test if we can set the location to a valid keyspace path with special characters'''
    def test_awr_location_special_characters(self):
        pass
    
    '''This will test if we can set the number of statements to a valid number'''
    def test_awr_num_statements(self):
        update_awr = self.run_cbq_query("UPDATE system:awr SET num_statements = 100000")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['num_statements'], 100000, f"AWR num_statements is not set to the expected value, please check the results {check_awr}")
    
    '''This will test if we can set the number of statements to a number that is not an integer or is negative'''
    def test_awr_num_statements_negative(self):
        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET num_statements = -1")
            self.fail("We expect this query to error out")
        except Exception as e:
            self.assertTrue("must be: A positive integer" in str(e), f"Error is not as expected, please check the results {e}")

        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET num_statements = 1.5") 
            self.fail("We expect this query to error out")
        except Exception as e:
            self.assertTrue("must be: A positive integer" in str(e), f"Error is not as expected, please check the results {e}")
        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET num_statements = 2147483647")  # Max 32-bit integer
            self.fail("We expect this query to error out")
        except Exception as e:
            self.assertTrue("must be: A positive integer. The maximum allowable value is 100000." in str(e), f"Error is not as expected, please check the results {e}")
    
    '''This will test if we can set the queue length to a valid number'''
    def test_awr_queue_length(self):
        update_awr = self.run_cbq_query("UPDATE system:awr SET queue_len = 160")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['queue_len'], 160, f"AWR queue_len is not set to the expected value, please check the results {check_awr}")
    
    '''This will test if we can set the queue length to a number that is not an integer or is negative'''
    def test_awr_queue_length_negative(self):
        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET queue_len = -1")
            self.fail("We expect this query to error out")
        except Exception as e:
            self.assertTrue("must be: A positive integer" in str(e), f"Error is not as expected, please check the results {e}")

        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET queue_len = 1.5") 
            self.fail("We expect this query to error out")
        except Exception as e:
            self.assertTrue("must be: A positive integer" in str(e), f"Error is not as expected, please check the results {e}")
        
        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET queue_len = 2147483647")  # Max 32-bit integer
            self.fail("We expect this query to error out")
        except Exception as e:
            self.assertTrue("must be: A positive integer. The maximum allowable value is" in str(e), f"Error is not as expected, please check the results {e}")
    
    '''This will test if we can set the threshold to a valid number of seconds'''
    def test_threshold(self):
        update_awr = self.run_cbq_query("UPDATE system:awr SET threshold = '0s'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['threshold'], '0s', f"AWR threshold is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET threshold = '1s'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['threshold'], '1s', f"AWR threshold is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET threshold = '1h10m5s'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['threshold'], '1h10m5s', f"AWR threshold is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET threshold = '600s'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['threshold'], '10m0s', f"AWR threshold is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET threshold = '3600s'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['threshold'], '1h0m0s', f"AWR threshold is not set to the expected value, please check the results {check_awr}")
    
        update_awr = self.run_cbq_query("UPDATE system:awr SET threshold = '90m'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['threshold'], '1h30m0s', f"AWR threshold is not set to the expected value, please check the results {check_awr}")

        update_awr = self.run_cbq_query("UPDATE system:awr SET threshold = '48h'")
        self.assertEqual(update_awr['status'], 'success')
        check_awr = self.run_cbq_query("SELECT * FROM system:awr")
        self.assertEqual(check_awr['results'][0]['awr']['threshold'], '48h0m0s', f"AWR threshold is not set to the expected value, please check the results {check_awr}")
    
    '''This will test if we can set the threshold to a number that is not given in seconds'''
    def test_threshold_negative(self):
        try:
            update_awr = self.run_cbq_query("UPDATE system:awr SET threshold = 'invalid'")
            self.fail("We expect this query to error out")
        except Exception as e:
            self.assertTrue("must be: A valid duration string." in str(e), f"Error is not as expected, please check the results {e}")  