from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests
import time
from deepdiff import DeepDiff
from membase.api.exception import CBQError
import threading

class QueryUpdateStatsTests(QueryTests):

    def setUp(self):
        super(QueryUpdateStatsTests, self).setUp()
        self.log.info("==============  QueryUpdateStatsTests setup has started ==============")
        self.analyze = self.input.param("analyze", False)
        self.opt_keyspace = self.input.param("opt_keyspace", "")
        self.scope = self.input.param("scope", "_default")
        self.collection = self.input.param("collection", "_default")
        self.resolution = self.input.param("resolution", 1)
        self.sample_size = self.input.param("sample_size", 0)
        self.process = self.input.param("process","indexer")

        # Set update statistics vs analyze syntax
        if self.analyze:
            self.UPDATE_STATISTICS = "ANALYZE"
            self.DELETE = "DELETE STATISTICS"
            self.DELETE_ALL = "DELETE STATISTICS"
            self.FOR = ""
        else:
            self.UPDATE_STATISTICS = "UPDATE STATISTICS"
            self.DELETE = "DELETE"
            self.DELETE_ALL = "DELETE ALL"
            self.FOR = "FOR"

        # Set keyspace for default or scope/collection
        if self.scope == "_default" and self.collection == "_default":
            self.keyspace = f"`{self.bucket_name}`"
        else:
            self.keyspace = f"`{self.bucket_name}`.`{self.scope}`.`{self.collection}`"

        if self.load_sample:
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
        
        self.delete_stats(bucket="travel-sample")

        self.log.info("==============  QueryUpdateStatsTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryUpdateStatsTests, self).suite_setUp()
        self.log.info("==============  QueryUpdateStatsTests suite_setup has started ==============")
        self.log.info("==============  QueryUpdateStatsTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryUpdateStatsTests tearDown has started ==============")
        travel_sample = self.get_bucket_from_name("travel-sample")
        if travel_sample:
            self.delete_bucket(travel_sample)
        self.log.info("==============  QueryUpdateStatsTests tearDown has completed ==============")
        super(QueryUpdateStatsTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryUpdateStatsTests suite_tearDown has started ==============")
        self.log.info("==============  QueryUpdateStatsTests suite_tearDown has completed ==============")
        super(QueryUpdateStatsTests, self).suite_tearDown()

    def test_update_stats(self):
        histogram_expected = [
            {'scope': self.scope, 'collection': self.collection, 'histogramKey': '(meta().id)'},
            {'scope': self.scope, 'collection': self.collection, 'histogramKey': 'city'},
            {'scope': self.scope, 'collection': self.collection, 'histogramKey': 'country'}
        ]
        update_stats_queries = [
            f"{self.UPDATE_STATISTICS} {self.keyspace}(city, country)", 
            f"UPDATE STATISTICS FOR {self.keyspace}(city, country)"]
        for update_stats in update_stats_queries:
            try:
                stats = self.run_cbq_query(query=update_stats)
                histogram = self.run_cbq_query(query="select `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'")
                with self.subTest("Update stats syntax", query=update_stats):
                    self.assertEqual(histogram['results'],histogram_expected)
            except Exception as e:
                self.log.error(f"Update statistics failed: {e}")
                self.fail()

    def test_update_stats_with(self):
        with_clauses = [
            "{'resolution':0.5}",
            "{'resolution':1, 'sample_size':20000}",
            "{'resolution':1.5, 'sample_size':20000, 'update_statistics_timeout':120}",
            "{'resolution':1.5, 'sample_size':20000, 'update_statistics_timeout':120, 'batch_size':5}"
        ]
        for with_clause in with_clauses:
            update_stats = f"{self.UPDATE_STATISTICS} {self.keyspace}(city, country) WITH {with_clause}"
            try:
                stats = self.run_cbq_query(query=update_stats)
            except Exception as e:
                self.log.error(f"Update statistics failed: {e}")
                self.fail()

    def test_delete_stats_all(self):
        histogram_expected = [
            {'scope': '_default', 'collection': '_default', 'histogramKey': '(meta().id)'}, 
            {'scope': '_default', 'collection': '_default', 'histogramKey': 'city'}, 
            {'scope': '_default', 'collection': '_default', 'histogramKey': 'country'}, 
            {'scope': 'inventory', 'collection': 'airport', 'histogramKey': '(meta().id)'}, 
            {'scope': 'inventory', 'collection': 'airport', 'histogramKey': 'city'}, 
            {'scope': 'inventory', 'collection': 'airport', 'histogramKey': 'country'}
        ]
        update_stats_collection = "UPDATE STATISTICS FOR `travel-sample`.inventory.airport(city, country)"
        update_stats_default = "UPDATE STATISTICS `travel-sample`(city, country)"
        if self.scope == "_default" and self.collection == "_default":
            histogram_expected_after_delete = histogram_expected[3:]
        else:
            histogram_expected_after_delete = histogram_expected[:3]
        delete_stats = f"{self.UPDATE_STATISTICS} {self.opt_keyspace} {self.keyspace} {self.DELETE_ALL}"
        try:
            # collect statistics
            stats = self.run_cbq_query(query=update_stats_default)
            stats = self.run_cbq_query(query=update_stats_collection)
            histogram = self.run_cbq_query(query="select `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected)
            # delete statistics
            del_stats = self.run_cbq_query(query=delete_stats)
            histogram = self.run_cbq_query(query="select `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected_after_delete)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_delete_stats_fields(self):
        histogram_expected = [
            {'scope': '_default', 'collection': '_default', 'histogramKey': '(meta().id)'}, 
            {'scope': '_default', 'collection': '_default', 'histogramKey': 'city'}, 
            {'scope': '_default', 'collection': '_default', 'histogramKey': 'country'}, 
            {'scope': '_default', 'collection': '_default', 'histogramKey': 'state'}, 
            {'scope': 'inventory', 'collection': 'landmark', 'histogramKey': '(meta().id)'}, 
            {'scope': 'inventory', 'collection': 'landmark', 'histogramKey': 'city'}, 
            {'scope': 'inventory', 'collection': 'landmark', 'histogramKey': 'country'},
            {'scope': 'inventory', 'collection': 'landmark', 'histogramKey': 'state'}
        ]
        update_stats_default = "UPDATE STATISTICS `travel-sample`(city, country, state)"
        update_stats_collection = "UPDATE STATISTICS `travel-sample`.inventory.landmark(city, country, state)"
        if self.scope == "_default" and self.collection == "_default":
            histogram_expected_after_delete = histogram_expected[:1] + histogram_expected[3:]
        else:
            histogram_expected_after_delete = histogram_expected[:5] +  histogram_expected[-1:]
        delete_stats = f"{self.UPDATE_STATISTICS} {self.opt_keyspace} {self.keyspace} {self.DELETE} (city,country)"
        try:
            # collect statistics
            stats = self.run_cbq_query(query=update_stats_default)
            stats = self.run_cbq_query(query=update_stats_collection)
            histogram = self.run_cbq_query(query="select `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected)
            # delete statistics
            del_stats = self.run_cbq_query(query=delete_stats)
            histogram = self.run_cbq_query(query="SELECT `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'")
            self.log.info(histogram['results'])
            self.assertEqual(histogram['results'],histogram_expected_after_delete)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_update_stats_timeout(self):
        error_code = 5360
        error_message = "Update Statistics timeout (2 seconds) exceeded"
        update_stats = f"UPDATE STATISTICS `{self.bucket_name}`(city, country) WITH {{'update_statistics_timeout':2}}"
        try:
            stats = self.run_cbq_query(query=update_stats)
            self.fail(f"Update statistics dit not time-out. Expected error is {error}")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_message)

    def test_update_stats_index(self):
        histogram_expected = [
            {'scope': self.scope, 'collection': self.collection, 'histogramKey': '(meta().id)'},
            {'scope': self.scope, 'collection': self.collection, 'histogramKey': 'city'}
        ]
        if self.scope == "_default" and self.collection == "_default":
            index_name = "def_city"
        else:
            index_name = "def_inventory_airport_city"
            self.keyspace = "default:" + self.keyspace
        update_stats_indexes = [
            f"{self.UPDATE_STATISTICS} {self.FOR} INDEX {self.keyspace}.{index_name}",
            f"{self.UPDATE_STATISTICS} {self.FOR} INDEX {index_name} ON {self.keyspace}"
        ]
        for update_stats in update_stats_indexes:
            try:
                stats = self.run_cbq_query(query=update_stats)
                # Check
                histogram = self.run_cbq_query(query="SELECT `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'")
                self.assertEqual(histogram['results'],histogram_expected)
                # delete stats
                delete = self.run_cbq_query(query=f"UPDATE STATISTICS {self.keyspace} DELETE ALL")
            except Exception as e:
                self.log.error(f"Update statistics failed: {e}")
                self.fail()        

    def test_update_stats_index_multi(self):
        histogram_expected = [
            {'scope': self.scope, 'collection': self.collection, 'histogramKey': '(meta().id)'},
            {'scope': self.scope, 'collection': self.collection, 'histogramKey': 'airportname'},
            {'scope': self.scope, 'collection': self.collection, 'histogramKey': 'city'},
            {'scope': self.scope, 'collection': self.collection, 'histogramKey': 'faa'}
        ]
        if self.scope == "_default" and self.collection == "_default":
            index_list = "def_airportname,def_city,def_faa"
        else:
            index_list = "def_inventory_airport_airportname,def_inventory_airport_city,def_inventory_airport_faa"
        update_stats_indexes = [
            f"{self.UPDATE_STATISTICS} {self.keyspace} INDEX ({index_list})",
            f"{self.UPDATE_STATISTICS} {self.FOR} {self.keyspace} INDEX ((SELECT RAW name FROM system:indexes WHERE state = 'online' AND `using` = 'gsi' AND keyspace_id = 'travel-sample' and name in {index_list.split(',')}))"
        ]
        
        self.run_cbq_query(query="UPDATE STATISTICS `travel-sample` DELETE ALL")
        
        for update_stats in update_stats_indexes:
            try:
                stats = self.run_cbq_query(query=update_stats)
                # Check
                histogram = self.run_cbq_query(query="SELECT `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'")
                self.assertEqual(histogram['results'],histogram_expected)
                # delete stats
                delete = self.run_cbq_query(query="UPDATE STATISTICS `travel-sample` DELETE ALL")
            except Exception as e:
                self.log.error(f"Update statistics failed: {e}")
                self.fail()
    
    def test_negative_index_all(self):
        error = "INDEX ALL option for UPDATE STATISTICS (ANALYZE) can only be used for a collection."
        update_stats = "UPDATE STATISTICS `travel-sample` INDEX ALL"
        try:
            self.run_cbq_query(query=update_stats)
            self.fail(f"Query did not fail as expected with error: {error}")
        except CBQError as ex:
            self.assertTrue(str(ex).find(error) > 0)

    def test_update_stats_index_all(self):
        histogram_expected = [
            {'scope': 'inventory', 'collection': 'airport', 'histogramKey': '(meta().id)'},
            {'scope': 'inventory', 'collection': 'airport', 'histogramKey': 'airportname'},
            {'scope': 'inventory', 'collection': 'airport', 'histogramKey': 'city'},
            {'scope': 'inventory', 'collection': 'airport', 'histogramKey': 'faa'}
        ]
        update_stats = f"{self.UPDATE_STATISTICS} {self.FOR} {self.opt_keyspace} `travel-sample`.`inventory`.`airport` INDEX ALL"
        try:
            stats = self.run_cbq_query(query=update_stats)
            # Check
            histogram = self.run_cbq_query(query="SELECT `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_update_stats_function(self):
        histogram_expected = [
            {'scope': '_default', 'collection': '_default', 'histogramKey': '(meta().id)'}, 
            {'scope': '_default', 'collection': '_default', 'histogramKey': 'lower(city)'}
        ]
        update_stats = f"UPDATE STATISTICS `{self.bucket_name}`(LOWER(city))"
        try:
            stats = self.run_cbq_query(query=update_stats)
            # Check
            histogram = self.run_cbq_query(query="SELECT `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()        

    def test_update_stats_array(self):
        histogram_expected = [
            {'scope': '_default', 'collection': '_default', 'histogramKey': '(meta().id)'}, 
            {'scope': '_default', 'collection': '_default', 'histogramKey': '(all (array (_usv_1.utc) for _usv_1 in schedule end))'},
            {'scope': '_default', 'collection': '_default', 'histogramKey': '(distinct (array (_usv_1.day) for _usv_1 in schedule end))'},
        ]
        update_stats_array = [
            f"UPDATE STATISTICS `{self.bucket_name}`(DISTINCT (ARRAY (`v`.`day`) FOR `v` IN `schedule` END))",
            f"UPDATE STATISTICS `{self.bucket_name}`(ALL (ARRAY (`s`.`utc`) FOR `s` in `schedule` END))"
        ]
        try:
            # collect stats for distinc and all arrays
            stats = self.run_cbq_query(query=update_stats_array[0])
            stats = self.run_cbq_query(query=update_stats_array[1])
            # check
            histogram = self.run_cbq_query(query="SELECT `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_update_stats_gsi(self):
        update_stats = "UPDATE STATISTICS FOR INDEX `travel-sample`.def_city USING GSI"
        try:
            stats = self.run_cbq_query(query=update_stats)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_update_stats_fts(self):
        error = "UPDATE STATISTICS (ANALYZE) supports GSI indexes only for INDEX option."
        update_stats = "UPDATE STATISTICS FOR INDEX `travel-sample`.def_city USING FTS"
        try:
            stats = self.run_cbq_query(query=update_stats)
            self.fail(f"Update statistics did return expected error: {error}")
        except CBQError as ex:
            self.assertTrue(str(ex).find(error) > 0)

    def test_drop_sys_collection(self):
        update_stats = "UPDATE STATISTICS `travel-sample`(city) WITH {'update_statistics_timeout':600}"
        self.run_cbq_query(query=update_stats, query_params={'timeout':'600s'})
        try:
            # drop collection
            self.run_cbq_query(query="DROP COLLECTION `travel-sample`.`_system`.`_query`")
            self.fail(f"Query did not fail as expected!")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 12028)
            self.assertEqual(error['msg'], 'Error while dropping collection default:travel-sample._system._query - cause: [_:Cannot drop system collection "_query" for scope "_system"]')

    def test_drop_sys_scope(self):
        update_stats = "UPDATE STATISTICS `travel-sample`(city) WITH {'update_statistics_timeout':600}"
        self.run_cbq_query(query=update_stats, query_params={'timeout':'600s'})
        try:
            # drop scope
            self.run_cbq_query(query="DROP SCOPE `travel-sample`.`_system`")
            self.fail(f"Query did not fail as expected!")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 12026)
            self.assertEqual(error['msg'], 'Error while dropping scope default:travel-sample._system - cause: [_:Deleting _system scope is not allowed]')

    def test_drop_sys_bucket(self):
        histogram_query = "SELECT `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'"
        histogram_expected = [
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "city", "scope": "_default"}
        ]
        update_stats = "UPDATE STATISTICS `travel-sample`(city) WITH {'update_statistics_timeout':600}"
        try:
            stats = self.run_cbq_query(query=update_stats, query_params={'timeout':'600s'})
            # drop bucket
            self.delete_bucket("N1QL_SYSTEM_BUCKET")
            self.sleep(2)
            # update stats after drop sytem bucket
            stats = self.run_cbq_query(query=update_stats)
            # check stats
            histogram = self.run_cbq_query(query=histogram_query)
            self.assertEqual(histogram['results'],histogram_expected)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_negative_txn(self):
        error_code = 17002
        error_msg = "UPDATE_STATISTICS statement is not supported within the transaction"
        results = self.run_cbq_query(query="BEGIN WORK", server=self.master)
        txid = results['results'][0]['txid']
        try:
            start = self.run_cbq_query(query="UPDATE STATISTICS `travel-sample`(city)", txnid=txid, server=self.master)
            self.fail("Update statistics did not fail. Error expected: {0}".format(error))
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], error_code)
            self.assertEqual(error['msg'], error_msg)

    def test_negative_authorization(self):
        error = "User does not have credentials to run"
        queries = [
            "DELETE FROM `travel-sample`.`_system`.`_query` WHERE type = 'histogram'",
            "DROP COLLECTION `travel-sample`.`_system`.`_query`"
        ]
        # create user with select permission on travel-sample only
        self.users = [{"id": "jackDoe", "name": "Jack Downing", "password": "password1"}]
        self.create_users()
        user_id, user_pwd = self.users[0]['id'], self.users[0]['password']
        self.run_cbq_query(query=f"GRANT query_select on `{self.bucket_name}` to {user_id}")
        # collect some stats
        self.run_cbq_query(query="UPDATE STATISTICS `travel-sample`(city)")
        # try to access system bucket with user
        for query in queries:
            try:
                self.run_cbq_query(query=query, username=user_id, password=user_pwd)
                self.fail(f"Query did not fail as expected with error: {error}")
            except CBQError as ex:
                self.assertTrue(str(ex).find(error) > 0)

    def test_sys_bucket_100mb(self):
        # Update stats in order to create N1QL_SYSTEM_BUCKET
        self.run_cbq_query(query="UPDATE STATISTICS `travel-sample`(city) WITH {'update_statistics_timeout':240}")
        # Check N1QL_SYSTEM_BUCKET memory quota
        system_bucket = self.rest.get_bucket_json(bucket="N1QL_SYSTEM_BUCKET")
        system_stats = self.rest.fetch_system_stats()
        num_nodes = len(system_stats['nodes'])
        self.assertEqual(system_bucket['quota']['ram'], 100*1024*1024*num_nodes)

    def test_update_stats_quota_full(self):
        error = "Error while creating system bucket N1QL_SYSTEM_BUCKET - cause: [ramQuota:RAM quota specified is too large to be provisioned into this cluster.]"
        histogram_query = "SELECT `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'"
        histogram_expected = [
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "city", "scope": "_default"}
        ]
        self.log.info("Drop N1QL_SYSTEM_BUCKET if exists")
        self.ensure_bucket_does_not_exist("N1QL_SYSTEM_BUCKET")
        system_stats = self.rest.fetch_system_stats()
        num_nodes = len(system_stats['nodes'])
        quota_total = system_stats['storageTotals']['ram']['quotaTotal']
        quota_used = system_stats['storageTotals']['ram']['quotaUsed']
        quota_remaining = (quota_total - quota_used) // 1024 // 1024
        # create big bucket to fill up quota
        self.rest.create_bucket(bucket="bigbucket",ramQuotaMB=quota_remaining//num_nodes)
        # update statistics should fail since there is no more quota
        try:
            self.run_cbq_query(query="UPDATE STATISTICS `travel-sample`(city)")
            self.fail(f"Query did not fail as expected with error: {error}")
        except CBQError as ex:
            self.assertTrue(str(ex).find(error) > 0)
        # update bucket quota
        self.rest.change_bucket_props(bucket="bigbucket", ramQuotaMB=256)
        try:
            self.run_cbq_query(query="UPDATE STATISTICS `travel-sample`(city)")
            # check stats
            histogram = self.run_cbq_query(query=histogram_query)
            self.assertEqual(histogram['results'],histogram_expected)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_update_stats_resolution(self):
        error_message = f"Invalid resolution ({'%.2f' % self.resolution})specified, resolution must be between 0.02 and 5.0"
        error_code = 5360
        field = "city"
        distribution_query = f"with distribution as (select meta().distributions.{field} from system:dictionary where `bucket` = \"travel-sample\" and `scope` = \"{self.scope}\" and `keyspace` = \"{self.collection}\") select distribution[0].{field}.resolution"
        update_stats = f"{self.UPDATE_STATISTICS} {self.keyspace}({field}) WITH {{'resolution':{self.resolution}}}"
        if self.resolution >= 0.02 and self.resolution <= 5:
            # Valid values for resolution
            try:
                # run stats
                self.run_cbq_query(query=update_stats)
                # check resolution
                self.sleep(2)
                distribution = self.run_cbq_query(query=distribution_query)
                self.assertEqual(distribution['results'][0]['resolution'], self.resolution)
            except Exception as e:
                self.log.error(f"Update statistics failed: {e}")
                self.fail()
        else:
            # Out of range values for resolution
            try:
                self.run_cbq_query(query=update_stats)
                self.fail(f"Query did not fail as expected with error: {error_message}")
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], error_code)
                self.assertEqual(error['msg'], error_message)

    def test_update_stats_sample(self):
        field = "city"
        distribution_query = f"with distribution as (select meta().distributions.{field} from system:dictionary where `bucket` = \"travel-sample\" and `scope` = \"{self.scope}\" and `keyspace` = \"{self.collection}\") select distribution[0].{field}.sampleSize"
        update_stats = f"{self.UPDATE_STATISTICS} {self.keyspace}({field}) WITH {{'sample_size':{self.sample_size}}}"
        if self.scope == '_default' and self.collection == '_default':
            min_size, max_size = 11507, 31591
        else:
            min_size, max_size = 1424, 1968
        try:
            # run stats
            self.run_cbq_query(query=update_stats)
            # check resolution
            self.sleep(3)
            distribution = self.run_cbq_query(query=distribution_query)
            if self.sample_size >= min_size and self.sample_size <=max_size:
                self.assertTrue(distribution['results'][0]['sampleSize'] >= self.sample_size)
            elif self.sample_size >= max_size:
                self.assertTrue(distribution['results'][0]['sampleSize'] >= min_size)
            elif self.scope == '_default':
                self.assertTrue(distribution['results'][0]['sampleSize'] >= min_size)
            else:
                self.assertEqual(distribution['results'][0]['sampleSize'], max_size)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def run_update_stats(self, query, server):
        try:
            results = self.run_cbq_query(query=query,server=server)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")

    def test_kill_service(self):
        node1 = self.servers[0]
        update_stats = "UPDATE STATISTICS FOR `travel-sample` INDEX ((select raw name from system:indexes where state = 'online' and keyspace_id = 'travel-sample')) WITH {'update_statistics_timeout':240}"
        th = threading.Thread(target=self.run_update_stats,args=(update_stats, node1))
        remote_client = RemoteMachineShellConnection(node1)
        # Spawn update statistics in a thread
        th.start()
        # Kill indexer
        self.sleep(1)
        self.log.info(f"Kill process: {self.process} ...")
        remote_client.terminate_process(process_name=self.process, force=True)
        th.join()
        self.sleep(3)
        try:
            self.run_cbq_query(query=update_stats, server=node1)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_drop_bucket(self):
        histogram_query = "SELECT `scope`, `collection`, `histogramKey` from `travel-sample`.`_system`.`_query` data WHERE type = 'histogram'"
        histogram_expected = [
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "city", "scope": "_default"}
        ]
        update_stats = "UPDATE STATISTICS `travel-sample`(city) WITH {'update_statistics_timeout':600}"
        try:
            stats = self.run_cbq_query(query=update_stats, query_params={'timeout':'600s'})
            # drop and recreate bucket
            self.log.info("Drop travel-sample bucket")
            self.delete_bucket("travel-sample")
            self.sleep(2)
            self.log.info("Recreate travel-sample bucket")
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
            # update stats after drop sytem bucket
            stats = self.run_cbq_query(query=update_stats)
            # check stats
            histogram = self.run_cbq_query(query=histogram_query)
            self.assertEqual(histogram['results'],histogram_expected)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_missing_field(self):
        for field in ["fake_field", "DISTINCT (ARRAY (`v`.`day`) FOR `v` IN `fake_field` END)"]:
            update_stats = f"{self.UPDATE_STATISTICS} {self.keyspace}({field})"
            try:
                self.run_cbq_query(query=update_stats)
            except Exception as e:
                self.log.error(f"Update statistics failed: {e}")
                self.fail()
    
    def test_missing_keyspace(self):
        error_code = 12003
        fake_keyspaces = {
            "`fake-bucket`": "Keyspace not found in CB datastore: default:fake-bucket - cause: No bucket named fake-bucket",
            "`travel-sample`.inventory.`fake-collection`": "Keyspace not found in CB datastore: default:travel-sample.inventory.fake-collection"
        }
        for keyspace in fake_keyspaces:
            update_stats = f"{self.UPDATE_STATISTICS} {keyspace}(city)"
            try:
                self.run_cbq_query(query=update_stats)
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], error_code)
                self.assertEqual(error['msg'], fake_keyspaces[keyspace])

    def test_multi_node(self):
        node1 = self.servers[0]
        node2 = self.servers[1]
        update_stats = f"{self.UPDATE_STATISTICS} {self.keyspace}(city) WITH {{'update_statistics_timeout':600}}"
        explain_query = f"explain select airportname from {self.keyspace} where city = 'Lyon'"
        try:
            explain_before = self.run_cbq_query(query=explain_query, server=node2)
            self.assertEqual(list(explain_before['results'][0].keys()), ['plan', 'text'])
            # run update statistics
            self.run_cbq_query(query=update_stats, server=node1)
            explain_after = self.run_cbq_query(query=explain_query, server=node2)
            self.assertTrue(explain_after['results'][0]['cost'] > 0 and explain_after['results'][0]['cardinality'] > 0)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_stats_query_join(self):
        update_stats = f"{self.UPDATE_STATISTICS} `travel-sample`.inventory.airport(city) WITH {{'update_statistics_timeout':600}}"
        explain_query = f"explain SELECT DISTINCT  MIN(aport.airportname) AS Airport__Name, MIN(lmark.name) AS Landmark_Name, MIN(aport.tz) AS Landmark_Time FROM `travel-sample`.inventory.airport aport LEFT JOIN `travel-sample`.inventory.landmark lmark ON aport.city = lmark.city WHERE lmark.country = 'United States' GROUP BY lmark.name ORDER BY lmark.name LIMIT 3"
        try:
            explain_before = self.run_cbq_query(query=explain_query)
            fetch_operator = explain_before['results'][0]['plan']['~children'][0]['~children'][1] 
            self.assertEqual(list(fetch_operator.keys()), ['#operator', 'as', 'bucket', 'early_projection', 'keyspace', 'namespace', 'scope'])
            # run update statistics
            self.run_cbq_query(query=update_stats)
            explain_after = self.run_cbq_query(query=explain_query)
            fetch_operator = explain_after['results'][0]['plan']['~children'][0]['~children'][1]['optimizer_estimates']
            self.assertTrue(fetch_operator['cost'] > 0 and fetch_operator['cardinality'] > 0)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_stats_query(self):
        update_stats = f"{self.UPDATE_STATISTICS} {self.keyspace}(city, free_internet) WITH {{'update_statistics_timeout':600}}"
        explain_query = f"explain select name, city from {self.keyspace} where type = 'hotel' and state = 'Rhône-Alpes' and city='Chamonix-Mont-Blanc' and vacancy"
        try:
            explain_before = self.run_cbq_query(query=explain_query)
            interscan_operator = explain_before['results'][0]['plan']['~children'][0]['scans'][0]
            self.assertEqual(list(interscan_operator.keys()), ['#operator', 'index', 'index_id', 'index_projection', 'keyspace', 'namespace', 'spans', 'using'])
            # run update statistics
            self.run_cbq_query(query=update_stats)
            explain_after = self.run_cbq_query(query=explain_query)
            self.log.info(f"plan is: {explain_after['results'][0]['plan']}")
            self.assertTrue("optimizer_estimates" in str(explain_after['results'][0]['plan']))
            # run query
            self.run_cbq_query(query=explain_query[8:])
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_update_histogram(self):
        update_stats = f"{self.UPDATE_STATISTICS} {self.keyspace}(city)"
        update_histogram = "update `travel-sample`.`_system`.`_query` set histogram = 'abcdef' where type = 'histogram'"
        select_histogram = "select histogram from `travel-sample`.`_system`.`_query` WHERE type = 'histogram'"
        select_query = f"select airportname from {self.keyspace} where type = 'airport' and city = 'Lyon' order by airportname"
        expected_results = [
            {"airportname": "Bron"},
            {"airportname": "Lyon Part-Dieu Railway"},
            {"airportname": "Saint Exupery"}
        ]
        try:
            # update stats
            self.run_cbq_query(query=update_stats)
            # run query
            results = self.run_cbq_query(query=select_query)
            self.assertEqual(results['results'], expected_results)
            # update histogram
            self.run_cbq_query(query=update_histogram)
            histogram = self.run_cbq_query(query=select_histogram)
            self.assertEqual(histogram['results'], [{"histogram": "abcdef"}, {"histogram": "abcdef"}])
            # run query
            results = self.run_cbq_query(query=select_query)
            self.assertEqual(results['results'], expected_results)
            # update stats again and check histogram
            self.run_cbq_query(query=update_stats)
            histogram = self.run_cbq_query(query=select_histogram)
            self.assertNotEqual(histogram['results'], [{"histogram": "abcdef"}, {"histogram": "abcdef"}])
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_stats_insert(self):
        update_query_lyon = f"update {self.keyspace} set best_airport = 'yes' where type = 'airport' and city = 'Lyon'"
        update_query_portland = f"update {self.keyspace} set best_airport = 'yes' where type = 'airport' and city = 'Portland'"
        update_stats_city = f"{self.UPDATE_STATISTICS} {self.keyspace}(city)"
        update_stats_best_airport = f"{self.UPDATE_STATISTICS} {self.keyspace}(best_airport)"
        select_query_city = f"select airportname from {self.keyspace} where type = 'airport' and city = 'Lyon' order by airportname"
        select_query_best_airport = f"select airportname from {self.keyspace} where type = 'airport' and best_airport = 'yes' order by airportname"
        expected_results_lyon = [
            {"airportname": "Bron"},
            {"airportname": "Lyon Part-Dieu Railway"},
            {"airportname": "Saint Exupery"}
        ]
        expected_results_best_airport = [
            {"airportname": "Bron"},
            {"airportname": "Lyon Part-Dieu Railway"},
            {"airportname": "Portland Intl"},
            {"airportname": "Portland Intl Jetport"},
            {"airportname": "Saint Exupery"}
        ]
        try:
            # update stats city
            self.run_cbq_query(query=update_stats_city)
            # run query on city
            results = self.run_cbq_query(query=select_query_city)
            self.assertEqual(results['results'], expected_results_lyon)
            # insert data (best airport)
            self.run_cbq_query(query=update_query_lyon)
            # update stats city
            self.run_cbq_query(query=update_stats_city)
            # run query on city
            results = self.run_cbq_query(query=select_query_city)
            self.assertEqual(results['results'], expected_results_lyon)
            # update stats best
            self.run_cbq_query(query=update_stats_best_airport)
            # run query on city
            results = self.run_cbq_query(query=select_query_city)
            self.assertEqual(results['results'], expected_results_lyon)
            # run query on best
            results = self.run_cbq_query(query=select_query_best_airport)
            self.assertEqual(results['results'], expected_results_lyon)
            # insert data (best airport)
            self.run_cbq_query(query=update_query_portland)
            # run query on best
            results = self.run_cbq_query(query=select_query_best_airport)
            self.assertEqual(results['results'], expected_results_best_airport)
            # update stats best
            self.run_cbq_query(query=update_stats_best_airport)
            # run query on best
            results = self.run_cbq_query(query=select_query_best_airport)
            self.assertEqual(results['results'], expected_results_best_airport)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_update_stats_batch(self):
        for batch in [0,10,5,99]:
            update_stats = f"{self.UPDATE_STATISTICS} {self.keyspace}(city) WITH {{'batch_size':{batch}}}"
            try:
                self.run_cbq_query(query=update_stats)
            except Exception as e:
                self.log.error(f"Update statistics failed: {e}")
                self.fail()

    def test_negative_batch(self):
        cases = [
            {'code': 5360, 'message': "'batch_size' option must be an integer, not string", 'batch_size': {'batch_size': '20'}},
            {'code': 5360, 'message': "'batch_size' option must be an integer, not float", 'batch_size': {'batch_size': 3.1}}
        ]
        for case in cases:
            error_code = case['code']
            error_message = case['message']
            update_stats = f"{self.UPDATE_STATISTICS} {self.keyspace}(city) WITH {case['batch_size']}"
            try:
                self.run_cbq_query(query=update_stats)
                self.fail(f"Query did not fail as expected with error: {error_message}")
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], error_code)
                self.assertEqual(error['msg'], error_message)

    def test_more_than_ten_column(self):
        update_stats = "UPDATE STATISTICS FOR `travel-sample`.inventory.hotel(title, name, address, directions, phone, tollfree, email, fax, url, checkin, checkout, price, geo.lat, geo.lon, geo.accuracy)"
        select_distribution = "SELECT distributionKeys FROM system:dictionary WHERE `bucket` = 'travel-sample' and `keyspace` = 'hotel';"
        self.run_cbq_query(update_stats)
        system_dictionary = self.run_cbq_query(select_distribution)
        expected_result = [
            '(geo.accuracy)', '(geo.lat)', '(geo.lon)', '(meta().id)',
            'address', 'checkin', 'checkout', 'directions',
            'email', 'fax', 'name', 'phone', 'price',
            'title', 'tollfree', 'url']
        actual_result = system_dictionary['results'][0]['distributionKeys']
        actual_result.sort()
        self.assertEqual(actual_result, expected_result)
