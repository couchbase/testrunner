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

            self.wait_for_all_indexes_online()
        
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
            {"bucket": "travel-sample", "collection": self.collection, "histogramKey": "city", "scope": self.scope},
            {"bucket": "travel-sample", "collection": self.collection, "histogramKey": "country", "scope": self.scope}]
        update_stats_queries = [
            f"{self.UPDATE_STATISTICS} {self.keyspace}(city, country)", 
            f"UPDATE STATISTICS FOR {self.keyspace}(city, country)"]
        for update_stats in update_stats_queries:
            try:
                stats = self.run_cbq_query(query=update_stats)
                histogram = self.run_cbq_query(query="select `bucket`, `scope`, `collection`, `histogramKey` from `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'")
                with self.subTest("Update stats syntax", query=update_stats):
                    self.assertEqual(histogram['results'],histogram_expected)
            except Exception as e:
                self.log.error(f"Update statistics failed: {e}")
                self.fail()

    def test_update_stats_with(self):
        with_clauses = [
            "{'resolution':0.5}",
            "{'resolution':1, 'sample_size':20000}",
            "{'resolution':1.5, 'sample_size':20000, 'update_statistics_timeout':120}"
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
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "city", "scope": "_default"},
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "country", "scope": "_default"},
            {"bucket": "travel-sample", "collection": "airport", "histogramKey": "city", "scope": "inventory"},
            {"bucket": "travel-sample", "collection": "airport", "histogramKey": "country", "scope": "inventory"}]
        update_stats_collection = "UPDATE STATISTICS FOR `travel-sample`.inventory.airport(city, country)"
        update_stats_default = "UPDATE STATISTICS `travel-sample`(city, country)"
        if self.scope == "_default" and self.collection == "_default":
            histogram_expected_after_delete = histogram_expected[2:]
        else:
            histogram_expected_after_delete = histogram_expected[:2]
        delete_stats = f"{self.UPDATE_STATISTICS} {self.opt_keyspace} {self.keyspace} {self.DELETE_ALL}"
        try:
            # collect statistics
            stats = self.run_cbq_query(query=update_stats_default)
            stats = self.run_cbq_query(query=update_stats_collection)
            histogram = self.run_cbq_query(query="select `bucket`, `scope`, `collection`, `histogramKey` from `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected)
            # delete statistics
            del_stats = self.run_cbq_query(query=delete_stats)
            histogram = self.run_cbq_query(query="select `bucket`, `scope`, `collection`, `histogramKey` from `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected_after_delete)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_delete_stats_fields(self):
        histogram_expected = [
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "city", "scope": "_default"},
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "country", "scope": "_default"},
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "state", "scope": "_default"},
            {"bucket": "travel-sample", "collection": "landmark", "histogramKey": "city", "scope": "inventory"},
            {"bucket": "travel-sample", "collection": "landmark", "histogramKey": "country", "scope": "inventory"},
            {"bucket": "travel-sample", "collection": "landmark", "histogramKey": "state", "scope": "inventory"}]
        update_stats_default = "UPDATE STATISTICS `travel-sample`(city, country, state)"
        update_stats_collection = "UPDATE STATISTICS `travel-sample`.inventory.landmark(city, country, state)"
        if self.scope == "_default" and self.collection == "_default":
            histogram_expected_after_delete = histogram_expected[2:]
        else:
            histogram_expected_after_delete = histogram_expected[:3] +  histogram_expected[-1:]
        delete_stats = f"{self.UPDATE_STATISTICS} {self.opt_keyspace} {self.keyspace} {self.DELETE} (city,country)"
        try:
            # collect statistics
            stats = self.run_cbq_query(query=update_stats_default)
            stats = self.run_cbq_query(query=update_stats_collection)
            histogram = self.run_cbq_query(query="select `bucket`, `scope`, `collection`, `histogramKey` from `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected)
            # delete statistics
            del_stats = self.run_cbq_query(query=delete_stats)
            histogram = self.run_cbq_query(query="select `bucket`, `scope`, `collection`, `histogramKey` from `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected_after_delete)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_update_stats_timeout(self):
        error = "Update Statistics timeout (2 seconds) exceeded"
        update_stats = f"UPDATE STATISTICS `{self.bucket_name}`(city, country) WITH {{'update_statistics_timeout':2}}"
        try:
            stats = self.run_cbq_query(query=update_stats)
            self.fail(f"Update statistics dit not time-out. Expected error is {error}")
        except CBQError as ex:
            self.assertTrue(str(ex).find(error) > 0)

    def test_update_stats_index(self):
        histogram_expected = [
            {"bucket": "travel-sample", "collection": self.collection, "histogramKey": "city", "scope": self.scope}
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
                histogram = self.run_cbq_query(query="SELECT `bucket`, `scope`, `collection`, `histogramKey` FROM `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'")
                self.assertEqual(histogram['results'],histogram_expected)
                # delete stats
                delete = self.run_cbq_query(query=f"UPDATE STATISTICS {self.keyspace} DELETE ALL")
            except Exception as e:
                self.log.error(f"Update statistics failed: {e}")
                self.fail()        

    def test_update_stats_index_multi(self):
        histogram_expected = [
            {"bucket": "travel-sample", "collection": self.collection, "histogramKey": "airportname", "scope": self.scope},
            {"bucket": "travel-sample", "collection": self.collection, "histogramKey": "city", "scope": self.scope},
            {"bucket": "travel-sample", "collection": self.collection, "histogramKey": "faa", "scope": self.scope}
        ]
        if self.scope == "_default" and self.collection == "_default":
            index_list = "def_airportname,def_city,def_faa"
        else:
            index_list = "def_inventory_airport_airportname,def_inventory_airport_city,def_inventory_airport_faa"
        update_stats_indexes = [
            f"{self.UPDATE_STATISTICS} {self.keyspace} INDEX ({index_list})",
            f"{self.UPDATE_STATISTICS} {self.FOR} {self.keyspace} INDEX ((SELECT RAW name FROM system:indexes WHERE state = 'online' AND `using` = 'gsi' AND keyspace_id = 'travel-sample' and name in {index_list.split(',')}))"
        ]
        for update_stats in update_stats_indexes:
            try:
                stats = self.run_cbq_query(query=update_stats)
                # Check
                histogram = self.run_cbq_query(query="SELECT `bucket`, `scope`, `collection`, `histogramKey` FROM `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'")
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
            {"bucket": "travel-sample", "collection": "airport", "histogramKey": "airportname", "scope": "inventory"},
            {"bucket": "travel-sample", "collection": "airport", "histogramKey": "city", "scope": "inventory"},
            {"bucket": "travel-sample", "collection": "airport", "histogramKey": "faa", "scope": "inventory"}
        ]
        update_stats = f"{self.UPDATE_STATISTICS} {self.FOR} {self.opt_keyspace} `travel-sample`.`inventory`.`airport` INDEX ALL"
        try:
            stats = self.run_cbq_query(query=update_stats)
            # Check
            histogram = self.run_cbq_query(query="SELECT `bucket`, `scope`, `collection`, `histogramKey` FROM `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_update_stats_function(self):
        histogram_expected = [
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "lower(city)", "scope": "_default"}
        ]
        update_stats = f"UPDATE STATISTICS `{self.bucket_name}`(LOWER(city))"
        try:
            stats = self.run_cbq_query(query=update_stats)
            # Check
            histogram = self.run_cbq_query(query="SELECT `bucket`, `scope`, `collection`, `histogramKey` FROM `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'")
            self.assertEqual(histogram['results'],histogram_expected)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()        

    def test_update_stats_array(self):
        histogram_expected = [
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "(all (array (_usv_1.utc) for _usv_1 in schedule end))", "scope": "_default"},      
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "(distinct (array (_usv_1.day) for _usv_1 in schedule end))", "scope": "_default"}
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
            histogram = self.run_cbq_query(query="SELECT `bucket`, `scope`, `collection`, `histogramKey` FROM `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'")
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
        histogram_query = "SELECT `bucket`, `scope`, `collection`, `histogramKey` FROM `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'"
        histogram_expected = [
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "city", "scope": "_default"}
        ]
        update_stats = "UPDATE STATISTICS `travel-sample`(city) WITH {'update_statistics_timeout':600}"
        try:
            stats = self.run_cbq_query(query=update_stats, query_params={'timeout':'600s'})
            # drop system collection
            self.run_cbq_query(query="DROP COLLECTION `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS`")
            self.sleep(1)
            # update stats after drop system collection
            stats = self.run_cbq_query(query=update_stats, query_params={'timeout':'600s'})
            # check stats
            histogram = self.run_cbq_query(query=histogram_query)
            self.assertEqual(histogram['results'],histogram_expected)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_drop_sys_scope(self):
        histogram_query = "SELECT `bucket`, `scope`, `collection`, `histogramKey` FROM `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'"
        histogram_expected = [
            {"bucket": "travel-sample", "collection": "_default", "histogramKey": "city", "scope": "_default"}
        ]
        update_stats = "UPDATE STATISTICS `travel-sample`(city) WITH {'update_statistics_timeout':600}"
        try:
            stats = self.run_cbq_query(query=update_stats, query_params={'timeout':'600s'})
            # drop scope
            self.run_cbq_query(query="DROP SCOPE `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`")
            self.sleep(1)
            # update stats after drop system scope
            stats = self.run_cbq_query(query=update_stats, query_params={'timeout':'600s'})
            # check stats
            histogram = self.run_cbq_query(query=histogram_query)
            self.assertEqual(histogram['results'],histogram_expected)
        except Exception as e:
            self.log.error(f"Update statistics failed: {e}")
            self.fail()

    def test_drop_sys_bucket(self):
        histogram_query = "SELECT `bucket`, `scope`, `collection`, `histogramKey` FROM `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'"
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
        results = self.run_cbq_query(query="BEGIN WORK", server=self.master)
        query_params = {'txid': results['results'][0]['txid']}
        error = "UPDATE_STATISTICS statement is not supported within the transaction"
        try:
            start = self.run_cbq_query(query="UPDATE STATISTICS `travel-sample`(city)", query_params=query_params, server=self.master)
            self.fail("Update statistics did not fail. Error expected: {0}".format(error))
        except CBQError as ex:
            self.assertTrue(str(ex).find(error) > 0)

    def test_negative_authorization(self):
        error = "User does not have credentials to run"
        queries = [
            "SELECT `bucket`, `scope`, `collection`, `histogramKey` FROM `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` data WHERE type = 'histogram'",
            "DELETE FROM `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS` WHERE type = 'histogram'",
            "DROP COLLECTION `N1QL_SYSTEM_BUCKET`.`N1QL_SYSTEM_SCOPE`.`N1QL_CBO_STATS`"
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






