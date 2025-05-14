import datetime
import json
import math
import random
import re
import time
import threading
from datetime import date

from deepdiff import DeepDiff
from membase.api.exception import CBQError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from collection.collections_n1ql_client import CollectionsN1QL

from .tuq import QueryTests


class QuerySanityTests(QueryTests):

    def setUp(self):
        super(QuerySanityTests, self).setUp()
        self.log.info("==============  QuerySanityTests setup has started ==============")
        self.index_to_be_created = self.input.param("index_to_be_created", '')
        self.query_to_be_run = self.input.param("query_to_be_run", '')
        if self.load_sample:
            self.rest.load_sample("travel-sample")
            self.wait_for_all_indexes_online()
        self.log.info("==============  QuerySanityTests setup has completed ==============")
        self.log_config_info()
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)
        self.collection_names = []
        self.creation_failure = []
        self.deletion_failure = []

    def suite_setUp(self):
        super(QuerySanityTests, self).suite_setUp()
        self.log.info("==============  QuerySanityTests suite_setup has started ==============")
        if self.input.param("fast_count", False):
            random_number = 23917
            shell = RemoteMachineShellConnection(self.master)
            shell.execute_cbworkloadgen(self.rest.username, self.rest.password, random_number, 100,
                                        self.default_bucket_name, 1024, '-j')
            self.run_cbq_query(
                query="INSERT INTO " + self.query_buckets[0] +
                      " ( key, value) VALUES ('pymc100205',{'name':'Sara','age':'30'})")

        self.log.info("==============  QuerySanityTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QuerySanityTests tearDown has started ==============")
        self.log.info("==============  QuerySanityTests tearDown has completed ==============")
        super(QuerySanityTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QuerySanityTests suite_tearDown has started ==============")
        self.log.info("==============  QuerySanityTests suite_tearDown has completed ==============")
        super(QuerySanityTests, self).suite_tearDown()
    ##############################################################################################
    #
    #   SIMPLE CHECKS
    ##############################################################################################

    def test_MB63998(self):
        self.run_cbq_query(query='UPSERT INTO default VALUES("k01", {"c1":10, "c3":30})')
        self.run_cbq_query(query='CREATE INDEX ix100 ON default (c1,IFMISSING(c2,"abc"), c2, c3)')
        explain_results = self.run_cbq_query(query= 'EXPLAIN SELECT META().id '
                                           'FROM default '
                                           'WHERE c1 = 10 AND IFMISSING(c2,"abc") > "" AND c3 = 30')
        self.assertTrue( "'low': 'null'" not in str(explain_results), f"Null should not be in the explain plan attached to c2 please check {explain_results}")
        results = self.run_cbq_query(query='SELECT META().id '
                                           'FROM default '
                                           'WHERE c1 = 10 AND IFMISSING(c2,"abc") > "" AND c3 = 30')
        self.assertEqual(results['results'],[{'id': 'k01'}])

    def test_error_message_collections_full(self):
        try:
            self.run_cbq_query(query="SELECT * FROM default:default.test.tes")
            self.fail("This query should error")
        except Exception as e:
            self.assertTrue("Keyspace not found in CB datastore: default:default.test.tes" in str(e), "The full path was not inside the error message! {0}".format(str(e)))
        try:
            self.run_cbq_query(query="SELECT * FROM default.test.tes")
            self.fail("This query should error")
        except Exception as e:
            self.assertTrue("Keyspace not found in CB datastore: default:default.test.tes" in str(e), "The full path was not inside the error message! {0}".format(str(e)))

    def test_error_message_collections_query_context(self):
        try:
            self.run_cbq_query(query="SELECT * FROM tes", query_context=self.query_context)
            self.fail("This query should error")
        except Exception as e:
            self.assertTrue("Keyspace not found in CB datastore: default:default.test.tes" in str(e),
                            "The full path was not inside the error message! {0}".format(str(e)))

    def test_error_namespace(self):
        try:
            results = self.run_cbq_query(query='select * from default.test.tes where name = "new hotel"', query_context='default:')
            self.fail("This query should error")
        except Exception as e:
            self.assertTrue("Keyspace not found in CB datastore: default:default.test.tes" in str(e), "The full path was not inside the error message! {0}".format(str(e)))

    def test_stats_collections(self):
        curl_output = self.shell.execute_command(
            "curl http://{0}:{1}/admin/stats".format(self.master.ip, self.n1ql_port))
        curl = json.loads(curl_output[0][0])
        starting_deletes = curl['deletes.count']
        starting_selects = curl['selects.count']
        starting_inserts = curl['inserts.count']

        try:
            self.run_cbq_query(query="CREATE PRIMARY INDEX ON default:default.{0}.{1}".format(self.scope, self.collections[0]))
            self.run_cbq_query(query="SELECT * FROM default:default.test.test1 b WHERE b.name = 'new hotel'")
            self.run_cbq_query(query="SELECT * FROM test1 b WHERE b.name = 'new hotel'",
                               query_context='default:default.test')
            self.run_cbq_query(
                query=('INSERT INTO default:default.{0}.{1}'.format(self.scope, self.collections[
                    0]) + '(KEY, VALUE) VALUES ("key6", { "type" : "hotel", "name" : "new hotel" })'))
            self.run_cbq_query(
                query='DELETE FROM default:default.test.test1 LIMIT 1')
            curl_output = self.shell.execute_command(
                "curl http://{0}:{1}/admin/stats".format(self.master.ip, self.n1ql_port))
            curl = json.loads(curl_output[0][0])
            self.assertEqual(curl['deletes.count'], starting_deletes + 1)
            self.assertEqual(curl['selects.count'], starting_selects + 2)
            self.assertEqual(curl['inserts.count'], starting_inserts + 1)

        except Exception as e:
            self.log.error("One of the queries failed or the stats were wrong: {0}".format(str(e)))
            self.fail()

    def test_stats_collections_prepareds(self):
        try:
            self.run_cbq_query(query="PREPARE p1 AS SELECT * FROM default:default.test.test1 b WHERE b.name = 'new hotel'")
            self.run_cbq_query(query="PREPARE p2 AS SELECT * FROM test1 b WHERE b.name = 'new hotel'", query_context='default:default.test')
            self.run_cbq_query(query="execute p2", query_context='default:default.test')
            self.run_cbq_query(query="execute p1", query_context='default:default.test')
            curl_output = self.shell.execute_command("curl http://{0}:{1}/admin/stats".format(self.master.ip, self.n1ql_port))
            curl = json.loads(curl_output[0][0])
            self.assertEqual(curl['prepared.count'], 2)
        finally:
            self.run_cbq_query(query="DELETE from system:prepareds")

    def test_escaped_identifiers(self):
        self.fail_if_no_buckets()
        queries_errors = {'SELECT name FROM {0} as bucket': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)
        for query_bucket in self.query_buckets:
            self.query = 'SELECT name FROM %s as `bucket` ORDER BY name' % query_bucket
            actual_result = self.run_cbq_query()

            expected_list = [{"name": doc["name"]} for doc in self.full_list]
            expected_list_sorted = sorted(expected_list, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_list_sorted)

    def test_prepared_encoded_rest(self):
        result_count = 1412
        if not self.sample_bucket:
            self.sample_bucket = 'beer-sample'
        self.rest.load_sample(self.sample_bucket)
        self.wait_for_buckets_status({self.sample_bucket: "healthy"}, 5, 120)
        self.wait_for_bucket_docs({self.sample_bucket: 7303}, 5, 120)
        self._wait_for_index_online('beer-sample', 'beer_primary')
        query_bucket = self.get_collection_name(self.sample_bucket)
        try:
            query = 'create index myidx on {0}(name,country,code) where (type="brewery")'.format(query_bucket)
            self.run_cbq_query(query)
            self._wait_for_index_online('beer-sample', 'myidx')
            query = 'prepare s1 from SELECT name, IFMISSINGORNULL(country,999), IFMISSINGORNULL(code,999)' \
                    ' FROM {0} WHERE type = "brewery" AND name IS NOT MISSING'.format(query_bucket)
            result = self.run_cbq_query(query)
            encoded_plan = '"' + result['results'][0]['encoded_plan'] + '"'
            for server in self.servers:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()

            time.sleep(20)
            server = None
            for server in self.servers:
                remote = RemoteMachineShellConnection(server)
                remote.start_server()
            self.wait_for_buckets_status({self.sample_bucket: "healthy"}, 5, 120)
            self.wait_for_bucket_docs({self.sample_bucket: 7303}, 5, 120)
            self._wait_for_index_online('beer-sample', 'beer_primary')
            cmd = "%s http://%s:%s/query/service -u %s:%s -H 'Content-Type: application/json' " \
                  "-d '{ \"prepared\": \"s1\", \"encoded_plan\": %s }'" % (
                      self.curl_path, server.ip, self.n1ql_port, self.username, self.password, encoded_plan)
            for server in self.servers:
                remote = RemoteMachineShellConnection(server)
                result = remote.execute_command(cmd)
                new_list = [string.strip() for string in result[0]]
                concat_string = ''.join(new_list)
                json_output = json.loads(concat_string)
                self.log.info(json_output['metrics']['resultCount'])
                self.assertEqual(json_output['metrics']['resultCount'], result_count)
        finally:
            self.rest.delete_bucket(self.sample_bucket)
            self.wait_for_bucket_delete(self.sample_bucket, 5, 120)

    # These bugs are not planned to be fixed therefore this test isnt in any confs
    '''MB-19887 and MB-24303: These queries were returning incorrect results with views.'''

    def test_views(self):
        self.fail_if_no_buckets()
        created_indexes = []
        try:
            idx = "ix1"
            self.query = "CREATE INDEX %s ON " % idx + self.query_buckets[0] + " (x,y) USING VIEW"
            self.run_cbq_query()
            created_indexes.append(idx)
            time.sleep(15)
            self.run_cbq_query("insert into " + self.query_buckets[0] + "  values ('k01',{'x':10})")
            result = self.run_cbq_query("select x,y from " + self.query_buckets[0] + "  where x > 3")
            self.assertTrue(result['results'][0] == {"x": 10})

            self.run_cbq_query('insert into ' + self.query_buckets[0] + '  values("k02", {"x": 20, "y": 20})')
            self.run_cbq_query('insert into ' + self.query_buckets[0] + '  values("k03", {"x": 30, "z": 30})')
            self.run_cbq_query('insert into ' + self.query_buckets[0] + '  values("k04", {"x": 40, "y": 40, "z": 40})')
            idx2 = "iv1"
            self.query = "CREATE INDEX %s ON " % idx2 + self.query_buckets[0] + " (x,y,z) USING VIEW"
            self.run_cbq_query()
            created_indexes.append(idx2)
            expected_result = [{'x': 10}, {'x': 20, 'y': 20}, {'x': 30, 'z': 30}, {'x': 40, 'y': 40, 'z': 40}]
            result = self.run_cbq_query('select x,y,z from ' + self.query_buckets[0] + '  use index (iv1 using view) '
                                                                                       'where x is not missing')
            self.assertTrue(result['results'] == expected_result)
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX %s ON %s USING VIEW" % (self.query_buckets[0], idx)
                self.run_cbq_query()

    def test_collections_meta_keyspace(self):
        results = self.run_cbq_query(query='select meta(d) from default:default.test.test1 as d')
        self.assertEqual(results['results'][0]['$1']['keyspace'], 'default:default.test.test1')

    def test_collections_meta_query_context(self):
        results = self.run_cbq_query(query='select meta(d) from test1 as d', query_context='default:default.test')
        self.assertEqual(results['results'][0]['$1']['keyspace'], 'default:default.test.test1')

    def test_collections_meta_keyspace_full(self):
        results = self.run_cbq_query(query='select meta(default.test.test1) from default:default.test.test1')
        self.assertEqual(results['results'][0]['$1']['keyspace'], 'default:default.test.test1')
        results = self.run_cbq_query(query='select meta(default:default.test.test1) from default:default.test.test1')
        self.assertEqual(results['results'][0]['$1']['keyspace'], 'default:default.test.test1')

    def test_collections_meta_id(self):
        results = self.run_cbq_query(query='select meta(d).id from default:default.test.test1 as d')
        self.assertEqual(results['results'], [{'id': 'key1'}, {'id': 'key2'}, {'id': 'key3'}, {'id': 'key4'}])
        results = self.run_cbq_query(query='select meta(d).id from test1 as d', query_context='default:default.test')
        self.assertEqual(results['results'], [{'id': 'key1'}, {'id': 'key2'}, {'id': 'key3'}, {'id': 'key4'}])

    def test_collections_meta_id_full_path(self):
        results = self.run_cbq_query(query='select meta(default.test.test1).id from default:default.test.test1')
        self.assertEqual(results['results'], [{'id': 'key1'}, {'id': 'key2'}, {'id': 'key3'}, {'id': 'key4'}])
        results = self.run_cbq_query(query='select meta(default:default.test.test1).id from default:default.test.test1')
        self.assertEqual(results['results'], [{'id': 'key1'}, {'id': 'key2'}, {'id': 'key3'}, {'id': 'key4'}])

    # No validation of query, just tests to see if query causes a panic in the log
    def test_MB63281(self):
        self.run_cbq_query(query='CREATE INDEX ix1 ON default(type, META().id)')
        self.run_cbq_query(query='UPSERT INTO default (KEY t.id, VALUE t) '
                                           'SELECT {"type":"doc", "id":"aa_"||TO_STR(d), '
                                           '"a1": ARRAY {"ac0":IMOD(d1,1), "ac1":d+IMOD(d1,10), "ac2":IMOD(d1,2), '
                                           '"ac3":IMOD(d1,3)} FOR d1 IN ARRAY_RANGE(0,50) END  } AS t '
                                           'FROM ARRAY_RANGE(0,10000) AS d')
        results = self.run_cbq_query(query='SELECT g.ac1, COUNT(1) FROM '
                                           '(SELECT ARRAY v FOR v IN a1 END AS a1 FROM default AS t WHERE type = "doc" '
                                           'AND META().id LIKE "aa_%" ) AS d UNNEST d.a1 AS u GROUP BY {u.ac1, u.ac2} AS g '
                                           'UNION SELECT g.ac1, COUNT(1) '
                                           'FROM (SELECT ARRAY v FOR v IN a1 END AS a1 FROM default AS t '
                                           'WHERE type = "doc" AND META().id LIKE "aa_%" ) AS d UNNEST d.a1 AS u '
                                           'GROUP BY {u.ac1, u.ac2} AS g;',query_params={"max_parallelism":2,"memory_quota":13000})
        self.assertTrue(results['status'] == "success")
    
    '''This test tests that the simplified version of an index returns correct results (which is no results) MB-56787'''
    def test_simplified_array(self):
        self.run_cbq_query(query='UPSERT INTO default VALUES ("f01",{"a1":[ {"f1":1}, {"f1":1}], "a2": 1})')
        self.run_cbq_query(query='CREATE INDEX ix40 ON default (ALL a2)')
        results = self.run_cbq_query(query='SELECT v FROM default AS d UNNEST d.a2 AS v where v > 0;')
        self.assertTrue(results['status'] == "success")
        self.assertTrue(results['results'] == [], f"We expect no results from this query, please check the results {results}")

    #MB-55851
    def test_mem_quota_join_subquery(self):
        for i in range(100):
            results = self.run_cbq_query(query=' SELECT COUNT(*) FROM '
                                               '(SELECT DISTINCT route.destinationairport, route.stops, route.airline, airline.name, airline.callsign '
                                               'FROM `travel-sample`.inventory.route '
                                               'LEFT JOIN `travel-sample`.inventory.airline ON route.airlineid = META(airline).id '
                                               'WHERE route.sourceairport = "SFO" AND route.stops = 0) t ;',
                                         query_params={"memory_quota": 100})
            self.assertTrue(results['status'] == "success")
            self.assertTrue(results['results'] == [{"$1": 249}])

    def test_collections_meta_cas(self):
        results = self.run_cbq_query(query='select meta(d).cas from default:default.test.test1 as d')
        self.assertEqual(results['metrics']['resultCount'], 4)
        results = self.run_cbq_query(query='select meta(d).cas from test1 as d', query_context='default:default.test')
        self.assertEqual(results['metrics']['resultCount'], 4)

    def test_collections_meta_cas_full(self):
        results = self.run_cbq_query(query='select meta(default.test.test1).cas from default:default.test.test1')
        self.assertEqual(results['metrics']['resultCount'], 4)
        results = self.run_cbq_query(query='select meta(default:default.test.test1).cas from default:default.test.test1')
        self.assertEqual(results['metrics']['resultCount'], 4)

    def test_collections_meta_expiration(self):
        results = self.run_cbq_query(query='select meta(d).expiration from default:default.test.test1 as d')
        self.assertEqual(results['results'], [{'expiration': 0}, {'expiration': 0}, {'expiration': 0}, {'expiration': 0}])
        results = self.run_cbq_query(query='select meta(d).expiration from test1 as d', query_context='default:default.test')
        self.assertEqual(results['results'], [{'expiration': 0}, {'expiration': 0}, {'expiration': 0}, {'expiration': 0}])

    def test_collections_meta_expiration_full(self):
        results = self.run_cbq_query(query='select meta(default.test.test1).expiration from default:default.test.test1')
        self.assertEqual(results['results'], [{'expiration': 0}, {'expiration': 0}, {'expiration': 0}, {'expiration': 0}])
        results = self.run_cbq_query(query='select meta(default:default.test.test1).expiration from default:default.test.test1')
        self.assertEqual(results['results'], [{'expiration': 0}, {'expiration': 0}, {'expiration': 0}, {'expiration': 0}])

    ''' This test will concurrently create and drop collections, and then give them 5 seconds to appear in system:keyspaces, if any one collection creation/deletion fails the whole test fails '''
    def test_create_drop_collections(self):
        self.collections_helper = CollectionsN1QL(self.master)

        self.collections_helper.create_scope(bucket_name="default", scope_name="scope1")
        self.collections_helper.create_scope(bucket_name="default", scope_name="scope2")

        for i in range(0, 100):
            thread_name = threading.Thread(name='run_collection', target=self.run_create_collection,args=("scope1", "collection1" + str(i)))
            thread2_name = threading.Thread(name='run_collection', target=self.run_create_collection,args=("scope2", "collection2" + str(i)))
            thread_name.start()
            thread2_name.start()

            self.sleep(1)
            if len(self.creation_failure) > 0:
                for collection in self.creation_failure:
                    self.log.error("Creation failed for collection: {0}".format(str(collection)))
                self.fail("Some collections failed to create! Check logs for more details")

            if len(self.collection_names) > 0:
                for collection in self.collection_names:
                    delete_thread = threading.Thread(name='drop_collection', target=self.run_drop_collection,
                                                   args=(collection[0], collection[1]))


                    delete_thread.start()
                    delete_thread.join()
                if len(self.deletion_failure) > 0:
                    for collection in self.deletion_failure:
                        self.log.error("Deletion failed for collection: {0}".format(str(collection)))
                    self.fail("Some collections failed to drop! Check logs for more details")

        retry_count = 100
        while retry_count > 0:
            if len(self.collection_names) > 0:
                for collection in self.collection_names:
                    delete_thread = threading.Thread(name='drop_collection', target=self.run_drop_collection,
                                                   args=(collection[0], collection[1]))


                    delete_thread.start()
                if len(self.deletion_failure) > 0:
                    for collection in self.deletion_failure:
                        self.log.error("Deletion failed for collection: {0}".format(str(collection)))
                    self.fail("Some collections failed to drop! Check logs for more details")
            self.sleep(.5)
            retry_count -= 1

        if len(self.creation_failure) > 0:
            for collection in self.creation_failure:
                self.log.error("Creation failed for collection: {0}".format(str(collection)))
            self.fail("Some collections failed to create! Check logs for more details")

        if len(self.deletion_failure) > 0:
            for collection in self.deletion_failure:
                self.log.error("Deletion failed for collection: {0}".format(str(collection)))
            self.fail("Some collections failed to drop! Check logs for more details")

    '''Create a collection and verify that within 5 seconds it is apart of system:keyspaces, print any errors that occur'''
    def run_create_collection(self, scope_name='',collection_name=''):
        retry_count = 5
        created = self.collections_helper.create_collection(bucket_name="default", scope_name=scope_name, collection_name=collection_name)
        while retry_count > 0:
            try:
                results = self.run_cbq_query('select * from system:keyspaces where name = "{0}"'.format(collection_name))
                if results['metrics']['resultCount'] == 1:
                    break
            except Exception as e:
                self.log.info(str(e))
                continue
            self.sleep(1)
            retry_count -= 1
        if not retry_count > 0:
            if collection_name not in self.creation_failure:
                self.creation_failure.append((scope_name, collection_name, "Entry not found in system:keyspaces"))
        if collection_name not in self.creation_failure:
            self.collection_names.append((scope_name, collection_name))
        return

    '''Drop a collection and verify that within 5 seconds it is removed from system:keyspaces'''
    def run_drop_collection(self, scope_name='', collection_name=''):
        retry_count = 5
        deleted = self.collections_helper.delete_collection(bucket_name="default", scope_name=scope_name, collection_name=collection_name)
        while retry_count > 0:
            try:
                results = self.run_cbq_query('select * from system:keyspaces where name = "{0}"'.format(collection_name))
                if results['metrics']['resultCount'] == 0:
                    break
            except Exception as e:
                if 'Keyspace not found in CB datastore' in str(e):
                    break
                continue
            self.sleep(1)
            retry_count -= 1
        if not retry_count > 0:
            if collection_name not in self.deletion_failure:
                self.deletion_failure.append((scope_name, collection_name, "Entry not deleted from system:keyspaces"))
        self.collection_names.remove((scope_name, collection_name))
        return


    ##############################################################################################
    #
    #   ALL
    ##############################################################################################

    def test_all(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'SELECT ALL job_title FROM %s ORDER BY job_title' % query_bucket
            actual_result = self.run_cbq_query()
            expected_result = [{"job_title": doc['job_title']}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result['results'], expected_result)

    def test_all_nested(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'SELECT ALL tasks_points.task1 FROM %s ' % query_bucket + \
                         'ORDER BY tasks_points.task1'
            actual_result = self.run_cbq_query()
            expected_result = [{"task1": doc['tasks_points']['task1']}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task1']))
            self._verify_results(actual_result['results'], expected_result)
            self.query = 'SELECT ALL skills[0] as skill' + \
                         ' FROM %s ORDER BY skills[0]' % query_bucket
            actual_result = self.run_cbq_query()
            expected_result = [{"skill": doc['skills'][0]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['skill']))
            self._verify_results(actual_result['results'], expected_result)

            if not self.analytics:
                self.query = 'SELECT ALL tasks_points.* FROM %s' % query_bucket
                actual_result = self.run_cbq_query()
                expected_result = [doc['tasks_points'] for doc in self.full_list]
                expected_result = sorted(expected_result, key=lambda doc: (doc['task1'], doc['task2']))
                actual_result = sorted(actual_result['results'], key=lambda doc: (doc['task1'], doc['task2']))
                self._verify_results(actual_result, expected_result)

    # This method is not used anywhere
    def set_indexer_pokemon_settings(self):
        projector_json = {"projector.dcp.numConnections": 1}
        moi_json = {"indexer.moi.useMemMgmt": True}
        server = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(server)
        rest.set_index_settings(projector_json)
        self.log.info("{0} set".format(projector_json))
        self.sleep(60)
        servers = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        for server in servers:
            remote = RemoteMachineShellConnection(server)
            remote.terminate_process(process_name="projector")
            self.sleep(60)
        self.sleep(60)
        # self.set_indexer_logLevel()
        self.loglevel = "info"
        self.log.info("Setting indexer log level to {0}".format(self.loglevel))
        server = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(server)

        rest.set_indexer_params("logLevel", self.loglevel)
        self.sleep(30)
        rest.set_index_settings(moi_json)
        self.log.info("{0} set".format(moi_json))
        self.sleep(30)

    def test_all_negative(self):
        queries_errors = {'SELECT ALL * FROM %s': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_keywords(self):
        queries_errors = {'SELECT description as DESC FROM %s order by DESC': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_distinct_negative(self):
        queries_errors = {'SELECT name FROM {0} ORDER BY DISTINCT name': ('syntax error', 3000),
                          'SELECT name FROM {0} GROUP BY DISTINCT name': ('syntax error', 3000),
                          'SELECT ANY tasks_points FROM {0}': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_any(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name, email FROM {0} d WHERE (ANY skill IN d.skills SATISFIES skill = 'skill2010' " \
                         "END) AND (ANY vm IN d.VMs SATISFIES vm.RAM = 5 END) AND  NOT (job_title = 'Sales') ORDER BY" \
                         " name".format(query_bucket)

            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name'], "email": doc["email"]}
                               for doc in self.full_list
                               if len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0 and
                               len([vm for vm in doc["VMs"]
                                    if vm["RAM"] == 5]) > 0 and
                               doc["job_title"] != 'Sales']
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    # This test isnt used anywhere
    def test_any_within(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name, email FROM %s " % query_bucket + \
                         " d WHERE ANY vm within d.VMs SATISFIES vm.RAM = 5 END"

            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name'], "email": doc["email"]}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"]
                                       if vm["RAM"] == 5]) > 0]
            self._verify_results(actual_result['results'], expected_result)

    def test_any_no_in_clause(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name, email FROM %s d WHERE " % query_bucket + \
                         "(ANY skill IN d.skills SATISFIES skill = 'skill2010' end)"  + \
                         "AND (ANY vm IN d.VMs SATISFIES vm.RAM = 5 end) " + \
                         "AND  NOT (job_title = 'Sales') ORDER BY name"

            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name'], "email": doc["email"]}
                               for doc in self.full_list
                               if len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0 and
                               len([vm for vm in doc["VMs"]
                                    if vm["RAM"] == 5]) > 0 and
                               doc["job_title"] != 'Sales']
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_any_no_in_clause(self):
        self.fail_if_no_buckets()
        if self.monitoring:
            self.query = "select * from system:prepareds"
            result = self.run_cbq_query()
            self.assertTrue(result['metrics']['resultCount'] == 0)
        for query_bucket in self.query_buckets:
            query_bucket = self.get_collection_name(query_bucket)
            self.query = "SELECT name, email FROM %s as d WHERE " % query_bucket + \
                         "(ANY skill IN d.skills SATISFIES skill = 'skill2010' end) AND" \
                         " (ANY vm IN d.VMs SATISFIES vm.RAM = 5 end) " \
                         "AND  NOT (job_title = 'Sales') ORDER BY name"
            self.prepared_common_body()
        if self.monitoring:
            self.query = "select * from system:prepareds"
            result = self.run_cbq_query()
            self.assertTrue(result['metrics']['resultCount'] == 1)

    def test_any_external(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'SELECT name FROM %s WHERE ' % query_bucket + \
                         'ANY x IN ["Support", "Management"] SATISFIES job_title = x END ' + \
                         'ORDER BY name'

            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name']}
                               for doc in self.full_list
                               if doc["job_title"] in ["Support", "Management"]]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_every(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s d WHERE " % query_bucket + \
                         "(EVERY vm IN d.VMs SATISFIES CEIL(vm.memory) > 5 END) ORDER BY name"

            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name']}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"]
                                       if math.ceil(vm['memory']) > 5]) == len(doc["VMs"])]

            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_satisfy_negative(self):
        queries_errors = {'SELECT name FROM %s WHERE ANY x IN 123 SATISFIES job_title = x END': ('syntax error', 3000),
                          'SELECT name FROM %s WHERE ANY x IN ["Sales"] SATISFIES job_title = x': (
                              'syntax error', 3000),
                          'SELECT job_title FROM %s WHERE ANY job_title IN ["Sales"] SATISFIES job_title = job_title '
                          'END': (
                              'syntax error', 3000),
                          'SELECT job_title FROM %s WHERE EVERY ANY x IN ["Sales"] SATISFIES x = job_title END': (
                              'syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_check_is_isnot_negative(self):
        queries_errors = {'SELECT * FROM %s WHERE name is foo': ('syntax error', 3000),
                          'SELECT * FROM %s WHERE name is not foo': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_array(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT ARRAY vm.memory FOR vm IN VMs END AS vm_memories" + \
                         " FROM %s WHERE VMs IS NOT NULL " % query_bucket
            if self.analytics:
                self.query = 'SELECT (SELECT VALUE vm.memory FROM VMs AS vm) AS vm_memories FROM %s WHERE VMs IS NOT ' \
                             'NULL ' % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['vm_memories']))
            expected_result = [{"vm_memories": [vm["memory"] for vm in doc['VMs']]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['vm_memories']))
            self._verify_results(actual_result, expected_result)

    # This test is not used anywhere
    def test_array_objects(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT VMs[*].os from %s" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            expected_result = [{"os": [vm["os"] for vm in doc['VMs']]}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_arrays_negative(self):
        queries_errors = {'SELECT ARRAY vm.memory FOR vm IN 123 END AS vm_memories FROM %s': ('syntax error', 3000),
                          'SELECT job_title, array_agg(name)[:5] as names FROM %s': ('syntax error', 3000),
                          'SELECT job_title, array_agg(name)[-20:-5] as names FROM %s': ('syntax error', 3000),
                          'SELECT job_title, array_agg(name)[a:-b] as names FROM %s': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    # This test is not used anywhere
    def test_slicing(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_agg(name)[0:5] as names" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_result = self.run_cbq_query()
            for item in actual_result['results']:
                self.log.info("Result: %s" % actual_result['results'])
                self.assertTrue(len(item['names']) <= 5, "Slicing doesn't work")

            self.query = "SELECT job_title, array_agg(name)[5:] as names" + \
                         " FROM %s GROUP BY job_title" % query_bucket
            actual_result = self.run_cbq_query()

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_list = [{"job_title": group,
                              "names": [x["name"] for x in self.full_list
                                        if x["job_title"] == group]}
                             for group in tmp_groups]
            expected_item = None
            for item in actual_result['results']:
                for tmp_item in expected_list:
                    if item['job_title'] == tmp_item['job_title']:
                        expected_item = tmp_item
                self.log.info("Result: %s" % actual_result['results'])
                self.assertTrue(len(item['names']) == len(expected_item['names']),
                                "Slicing doesn't work")

            self.query = "SELECT job_title, array_agg(name)[5:10] as names" + \
                         " FROM %s GROUP BY job_title" % query_bucket
            actual_result = self.run_cbq_query()
            for item in actual_result['results']:
                self.log.info("Result: %s" % actual_result['results'])
                self.assertTrue(len(item['names']) <= 5, "Slicing doesn't work")

    def test_count_prepare(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "create index idx_cover on %s(join_mo,join_day) where join_mo > 7" % query_bucket
            self.run_cbq_query()
            self.query = "SELECT count(*) AS cnt from %s " % query_bucket + \
                         "WHERE join_mo > 7 and join_day > 1"
            self.prepared_common_body()

    def test_leak_goroutine(self):
        shell = RemoteMachineShellConnection(self.master)
        for i in range(20):
            cmd = 'curl http://%s:6060/debug/pprof/goroutine?debug=2 | grep NewLexer' % self.master.ip
            o = shell.execute_command(cmd)
            new_curl = json.dumps(o)
            string_curl = json.loads(new_curl)
            self.assertTrue("curl: (7) couldn't connect to host" == str(string_curl[1][1]))
            cmd = "curl http://%s:8093/query/service -d 'statement=select * from 1+2+3'" % self.master.ip
            o = shell.execute_command(cmd)
            new_curl = json.dumps(o)
            string_curl = json.loads(new_curl)
            self.assertTrue(len(string_curl) == 2)
            cmd = 'curl http://%s:6060/debug/pprof/goroutine?debug=2 | grep NewLexer' % self.master.ip
            o = shell.execute_command(cmd)
            new_curl = json.dumps(o)
            string_curl = json.loads(new_curl)
            self.assertTrue(len(string_curl) == 2)

    ##############################################################################################
    #
    #   COUNT
    ##############################################################################################

    def test_fast_count(self):
        if self.index_to_be_created:
            if "EQUALS" in self.index_to_be_created:
                self.index_to_be_created = self.index_to_be_created.replace("EQUALS", "=")
            self.run_cbq_query(query=self.index_to_be_created)
        self.wait_for_all_indexes_online()
        try:
            if "STAR" in self.query_to_be_run:
                self.query_to_be_run = self.query_to_be_run.replace("STAR", "*")
                if "EQUALS" in self.query_to_be_run:
                    self.query_to_be_run = self.query_to_be_run.replace("EQUALS", "=")
            # add use primary index hint to the query to compare the GSI query to the primary index query
            actual_results = self.run_cbq_query(query=self.query_to_be_run)
            split_query = self.query_to_be_run.split("where")
            primary_index_query = split_query[0] + "USE INDEX(`#primary`) where" + split_query[1]
            expected_results = self.run_cbq_query(query=primary_index_query)
            self.assertEqual(actual_results['results'][0]['$1'], expected_results['results'][0]['$1'])
        finally:
            if self.load_sample:
                query_bucket = self.get_collection_name('`travel-sample`')
                self.run_cbq_query(query="DROP INDEX idx_flight_stops ON {0}".format(query_bucket))
            else:
                query_bucket = self.get_collection_name(self.default_bucket_name)
                self.run_cbq_query(query="DROP INDEX idx1 ON {0}".format(query_bucket))

    def test_primary_count(self):
        # number of documents inserted at the beginning of suite_setup
        random_number = 23918
        RemoteMachineShellConnection(self.master)
        if "STAR" in self.query_to_be_run:
            self.query_to_be_run = self.query_to_be_run.replace("STAR", "*")
        actual_results = self.run_cbq_query(query=self.query_to_be_run)
        self.assertEqual(actual_results['results'][0]['$1'], random_number + self.docs_per_day * 2016)

    ##############################################################################################
    #
    #   LIKE
    ##############################################################################################

    def test_like(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM {0} WHERE job_title LIKE 'S%' ORDER BY name".format(query_bucket)
            actual_result = self.run_cbq_query()

            expected_result = [{"name": doc['name']} for doc in self.full_list
                               if doc["job_title"].startswith('S')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title LIKE '%u%' ORDER BY name".format(query_bucket)
            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name']} for doc in self.full_list
                               if doc["job_title"].find('u') != -1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title NOT LIKE 'S%' ORDER BY name".format(query_bucket)
            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name']} for doc in self.full_list
                               if not doc["job_title"].startswith('S')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title NOT LIKE '_ales' ORDER BY name".format(query_bucket)
            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name']} for doc in self.full_list
                               if not (doc["job_title"].endswith('ales') and
                                       len(doc["job_title"]) == 5)]
            self.query = "SELECT name FROM {0} WHERE reverse(job_title) NOT LIKE 'sela_' " \
                         "ORDER BY name".format(query_bucket)
            actual_result1 = self.run_cbq_query()

            self.assertEqual(actual_result1['results'], actual_result['results'])
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_like_negative(self):
        queries_errors = {"SELECT tasks_points FROM {0} WHERE tasks_points.* LIKE '_1%'": ('syntax error', 3000)}
        self.negative_common_body(queries_errors)
        queries_errors = {"SELECT tasks_points FROM {0} WHERE REVERSE(tasks_points.*) LIKE '%1_'": ('syntax error',
                                                                                                    3000)}
        self.negative_common_body(queries_errors)

    def test_like_any(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name, email FROM %s d WHERE (ANY vm IN d.VMs" % query_bucket + \
                         " SATISFIES vm.os LIKE '%bun%'" + \
                         "END) AND (ANY skill IN d.skills " + \
                         "SATISFIES skill = 'skill2010' END) ORDER BY name"
            actual_result = self.run_cbq_query()

            expected_result = [{"name": doc['name'], "email": doc["email"]}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"]
                                       if vm["os"].find('bun') != -1]) > 0 and
                               len([skill for skill in doc["skills"]
                                    if skill == 'skill2010']) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_like_every(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name, email FROM %s d  WHERE (EVERY vm IN d.VMs " % query_bucket + \
                         "SATISFIES vm.os NOT LIKE '%cent%' END)" + \
                         " AND (ANY skill IN d.skills SATISFIES skill =" + \
                         " 'skill2010' END) ORDER BY name"
            actual_result = self.run_cbq_query()

            expected_result = [{"name": doc['name'], "email": doc["email"]}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"]
                                       if vm["os"].find('cent') == -1]) == len(doc["VMs"]) and
                               len([skill for skill in doc["skills"]
                                    if skill == 'skill2010']) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_like_aliases(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name AS NAME from %s " % query_bucket + \
                         "AS EMPLOYEE where EMPLOYEE.name LIKE '_mpl%' ORDER BY name"
            actual_result = self.run_cbq_query()
            self.query = "select name AS NAME from %s " % query_bucket + \
                         "AS EMPLOYEE where reverse(EMPLOYEE.name) LIKE '%lpm_' ORDER BY name"
            actual_result1 = self.run_cbq_query()
            self.assertEqual(actual_result['results'], actual_result1['results'])
            expected_result = [{"NAME": doc['name']} for doc in self.full_list
                               if doc["name"].find('mpl') == 1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['NAME']))
            self._verify_results(actual_result['results'], expected_result)

    def test_like_wildcards(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT email FROM %s WHERE email " % query_bucket + \
                         "LIKE '%@%.%' ORDER BY email"
            actual_result = self.run_cbq_query()
            self.query = "SELECT email FROM %s WHERE reverse(email) " % query_bucket + \
                         "LIKE '%.%@%' ORDER BY email"
            actual_result1 = self.run_cbq_query()
            self.assertEqual(actual_result['results'], actual_result1['results'])

            expected_result = [{"email": doc['email']} for doc in self.full_list
                               if re.match(r'.*@.*\..*', doc['email'])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['email']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT email FROM %s WHERE email" % query_bucket + \
                         " LIKE '%@%.h' ORDER BY email"
            actual_result = self.run_cbq_query()
            expected_result = []
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_like_wildcards(self):
        self.fail_if_no_buckets()
        if self.monitoring:
            self.query = "select * from system:prepareds"
            result = self.run_cbq_query()
            self.assertTrue(result['metrics']['resultCount'] == 0)
        for query_bucket in self.query_buckets:
            self.query = "SELECT email FROM %s WHERE email " % query_bucket + \
                         "LIKE '%@%.%' ORDER BY email"
            self.prepared_common_body()
        if self.monitoring:
            self.query = "select * from system:prepareds"
            result = self.run_cbq_query()
            self.assertTrue(result['metrics']['resultCount'] == 1)

    def test_between(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM {0} WHERE join_mo BETWEEN 1 AND 6 ORDER BY name".format(query_bucket)
            actual_result = self.run_cbq_query()

            expected_result = [{"name": doc['name']} for doc in self.full_list if 1 <= doc["join_mo"] <= 6]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE join_mo NOT BETWEEN 1 AND 6 ORDER BY name".format(query_bucket)
            actual_result = self.run_cbq_query()

            expected_result = [{"name": doc['name']} for doc in self.full_list if not 1 <= doc["join_mo"] <= 6]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_between_negative(self):
        queries_errors = {'SELECT name FROM %s WHERE join_mo BETWEEN 1 AND -10 ORDER BY name': ('syntax error', 3000),
                          'SELECT name FROM %s WHERE join_mo BETWEEN 1 AND a ORDER BY name': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    ##############################################################################################
    #
    #   GROUP BY
    ##############################################################################################

    def test_group_by(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT tasks_points.task1 AS task from %s " % query_bucket + \
                         "WHERE join_mo>7 GROUP BY tasks_points.task1 " + \
                         "ORDER BY tasks_points.task1"
            if self.analytics:
                self.query = "SELECT d.tasks_points.task1 AS task from %s d " % query_bucket + \
                             "WHERE d.join_mo>7 GROUP BY d.tasks_points.task1 " + \
                             "ORDER BY d.tasks_points.task1"
            actual_result = self.run_cbq_query()

            expected_result = [{"task": doc['tasks_points']["task1"]}
                               for doc in self.full_list
                               if doc["join_mo"] > 7]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result['results'], expected_result)

    def test_group_by_having(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "from %s WHERE join_mo>7 GROUP BY tasks_points.task1 " % query_bucket + \
                         "HAVING COUNT(tasks_points.task1) > 0 SELECT tasks_points.task1 " + \
                         "AS task ORDER BY tasks_points.task1"
            actual_result = self.run_cbq_query()

            expected_result = [{"task": doc['tasks_points']["task1"]}
                               for doc in self.full_list
                               if doc["join_mo"] > 7]
            expected_result = [doc for doc in expected_result
                               if expected_result.count(doc) > 0]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result['results'], expected_result)

    def test_group_by_aggr_fn(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT tasks_points.task1 AS task from %s " % query_bucket + \
                         "WHERE join_mo>7 GROUP BY tasks_points.task1 " + \
                         "HAVING COUNT(tasks_points.task1) > 0 AND " + \
                         "(MIN(join_day)=1 OR MAX(join_yr=2011)) " + \
                         "ORDER BY tasks_points.task1"
            actual_result = self.run_cbq_query()

            if self.analytics:
                self.query = "SELECT d.tasks_points.task1 AS task from %s d " % query_bucket + \
                             "WHERE d.join_mo>7 GROUP BY d.tasks_points.task1 " + \
                             "HAVING COUNT(d.tasks_points.task1) > 0 AND " + \
                             "(MIN(d.join_day)=1 OR MAX(d.join_yr=2011)) " + \
                             "ORDER BY d.tasks_points.task1"

            tmp_groups = {doc['tasks_points']["task1"] for doc in self.full_list}
            expected_result = [{"task": group} for group in tmp_groups
                               if [doc['tasks_points']["task1"]
                                   for doc in self.full_list].count(group) > 0 and
                               (min([doc["join_day"] for doc in self.full_list
                                     if doc['tasks_points']["task1"] == group]) == 1 or
                                max([doc["join_yr"] for doc in self.full_list
                                     if doc['tasks_points']["task1"] == group]) == 2011)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_group_by_aggr_fn(self):
        self.fail_if_no_buckets()
        if self.monitoring:
            self.query = "select * from system:prepareds"
            result = self.run_cbq_query()
            self.assertTrue(result['metrics']['resultCount'] == 0)
        for query_bucket in self.query_buckets:
            self.query = "SELECT tasks_points.task1 AS task from %s " % query_bucket + \
                         "WHERE join_mo>7 GROUP BY tasks_points.task1 " + \
                         "HAVING COUNT(tasks_points.task1) > 0 AND " + \
                         "(MIN(join_day)=1 OR MAX(join_yr=2011)) " + \
                         "ORDER BY tasks_points.task1"
            self.prepared_common_body()
        if self.monitoring:
            self.query = "select * from system:prepareds"
            result = self.run_cbq_query()
            self.assertTrue(result['metrics']['resultCount'] == 1)
            self.query = "delete from system:prepareds"
            self.run_cbq_query()
            self.query = "select * from system:prepareds"
            result = self.run_cbq_query()
            self.assertTrue(result['metrics']['resultCount'] == 0)

    def test_group_by_satisfy(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, AVG(test_rate) as avg_rate FROM %s d " % query_bucket + \
                         "WHERE (ANY skill IN d.skills SATISFIES skill = 'skill2010' end) " + \
                         "AND (ANY vm IN d.VMs SATISFIES vm.RAM = 5 end) " + \
                         "GROUP BY job_title ORDER BY job_title"

            if self.analytics:
                self.query = "SELECT d.job_title, AVG(d.test_rate) as avg_rate FROM %s d " % query_bucket + \
                             "WHERE (ANY skill IN d.skills SATISFIES skill = 'skill2010' end) " + \
                             "AND (ANY vm IN d.VMs SATISFIES vm.RAM = 5 end) " + \
                             "GROUP BY d.job_title ORDER BY d.job_title"

            actual_result = self.run_cbq_query()

            tmp_groups = {doc["job_title"] for doc in self.full_list}
            expected_result = [{"job_title": doc['job_title'],
                                "test_rate": doc["test_rate"]}
                               for doc in self.full_list
                               if 'skill2010' in doc["skills"] and
                               len([vm for vm in doc["VMs"] if vm["RAM"] == 5]) > 0]
            expected_result = [{"job_title": group,
                                "avg_rate": math.fsum([doc["test_rate"]
                                                       for doc in expected_result
                                                       if doc["job_title"] == group]) /
                                            len([doc["test_rate"] for doc in expected_result
                                                 if doc["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result['results'], expected_result)

    def test_group_by_negative(self):
        queries_errors = {"SELECT tasks_points from {0} WHERE tasks_points.task2>3" +
                          " GROUP BY tasks_points.*":
                              ('syntax error', 3000),
                          "from {0} WHERE join_mo>7 GROUP BY tasks_points.task1 " +
                          "SELECT tasks_points.task1 AS task HAVING COUNT(tasks_points.task1) > 0":
                              ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    # This test has no usages anywhere
    def test_groupby_first(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select job_title, (FIRST p FOR p IN ARRAY_AGG(name) END) as names from {0} group by " \
                         "job_title order by job_title ".format(query_bucket)
            actual_result = self.run_cbq_query()
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_list = [{"job_title": group,
                              "names": ([x["name"] for x in self.full_list
                                         if x["job_title"] == group][0])}
                             for group in tmp_groups]
            expected_result = self.sort_nested_list(expected_list)
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self.assertTrue(actual_result['results'] == expected_result)

    ##############################################################################################
    #
    #   SCALAR FN
    ##############################################################################################

    def test_ceil(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, ceil(test_rate) as rate from %s" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['rate']))

            expected_result = [{"name": doc['name'], "rate": math.ceil(doc['test_rate'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where ceil(test_rate) > 5" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name']))
            expected_result = [{"name": doc['name']} for doc in self.full_list
                               if math.ceil(doc['test_rate']) > 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_floor(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, floor(test_rate) as rate from %s" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['rate']))

            expected_result = [{"name": doc['name'], "rate": math.floor(doc['test_rate'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where floor(test_rate) > 5" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name']))
            expected_result = [{"name": doc['name']} for doc in self.full_list
                               if math.floor(doc['test_rate']) > 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where floor(job_title) > 5" % query_bucket
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], [])

    def test_greatest(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, GREATEST(skills[0], skills[1]) as SKILL from %s" % (
                query_bucket)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['SKILL']))

            expected_result = [{"name": doc['name'], "SKILL":
                (doc['skills'][0], doc['skills'][1])[doc['skills'][0] < doc['skills'][1]]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['SKILL']))
            self._verify_results(actual_result, expected_result)

    def test_least(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, LEAST(skills[0], skills[1]) as SKILL from %s" % (
                query_bucket)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['SKILL']))

            expected_result = [{"name": doc['name'], "SKILL":
                (doc['skills'][0], doc['skills'][1])[doc['skills'][0] > doc['skills'][1]]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['SKILL']))
            self._verify_results(actual_result, expected_result)

    def test_meta(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'SELECT distinct name FROM %s d WHERE META(d).`type` = "json"' % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name']))

            expected_result = [{"name": doc['name']} for doc in self.full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT distinct name FROM %s WHERE META().id IS NOT NULL" % query_bucket
            actual_result = self.run_cbq_query()

            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_meta_like(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'SELECT name FROM %s d WHERE META(d).id LIKE "%s"' % (query_bucket, "query%")
            actual_result = self.run_cbq_query()

            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name']))

            expected_result = [{"name": doc['name']} for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_prepared_meta_like(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'SELECT name FROM %s d WHERE META(d).id ' % query_bucket + 'LIKE "employee%"'
            self.prepared_common_body()
            if self.monitoring:
                self.query = 'select * from system:completed_requests'
                result = self.run_cbq_query()
                request_id = result['request_id']
                self.query = 'delete from system:completed_requests where request_id  =  "%s"' % request_id
                self.run_cbq_query()
                result = self.run_cbq_query(
                    'select * from system:completed_requests where request_id  =  "%s"' % request_id)
                self.assertTrue(result['metrics']['resultCount'] == 0)

    def test_meta_flags(self):
        self.fail_if_no_buckets()
        self.sleep(5)
        for query_bucket in self.query_buckets:
            self.query = 'SELECT DISTINCT META().flags as flags FROM %s' % query_bucket
            actual_result = self.run_cbq_query()
            expected_result = [{"flags": self.item_flag}]
            self._verify_results(actual_result['results'], expected_result)

    def test_long_values(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'insert into %s values("k051", { "id":-9223372036854775808  } )' % query_bucket
            self.run_cbq_query()
            self.query = 'insert into %s values("k031", { "id":-9223372036854775807  } )' % query_bucket
            self.run_cbq_query()
            self.query = 'insert into %s values("k021", { "id":1470691191458562048 } )' % query_bucket
            self.run_cbq_query()
            self.query = 'insert into %s values("k011", { "id":9223372036854775807 } )' % query_bucket
            self.run_cbq_query()
            self.query = 'insert into %s values("k041", { "id":9223372036854775808 } )' % query_bucket
            self.run_cbq_query()

            self.query = 'select * from ' + query_bucket + ' d where meta().id = "{0}"'.format("k051")
            actual_result = self.run_cbq_query()
            print("k051 results is {0}".format(actual_result['results'][0]['d']))
            # self.assertEqual(actual_result['results'][0]["default"],{'id': -9223372036854775808})
            self.query = 'select * from ' + query_bucket + ' d where meta().id = "{0}"'.format("k031")
            actual_result = self.run_cbq_query()
            print("k031 results is {0}".format(actual_result['results'][0]['d']))
            # self.assertEqual(actual_result['results'][0]["default"],{'id': -9223372036854775807})
            self.query = 'select * from ' + query_bucket + ' d where meta().id = "{0}"'.format("k021")
            actual_result = self.run_cbq_query()
            print("k021 results is {0}".format(actual_result['results'][0]['d']))
            # self.assertEqual(actual_result['results'][0]["default"],{'id': 1470691191458562048})
            self.query = 'select * from ' + query_bucket + ' d where meta().id = "{0}"'.format("k011")
            actual_result = self.run_cbq_query()
            print("k011 results is {0}".format(actual_result['results'][0]['d']))
            # self.assertEqual(actual_result['results'][0]["default"],{'id': 9223372036854775807})
            self.query = 'select * from ' + query_bucket + ' d where meta().id = "{0}"'.format("k041")
            actual_result = self.run_cbq_query()
            print("k041 results is {0}".format(actual_result['results'][0]['d']))
            # self.assertEqual(actual_result['results'][0]["default"],{'id':  9223372036854776000L})
            self.query = 'delete from ' + query_bucket + ' where meta().id in ["k051","k021","k011","k041","k031"]'
            self.run_cbq_query()

    def test_meta_cas(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'select meta().cas from {0} order by meta().id limit 10'.format(query_bucket)
            actual_result = self.run_cbq_query()
            print(actual_result)

    def test_meta_negative(self):
        queries_errors = {'SELECT distinct name FROM %s WHERE META().type = "json"': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_length(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'Select name, email from %s where LENGTH(job_title) = 5' % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name']))

            expected_result = [{"name": doc['name'], "email": doc['email']}
                               for doc in self.full_list
                               if len(doc['job_title']) == 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_upper(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'SELECT DISTINCT UPPER(job_title) as JOB from %s' % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['JOB']))

            expected_result = [{"JOB": doc['job_title'].upper()}
                               for doc in self.full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['JOB']))
            self._verify_results(actual_result, expected_result)

    def test_lower(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT distinct email from %s" % query_bucket + \
                         " WHERE LOWER(job_title) < 't'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['email']))

            expected_result = [{"email": doc['email'].lower()}
                               for doc in self.full_list
                               if doc['job_title'].lower() < 't']
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['email']))
            self._verify_results(actual_result, expected_result)

    def test_round(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, round(test_rate) as rate from %s" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['rate']))

            expected_result = [{"name": doc["name"], "rate": round(doc['test_rate'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

    def test_trunc(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, TRUNC(test_rate, 0) as rate from %s" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['rate']))

            expected_result = [{"name": doc["name"], "rate": math.trunc(doc['test_rate'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

    def test_first(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, FIRST vm.os for vm in VMs end as OS from %s" % (
                query_bucket)
            if self.analytics:
                self.query = "select name, VMs[0].os as OS from %s" % query_bucket

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['OS']))

            expected_result = [{"name": doc["name"], "OS": doc['VMs'][0]["os"]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['OS']))
            self._verify_results(actual_result, expected_result)

    ##############################################################################################
    #
    #   Substring
    ##############################################################################################

    def test_substr(self):
        self.fail_if_no_buckets()
        indices_to_test = [-100, -2, -1, 0, 1, 2, 100]
        for index in indices_to_test:
            for query_bucket in self.query_buckets:
                self.query = "select name, SUBSTR(email, %s) as DOMAIN from %s" % (
                    str(index), query_bucket)
                query_result = self.run_cbq_query()
                query_docs = query_result['results']
                sorted_query_docs = sorted(query_docs,
                                           key=lambda doc: (doc['name'], doc['DOMAIN']))

                expected_result = [{"name": doc["name"],
                                    "DOMAIN": self.expected_substr(doc['email'], 0, index)}
                                   for doc in self.full_list]
                sorted_expected_result = sorted(expected_result, key=lambda doc: (
                    doc['name'], doc['DOMAIN']))

                self._verify_results(sorted_query_docs, sorted_expected_result)

    def test_substr0(self):
        self.fail_if_no_buckets()
        indices_to_test = [-100, -2, -1, 0, 1, 2, 100]
        for index in indices_to_test:
            for query_bucket in self.query_buckets:
                self.query = "select name, SUBSTR0(email, %s) as DOMAIN from %s" % (
                    str(index), query_bucket)
                query_result = self.run_cbq_query()
                query_docs = query_result['results']
                sorted_query_docs = sorted(query_docs,
                                           key=lambda doc: (doc['name'], doc['DOMAIN']))

                expected_result = [{"name": doc["name"],
                                    "DOMAIN": self.expected_substr(doc['email'], 0, index)}
                                   for doc in self.full_list]
                sorted_expected_result = sorted(expected_result, key=lambda doc: (
                    doc['name'], doc['DOMAIN']))

                self._verify_results(sorted_query_docs, sorted_expected_result)

    def test_substr1(self):
        self.fail_if_no_buckets()
        indices_to_test = [-100, -2, -1, 0, 1, 2, 100]
        for index in indices_to_test:
            for query_bucket in self.query_buckets:
                self.query = "select name, SUBSTR1(email, %s) as DOMAIN from %s" % (
                    str(index), query_bucket)
                query_result = self.run_cbq_query()
                query_docs = query_result['results']
                sorted_query_docs = sorted(query_docs,
                                           key=lambda doc: (doc['name'], doc['DOMAIN']))

                expected_result = [{"name": doc["name"],
                                    "DOMAIN": self.expected_substr(doc['email'], 1, index)}
                                   for doc in self.full_list]
                sorted_expected_result = sorted(expected_result, key=lambda doc: (
                    doc['name'], doc['DOMAIN']))

                self._verify_results(sorted_query_docs, sorted_expected_result)

    ##############################################################################################
    #
    #   AGGR FN
    ##############################################################################################

    def test_agg_counters(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            vals = []
            keys = []
            for i in range(10):
                new_val = random.randint(0, 99)
                new_counter = "counter_US" + str(i)
                vals.append(new_val)
                keys.append(new_counter)
                self.query = 'INSERT INTO %s VALUES ("%s",%s)' % (query_bucket, new_counter, new_val)
                self.run_cbq_query()

            self.query = 'SELECT sum(d) FROM %s d USE KEYS %s;' % (query_bucket, str(keys))
            actual_results = self.run_cbq_query()
            self.assertEqual(actual_results['results'][0]['$1'], sum(vals))

            self.query = 'SELECT avg(d) FROM %s d USE KEYS %s;' % (query_bucket, str(keys))
            actual_results = self.run_cbq_query()
            self.assertEqual(actual_results['results'][0]['$1'], float(sum(vals)) / max(len(vals), 1))

            self.query = 'DELETE FROM %s USE KEYS %s;' % (query_bucket, str(keys))
            actual_results = self.run_cbq_query()
            self.assertEqual(actual_results['results'], [])

    def test_sum(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT join_mo, SUM(tasks_points.task1) as points_sum" + \
                         " FROM %s WHERE join_mo < 5 GROUP BY join_mo " % query_bucket + \
                         "ORDER BY join_mo"
            if self.analytics:
                self.query = "SELECT d.join_mo, SUM(d.tasks_points.task1) as points_sum" + \
                             " FROM %s d WHERE d.join_mo < 5 GROUP BY d.join_mo " % query_bucket + \
                             "ORDER BY d.join_mo"
            actual_result = self.run_cbq_query()

            tmp_groups = {doc['join_mo'] for doc in self.full_list
                          if doc['join_mo'] < 5}
            expected_result = [{"join_mo": group,
                                "points_sum": int(math.fsum([doc['tasks_points']['task1']
                                                             for doc in self.full_list
                                                             if doc['join_mo'] == group]))}
                               for group in tmp_groups]
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT join_mo, SUM(test_rate) as rate FROM %s " % query_bucket + \
                         "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                         "HAVING SUM(employees.test_rate) > 0 and " + \
                         "SUM(test_rate) < 100000"
            if self.analytics:
                self.query = "SELECT d.join_mo, SUM(d.test_rate) as rate FROM %s d " % query_bucket + \
                             " WHERE d.job_title='Sales' GROUP BY d.join_mo " + \
                             "HAVING SUM(d.test_rate) > 0 and " + \
                             "SUM(d.test_rate) < 100000"
            actual_result = self.run_cbq_query()
            actual_result = [{"join_mo": doc["join_mo"], "rate": round(doc["rate"])} for doc in
                             actual_result['results']]
            actual_result = sorted(actual_result, key=lambda doc: (doc['join_mo']))
            tmp_groups = {doc['join_mo'] for doc in self.full_list}
            expected_result = [{"join_mo": group,
                                "rate": round(math.fsum([doc['test_rate']
                                                         for doc in self.full_list
                                                         if doc['join_mo'] == group and
                                                         doc['job_title'] == 'Sales']))}
                               for group in tmp_groups
                               if math.fsum([doc['test_rate']
                                             for doc in self.full_list
                                             if doc['join_mo'] == group and
                                             doc['job_title'] == 'Sales']) > 0 and
                               math.fsum([doc['test_rate']
                                          for doc in self.full_list
                                          if doc['join_mo'] == group and
                                          doc['job_title'] == 'Sales']) < 100000]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result, expected_result)

    def test_prepared_sum(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT join_mo, SUM(tasks_points.task1) as points_sum" + \
                         " FROM %s WHERE join_mo < 5 GROUP BY join_mo " % query_bucket + \
                         "ORDER BY join_mo"
            self.prepared_common_body()

    def test_sum_negative(self):
        queries_errors = {'SELECT join_mo, SUM(job_title) as rate FROM %s as employees' +
                          ' WHERE job_title="Sales" GROUP BY join_mo': ('syntax error', 3000),
                          'SELECT join_mo, SUM(VMs) as rate FROM %s as employees' +
                          ' WHERE job_title="Sales" GROUP BY join_mo': ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_avg(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT join_mo, AVG(tasks_points.task1) as points_avg" + \
                         " FROM %s WHERE join_mo < 5 GROUP BY join_mo " % query_bucket + \
                         "ORDER BY join_mo"

            if self.analytics:
                self.query = "SELECT d.join_mo, AVG(d.tasks_points.task1) as points_avg" + \
                             " FROM %s d WHERE d.join_mo < 5 GROUP BY d.join_mo " % query_bucket + \
                             "ORDER BY d.join_mo"
            actual_result = self.run_cbq_query()
            tmp_groups = {doc['join_mo'] for doc in self.full_list if doc['join_mo'] < 5}
            expected_result = [{"join_mo": group,
                                "points_avg": math.fsum([doc['tasks_points']['task1']
                                                         for doc in self.full_list
                                                         if doc['join_mo'] == group]) /
                                              len([doc['tasks_points']['task1']
                                                   for doc in self.full_list
                                                   if doc['join_mo'] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT join_mo, AVG(test_rate) as rate FROM %s " % query_bucket + \
                         "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                         "HAVING AVG(employees.test_rate) > 0 and " + \
                         "SUM(test_rate) < 100000"

            if self.analytics:
                self.query = "SELECT d.join_mo, AVG(d.test_rate) as rate FROM %s d" % query_bucket + \
                             " WHERE d.job_title='Sales' GROUP BY d.join_mo " + \
                             "HAVING AVG(d.test_rate) > 0 and " + \
                             "SUM(d.test_rate) < 100000"

            actual_result = self.run_cbq_query()

            actual_result = [{'join_mo': doc['join_mo'],
                              'rate': round(doc['rate'], 2)}
                             for doc in actual_result['results']]
            actual_result = sorted(actual_result, key=lambda doc: (doc['join_mo']))
            tmp_groups = {doc['join_mo'] for doc in self.full_list}
            expected_result = [{"join_mo": group,
                                "rate": math.fsum([doc['test_rate']
                                                   for doc in self.full_list
                                                   if doc['join_mo'] == group and
                                                   doc['job_title'] == 'Sales']) /
                                        len([doc['test_rate']
                                             for doc in self.full_list
                                             if doc['join_mo'] == group and
                                             doc['job_title'] == 'Sales'])}
                               for group in tmp_groups
                               if (math.fsum([doc['test_rate']
                                              for doc in self.full_list
                                              if doc['join_mo'] == group and
                                              doc['job_title'] == 'Sales']) /
                                   len([doc['test_rate']
                                        for doc in self.full_list
                                        if doc['join_mo'] == group and
                                        doc['job_title'] == 'Sales']) > 0) and
                               math.fsum([doc['test_rate']
                                          for doc in self.full_list
                                          if doc['join_mo'] == group and
                                          doc['job_title'] == 'Sales']) < 100000]
            expected_result = [{'join_mo': doc['join_mo'],
                                'rate': round(doc['rate'], 2)}
                               for doc in expected_result]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result, expected_result)

    def test_min(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            query_bucket = self.get_collection_name(query_bucket)
            self.query = "SELECT join_mo, MIN(test_rate) as rate FROM %s " % query_bucket + \
                         "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                         "HAVING MIN(employees.test_rate) > 0 and " + \
                         "SUM(test_rate) < 100000"

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['join_mo']))
            tmp_groups = {doc['join_mo'] for doc in self.full_list}
            expected_result = [{"join_mo": group,
                                "rate": min([doc['test_rate']
                                             for doc in self.full_list
                                             if doc['join_mo'] == group and
                                             doc['job_title'] == 'Sales'])}
                               for group in tmp_groups
                               if min([doc['test_rate']
                                       for doc in self.full_list
                                       if doc['join_mo'] == group and
                                       doc['job_title'] == 'Sales']) > 0 and
                               math.fsum([doc['test_rate']
                                          for doc in self.full_list
                                          if doc['join_mo'] == group and
                                          doc['job_title'] == 'Sales']) < 100000]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result, expected_result)

    def test_max(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT join_mo, MAX(test_rate) as rate FROM %s " % query_bucket + \
                         "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                         "HAVING MAX(employees.test_rate) > 0 and " + \
                         "SUM(test_rate) < 100000"

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['join_mo']))
            tmp_groups = {doc['join_mo'] for doc in self.full_list}
            expected_result = [{"join_mo": group,
                                "rate": max([doc['test_rate']
                                             for doc in self.full_list
                                             if doc['join_mo'] == group and
                                             doc['job_title'] == 'Sales'])}
                               for group in tmp_groups
                               if max([doc['test_rate']
                                       for doc in self.full_list
                                       if doc['join_mo'] == group and
                                       doc['job_title'] == 'Sales']) > 0 and
                               math.fsum([doc['test_rate']
                                          for doc in self.full_list
                                          if doc['join_mo'] == group and
                                          doc['job_title'] == 'Sales']) < 100000]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result, expected_result)

    def test_array_agg_distinct(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_agg(DISTINCT name) as names" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_list = [{"job_title": group, "names": {x["name"] for x in self.full_list
                                                            if x["job_title"] == group}} for group in tmp_groups]
            expected_result = self.sort_nested_list(expected_list)
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_length(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_length(array_agg(DISTINCT name)) as num_names" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['job_title']))

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "num_names": len({x["name"] for x in self.full_list
                                                  if x["job_title"] == group})}
                               for group in tmp_groups]

            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)
            self.query = "SELECT job_title, array_length(array_agg(DISTINCT test_rate)) as rates" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['job_title']))
            expected_result = [{"job_title": group,
                                "rates": len({x["test_rate"] for x in self.full_list
                                              if x["job_title"] == group})}
                               for group in tmp_groups]

            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_append(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            query_bucket = self.get_collection_name(query_bucket)
            self.query = "SELECT job_title," + \
                         " array_append(array_agg(DISTINCT name), 'new_name') as names" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "names": sorted(set([x["name"] for x in self.full_list
                                                     if x["job_title"] == group] + ['new_name']))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT job_title," + \
                         " array_append(array_agg(DISTINCT name), 'new_name','123') as names" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "names": sorted(set([x["name"] for x in self.full_list
                                                     if x["job_title"] == group] + ['new_name'] + ['123']))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_prepared_array_append(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title," + \
                         " array_append(array_agg(DISTINCT name), 'new_name') as names" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            self.prepared_common_body()

    def test_array_union_symdiff(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'select ARRAY_SORT(ARRAY_UNION(["skill1","skill2","skill2010","skill2011"],skills)) as ' \
                         'skills_union from {0} order by meta().id limit 5'.format(query_bucket)
            actual_result = self.run_cbq_query()
            expected_result = [{'skills_union': ['skill1', 'skill2', 'skill2010', 'skill2011']},
                               {'skills_union': ['skill1', 'skill2', 'skill2010', 'skill2011']},
                               {'skills_union': ['skill1', 'skill2', 'skill2010', 'skill2011']},
                               {'skills_union': ['skill1', 'skill2', 'skill2010', 'skill2011']},
                               {'skills_union': ['skill1', 'skill2', 'skill2010', 'skill2011']}]
            self.assertTrue(actual_result['results'] == expected_result,
                            f"{actual_result['results']} not matching with {expected_result}")

            self.query = 'select ARRAY_SORT(ARRAY_SYMDIFF(["skill1","skill2","skill2010","skill2011"],skills)) as ' \
                         'skills_diff1 from {0} order by meta().id limit 5'.format(query_bucket)
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'] == [{'skills_diff1': ['skill1', 'skill2']},
                                                         {'skills_diff1': ['skill1', 'skill2']},
                                                         {'skills_diff1': ['skill1', 'skill2']},
                                                         {'skills_diff1': ['skill1', 'skill2']},
                                                         {'skills_diff1': ['skill1', 'skill2']}])

            self.query = 'select ARRAY_SORT(ARRAY_SYMDIFF1(skills,["skill2010","skill2011","skill2012"],' \
                         '["skills2010","skill2017"])) as skills_diff2 from {0} order by ' \
                         'meta().id limit 5'.format(query_bucket)
            actual_result1 = self.run_cbq_query()
            self.assertTrue(
                actual_result1['results'] == [{'skills_diff2': ['skill2012', 'skill2017', 'skills2010']},
                                              {'skills_diff2': ['skill2012', 'skill2017', 'skills2010']},
                                              {'skills_diff2': ['skill2012', 'skill2017', 'skills2010']},
                                              {'skills_diff2': ['skill2012', 'skill2017', 'skills2010']},
                                              {'skills_diff2': ['skill2012', 'skill2017', 'skills2010']}])

            self.query = 'select ARRAY_SORT(ARRAY_SYMDIFFN(skills,["skill2010","skill2011","skill2012"],' \
                         '["skills2010","skill2017"])) as skills_diff3 from {0} order by ' \
                         'meta().id limit 5'.format(query_bucket)
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'] == [{'skills_diff3': ['skill2012', 'skill2017', 'skills2010']},
                                                         {'skills_diff3': ['skill2012', 'skill2017', 'skills2010']},
                                                         {'skills_diff3': ['skill2012', 'skill2017', 'skills2010']},
                                                         {'skills_diff3': ['skill2012', 'skill2017', 'skills2010']},
                                                         {'skills_diff3': ['skill2012', 'skill2017',
                                                                           'skills2010']}])

    def test_let(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'select * from %s let x1 = {"name":1} order by meta().id limit 1' % query_bucket
            actual_result = self.run_cbq_query()
            if "query-testemployee10153.1877827" not in str(actual_result['results']):
                self.assertTrue(False, str(actual_result['results']))

    def test_let_missing(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            created_indexes = []
            try:
                self.query = 'CREATE INDEX ix1 on %s(x1)' % query_bucket
                self.run_cbq_query()
                created_indexes.append("ix1")
                self.query = 'INSERT INTO %s VALUES ("k01",{"x1":5, "type":"doc", "x2": "abc"}), ' \
                             '("k02",{"x1":5, "type":"d", "x2": "def"})' % query_bucket
                self.run_cbq_query()
                self.query = 'EXPLAIN SELECT x1, x2 FROM %s o LET o = CASE WHEN o.type = "doc" THEN o ELSE MISSING ' \
                             'END WHERE x1 = 5' % query_bucket
                try:
                    self.run_cbq_query(self.query)
                except CBQError as ex:
                    self.assertTrue("Duplicate variable: o" in str(ex) or "Duplicate variable: 'o'" in str(ex), f"Error is incorrect. Please take a look at output: {str(ex)}")
                else:
                    self.fail("There was no errors.")
                self.query = 'delete from %s use keys["k01","k02"]' % query_bucket
                self.run_cbq_query()
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s ON %s USING %s" % (idx, query_bucket, self.index_type)
                    self.run_cbq_query()

    def test_optimized_let(self):
        self.fail_if_no_buckets()
        self.query = 'explain select name1 from ' + self.query_buckets[
            0] + ' d let name1 = substr(name[0].FirstName,0,10) WHERE name1 = "employeefi"'
        res = self.run_cbq_query()
        plan = self.ExplainPlanHelper(res)
        for operator in plan['~children'][2]['~child']['~children']:
            if operator['#operator'] == 'Filter':
                self.assertTrue(operator['condition'] == '(`name1` = "employeefi")' or operator['condition'] == '(substr0((((`d`.`name`)[0]).`FirstName`), 0, 10) = "employeefi")')
            if operator['#operator'] == 'Let':
                self.assertEqual(operator['bindings'], [{'expr': 'substr0((((`d`.`name`)[0]).`FirstName`), 0, 10)', 'var': 'name1'}])
            if operator['#operator'] == 'InitialProject':
                self.assertEqual(operator['result_terms'], [{'expr': '`name1`'}])

        self.query = 'select name1 from ' + self.query_buckets[
            0] + ' d let name1 = substr(name[0].FirstName,0,10) WHERE name1 = "employeefi" limit 2'
        res = self.run_cbq_query()
        self.assertEqual(res['results'], [{'name1': 'employeefi'}, {'name1': 'employeefi'}])
        self.query = 'explain select name1 from ' + self.query_buckets[
            0] + ' d let name1 = substr(name[0].FirstName,0,10) WHERE name[0].MiddleName = "employeefirstname-4"'
        res = self.run_cbq_query()
        plan = self.ExplainPlanHelper(res)
        self.assertTrue( "'#operator': 'Filter'" in str(plan['~children'][2]['~child']['~children']), f"The operator is wrong please check {plan}")
        self.assertTrue( "'condition': '((((`d`.`name`)[0]).`MiddleName`) = \"employeefirstname-4\")'" in str(plan['~children'][2]['~child']['~children']), f"The condition is wrong please check {plan}")
        self.assertTrue( "'expr': 'substr0((((`d`.`name`)[0]).`FirstName`), 0, 10)', 'var': 'name1'" in str(plan['~children'][2]['~child']['~children']), f"The expression is wrong please check {plan}")
        self.query = 'select name1 from ' + self.query_buckets[0] + \
                     ' let name1 = substr(name[0].FirstName,0,10) WHERE name[0].MiddleName = "employeefirstname-4" ' \
                     'limit 10 '
        res = self.run_cbq_query()
        self.assertTrue(res['results'] == [])

    def test_object_types(self):
        expected_results = [{"object_types": {"field1": "number","field2": "string","field3": "null","field4": "object"}}]
        actual_results = self.run_cbq_query('SELECT OBJECT_TYPES({"field1":1,"field2":"two","field3":null,"field4":{"sub1":true}}) AS object_types;')
        diffs = DeepDiff(actual_results['results'], expected_results, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)
        expected_results2 = [{"object_types_nested": {"field1": "number",
                                                      "field2": ["number","number","boolean"],
                                                      "field3": "null",
                                                      "field4": {"sub1": "boolean"}}}]
        actual_results2 = self.run_cbq_query('SELECT OBJECT_TYPES_NESTED({"field1":1,"field2":[1,2,true],"field3":null,"field4":{"sub1":true}}) AS object_types_nested;')
        diffs = DeepDiff(actual_results2['results'], expected_results2, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)
        expected_results3 = [{"nested_pairs": [{"name": "attribute.first-part","val": 1},{"name": "attribute.second-part","val": 2}],
                              "nested_pairs_comp": [{"name": "attribute","val": {"first-part": 1,"second-part": 2}},
                                                    {"name": "attribute.first-part","val": 1},
                                                    {"name": "attribute.second-part","val": 2}],
                              "nested_pairs_pattern": [{"name": "attribute.first-part","val": 1}],
                              "nested_pairs_types": [{"name": "attribute.first-part","type": "number"},{"name": "attribute.second-part","type": "number"}]}]
        actual_results3 = self.run_cbq_query('WITH input AS ({"attribute": {"first-part": 1, "second-part": 2}}) '
                           'SELECT OBJECT_PAIRS_NESTED(input) AS nested_pairs,'
                           'OBJECT_PAIRS_NESTED(input, {"composites": true}) AS nested_pairs_comp,'
                           'OBJECT_PAIRS_NESTED(input, {"pattern": "first"}) AS nested_pairs_pattern,'
                           'OBJECT_PAIRS_NESTED(input, {"types": true}) AS nested_pairs_types;')
        diffs = DeepDiff(actual_results3['results'], expected_results3, ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_correlated_queries(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            if query_bucket == self.default_bucket_name:
                try:
                    self.query = 'create index ix1 on %s(x,id)' % query_bucket
                    self.run_cbq_query()
                    self.query = 'insert into %s (KEY, VALUE) VALUES ' \
                                 '("kk02",{"x":100,"y":101,"z":102,"id":"kk02"})' % query_bucket
                    self.run_cbq_query()
                    self.query = 'explain select d.x from {0} d where x in (select raw d.x from {0} b ' \
                                 'use keys ["kk02"])'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue("covers" in str(plan))
                    self.assertTrue(plan['~children'][0]['index'] == 'ix1')
                    self.query = 'select d.x from {0} d where x in (select raw d.x from {0} b ' \
                                 'use keys ["kk02"])'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self.assertTrue(actual_result['results'] == [{'x': 100}])
                    self.query = 'explain select d.x from {0} d where x IN (select raw d.x from {0} ' \
                                 'b use keys[d.id])'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue("covers" in str(plan))
                    self.assertTrue(plan['~children'][0]['index'] == 'ix1')
                    self.query = 'select d.x from {0} d where x IN (select raw d.x from {0}' \
                                 ' b use keys[d.id])'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self.assertTrue(actual_result['results'] == [{'x': 100}])
                    self.query = 'explain select d.x from {0} d where x IN (select raw b.x from {0} b  where b.x IN (' \
                                 'select raw d.x from {0} c  use keys["kk02"]))'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue("covers" in str(plan))
                    self.assertTrue(plan['~children'][0]['index'] == 'ix1')
                    self.query = 'select d.x from {0} d where x IN (select raw b.x from {0} b ' \
                                 ' where b.x IN (select raw ' \
                                 'd.x from {0} c  use keys["kk02"]))'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self.assertTrue(actual_result['results'] == [{'x': 100}])
                    self.query = 'explain select d.x from {0} d where x IN (select raw b.x from {0} b  where b.x IN (' \
                                 'select raw d.x from {0} c  use keys["kk02"] where d.x = b.x))'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue("covers" in str(plan))
                    self.assertTrue(plan['~children'][0]['index'] == 'ix1')
                    self.query = 'select d.x from {0} d where x IN (select raw b.x from {0} b ' \
                                 ' where b.x IN (select raw ' \
                                 'd.x from {0} c  use keys["kk02"] where d.x = b.x))'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self.assertTrue(actual_result['results'] == [{'x': 100}])
                    self.query = 'explain select d.x from {0} d where x IN (select raw b.x from {0} b  where b.x IN (' \
                                 'select raw d.x from {0} c  use keys["kk02"] where d.x = b.x))'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue("covers" in str(plan))
                    self.assertTrue(plan['~children'][0]['index'] == 'ix1')

                    self.query = 'select d.x from {0} d where x IN (select raw b.x from {0} b' \
                                 '  where b.x IN (select raw ' \
                                 'd.x from {0} c  use keys["kk02"] where d.x = b.x))'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self.assertTrue(actual_result['results'] == [{'x': 100}])

                    self.query = 'explain select d.x from {0} d where x IN (select raw b.x' \
                                 ' from {0} b use keys["kk02"] ' \
                                 'where b.x IN (select raw d.x from {0} c  use keys["kk02"]))'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue("covers" in str(plan))
                    self.assertTrue(plan['~children'][0]['index'] == 'ix1')

                    self.query = 'select d.x from {0} d where x IN (select raw b.x from {0} b use keys["kk02"] ' \
                                 'where b.x IN (select raw d.x from {0} c  use keys["kk02"]))'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self.assertTrue(actual_result['results'] == [{'x': 100}])

                    self.query = 'explain select d.x,d.id from {0} d where x IN (select raw b.x from {0} b  where ' \
                                 'b.x IN (select raw d.x from {0} c  use keys["kk02"]))'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue("covers" in str(plan))
                    self.assertTrue(plan['~children'][0]['index'] == 'ix1')

                    self.query = 'select d.x,d.y from {0} d where x IN (select raw b.x from {0} b  where b.x  ' \
                                 'IN (select raw c.x from {0} c  use keys["kk02"]))'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    diffs = DeepDiff(actual_result['results'], [{'y': 101, 'x': 100}], ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
                    self.query = 'explain select d.x from {0} d where x IN (select raw (select raw d.x from {0}' \
                                 ' c use keys[d.id] where d.x = b.x)[0] from {0} b ' \
                                 'where b.x is not null)'.format(query_bucket)
                    self.run_cbq_query()
                    self.assertTrue("covers" in str(plan))
                    self.assertTrue(plan['~children'][0]['index'] == 'ix1')

                    self.query = 'select d.x from {0} d where x IN (select raw (select raw d.x from {0} c ' \
                                 'use keys[d.id] where d.x = b.x)[0] from {0} b where b.x ' \
                                 'is not null)'.format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self.assertTrue(actual_result['results'] == [{'x': 100}])

                    self.query = 'explain select (select raw d.x from ' + self.query_buckets[0] + \
                                 '  c  use keys[d.id]) as s, d.x from ' + self.query_buckets[0] + \
                                 '  d where x is not null'
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    # print plan
                    self.assertTrue("covers" in str(plan))
                    self.assertTrue(plan['~children'][0]['index'] == 'ix1')
                    self.query = 'select (select raw d.x from ' + self.query_buckets[0] + \
                                 '  c  use keys[d.id]) as s, d.x from ' + self.query_buckets[0] + \
                                 '  d where x is not null'
                    actual_result = self.run_cbq_query()
                    diffs = DeepDiff(actual_result['results'], [{'x': 100, 's': [100]}], ignore_order=True)

                    self.assertEqual(diffs, {})
                finally:
                    self.query = 'delete from %s use keys["kk02"]' % query_bucket
                    self.run_cbq_query()
                    self.query = "DROP INDEX %s ON %s USING %s" % ("ix1", query_bucket, self.index_type)
                    self.run_cbq_query()

    # This test has no uses anywhere
    def test_object_concat_remove(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'select object_concat({"name":"test"},{"test":123},tasks_points) as obj from %s order by ' \
                         'meta().id limit 10;' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'] == (
                [{'obj': {'test': 123, 'task1': 1, 'task2': 1, 'name': 'test'}},
                 {'obj': {'test': 123, 'task1': 1, 'task2': 1, 'name': 'test'}},
                 {'obj': {'test': 123, 'task1': 1, 'task2': 1, 'name': 'test'}},
                 {'obj': {'test': 123, 'task1': 1, 'task2': 1, 'name': 'test'}},
                 {'obj': {'test': 123, 'task1': 1, 'task2': 1, 'name': 'test'}},
                 {'obj': {'test': 123, 'task1': 1, 'task2': 1, 'name': 'test'}},
                 {'obj': {'test': 123, 'task1': 1, 'task2': 1, 'name': 'test'}},
                 {'obj': {'test': 123, 'task1': 1, 'task2': 1, 'name': 'test'}},
                 {'obj': {'test': 123, 'task1': 1, 'task2': 1, 'name': 'test'}},
                 {'obj': {'test': 123, 'task1': 1, 'task2': 1, 'name': 'test'}}]))
            self.query = 'select OBJECT_REMOVE({"abc":1,"def":2,"fgh":3},"def")'
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'], ([{'$1': {'abc': 1, 'fgh': 3}}]))

    def test_array_concat(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title," + \
                         " array_concat(array_agg(name), array_agg(email)) as names" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result1 = sorted(actual_result, key=lambda doc: (doc['job_title']))
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "names": [x["name"] for x in self.full_list
                                          if x["job_title"] == group] +
                                         [x["email"] for x in self.full_list
                                          if x["job_title"] == group]}
                               for group in tmp_groups]
            expected_result1 = sorted(expected_result, key=lambda doc: (doc['job_title']))

            self._verify_results(actual_result1, expected_result1)

            self.query = "SELECT job_title," + \
                         " array_concat(array_agg(name), array_agg(email),array_agg(join_day)) as names" + \
                         " FROM %s GROUP BY job_title  limit 10" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']

            expected_result = [{"job_title": group,
                                "names": [x["name"] for x in self.full_list
                                          if x["job_title"] == group] +
                                         [x["email"] for x in self.full_list
                                          if x["job_title"] == group] +
                                         [x["join_day"] for x in self.full_list
                                          if x["job_title"] == group]}
                               for group in tmp_groups][0:10]
            diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

    def test_array_prepend(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title," + \
                         " array_prepend(1.2, array_agg(test_rate)) as rates" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "rates": [x["test_rate"] for x in self.full_list
                                          if x["job_title"] == group] + [1.2]}

                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)
            self.query = "SELECT job_title," + \
                         " array_prepend(1.2,2.4, array_agg(test_rate)) as rates" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "rates": [x["test_rate"] for x in self.full_list
                                          if x["job_title"] == group] + [1.2] + [2.4]}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT job_title," + \
                         " array_prepend(['skill5', 'skill8'], array_agg(skills)) as skills_new" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "skills_new": [x["skills"] for x in self.full_list
                                               if x["job_title"] == group] +
                                              [['skill5', 'skill8']]}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT job_title," + \
                         " array_prepend(['skill5', 'skill8'],['skill9','skill10'], array_agg(skills)) as " + \
                         " skills_new FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "skills_new": [x["skills"] for x in self.full_list
                                               if x["job_title"] == group] +
                                              [['skill5', 'skill8']] + [['skill9', 'skill10']]}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))

            self._verify_results(actual_result, expected_result)

    def test_array_remove(self):
        self.fail_if_no_buckets()
        value = 'employee-1'
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title," + \
                         " array_remove(array_agg(DISTINCT name), '%s') as names" % value + \
                         " FROM %s GROUP BY job_title ORDER BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "names": [x["name"] for x in self.full_list
                                          if x["job_title"] == group and x["name"] != value]}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            value1 = 'employee-2'
            value2 = 'emp-2'
            value3 = 'employee-1'
            self.query = "SELECT job_title," + \
                         " array_remove(array_agg(DISTINCT name), '%s','%s','%s') as " % (value1, value2, value3) + \
                         " names FROM %s GROUP BY job_title ORDER BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "names": [x["name"] for x in self.full_list
                                          if x["job_title"] == group and x["name"] != value1 and x[
                                              "name"] != value3]}
                               for group in tmp_groups]
            self._verify_results(actual_result, expected_result)

    def test_array_insert(self):
        value1 = 'skill-20'
        value2 = 'skill-21'
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT array_insert(skills, 1, '%s','%s') " % (value1, value2) + \
                         " FROM %s limit 1" % query_bucket

            actual_list = self.run_cbq_query()
            expected_result = [{'$1': ['skill2010', 'skill-20', 'skill-21', 'skill2011']}]
            self.assertTrue(actual_list['results'] == expected_result)

    def test_array_avg(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_avg(array_agg(test_rate))" + \
                         " as rates FROM %s GROUP BY job_title ORDER BY job_title" % query_bucket

            actual_result = self.run_cbq_query()
            for doc in actual_result['results']:
                doc['rates'] = round(doc['rates'])
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "rates": round(sum([x["test_rate"] for x in self.full_list
                                                    if x["job_title"] == group]) / float(len([x["test_rate"]
                                                                                              for x in self.full_list
                                                                                              if x[
                                                                                                  "job_title"] == group])))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result['results'], expected_result)

    def test_array_contains(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_contains(array_agg(name), 'employee-1')" + \
                         " as emp_job FROM %s GROUP BY job_title ORDER BY job_title" % query_bucket

            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "emp_job": 'employee-1' in [x["name"] for x in self.full_list
                                                            if x["job_title"] == group]}
                               for group in tmp_groups]
            self._verify_results(actual_result, expected_result)

    def test_array_count(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_count(array_agg(name)) as names" + \
                         " FROM %s GROUP BY job_title ORDER BY job_title" % query_bucket

            actual_result = self.run_cbq_query()

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "names": len([x["name"] for x in self.full_list
                                              if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result['results'], expected_result)

    def test_array_distinct(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_distinct(array_agg(name)) as names" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "names": [x["name"] for x in self.full_list
                                          if x["job_title"] == group]}
                               for group in tmp_groups]
            for item in actual_result:
                self.assertTrue(item not in expected_result, f"{actual_result} not matching with {expected_result}")

    def test_array_max(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_max(array_agg(test_rate)) as rates" + \
                         " FROM %s GROUP BY job_title ORDER BY job_title" % query_bucket

            actual_result = self.run_cbq_query()

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "rates": max([x["test_rate"] for x in self.full_list
                                              if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result['results'], expected_result)

    def test_array_sum(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, round(array_sum(array_agg(test_rate))) as rates" + \
                         " FROM %s GROUP BY job_title ORDER BY job_title" % query_bucket

            actual_result = self.run_cbq_query()

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "rates": round(sum([x["test_rate"] for x in self.full_list
                                                    if x["job_title"] == group]))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result['results'], expected_result)

    def test_array_min(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_min(array_agg(test_rate)) as rates" + \
                         " FROM %s GROUP BY job_title ORDER BY job_title" % query_bucket

            actual_result = self.run_cbq_query()

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "rates": min([x["test_rate"] for x in self.full_list
                                              if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result['results'], expected_result)

    def test_array_position(self):
        self.query = "select array_position(['support', 'qa'], 'dev') as index_num"
        actual_result = self.run_cbq_query()
        expected_result = [{"index_num": -1}]
        self._verify_results(actual_result['results'], expected_result)

        self.query = "select array_position(['support', 'qa'], 'qa') as index_num"
        actual_result = self.run_cbq_query()
        expected_result = [{"index_num": 1}]
        self._verify_results(actual_result['results'], expected_result)

    def test_array_put(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_put(array_agg(distinct name), 'employee-1') as emp_job" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "emp_job": [x["name"] for x in self.full_list
                                            if x["job_title"] == group]}
                               for group in tmp_groups]
            for item in actual_result:
                self.assertTrue(item not in expected_result, f"{actual_result} not matching with {expected_result}")

            self.query = "SELECT job_title, array_put(array_agg(distinct name), 'employee-50','employee-51') as " + \
                         " emp_job FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "emp_job": [x["name"] for x in self.full_list
                                                if x["job_title"] == group] + ['employee-50'] + [
                                                   'employee-51']}
                               for group in tmp_groups]
            for item in actual_result:
                self.assertTrue(item not in expected_result, f"{actual_result} not matching with {expected_result}")

            self.query = "SELECT job_title, array_put(array_agg(distinct name), 'employee-47') as emp_job" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"job_title": group,
                                "emp_job": [x["name"] for x in self.full_list
                                                if x["job_title"] == group] + ['employee-47']}
                               for group in tmp_groups]
            for item in actual_result:
                self.assertTrue(item not in expected_result, f"{actual_result} not matching with {expected_result}")

    def test_array_range(self):
        self.query = "select array_range(0,10) as num"
        actual_result = self.run_cbq_query()
        expected_result = [{"num": list(range(10))}]
        self._verify_results(actual_result['results'], expected_result)

    def test_array_replace(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_replace(array_agg(name), 'employee-1', 'employee-47') as emp_job" + \
                         " FROM %s GROUP BY job_title ORDER BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "emp_job": ["employee-47" if x["name"] == 'employee-1' else x["name"]
                                            for x in self.full_list
                                            if x["job_title"] == group]}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_repeat(self):
        self.query = "select ARRAY_REPEAT(2, 2) as num"
        actual_result = self.run_cbq_query()
        expected_result = [{"num": [2] * 2}]
        self._verify_results(actual_result['results'], expected_result)

    def test_array_reverse(self):
        self.query = "select array_reverse([1,2,3]) as num"
        actual_result = self.run_cbq_query()
        expected_result = [{"num": [3, 2, 1]}]
        self._verify_results(actual_result['results'], expected_result)

    def test_array_sort(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_sort(array_agg(distinct test_rate)) as emp_job" + \
                         " FROM %s GROUP BY job_title ORDER BY job_title" % query_bucket

            actual_result = self.run_cbq_query()
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "emp_job": [x["test_rate"] for x in self.full_list
                                            if x["job_title"] == group]}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            for item in actual_result['results']:
                self.assertTrue(item not in expected_result, f"{actual_result} not matching with {expected_result}")

    # This test has no usages anywhere
    def test_poly_length(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            query_fields = ['tasks_points', 'VMs', 'skills']
            for query_field in query_fields:
                self.query = "Select length(%s) as custom_num from %s order by custom_num" % (query_field, query_bucket)
                actual_result = self.run_cbq_query()
                expected_result = [{"custom_num": None} for _ in self.full_list]
                self._verify_results(actual_result['results'], expected_result)

                self.query = "Select poly_length(%s) as custom_num from %s order by custom_num" % (
                    query_field, query_bucket)
                actual_result = self.run_cbq_query()
                expected_result = [{"custom_num": len(doc[query_field])}
                                   for doc in self.full_list]
                expected_result = sorted(expected_result, key=lambda doc: (doc['custom_num']))
                self._verify_results(actual_result['results'], expected_result)

    def test_array_agg(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, array_agg(name) as names" + \
                         " FROM %s GROUP BY job_title" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_list = [{"job_title": group,
                              "names": [x["name"] for x in self.full_list
                                        if x["job_title"] == group]}
                             for group in tmp_groups]
            expected_result = self.sort_nested_list(expected_list)
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    ##############################################################################################
    #
    #   EXPRESSIONS
    ##############################################################################################

    def test_case(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            query_bucket = self.get_collection_name(query_bucket)
            self.query = "SELECT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN 'winter'" + \
                         " WHEN join_mo < 6 AND join_mo > 2 THEN 'spring' " + \
                         "WHEN join_mo < 9 AND join_mo > 5 THEN 'summer' " + \
                         "ELSE 'autumn' END AS period FROM %s" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['name'],
                doc['period']))

            expected_result = [{"name": doc['name'],
                                "period": ((('autumn', 'summer')[doc['join_mo'] in [6, 7, 8]],
                                            'spring')[doc['join_mo'] in [3, 4, 5]], 'winter')
                                [doc['join_mo'] in [12, 1, 2]]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_case_expr(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            query_bucket = self.get_collection_name(query_bucket)
            self.query = "SELECT name, CASE job_title WHEN 'Sales' THEN 'Marketing'" + \
                         "ELSE job_title END AS dept FROM %s" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['name'],
                doc['dept']))

            expected_result = [{"name": doc['name'],
                                "dept": (doc['job_title'], 'Marketing')[doc['job_title'] == 'Sales']}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['dept']))
            self._verify_results(actual_result, expected_result)

    def test_case_arithm(self):
        self.query = "SELECT CASE WHEN 1+1=3 THEN 7+7 WHEN 2+2=5 THEN 8+8 ELSE 2 END"
        actual_result = self.run_cbq_query()
        expected_result = [{"$1": 2}]
        self._verify_results(actual_result['results'], expected_result)

    def test_in_int(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name from %s where join_mo in [1,6]" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name']))

            expected_result = [{"name": doc['name']}
                               for doc in self.full_list
                               if doc['join_mo'] in [1, 6]]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where join_mo not in [1,6]" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['name']))
            expected_result = [{"name": doc['name']}
                               for doc in self.full_list
                               if not (doc['join_mo'] in [1, 6])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_in_str(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name from %s where job_title in ['Sales', 'Support']" % query_bucket
            actual_result = self.run_cbq_query()
            self.query = "select name from %s where REVERSE(job_title) in ['selaS', 'troppuS']" % query_bucket
            actual_result1 = self.run_cbq_query()
            self.assertEqual(actual_result['results'], actual_result1['results'])
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['name']))

            expected_result = [{"name": doc['name']}
                               for doc in self.full_list
                               if doc['job_title'] in ['Sales', 'Support']]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where job_title not in ['Sales', 'Support']" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['name']))
            expected_result = [{"name": doc['name']}
                               for doc in self.full_list
                               if not (doc['job_title'] in ['Sales', 'Support'])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_prepared_in_str(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name from %s where job_title in ['Sales', 'Support']" % query_bucket
            self.prepared_common_body()

    def test_logic_expr(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % query_bucket + \
                         "tasks_points.task1 > 1 AND tasks_points.task1 < 4"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['task']))

            expected_result = [{"task": doc['tasks_points']['task1']}
                               for doc in self.full_list
                               if 1 < doc['tasks_points']['task1'] < 4]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result, expected_result)

    def test_comparition_equal_int(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % query_bucket + "tasks_points.task1 = 4"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['task']))

            expected_result = [{"task": doc['tasks_points']['task1']}
                               for doc in self.full_list
                               if doc['tasks_points']['task1'] == 4]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % query_bucket + "tasks_points.task1 == 4"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['task']))
            self._verify_results(actual_result, expected_result)

    def test_comparition_equal_str(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s WHERE " % query_bucket + "name = 'employee-4'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['name']))

            expected_result = [{"name": doc['name']}
                               for doc in self.full_list
                               if doc['name'] == 'employee-4']
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT name FROM %s WHERE " % query_bucket + "name == 'employee-4'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_comparition_not_equal(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % query_bucket + \
                         "tasks_points.task1 != 1 ORDER BY task"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']

            expected_result = [{"task": doc['tasks_points']['task1']}
                               for doc in self.full_list
                               if doc['tasks_points']['task1'] != 1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result, expected_result)

    def test_comparition_not_equal_more_less(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % query_bucket + \
                         "tasks_points.task1 <> 1 ORDER BY task"
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']

            expected_result = [{"task": doc['tasks_points']['task1']}
                               for doc in self.full_list
                               if doc['tasks_points']['task1'] != 1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result, expected_result)

    def test_every_comparision_not_equal(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s d WHERE " % query_bucket + \
                         "(EVERY vm IN d.VMs SATISFIES vm.memory != 5 END) ORDER BY name"

            if self.analytics:
                self.query = "SELECT name FROM %s d WHERE " % query_bucket + \
                             "(EVERY vm IN d.VMs SATISFIES vm.memory != 5 ) ORDER BY name"

            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name']}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"]
                                       if vm['memory'] != 5]) == len(doc["VMs"])]

            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_every_comparision_not_equal_less_more(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s d WHERE " % query_bucket + \
                         "(EVERY vm IN d.VMs SATISFIES vm.memory <> 5 END) ORDER BY name"

            if self.analytics:
                self.query = "SELECT name FROM %s d WHERE " % query_bucket + \
                             "(EVERY vm IN d.VMs SATISFIES vm.memory <> 5 ) ORDER BY name"

            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name']}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"]
                                       if vm['memory'] != 5]) == len(doc["VMs"])]

            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_any_between(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name, email FROM %s d WHERE " % query_bucket + \
                         "(ANY skill IN d.skills SATISFIES skill = 'skill2010' END)" + \
                         " AND (ANY vm IN d.VMs SATISFIES vm.RAM between 1 and 5 END)" + \
                         "AND  NOT (job_title = 'Sales') ORDER BY name"

            if self.analytics:
                self.query = "SELECT name, email FROM %s d WHERE " % query_bucket + \
                             "(ANY skill IN d.skills SATISFIES skill = 'skill2010' )" + \
                             " AND (ANY vm IN d.VMs SATISFIES vm.RAM between 1 and 5 )" + \
                             "AND  NOT (job_title = 'Sales') ORDER BY name"

            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name'], "email": doc["email"]}
                               for doc in self.full_list
                               if len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0 and
                               len([vm for vm in doc["VMs"]
                                    if vm["RAM"] in [1, 2, 3, 4, 5]]) > 0 and
                               doc["job_title"] != 'Sales']
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_any_less_equal(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name, email FROM %s d WHERE " % query_bucket + \
                         "(ANY skill IN d.skills SATISFIES skill = 'skill2010' END)" + \
                         " AND (ANY vm IN d.VMs SATISFIES vm.RAM <= 5 END)" + \
                         "AND  NOT (job_title = 'Sales') ORDER BY name"

            if self.analytics:
                self.query = "SELECT name, email FROM %s d WHERE " % query_bucket + \
                             "(ANY skill IN d.skills SATISFIES skill = 'skill2010' )" + \
                             " AND (ANY vm IN d.VMs SATISFIES vm.RAM <= 5 )" + \
                             "AND  NOT (job_title = 'Sales') ORDER BY name"

            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name'], "email": doc["email"]}
                               for doc in self.full_list
                               if len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0 and
                               len([vm for vm in doc["VMs"]
                                    if vm["RAM"] <= 5]) > 0 and
                               doc["job_title"] != 'Sales']
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_any_more_equal(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name, email FROM %s d WHERE " % query_bucket + \
                         "(ANY skill IN d.skills SATISFIES skill = 'skill2010' END)" + \
                         " AND (ANY vm IN d.VMs SATISFIES vm.RAM >= 5 END)" + \
                         "AND  NOT (job_title = 'Sales') ORDER BY name"

            if self.analytics:
                self.query = "SELECT name, email FROM %s d WHERE " % query_bucket + \
                             "(ANY skill IN d.skills SATISFIES skill = 'skill2010' )" + \
                             " AND (ANY vm IN d.VMs SATISFIES vm.RAM >= 5 )" + \
                             "AND  NOT (job_title = 'Sales') ORDER BY name"

            actual_result = self.run_cbq_query()
            expected_result = [{"name": doc['name'], "email": doc["email"]}
                               for doc in self.full_list
                               if len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0 and
                               len([vm for vm in doc["VMs"]
                                    if vm["RAM"] >= 5]) > 0 and
                               doc["job_title"] != 'Sales']
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_between(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            query_bucket = self.get_collection_name(query_bucket)
            self.query = "SELECT name, email FROM %s d WHERE " % query_bucket + \
                         "(ANY skill IN d.skills SATISFIES skill = 'skill2010' END)" + \
                         " AND (ANY vm IN d.VMs SATISFIES vm.RAM between 1 and 5 END)" + \
                         "AND  NOT (job_title = 'Sales') ORDER BY name"
            self.prepared_common_body()

    def test_named_prepared_between(self):
        self.fail_if_no_buckets()
        if self.monitoring:
            self.query = "select * from system:prepareds"
            result = self.run_cbq_query()
            self.assertTrue(result['metrics']['resultCount'] == 0)
        for query_bucket in self.query_buckets:
            self.query = "SELECT name, email FROM %s d WHERE " % query_bucket + \
                         "(ANY skill IN d.skills SATISFIES skill = 'skill2010' END)" + \
                         " AND (ANY vm IN d.VMs SATISFIES vm.RAM between 1 and 5 END)" + \
                         "AND  NOT (job_title = 'Sales') ORDER BY name"
            self.prepared_common_body()
        if self.monitoring:
            self.query = "select * from system:prepareds"
            result = self.run_cbq_query()
            self.assertTrue(result['metrics']['resultCount'] == 1)
            name = result['results'][0]['prepareds']['name']
            self.query = 'delete from system:prepareds where name = "%s" ' % name
            self.run_cbq_query()
            self.query = 'select * from system:prepareds where name = "%s" ' % name
            result = self.run_cbq_query()
            self.assertTrue(result['metrics']['resultCount'] == 0)

    def test_prepared_comparision_not_equal_less_more(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s d WHERE " % query_bucket + \
                         "(EVERY vm IN d.VMs SATISFIES vm.memory <> 5 END) ORDER BY name"
            self.prepared_common_body()

    def test_prepared_comparision_not_equal(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s d WHERE " % query_bucket + \
                         "(EVERY vm IN d.VMs SATISFIES vm.memory != 5 END) ORDER BY name"
            self.prepared_common_body()

    def test_prepared_more_equal(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s d WHERE " % query_bucket + \
                         "(EVERY vm IN d.VMs SATISFIES vm.memory >= 5 END) ORDER BY name"
            self.prepared_common_body()

    def test_prepared_less_equal(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s d WHERE " % query_bucket + \
                         "(EVERY vm IN d.VMs SATISFIES vm.memory <= 5 END) ORDER BY name"
            self.prepared_common_body()

    def test_let_not_equal(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select compare from %s let compare = (test_rate != 2)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"compare": doc["test_rate"] != 2}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_let_between(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select compare from %s let compare = (test_rate between 1 and 3)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"compare": 1 <= doc["test_rate"] <= 3} for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_let_not_equal_less_more(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select compare from %s let compare = (test_rate <> 2)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"compare": doc["test_rate"] != 2}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_let_more_equal(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select compare from %s let compare = (test_rate >= 2)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"compare": doc["test_rate"] >= 2}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_let_less_equal(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select compare from %s let compare = (test_rate <= 2)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"compare": doc["test_rate"] <= 2}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_comparition_equal_not_equal(self):
        self.fail_if_no_buckets()
        template = "SELECT join_day, join_mo FROM %s WHERE " + \
                   "join_day == 1 and join_mo != 2 ORDER BY join_day, join_mo"
        for query_bucket in self.query_buckets:
            self.query = template % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['join_day'], doc['join_mo']))

            expected_result = [{"join_mo": doc['join_mo'], "join_day": doc['join_day']}
                               for doc in self.full_list
                               if doc['join_day'] == 1 and doc["join_mo"] != 2]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_day'], doc['join_mo']))
            self._verify_results(actual_result, expected_result)
        return template

    def test_comparition_more_and_less_equal(self):
        self.fail_if_no_buckets()
        template = "SELECT join_yr, test_rate FROM %s WHERE join_yr >= 2010 AND test_rate <= 4"
        for query_bucket in self.query_buckets:
            self.query = template % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['test_rate'], doc['join_yr']))

            expected_result = [{"test_rate": doc['test_rate'], "join_yr": doc['join_yr']}
                               for doc in self.full_list
                               if doc['test_rate'] <= 4 and
                               doc['join_yr'] >= 2010]
            expected_result = sorted(expected_result, key=lambda doc: (doc['test_rate'], doc['join_yr']))
            self._verify_results(actual_result, expected_result)
        return template

    def test_comparition_null_missing(self):
        self.fail_if_no_buckets()
        template = "SELECT skills, VMs FROM %s WHERE " + \
                   "skills is not null AND VMs is not missing"
        for query_bucket in self.query_buckets:
            self.query = template % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['VMs'], doc['skills']))

            expected_result = [{"VMs": doc['VMs'], "skills": doc['skills']}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['VMs'], doc['skills']))
            self._verify_results(actual_result, expected_result)
        return template

    def test_comparition_aggr_fns(self):
        self.fail_if_no_buckets()
        template = "SELECT count(join_yr) years, sum(test_rate) rate FROM %s"
        for query_bucket in self.query_buckets:
            self.query = template % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']

            expected_result = [{"years": len([doc['join_yr'] for doc in self.full_list]),
                                "rate": sum([doc['test_rate'] for doc in self.full_list])}]
            self.assertTrue(round(actual_result[0]['rate']) == round(expected_result[0]['rate']))
            self.assertTrue((actual_result[0]['years']) == (expected_result[0]['years']))
        return template

    # This test has no uses anywhere
    def test_comparition_meta(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT meta(d).id, meta(d).type FROM %s d" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']

    def test_comparition_more_less_equal(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % query_bucket + \
                         "tasks_points.task1 >= 1 AND tasks_points.task1 <= 4"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                doc['task']))

            expected_result = [{"task": doc['tasks_points']['task1']} for doc in self.full_list
                               if 1 <= doc['tasks_points']['task1'] <= 4]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result, expected_result)

    def test_comparition_expr(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % query_bucket + \
                         "tasks_points.task1 > tasks_points.task1"
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], [])

    def test_arithm(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, SUM(test_rate) % COUNT(distinct join_yr)" + \
                         " as avg_per_year from {0} group by job_title".format(query_bucket)

            if self.analytics:
                self.query = "SELECT d.job_title, SUM(d.test_rate) % COUNT(d.join_yr)" + \
                             " as avg_per_year from {0} d group by d.job_title".format(query_bucket)

            actual_result = self.run_cbq_query()
            actual_result = [{"job_title": doc["job_title"],
                              "avg_per_year": round(doc["avg_per_year"], 2)}
                             for doc in actual_result['results']]
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "avg_per_year": math.fsum([doc['test_rate']
                                                           for doc in self.full_list
                                                           if doc['job_title'] == group]) %
                                                len({doc['join_yr'] for doc in self.full_list
                                                     if doc['job_title'] == group})}
                               for group in tmp_groups]
            expected_result = [{"job_title": doc["job_title"],
                                "avg_per_year": round(doc["avg_per_year"], 2)}
                               for doc in expected_result]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT job_title, (SUM(tasks_points.task1) +" + \
                         " SUM(tasks_points.task2)) % COUNT(distinct join_yr) as avg_per_year" + \
                         " from {0} group by job_title".format(query_bucket)

            if self.analytics:
                self.query = "SELECT d.job_title, (SUM(d.tasks_points.task1) +" + \
                             " SUM(d.tasks_points.task2)) % COUNT(d.join_yr) as avg_per_year" + \
                             " from {0} d group by d.job_title".format(query_bucket)
            actual_result = self.run_cbq_query()
            actual_result = [{"job_title": doc["job_title"],
                              "avg_per_year": round(doc["avg_per_year"], 2)}
                             for doc in actual_result['results']]
            actual_result = sorted(actual_result, key=lambda doc: (
                doc['job_title']))
            tmp_groups = {doc['job_title'] for doc in self.full_list}
            expected_result = [{"job_title": group,
                                "avg_per_year": (math.fsum([doc['tasks_points']['task1']
                                                            for doc in self.full_list
                                                            if doc['job_title'] == group]) +
                                                 math.fsum([doc['tasks_points']['task2']
                                                            for doc in self.full_list
                                                            if doc['job_title'] == group])) %
                                                len({doc['join_yr'] for doc in self.full_list
                                                     if doc['job_title'] == group})}
                               for group in tmp_groups]
            expected_result = [{"job_title": doc["job_title"],
                                "avg_per_year": int(round(doc["avg_per_year"], 2))}
                               for doc in expected_result]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

        # https://issues.couchbase.com/browse/MB-29401

    def test_sum_large_negative_numbers(self):
        self.fail_if_no_buckets()

        self.run_cbq_query(
            "insert into " + self.query_buckets[0] + "  (KEY, VALUE) VALUES ('doc1',{ 'f1' : -822337203685477580 })")
        self.run_cbq_query(
            "insert into " + self.query_buckets[0] + "  (KEY, VALUE) VALUES ('doc2',{ 'f1' : -822337203685477580 })")
        self.query = "select SUM(f1) from " + self.query_buckets[0] + " "
        result = self.run_cbq_query()
        found_result = result['results'][0]['$1']
        expected_result = -1644674407370955160
        self.assertEqual(found_result, expected_result)

        self.run_cbq_query(
            "insert into " + self.query_buckets[0] + "  (KEY, VALUE) VALUES ('doc3',{ 'f2' : -822337203685477580 })")
        self.run_cbq_query("insert into " + self.query_buckets[0] + "  (KEY, VALUE) VALUES ('doc4',{ 'f2' : 10 })")
        self.query = "select SUM(f2) from " + self.query_buckets[0] + " "
        result = self.run_cbq_query()
        found_result = result['results'][0]['$1']
        expected_result = -822337203685477570
        self.assertEqual(found_result, expected_result)

    ##############################################################################################
    #
    #   EXPLAIN
    ##############################################################################################

    def test_explain(self):
        self.fail_if_no_buckets()
        for _ in self.query_buckets:
            res = self.run_cbq_query()
            self.log.info(res)
            plan = self.ExplainPlanHelper(res)
            self.assertTrue(plan["~children"][0]["index"] == "#primary",
                            "Type should be #primary, but is: %s" % plan["~children"][0]["index"])

    ##############################################################################################
    #
    #   EXPLAIN WITH A PARTICULAR INDEX
    ##############################################################################################
    # Test has no usages anywhere
    def test_explain_particular_index(self, index):
        self.fail_if_no_buckets()
        for _ in self.query_buckets:
            res = self.run_cbq_query()
            self.log.info(res)
            plan = self.ExplainPlanHelper(res)
            self.assertTrue(plan['~children'][2]['~child']['~children'][0]['scan']['index'] == index,
                            "wrong index used")

    ###############################################################################################
    #
    #   EXPLAIN WITH UNION SCAN: Covering Indexes
    ##############################################################################################
    # Test has no usages anywhere
    def test_explain_union(self, index):
        self.fail_if_no_buckets()
        for _ in self.query_buckets:
            res = self.run_cbq_query()
            plan = self.ExplainPlanHelper(res)
            if "IN" in self.query:
                self.assertTrue(plan["~children"][0]["~children"][0]["#operator"] == "DistinctScan",
                                "DistinctScan Operator is not used by this query")
            else:
                self.assertTrue(plan["~children"][0]["~children"][0]["#operator"] == "UnionScan",
                                "UnionScan Operator is not used by this query")
            if plan["~children"][0]["~children"][0]["scan"]["#operator"] == "IndexScan":
                self.log.info("IndexScan Operator is also used by this query in scans")
            else:
                self.log.error("IndexScan Operator is not used by this query, Covering Indexes not used properly")
                self.fail("IndexScan Operator is not used by this query, Covering Indexes not used properly")
            if plan["~children"][0]["~children"][0]["scan"]["index"] == index:
                self.log.info("Query is using specified index")
            else:
                self.log.info("Query is not using specified index")

    ##############################################################################################
    #
    #   DATETIME
    ##############################################################################################

    def test_clock_millis(self):
        self.query = "select clock_millis() as now"
        res = self.run_cbq_query()
        self.assertFalse("error" in str(res).lower())

    def test_clock_str(self):
        self.query = "select clock_str() as now"
        now = datetime.datetime.now()
        res = self.run_cbq_query()
        expected = "%s-%02d-%02dT" % (now.year, now.month, now.day)
        self.assertTrue(res["results"][0]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))

    def test_date_add_millis(self):
        self.query = "select date_add_millis(clock_millis(), 100, 'day') as now"
        now = time.time()
        res = self.run_cbq_query()
        self.assertTrue((res["results"][0]["now"] > now * 1000 + 100 * 24 * 60 * 60),
                        "Result expected to be in: [%s ... %s]. Actual %s" % (
                            now * 1000 + 100 * 24 * 60 * 60, (now + 10) * 1000 + 100 * 24 * 60 * 60, res["results"]))

    def test_date_add_str(self):
        self.query = "select date_add_str(clock_utc(), 10, 'day') as now"
        now = datetime.datetime.utcnow() + datetime.timedelta(days=10)
        res = self.run_cbq_query()
        expected = "%s-%02d-%02dT%02d:" % (now.year, now.month, now.day, now.hour)
        expected_delta = "%s-%02d-%02dT%02d:" % (now.year, now.month, now.day, now.hour + 1)
        self.assertTrue(
            res["results"][0]["now"].startswith(expected) or res["results"][0]["now"].startswith(expected_delta),
            "Result expected: %s. Actual %s" % (expected, res["results"]))

    def test_date_diff_millis(self):
        self.query = "select date_diff_millis(clock_millis(), date_add_millis(clock_millis(), 100, 'day'), 'day') as " \
                     "now "
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["now"] == -100,
                        "Result expected: %s. Actual %s" % (-100, res["results"]))

    def test_date_diff_str(self):
        self.query = 'select date_diff_str("2014-08-24T01:33:59", "2014-08-24T07:33:59", "minute") as now'
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["now"] == -360,
                        "Result expected: %s. Actual %s" % (-360, res["results"]))
        self.query = 'select date_diff_str("2014-08-24T01:33:59", "2014-08-24T07:33:59", "hour") as now'
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["now"] == -6,
                        "Result expected: %s. Actual %s" % (-6, res["results"]))

    def test_now(self):
        self.query = "select now_str() as now"
        now = datetime.datetime.now()
        today = date.today()
        res = self.run_cbq_query()
        expected = "%s-%02d-%02dT" % (today.year, today.month, today.day,)
        self.assertFalse("error" in str(res).lower())

    def test_hours(self):
        self.query = 'select date_part_str(now_utc(), "hour") as hour, ' + \
                     'date_part_str(now_utc(),"minute") as minute, date_part_str(' + \
                     'now_utc(),"second") as sec, date_part_str(now_utc(),"millisecond") as msec'
        now = datetime.datetime.utcnow()
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["hour"] == now.hour or res["results"][0]["hour"] == (now.hour + 1),
                        "Result for hours expected: %s. Actual %s" % (now.hour, res["results"]))
        self.assertTrue("minute" in res["results"][0], "No minute field")
        self.assertTrue("sec" in res["results"][0], "No second field")
        self.assertTrue("msec" in res["results"][0], "No msec field")

    def test_where(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'select name, join_yr, join_mo, join_day from %s' % query_bucket + \
                         ' where date_part_str(now_str(),"month") < join_mo AND date_part_str(now_str(),"year")' + \
                         ' > join_yr AND date_part_str(now_str(),"day") < join_day'
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result["results"], key=lambda doc: (doc['name'],
                                                                              doc['join_yr'],
                                                                              doc['join_mo'],
                                                                              doc['join_day']))

            today = date.today()
            expected_result = [{"name": doc['name'], "join_yr": doc['join_yr'],
                                "join_mo": doc['join_mo'], "join_day": doc['join_day']}
                               for doc in self.full_list
                               if doc['join_yr'] < today.year and
                               doc['join_mo'] > today.month and
                               doc['join_day'] > today.day]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['join_yr'],
                                                                       doc['join_mo'],
                                                                       doc['join_day']))
            self._verify_results(actual_result, expected_result)

    def test_complex_where(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.run_cbq_query(query=f"CREATE INDEX ishix3 ON {query_bucket}(c5, c6, c7, c8, c9);")
            self.run_cbq_query(query=f"CREATE INDEX ishix4 ON {query_bucket}(c5, c8, c10) WHERE c6 != 1 AND c7 NOT IN [1,2] AND c11 != 0;")
            explain_plan = self.run_cbq_query(query=f"explain SELECT 1 FROM {query_bucket} WHERE c5 = 10 AND c8 >= 11 AND c8 <= 20 AND c6 != 1 AND c7 NOT IN [1,2] AND c11 != 0;")
            self.assertTrue("ishix4" in str(explain_plan), f"The wrong index is being picked up please check {explain_plan}")

    def test_prepared_date_where(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'select name, join_yr, join_mo, join_day from %s' % query_bucket + \
                         ' where date_part_str(now_str(),"month") < join_mo AND date_part_str(now_str(),"year")' + \
                         ' > join_yr AND date_part_str(now_str(),"day") < join_day'
            self.prepared_common_body()

    def test_now_millis(self):
        self.query = "select now_millis() as now"
        now = time.time()
        res = self.run_cbq_query()
        self.assertFalse("error" in str(res).lower())

    def test_str_to_millis(self):
        now_millis = time.time()
        now_time = datetime.datetime.fromtimestamp(now_millis)
        now_millis = now_millis * 1000
        try:
            now_time_zone = round(
                (round((datetime.datetime.now() - datetime.datetime.utcnow()).total_seconds()) // 1800) // 2)
        except AttributeError as ex:
            raise Exception("Test requires python 2.7 : SKIPPING TEST")
        now_time_str = "%s-%02d-%02d" % (now_time.year, now_time.month, now_time.day)
        self.query = "select str_to_millis('%s') as now" % now_time_str
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["now"] < now_millis and
                        (res["results"][0]["now"] > (now_millis - 5184000000)),
                        "Result expected to be in: [%s ... %s]. Actual %s" % (
                            now_millis - 5184000000, now_millis, res["results"]))

    def test_millis_to_str(self):
        now_millis = time.time()
        now_time = datetime.datetime.utcfromtimestamp(now_millis)
        expected = "%s-%02d-%02dT%02d:%02d" % (now_time.year, now_time.month, now_time.day,
                                               now_time.hour, now_time.minute)
        self.query = "select millis_to_utc(%s) as now" % (now_millis * 1000)
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))

    def test_date_part_millis(self):
        now_millis = time.time()
        now_time = datetime.datetime.utcfromtimestamp(now_millis)
        now_millis = now_millis * 1000
        self.query = 'select date_part_millis(%s, "hour", "UTC") as hour, ' % now_millis + \
                     'date_part_millis(%s,"minute", "UTC") as minute, date_part_millis(' % now_millis + \
                     '%s,"second") as sec, date_part_millis(%s,"millisecond", "UTC") as msec' % (now_millis, now_millis)
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["hour"] == now_time.hour,
                        "Result expected: %s. Actual %s" % (now_time.hour, res["results"]))
        self.assertTrue(res["results"][0]["minute"] == now_time.minute,
                        "Result expected: %s. Actual %s" % (now_time.minute, res["results"]))
        self.assertTrue(res["results"][0]["sec"] == now_time.second,
                        "Result expected: %s. Actual %s" % (now_time.second, res["results"]))
        self.assertTrue("msec" in res["results"][0], "There are no msec in results")

    def test_where_millis(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select join_yr, join_mo, join_day, name from %s" % query_bucket + \
                         " where join_mo < 10 and join_day < 10 and str_to_millis(tostr(join_yr) || '-0'" + \
                         " || tostr(join_mo) || '-0' || tostr(join_day)) < now_millis()"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result["results"], key=lambda doc: (doc['name'],
                                                                              doc['join_yr'],
                                                                              doc['join_mo'],
                                                                              doc['join_day']))

            expected_result = [{"name": doc['name'], "join_yr": doc['join_yr'],
                                "join_mo": doc['join_mo'], "join_day": doc['join_day']}
                               for doc in self.full_list
                               if doc['join_mo'] < 10 and doc['join_day'] < 10]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['join_yr'],
                                                                       doc['join_mo'],
                                                                       doc['join_day']))
            self._verify_results(actual_result, expected_result)

    def test_order_by_dates(self):
        self.fail_if_no_buckets()
        orders = ["asc", "desc"]
        for order in orders:
            for query_bucket in self.query_buckets:
                self.query = "select millis_to_str(str_to_millis('2010-01-01')) as date"
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result["results"][0]["date"][:10] == '2010-01-01',
                                'Actual result %s' % actual_result)
                self.query = "select join_yr, join_mo, join_day," \
                             " millis_to_str(str_to_millis(tostr(join_yr) || '-0' ||" + \
                             " tostr(join_mo) || '-0' || tostr(join_day))) as date from %s" % query_bucket + \
                             " where join_mo < 10 and join_day < 10 ORDER BY date %s" % order
                actual_result = self.run_cbq_query()
                actual_result = ([{"date": doc["date"][:10],
                                   "join_yr": doc['join_yr'],
                                   "join_mo": doc['join_mo'],
                                   "join_day": doc['join_day']} for doc in actual_result["results"]])

                expected_result = [{"date": '%s-0%s-0%s' % (doc['join_yr'],
                                                            doc['join_mo'], doc['join_day']),
                                    "join_yr": doc['join_yr'],
                                    "join_mo": doc['join_mo'],
                                    "join_day": doc['join_day']}
                                   for doc in self.full_list
                                   if doc['join_mo'] < 10 and doc['join_day'] < 10]
                expected_result = sorted(expected_result, key=lambda doc: (doc['date']), reverse=(order == 'desc'))
                self._verify_results(actual_result, expected_result)

    ##############################################################################################
    #
    #   TYPE FNS
    ##############################################################################################

    def test_type(self):
        self.fail_if_no_buckets()
        types_list = [("name", "string"), ("tasks_points", "object"),
                      ("some_wrong_key", "missing"),
                      ("skills", "array"), ("VMs[0].RAM", "number"),
                      ("true", "boolean"), ("test_rate", "number"),
                      ("test.test[0]", "missing")]
        for query_bucket in self.query_buckets:
            for name_item, type_item in types_list:
                self.query = 'SELECT TYPENAME(%s) as type_output FROM %s' % (
                    name_item, query_bucket)
                actual_result = self.run_cbq_query()
                for doc in actual_result['results']:
                    self.assertTrue(doc["type_output"] == type_item,
                                    "Expected type for %s: %s. Actual: %s" % (
                                        name_item, type_item, doc["type_output"]))
                self.log.info("Type for %s(%s) is checked." % (name_item, type_item))

    def test_check_types(self):
        self.fail_if_no_buckets()
        types_list = [("name", "ISSTR", True), ("skills[0]", "ISSTR", True),
                      ("test_rate", "ISSTR", False), ("VMs", "ISSTR", False),
                      ("false", "ISBOOL", True), ("join_day", "ISBOOL", False),
                      ("VMs", "ISARRAY", True), ("VMs[0]", "ISARRAY", False),
                      ("skills[0]", "ISARRAY", False), ("skills", "ISARRAY", True)]
        for query_bucket in self.query_buckets:
            for name_item, fn, expected_result in types_list:
                self.query = 'SELECT %s(%s) as type_output FROM %s' % (
                    fn, name_item, query_bucket)
                actual_result = self.run_cbq_query()
                for doc in actual_result['results']:
                    self.assertTrue(doc["type_output"] == expected_result,
                                    "Expected output for fn %s( %s) : %s. Actual: %s" % (
                                        fn, name_item, expected_result, doc["type_output"]))
                self.log.info("Fn %s(%s) is checked. (%s)" % (fn, name_item, expected_result))

    def test_types_in_satisfy(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name FROM %s d WHERE " % query_bucket + \
                         "(EVERY vm IN d.VMs SATISFIES ISOBJ(vm) END) AND" + \
                         " ISSTR(email) ORDER BY name"

            if self.analytics:
                self.query = "SELECT name FROM %s d WHERE " % query_bucket + \
                             "(EVERY vm IN d.VMs SATISFIES ISOBJ(vm) ) AND" + \
                             " ISSTR(email) ORDER BY name"

            actual_result = self.run_cbq_query()

            expected_result = [{"name": doc['name']}
                               for doc in self.full_list]

            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_to_num(self):
        self.query = 'SELECT tonum("12.12") - tonum("0.12") as num'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'][0]['num'] == 12,
                        "Expected: 12. Actual: %s" % (actual_result['results']))
        self.log.info("TONUM is checked")

    def test_to_str(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT TOSTR(join_mo) month FROM %s" % query_bucket
            actual_result = self.run_cbq_query()
            actual_result = actual_result['results']
            self.query = "SELECT REVERSE(TOSTR(join_mo)) rev_month FROM %s" % query_bucket
            actual_result1 = self.run_cbq_query()
            actual_result2 = actual_result1['results']
            expected_result = [{"month": str(doc['join_mo'])} for doc in self.full_list]
            expected_result2 = [{"rev_month": str(doc['join_mo'])[::-1]} for doc in self.full_list]
            self._verify_results(actual_result, expected_result)
            self._verify_results(actual_result2, expected_result2)

    def test_to_bool(self):
        self.query = 'SELECT tobool("true") as boo'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'][0]['boo'],
                        "Expected: true. Actual: %s" % (actual_result['results']))
        self.log.info("TOBOOL is checked")

    # Test has no usages anywhere
    def test_to_array(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT job_title, toarray(name) as names" + \
                         " FROM %s" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'], key=lambda doc: (doc['job_title'],
                                                                            doc['names']))
            expected_result = [{"job_title": doc["job_title"],
                                "names": [doc["name"]]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title'],
                                                                       doc['names']))
            self._verify_results(actual_result, expected_result)

    ##############################################################################################
    #
    #   CONCATENATION
    ##############################################################################################

    def test_concatenation(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT name || \" \" || job_title as employee" + \
                         " FROM %s" % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'], key=lambda doc: (doc['employee']))
            expected_result = [{"employee": doc["name"] + " " + doc["job_title"]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['employee']))
            self._verify_results(actual_result, expected_result)

    def test_concatenation_where(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'SELECT name, skills' + \
                         ' FROM %s WHERE skills[0]=("skill" || "2010")' % query_bucket
            actual_list = self.run_cbq_query()
            self.query = 'SELECT name, skills' + \
                         ' FROM %s WHERE reverse(skills[0])=("0102" || "lliks")' % query_bucket
            actual_list1 = self.run_cbq_query()
            actual_result = actual_list['results']
            actual_result2 = actual_list1['results']
            expected_result = [{"name": doc["name"], "skills": doc["skills"]}
                               for doc in self.full_list
                               if doc["skills"][0] == 'skill2010']
            self._verify_results(actual_result, expected_result)
            self._verify_results(actual_result, actual_result2)

    ##############################################################################################
    #
    #   SPLIT
    ##############################################################################################

    def test_select_split_fn(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT SPLIT(email, '@')[0] as login" + \
                         " FROM %s" % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"login": doc["email"].split('@')[0]}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_split_where(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'SELECT name FROM %s' % query_bucket + \
                         ' WHERE SPLIT(email, \'-\')[0] = SPLIT(name, \'-\')[1]'
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"]}
                               for doc in self.full_list
                               if doc["email"].split('-')[0] == doc["name"].split('-')[1]]
            self._verify_results(actual_result, expected_result)

    ##############################################################################################
    #
    #   UNION
    ##############################################################################################

    def test_union(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            query_bucket = self.get_collection_name(query_bucket)
            self.query = "select name from %s union select email from %s" % (query_bucket, query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"]}
                               for doc in self.full_list]
            expected_result.extend([{"email": doc["email"]}
                                    for doc in self.full_list])
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            self._verify_results(actual_result, expected_result)

    def test_prepared_union(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            query_bucket = self.get_collection_name(query_bucket)
            self.query = "select name from %s union select email from %s" % (query_bucket, query_bucket)
            self.prepared_common_body()

    def test_union_multiply_buckets(self):
        self.assertTrue(len(self.buckets) > 1, 'This test needs more than one bucket')
        self.query = "select name from %s union select email from %s" % (self.buckets[0].name, self.buckets[1].name)
        actual_list = self.run_cbq_query()
        actual_result = actual_list['results']
        expected_result = [{"name": doc["name"]}
                           for doc in self.full_list]
        expected_result.extend([{"email": doc["email"]}
                                for doc in self.full_list])
        expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
        self._verify_results(actual_result, expected_result)

    def test_union_all(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            query_bucket = self.get_collection_name(query_bucket)
            self.query = "select name from %s union all select email from %s" % (query_bucket, query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"]}
                               for doc in self.full_list]
            expected_result.extend([{"email": doc["email"]}
                                    for doc in self.full_list])
            self._verify_results(actual_result, expected_result)

    def test_union_all_multiply_buckets(self):
        self.assertTrue(len(self.buckets) > 1, 'This test needs more than one bucket')
        self.query = "select name from %s union all select email from %s" % (self.query_buckets[0],
                                                                             self.query_buckets[1])
        actual_list = self.run_cbq_query()
        actual_result = actual_list['results']
        expected_result = [{"name": doc["name"]}
                           for doc in self.full_list]
        expected_result.extend([{"email": doc["email"]}
                                for doc in self.full_list])
        self._verify_results(actual_result, expected_result)

    def test_union_where(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name from %s union select email from %s where join_mo > 2" % (query_bucket,
                                                                                               query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"]}
                               for doc in self.full_list]
            expected_result.extend([{"email": doc["email"]}
                                    for doc in self.full_list if doc["join_mo"] > 2])
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            self._verify_results(actual_result, expected_result)

    def test_union_where_covering(self):
        created_indexes = []
        ind_list = ["one", "two"]
        index_name = "one"
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for ind in ind_list:
                index_name = "coveringindex{0}".format(ind)
                if ind == "one":
                    self.query = "CREATE INDEX {0} ON {1}(name, email, join_mo)  USING {2}".format(index_name, query_bucket,
                                                                                            self.index_type)
                elif ind == "two":
                    self.query = "CREATE INDEX {0} ON {1}(email, join_mo) USING {2}".format(index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for query_bucket in self.query_buckets:
            self.query = "explain select name from {0} where name is not null union select email from {0} where email " \
                         "is not null and join_mo >2 ".format(query_bucket)
            if self.covering_index:
                self.check_explain_covering_index(index_name[0])
            self.query = "select name from {0} where name is not null union select email from {0} where email is not " \
                         "null and join_mo >2".format(query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"]} for doc in self.full_list]
            expected_result.extend([{"email": doc["email"]} for doc in self.full_list if doc["join_mo"] > 2])
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            self._verify_results(actual_result, expected_result)
            for index_name in created_indexes:
                try:
                    self.query = "DROP INDEX {0} ON {1} USING {2}".format(index_name, query_bucket, self.index_type)
                    self.run_cbq_query()
                except Exception as e:
                    self.log.error("Drop index failed {0}".format(str(e)))
            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "select name from {0} where name is not null union select email from {0} where email is not " \
                         "null and join_mo >2".format(query_bucket)
            result = self.run_cbq_query()
            diffs = DeepDiff(actual_result, result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            #self.assertEqual(actual_result, sorted(result['results']))
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def test_union_aggr_fns(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select count(name) as names from %s union select count(email) as " \
                         "emails from %s" % (query_bucket, query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"names": len(self.full_list)}]
            expected_result.extend([{"emails": len(self.full_list)}])
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            self._verify_results(actual_result, expected_result)

    def test_union_aggr_fns_covering(self):
        created_indexes = []
        ind_list = ["one", "two"]
        index_name = "one"
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for ind in ind_list:
                index_name = "coveringindex%s" % ind
                if ind == "one":
                    self.query = "CREATE INDEX %s ON %s(name, email, join_day) USING %s" % (index_name,
                                                                                            query_bucket,
                                                                                            self.index_type)
                elif ind == "two":
                    self.query = "CREATE INDEX %s ON %s(email)  USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for query_bucket in self.query_buckets:
            self.query = "explain select count(name) as names from %s where join_day is not null union select count(" \
                         "email) as emails from %s where email is not null" % (query_bucket, query_bucket)
            if self.covering_index:
                self.check_explain_covering_index(index_name)
            self.query = "select count(name) as names from %s where join_day is not null union select count(email) " \
                         " as emails from %s where email is not null" % (query_bucket, query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"names": len(self.full_list)}]
            expected_result.extend([{"emails": len(self.full_list)}])
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            self._verify_results(actual_result, expected_result)
            for index_name in created_indexes:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "select count(name) as names from %s where join_day is not null union select count(email)  " \
                         " as emails from %s where email is not null" % (
                             query_bucket, query_bucket)
            result = self.run_cbq_query()
            self.assertEqual(actual_result, sorted(result['results']))
            self.query = "DROP PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()

        # ############# META NEW ###################

    def test_meta_basic(self):
        created_indexes = []
        ind_list = ["one"]
        index_name = "one"
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for ind in ind_list:
                index_name = "metaindex%s" % ind
                if ind == "one":
                    self.query = "CREATE INDEX %s ON %s(meta().id,meta().cas)  USING %s" % (
                        index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = "explain select meta().id, meta().cas from {0} where meta().id is not null order by meta(" \
                         ").id limit 10".format(query_bucket)
            if self.covering_index:
                self.check_explain_covering_index(index_name[0])
            self.query = "select meta().id, meta().cas from {0} where meta().id is not null order by " \
                         "meta().id limit 10".format(query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            for index_name in created_indexes:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()
            self.covering_index = False
            self.query = "CREATE PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()
            self._wait_for_index_online(bucket, '#primary')
            self.query = "select meta().id, meta().cas from {0} use index(`#primary`) where meta().id is not null " \
                         "order by meta().id limit 10".format(query_bucket)
            expected_list = self.run_cbq_query()
            diffs = DeepDiff(actual_result, expected_list['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()

    def test_meta_where(self):
        created_indexes = []
        ind_list = ["one"]
        index_name = "one"
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for ind in ind_list:
                index_name = "meta_where%s" % ind
                if ind == "one":
                    self.query = "CREATE INDEX {0} ON {1}(meta().id,meta().cas)  where meta().id like " \
                                 "'query-testemployee6%' USING {2}".format(index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = "explain select meta().id, meta().cas from {0} where meta().id like 'query-testemployee6%' " \
                         "order by meta().id limit 10".format(
                query_bucket)
            if self.covering_index:
                self.check_explain_covering_index(index_name[0])
            self.query = "select meta().id, meta().cas from {0} where meta().id like 'query-testemployee6%' order by " \
                         "meta().id limit 10".format(query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            for index_name in created_indexes:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()
            self.covering_index = False
            self.query = "CREATE PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()
            self._wait_for_index_online(bucket, '#primary')
            self.query = "select meta().id, meta().cas from {0} where meta().id like 'query-testemployee6%' order by " \
                         "meta().id limit 10".format(query_bucket)
            expected_list = self.run_cbq_query()
            diffs = DeepDiff(actual_result, expected_list['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()

    def test_meta_ambiguity(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "create index idx on %s(META())" % query_bucket
            self.run_cbq_query()
            self.query = "create index idx2 on {0}(META())".format(query_bucket)
            self.run_cbq_query()
            self.query = "SELECT  META() as meta_c FROM %s  ORDER BY meta_c limit 10" % query_bucket
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['status'] == "success")
            self.query = "SELECT  META(test) as meta_c FROM %s as test  ORDER BY meta_c limit 10" % query_bucket
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['status'] == "success")
            self.query = "SELECT META(t1).id as id FROM " + self.query_buckets[0] + "  t1 JOIN " + \
                         self.query_buckets[0] + "  t2 ON KEYS t1.id;"
            self.assertTrue(actual_result['status'] == "success")
            self.query = "drop index idx ON %s" % query_bucket
            self.run_cbq_query()
            self.query = "drop index idx2 ON %s" % query_bucket
            self.run_cbq_query()

    def test_meta_where_greater_than(self):
        created_indexes = []
        ind_list = ["one"]
        index_name = "one"
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for ind in ind_list:
                index_name = "meta_where%s" % ind
                if ind == "one":
                    self.query = "CREATE INDEX {0} ON {1}(meta().id,meta().cas)  where meta().id >10 USING {2}".format(
                        index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = "explain select meta().id, meta().cas from {0} where meta().id >10 order by meta().id".format(
                query_bucket)
            if self.covering_index:
                self.check_explain_covering_index(index_name[0])
            self.query = "select meta().id, meta().cas from {0} where meta().id >10 order by meta().id limit 10".format(
                query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            for index_name in created_indexes:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()
            self.covering_index = False
            self.query = "CREATE PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()
            self._wait_for_index_online(bucket, '#primary')
            self.query = "select meta().id, meta().cas from {0} use index(`#primary`)  where meta().id > 10 order by " \
                         "meta().id limit 10".format(query_bucket)
            expected_list = self.run_cbq_query()
            diffs = DeepDiff(actual_result, expected_list['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()

    def test_meta_partial(self):
        created_indexes = []
        ind_list = ["one"]
        index_name = "one"
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for ind in ind_list:
                index_name = "meta_where%s" % ind
                if ind == "one":
                    self.query = "CREATE INDEX {0} ON {1}(meta().id, name)  where meta().id >10 USING {2}".format(
                        index_name, query_bucket, self.index_type)
                # if self.gsi_type:
                #     self.query += "WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)

        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = "explain select meta().id, name from {0} where meta().id >10 and name is not null order by " \
                         "meta().id limit 10".format(query_bucket)
            if self.covering_index:
                self.check_explain_covering_index(index_name[0])
            self.query = "select meta().id, name from {0} where meta().id >10 and name is not null order by " \
                         "meta().id limit 10".format(query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            for index_name in created_indexes:
                self.query = "DROP INDEX %s ON %s USING %s" % (index_name, query_bucket, self.index_type)
                self.run_cbq_query()
            self.covering_index = False
            self.query = "CREATE PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()
            self._wait_for_index_online(bucket, '#primary')
            self.query = "select meta().id, name from {0} use index(`#primary`) where meta().id > 10 and name is not " \
                         "null order by meta().id limit 10".format(query_bucket)
            expected_list = self.run_cbq_query()
            diffs = DeepDiff(actual_result, expected_list['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON %s" % query_bucket
            self.run_cbq_query()

    def test_meta_non_supported(self):
        created_indexes = []
        ind_list = ["one"]
        index_name = "one"
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            for ind in ind_list:
                index_name = "meta_cas_%s" % ind
                if ind == "one":
                    queries_errors = {'CREATE INDEX ONE ON ' + self.query_buckets[0] + ' (meta().cas) using GSI': (
                        'syntax error', 3000),
                        'CREATE INDEX ONE ON ' + self.query_buckets[0] + ' (meta().flags) using GSI': (
                            'syntax error', 3000),
                        'CREATE INDEX ONE ON ' + self.query_buckets[
                            0] + ' (meta().expiration) using GSI': (
                            'syntax error', 3000),
                        'CREATE INDEX ONE ON ' + self.query_buckets[0] + ' (meta().cas) using VIEW': (
                            'syntax error', 3000)}

    def test_meta_negative_namespace(self):
        created_indexes = []
        ind_list = ["one"]
        index_name = "one"
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            for ind in ind_list:
                index_name = "meta_cas_%s" % ind
                if ind == "one":
                    queries_errors = {
                        'CREATE INDEX TWO ON ' + self.query_buckets[0] + ' (meta(invalid).id) using GSI': (
                            'syntax error', 3000),
                        'CREATE INDEX THREE ON ' + self.query_buckets[0] + ' (meta(invalid).id) using VIEW': (
                            'syntax error', 3000),
                        'CREATE INDEX FOUR ON ' + self.query_buckets[0] + ' (meta()) using GSI': ('syntax error', 3000),
                        'CREATE INDEX FIVE ON ' + self.query_buckets[0] + ' (meta()) using VIEW': (
                            'syntax error', 3000)}
                    self.negative_common_body(queries_errors)

    # ####################### META NEW END ######################################

    def test_intersect(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name from %s intersect select name from %s s where s.join_day>5" % (
                query_bucket, query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"]}
                               for doc in self.full_list if doc['join_day'] > 5]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            self._verify_results(actual_result, expected_result)

    def test_intersect_covering(self):
        created_indexes = []
        ind_list = ["one", "two"]
        index_name = "one"
        self.fail_if_no_buckets()
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            for ind in ind_list:
                index_name = "coveringindex{0}".format(ind)
                if ind == "one":
                    self.query = "CREATE INDEX {0} ON {1}(job_title, name)  USING {2}".format(index_name, query_bucket, self.index_type)
                elif ind == "two":
                    self.query = "CREATE INDEX {0} ON {1}(join_day, name)  USING {2}".format(index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                created_indexes.append(index_name)
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = "explain select name from {0} where job_title='Engineer' intersect select name from {0} s where s.join_day>5".format(query_bucket)
            if self.covering_index:
                self.check_explain_covering_index(index_name)
            self.query = "select name from {0} where job_title='Engineer' intersect select name from {0} s where s.join_day>5".format(query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"]}
                               for doc in self.full_list if doc['join_day'] > 5]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            self._verify_results(actual_result, expected_result)

            for ind in ind_list:
                index_name = "coveringindex{0}".format(ind)
                try:
                    self.query = "DROP INDEX {0} ON {1} USING {2}".format(index_name, query_bucket, self.index_type)
                    self.run_cbq_query()
                except Exception as e:
                    self.log.error("Drop index failed {0}".format(str(e)))
            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()
            self._wait_for_index_online(bucket, '#primary')
            self.query = "select name from {0} where job_title='Engineer' intersect select name from {0} s where s.join_day>5".format(query_bucket, query_bucket)
            expected_list = self.run_cbq_query()
            diffs = DeepDiff(actual_result, expected_list['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def test_intersect_all(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name from %s intersect all select name from %s s where s.join_day>5" % (
                query_bucket, query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"]}
                               for doc in self.full_list if doc['join_day'] > 5]
            self._verify_results(actual_result, expected_result)

    def test_prepared_intersect(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name from %s intersect all select name from %s s where s.join_day>5" % (
                query_bucket, query_bucket)
            self.prepared_common_body()

    def test_except_secondsetempty(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "drop primary index on %s USING %s" % (query_bucket, self.primary_indx_type)
            self.run_cbq_query()
        try:
            self.query = "(select id keyspace_id from system:keyspaces) except (select indexes.keyspace_id from " \
                         "system:indexes) "
            actual_list = self.run_cbq_query()
            bucket_names = []
            for bucket in self.buckets:
                bucket_names.append(bucket.name)
            count = 0
            for _ in self.query_buckets:
                if actual_list['results'][count]['keyspace_id'] in bucket_names:
                    count += 1
                else:
                    self.log.error("Wrong keyspace id returned or empty keyspace id returned")
        finally:
            for query_bucket in self.query_buckets:
                self.query = "create primary index on %s" % query_bucket
                self.run_cbq_query()

    def test_except(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name from %s except select name from %s s where s.join_day>5" % (
                query_bucket, query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"]}
                               for doc in self.full_list if not doc['join_day'] > 5]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            self._verify_results(actual_result, expected_result)

    def test_except_all(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name from %s except all select name from %s s where s.join_day>5" % (
                query_bucket, query_bucket)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"]}
                               for doc in self.full_list if not doc['join_day'] > 5]
            self._verify_results(actual_result, expected_result)

    ##############################################################################################
    #
    #   WITHIN
    ##############################################################################################

    def test_within_list_object(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, VMs from %s WHERE 5 WITHIN VMs" % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"], "VMs": doc["VMs"]}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"] if vm["RAM"] == 5])]
            self._verify_results(actual_result, expected_result)

    def test_prepared_within_list_object(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            query_bucket = self.get_collection_name(query_bucket)
            self.query = "select name, VMs from %s WHERE 5 WITHIN VMs" % query_bucket
            self.prepared_common_body()

    def test_within_list_of_lists(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, VMs from %s where name within [['employee-2', 'employee-4']," \
                         " ['employee-5']] " % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"], "VMs": doc["VMs"]}
                               for doc in self.full_list
                               if doc["name"] in ['employee-2', 'employee-4', 'employee-5']]
            self._verify_results(actual_result, expected_result)

    def test_within_object(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, tasks_points from %s WHERE 1 WITHIN tasks_points" % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"], "tasks_points": doc["tasks_points"]}
                               for doc in self.full_list
                               if doc["tasks_points"]["task1"] == 1 or doc["tasks_points"]["task2"] == 1]
            self._verify_results(actual_result, expected_result)

    def test_within_array(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = " select name, skills from %s where 'skill2010' within skills" % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"], "skills": doc["skills"]}
                               for doc in self.full_list
                               if 'skill2010' in doc["skills"]]
            self._verify_results(actual_result, expected_result)

    ##############################################################################################
    #
    #   RAW
    ##############################################################################################

    def test_raw(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select raw name from %s " % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            self.query = "select raw reverse(reverse(name)) from %s " % query_bucket
            actual_list1 = self.run_cbq_query()
            actual_result1 = actual_list1['results']
            expected_result = [doc["name"] for doc in self.full_list]
            self._verify_results(actual_result, expected_result)
            self._verify_results(actual_result, actual_result1)

    def test_raw_limit(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select raw skills[0] from %s limit 5" % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [doc["skills"][0] for doc in self.full_list][:5]
            self._verify_results(actual_result, expected_result)

    def test_raw_order(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select raw name from {0} order by name {1}".format(query_bucket, "desc")
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [doc["name"]
                               for doc in self.full_list]
            diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "select raw name from {0} order by name {1}".format(query_bucket, "asc")
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [doc["name"] for doc in self.full_list]
            self._verify_results(actual_result, expected_result)
            self.query = "select raw meta().id from {0} order by meta().id {1}".format(query_bucket, "asc")
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = sorted(actual_result)
            self.assertEqual(actual_result, expected_result)
            self.query = "select raw meta().id from {0} order by meta().id {1}".format(query_bucket, "desc")
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = sorted(actual_result, reverse=True)
            self.assertEqual(actual_result, expected_result)

    def test_push_limit(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'insert into %s(KEY, VALUE) VALUES ("f01", {"f1":"f1"})' % query_bucket
            self.run_cbq_query()
            self.query = 'insert into %s(KEY, VALUE) VALUES ("f02", {"f1":"f1","f2":"f2"})' % query_bucket
            self.run_cbq_query()
            self.query = 'create index if1 on %s(f1)' % query_bucket
            self.query = 'select q.id, q.f1,q.f2 from (select meta().id, f1,f2 from %s where f1="f1") q where q.f2 = ' \
                         '"f2" limit 1' % query_bucket
            result = self.run_cbq_query()
            self.assertTrue(result['metrics']['resultCount'] == 1)
            self.query = 'delete from %s use keys["f01","f02"]' % query_bucket
            self.run_cbq_query()

    def test_limit_offset(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.run_cbq_query(query=f'CREATE INDEX ix10 ON {query_bucket}(c0)')
            self.run_cbq_query(query=f'INSERT INTO {query_bucket} VALUES ("k01", {{"c0":0, "c1":"1", "c2":1}})')
            self.run_cbq_query(query=f'INSERT INTO {query_bucket} VALUES ("k02", {{"c0":0, "c1":"1", "c2":2}})')
            self.run_cbq_query(query=f'INSERT INTO {query_bucket} VALUES ("k03", {{"c0":0, "c1":"1", "c2":3}})')
            self.run_cbq_query(query=f'INSERT INTO {query_bucket} VALUES ("k04", {{"c0":0, "c1":"1", "c2":4}})')
            self.run_cbq_query(query=f'INSERT INTO {query_bucket} VALUES ("k05", {{"c0":0, "c1":"1", "c2":5}})')
            results1 = self.run_cbq_query(query=f'SELECT c1, c2 FROM {query_bucket} USE INDEX(ix10) WHERE c0 >= 0  ORDER BY c1 OFFSET 1 LIMIT 1')
            results2 = self.run_cbq_query(query=f'SELECT c1, c2 FROM {query_bucket} USE INDEX(ix10) WHERE c0 >= 0  ORDER BY c1 OFFSET 2 LIMIT 1')
            results3 = self.run_cbq_query(query=f'SELECT c1, c2 FROM {query_bucket} USE INDEX(ix10) WHERE c0 >= 0  ORDER BY c1 OFFSET 3 LIMIT 1')
            self.assertEqual(results1['results'],[{'c1': '1', 'c2': 2}])
            self.assertEqual(results2['results'],[{'c1': '1', 'c2': 3}])
            self.assertEqual(results3['results'],[{'c1': '1', 'c2': 4}])
    def test_limit_offset_early_order(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.run_cbq_query(query=f'CREATE INDEX ix10 ON {query_bucket}(c0,c1)')
            self.run_cbq_query(query=f'INSERT INTO {query_bucket} VALUES ("k01", {{"c0":0, "c1":"1", "c2":1}})')
            self.run_cbq_query(query=f'INSERT INTO {query_bucket} VALUES ("k02", {{"c0":0, "c1":"1", "c2":2}})')
            self.run_cbq_query(query=f'INSERT INTO {query_bucket} VALUES ("k03", {{"c0":0, "c1":"1", "c2":3}})')
            self.run_cbq_query(query=f'INSERT INTO {query_bucket} VALUES ("k04", {{"c0":0, "c1":"1", "c2":4}})')
            self.run_cbq_query(query=f'INSERT INTO {query_bucket} VALUES ("k05", {{"c0":0, "c1":"1", "c2":5}})')
            results1 = self.run_cbq_query(
                query=f'SELECT c1, c2 FROM {query_bucket} USE INDEX(ix10) WHERE c0 >= 0  ORDER BY c1 OFFSET 1 LIMIT 1')
            results2 = self.run_cbq_query(
                query=f'SELECT c1, c2 FROM {query_bucket} USE INDEX(ix10) WHERE c0 >= 0  ORDER BY c1 OFFSET 2 LIMIT 1')
            results3 = self.run_cbq_query(
                query=f'SELECT c1, c2 FROM {query_bucket} USE INDEX(ix10) WHERE c0 >= 0  ORDER BY c1 OFFSET 3 LIMIT 1')
            self.assertEqual(results1['results'], [{'c1': '1', 'c2': 2}])
            self.assertEqual(results2['results'], [{'c1': '1', 'c2': 3}])
            self.assertEqual(results3['results'], [{'c1': '1', 'c2': 4}])

    ##############################################################################################
    #
    #  Number fns
    ##############################################################################################
    # This test has no usages anywhere
    def test_abs(self):
        for query_bucket in self.query_buckets:
            self.query = "select join_day from %s where join_day > abs(-10)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [doc["join_day"] for doc in self.full_list
                               if doc["join_day"] > abs(-10)]
            self._verify_results(actual_result, expected_result)

    # This test has no usages anywhere
    def test_acos(self):
        self.query = "select degrees(acos(0.5))"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 60}]
        self._verify_results(actual_list['results'], expected_result)

    # Test has no usages anywhere
    def test_asin(self):
        self.query = "select degrees(asin(0.5))"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 30}]
        self._verify_results(actual_list['results'], expected_result)

    # Test has no usages anywhere
    def test_tan(self):
        self.query = "select tan(radians(45))"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 1}]
        self._verify_results(actual_list['results'], expected_result)

    # This test has no usages anywhere
    def test_ln(self):
        self.query = "select ln(10) = ln(2) + ln(5)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': True}]
        self._verify_results(actual_list['results'], expected_result)

    # This test has no usages anywhere
    def test_power(self):
        self.query = "select power(sin(radians(33)), 2) + power(cos(radians(33)), 2)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 1}]
        self._verify_results(actual_list['results'], expected_result)

    # Test has no usages anywhere
    def test_sqrt(self):
        self.query = "select sqrt(9)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 3}]
        self._verify_results(actual_list['results'], expected_result)

    # This test has no uses anywhere
    def test_sign(self):
        self.query = "select sign(-5)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': -1}]
        self._verify_results(actual_list['results'], expected_result)
        self.query = "select sign(5)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 1}]
        self._verify_results(actual_list['results'], expected_result)
        self.query = "select sign(0)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 0}]
        self._verify_results(actual_list['results'], expected_result)

    ##############################################################################################
    #
    #   CONDITIONAL FNS
    ##############################################################################################

    def test_nanif(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select join_day, join_mo, NANIF(join_day, join_mo) as equality" + \
                         " from %s" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"join_day": doc["join_day"], "join_mo": doc["join_mo"],
                                "equality": doc["join_day"] if doc["join_day"] != doc["join_mo"] else 'NaN'}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_posinf(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select join_day, join_mo, POSINFIF(join_day, join_mo) as equality" + \
                         " from %s" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"join_day": doc["join_day"], "join_mo": doc["join_mo"],
                                "equality": doc["join_day"] if doc["join_day"] != doc["join_mo"] else '+Infinity'}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    ##############################################################################################
    #
    #   String FUNCTIONS
    ##############################################################################################

    def test_uuid(self):
        self.query = "select uuid() as uuid"
        actual_list = self.run_cbq_query()
        self.assertTrue('uuid' in actual_list['results'][0] and actual_list['results'][0]['uuid'],
                        'UUid is not working')

    def test_string_fn_negative(self):
        queries_errors = {'select name from %s when contains(VMs, "Sale")': ('syntax error', 3000),
                          'select TITLE(test_rate) as OS from %s': ('syntax error', 3000),
                          'select REPEAT(name, -2) as name from %s': ('syntax error', 3000),
                          'select REPEAT(name, a) as name from %s': ('syntax error', 3000), }
        self.negative_common_body(queries_errors)

    def test_contains(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name from %s where contains(job_title, 'Sale')" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']

            self.query = "select name from %s where contains(reverse(job_title), reverse('Sale'))" % query_bucket
            actual_list1 = self.run_cbq_query()
            actual_result1 = actual_list1['results']
            diffs = DeepDiff(actual_result, actual_result1, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

            expected_result = [{"name": doc["name"]}
                               for doc in self.full_list
                               if doc['job_title'].find('Sale') != -1]
            self._verify_results(actual_result, expected_result)

    def test_initcap(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select INITCAP(VMs[0].os) as OS from %s" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"OS": (doc["VMs"][0]["os"][0].upper() + doc["VMs"][0]["os"][1:])}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_title(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select TITLE(VMs[0].os) as OS from %s" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            self.query = "select TITLE(REVERSE(VMs[0].os)) as rev_os from %s" % query_bucket

            actual_list1 = self.run_cbq_query()
            actual_result1 = actual_list1['results']

            expected_result = [{"OS": (doc["VMs"][0]["os"][0].upper() + doc["VMs"][0]["os"][1:])}
                               for doc in self.full_list]
            expected_result1 = [{"rev_os": (doc["VMs"][0]["os"][::-1][0].upper() + doc["VMs"][0]["os"][::-1][1:])} for
                                doc in self.full_list]
            self._verify_results(actual_result, expected_result)
            self._verify_results(actual_result1, expected_result1)

    def test_prepared_title(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select TITLE(VMs[0].os) as OS from %s" % query_bucket
            self.prepared_common_body()

    def test_position(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select POSITION(VMs[1].name, 'vm') pos from %s" % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"pos": (doc["VMs"][1]["name"].find('vm'))} for doc in self.full_list]
            self._verify_results(actual_result, expected_result)
            self.query = "select POSITION(VMs[1].name, 'server') pos from %s" % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"pos": (doc["VMs"][1]["name"].find('server'))}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

        test_word = 'california'
        for letter in test_word:
            actual = self.run_position_query(test_word, letter)
            expected = test_word.find(letter)
            self.assertEqual(actual, expected)

        letter = ''
        actual = self.run_position_query(test_word, letter)
        expected = test_word.find(letter)
        self.assertEqual(actual, expected)

        letter = 'd'
        actual = self.run_position_query(test_word, letter)
        expected = test_word.find(letter)
        self.assertEqual(actual, expected)

    def test_position0(self):
        test_word = 'california'
        for letter in test_word:
            actual = self.run_position_query(test_word, letter, position_type='0')
            expected = test_word.find(letter)
            self.assertEqual(actual, expected)

        letter = ''
        actual = self.run_position_query(test_word, letter, position_type='0')
        expected = test_word.find(letter)
        self.assertEqual(actual, expected)

        letter = 'd'
        actual = self.run_position_query(test_word, letter, position_type='0')
        expected = test_word.find(letter)
        self.assertEqual(actual, expected)

    def test_position1(self):
        test_word = 'california'
        for letter in test_word:
            actual = self.run_position_query(test_word, letter, position_type='1')
            expected = test_word.find(letter) + 1
            self.assertEqual(actual, expected)

        letter = ''
        actual = self.run_position_query(test_word, letter, position_type='1')
        expected = test_word.find(letter) + 1
        self.assertEqual(actual, expected)

        letter = 'd'
        actual = self.run_position_query(test_word, letter, position_type='1')
        expected = test_word.find(letter) + 1
        self.assertEqual(actual, expected)

    def test_regex_contains(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select email from %s where REGEXP_CONTAINS(email, '-m..l')" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            self.query = "select email from %s where REGEXP_CONTAINS(reverse(email), 'l..m-')" % query_bucket
            actual_list1 = self.run_cbq_query()
            actual_result1 = actual_list1['results']
            diffs = DeepDiff(actual_result, actual_result1, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"email": doc["email"]}
                               for doc in self.full_list
                               if len(re.compile('-m..l').findall(doc['email'])) > 0]
            self._verify_results(actual_result, expected_result)

    def test_regex_like(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select email from %s where REGEXP_LIKE(email, '.*-mail.*')" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']

            expected_result = [{"email": doc["email"]}
                               for doc in self.full_list
                               if re.compile('.*-mail.*').search(doc['email'])]
            self._verify_results(actual_result, expected_result)

    def test_regex_position(self):
        test_word = 'california'
        for letter in test_word:
            actual = self.run_regex_query(test_word, letter)
            expected = test_word.find(letter)
            self.assertEqual(actual, expected)

        letter = ''
        actual = self.run_regex_query(test_word, letter)
        expected = test_word.find(letter)
        self.assertEqual(actual, expected)

        letter = 'd'
        actual = self.run_regex_query(test_word, letter)
        expected = test_word.find(letter)
        self.assertEqual(actual, expected)

    def test_regex_position0(self):
        test_word = 'california'
        for letter in test_word:
            actual = self.run_regex_query(test_word, letter, regex_type='0')
            expected = test_word.find(letter)
            self.assertEqual(actual, expected)

        letter = ''
        actual = self.run_regex_query(test_word, letter, regex_type='0')
        expected = test_word.find(letter)
        self.assertEqual(actual, expected)

        letter = 'd'
        actual = self.run_regex_query(test_word, letter, regex_type='0')
        expected = test_word.find(letter)
        self.assertEqual(actual, expected)

    def test_regex_position1(self):
        test_word = 'california'
        for letter in test_word:
            actual = self.run_regex_query(test_word, letter, regex_type='1')
            expected = test_word.find(letter) + 1
            self.assertEqual(actual, expected)

        letter = ''
        actual = self.run_regex_query(test_word, letter, regex_type='1')
        expected = test_word.find(letter) + 1
        self.assertEqual(actual, expected)

        letter = 'd'
        actual = self.run_regex_query(test_word, letter, regex_type='1')
        expected = test_word.find(letter)
        self.assertEqual(actual, expected)

    def test_regex_replace(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, REGEXP_REPLACE(email, '-mail', 'domain') as mail from %s" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"],
                                "mail": doc["email"].replace('-mail', 'domain')}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)
            self.query = "select name, REGEXP_REPLACE(email, 'e', 'a', 2) as mail from %s" % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"],
                                "mail": doc["email"].replace('e', 'a', 2)}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_replace(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, REPLACE(email, 'a', 'e', 1) as mail from %s" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"],
                                "mail": doc["email"].replace('a', 'e', 1)}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_repeat(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select REPEAT(name, 2) as name from %s" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"] * 2}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    ##############################################################################################
    #
    #   LET
    ##############################################################################################

    def test_let_nums(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select test_r, test_r > 2 compare from %s let test_r = (test_rate / 2)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"test_r": doc["test_rate"] / 2,
                                "compare": (doc["test_rate"] / 2) > 2}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_prepared_let_nums(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select test_r, test_r > 2 compare from %s let test_r = (test_rate / 2)" % query_bucket
            self.prepared_common_body()

    def test_let_string(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "select name, join_date as date from %s let join_date = tostr(join_yr) || '-' || tostr(" \
                         "join_mo)" % query_bucket
            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"name": doc["name"],
                                "date": '%s-%s' % (doc['join_yr'], doc['join_mo'])}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

            self.query = "select name, join_date as date from %s let join_date = reverse(tostr(join_yr)) || '-' || " \
                         "reverse(tostr(join_mo)) order by name, meta().id limit 10" % query_bucket
            actual_list2 = self.run_cbq_query()
            actual_result2 = actual_list2['results']
            expected_result2 = [{'date': '1102-9', 'name': 'employee-1'}, {'date': '1102-9', 'name': 'employee-1'},
                                {'date': '1102-9', 'name': 'employee-1'}, {'date': '1102-9', 'name': 'employee-1'},
                                {'date': '1102-9', 'name': 'employee-1'}, {'date': '1102-9', 'name': 'employee-1'},
                                {'date': '0102-4', 'name': 'employee-1'}, {'date': '0102-4', 'name': 'employee-1'},
                                {'date': '0102-4', 'name': 'employee-1'}, {'date': '0102-4', 'name': 'employee-1'}]
            self._verify_results(actual_result2, expected_result2)

    def test_letting(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT join_mo, sum_test from %s WHERE join_mo>7 group by join_mo letting sum_test = sum(" \
                         "tasks_points.task1)" % query_bucket
            if self.analytics:
                self.query = "SELECT d.join_mo, sum_test from %s d WHERE d.join_mo>7 group by d.join_mo letting " \
                             "sum_test = sum(d.tasks_points.task1)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            tmp_groups = {doc['join_mo'] for doc in self.full_list if doc['join_mo'] > 7}
            expected_result = [{"join_mo": group,
                                "sum_test": sum([x["tasks_points"]["task1"] for x in self.full_list
                                                 if x["join_mo"] == group])}
                               for group in tmp_groups]
            self._verify_results(actual_result, expected_result)

    def test_prepared_letting(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = "SELECT join_mo, sum_test from %s WHERE join_mo>7 group by join_mo letting sum_test = sum(" \
                         "tasks_points.task1)" % query_bucket
            self.prepared_common_body()

    # https://issues.couchbase.com/browse/MB-26086
    def check_special_symbols(self):
        self.fail_if_no_buckets()
        symbols = ['~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '-', '+', '=', '{', '[', '}', ']', '|',
                   '.', ':', ';', '"', '<', ',', '>', '.', '?', '/']
        errors_simple = {}
        errors_complex = {}
        query = "INSERT INTO " + self.query_buckets[0] + "  VALUES ('simple', {"

        for i in range(len(symbols)):
            query += "'a" + symbols[i] + "b':1"
            if i < len(symbols) - 1:
                query += ","
        query += "})"
        self.run_cbq_query(query)

        for i in range(len(symbols)):
            query = "SELECT `a" + symbols[i] + "b` FROM " + self.query_buckets[0] + "  USE KEYS ['simple'] WHERE `a" + \
                    symbols[i] + "b` IS NOT MISSING"
            result = self.run_cbq_query(query)
            if result['metrics']['resultCount'] < 1:
                errors_simple[symbols[i]] = query
        # Assuming I have only 3 special characters: ~!@ the resulting query will be like
        # INSERT INTO default VALUES ('complex', {'a~b':{'a~b':12, 'a!b':12, 'a@b':12},
        #                                         'a!b':{'a~b':12, 'a!b':12, 'a@b':12},
        #                                         'a@b':{'a~b':12, 'a!b':12, 'a@b':12 }})
        query = "INSERT INTO " + self.query_buckets[0] + "  VALUES ('complex', {"
        for i in range(len(symbols)):
            query += "'a" + symbols[i] + "b':{"
            for j in range(len(symbols)):
                query += "'a" + symbols[j] + "b':12"
                if j < len(symbols) - 1:
                    query += ","
                else:
                    query += "}"
            if i < len(symbols) - 1:
                query += ","
        query += "})"
        self.run_cbq_query(query)

        for i in range(len(symbols)):
            for j in range(len(symbols)):
                query = "SELECT `a" + symbols[i] + "b`.`a" + symbols[j] + \
                        "b` FROM " + self.query_buckets[0] + "  USE KEYS ['complex'] WHERE `a" + symbols[i] + \
                        "b`.`a" + symbols[j] + "b` IS NOT MISSING"
                result = self.run_cbq_query(query)
                if result['metrics']['resultCount'] < 1:
                    errors_complex[str(symbols[i]) + str(symbols[j])] = query

        self.assertEqual(len(errors_simple) + len(errors_complex), 0)

    # MB-63267
    def test_limit_index_aggregate(self):
        self.fail_if_no_buckets()
        query = 'SELECT type, COUNT(1) FROM `travel-sample` GROUP BY type LIMIT 2'
        result = self.run_cbq_query(query)
        self.log.info(result['results'])
        self.assertTrue(result['results'][0]['type'] in ['airline', 'airport', 'hotel', 'route'])
        self.assertTrue(result['results'][1]['type'] in ['airline', 'airport', 'hotel', 'route'])
        self.assertNotEqual(result['results'][0]['type'], result['results'][1]['type'])
        self.assertTrue(result['results'][0]['$1'] > 0 and result['results'][1]['$1'] > 0)

    # MB-62824
    def test_prepared_array_binding(self):
        self.fail_if_no_buckets()
        prepare = 'prepare p22 AS with aa AS ([1,2,3]) SELECT d, aa FROM [1,2] AS d'
        expected = [
            {"d": 1, "aa": [1, 2, 3]},
            {"d": 2, "aa": [1, 2, 3]}
        ]
        self.run_cbq_query(prepare)
        # first run
        result = self.run_cbq_query('execute p22')
        self.log.info(result['results'])
        self.assertEqual(result['results'], expected)
        # second run
        result = self.run_cbq_query('execute p22')
        self.log.info(result['results'])
        self.assertEqual(result['results'], expected)
    # MB-64484
    def test_prepared_hints(self):
        self.fail_if_no_buckets()
        prepare = 'prepare prepared_hint FROM SELECT * FROM `travel-sample` USE INDEX (def_city) WHERE city = "Los Angeles";'
        expected = 'SELECT * FROM `travel-sample` USE INDEX (def_city) WHERE city = "Los Angeles";'
        self.run_cbq_query(prepare)
        # We are looking to see that the hints followed section does not display any syntax errors in a 2 node environment
        hints_followed = self.run_cbq_query('select * from system:prepareds')
        self.assertTrue("['INDEX(`travel-sample` `def_city`)']" in str(hints_followed), f"We expect the indexer hint to be backticked, please check the plan: {hints_followed}")
        actual_result = self.run_cbq_query('execute prepared_hint')
        expected_result = self.run_cbq_query(expected)
        diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    # MB-65246
    def test_array_agg_perf(self):
        self.fail_if_no_buckets()
        self.run_cbq_query('CREATE INDEX ix1 ON default(c1, a1)')
        upsert_query = '''
        UPSERT INTO default (KEY v.id, VALUE v)
        SELECT {"c1": 1, "id": "k"||SUBSTR("0000000"||TOSTR(d),-7), "a1": ["aa","bb","cc"]} AS v
        FROM ARRAY_RANGE(0,32000) AS d
        '''
        self.run_cbq_query(upsert_query)
        array_agg_query = 'SELECT d1.c1 FROM default AS d1 WHERE d1.c1 = 1 GROUP BY d1.c1 LETTING aa = ARRAY_AGG(d1.a1)'
        expected_result = [{'c1': 1}]
        # Query should run within 2 seconds
        result = self.run_cbq_query(array_agg_query, query_params={'timeout': '2s'})
        self.assertEqual(result['results'], expected_result)

    def test_array_agg_distinct(self):
        self.fail_if_no_buckets()
        self.run_cbq_query('DROP INDEX ix1 IF EXISTS ON default')
        self.run_cbq_query('DELETE FROM default WHERE c1 is not missing')
        upsert_query = '''
        UPSERT INTO default (KEY k, VALUE t)
        SELECT "k"|| TOSTR(d) AS k, {"c1":IMOD(d,500), "o1" : OBJECT "o"||TOSTR(o):o FOR o IN ARRAY_RANGE(0,25) END} AS t
        FROM ARRAY_RANGE(0,2000) AS d
        '''
        self.run_cbq_query(upsert_query)
        array_agg_distinct_query = 'SELECT d.c1, ARRAY_AGG(DISTINCT d) AS a1 FROM (SELECT d1.* FROM default AS d1 ) AS d GROUP BY d.c1 ORDER BY d.c1 LIMIT 100'
        result = self.run_cbq_query(array_agg_distinct_query)
        self.assertEqual(result['metrics']['resultCount'], 100)

    def test_unnest_array_memory_quota(self):
        self.fail_if_no_buckets()
        upsert_query = '''
        UPSERT INTO default VALUES ("kk01", { "a1": ARRAY { "sa1": [ { "id": IDIV(d1,3), "type": "doc" } ] } FOR d1 IN ARRAY_RANGE(1,1200) END });
        '''
        self.run_cbq_query(upsert_query)
        unnest_query = 'SELECT DISTINCT u1.* FROM default AS d USE KEYS "kk01" UNNEST d.a1 AS u1 UNNEST u1.sa1 AS u2 WHERE u2.id = 300 AND u2.type = "doc"'
        result = self.run_cbq_query(unnest_query, query_params={'memory_quota': 1000})
        expected_result = [{"sa1": [{"id": 300, "type": "doc"}]}]
        self.assertEqual(result['results'], expected_result)

    # MB-65862
    def test_array_distinct_null_values(self):
        self.fail_if_no_buckets()
        # insert data into default collection
        upsert_query = 'UPSERT INTO default(KEY k, VALUE v) SELECT "k"||TO_STR(d) AS k, {"cc1":IMOD(d,2), "cc2":null} AS v FROM ARRAY_RANGE(0,4) AS d'
        self.run_cbq_query(upsert_query)
        # query the data with array_distinct
        query = 'SELECT  d1.cc1, ARRAY_DISTINCT(ARRAY_AGG(d1)) AS aa FROM (SELECT d.* FROM default AS d WHERE cc1 = 0) AS d1 GROUP BY d1.cc1'
        result = self.run_cbq_query(query)
        expected_result = [{"cc1": 0, "aa": [{"cc1": 0, "cc2": None}]}]
        self.assertEqual(result['results'], expected_result)

    def test_unnest_aggregate(self):
        self.fail_if_no_buckets()
        # create collection
        self.run_cbq_query('CREATE COLLECTION default._default.test_unnest_aggregate IF NOT EXISTS')
        self.run_cbq_query('UPSERT INTO default._default.test_unnest_aggregate VALUES("k001", {"id":100, "a1" : [{"c1":10}, {"c1":10}, {"c1":20}]})')

        # create index on collection
        self.run_cbq_query('CREATE INDEX ix11 IF NOT EXISTS ON default._default.test_unnest_aggregate(ALL ARRAY u.c1 FOR u IN a1 END,id)')

        # query the data with array_distinct
        query = 'SELECT  d.id, SUM(u.c1) as c1_cnt FROM default._default.test_unnest_aggregate AS d UNNEST d.a1 AS u WHERE u.c1 > 0 GROUP BY d.id ORDER BY d.id'
        result = self.run_cbq_query(query)
        expected_result = [{"id": 100, "c1_cnt": 40}]
        self.assertEqual(result['results'], expected_result)

    # MB-66660
    def test_prepared_cte(self):
        self.fail_if_no_buckets()

        # prepare the query
        prepare = '''
            PREPARE p66660 FROM
            WITH w1 AS ( [{"a":"f1", "b":10}]),
                 w2 AS (SELECT RAW OBJECT v.a:v.b FOR v IN (SELECT x.* FROM w1 AS x) END)
            SELECT w2
        '''
        self.run_cbq_query(prepare)

        node1 = self.servers[0]
        node2 = self.servers[1]

        # execute the query
        result_node1 = self.run_cbq_query('execute p66660', server=node1)
        result_node2 = self.run_cbq_query('execute p66660', server=node2)
        self.log.info(result_node1['results'])
        self.log.info(result_node2['results'])
        expected_result = [
            {"w2": [{"f1": 10}]}
        ]
        self.assertEqual(result_node1['results'], expected_result)
        self.assertEqual(result_node2['results'], expected_result)

    # MB-66691
    def test_join_memory_quota(self):
        self.fail_if_no_buckets()
        # create collection
        self.run_cbq_query('CREATE COLLECTION default._default.test_join_memory_quota IF NOT EXISTS')

        # insert data into collection
        upsert_query = '''
        UPSERT INTO test_join_memory_quota VALUES ("k01", { "a1": ARRAY { "id": TO_STR(d1), "type": RPAD("a",100,"b") } FOR d1 IN ARRAY_RANGE(1,5000) END })
        '''
        self.run_cbq_query(upsert_query, query_context='default._default')
        
        # run query with memory quota 1000
        query = 'SELECT b.* FROM test_join_memory_quota AS a USE KEYS "k01" UNNEST a1 AS b JOIN test_join_memory_quota AS c ON KEYS b.id'
        result = self.run_cbq_query(query, query_params={'memory_quota': 1000}, query_context='default._default')
        self.assertEqual(result['status'], 'success')

    def test_prepared_nested_subquery(self):
        self.fail_if_no_buckets()

        # prepare query
        prepare_query = 'PREPARE p66660b FROM SELECT a, (SELECT RAW (SELECT RAW a) ) AS b FROM [1,2,3] a'      
        self.run_cbq_query(prepare_query)

        node1 = self.servers[0]
        node2 = self.servers[1]

        expected_result = [
            {"a": 1, "b": [[1]]},
            {"a": 2, "b": [[2]]},
            {"a": 3, "b": [[3]]}
        ]
        # execute query
        result_node1 = self.run_cbq_query('EXECUTE p66660b', server=node1)
        result_node2 = self.run_cbq_query('EXECUTE p66660b', server=node2)  
        self.log.info(f"result_node1: {result_node1['results']}")
        self.log.info(f"result_node2: {result_node2['results']}")
        self.assertEqual(result_node1['results'], expected_result)
        self.assertEqual(result_node2['results'], expected_result)
