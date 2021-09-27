
from .tuq import QueryTests
from collection.collections_n1ql_client import CollectionsN1QL
from membase.api.rest_client import RestConnection, RestHelper
import time

class QuerySystemKeyspacesTests(QueryTests):
    def setUp(self):
        super(QuerySystemKeyspacesTests, self).setUp()
        self.log.info("==============  QuerySanityTests setup has started ==============")
        self.collections_helper = CollectionsN1QL(self.master)
        self.recreate_keyspace = self.input.param('recreate_keyspace', False)
        self.use_deferred = self.input.param('use_deferred', False)
        self.use_replicas = self.input.param('use_replicas', False)
        self.use_partitioned = self.input.param('use_partitioned', False)
        self.use_partial = self.input.param('use_partial', False)
        self.use_query_context = self.input.param('use_query_context', False)
        self.move_index = self.input.param('move_index', False)
        self.use_partial_partitioned = self.input.param('use_partial_partitioned', False)
        self.log.info("==============  QuerySanityTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QuerySystemKeyspacesTests, self).suite_setUp()
        self.log.info("==============  QuerySanityTests suite_setup has started ==============")
        self.log.info("==============  QuerySanityTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QuerySanityTests tearDown has started ==============")
        self.log.info("==============  QuerySanityTests tearDown has completed ==============")
        super(QuerySystemKeyspacesTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QuerySanityTests suite_tearDown has started ==============")
        self.log.info("==============  QuerySanityTests suite_tearDown has completed ==============")
        super(QuerySystemKeyspacesTests, self).suite_tearDown()

    # Create 10 scopes and 10 collections per scope, make sure all appear in system:all_keyspaces
    def test_keyspaces_basic(self):
        keyspace_names = []
        for x in range(0, 10):
            self.collections_helper.create_scope(bucket_name="default", scope_name='scope' + str(x))
            for y in range(0, 10):
                self.collections_helper.create_collection(bucket_name="default", scope_name='scope' + str(x), collection_name="collection" + str(y))
                keyspace_names.append(('scope' + str(x), 'collection' + str(y)))
        self.verify_all_keyspaces(keyspace_names)

    def test_drop_keyspace(self):
        self.collections_helper.create_scope(bucket_name="default", scope_name='scope1')
        self.collections_helper.create_scope(bucket_name="default", scope_name='scope2')
        self.collections_helper.create_collection(bucket_name="default", scope_name='scope2',
                                                      collection_name="collection7")
        for i in range (0,10):
            self.collections_helper.create_collection(bucket_name="default", scope_name='scope1',
                                                      collection_name="collection" + str(i))
        self.run_cbq_query(query="DROP collection default:default.scope1.collection7")
        self.sleep(2)
        results = self.run_cbq_query(query="select * from system:all_keyspaces")
        for result in results['results']:
            if result['all_keyspaces']['name'] == "collection7" and result['all_keyspaces']['scope'] == "scope1":
                self.log.error("Keyspace found when it should have been deleted!")
                self.assertTrue(False, "Keyspace found when it should have been deleted! {0}".format(result))
        if self.recreate_keyspace:
            found = False
            self.collections_helper.create_collection(bucket_name="default", scope_name='scope1',
                                                      collection_name="collection7")
            self.sleep(2)
            results = self.run_cbq_query(query="select * from system:all_keyspaces")
            for result in results['results']:
                if result['all_keyspaces']['name'] == "collection7" and result['all_keyspaces']['scope'] == "scope1":
                    found = True
                    break
            self.assertTrue(found, "Recreated collection not found! {0}".format(results))

    def test_special_character(self):
        keyspace_names = []
        for x in range(0, 10):
            self.collections_helper.create_scope(bucket_name="default", scope_name='scope' + str(x))
            for y in range(0, 7):
                self.collections_helper.create_collection(bucket_name="default", scope_name='scope' + str(x), collection_name="collection" + str(y))
                keyspace_names.append(('scope' + str(x), 'collection' + str(y)))

        self.collections_helper.create_collection(bucket_name="default", scope_name='scope0',
                                                  collection_name="`coll-ection`")
        self.collections_helper.create_collection(bucket_name="default", scope_name='scope0',
                                                  collection_name="`coll%ection`")
        self.collections_helper.create_collection(bucket_name="default", scope_name='scope0',
                                                  collection_name="`colle_ction`")

        self.verify_all_keyspaces(keyspace_names)

    def test_drop_scope(self):
        for x in range(0, 10):
            self.collections_helper.create_scope(bucket_name="default", scope_name='scope' + str(x))
            for y in range(0, 10):
                self.collections_helper.create_collection(bucket_name="default", scope_name='scope' + str(x), collection_name="collection" + str(y))
        self.collections_helper.delete_scope(bucket_name="default", scope_name='scope1')
        self.sleep(2)
        results = self.run_cbq_query(query="select * from system:all_keyspaces where `scope` = 'scope1'")
        self.assertTrue(results['metrics']['resultCount'] == 0, "Found a keyspace from the scope that was dropped! {0}".format(results))
        results = self.run_cbq_query(query="select * from system:all_keyspaces")
        self.assertFalse(results['metrics']['resultCount'] == 0, "There should be keyspaces in the system!".format(results))

    def test_delete_bucket(self):
        found = False
        found_keyspace = ()
        for x in range(0, 10):
            self.collections_helper.create_scope(bucket_name="default", scope_name='scope' + str(x))
            for y in range(0, 10):
                self.collections_helper.create_collection(bucket_name="default", scope_name='scope' + str(x), collection_name="collection" + str(y))
        self.ensure_bucket_does_not_exist("default")
        results = self.run_cbq_query(query="select * from system:all_keyspaces")
        for result in results['results']:
            for i in range(0, 10):
                if 'scope' in result['all_keyspaces']:
                    if result['all_keyspaces']['scope'] == "scope{0}".format(i):
                        found = True
                        found_keyspace = (result['all_keyspaces']['scope'], result['all_keyspaces']['name'])
                        break
        self.assertFalse(found, "Should be no scopes left in system:all_keyspaces! {0}".format(found_keyspace))

    def test_system_scopes(self):
        found = False
        for x in range(0, 10):
            self.collections_helper.create_scope(bucket_name="default", scope_name='scope' + str(x))
            results = self.run_cbq_query(query="select * from system:scopes")
            for result in results['results']:
                if result['scopes']['name'] == "scope{0}".format(x):
                    found = True
                    break
            if not found:
                self.assertTrue(False, "Scope was not found in system:scopes {0}".format(results))

    def test_rebalance_keyspaces(self):
        keyspace_names = []
        for x in range(0, 10):
            self.collections_helper.create_scope(bucket_name="default", scope_name='scope' + str(x))
            for y in range(0, 10):
                self.collections_helper.create_collection(bucket_name="default", scope_name='scope' + str(x), collection_name="collection" + str(y))
                keyspace_names.append(('scope' + str(x), 'collection' + str(y)))

        # Rebalance in a data node
        rebalance = self.cluster.async_rebalance(self.servers, [self.servers[2]], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        self.verify_all_keyspaces(keyspace_names)

        # Rebalance out a data node
        rebalance = self.cluster.async_rebalance(self.servers, [], [self.servers[2]])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        self.verify_all_keyspaces(keyspace_names)

    def test_system_indexes_basic(self):
        index_names = []
        self.collections_helper.create_scope(bucket_name="default", scope_name='scope1')
        for y in range(0, 10):
            self.collections_helper.create_collection(bucket_name="default", scope_name='scope1', collection_name="collection" + str(y))
        for i in range(0, 10):
            if self.use_deferred:
                self.run_cbq_query("CREATE INDEX idx{0} on default:default.scope1.collection{0}(fake) WITH {{'defer_build':true}}".format(i))
            elif self.use_replicas:
                self.run_cbq_query("CREATE INDEX idx{0} on default:default.scope1.collection{0}(fake) WITH {{'num_replica': 1}}".format(i))
            elif self.use_partitioned:
                self.run_cbq_query("CREATE INDEX idx{0} on default:default.scope1.collection{0}(fake) PARTITION BY HASH(META().id)".format(i))
            elif self.use_partial:
                self.run_cbq_query("CREATE INDEX idx{0} on default:default.scope1.collection{0}(fake) WHERE fake = 'fakevalue'".format(i))
            elif self.use_partial_partitioned:
                self.run_cbq_query("CREATE INDEX idx{0} on default:default.scope1.collection{0}(fake) PARTITION BY HASH(META().id) where fake = 'fakevalue'".format(i))
            else:
                self.run_cbq_query("CREATE INDEX idx{0} on default:default.scope1.collection{0}(fake)".format(i))
            index_names.append(("idx" + str(i), "collection" + str(i)))
        if not self.use_deferred:
            self.wait_for_all_indexes_online()
        self.verify_all_indexes(index_names)

        if self.use_deferred:
            for i in range(0, 10):
                self.run_cbq_query("BUILD INDEX ON default:default.scope1.collection{0}(idx{0})".format(i))
            self.wait_for_all_indexes_online()
            self.verify_all_indexes(index_names)

    def test_system_indexes_alter(self):
        index_names = []
        self.collections_helper.create_scope(bucket_name="default", scope_name='scope1')
        for y in range(0, 10):
            self.collections_helper.create_collection(bucket_name="default", scope_name='scope1',
                                                      collection_name="collection" + str(y))
        for i in range(0, 10):
            if self.move_index:
                self.run_cbq_query("CREATE INDEX idx{0} on default:default.scope1.collection{0}(fake) WITH {{'nodes': '{1}:{2}'}}".format(i, self.servers[1].ip, self.servers[1].port))
            else:
                self.run_cbq_query("CREATE INDEX idx{0} on default:default.scope1.collection{0}(fake)".format(i))
            index_names.append(("idx" + str(i), "collection" + str(i)))

        if self.use_query_context:
            alter_index_query = 'ALTER INDEX collection1.idx1 WITH {"action":"replica_count", "num_replica": 1}'
            self.run_cbq_query(query=alter_index_query, query_context="default:default.scope1")
        elif self.move_index:
            alter_index_query = 'ALTER INDEX default:default.scope1.collection1.idx1 WITH {{"action":"move","nodes": "{0}:{1}"}}'.format(self.master.ip, self.master.port)
            self.run_cbq_query(query=alter_index_query)
        else:
            alter_index_query = 'ALTER INDEX default:default.scope1.collection1.idx1 WITH {"action":"replica_count", "num_replica": 1}'
            self.run_cbq_query(query=alter_index_query)

        time.sleep(5)
        self.wait_for_all_indexes_online()
        self.verify_all_indexes(index_names)

    def test_system_indexes_rebalance(self):
        index_names = []
        self.collections_helper.create_scope(bucket_name="default", scope_name='scope1')
        for y in range(0, 10):
            self.collections_helper.create_collection(bucket_name="default", scope_name='scope1',
                                                      collection_name="collection" + str(y))
        for i in range(0, 10):
            self.run_cbq_query("CREATE INDEX idx{0} on default:default.scope1.collection{0}(fake) WITH {{'nodes': '{1}:{2}'}}".format(i, self.servers[1].ip, self.servers[1].port))
            index_names.append(("idx" + str(i), "collection" + str(i)))

        # Rebalance in an index node
        rebalance = self.cluster.async_rebalance(self.servers, [self.servers[2]], [], services=["index"])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        self.verify_all_indexes(index_names)

        # Rebalance out an index node
        rebalance = self.cluster.async_rebalance(self.servers, [], [self.servers[1]])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()

        time.sleep(5)
        self.wait_for_all_indexes_online()
        self.verify_all_indexes(index_names)


    def test_system_indexes_bucket(self):
        found = False
        self.run_cbq_query(query="CREATE index idx1 on default(fake)")
        time.sleep(5)
        self.wait_for_all_indexes_online()
        results = self.run_cbq_query(query="select * from system:all_indexes")
        for result in results['results']:
            if result['all_indexes']['name'] == "idx1":
                found = True
                break
        self.assertTrue(found, "Idx1 not found! {0}".format(results))

    def verify_all_indexes(self, index_names):
        found = False
        results = self.run_cbq_query(query="select * from system:all_indexes")
        for index in index_names:
            found = False
            for result in results['results']:
                if result['all_indexes']['name'] == index[0] and result['all_indexes']['scope_id'] == "scope1" and result['all_indexes']['keyspace_id'] == index[1]:
                    found = True
                    break
                else:
                    found = False
                    missing_index = index
            if not found:
                self.log.error("index not found!")
                break
        self.assertTrue(found, "index not found! {0} ".format(missing_index))

    def verify_all_keyspaces(self, keyspace_names):
        found = False
        results = self.run_cbq_query(query="select * from system:all_keyspaces")
        for keyspace in keyspace_names:
            found = False
            for result in results['results']:
                if result['all_keyspaces']['name'] == keyspace[1] and result['all_keyspaces']['scope'] == keyspace[0]:
                    found = True
                    break
                else:
                    found = False
                    missing_keyspace = keyspace
            if not found:
                self.log.error("keyspace not found!")
                break
        self.assertTrue(found, "Keyspace not found! {0} ".format(missing_keyspace))