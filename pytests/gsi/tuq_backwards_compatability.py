from remote.remote_util import RemoteMachineShellConnection
from .upgrade_gsi import UpgradeSecondaryIndex
from membase.api.rest_client import RestConnection, RestHelper
import time

class UpgradeBackwardsCollections(UpgradeSecondaryIndex):
    def setUp(self):
        super(UpgradeBackwardsCollections, self).setUp()

    def tearDown(self):
        super(UpgradeBackwardsCollections, self).tearDown()

    def test_backwards_compatability(self):
        create_index_query = "CREATE INDEX idx_name ON {0}(name)".format(
            self.bucket_name)
        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        create_index_query = "CREATE INDEX idx_day ON {0}(join_day)".format(
            self.bucket_name)
        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.wait_until_indexes_online()

        result = self.n1ql_helper.run_cbq_query(query='SELECT * FROM {0} where name = "employee-9"'.format(self.bucket_name))
        self.assertEqual(result['metrics']['resultCount'], 72)

        result2 = self.n1ql_helper.run_cbq_query(query='SELECT * FROM {0} where join_day = 9'.format(self.bucket_name))
        self.assertEqual(result2['metrics']['resultCount'], 72)

        upgrade_nodes = self.servers[:self.nodes_init]

        for server in upgrade_nodes:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()
            upgrade_threads = self._async_update(self.upgrade_to, [server])
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            self.upgrade_servers.append(server)
        self.sleep(180)
        msg = "Cluster is not healthy after upgrade"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        self.log.info("Cluster is healthy")
        rest = RestConnection(self.master)
        nodes_all = rest.node_statuses()
        try:
            for cluster_node in nodes_all:
                if cluster_node.ip == self.master.ip:
                    self.log.info("Adding Back: {0}".format(self.master.ip))
                    rest.add_back_node(cluster_node.id)
                    rest.set_recovery_type(otpNode=cluster_node.id,
                                       recoveryType="full")
        except Exception as e:
            self.log.error(str(e))
        self.log.info("Adding node back to cluster...")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        rebalance.result()
        self.assertTrue(self.wait_until_indexes_online(), "Some indexes are not online")
        self.log.info("All indexes are online")
        self.add_built_in_server_user()
        self.sleep(20)

        try:
            create_index_query = "CREATE INDEX idx_name ON {0}(name)".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        except Exception as e:
            self.log.info("indexes already exist")
        try:
            create_index_query = "CREATE INDEX idx_day ON {0}(join_day)".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
            self.wait_until_indexes_online()
        except Exception as e:
            self.log.info("indexes already exist")

        result = self.n1ql_helper.run_cbq_query(query='SELECT * FROM {0} where name = "employee-9"'.format(self.bucket_name))
        self.assertEqual(result['metrics']['resultCount'], 72)

        result2 = self.n1ql_helper.run_cbq_query(query='SELECT * FROM {0} where join_day = 9'.format(self.bucket_name))
        self.assertEqual(result2['metrics']['resultCount'], 72)

        result = self.n1ql_helper.run_cbq_query(query='SELECT * FROM default:{0}._default._default where name = "employee-9"'.format(self.bucket_name))
        self.assertEqual(result['metrics']['resultCount'], 72)

        result2 = self.n1ql_helper.run_cbq_query(query='SELECT * FROM default:{0}._default._default where join_day = 9'.format(self.bucket_name))
        self.assertEqual(result2['metrics']['resultCount'], 72)

        result = self.n1ql_helper.run_cbq_query(query='SELECT * FROM _default where name = "employee-9"', query_context='default:{0}._default'.format(self.bucket_name))
        self.assertEqual(result['metrics']['resultCount'], 72)

        result2 = self.n1ql_helper.run_cbq_query(query='SELECT * FROM _default where join_day = 9', query_context='default:{0}._default'.format(self.bucket_name))
        self.assertEqual(result2['metrics']['resultCount'], 72)

    def test_backwards_compatability_indexes(self):
        create_index_query = "CREATE INDEX idx_name ON {0}(name)".format(
            self.bucket_name)
        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.wait_until_indexes_online()

        result = self.n1ql_helper.run_cbq_query(query='SELECT * FROM {0} where name = "employee-9"'.format(self.bucket_name))
        self.assertEqual(result['metrics']['resultCount'], 72)


        upgrade_nodes = self.servers[:self.nodes_init]

        for server in upgrade_nodes:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()
            upgrade_threads = self._async_update(self.upgrade_to, [server])
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            self.upgrade_servers.append(server)
        self.sleep(180)
        msg = "Cluster is not healthy after upgrade"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        self.log.info("Cluster is healthy")
        rest = RestConnection(self.master)
        nodes_all = rest.node_statuses()
        try:
            for cluster_node in nodes_all:
                if cluster_node.ip == self.master.ip:
                    self.log.info("Adding Back: {0}".format(self.master.ip))
                    rest.add_back_node(cluster_node.id)
                    rest.set_recovery_type(otpNode=cluster_node.id,
                                           recoveryType="full")
        except Exception as e:
            self.log.error(str(e))
        self.log.info("Adding node back to cluster...")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        rebalance.result()
        self.assertTrue(self.wait_until_indexes_online(), "Some indexes are not online")
        self.log.info("All indexes are online")
        self.add_built_in_server_user()
        self.sleep(20)

        try:
            create_index_query = "CREATE INDEX idx_name ON {0}(name)".format(
                self.bucket_name)
        except Exception as e:
            self.log.info("indexes already exist")
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
            create_index_query = "CREATE INDEX idx_day ON {0}(join_day)".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
            self.wait_until_indexes_online()
        except Exception as e:
            self.log.info("indexes already exist")

        result = self.n1ql_helper.run_cbq_query(query='SELECT * FROM {0} where name = "employee-9"'.format(self.bucket_name))
        self.assertEqual(result['metrics']['resultCount'], 72)

        result2 = self.n1ql_helper.run_cbq_query(query='SELECT * FROM {0} where join_day = 9'.format(self.bucket_name))
        self.assertEqual(result2['metrics']['resultCount'], 72)

        self.n1ql_helper.create_scope(server=self.master, bucket_name=self.bucket_name, scope_name="test")
        self.n1ql_helper.create_collection(server=self.master, bucket_name=self.bucket_name, scope_name="test", collection_name="test1")
        self.n1ql_helper.create_collection(server=self.master, bucket_name=self.bucket_name, scope_name="test", collection_name="test2")

        self.n1ql_helper.run_cbq_query(
            query=('INSERT INTO default:{0}.test.test1'.format(self.bucket_name) + '(KEY, VALUE) VALUES ("key2", { "type" : "hotel", "name" : "new hotel" })'))
        self.n1ql_helper.run_cbq_query(
            query=('INSERT INTO default:{0}.test.test1'.format(self.bucket_name) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "old hotel" })'))
        self.n1ql_helper.run_cbq_query(
            query=('INSERT INTO default:{0}.test.test1'.format(self.bucket_name) + ' (KEY, VALUE) VALUES ("key3", { "nested" : {"fields": "fake"}, "name" : "old hotel" })'))
        self.n1ql_helper.run_cbq_query(
            query=('INSERT INTO default:{0}.test.test1'.format(self.bucket_name) + ' (KEY, VALUE) VALUES ("key4", { "numbers": [1,2,3,4] , "name" : "old hotel" })'))
        time.sleep(20)

        self.n1ql_helper.run_cbq_query(
            query="CREATE INDEX idx1 on default:{0}.test.test1(name) ".format(self.bucket_name))
        self.n1ql_helper.run_cbq_query(
            query="CREATE INDEX idx2 on default:{0}.test.test1(name) ".format(self.bucket_name))
        self.n1ql_helper.run_cbq_query(
            query="CREATE INDEX idx3 on default:{0}.test.test1(nested)".format(self.bucket_name))
        self.n1ql_helper.run_cbq_query(query="CREATE INDEX idx4 on default:{0}.test.test1(ALL numbers)".format(self.bucket_name))


    def test_backwards_compatability_prepared(self):
        create_index_query = "CREATE INDEX idx_name ON {0}(name)".format(
            self.bucket_name)
        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        create_index_query = "CREATE INDEX idx_day ON {0}(join_day)".format(
            self.bucket_name)
        self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        self.wait_until_indexes_online()

        self.n1ql_helper.run_cbq_query(query='PREPARE p1 as SELECT * FROM {0} where name = "employee-9"'.format(self.bucket_name))
        result = self.n1ql_helper.run_cbq_query(query='EXECUTE p1')
        self.assertEqual(result['metrics']['resultCount'], 72)

        self.n1ql_helper.run_cbq_query(query='PREPARE p2 as SELECT * FROM {0} where join_day = 9'.format(self.bucket_name))
        result2 = self.n1ql_helper.run_cbq_query(query='EXECUTE p2')
        self.assertEqual(result2['metrics']['resultCount'], 72)

        upgrade_nodes = self.servers[:self.nodes_init]

        for server in upgrade_nodes:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.disconnect()
            upgrade_threads = self._async_update(self.upgrade_to, [server])
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            self.upgrade_servers.append(server)
        self.sleep(180)
        msg = "Cluster is not healthy after upgrade"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        self.log.info("Cluster is healthy")
        rest = RestConnection(self.master)
        nodes_all = rest.node_statuses()
        try:
            for cluster_node in nodes_all:
                if cluster_node.ip == self.master.ip:
                    self.log.info("Adding Back: {0}".format(self.master.ip))
                    rest.add_back_node(cluster_node.id)
                    rest.set_recovery_type(otpNode=cluster_node.id,
                                           recoveryType="full")
        except Exception as e:
            self.log.error(str(e))
        self.log.info("Adding node back to cluster...")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        rebalance.result()
        self.assertTrue(self.wait_until_indexes_online(), "Some indexes are not online")
        self.log.info("All indexes are online")
        self.add_built_in_server_user()
        self.sleep(20)

        try:
            create_index_query = "CREATE INDEX idx_name ON {0}(name)".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
        except Exception as e:
            self.log.info("indexes already exist")
        try:
            create_index_query = "CREATE INDEX idx_day ON {0}(join_day)".format(
                self.bucket_name)
            self.n1ql_helper.run_cbq_query(query=create_index_query, server=self.n1ql_node)
            self.wait_until_indexes_online()
        except Exception as e:
            self.log.info("indexes already exist")

        # Make sure we are able to create prepared statements after the upgrade on default bucket
        try:
            self.n1ql_helper.run_cbq_query(query='PREPARE p3 as SELECT * FROM {0}.`_default`.`_default` where name = "employee-9"'.format(self.bucket_name))
        except Exception as e:
            self.log.info("Let's try prepare again in case could not find scope on first try")
            self.n1ql_helper.run_cbq_query(query='PREPARE p3 as SELECT * FROM {0}.`_default`.`_default` where name = "employee-9"'.format(self.bucket_name))
        try:
            self.n1ql_helper.run_cbq_query(query='PREPARE p4 as SELECT * FROM {0}.`_default`.`_default` where join_day = 9'.format(self.bucket_name))
        except Exception as e:
            self.log.info("Let's try prepare again in case could not find scope onf first try")
            self.n1ql_helper.run_cbq_query(query='PREPARE p4 as SELECT * FROM {0}.`_default`.`_default` where join_day = 9'.format(self.bucket_name))

        result = self.n1ql_helper.run_cbq_query(query='EXECUTE p1')
        self.assertEqual(result['metrics']['resultCount'], 72)

        result2 = self.n1ql_helper.run_cbq_query(query='EXECUTE p2')
        self.assertEqual(result2['metrics']['resultCount'], 72)

        result = self.n1ql_helper.run_cbq_query(query='EXECUTE p3')
        self.assertEqual(result['metrics']['resultCount'], 72)

        result2 = self.n1ql_helper.run_cbq_query(query='EXECUTE p4')
        self.assertEqual(result2['metrics']['resultCount'], 72)

        self.n1ql_helper.create_scope(server=self.master, bucket_name=self.bucket_name, scope_name="test")
        self.n1ql_helper.create_collection(server=self.master, bucket_name=self.bucket_name, scope_name="test", collection_name="test1")
        self.n1ql_helper.create_collection(server=self.master, bucket_name=self.bucket_name, scope_name="test", collection_name="test2")

        self.n1ql_helper.run_cbq_query(
            query=('INSERT INTO default:{0}.test.test1'.format(self.bucket_name) + '(KEY, VALUE) VALUES ("key2", { "type" : "hotel", "name" : "new hotel" })'))
        self.n1ql_helper.run_cbq_query(
            query=('INSERT INTO default:{0}.test.test1'.format(self.bucket_name) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "old hotel" })'))
        time.sleep(20)

        self.n1ql_helper.run_cbq_query(
            query="CREATE INDEX idx1 on default:{0}.test.test1(name) ".format(self.bucket_name))
        time.sleep(20)

        #Create a prepared statement on a collection and make sure this works post upgrade
        self.n1ql_helper.run_cbq_query(query='PREPARE p5 as SELECT * FROM {0}.test.test1 where name = "new hotel"'.format(self.bucket_name))
        result2 = self.n1ql_helper.run_cbq_query(query='EXECUTE p5')
        self.assertEqual(result2['metrics']['resultCount'], 1)
