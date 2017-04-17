from base_2i import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection, RestHelper
import random
from lib import testconstants
from lib.remote.remote_util import RemoteMachineShellConnection
from threading import Thread


class GSIReplicaIndexesTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(GSIReplicaIndexesTests, self).setUp()
        self.rest = RestConnection(self.servers[0])
        self.n1ql_server = self.get_nodes_from_services_map(service_type="n1ql",
                                                            get_all_nodes=False)
        self.create_primary_index = False
        shell = RemoteMachineShellConnection(self.servers[0])
        info = shell.extract_remote_info().type.lower()
        if info == 'linux':
            self.cli_command_location = testconstants.LINUX_COUCHBASE_BIN_PATH
        elif info == 'windows':
            self.cmd_ext = ".exe"
            self.cli_command_location = testconstants.WIN_COUCHBASE_BIN_PATH_RAW
        elif info == 'mac':
            self.cli_command_location = testconstants.MAC_COUCHBASE_BIN_PATH
        else:
            raise Exception("OS not supported.")

        self.expected_err_msg = self.input.param("expected_err_msg", None)
        self.nodes = self.input.param("nodes", None)
        self.override_default_num_replica_with_num = self.input.param(
            "override_with_num", 0)
        self.override_default_num_replica_with_nodes = self.input.param(
            "override_with_nodes", None)
        if self.override_default_num_replica_with_nodes:
            self.nodes = self.override_default_num_replica_with_nodes
        self.node_out = self.input.param("node_out", 0)
        self.server_grouping = self.input.param("server_grouping", None)
        self.eq_index_node = self.input.param("eq_index_node", None)
        self.recovery_type = self.input.param("recovery_type", None)

    def tearDown(self):
        super(GSIReplicaIndexesTests, self).tearDown()

    def test_create_replica_index_with_num_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_replicas)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_replicas)

    def test_create_replica_index_one_failed_node_num_replica(self):
        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_replicas)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_replicas)

    def test_failover_during_create_index_with_replica(self):
        node_out = self.servers[self.node_out]
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': true}};".format(
            self.num_replicas)

        threads = [
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query",
                   args=(create_index_query, 10, self.n1ql_node)),
            Thread(target=self.cluster.async_failover, name="failover", args=(
                self.servers[:self.nodes_init], [node_out], self.graceful))]
        for thread in threads:
            thread.start()
            thread.join()
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)

        try:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_replicas)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")

    def test_create_replica_index_with_node_list(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

    def test_create_replica_index_one_failed_node_with_node_list(self):
        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful)

        failover_task.result()

        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

    def test_create_replica_index_with_num_replicas_and_node_list(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0},'nodes': {1}}};".format(
            self.num_replicas, nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_replicas, nodes)

    def test_create_replica_index_with_server_groups(self):
        nodes = self._get_node_list()
        self.log.info(nodes)

        self._create_server_groups()
        self.sleep(5)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_replicas)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map = self.get_index_map()

        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_replicas)

    def test_default_num_indexes(self):
        self.rest.set_indexer_num_replica(self.num_replicas)
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age)"

        if self.override_default_num_replica_with_nodes and self.override_default_num_replica_with_num:
            create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0},'nodes': {1}}};".format(
                self.override_default_num_replica_with_num, nodes)

        elif self.override_default_num_replica_with_nodes:
            create_index_query += "USING GSI  WITH {{'nodes': {0}}};".format(
                nodes)

        elif self.override_default_num_replica_with_num:
            create_index_query += "USING GSI  WITH {{'num_replica': {0}}};".format(
                self.override_default_num_replica_with_num)

        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        expected_num_replicas = self.num_replicas
        if self.override_default_num_replica_with_num > 0:
            expected_num_replicas = self.override_default_num_replica_with_num
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    expected_num_replicas,
                                                    nodes)

        # Reset the default value for num_replica
        self.rest.set_indexer_num_replica(0)

    def test_build_index_with_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': true}};".format(
            self.num_replicas)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_replicas)

        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             self.num_replicas,
                                                             defer_build=True)

        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"

        try:
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             self.num_replicas,
                                                             defer_build=False)

    def test_build_index_with_replica_one_failed_node(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}, 'defer_build': true}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             len(nodes) - 1,
                                                             defer_build=True)

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful)

        failover_task.result()

        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"

        try:
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        try:
            self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                                 len(nodes) - 1,
                                                                 defer_build=False)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

    def test_failover_during_build_index(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}, 'defer_build': true}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             len(nodes) - 1,
                                                             defer_build=True)

        node_out = self.servers[self.node_out]
        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"
        threads = []
        threads.append(
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query",
                   args=(build_index_query, 10, self.n1ql_node)))
        threads.append(
            Thread(target=self.cluster.async_failover, name="failover", args=(
                self.servers[:self.nodes_init], [node_out], self.graceful)))
        for thread in threads:
            thread.start()
            thread.join()
        self.sleep(30)

        index_map = self.get_index_map()
        try:
            self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                                 len(nodes) - 1,
                                                                 defer_build=False)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

    def test_build_index_with_replica_failover_addback(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}, 'defer_build': true}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    len(nodes) - 1, nodes)

        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             len(nodes) - 1,
                                                             defer_build=True)

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful)

        failover_task.result()

        build_index_query = "BUILD INDEX on `default`(" + index_name_prefix + ")"

        try:
            self.n1ql_helper.run_cbq_query(query=build_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index building did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index building failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             len(nodes) - 2,
                                                             defer_build=False)

        nodes_all = self.rest.node_statuses()
        for node in nodes_all:
            if node.ip == node_out.ip:
                break

        self.rest.set_recovery_type(node.id, self.recovery_type)
        self.rest.add_back_node(node.id)

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map = self.get_index_map()
        self.n1ql_helper.verify_replica_indexes_build_status(index_map,
                                                             len(nodes) - 1,
                                                             defer_build=False)

    def test_drop_index_with_replica(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': {1}}};".format(
            self.num_replicas, self.defer_build)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_replicas)

        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Drop index did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Drop index failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_drop_index_with_replica_one_failed_node(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': {1}}};".format(
            self.num_replicas, self.defer_build)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_replicas)

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Drop index did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Drop index failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_failover_during_drop_index(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}}};".format(
            self.num_replicas)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_replicas)

        node_out = self.servers[self.node_out]
        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        threads = []
        threads.append(
            Thread(target=self.n1ql_helper.run_cbq_query, name="run_query",
                   args=(drop_index_query, 10, self.n1ql_node)))
        threads.append(
            Thread(target=self.cluster.async_failover, name="failover", args=(
                self.servers[:self.nodes_init], [node_out], self.graceful)))

        for thread in threads:
            thread.start()
        self.sleep(5)
        for thread in threads:
            thread.join()

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_drop_index_with_replica_failover_addback(self):
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'num_replica': {0}, 'defer_build': {1}}};".format(
            self.num_replicas, self.defer_build)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "index creation did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Index creation failed as expected")
        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info(index_map)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map,
                                                    self.num_replicas)

        node_out = self.servers[self.node_out]
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful)

        failover_task.result()

        drop_index_query = "DROP INDEX `default`." + index_name_prefix

        try:
            self.n1ql_helper.run_cbq_query(query=drop_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Drop index did not fail with expected error : {0}".format(
                        str(ex)))
            else:
                self.log.info("Drop index failed as expected")

        self.sleep(30)
        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

        nodes_all = self.rest.node_statuses()
        for node in nodes_all:
            if node.ip == node_out.ip:
                break

        self.rest.set_recovery_type(node.id, self.recovery_type)
        self.rest.add_back_node(node.id)

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map = self.get_index_map()
        self.log.info("Index map after drop index: %s", index_map)
        if not index_map == {}:
            self.fail("Indexes not dropped correctly")

    def test_replica_movement_with_rebalance_out(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance = self.get_index_map()
        stats_map_after_rebalance = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [node_out])
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_dropped_replica_add_new_node(self):
        nodes = self._get_node_list()
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance1 = self.get_index_map()
        stats_map_after_rebalance1 = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance1,
                stats_map_before_rebalance,
                stats_map_after_rebalance1,
                [],
                [node_out])
        except Exception, ex:
            self.log.info(str(ex))
            if "some indexes are missing after rebalance" not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

        node_in = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [node_in], [],
                                                 services=["index"])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance2 = self.get_index_map()
        stats_map_after_rebalance2 = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance2,
                stats_map_before_rebalance,
                stats_map_after_rebalance2,
                [node_in],
                [])
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_replica_movement_with_rebalance_out_and_server_groups(self):
        nodes = self._get_node_list()
        self.log.info(nodes)

        self._create_server_groups()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance = self.get_index_map()
        stats_map_after_rebalance = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [node_out])
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_dropped_replica_add_new_node_with_server_group(self):
        # Remove the last node from the cluster
        node_out = self.servers[self.nodes_init - 1]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        nodes = self._get_node_list()
        self.log.info(nodes)

        # Default source zone
        zones = self.rest.get_zone_names().keys()
        source_zone = zones[0]
        self._create_server_groups()

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance1 = self.get_index_map()
        stats_map_after_rebalance1 = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance1,
                stats_map_before_rebalance,
                stats_map_after_rebalance1,
                [],
                [node_out])
        except Exception, ex:
            self.log.info(str(ex))
            if "some indexes are missing after rebalance" not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

        self.rest.add_zone("server_group_3")

        # Add back the node that was recently removed.
        node_in = [self.servers[self.node_out]]
        rebalance1 = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            node_in, [],
            services=["index"])

        # Add back the node that was previously removed.
        node_in = [self.servers[self.nodes_init - 1]]
        rebalance2 = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            node_in, [],
            services=["index"])

        # Add nodes to server groups
        self.rest.shuffle_nodes_in_zones([self.servers[self.node_out].ip],
                                         source_zone, "server_group_1")
        self.rest.shuffle_nodes_in_zones([self.servers[3].ip],
                                         source_zone, "server_group_3")

        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance1.result()
        rebalance2.result()
        self.sleep(30)

        index_map_after_rebalance2 = self.get_index_map()
        stats_map_after_rebalance2 = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance2,
                stats_map_before_rebalance,
                stats_map_after_rebalance2,
                [node_in],
                [])
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_replica_movement_with_rebalance_out_and_equivalent_index(self):
        nodes = self._get_node_list()
        self.log.info(nodes)

        eq_index_node = self.servers[int(self.eq_index_node)].ip + ":" + \
                        self.servers[int(self.eq_index_node)].port

        # Create Equivalent Index
        equivalent_index_query = "CREATE INDEX eq_index ON default(age) USING GSI  WITH {{'nodes': '{0}'}};".format(
            eq_index_node)
        self.log.info(equivalent_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=equivalent_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_rebalance = self.get_index_map()
        stats_map_before_rebalance = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_rebalance)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_rebalance,
                                                    len(nodes) - 1, nodes)

        node_out = self.servers[self.node_out]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_rebalance = self.get_index_map()
        stats_map_after_rebalance = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_rebalance,
                index_map_after_rebalance,
                stats_map_before_rebalance,
                stats_map_after_rebalance,
                [],
                [node_out])
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post rebalance : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_replica_movement_with_failover(self):
        nodes = self._get_node_list()
        node_out = self.servers[self.node_out]
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_failover = self.get_index_map()
        stats_map_before_failover = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_failover)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_failover,
                                                    len(nodes) - 1, nodes)

        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [node_out])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_failover = self.get_index_map()
        stats_map_after_failover = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_failover,
                index_map_after_failover,
                stats_map_before_failover,
                stats_map_after_failover,
                [],
                [node_out])
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post failover : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def test_replica_movement_with_failover_and_addback(self):
        nodes = self._get_node_list()
        node_out = self.servers[self.node_out]
        self.log.info(nodes)
        index_name_prefix = "random_index_" + str(
            random.randint(100000, 999999))
        create_index_query = "CREATE INDEX " + index_name_prefix + " ON default(age) USING GSI  WITH {{'nodes': {0}}};".format(
            nodes)
        self.log.info(create_index_query)
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
        except Exception, ex:
            self.log.info(str(ex))
            self.fail("Index creation Failed : %s", str(ex))

        self.sleep(30)
        index_map_before_failover = self.get_index_map()
        stats_map_before_failover = self.get_index_stats(perNode=False)

        self.log.info(index_map_before_failover)
        if not self.expected_err_msg:
            self.n1ql_helper.verify_replica_indexes([index_name_prefix],
                                                    index_map_before_failover,
                                                    len(nodes) - 1, nodes)

        nodes_all = self.rest.node_statuses()
        for node in nodes_all:
            if node.ip == node_out.ip:
                break

        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init],
            [node_out],
            self.graceful, wait_for_pending=180)

        failover_task.result()

        self.rest.set_recovery_type(node.id, self.recovery_type)
        self.rest.add_back_node(node.id)

        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                 [], [])
        reached = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rebalance.result()
        self.sleep(30)

        index_map_after_failover = self.get_index_map()
        stats_map_after_failover = self.get_index_stats(perNode=False)

        try:
            self.n1ql_helper.verify_indexes_redistributed(
                index_map_before_failover,
                index_map_after_failover,
                stats_map_before_failover,
                stats_map_after_failover,
                [],
                [])
        except Exception, ex:
            self.log.info(str(ex))
            if self.expected_err_msg not in str(ex):
                self.fail(
                    "Error in index distribution post failover : ".format(
                        str(ex)))
            else:
                self.log.info(str(ex))

    def _get_node_list(self):
        # 1. Parse node string
        nodes = []
        invalid_ip = "10.111.151.256"
        if self.nodes:
            nodes = self.nodes.split(":")
            for i in range(0, len(nodes)):
                if nodes[i] not in ("empty", "invalid"):
                    nodes[i] = self.servers[int(nodes[i])].ip + ":" + \
                               self.servers[int(nodes[i])].port
                elif nodes[i] == "invalid":
                    nodes[i] = invalid_ip + ":" + "8091"
                elif nodes[i] == "empty":
                    nodes[i] = ""
        else:
            self.log.info("No nodes in list")

        return nodes

    def _create_server_groups(self):
        zones = self.rest.get_zone_names().keys()
        source_zone = zones[0]

        if self.server_grouping:
            server_groups = self.server_grouping.split(":")
            self.log.info("server groups : %s", server_groups)

            # Create Server groups
            for i in range(1, len(server_groups) + 1):
                server_grp_name = "server_group_" + str(i)
                self.rest.add_zone(server_grp_name)

            # Add nodes to Server groups
            i = 1
            for server_grp in server_groups:
                server_grp_name = "server_group_" + str(i)
                i += 1
                nodes_in_server_group = server_grp.split("-")
                for node in nodes_in_server_group:
                    self.rest.shuffle_nodes_in_zones(
                        [self.servers[int(node)].ip], source_zone,
                        server_grp_name)
