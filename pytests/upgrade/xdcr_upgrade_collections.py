import queue
import time
from .newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection, RestHelper
from security.rbac_base import RbacBase
from threading import Thread
from collection.collections_cli_client import CollectionsCLI
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats


class XDCRUpgradeCollectionsTests(NewUpgradeBaseTest):
    def setUp(self):
        super(XDCRUpgradeCollectionsTests, self).setUp()
        self.nodes_init = self.input.param('nodes_init', 2)
        self.queue = queue.Queue()
        self.rate_limit = self.input.param("rate_limit", 100000)
        self.batch_size = self.input.param("batch_size", 1000)
        self.doc_size = self.input.param("doc_size", 100)
        self.loader = self.input.param("loader", "high_doc_ops")
        self.instances = self.input.param("instances", 4)
        self.threads = self.input.param("threads", 5)
        self.use_replica_to = self.input.param("use_replica_to", False)
        self.index_name_prefix = None
        self.rest_src = RestConnection(self.servers[0])

    def tearDown(self):
        super(XDCRUpgradeCollectionsTests, self).tearDown()

    def enable_migration_mode(self, src_bucket, dest_bucket):
        setting_val_map = {"collectionsMigrationMode": "true",
                           "colMappingRules": '{"REGEXP_CONTAINS(META().id,\'0$\')":"scope1.mycollection_scope1"}'
                           }
        self.rest_src.set_xdcr_params(src_bucket, dest_bucket, setting_val_map)

    def verify_doc_counts(self):
        des_master = self.servers[self.nodes_init]
        src_cbver = RestConnection(self.master).get_nodes_version()
        des_cbver = RestConnection(des_master).get_nodes_version()
        src_items = RestConnection(self.master).get_buckets_itemCount()
        des_items = RestConnection(des_master).get_buckets_itemCount()
        if src_cbver[:3] < "7.0" and des_cbver[:3] >= "7.0":
            des_items = self.get_col_item_count(des_master, "default", "_default",
                                                "_default", self.des_stat_col)
            if src_items["default"] != des_items:
                self.fail("items do not match. src: {0} != des: {1}"
                          .format(src_items["default"], des_items))
        elif src_cbver[:3] >= "7.0" and des_cbver[:3] < "7.0":
            src_items = self.get_col_item_count(self.master, "default", "_default",
                                                "_default", self.stat_col)
            if src_items != des_items["default"]:
                self.fail("items do not match. src: {0} != des: {1}"
                          .format(src_items, des_items["default"]))
        elif src_cbver[:3] >= "7.0" and des_cbver[:3] >= "7.0":
            if src_items["default"] != des_items["default"]:
                self.fail("items do not match. src: {0} != des: {1}"
                          .format(src_items["default"], des_items["default"]))

    def test_xdcr_upgrade_with_services(self):
        after_upgrade_services_in = self.input.param("after_upgrade_services_in", False)
        after_upgrade_buckets_in = self.input.param("after_upgrade_buckets_in", False)
        after_upgrade_buckets_out = self.input.param("after_upgrade_buckets_out", False)
        after_upgrade_buckets_flush = self.input.param("after_upgrade_buckets_flush", False)

        # Install initial version on the specified nodes
        self._install(self.servers[:self.nodes_init])
        # Configure the nodes with services on cluster1
        self.operations(self.servers[:self.nodes_init], services="kv,kv")
        # get the n1ql node which will be used in pre,during and post upgrade for running n1ql commands
        self.n1ql_server = self.get_nodes_from_services_map(service_type="n1ql")
        # Run the pre upgrade operations, typically creating index
        self.pre_upgrade(self.servers[:self.nodes_init])
        if self.input.param("ddocs_num", 0) > 0:
            self.create_ddocs_and_views()

        self._install(self.servers[self.nodes_init:self.num_servers])
        self.master = self.servers[self.nodes_init]
        # Configure the nodes with services on the other cluster2
        try:
            self.operations(self.servers[self.nodes_init:self.num_servers], services="kv,kv")
            self.sleep(timeout=10)
        except Exception as ex:
            if ex:
                print("error: ", str(ex))
            self.log.info("bucket is created")

        # create a xdcr relationship between cluster1 and cluster2
        self.rest_src.add_remote_cluster(self.servers[self.nodes_init].ip,
                                    self.servers[self.nodes_init].port,
                                    'Administrator', 'password', "C2")

        repl_id = self.rest_src.start_replication('continuous', 'default', "C2")
        if repl_id is not None:
            self.log.info("Replication created successfully")
        # Run the post_upgrade operations
        self._create_ephemeral_buckets()
        self.post_upgrade(self.servers[:self.nodes_init])
        # Add new services after the upgrade
        for upgrade_version in self.upgrade_versions:
            src_nodes = self.servers[:self.nodes_init]
            for server in src_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
            src_upgrade_threads = self._async_update(upgrade_version, src_nodes)

            for upgrade_thread in src_upgrade_threads:
                upgrade_thread.join()
            src_success_upgrade = True
            while not self.queue.empty():
                src_success_upgrade &= self.queue.get()
            if not src_success_upgrade:
                self.fail("Upgrade failed in source cluster. See logs above!")
            else:
                self.log.info("Upgrade source cluster success")

            des_nodes = self.servers[self.nodes_init:self.num_servers]
            self.master = self.servers[self.nodes_init]
            for server in des_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
            des_upgrade_threads = self._async_update(upgrade_version, des_nodes)
            for upgrade_thread in des_upgrade_threads:
                upgrade_thread.join()
            des_success_upgrade = True
            while not self.queue.empty():
                des_success_upgrade &= self.queue.get()
            if not des_success_upgrade:
                self.fail("Upgrade failed in des cluster. See logs above!")
            else:
                self.log.info("Upgrade des cluster success")

        self.master = self.servers[0]
        self.rest = RestConnection(self.master)
        self.rest_col = CollectionsRest(self.master)
        self.cli_col = CollectionsCLI(self.master)
        self.stat_col = CollectionsStats(self.master)
        self.log.info("Create scope collection at src cluster")
        #self.rest_col.create_scope_collection_count()
        self.sleep(10)

        self.des_rest = RestConnection(self.servers[self.nodes_init])
        self.des_rest_col = CollectionsRest(self.servers[self.nodes_init])
        self.des_cli_col = CollectionsCLI(self.servers[self.nodes_init])
        self.des_stat_col = CollectionsStats(self.servers[self.nodes_init])
        self.log.info("Create scope collection at des cluster")
        self.buckets = RestConnection(self.servers[self.nodes_init]).get_buckets()
        self._create_scope_collection(self.rest_col, self.cli_col, self.buckets[0].name)
        self._create_scope_collection(self.des_rest_col, self.des_cli_col, self.buckets[0].name)
        self.load_to_collections_bucket()

        self.enable_migration = self.input.param("enable_migration", False)
        if self.enable_migration:
            self.enable_migration_mode(self.buckets[0].name, self.buckets[0].name)
        self.verify_doc_counts()

        if after_upgrade_buckets_in is not False:
            self.bucket_size = 256
            self._create_sasl_buckets(self.master, 1)
            self._create_standard_buckets(self.master, 1)
            if self.input.param("ddocs_num", 0) > 0:
                self.create_ddocs_and_views()
                gen_load = BlobGenerator('upgrade', 'upgrade-', self.value_size,
                                         end=self.num_items)
                self._load_all_buckets(self.master, gen_load, "create", self.expire_time,
                                       flag=self.item_flag)

        # deleting buckets after upgrade
        if after_upgrade_buckets_out is not False:
            self._all_buckets_delete(self.master)
        # flushing buckets after upgrade
        if after_upgrade_buckets_flush is not False:
            self._all_buckets_flush()

    def test_xdcr_online_upgrade_with_services(self):
        """
        Online counterpart of test_xdcr_upgrade_with_services. C1 and then C2 are
        upgraded one node at a time (graceful failover, in-place package upgrade,
        delta recovery, rebalance) while the C1->C2 replication keeps running. After
        every node a fresh batch is loaded on C1 and must reach C2, so replication is
        checked through both mixed-version windows: C1 mixed with C2 old, then C1 new
        with C2 mixed.
        """
        # An unfetchable build makes the installer kill the whole testrunner, and the
        # class defaults are unfetchable, so never fall back on them.
        for param in ("initial_version", "upgrade_version"):
            if param not in self.input.test_params:
                self.fail("test_xdcr_online_upgrade_with_services needs {0}".format(param))
        if self.initial_version == self.upgrade_versions[-1]:
            self.fail("initial_version and upgrade_version are both {0}; nothing would be "
                      "upgraded".format(self.initial_version))
        step_items = self.input.param("step_items", 1000)
        catchup_timeout = self.input.param("catchup_timeout", 600)
        src_nodes = self.servers[:self.nodes_init]
        des_nodes = self.servers[self.nodes_init:self.num_servers]
        if len(src_nodes) < 2 or len(des_nodes) < 2:
            self.fail("Online upgrade needs at least 2 nodes per cluster, got C1={0} C2={1}"
                      .format(len(src_nodes), len(des_nodes)))

        # Cluster setup as in test_xdcr_upgrade_with_services, except that every node
        # joins: operations() rebalances in one node per services entry, so the
        # offline test's "kv,kv" leaves all but two nodes of each cluster standalone
        self._install(src_nodes)
        self.operations(src_nodes, services=",".join(["kv"] * len(src_nodes)))
        self.pre_upgrade(src_nodes)

        self._install(des_nodes)
        self.master = des_nodes[0]
        try:
            self.operations(des_nodes, services=",".join(["kv"] * len(des_nodes)))
            self.sleep(timeout=10)
        except Exception as ex:
            self.log.warning("C2 setup raised: {0}".format(ex))
        if "default" not in RestConnection(des_nodes[0]).get_buckets_itemCount():
            self.fail("C2 has no default bucket after setup")
        for cluster_name, nodes in (("C1", src_nodes), ("C2", des_nodes)):
            members = len(RestConnection(nodes[0]).get_nodes())
            if members != len(nodes):
                self.fail("{0} has {1} active nodes after setup, expected {2}"
                          .format(cluster_name, members, len(nodes)))

        self.rest_src.add_remote_cluster(des_nodes[0].ip, des_nodes[0].port,
                                         'Administrator', 'password', "C2")
        repl_id = self.rest_src.start_replication('continuous', 'default', "C2")
        if repl_id is None:
            self.fail("Could not create the default C1->C2 replication")
        self._create_ephemeral_buckets()
        self.post_upgrade(src_nodes)

        # operations() loaded the same num_items keys into default on both clusters
        expected_items = self.num_items
        self._wait_for_default_items(src_nodes[0], des_nodes[0], expected_items,
                                     "before upgrade", catchup_timeout)

        for hop, upgrade_version in enumerate(self.upgrade_versions):
            for cluster_name, nodes in (("C1", src_nodes), ("C2", des_nodes)):
                for index, node in enumerate(nodes, 1):
                    step = "{0} node {1}/{2} ({3}) to {4}".format(
                        cluster_name, index, len(nodes), node.ip, upgrade_version)
                    self._online_upgrade_node(nodes, node, upgrade_version, step)
                    key_prefix = "online_{0}_{1}_{2}_".format(hop, cluster_name.lower(), index)
                    gen_load = BlobGenerator(key_prefix, key_prefix, self.value_size,
                                             end=step_items)
                    self._load_all_buckets(src_nodes[0], gen_load, "create", 0)
                    expected_items += step_items
                    self._wait_for_default_items(src_nodes[0], des_nodes[0], expected_items,
                                                 "after upgrading " + step, catchup_timeout)

        # Same post-upgrade collection checks as test_xdcr_upgrade_with_services
        self.master = self.servers[0]
        self.rest = RestConnection(self.master)
        self.rest_col = CollectionsRest(self.master)
        self.cli_col = CollectionsCLI(self.master)
        self.stat_col = CollectionsStats(self.master)
        self.des_rest_col = CollectionsRest(des_nodes[0])
        self.des_cli_col = CollectionsCLI(des_nodes[0])
        self.des_stat_col = CollectionsStats(des_nodes[0])
        self.buckets = RestConnection(des_nodes[0]).get_buckets()
        self._create_scope_collection(self.rest_col, self.cli_col, self.buckets[0].name)
        self._create_scope_collection(self.des_rest_col, self.des_cli_col, self.buckets[0].name)
        self.load_to_collections_bucket()
        if self.input.param("enable_migration", False):
            self.enable_migration_mode(self.buckets[0].name, self.buckets[0].name)
        self.verify_doc_counts()

    def _online_upgrade_node(self, cluster_nodes, node, upgrade_version, step):
        """Graceful failover, package upgrade and delta recovery of one node."""
        peer = [n for n in cluster_nodes if (n.ip, str(n.port)) != (node.ip, str(node.port))][0]
        rest = RestConnection(peer)
        otp_id = self._cluster_node(rest, node, step).id

        rest.fail_over(otp_id, graceful=True)
        if not rest.monitorRebalance():
            self.fail("Graceful failover of {0} did not complete".format(step))
        membership = self._cluster_node(rest, node, step).clusterMembership
        if membership != "inactiveFailed":
            self.fail("{0} is {1} after graceful failover, expected inactiveFailed"
                      .format(step, membership))

        remote = RemoteMachineShellConnection(node)
        try:
            remote.stop_server()
        finally:
            remote.disconnect()
        # The node is still a cluster member, so it must not be re-initialised
        self._upgrade(upgrade_version, node, queue=self.queue, skip_init=True)

        # setRecoveryType alone marks the node for recovery; reAddNode would ask for a
        # full recovery instead
        rest.set_recovery_type(otp_id, "delta")
        state = self._cluster_node(rest, node, step)
        if state.clusterMembership != "inactiveAdded" or state.recoveryType != "delta":
            self.fail("{0}: node is {1} with recoveryType {2}, expected a pending delta recovery"
                      .format(step, state.clusterMembership, state.recoveryType))
        rest.rebalance(otpNodes=[n.id for n in rest.node_statuses()], ejectedNodes=[])
        if not rest.monitorRebalance():
            self.fail("Delta-recovery rebalance after {0} failed".format(step))

        # couchbase_upgrade does not check what dpkg/rpm did, so a failed package
        # install would leave the old build running unnoticed
        state = self._cluster_node(rest, node, step)
        if state.clusterMembership != "active" or not state.version.startswith(upgrade_version):
            self.fail("{0}: node is {1} running {2} after delta recovery"
                      .format(step, state.clusterMembership, state.version))
        self.log.info("Upgraded {0}".format(step))

    def _cluster_node(self, rest, server, step):
        for node in rest.get_nodes(get_all_nodes=True):
            if node.ip == server.ip and str(node.port) == str(server.port):
                return node
        self.fail("{0}: {1}:{2} is not in the cluster".format(step, server.ip, server.port))

    def _wait_for_default_items(self, src_node, des_node, expected, context, timeout):
        """Wait until default holds exactly `expected` items on both clusters."""
        end_time = time.time() + timeout
        while True:
            src_items = RestConnection(src_node).get_buckets_itemCount().get("default")
            des_items = RestConnection(des_node).get_buckets_itemCount().get("default")
            if src_items == expected and des_items == expected:
                self.log.info("{0}: default holds {1} items on both clusters"
                              .format(context, expected))
                return
            if time.time() > end_time:
                self.fail("{0}: default did not reach {1} items within {2}s: src={3}, des={4}"
                          .format(context, expected, timeout, src_items, des_items))
            self.sleep(10, "{0}: expecting {1}, src={2}, des={3}"
                       .format(context, expected, src_items, des_items))

    def run_view_queries(self):
        view_query_thread = Thread(target=self.view_queries, name="run_queries",
                                   args=(self.run_view_query_iterations,))
        return view_query_thread

    def view_queries(self, iterations):
        query = {"connectionTimeout": 60000}
        for count in range(iterations):
            for i in range(self.view_num):
                self.cluster.query_view(self.master, self.ddocs[0].name,
                                        self.default_view_name + str(i), query,
                                        expected_rows=None, bucket="default", retry_time=2)

    def create_user(self, node):
        self.log.info("inside create_user")
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                     'password': 'password'}]
        rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                     'roles': 'admin'}]
        self.log.info("before create_user_source")
        RbacBase().create_user_source(testuser, 'builtin', node)
        self.log.info("before add_user_role")
        RbacBase().add_user_role(rolelist, RestConnection(node), 'builtin')
