import time
from threading import Thread
from newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper

class SingleNodeUpgradeTests(NewUpgradeBaseTest):
    def setUp(self):
        super(SingleNodeUpgradeTests, self).setUp()

    def tearDown(self):
        super(SingleNodeUpgradeTests, self).tearDown()

    def test_upgrade(self):
        self._install([self.master])
        self.operations()
        self.log.info("Installation of old version is done. Wait for %s sec for upgrade" % (self.sleep_time))
        time.sleep(self.sleep_time)
        for upgrade_version in self.upgrade_versions:
            remote = RemoteMachineShellConnection(self.master)
            self._upgrade(upgrade_version, self.master)
            time.sleep(self.expire_time)
            for bucket in self.buckets:
                remote.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
            time.sleep(30)
            remote.disconnect()
            self.verification([self.master])

class MultiNodesUpgradeTests(NewUpgradeBaseTest):
    def setUp(self):
        super(MultiNodesUpgradeTests, self).setUp()
        self.initial_num_servers = self.input.param('initial_num_servers', 2)

    def tearDown(self):
        super(MultiNodesUpgradeTests, self).tearDown()

    def _verify_upgrade_rebalance_in_out(self):
        self.master = self.servers[self.initial_num_servers]
        self.rest = RestConnection(self.master)
        self.rest_helper = RestHelper(self.rest)
        for bucket in self.buckets:
            if self.rest_helper.bucket_exists(bucket.name):
                continue
            else:
                raise Exception("bucket:- %s not found" % bucket.name)
        if self.op_types == "bucket":
            bucketinfo = self.rest.get_bucket(bucket.name)
            self.log.info("bucket info :- %s" % bucketinfo)
        if self.op_types == "data":
            self._wait_for_stats_all_buckets(self.servers[self.initial_num_servers : self.num_servers])
            self._verify_all_buckets(self.master, 1, self.wait_timeout * 50, self.max_verify, True, 1)
            self._verify_stats_all_buckets(self.servers[self.initial_num_servers : self.num_servers])
        if self.ddocs:
            query = {}
            query["connectionTimeout"] = 60000;
            expected_rows = self.num_items
            if self.max_verify:
                expected_rows = self.max_verify
                query["limit"] = expected_rows
            if self.input.param("wait_expiration", False):
                expected_rows = 0
            query["stale"] = "false"
            for bucket in self.buckets:
                for ddoc in self.ddocs:
                    prefix = ("", "dev_")[ddoc.views[0].dev_view]
                    self.perform_verify_queries(len(ddoc.views), prefix, ddoc.name, query, bucket=bucket,
                                                wait_time=self.wait_timeout * 5, expected_rows=expected_rows,
                                                retry_time=10)
        if self.input.param("update_notifications", True):
            if self.rest.get_notifications() != self.input.param("update_notifications", True):
                self.log.error("update notifications settings wasn't saved")
        if self.input.param("autofailover_timeout", None):
            if self.rest.get_autofailover_settings() != self.input.param("autofailover_timeout", None):
                self.log.error("autofailover settings wasn't saved")

    def offline_cluster_upgrade(self):
        self._install(self.servers[:self.initial_num_servers])
        self.log.info("Installation done going to sleep for %s sec", self.sleep_time)
        self.operations(multi_nodes=True)
        if self.ddocs_num:
            self.create_ddocs_and_views()
        time.sleep(self.sleep_time)
        if self.during_ops:
            for opn in self.during_ops:
                getattr(self, opn)()
        for upgrade_version in self.upgrade_versions:
            for server in self.servers[:self.initial_num_servers]:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                time.sleep(self.sleep_time)
                remote.disconnect()
            for server in self.servers[:self.initial_num_servers]:
                self._upgrade(upgrade_version, server)
                time.sleep(self.sleep_time)
            time.sleep(self.expire_time)
            self.verification(self.servers[:self.initial_num_servers])

    def offline_cluster_upgrade_non_default_path(self):
        num_nodes_with_not_default = self.input.param('num_nodes_with_not_default', 1)
        data_path = self.input.param('data_path', '/tmp/data').replace('|', "/")
        index_path = self.input.param('index_path', data_path).replace('|', "/")
        servers_with_not_default = self.servers[:num_nodes_with_not_default]
        for server in servers_with_not_default:
            server.data_path = data_path
            server.index_path = index_path
        self._install(self.servers)
        self.log.info("Installation done going to sleep for %s sec", self.sleep_time)
        self.operations(multi_nodes=True)
        time.sleep(self.sleep_time)
        for upgrade_version in self.upgrade_versions:
            for server in self.servers[:self.initial_num_servers]:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                time.sleep(self.sleep_time)
                remote.disconnect()
            upgrade_threads = []
            for server in self.servers[:self.initial_num_servers]:
                upgrade_thread = Thread(target=self._upgrade,
                                       name="upgrade_thread" + server.ip,
                                       args=(upgrade_version, server))
                upgrade_threads.append(upgrade_thread)
                upgrade_thread.start()
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            time.sleep(self.expire_time)
            self.verification(self.servers[:self.initial_num_servers])
            for server in servers_with_not_default:
                rest = RestConnection(server)
                node = rest.get_nodes_self()
                self.assertTrue(node.storage[0].path, data_path)
                self.assertTrue(node.storage[0].index_path, index_path)

    def online_upgrade_rebalance_in_out(self):
        self._install(self.servers[:self.initial_num_servers])
        self.operations(multi_nodes=True)
        self.log.info("Installation of old version is done. Wait for %s sec for upgrade" % (self.sleep_time))
        if self.ddocs_num:
            self.create_ddocs_and_views()
        time.sleep(self.sleep_time)
        self.initial_version = self.upgrade_versions[0]
        self.product = 'couchbase-server'
        self._install(self.servers[self.initial_num_servers:self.num_servers])
        self.log.info("Installation of new version is done. Wait for %s sec for rebalance" % (self.sleep_time))
        time.sleep(self.sleep_time)
        if self.during_ops:
            for opn in self.during_ops:
                getattr(self, opn)()
        self.online_upgrade()
        time.sleep(self.sleep_time)
        self._verify_upgrade_rebalance_in_out()

    def online_upgrade_incremental(self):
        self._install(self.servers)
        self.operations(multi_nodes=True)
        self.log.info("Installation of old version is done. Wait for %s sec for upgrade" % (self.sleep_time))
        if self.ddocs_num:
            self.create_ddocs_and_views()
        time.sleep(self.sleep_time)
        for server in self.servers[1:]:
            self.cluster.rebalance(self.servers, [], [server])
            self.initial_version = self.upgrade_versions[0]
            self.product = 'couchbase-server'
            self._install([server])
            self.log.info("Installation of new version is done. Wait for %s sec for rebalance" % (self.sleep_time))
            time.sleep(self.sleep_time)
            self.cluster.rebalance(self.servers, [server], [])
            self.log.info("Rebalanced in upgraded nodes")
            time.sleep(self.sleep_time)
            self._verify_upgrade_rebalance_in_out()
        self.master = self.servers[1]
        self.cluster.rebalance(self.servers[:self.num_servers], [], [self.servers[0]])
        self.log.info("Rebalanced out all old version nodes")
        time.sleep(self.sleep_time)
        self._verify_upgrade_rebalance_in_out()

    def online_upgrade(self):
        servers_in = self.servers[self.initial_num_servers:self.num_servers]
        self.cluster.rebalance(self.servers[:self.initial_num_servers], servers_in, [])
        self.log.info("Rebalance in all 2.0 Nodes")
        time.sleep(self.sleep_time)
        status, content = ClusterOperationHelper.find_orchestrator(self.master)
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
                        format(status, content))
        FIND_MASTER = False
        for new_server in servers_in:
            if content.find(new_server.ip) >= 0:
                FIND_MASTER = True
                self.log.info("2.0 Node %s becomes the master" % (new_server.ip))
        if not FIND_MASTER:
            raise Exception("After rebalance in 2.0 Nodes, 2.0 doesn't become the master")

        servers_out = self.servers[:self.initial_num_servers]
        self.cluster.rebalance(self.servers[:self.num_servers], [], servers_out)
        self.log.info("Rebalanced out all old version nodes")
        time.sleep(self.sleep_time)

    def online_upgrade_swap_rebalance(self):
        self._install(self.servers[:self.initial_num_servers])
        self.operations(multi_nodes=True)
        self.log.info("Installation of old version is done. Wait for %s sec for upgrade" % (self.sleep_time))
        time.sleep(self.sleep_time)
        self.initial_version = self.upgrade_versions[0]
        self.product = 'couchbase-server'
        self._install(self.servers[self.initial_num_servers:self.num_servers])
        self.log.info("Installation of new version is done. Wait for %s sec for rebalance" % (self.sleep_time))
        time.sleep(self.sleep_time)

        self.swap_num_servers = self.input.param('swap_num_servers', 1)
        old_servers = self.servers[:self.initial_num_servers]
        new_servers = []
        for i in range(self.initial_num_servers / self.swap_num_servers):
            servers_in = self.servers[(self.initial_num_servers + i * self.swap_num_servers):
                                      (self.initial_num_servers + (i + 1) * self.swap_num_servers)]
            servers_out = self.servers[(i * self.swap_num_servers):((i + 1) * self.swap_num_servers)]
            servers = old_servers + new_servers
            self.cluster.rebalance(servers, servers_in, servers_out)
            self.log.info("Swap rebalance: rebalance out %s old version nodes, rebalance in %s 2.0 Nodes"
                          % (self.swap_num_servers, self.swap_num_servers))
            time.sleep(self.sleep_time)
            old_servers = self.servers[((i + 1) * self.swap_num_servers):self.initial_num_servers]
            new_servers = new_servers + servers_in
            servers = old_servers + new_servers
            status, content = ClusterOperationHelper.find_orchestrator(servers[0])
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
                            format(status, content))
            FIND_MASTER = False
            for new_server in new_servers:
                if content.find(new_server.ip) >= 0:
                    FIND_MASTER = True
                    self.log.info("2.0 Node %s becomes the master" % (new_server.ip))
            if not FIND_MASTER:
                raise Exception("After rebalance in 2.0 nodes, 2.0 doesn't become the master ")

        self._verify_upgrade_rebalance_in_out()
