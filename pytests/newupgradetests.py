import time
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
            self._upgrade(upgrade_version, self.master, remote)
            time.sleep(self.expire_time)
            for bucket in self.buckets:
                remote.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
            time.sleep(30)
            remote.disconnect()
            self.verification()

class MultiNodesUpgradeTests(NewUpgradeBaseTest):
    def setUp(self):
        super(MultiNodesUpgradeTests, self).setUp()
        self.initial_num_servers = self.input.param('initial_num_servers', 2)

    def tearDown(self):
        super(MultiNodesUpgradeTests, self).tearDown()

    def offline_cluster_upgrade(self):
        self._install(self.servers[:self.initial_num_servers])
        self.operations(multi_nodes=True)
        self.log.info("Installation done going to sleep for %s sec", self.sleep_time)
        time.sleep(self.sleep_time)
        for upgrade_version in self.upgrade_versions:
            for server in self.servers[:self.initial_num_servers]:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                time.sleep(self.sleep_time)
                remote.disconnect()
            for server in self.servers[:self.initial_num_servers]:
                remote = RemoteMachineShellConnection(server)
                self._upgrade(upgrade_version, server, remote)
                time.sleep(self.sleep_time)
                remote.disconnect()
            time.sleep(self.expire_time)
            self.num_servers = self.initial_num_servers
            self.verification(multi_nodes=True)

    def online_upgrade_rebalance_in_out(self):
        self._install(self.servers[:self.initial_num_servers])
        self.operations(multi_nodes=True)
        self.log.info("Installation of old version is done. Wait for %s sec for upgrade" % (self.sleep_time))
        time.sleep(self.sleep_time)
        self.initial_version = self.upgrade_versions[0]
        self.product = 'couchbase-server'
        self._install(self.servers[self.initial_num_servers:self.num_servers])
        self.log.info("Installation of new version is done. Wait for %s sec for rebalance" % (self.sleep_time))
        time.sleep(self.sleep_time)

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
        self.log.info("Rebalance out all old version nodes")
        time.sleep(self.sleep_time)
        self.verify_upgrade_rebalance_in_out()

    def verify_upgrade_rebalance_in_out(self):
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

        self.verify_upgrade_rebalance_in_out()
