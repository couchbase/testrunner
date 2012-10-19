import time
from newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper as ClusterHelper, ClusterOperationHelper

class SingleNodeUpgradeTests(NewUpgradeBaseTest):
    def setUp(self):
        super(SingleNodeUpgradeTests, self).setUp()
        if self.initial_version.startswith("1.6") or self.initial_version.startswith("1.7"):
            self.product = 'membase-server'
        else:
            self.product = 'couchbase-server'

    def tearDown(self):
        super(SingleNodeUpgradeTests, self).tearDown()

    def test_upgrade(self):
        self._install([self.master])
        self.operations()
        upgrade_versions = self.input.param('upgrade_version', '2.0.0-1869-rel')
        upgrade_versions = upgrade_versions.split(";")
        self.log.info("Installation of old version is done. Wait for %s sec for upgrade" % (self.sleep_time))
        time.sleep(self.sleep_time)
        for upgrade_version in upgrade_versions:
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
        if self.initial_version.startswith("1.6") or self.initial_version.startswith("1.7"):
            self.product = 'membase-server'
        else:
            self.product = 'couchbase-server'
        self.initial_num_servers = self.input.param('initial_num_servers', 2)

    def tearDown(self):
        super(MultiNodesUpgradeTests, self).tearDown()

    def offline_cluster_upgrade(self):
        self._install(self.servers[:self.initial_num_servers])
        self.operations(multi_nodes=True)
        upgrade_versions = self.input.param('upgrade_version', '2.0.0-1869-rel')
        upgrade_versions = upgrade_versions.split(";")
        self.log.info("Installation done going to sleep for %s sec", self.sleep_time)
        time.sleep(self.sleep_time)
        for upgrade_version in upgrade_versions:
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
        upgrade_version = self.input.param('upgrade_version', '2.0.0-1869-rel')
        self.initial_version = upgrade_version
        self.product = 'couchbase-server'
        self._install(self.servers[self.initial_num_servers:self.num_servers])
        self.log.info("Installation of new version is done. Wait for %s sec for rebalance" % (self.sleep_time))
        time.sleep(self.sleep_time)

        servers_in = self.servers[self.initial_num_servers:self.num_servers]
        self.cluster.rebalance(self.servers[:self.initial_num_servers], servers_in, [])
        self.log.info("Rebalance in all 2.0 Nodes")
        time.sleep(self.sleep_time)
        status, content = ClusterHelper.find_orchestrator(self.master)
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
            self._verify_all_buckets(self.master, 1, self.wait_timeout*50, None, True, 1)
            self._verify_stats_all_buckets(self.servers[self.initial_num_servers : self.num_servers])

    def online_upgrade_swap_rebalance(self):
        self._install(self.servers[:self.initial_num_servers])
        self.operations(multi_nodes=True)
        self.log.info("Installation of old version is done. Wait for %s sec for upgrade" % (self.sleep_time))
        time.sleep(self.sleep_time)
        upgrade_version = self.input.param('upgrade_version', '2.0.0-1869-rel')
        self.initial_version = upgrade_version
        self.product = 'couchbase-server'
        self._install(self.servers[self.initial_num_servers:self.num_servers])
        self.log.info("Installation of new version is done. Wait for %s sec for rebalance" % (self.sleep_time))
        time.sleep(self.sleep_time)

        self.swap_num_servers = self.input.param('swap_num_servers', 1)
        old_servers = self.servers[:self.initial_num_servers]
        new_servers = []
        for i in range(self.initial_num_servers/self.swap_num_servers):
            servers_in = self.servers[(self.initial_num_servers+i*self.swap_num_servers):
                                      (self.initial_num_servers+(i+1)*self.swap_num_servers)]
            servers_out = self.servers[(i*self.swap_num_servers):((i+1)*self.swap_num_servers)]
            servers = old_servers + new_servers
            self.cluster.rebalance(servers, servers_in, servers_out)
            self.log.info("Swap rebalance: rebalance out %s old version nodes, rebalance in %s 2.0 Nodes"
                          % (self.swap_num_servers, self.swap_num_servers))
            time.sleep(self.sleep_time)
            old_servers = self.servers[((i+1)*self.swap_num_servers):self.initial_num_servers]
            new_servers = new_servers + servers_in
            servers = old_servers + new_servers
            status, content = ClusterHelper.find_orchestrator(servers[0])
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