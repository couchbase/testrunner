import time
from threading import Thread
from newupgradebasetest import NewUpgradeBaseTest
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper
from membase.api.exception import RebalanceFailedException
from membase.helper.cluster_helper import ClusterOperationHelper

class SingleNodeUpgradeTests(NewUpgradeBaseTest):
    def setUp(self):
        super(SingleNodeUpgradeTests, self).setUp()

    def tearDown(self):
        super(SingleNodeUpgradeTests, self).tearDown()

    def test_upgrade(self):
        self._install([self.master])
        self.operations([self.master])
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
        self.nodes_init = self.input.param('nodes_init', 2)

    def tearDown(self):
        super(MultiNodesUpgradeTests, self).tearDown()

    def _async_update(self, upgrade_version, servers):
        upgrade_threads = []
        for server in servers:
            upgrade_thread = Thread(target=self._upgrade,
                                    name="upgrade_thread" + server.ip,
                                    args=(upgrade_version, server))
            upgrade_threads.append(upgrade_thread)
            upgrade_thread.start()
        return upgrade_threads

    def offline_cluster_upgrade(self):
        self._install(self.servers[:self.nodes_init])
        self.log.info("Installation done going to sleep for %s sec", self.sleep_time)
        self.operations(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        time.sleep(self.sleep_time)
        if self.during_ops:
            for opn in self.during_ops:
                getattr(self, opn)()
        num_stoped_nodes = self.input.param('num_stoped_nodes', self.nodes_init)
        upgrade_nodes = self.servers[self.nodes_init - num_stoped_nodes :self.nodes_init]
        for upgrade_version in self.upgrade_versions:
            for server in upgrade_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                time.sleep(self.sleep_time)
                remote.disconnect()
            upgrade_threads = self._async_update(upgrade_version, upgrade_nodes)
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            time.sleep(self.expire_time)
            self.verification(self.servers[:self.nodes_init])

    def offline_cluster_upgrade_and_reboot(self):
        self._install(self.servers[:self.nodes_init])
        self.log.info("Installation done going to sleep for %s sec", self.sleep_time)
        self.operations(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        time.sleep(self.sleep_time)
        if self.during_ops:
            for opn in self.during_ops:
                getattr(self, opn)()
        num_stoped_nodes = self.input.param('num_stoped_nodes', self.nodes_init)
        stoped_nodes = self.servers[self.nodes_init - num_stoped_nodes :self.nodes_init]
        for upgrade_version in self.upgrade_versions:
            for server in stoped_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.disconnect()
            time.sleep(self.sleep_time)
            upgrade_threads = self._async_update(upgrade_version, stoped_nodes)
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            for server in stoped_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.disconnect()
            for server in stoped_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.disconnect()
            ClusterOperationHelper.wait_for_ns_servers_or_assert(stoped_nodes, self)
            self.verification(self.servers[:self.nodes_init])

    def offline_cluster_upgrade_and_rebalance(self):
        num_stoped_nodes = self.input.param('num_stoped_nodes', self.nodes_init)
        stoped_nodes = self.servers[self.nodes_init - num_stoped_nodes :self.nodes_init]
        servs_out = self.servers[self.nodes_init - num_stoped_nodes - self.nodes_out :self.nodes_init - num_stoped_nodes]
        servs_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        self._install(self.servers)
        self.log.info("Installation done going to sleep for %s sec", self.sleep_time)
        self.operations(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        time.sleep(self.sleep_time)
        if self.during_ops:
            for opn in self.during_ops:
                getattr(self, opn)()
        for upgrade_version in self.upgrade_versions:

            for server in stoped_nodes:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                remote.disconnect()
            upgrade_threads = self._async_update(upgrade_version, stoped_nodes)
            try:
                self.cluster.rebalance(self.servers[:self.nodes_init], servs_in, servs_out)
            except RebalanceFailedException:
                self.log.info("rebalance failed as expected")
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            ClusterOperationHelper.wait_for_ns_servers_or_assert(stoped_nodes, self)
            self.cluster.rebalance(self.servers[:self.nodes_init], [], servs_out)
            self.verification(list(set(self.servers[:self.nodes_init] + servs_in) - set(servs_out)))

    def offline_cluster_upgrade_non_default_path(self):
        num_nodes_with_not_default = self.input.param('num_nodes_with_not_default', 1)
        data_path = self.input.param('data_path', '/tmp/data').replace('|', "/")
        index_path = self.input.param('index_path', data_path).replace('|', "/")
        servers_with_not_default = self.servers[:num_nodes_with_not_default]
        for server in servers_with_not_default:
            server.data_path = data_path
            server.index_path = index_path
        self._install(self.servers[:self.nodes_init])
        self.log.info("Installation done going to sleep for %s sec", self.sleep_time)
        self.operations(self.servers[:self.nodes_init])
        time.sleep(self.sleep_time)
        if self.ddocs_num and not self.input.param('extra_verification', False):
            self.create_ddocs_and_views()
        for upgrade_version in self.upgrade_versions:
            for server in self.servers[:self.nodes_init]:
                remote = RemoteMachineShellConnection(server)
                remote.stop_server()
                time.sleep(self.sleep_time)
                remote.disconnect()
            upgrade_threads = []
            for server in self.servers[:self.nodes_init]:
                upgrade_thread = Thread(target=self._upgrade,
                                       name="upgrade_thread" + server.ip,
                                       args=(upgrade_version, server))
                upgrade_threads.append(upgrade_thread)
                upgrade_thread.start()
            for upgrade_thread in upgrade_threads:
                upgrade_thread.join()
            time.sleep(self.expire_time)
            self.verification(self.servers[:self.nodes_init])
            for server in servers_with_not_default:
                rest = RestConnection(server)
                node = rest.get_nodes_self()
                self.assertTrue(node.storage[0].path, data_path)
                self.assertTrue(node.storage[0].index_path, index_path)
        if self.input.param('extra_verification', False):
            self.bucket_size = 100
            self._create_sasl_buckets(self.master, "sasl_new")
            self._create_standard_buckets(self.master, "standard_new")
            if self.ddocs_num:
                self.create_ddocs_and_views()
            self.verification(self.servers[:self.nodes_init])

    def online_upgrade_rebalance_in_out(self):
        self._install(self.servers[:self.nodes_init])
        self.log.info("Installation of old version is done. Wait for %s sec for upgrade" % (self.sleep_time))
        self.operations(self.servers[:self.nodes_init])
        if self.ddocs_num:
            self.create_ddocs_and_views()
        time.sleep(self.sleep_time)
        self.initial_version = self.upgrade_versions[0]
        self.product = 'couchbase-server'
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.log.info("Installation of new version is done. Wait for %s sec for rebalance" % (self.sleep_time))
        time.sleep(self.sleep_time)
        if self.during_ops:
            for opn in self.during_ops:
                getattr(self, opn)()
        self.online_upgrade()
        time.sleep(self.sleep_time)
        self.verification(self.servers[self.nodes_init : self.num_servers])

    def online_upgrade_incremental(self):
        self._install(self.servers)
        self.operations(self.servers)
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
            self.verification(self.servers[self.nodes_init : self.num_servers])
        self.master = self.servers[1]
        self.cluster.rebalance(self.servers[:self.num_servers], [], [self.servers[0]])
        self.log.info("Rebalanced out all old version nodes")
        time.sleep(self.sleep_time)
        self.verification(self.servers[self.nodes_init : self.num_servers])

    def online_upgrade(self):
        servers_in = self.servers[self.nodes_init:self.num_servers]
        self.cluster.rebalance(self.servers[:self.nodes_init], servers_in, [])
        self.log.info("Rebalance in all 2.0 Nodes")
        time.sleep(self.sleep_time)
        status, content = ClusterOperationHelper.find_orchestrator(self.master)
        self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
                        format(status, content))
        FIND_MASTER = False
        for new_server in servers_in:
            if content.find(new_server.ip) >= 0:
                self.master = new_server
                self.rest = RestConnection(self.master)
                self.rest_helper = RestHelper(self.rest)
                FIND_MASTER = True
                self.log.info("2.0 Node %s becomes the master" % (new_server.ip))
        if not FIND_MASTER:
            raise Exception("After rebalance in 2.0 Nodes, 2.0 doesn't become the master")

        servers_out = self.servers[:self.nodes_init]
        self.cluster.rebalance(self.servers[:self.num_servers], [], servers_out)
        self.log.info("Rebalanced out all old version nodes")
        time.sleep(self.sleep_time)

    def online_upgrade_swap_rebalance(self):
        self._install(self.servers[:self.nodes_init])
        self.log.info("Installation of old version is done. Wait for %s sec for upgrade" % (self.sleep_time))
        self.operations(self.servers[:self.nodes_init])
        time.sleep(self.sleep_time)
        self.initial_version = self.upgrade_versions[0]
        self.product = 'couchbase-server'
        self._install(self.servers[self.nodes_init:self.num_servers])
        self.log.info("Installation of new version is done. Wait for %s sec for rebalance" % (self.sleep_time))
        time.sleep(self.sleep_time)

        self.swap_num_servers = self.input.param('swap_num_servers', 1)
        old_servers = self.servers[:self.nodes_init]
        new_servers = []
        for i in range(self.nodes_init / self.swap_num_servers):
            servers_in = self.servers[(self.nodes_init + i * self.swap_num_servers):
                                      (self.nodes_init + (i + 1) * self.swap_num_servers)]
            servers_out = self.servers[(i * self.swap_num_servers):((i + 1) * self.swap_num_servers)]
            servers = old_servers + new_servers
            self.cluster.rebalance(servers, servers_in, servers_out)
            self.log.info("Swap rebalance: rebalance out %s old version nodes, rebalance in %s 2.0 Nodes"
                          % (self.swap_num_servers, self.swap_num_servers))
            time.sleep(self.sleep_time)
            old_servers = self.servers[((i + 1) * self.swap_num_servers):self.nodes_init]
            new_servers = new_servers + servers_in
            servers = old_servers + new_servers
            status, content = ClusterOperationHelper.find_orchestrator(servers[0])
            self.assertTrue(status, msg="Unable to find orchestrator: {0}:{1}".\
                            format(status, content))
            FIND_MASTER = False
            for new_server in new_servers:
                if content.find(new_server.ip) >= 0:
                    self.master = new_server
                    self.rest = RestConnection(self.master)
                    self.rest_helper = RestHelper(self.rest)
                    FIND_MASTER = True
                    self.log.info("2.0 Node %s becomes the master" % (new_server.ip))
            if not FIND_MASTER:
                raise Exception("After rebalance in 2.0 nodes, 2.0 doesn't become the master ")
        self.verification(self.servers[self.nodes_init : self.num_servers])
