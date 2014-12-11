from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from base_2i import BaseSecondaryIndexingTests

class SecondaryIndexingRecoveryTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingRecoveryTests, self).setUp()

    def suite_setUp(self):
        super(SecondaryIndexingRecoveryTests, self).suite_setUp()

    def tearDown(self):
        super(SecondaryIndexingRecoveryTests, self).tearDown()

    def suite_tearDown(self):
        super(SecondaryIndexingRecoveryTests, self).suite_tearDown()

    def test_incr_rebalance_in(self):
        self.check_and_run_operations(before = True)
        self.assertTrue(len(self.servers) >= self.nodes_in + 1, "Servers are not enough")
        for i in xrange(1, self.nodes_in + 1):
            rebalance = self.cluster.async_rebalance(self.servers[:i],
                                                     self.servers[i:i+1], [])
            self.check_and_run_operations(in_between = True)
            rebalance.result()
        self.check_and_run_operations(after = True)

    def test_incr_rebalance_out(self):
        self.assertTrue(len(self.servers[:self.nodes_init]) > self.nodes_out,
                        "Servers are not enough")
        self.check_and_run_operations(before = True)
        for i in xrange(1, self.nodes_out + 1):
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init - (i-1)],
                                    [],
                                    self.servers[self.nodes_init - i:self.nodes_init - (i-1)])
            self.check_and_run_operations(in_between = True)
            rebalance.result()
        self.check_and_run_operations(after = True)

    def test_swap_rebalance(self):
        self.check_and_run_operations(before = True)
        self.assertTrue(len(self.servers) >= self.nodes_init + self.nodes_in,
                        "Servers are not enough")
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                               self.servers[self.nodes_init:self.nodes_init + self.nodes_in],
                               self.servers[self.nodes_init - self.nodes_out:self.nodes_init])
        self.check_and_run_operations(in_between = True)
        rebalance.result()
        self.check_and_run_operations(after = True)

    def test_rebalance_with_server_crash(self):
        self.check_and_run_operations(before = True)
        servr_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        self.test_case()
        for i in xrange(3):
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     servr_in, servr_out)
            self.sleep(5, "Wait some time for rebalance process and then kill memcached")
            remote = RemoteMachineShellConnection(self.servers[self.nodes_init -1])
            remote.terminate_process(process_name='memcached')
            try:
                self.check_and_run_operations(in_between = True)
                rebalance.result()
            except:
                pass
        self.check_and_run_operations(after = True)
        self.cluster.rebalance(self.servers[:self.nodes_init], servr_in, servr_out)

    def test_failover(self):
        self.check_and_run_operations(before = True)
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        self.cluster.failover(self.servers[:self.nodes_init], servr_out)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                               [], servr_out)
        self.check_and_run_operations(in_between = True)
        rebalance.result()
        self.check_and_run_operations(after = True)

    def test_failover_add_back(self):
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        nodes_all = RestConnection(self.master).node_statuses()
        nodes = []
        self.check_and_run_operations(before = True)
        for failover_node in servr_out:
            nodes.extend([node for node in nodes_all
                if node.ip != failover_node.ip or str(node.port) != failover_node.port])
        self.cluster.failover(self.servers[:self.nodes_init], servr_out)
        for node in nodes:
            RestConnection(self.master).add_back_node(node.id)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        self.check_and_run_operations(in_between = True)
        rebalance.result()
        self.check_and_run_operations(after = True)

    def test_autofailover(self):
        autofailover_timeout = 30
        status = RestConnection(self.master).update_autofailover_settings(True, autofailover_timeout)
        self.assertTrue(status, 'failed to change autofailover_settings!')
        self.check_and_run_operations(before = True)
        servr_out = self.servers[self.nodes_init - self.nodes_out:self.nodes_init]
        remote = RemoteMachineShellConnection(self.servers[self.nodes_init -1])
        try:
            remote.stop_server()
            self.sleep(autofailover_timeout + 10, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                   [], servr_out)
            self.check_and_run_operations(in_between = True)
            rebalance.result()
        finally:
            remote.start_server()
            self.check_and_run_operations(after = True)

    def test_warmup(self):
        num_srv_warm_up = self.input.param("srv_warm_up", self.nodes_init)
        if self.input.tuq_client is None:
            self.fail("For this test external tuq server is requiered. " +\
                      "Please specify one in conf")
        self.check_and_run_operations(before = True)
        for server in self.servers[self.nodes_init - num_srv_warm_up:self.nodes_init]:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.start_server()
            remote.disconnect()
        #run query, result may not be as expected, but tuq shouldn't fail
        try:
            self.check_and_run_operations(in_between = True)
        except:
            pass
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        self.check_and_run_operations(after = True)

