from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from base_2i import BaseSecondaryIndexingTests

class SecondaryIndexingRecoveryTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingRecoveryTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingRecoveryTests, self).tearDown()

    def test_rebalance_in(self):
        self.check_and_run_operations(buckets = self.buckets, before = True)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],self.nodes_in_list, [], services = self.services_in)
        self.sleep(3)
        self._run_aync_taks()
        rebalance.result()
        self.check_and_run_operations(buckets = self.buckets, after = True)


    def test_rebalance_out(self):
        self.check_and_run_operations(buckets = self.buckets, before = True)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],[],self.nodes_out_list)
        self.sleep(3)
        self._run_aync_taks()
        rebalance.result()
        self.check_and_run_operations(buckets = self.buckets, after = True)

    def test_rebalance_in_out(self):
        self.check_and_run_operations(buckets = self.buckets, before = True)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                self.nodes_in_list,
                               self.nodes_out_list, services = self.services_in)
        self.sleep(3)
        self._run_aync_taks()
        rebalance.result()
        self.check_and_run_operations(buckets = self.buckets, after = True)

    def test_server_crash(self):
        self.check_and_run_operations(buckets = self.buckets, before = True)
        self.sleep(5, "Wait some time for rebalance process and then kill memcached")
        for node in self.nodes_out_list:
            remote = RemoteMachineShellConnection(node)
            remote.terminate_process(process_name='memcached')
        self.sleep(3)
        self._run_aync_taks()
        self.check_and_run_operations(buckets = self.buckets, after = True)

    def test_server_retstart(self):
        self.check_and_run_operations(buckets = self.buckets, before = True)
        self.sleep(5, "Wait some time for rebalance process and then restart server")
        for node in self.nodes_out_list:
            remote = RemoteMachineShellConnection(node)
            remote.stop_server()
        self.sleep(3)
        self.check_and_run_operations(buckets = self.buckets, in_between = True)
        for node in self.nodes_out_list:
            remote = RemoteMachineShellConnection(node)
            remote.start_server()
        self._run_aync_taks()
        self.check_and_run_operations(buckets = self.buckets, after = True)

    def test_failover(self):
        self.check_and_run_operations(buckets = self.buckets, before = True)
        servr_out = self.nodes_out_list
        failover_task = self.cluster.async_failover([self.master],
                failover_nodes = servr_out, graceful=self.graceful)
        self.check_and_run_operations(buckets = self.buckets, in_between = True)
        failover_task.result()
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                               [], servr_out)
        self._run_aync_taks()
        rebalance.result()
        self.check_and_run_operations(buckets = self.buckets, after = True)

    def test_failover_add_back(self):
        recoveryType = self.input.param("recoveryType", "full")
        servr_out = self.nodes_out_list
        nodes_all = RestConnection(self.master).node_statuses()
        self.check_and_run_operations(buckets = self.buckets, before = True)
        failover_task =self.cluster.async_failover([self.master],
                failover_nodes = servr_out, graceful=self.graceful)
        self.check_and_run_operations(buckets = self.buckets, in_between = True)
        failover_task.result()
        self.log.info(servr_out)
        rest = RestConnection(self.master)
        nodes_all = rest.node_statuses()
        nodes = []
        for failover_node in servr_out:
            nodes.extend([node for node in nodes_all
                if node.ip == failover_node.ip or (node.ip == "127.0.0.1" and str(node.port) != failover_node.port)])
        for node in nodes:
            rest.add_back_node(node.id)
            rest.set_recovery_type(otpNode=node.id, recoveryType=recoveryType)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
        self._run_aync_taks()
        rebalance.result()
        self.check_and_run_operations(buckets = self.buckets, after = True)

    def test_autofailover(self):
        autofailover_timeout = 30
        status = RestConnection(self.master).update_autofailover_settings(True, autofailover_timeout)
        self.assertTrue(status, 'failed to change autofailover_settings!')
        self.check_and_run_operations(buckets = self.buckets, before = True)
        servr_out = self.nodes_out_list
        remote = RemoteMachineShellConnection(servr_out[0])
        try:
            remote.stop_server()
            self.sleep(autofailover_timeout + 10, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                   [], servr_out)
            self._run_aync_taks()
            rebalance.result()
        finally:
            remote.start_server()
            self.check_and_run_operations(buckets = self.buckets, after = True)

    def test_warmup(self):
        num_srv_warm_up = self.input.param("srv_warm_up", self.nodes_init)
        if self.input.tuq_client is None:
            self.fail("For this test external tuq server is requiered. " +\
                      "Please specify one in conf")
        self.check_and_run_operations(buckets = self.buckets, before = True)
        for server in self.servers[self.nodes_init - num_srv_warm_up:self.nodes_init]:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.start_server()
            remote.disconnect()
        #run query, result may not be as expected, but tuq shouldn't fail
        try:
            self._run_aync_taks()
        except:
            pass
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        self.check_and_run_operations(buckets = self.buckets, after = True)

    def _run_aync_taks(self):
        tasks = self.async_check_and_run_operations(buckets = self.buckets, in_between = True)
        # runs operations
        self.run_doc_ops()
        for task in tasks:
            task.result()