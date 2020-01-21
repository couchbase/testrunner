from .base_2i import BaseSecondaryIndexingTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection

class SecondaryIndexingBootstrapTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingBootstrapTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingBootstrapTests, self).tearDown()

    def test_rebalance_in(self):
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], self.nodes_in_list, [], services=self.services_in)
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception as ex:
            raise

    def test_rebalance_out(self):
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], self.nodes_out_list)
            self.sleep(3)
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception as ex:
            raise

    def test_rebalance_in_out(self):
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                    self.nodes_in_list,
                                   self.nodes_out_list, services=self.services_in)
            self.sleep(3)
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception as ex:
            raise

    def test_rebalance_with_stop_start(self):
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                    self.nodes_in_list,
                                   self.nodes_out_list, services=self.services_in)
            self.sleep(3)
            tasks = self.async_run_operations(buckets=self.buckets, phase="in_between")
            # runs operations
            for task in tasks:
                task.result()
            stopped = RestConnection(self.master).stop_rebalance(wait_timeout=self.wait_timeout // 3)
            self.assertTrue(stopped, msg="unable to stop rebalance")
            rebalance.result()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                    self.nodes_in_list,
                                   self.nodes_out_list, services=self.services_in)
            rebalance.result()
            self.run_after_operations()
        except Exception as ex:
            raise

    def test_failover(self):
        try:
            tasks = self.async_run_operations(buckets=self.buckets, phase="before")
            for task in tasks:
                task.result()
            servr_out = self.nodes_out_list
            failover_task = self.cluster.async_failover([self.master],
                    failover_nodes=servr_out, graceful=self.graceful)
            failover_task.result()
            self.log.info ("Rebalance first time")
            rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
            self.log.info ("Rebalance Second time")
            rebalance = self.cluster.rebalance(self.servers[:self.nodes_init], [], [])
            tasks = self.async_run_operations(buckets=self.buckets, phase="in_between")
            self._run_aync_tasks()
            self.run_after_operations()
        except Exception as ex:
            raise

    def test_failover_add_back(self):
        try:
            rest = RestConnection(self.master)
            recoveryType = self.input.param("recoveryType", "full")
            servr_out = self.nodes_out_list
            nodes_all = rest.node_statuses()
            tasks = self.async_run_operations(buckets=self.buckets, phase="before")
            for task in tasks:
                task.result()
            failover_task = self.cluster.async_failover([self.master],
                    failover_nodes=servr_out, graceful=self.graceful)
            failover_task.result()
            nodes_all = rest.node_statuses()
            nodes = []
            if servr_out[0].ip == "127.0.0.1":
                for failover_node in servr_out:
                    nodes.extend([node for node in nodes_all
                        if (str(node.port) == failover_node.port)])
            else:
                for failover_node in servr_out:
                    nodes.extend([node for node in nodes_all
                        if node.ip == failover_node.ip])
            for node in nodes:
                self.log.info(node)
                rest.add_back_node(node.id)
                rest.set_recovery_type(otpNode=node.id, recoveryType=recoveryType)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception as ex:
            raise

    def test_autofailover(self):
        autofailover_timeout = 30
        status = RestConnection(self.master).update_autofailover_settings(True, autofailover_timeout)
        self.assertTrue(status, 'failed to change autofailover_settings!')
        servr_out = self.nodes_out_list
        remote = RemoteMachineShellConnection(servr_out[0])
        try:
            remote.stop_server()
            self.sleep(autofailover_timeout + 10, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                   [], [servr_out[0]])
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception as ex:
            raise
        finally:
            remote.start_server()
            tasks = self.async_run_operations(buckets=self.buckets, phase="after")
            for task in tasks:
                task.result()

    def test_network_partitioning(self):
        try:
            for node in self.nodes_out_list:
                self.start_firewall_on_node(node)
            self._run_aync_tasks()
            self.run_after_operations()
        except Exception as ex:
            raise
        finally:
            for node in self.nodes_out_list:
                self.stop_firewall_on_node(node)
            self.sleep(10)

    def _run_aync_tasks(self):
        # Run Data verification
        self.run_doc_ops()

    def run_after_operations(self):
        # Run Data verification
        servers = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        self.verify_cluster_stats(servers, check_ep_items_remaining=True)