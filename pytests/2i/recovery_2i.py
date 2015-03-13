from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from base_2i import BaseSecondaryIndexingTests
import copy

class SecondaryIndexingRecoveryTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingRecoveryTests, self).setUp()

    def tearDown(self):
        if hasattr(self, 'query_definitions'):
            check = True
            try:
                self.log.info("<<<<<< WILL DROP THE INDEXES >>>>>")
                tasks = self.async_run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions)
                for task in tasks:
                    task.result()
            except Exception, ex:
                self.log.info(ex)
        super(SecondaryIndexingRecoveryTests, self).tearDown()

    def test_rebalance_in(self):
        try:
            tasks = self.async_check_and_run_operations(buckets = self.buckets, before = True)
            for task in tasks:
                task.result()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],self.nodes_in_list, [], services = self.services_in)
            self.sleep(3)
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception, ex:
            raise

    def test_rebalance_out(self):
        try:
            tasks = self.async_check_and_run_operations(buckets = self.buckets, before = True)
            for task in tasks:
                task.result()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],[],self.nodes_out_list)
            self.sleep(3)
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception, ex:
            raise

    def test_rebalance_in_out(self):
        try:
            tasks = self.async_check_and_run_operations(buckets = self.buckets, before = True)
            for task in tasks:
                task.result()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                    self.nodes_in_list,
                                   self.nodes_out_list, services = self.services_in)
            self.sleep(3)
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception, ex:
            raise

    def test_rebalance_with_stop_start(self):
        try:
            tasks = self.async_check_and_run_operations(buckets = self.buckets, before = True)
            for task in tasks:
                task.result()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                    self.nodes_in_list,
                                   self.nodes_out_list, services = self.services_in)
            self.sleep(3)
            tasks = self.async_check_and_run_operations(buckets = self.buckets, in_between = True)
            # runs operations
            for task in tasks:
                task.result()
            stopped = RestConnection(self.master).stop_rebalance(wait_timeout=self.wait_timeout / 3)
            self.assertTrue(stopped, msg="unable to stop rebalance")
            rebalance.result()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                    self.nodes_in_list,
                                   self.nodes_out_list, services = self.services_in)
            rebalance.result()
            self.run_after_operations()
        except Exception, ex:
            raise

    def test_server_crash(self):
        try:
            self.targetProcess= self.input.param("targetProcess",'memcached')
            tasks = self.async_check_and_run_operations(buckets = self.buckets, before = True)
            for task in tasks:
                task.result()
            self.sleep(5, "Wait some time for rebalance process and then kill {0}".format(self.targetProcess))
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.terminate_process(process_name=self.targetProcess)
            self.sleep(3)
            self._run_aync_tasks()
            self.run_after_operations()
        except Exception, ex:
            raise

    def test_server_restart(self):
        try:
            tasks = self.async_check_and_run_operations(buckets = self.buckets, before = True)
            for task in tasks:
                task.result()
            self.sleep(5, "Wait some time for rebalance process and then restart server")
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.stop_server()
            self.sleep(3)
            self._run_aync_tasks()
            self.run_after_operations()
        except Exception, ex:
            raise
        finally:
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.start_server()

    def test_failover(self):
        try:
            tasks = self.async_check_and_run_operations(buckets = self.buckets, before = True)
            for task in tasks:
                task.result()
            servr_out = self.nodes_out_list
            failover_task = self.cluster.async_failover([self.master],
                    failover_nodes = servr_out, graceful=self.graceful)
            tasks = self.async_check_and_run_operations(buckets = self.buckets, in_between = True)
            failover_task.result()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                   [], servr_out)
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception, ex:
            raise

    def test_failover_add_back(self):
        try:
            rest = RestConnection(self.master)
            recoveryType = self.input.param("recoveryType", "full")
            servr_out = self.nodes_out_list
            nodes_all = rest.node_statuses()
            tasks = self.async_check_and_run_operations(buckets = self.buckets, before = True)
            for task in tasks:
                task.result()
            failover_task =self.cluster.async_failover([self.master],
                    failover_nodes = servr_out, graceful=self.graceful)
            tasks = self.async_check_and_run_operations(buckets = self.buckets, in_between = True)
            failover_task.result()
            self.log.info(servr_out)
            nodes_all = rest.node_statuses()
            nodes = []
            for failover_node in servr_out:
                nodes.extend([node for node in nodes_all
                    if node.ip == failover_node.ip or (node.ip == "127.0.0.1" and str(node.port) != failover_node.port)])
            for node in nodes:
                rest.add_back_node(node.id)
                rest.set_recovery_type(otpNode=node.id, recoveryType=recoveryType)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [])
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception, ex:
            raise

    def test_autofailover(self):
        autofailover_timeout = 30
        status = RestConnection(self.master).update_autofailover_settings(True, autofailover_timeout)
        self.assertTrue(status, 'failed to change autofailover_settings!')
        tasks = self.async_check_and_run_operations(buckets = self.buckets, before = True)
        for task in tasks:
            task.result()
        servr_out = self.nodes_out_list
        remote = RemoteMachineShellConnection(servr_out[0])
        try:
            remote.stop_server()
            self.sleep(autofailover_timeout + 10, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                   [], servr_out)
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception, ex:
            raise
        finally:
            remote.start_server()
            tasks = self.async_check_and_run_operations(buckets = self.buckets, after = True)
            for task in tasks:
                task.result()

    def test_network_partitioning(self):
        tasks = self.async_check_and_run_operations(buckets = self.buckets, before = True)
        for task in tasks:
            task.result()
        try:
            for node in self.nodes_out_list:
                self.start_firewall_on_node(node)
            self.sleep(30, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                   [], self.nodes_out_list)
            self._run_aync_tasks()
            rebalance.result()
            self.run_after_operations()
        except Exception, ex:
            raise
        finally:
            for node in self.nodes_out_list:
                self.stop_firewall_on_node(node)

    def test_warmup(self):
        tasks = self.async_check_and_run_operations(buckets = self.buckets, before = True)
        for task in tasks:
            task.result()
        for server in self.nodes_out_list:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.start_server()
            remote.disconnect()
        tasks = self.async_check_and_run_operations(buckets = self.buckets, in_between = True)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        for task in tasks:
            task.result()
        self.run_after_operations()

    def _calculate_scan_vector(self):
        self.scan_vectors = None
        if self.scan_vectors != None:
            self.scan_vectors = self.gen_scan_vector(use_percentage = self.scan_vector_per_values,
             use_random = self.random_scan_vector)

    def _run_aync_tasks(self):
        self._calculate_scan_vector()
        if self.doc_ops:
            self._run_async_tasks_with_ops()
        else:
            qdfs = []
            if self.ops_map["in_between"]["query_ops"] or self.ops_map["after"]["query_ops"]:
                for query_definition in self.query_definitions:
                    if query_definition.index_name in self.index_lost_during_move_out:
                        query_definition.index_name = "#primary"
                    qdfs.append(query_definition)
                self.query_definitions = qdfs
            tasks = self.async_check_and_run_operations(buckets = self.buckets, in_between = True,
             scan_consistency = self.scan_consistency, scan_vectors = self.scan_vectors)
            for task in tasks:
                task.result()

    def _run_async_tasks_with_ops(self):
        # runs operations
        self.run_doc_ops()
        tasks = self.async_check_and_run_operations(buckets = self.buckets, in_between = True,
             scan_consistency = self.scan_consistency, scan_vectors = self.scan_vectors)
        for task in tasks:
            task.result()

    def run_after_operations(self):
        tasks = self.async_check_and_run_operations(buckets = self.buckets, after = True)
        for task in tasks:
            task.result()