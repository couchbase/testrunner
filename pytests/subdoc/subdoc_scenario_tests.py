from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from couchbase_helper.query_definitions import QueryDefinition
from membase.helper.cluster_helper import ClusterOperationHelper
from subdoc_autotestgenerator import SubdocAutoTestGenerator
import copy

class SubducScenarioTests(SubdocAutoTestGenerator):

    def setUp(self):
        super(SubducScenarioTests, self).setUp()
        self.find_nodes_in_list()
        self.generate_map_nodes_out_dist()
        self.run_sync_data()

    def tearDown(self):
        super(SubducScenarioTests, self).tearDown()

    def test_rebalance_in(self):
        try:
            self.run_async_data()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],self.nodes_in_list, [], services = self.services_in)
            self.run_mutation_operations_for_situational_tests()
            for t in self.load_thread_list:
                if t.isAlive():
                    if t != None:
                        t.signal = False
            rebalance.result()
        except Exception, ex:
            raise

    def test_rebalance_out(self):
        try:
            self.run_async_data()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],[],self.nodes_out_list)
            self.run_mutation_operations_for_situational_tests()
            for t in self.load_thread_list:
                if t.isAlive():
                    if t != None:
                        t.signal = False
        except Exception, ex:
            raise

    def test_rebalance_in_out(self):
        try:
            self.run_async_data()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                    self.nodes_in_list,
                                   self.nodes_out_list, services = self.services_in)
            self.run_mutation_operations_for_situational_tests()
            for t in self.load_thread_list:
                if t.isAlive():
                    if t != None:
                        t.signal = False
        except Exception, ex:
            raise

    def test_rebalance_with_stop_start(self):
        try:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                    self.nodes_in_list,
                                   self.nodes_out_list, services = self.services_in)
            stopped = RestConnection(self.master).stop_rebalance(wait_timeout=self.wait_timeout / 3)
            self.assertTrue(stopped, msg="unable to stop rebalance")
            rebalance.result()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                    self.nodes_in_list,
                                   self.nodes_out_list, services = self.services_in)
            self.test_mutation_operations()
            rebalance.result()
        except Exception, ex:
            raise

    def test_failover(self):
        try:
            self.run_async_data()
            servr_out = self.nodes_out_list
            failover_task = self.cluster.async_failover([self.master],
                    failover_nodes = servr_out, graceful=self.graceful)
            failover_task.result()
            if self.graceful:
                # Check if rebalance is still running
                msg = "graceful failover failed for nodes"
                self.assertTrue(RestConnection(self.master).monitorRebalance(stop_if_loop=True), msg=msg)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                   [], servr_out)
            self.run_mutation_operations_for_situational_tests()
            for t in self.load_thread_list:
                if t.isAlive():
                    if t != None:
                        t.signal = False
        except Exception, ex:
            raise

    def test_failover_add_back(self):
        try:
            self.run_async_data()
            rest = RestConnection(self.master)
            recoveryType = self.input.param("recoveryType", "full")
            servr_out = self.nodes_out_list
            nodes_all = rest.node_statuses()
            failover_task =self.cluster.async_failover([self.master],
                    failover_nodes = servr_out, graceful=self.graceful)
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
            self.run_mutation_operations_for_situational_tests()
            for t in self.load_thread_list:
                if t.isAlive():
                    if t != None:
                        t.signal = False
        except Exception, ex:
            raise

    def test_autofailover(self):
        self.run_async_data()
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
            self.run_mutation_operations_for_situational_tests()
            for t in self.load_thread_list:
                if t.isAlive():
                    if t != None:
                        t.signal = False
        except Exception, ex:
            raise
        finally:
            remote.start_server()
            tasks = self.async_check_and_run_operations(buckets = self.buckets, after = True)
            for task in tasks:
                task.result()

    def test_network_partitioning(self):
        try:
            self.run_async_data()
            for node in self.nodes_out_list:
                self.start_firewall_on_node(node)
            self.run_mutation_operations_for_situational_tests()
            for t in self.load_thread_list:
                if t.isAlive():
                    if t != None:
                        t.signal = False
        except Exception, ex:
            raise
        finally:
            for node in self.nodes_out_list:
                self.stop_firewall_on_node(node)

    def test_couchbase_bucket_compaction(self):
        # Run Compaction Here
        # Run auto-compaction to remove the tomb stones
        compact_tasks = []
        self.run_async_data()
        for bucket in self.buckets:
            compact_tasks.append(self.cluster.async_compact_bucket(self.master,bucket))
        self.run_mutation_operations_for_situational_tests()
        for task in compact_tasks:
            task.result()
        for t in self.load_thread_list:
            if t.isAlive():
                if t != None:
                    t.signal = False


    def test_warmup(self):
        self.run_async_data()
        for server in self.nodes_out_list:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.start_server()
            remote.disconnect()
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        self.run_mutation_operations_for_situational_tests()
        for t in self.load_thread_list:
            if t.isAlive():
                if t != None:
                    t.signal = False

    def test_couchbase_bucket_flush(self):
        #Flush the bucket
        self.run_async_data()
        for bucket in self.buckets:
            RestConnection(self.master).flush_bucket(bucket.name)
        self.run_mutation_operations_for_situational_tests()
        for t in self.load_thread_list:
            if t.isAlive():
                if t != None:
                    t.signal = False
