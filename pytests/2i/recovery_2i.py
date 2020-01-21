import logging
from threading import Thread
import time

from .base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition
from lib.memcached.helper.data_helper import MemcachedClientHelper
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection

log = logging.getLogger(__name__)

class SecondaryIndexingRecoveryTests(BaseSecondaryIndexingTests):

    def setUp(self):
        self.use_replica = True
        super(SecondaryIndexingRecoveryTests, self).setUp()
        self.load_query_definitions = []
        self.initial_index_number = self.input.param("initial_index_number", 10)
        for x in range(self.initial_index_number):
            index_name = "index_name_" + str(x)
            query_definition = QueryDefinition(
                index_name=index_name, index_fields=["VMs"],
                query_template="SELECT * FROM %s ", groups=["simple"],
                index_where_clause = " VMs IS NOT NULL ")
            self.load_query_definitions.append(query_definition)
        if self.load_query_definitions:
            self.multi_create_index(buckets=self.buckets,
                                    query_definitions=self.load_query_definitions)

    def tearDown(self):
        if hasattr(self, 'query_definitions') and not self.skip_cleanup:
            try:
                self.log.info("<<<<<< WILL DROP THE INDEXES >>>>>")
                tasks = self.async_multi_drop_index(
                    buckets=self.buckets, query_definitions=self.query_definitions)
                for task in tasks:
                    task.result()
                self.async_multi_drop_index(
                    buckets=self.buckets, query_definitions=self.load_query_definitions)
            except Exception as ex:
                log.info(ex)
        super(SecondaryIndexingRecoveryTests, self).tearDown()

    '''Test that checks if indexes that are ready during index warmup can be used'''
    def test_use_index_during_warmup(self):
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=False)
        rest = RestConnection(index_node)
        # Change indexer snapshot for a recovery point
        doc = {"indexer.settings.persisted_snapshot.moi.interval":60000}
        rest.set_index_settings(doc)

        create_index_query = "CREATE INDEX idx ON default(age)"
        create_index_query2 = "CREATE INDEX idx1 ON default(age)"
        create_index_query3 = "CREATE INDEX idx2 ON default(age)"
        create_index_query4 = "CREATE INDEX idx3 ON default(age)"
        create_index_query5 = "CREATE INDEX idx4 ON default(age)"
        try:
            self.n1ql_helper.run_cbq_query(query=create_index_query,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query2,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query3,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query4,
                                           server=self.n1ql_node)
            self.n1ql_helper.run_cbq_query(query=create_index_query5,
                                           server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail(
                "index creation failed with error : {0}".format(str(ex)))

        self.wait_until_indexes_online()

        rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=256)

        master_rest = RestConnection(self.master)

        self.shell.execute_cbworkloadgen(master_rest.username, master_rest.password, 700000, 100, "default", 1024, '-j')

        index_stats = rest.get_indexer_stats()
        self.log.info(index_stats["indexer_state"])
        self.assertTrue(index_stats["indexer_state"].lower() != 'warmup')

        # Sleep for 60 seconds to allow a snapshot to be created
        self.sleep(60)

        t1 = Thread(target=self.monitor_index_stats, name="monitor_index_stats", args=([index_node, 60]))

        t1.start()

        shell = RemoteMachineShellConnection(index_node)
        output1, error1 = shell.execute_command("killall -9 indexer")

        t1.join()

        use_index_query = "select * from default where age > 30"

        # Results are not garunteed to be accurate so the query successfully running is all we can check
        try:
            results = self.n1ql_helper.run_cbq_query(query=use_index_query, server=self.n1ql_node)
        except Exception as ex:
            self.log.info(str(ex))
            self.fail("query should run correctly, an index is available for use")

    '''Ensure that the index is in warmup, but there is an index ready to be used'''
    def monitor_index_stats(self, index_node=None, timeout=600):
        index_usable = False
        rest = RestConnection(index_node)
        init_time = time.time()
        next_time = init_time

        while not index_usable:
            index_stats = rest.get_indexer_stats()
            self.log.info(index_stats["indexer_state"])
            index_map = self.get_index_map()

            if index_stats["indexer_state"].lower() == 'warmup':
                for index in index_map['default']:
                    if index_map['default'][index]['status'] == 'Ready':
                        index_usable = True
                        break
            else:
                next_time = time.time()
            index_usable = index_usable or (next_time - init_time > timeout)
        return

    def test_rebalance_in(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                self.nodes_in_list,
                [], services=self.services_in)
            mid_recovery_tasks = self.async_run_operations(
                phase="in_between")
            rebalance.result()
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_rebalance_out(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            #self._create_replica_indexes()
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], self.nodes_out_list)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            rebalance.result()
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_rebalance_in_out(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            #self._create_replica_indexes()
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], self.nodes_in_list,
                self.nodes_out_list, services=self.services_in)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            rebalance.result()
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_rebalance_in_out_multi_nodes(self):
        """
        MB-16220
        1. Create cluster + Indexes
        2. Run Queries
        3. Rebalance out DAta and Rebalance In Data node.
        4. Rebalance out Index and Rebalance in Index Node.
        """
        try:
            extra_nodes = self.servers[self.nodes_init:]
            self.assertGreaterEqual(
                len(extra_nodes), 2,
                "Sufficient nodes not available for rebalance")
            self.nodes_out = 1
            self.nodes_in_list = [extra_nodes[0]]
            self.nodes_out_dist = "kv:1"
            self.services_in = ["kv"]
            self.targetMaster = False
            self.generate_map_nodes_out_dist()
            pre_recovery_tasks = self.async_run_operations(phase="before")
            self._run_tasks([pre_recovery_tasks])
            self.get_dgm_for_plasma()
            kvOps_tasks = self._run_kvops_tasks()
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                self.nodes_in_list,
                self.nodes_out_list,
                services=self.services_in)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            rebalance.result()
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self.nodes_out_dist = "index:1"
            self.services_in = ["index"]
            self.nodes_in_list = [extra_nodes[1]]
            self.generate_map_nodes_out_dist()
            #self._create_replica_indexes()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                    self.nodes_in_list,
                                   self.nodes_out_list, services = self.services_in)
            rebalance.result()
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_rebalance_with_stop_start(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            #self._create_replica_indexes()
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                self.nodes_in_list,
                self.nodes_out_list, services=self.services_in)
            stopped = RestConnection(self.master).stop_rebalance(
                wait_timeout=self.wait_timeout // 3)
            self.assertTrue(stopped, msg="Unable to stop rebalance")
            rebalance.result()
            self.sleep(100)
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], self.nodes_in_list,
                self.nodes_out_list, services=self.services_in)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            rebalance.result()
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_server_crash(self):
        if self.doc_ops:
            return
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            self.use_replica=False
            self._create_replica_indexes()
            self.targetProcess= self.input.param("targetProcess", 'memcached')
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                if self.targetProcess == "memcached":
                    remote.kill_memcached()
                else:
                    remote.terminate_process(process_name=self.targetProcess)
            self.sleep(60)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_server_stop(self):
        if self.doc_ops:
            return
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            self._create_replica_indexes()
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.stop_server()
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise
        finally:
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.start_server()
            self.sleep(20)

    def test_server_restart(self):
        if self.doc_ops:
            return
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.stop_server()
            self.sleep(30)
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                remote.start_server()
            self.sleep(30)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_failover(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            self._create_replica_indexes()
            servr_out = self.nodes_out_list
            failover_task = self.cluster.async_failover(
                [self.master],
                failover_nodes=servr_out,
                graceful=self.graceful)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            failover_task.result()
            if self.graceful:
                # Check if rebalance is still running
                msg = "graceful failover failed for nodes"
                check_rblnc = RestConnection(self.master).monitorRebalance(
                    stop_if_loop=True)
                self.assertTrue(check_rblnc, msg=msg)
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], [], servr_out)
            rebalance.result()
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_failover_add_back(self):
        try:
            rest = RestConnection(self.master)
            recoveryType = self.input.param("recoveryType", "full")
            servr_out = self.nodes_out_list
            failover_task =self.cluster.async_failover([self.master],
                    failover_nodes=servr_out, graceful=self.graceful)
            failover_task.result()
            pre_recovery_tasks = self.async_run_operations(phase="before")
            self._run_tasks([pre_recovery_tasks])
            self.get_dgm_for_plasma()
            kvOps_tasks = self._run_kvops_tasks()
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
                log.info("Adding Back: {0}".format(node))
                rest.add_back_node(node.id)
                rest.set_recovery_type(otpNode=node.id,
                                       recoveryType=recoveryType)
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], [], [])
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            rebalance.result()
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_failover_indexer_add_back(self):
        """
        Indexer add back scenarios
        :return:
        """
        rest = RestConnection(self.master)
        recoveryType = self.input.param("recoveryType", "full")
        indexer_out = int(self.input.param("nodes_out", 0))
        nodes = self.get_nodes_from_services_map(service_type="index",
                                                 get_all_nodes=True)
        self.assertGreaterEqual(len(nodes), indexer_out,
                                "Existing Indexer Nodes less than Indexer out nodes")
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            self.use_replica = False
            self._create_replica_indexes()
            servr_out = nodes[:indexer_out]
            failover_task =self.cluster.async_failover(
                [self.master], failover_nodes=servr_out,
                graceful=self.graceful)
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
                    log.info("Adding back {0} with recovery type {1}...".format(
                        node.ip, recoveryType))
                    rest.add_back_node(node.id)
                    rest.set_recovery_type(otpNode=node.id,
                                           recoveryType=recoveryType)
            log.info("Rebalancing nodes in...")
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], [], [])
            rebalance.result()
            self._run_tasks([mid_recovery_tasks, kvOps_tasks])
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_failover_indexer_restart(self):
        """
        CBQE-3153
        Indexer add back scenarios
        :return:
        """
        index_servers = self.get_nodes_from_services_map(service_type="index",
            get_all_nodes=True)
        self.multi_create_index(self.buckets, self.query_definitions)
        self.get_dgm_for_plasma()
        self.sleep(30)
        kvOps_tasks = self._run_kvops_tasks()
        remote = RemoteMachineShellConnection(index_servers[0])
        remote.stop_server()
        self.sleep(20)
        for bucket in self.buckets:
            for query in self.query_definitions:
                try:
                    self.query_using_index(bucket=bucket,
                                           query_definition=query)
                except Exception as ex:
                    msg = "queryport.indexNotFound"
                    if msg in str(ex):
                        continue
                    else:
                        log.info(str(ex))
                        break
        remote.start_server()
        self.sleep(20)
        self._run_tasks([kvOps_tasks])

    def test_autofailover(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        autofailover_timeout = 30
        conn = RestConnection(self.master)
        status = conn.update_autofailover_settings(True, autofailover_timeout)
        self.assertTrue(status, 'failed to change autofailover_settings!')
        try:
            self._create_replica_indexes()
            servr_out = self.nodes_out_list
            remote = RemoteMachineShellConnection(servr_out[0])
            remote.stop_server()
            self.sleep(10)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            self.sleep(autofailover_timeout + 10, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], [], servr_out)
            rebalance.result()
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise
        finally:
            remote.start_server()
            self.sleep(30)

    def test_network_partitioning(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            self._create_replica_indexes()
            for node in self.nodes_out_list:
                self.start_firewall_on_node(node)
                self.sleep(60)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise
        finally:
            for node in self.nodes_out_list:
                self.stop_firewall_on_node(node)
                self.sleep(30)
            #check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self._check_all_bucket_items_indexed()

    def test_couchbase_bucket_compaction(self):
        """
        Run Compaction Here
        Run auto-compaction to remove the tomb stones
        """
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        compact_tasks = []
        for bucket in self.buckets:
            compact_tasks.append(self.cluster.async_compact_bucket(
                self.master, bucket))
        mid_recovery_tasks = self.async_run_operations(phase="in_between")
        self._run_tasks([kvOps_tasks, mid_recovery_tasks])
        for task in compact_tasks:
            task.result()
        self._check_all_bucket_items_indexed()
        post_recovery_tasks = self.async_run_operations(phase="after")
        self._run_tasks([post_recovery_tasks])

    def test_warmup(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        for server in self.nodes_out_list:
            remote = RemoteMachineShellConnection(server)
            remote.stop_server()
            remote.start_server()
            remote.disconnect()
        mid_recovery_tasks = self.async_run_operations(phase="in_between")
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        self._run_tasks([kvOps_tasks, mid_recovery_tasks])
        #check if the nodes in cluster are healthy
        msg = "Cluster not in Healthy state"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        log.info("==== Cluster in healthy state ====")
        self._check_all_bucket_items_indexed()
        post_recovery_tasks = self.async_run_operations(phase="after")
        self._run_tasks([post_recovery_tasks])

    def test_couchbase_bucket_flush(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        #Flush the bucket
        for bucket in self.buckets:
            log.info("Flushing bucket {0}...".format(bucket.name))
            rest = RestConnection(self.master)
            rest.flush_bucket(bucket.name)
            count = 0
            while rest.get_bucket_status(bucket.name) != "healthy" and \
                            count < 10:
                log.info("Bucket {0} Status is {1}. Sleeping...".format(
                    bucket.name, rest.get_bucket_status(bucket.name)))
                count += 1
                self.sleep(10)
            log.info("Bucket {0} is {1}".format(
                bucket.name, rest.get_bucket_status(bucket.name)))
        mid_recovery_tasks = self.async_run_operations(phase="in_between")
        self._run_tasks([kvOps_tasks, mid_recovery_tasks])
        #check if the nodes in cluster are healthy
        msg = "Cluster not in Healthy state"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        log.info("==== Cluster in healthy state ====")
        self.sleep(180)
        self._check_all_bucket_items_indexed()
        post_recovery_tasks = self.async_run_operations(phase="after")
        self.sleep(180)
        self._run_tasks([post_recovery_tasks])

    def test_partial_rollback(self):
        self.multi_create_index()
        self.sleep(30)
        self.log.info("Stopping persistence on NodeA & NodeB")
        data_nodes = self.get_nodes_from_services_map(service_type="kv",
                                                  get_all_nodes=True)
        for data_node in data_nodes:
            for bucket in self.buckets:
                mem_client = MemcachedClientHelper.direct_client(data_node, bucket.name)
                mem_client.stop_persistence()
        self.run_doc_ops()
        self.sleep(10)
        # Get count before rollback
        bucket_before_item_counts = {}
        for bucket in self.buckets:
            bucket_count_before_rollback = self.get_item_count(self.master, bucket.name)
            bucket_before_item_counts[bucket.name] = bucket_count_before_rollback
            log.info("Items in bucket {0} before rollback = {1}".format(
                bucket.name, bucket_count_before_rollback))

        # Index rollback count before rollback
        self._verify_bucket_count_with_index_count()
        self.multi_query_using_index()

        # Kill memcached on Node A so that Node B becomes master
        self.log.info("Kill Memcached process on NodeA")
        shell = RemoteMachineShellConnection(data_nodes[0])
        shell.kill_memcached()

        # Start persistence on Node B
        self.log.info("Starting persistence on NodeB")
        for bucket in self.buckets:
            mem_client = MemcachedClientHelper.direct_client(data_nodes[1], bucket.name)
            mem_client.start_persistence()

        # Failover Node B
        self.log.info("Failing over NodeB")
        self.sleep(10)
        failover_task = self.cluster.async_failover(
            self.servers[:self.nodes_init], [data_nodes[1]], self.graceful,
            wait_for_pending=120)

        failover_task.result()

        # Wait for a couple of mins to allow rollback to complete
        # self.sleep(120)

        bucket_after_item_counts = {}
        for bucket in self.buckets:
            bucket_count_after_rollback = self.get_item_count(self.master, bucket.name)
            bucket_after_item_counts[bucket.name] = bucket_count_after_rollback
            log.info("Items in bucket {0} after rollback = {1}".format(
                bucket.name, bucket_count_after_rollback))

        for bucket in self.buckets:
            if bucket_after_item_counts[bucket.name] == bucket_before_item_counts[bucket.name]:
                log.info("Looks like KV rollback did not happen at all.")
        self._verify_bucket_count_with_index_count()
        self.multi_query_using_index()

    def _create_replica_indexes(self):
        query_definitions = []
        if not self.use_replica:
            return []
        if not self.index_nodes_out:
            return []
        index_nodes = self.get_nodes_from_services_map(service_type="index",
                                                       get_all_nodes=True)
        for node in self.index_nodes_out:
            if node in index_nodes:
                index_nodes.remove(node)
        if index_nodes:
            ops_map = self.generate_operation_map("in_between")
            if ("create_index" not in ops_map):
                indexes_lost = self._find_index_lost_when_indexer_down()
                deploy_node_info = ["{0}:{1}".format(index_nodes[0].ip,
                                                     index_nodes[0].port)]
                for query_definition in self.query_definitions:
                    if query_definition.index_name in indexes_lost:
                        query_definition.index_name = query_definition.index_name + "_replica"
                        query_definitions.append(query_definition)
                        for bucket in self.buckets:
                            self.create_index(bucket=bucket,
                                              query_definition=query_definition,
                                              deploy_node_info=deploy_node_info)
                    else:
                        query_definitions.append(query_definition)
            self.query_definitions = query_definitions

    def _find_index_lost_when_indexer_down(self):
        lost_indexes = []
        rest = RestConnection(self.master)
        index_map = rest.get_index_status()
        log.info("index_map: {0}".format(index_map))
        for index_node in self.index_nodes_out:
            host = "{0}:8091".format(index_node.ip)
            for index in index_map.values():
                for keys, vals in index.items():
                    if vals["hosts"] == host:
                        lost_indexes.append(keys)
        log.info("Lost Indexes: {0}".format(lost_indexes))
        return lost_indexes

    def _run_kvops_tasks(self):
        tasks_ops =[]
        if self.doc_ops:
            tasks_ops = self.async_run_doc_ops()
        return tasks_ops

    def _run_tasks(self, tasks_list):
        for tasks in tasks_list:
            for task in tasks:
                task.result()
