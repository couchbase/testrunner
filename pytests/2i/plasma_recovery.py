import logging

from .base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.tuq_generators import TuqGenerators
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection


log = logging.getLogger(__name__)

class SecondaryIndexingPlasmaDGMRecoveryTests(BaseSecondaryIndexingTests):

    def setUp(self):
        self.use_replica = True
        super(SecondaryIndexingPlasmaDGMRecoveryTests, self).setUp()
        self.initial_index_number = self.input.param("initial_index_number", 10)
        self.load_query_definitions = []
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
        if hasattr(self, 'query_definitions'):
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
        super(SecondaryIndexingPlasmaDGMRecoveryTests, self).tearDown()

    def test_rebalance_in(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self._start_disk_writes_for_plasma()
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
        self._start_disk_writes_for_plasma()
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
        self._start_disk_writes_for_plasma()
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
            self._start_disk_writes_for_plasma()
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
        self._start_disk_writes_for_plasma()
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
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self._start_disk_writes_for_plasma()
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
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self._start_disk_writes_for_plasma()
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

    def test_server_restart(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self._start_disk_writes_for_plasma()
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
        self._start_disk_writes_for_plasma()
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
            self._start_disk_writes_for_plasma()
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
        self._calculate_scan_vector()
        rest = RestConnection(self.master)
        recoveryType = self.input.param("recoveryType", "full")
        indexer_out = int(self.input.param("nodes_out", 0))
        nodes = self.get_nodes_from_services_map(service_type="index",
                                                 get_all_nodes=True)
        self.assertGreaterEqual(len(nodes), indexer_out,
                                "Existing Indexer Nodes less than Indexer out nodes")
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        self._start_disk_writes_for_plasma()
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
        self._start_disk_writes_for_plasma()
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
        mid_recovery_tasks = []
        self._run_tasks([pre_recovery_tasks])
        self._start_disk_writes_for_plasma()
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
            try:
                mid_recovery_tasks = self.async_run_operations(phase="in_between")
            except Exception as ex:
                if " Indexer will retry building index at later time" not in str(ex):
                    log.info("build index failed with some unexpected error : {0}".format(str(ex)))
            else:
                log.info("Build index did not fail")
            self.sleep(autofailover_timeout + 10, "Wait for autofailover")
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], [], servr_out)
            rebalance.result()
            self.sleep(60)
            try:
                mid_recovery_tasks = self.async_run_operations(phase="in_between")
            except Exception as ex:
                if "is being built" not in str(ex):
                    log.info("build index failed with some unexpected error : {0}".format(str(ex)))
            else:
                log.info("Build index did not fail")
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
        self._start_disk_writes_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            self._create_replica_indexes()
            for node in self.nodes_out_list:
                self.start_firewall_on_node(node)
                self.sleep(20)
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
        self._start_disk_writes_for_plasma()
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
        self._start_disk_writes_for_plasma()
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
        self._start_disk_writes_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        #Flush the bucket
        try:
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
            self.sleep(180)
            post_recovery_tasks = self.async_run_operations(phase="after")
            self._run_tasks([post_recovery_tasks])
        except Exception as ex:
            log.info(str(ex))
            raise

    def test_multiple_recovery_with_dgm(self):
        pre_recovery_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_recovery_tasks])
        indexer_node = self.get_nodes_from_services_map(service_type="index")
        self._start_disk_writes_for_plasma([indexer_node])
        self._create_replica_indexes()
        remote = RemoteMachineShellConnection(indexer_node)
        for i in range(3):
            try:
                remote.stop_server()
                mid_recovery_tasks = self.async_run_operations(phase="in_between")
                self._run_tasks([mid_recovery_tasks])
                self._check_all_bucket_items_indexed()
            except Exception as ex:
                log.info(str(ex))
            finally:
                remote.start_server()
                self.sleep(20)
        post_recovery_tasks = self.async_run_operations(phase="after")
        self._run_tasks([post_recovery_tasks])

    def test_multiple_recovery_with_nondgm_indexer(self):
        dgm_node = self.get_nodes_from_services_map(service_type="index",
                                                        get_all_nodes=True)[0]
        self.deploy_node_info = ["{0}:{1}".format(dgm_node.ip, dgm_node.port)]
        self.multi_create_index(
            buckets=self.buckets, query_definitions=self.query_definitions,
            deploy_node_info=self.deploy_node_info)
        self._start_disk_writes_for_plasma(dgm_node)
        self._create_replica_indexes()
        indexer_node = self.get_nodes_from_services_map(service_type="index",
                                                        get_all_nodes=True)[1]
        remote = RemoteMachineShellConnection(indexer_node.ip)
        for i in range(3):
            try:
                remote.stop_server()
                mid_recovery_tasks = self.async_run_operations(phase="in_between")
                self._run_tasks([mid_recovery_tasks])
                self._check_all_bucket_items_indexed()
            except Exception as ex:
                log.info(str(ex))
            finally:
                remote.start_server()
        post_recovery_tasks = self.async_run_operations(phase="after")
        self._run_tasks([post_recovery_tasks])

    def _calculate_scan_vector(self):
        self.scan_vectors = None
        if self.scan_vectors != None:
            self.scan_vectors = self.gen_scan_vector(
                use_percentage=self.scan_vector_per_values,
             use_random = self.random_scan_vector)

    def _start_disk_writes_for_plasma(self, indexer_nodes=None):
        """
        Internal Method to create OOM scenario
        :return:
        """
        def validate_disk_writes(indexer_nodes=None):
            if not indexer_nodes:
                indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                    get_all_nodes=True)
            for node in indexer_nodes:
                indexer_rest = RestConnection(node)
                content = indexer_rest.get_index_storage_stats()
                for index in list(content.values()):
                    for stats in list(index.values()):
                        if stats["MainStore"]["resident_ratio"] < 1.00:
                            return True
            return False

        def kv_mutations(self, docs=1):
            if not docs:
                docs = self.docs_per_day
            gens_load = self.generate_docs(docs)
            self.full_docs_list = self.generate_full_docs_list(gens_load)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.load(gens_load, buckets=self.buckets, flag=self.item_flag,
                  verify_data=False, batch_size=self.batch_size)
        if self.gsi_type != "plasma":
            return
        if not self.plasma_dgm:
            return
        log.info("Trying to get atleast one index in DGM...")
        log.info("Setting indexer memory quota to 256 MB...")
        node = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(node)
        rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=256)
        cnt = 0
        docs = 50 + self.docs_per_day
        while cnt < 100:
            if validate_disk_writes(indexer_nodes):
                log.info("========== DGM is achieved on atleast one index ==========")
                return True
            kv_mutations(self, docs)
            self.sleep(30)
            cnt += 1
            docs += 20
        return False

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
