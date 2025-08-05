import copy
import logging
import threading
from threading import Thread
import time

from .base_gsi import BaseSecondaryIndexingTests, ConCurIndexOps
from couchbase_helper.query_definitions import QueryDefinition
from lib.memcached.helper.data_helper import MemcachedClientHelper
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from collection.collections_rest_client import CollectionsRest
from tasks.taskmanager import TaskManager

log = logging.getLogger(__name__)

class CollectionsSecondaryIndexingRecoveryTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CollectionsSecondaryIndexingRecoveryTests, self).setUp()
        self.log.info("==============  PlasmaCollectionsTests setup has started ==============")
        self.rest.delete_all_buckets()
        self.num_scopes = self.input.param("num_scopes", 2)
        self.num_collections = self.input.param("num_collections", 2)
        self.num_pre_indexes = self.input.param("num_pre_indexes", 4)
        self.oso_indexes = self.input.param("oso_indexes", 200)
        self.test_timeout = self.input.param("test_timeout", 60)
        self.test_bucket = self.input.param('test_bucket', 'test_bucket')
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.cli_rest = CollectionsRest(self.master)
        self.scope_prefix = 'test_scope'
        self.collection_prefix = 'test_collection'
        self.run_cbq_query = self.n1ql_helper.run_cbq_query
        self.batch_size = self.input.param("batch_size", 50000)
        self.batch_size = self.input.param("batch_size", 100000)
        self.start_doc = self.input.param("start_doc", 1)
        self.enable_oso = self.input.param("enable_oso", False)
        self.num_items_in_collection = self.input.param("num_items_in_collection", 10000)
        self.percent_create = self.input.param("percent_create", 100)
        self.percent_update = self.input.param("percent_update", 0)
        self.percent_delete = self.input.param("percent_delete", 0)
        self.all_collections = self.input.param("all_collections", False)
        self.dataset_template = self.input.param("dataset_template", "Employee")
        self.moi_snapshot_interval = self.input.param("moi_snapshot_interval", 600000)
        self.index_ops_obj = ConCurIndexOps()

        self.log.info("Setting indexer memory quota to 256 MB and other settings...")
        self.index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=1000)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": self.moi_snapshot_interval})
        self.split_num_index_nodes = len(self.index_nodes)//2
        self.recovering_nodes = self.index_nodes[:self.split_num_index_nodes]
        self.verify_nodes = self.index_nodes[self.split_num_index_nodes:]

        self.disk_location = RestConnection(self.index_nodes[0]).get_index_path()
        self.index_create_task_manager = TaskManager(
            "index_create_task_manager")
        self.index_create_task_manager.start()
        self.sdk_loader_manager = TaskManager(
            "sdk_loader_manager")
        self.sdk_loader_manager.start()

        self.keyspace = []
        self.create_scope_collections(num_scopes=self.num_scopes, num_collections=self.num_collections)
        self.run_tasks = True
        self.data_nodes = self.get_kv_nodes()
        self.sleep_time = self.input.param("sleep_time", 1)
        self.dgm_check_timeout = self.input.param("dgm_check_timeout", 1800)

        self.log.info("==============  PlasmaCollectionsTests setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  PlasmaCollectionsTests tearDown has started ==============")
        super(CollectionsSecondaryIndexingRecoveryTests, self).tearDown()
        self.log.info("==============  PlasmaCollectionsTests tearDown has completed ==============")

    def test_recovery_disk_snapshot(self):

        if self.enable_oso:
            for index_node in self.index_nodes:
                rest = RestConnection(index_node)
                rest.set_index_settings({"indexer.build.enableOSO": True})

        index_create_tasks = self.create_indexes(num=self.num_pre_indexes)
        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 3000})

        self._kill_all_processes_index(self.index_nodes[0])

        self.load_docs(self.start_doc)

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("some indexes did not process mutations on time")

        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 1200000})
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for node in index_nodes:
            self._kill_all_processes_index(node)
        self.sleep(30)
        self.load_docs(self.num_items_in_collection)
        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("some indexes did not process mutations on time")
        index_list = self.index_ops_obj.get_create_index_list()
        self._kill_all_processes_index(self.index_nodes[0])
        if not self.check_gsi_logs_for_snapshot_recovery():
            self.fail("Some indexes did not recover from snapshot")
        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("some indexes did not process mutations on time")
        self.verify_query_results_from_new_index()

    def verify_query_results_from_new_index(self):
        index_list = self.index_ops_obj.get_create_index_list()
        for index_meta in index_list:
            query_def = index_meta["query_def"]
            query_def_verify = copy.deepcopy(query_def)
            index_name = query_def_verify.get_index_name()
            index_name = "verifying" + index_name
            query_def_verify.update_index_name(index_name)
            if "primary" in query_def_verify.groups:
                query = query_def_verify.generate_primary_index_create_query()
            else:
                query = query_def_verify.generate_index_create_query(use_gsi_for_secondary=True, gsi_type="plasma")
            try:
                # create index
                self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_nodes[0])
            except Exception as err:
                self.log.error(f'{err} occured while creating verifying index {query_def_verify.get_index_name()}')

            expected_result_query = query_def.generate_use_index_query(index_name)
            expected_result = self.n1ql_helper.run_cbq_query(query=expected_result_query,
                                                             server=self.n1ql_nodes[0], scan_consistency="request_plus")
            actual_query = query_def.generate_use_index_query(index_meta["name"])
            actual_result = self.n1ql_helper.run_cbq_query(query=actual_query,
                                                           server=self.n1ql_nodes[0], scan_consistency="request_plus")
            self.n1ql_helper._verify_results(actual_result['results'], expected_result['results'])


    def test_recovery_no_disk_snapshot(self):
        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 1200000})
        self._kill_all_processes_index(self.index_nodes[0])
        index_create_tasks = self.create_indexes(num=self.num_pre_indexes)
        for task in index_create_tasks:
            task.result()
        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))
        self.load_docs(self.start_doc)
        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("some indexes did not process mutations on time")
        self.sleep(10)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for node in index_nodes:
            self._kill_all_processes_index(node)
        self.sleep(30)
        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("Some indexes did not process mutations on time")
        self.verify_query_results_from_new_index()

    def test_recover_index_from_in_memory_snapshot(self):

        index_create_tasks = self.create_indexes(num=self.num_pre_indexes)
        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        self.load_docs(self.start_doc)

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("some indexes did not process mutations on time")

        bucket_name = self.buckets[0].name
        # Blocking node B firewall
        data_nodes = self.get_kv_nodes()
        self.assertTrue(len(data_nodes) >= 3, "Can't run this with less than 3 KV nodes")
        node_b, node_c = (None, None)
        for node in data_nodes:
            if node.ip == self.master.ip:
                continue
            if not node_b:
                node_b = node
            else:
                node_c = node
                break
        # get num_rollback stats before triggering in-memory recovery
        conn = RestConnection(self.master)
        num_rollback_before_recovery = conn.get_num_rollback_stat(bucket=bucket_name)
        try:
            self.block_incoming_network_from_node(node_b, node_c)

            # killing Memcached on Node B
            remote_client = RemoteMachineShellConnection(node_b)
            remote_client.kill_memcached()
            remote_client.disconnect()

            # Failing over Node B
            self.cluster.failover(servers=self.servers, failover_nodes=[node_b])
        finally:
            # resume the communication between node B and node C
            self.resume_blocked_incoming_network_from_node(node_b, node_c)
        # get num_rollback stats after in-memory recovery of indexes
        num_rollback_after_recovery = conn.get_num_rollback_stat(bucket=bucket_name)
        self.assertEqual(num_rollback_before_recovery, num_rollback_after_recovery,
                         "Recovery didn't happen from in-memory snapshot")
        self.log.info("Node has recovered from in-memory snapshots")
        # Loading few more docs so that indexer will index updated as well as new docs
        self.load_docs(self.num_items_in_collection)

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("some indexes did not process mutations on time")

        indexes_not_recovered = self.check_if_index_recovered(self.index_ops_obj.get_create_index_list())
        if indexes_not_recovered:
            self.index_ops_obj.update_errors(f'Some Indexes not recovered {indexes_not_recovered}')
            self.log.info(f'Some Indexes not recovered {indexes_not_recovered}')

        self.verify_query_results_from_new_index()

    def test_restart_timestamp_calculation_for_rollback(self):
        data_nodes = self.get_kv_nodes()
        self.assertTrue(len(data_nodes) >= 3, "Can't run this with less than 3 KV nodes")
        index_create_tasks = self.create_indexes(num=self.num_pre_indexes)
        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        self.load_docs(self.start_doc)

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("some indexes did not process mutations on time")

        indexes_not_recovered = self.check_if_index_recovered(self.index_ops_obj.get_create_index_list())
        if indexes_not_recovered:
            self.index_ops_obj.update_errors(f'Some Indexes not recovered {indexes_not_recovered}')
            self.log.info(f'Some Indexes not recovered {indexes_not_recovered}')

        data_nodes = self.get_kv_nodes()
        node_b, node_c = (None, None)
        for node in data_nodes:
            if node.ip == self.master.ip:
                continue
            if not node_b:
                node_b = node
            else:
                node_c = node
                break

        try:
            # Blocking communication between Node B and Node C
            self.block_incoming_network_from_node(node_b, node_c)

            # Mutating docs so that replica on Node C don't see changes on Node B
            self.load_docs(self.num_items_in_collection)

            # killing Memcached on Node B
            remote_client = RemoteMachineShellConnection(node_b)
            remote_client.kill_memcached()
            remote_client.disconnect()

            # Failing over Node B
            self.cluster.failover(servers=self.servers, failover_nodes=[node_b])
            self.sleep(timeout=10, message="Allowing indexer to rollback")

            # Validating that indexer has indexed item after rollback and catch up with items in bucket
            indexes_not_recovered = self.check_if_index_recovered(self.index_ops_obj.get_create_index_list())
            if indexes_not_recovered:
                self.index_ops_obj.update_errors(f'Some Indexes not recovered {indexes_not_recovered}')
                self.log.info(f'Some Indexes not recovered {indexes_not_recovered}')
        finally:
            self.resume_blocked_incoming_network_from_node(node_b, node_c)

        self.verify_query_results_from_new_index()

    def test_recovery_projector_crash(self):
        index_create_tasks = self.create_indexes(num=self.num_pre_indexes)
        for task in index_create_tasks:
            task.result()
        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))
        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 3000})
        self.sleep(60)
        self._kill_all_processes_index(self.index_nodes[0])
        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 1200000})
        self.load_docs(self.start_doc)
        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("some indexes did not process mutations on time")
        self._kill_all_processes_index(self.index_nodes[0])
        self.sleep(10)
        load_tasks = self.async_load_docs(self.num_items_in_collection, self.num_items_in_collection * 2)
        data_nodes = self.get_kv_nodes()
        for node in data_nodes:
            remote = RemoteMachineShellConnection(node)
            remote.terminate_process(process_name="projector")
        self.sleep(30)
        for task in load_tasks:
            task.result()
        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("some indexes did not process mutations on time")
        self.verify_query_results_from_new_index()

    def test_recovery_memcached_crash(self):

        index_create_tasks = self.create_indexes(num=self.num_pre_indexes)
        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 3000})

        self._kill_all_processes_index(self.index_nodes[0])

        self.load_docs(self.start_doc)

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("some indexes did not process mutations on time")

        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 1200000})

        self._kill_all_processes_index(self.index_nodes[0])

        self.sleep(10)
        data_nodes = self.get_kv_nodes()
        self.block_incoming_network_from_node(self.index_nodes[0], data_nodes[1])
        self.load_docs(self.num_items_in_collection)
        self.resume_blocked_incoming_network_from_node(self.index_nodes[0], data_nodes[1])

        remote = RemoteMachineShellConnection(data_nodes[1])
        remote.kill_memcached()
        self.sleep(1)

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.log.info("some indexes did not process mutations on time")

        self.verify_query_results_from_new_index()

    # OSO Cases

    def test_basic_oso_feature(self):

        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.build.enableOSO": False})

        self.load_docs(self.start_doc)

        index_create_tasks = self.create_indexes(num=self.num_pre_indexes, query_def_group="unique")
        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.build.enableOSO": True})

        self.create_scope_collections(num_scopes=2, num_collections=3, scope_prefix="oso_scope",
                                              collection_prefix="oso_collection")

        self.load_docs(self.num_items_in_collection)

        # there are 8 unique indexes to create so total indexes to num_collections * 8
        index_create_tasks = self.create_indexes(num=self.oso_indexes, index_name_prefix="oso_index",
                                                 query_def_group="unique")
        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.fail("some indexes did not process mutations on time")

        self.scan_indexes(self.index_ops_obj.get_create_index_list())

    def scan_indexes(self, query_def_list):
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        for index_to_scan in query_def_list:
            self.log.info(f'Processing index: {index_to_scan["name"]}')
            query_def = index_to_scan["query_def"]
            query = query_def.generate_query(bucket=query_def.keyspace)
            try:
                self.run_cbq_query(query=query, server=n1ql_node, scan_consistency="request_plus")
            except Exception as err:
                self.log.error(f'{query} failed with {err}')
                self.log.info("Retrying the query")
                try:
                    self.run_cbq_query(query=query, server=n1ql_node, scan_consistency="request_plus")
                except Exception as err:
                    self.fail(f'{query} failed with {err}')

    def test_basic_oso_feature_mutations(self):

        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.build.enableOSO": True})

        self.create_scope_collections(num_scopes=2, num_collections=5, scope_prefix="oso_scope",
                                              collection_prefix="oso_collection")

        self.load_docs(self.start_doc)

        index_create_tasks = self.create_indexes(num=112, query_def_group="unique")
        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        self.load_docs(self.num_items_in_collection)

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.fail("some indexes did not process mutations on time")

        self.scan_indexes(self.index_ops_obj.get_create_index_list())

    def test_basic_oso_feature_mutations_in_parallel(self):

        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.build.enableOSO": True})

        self.create_scope_collections(num_scopes=2, num_collections=5, scope_prefix="oso_scope",
                                              collection_prefix="oso_collection")

        self.load_docs(self.start_doc)

        index_create_tasks = self.create_indexes(num=self.num_pre_indexes, query_def_group="unique")
        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        load_tasks = self.async_load_docs(self.num_items_in_collection)

        index_create_tasks = self.create_indexes(num=112, index_name_prefix="oso_index",
                                                 query_def_group="unique")
        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        for task in load_tasks:
            task.result()

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.fail("some indexes did not process mutations on time")

        self.scan_indexes(self.index_ops_obj.get_create_index_list())

    def test_oso_drop_collection(self):

        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.build.enableOSO": True})

        self.create_scope_collections(num_scopes=1, num_collections=2, scope_prefix="oso_scope",
                                              collection_prefix="oso_collection")

        self.load_docs(self.start_doc)
        #50
        index_create_tasks = self.create_indexes(num=50, query_def_group="unique")
        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.fail("some indexes did not process mutations on time")

        status = self.cli_rest.delete_collection(bucket=self.test_bucket, scope='oso_scope_1', collection='oso_collection_1')

        if not status:
            self.fail("Drop collection failed")

        self.update_keyspace_list(bucket=self.test_bucket)
        #40
        index_create_tasks = self.create_indexes(num=40, index_name_prefix="oso_index_after_drop",
                                                 query_def_group="unique")

        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        self.create_scope_collections(num_scopes=1, num_collections=1, scope_prefix="oso_scope",
                                              collection_prefix="oso_collection")
        #50
        index_create_tasks = self.create_indexes(num=50, index_name_prefix="oso_index_after_recreate_collection",
                                                 query_def_group="unique")
        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.fail("some indexes did not process mutations on time")

        self.scan_indexes(self.index_ops_obj.get_create_index_list())

    def test_server_crash_while_building(self):

        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 1000})

        if self.enable_oso:
            for index_node in self.index_nodes:
                rest = RestConnection(index_node)
                rest.set_index_settings({"indexer.build.enableOSO": True})

        self.create_scope_collections(num_scopes=1, num_collections=2, scope_prefix="oso_scope",
                                              collection_prefix="oso_collection")

        self.load_docs(self.start_doc)
        self.sleep(5)

        index_create_tasks = self.create_indexes(num=50, query_def_group="unique", defer_build=True)

        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        self.build_indexes()

        self.sleep(2)

        if self.targetProcess == "memcached":
            remote = RemoteMachineShellConnection(self.data_nodes[1])
            remote.kill_memcached()
        elif self.targetProcess == "projector":
            remote = RemoteMachineShellConnection(self.data_nodes[1])
            remote.terminate_process(process_name=self.targetProcess)
        else:
            remote = RemoteMachineShellConnection(self.index_nodes[0])
            remote.terminate_process(process_name=self.targetProcess)

        self.sleep(10)
        if not self.wait_for_mutation_processing(self.index_nodes):
            self.fail("some indexes did not process mutations on time")

        self.sleep(10)
        self.scan_indexes(self.index_ops_obj.get_defer_index_list())
        self.verify_query_results_from_new_index()

    def test_rebalance_in_kv_node_while_building(self):

        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 1000})

        if self.enable_oso:
            for index_node in self.index_nodes:
                rest = RestConnection(index_node)
                rest.set_index_settings({"indexer.build.enableOSO": True})

        self.create_scope_collections(num_scopes=1, num_collections=2, scope_prefix="oso_scope",
                                              collection_prefix="oso_collection")

        self.load_docs(self.start_doc)
        #50
        index_create_tasks = self.create_indexes(num=50, query_def_group="unique", defer_build=True)

        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        self.build_indexes()

        self.sleep(2)

        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            [self.servers[self.nodes_init]],
            [], services=["kv"])
        rebalance.result()

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.fail("some indexes did not process mutations on time")

        self.scan_indexes(self.index_ops_obj.get_create_index_list())

    def test_multiple_rebalances(self):
        self.key_prefix = f'bigkeybigkeybigkeybigkeybigkeybigkey_{self.master.ip}_'
        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 1000})
        #self.load_docs(self.start_doc)
        #50
        index_create_tasks = self.create_indexes(num=250, query_def_group="unique", defer_build=True)
        for task in index_create_tasks:
            task.result()
        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))
        self.build_indexes()
        self.sleep(2)
        load_doc_thread = threading.Thread(name="load_doc_thread",
                                           target=self.load_docs,
                                           args=[self.start_doc],
                                           kwargs={'key_prefix': self.key_prefix})
        load_doc_thread.start()
        self.sleep(60, "sleeping for 60 sec for index to start processing docs")
        if not self.check_if_indexes_in_dgm():
            self.log.error("indexes not in dgm even after {}".format(self.dgm_check_timeout))
        self.kill_loader_process()
        self.sdk_loader_manager.shutdown(True)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not self.wait_for_mutation_processing(index_nodes=index_nodes):
            self.fail("some indexes did not process mutations on time")
        self.scan_indexes(self.index_ops_obj.get_create_index_list())
        #rebalance in
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            [self.servers[self.nodes_init]],
            [], services=["index"])
        rebalance.result()
        self.sleep(30)
        index_node = self.get_nodes_from_services_map(service_type="index")
        self.index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not self.wait_for_mutation_processing(self.index_nodes):
            self.fail("some indexes did not process mutations on time")
        self.scan_indexes(self.index_ops_obj.get_defer_index_list())
        indexes_not_created = self.index_ops_obj.check_if_indexes_created(index_node, defer_build=True)
        if indexes_not_created:
            self.index_ops_obj.update_errors(f'Expected Created indexes {indexes_not_created} found to be not created')
            self.fail(f'Expected Created indexes {indexes_not_created} found to be not created')
        #rebalance out
        kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        node_out = kv_nodes[1]
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init+1],
            [], [node_out])
        rebalance.result()
        self.sleep(30)
        index_node = self.get_nodes_from_services_map(service_type="index")
        self.index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not self.wait_for_mutation_processing(self.index_nodes):
            self.fail("some indexes did not process mutations on time")
        self.scan_indexes(self.index_ops_obj.get_defer_index_list())
        indexes_not_created = self.index_ops_obj.check_if_indexes_created(index_node, defer_build=True)
        if indexes_not_created:
            self.index_ops_obj.update_errors(f'Expected Created indexes {indexes_not_created} found to be not created')
            self.fail(f'Expected Created indexes {indexes_not_created} found to be not created')
        swap_node_out = self.get_nodes_from_services_map(service_type="index")
        #swap rebalance
        rebalance = self.cluster.async_rebalance(
            self.servers[:self.nodes_init],
            [node_out],
            [swap_node_out], services=["kv"])
        rebalance.result()
        self.sleep(30)
        index_node = self.get_nodes_from_services_map(service_type="index")
        self.index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not self.wait_for_mutation_processing(self.index_nodes):
            self.fail("some indexes did not process mutations on time")
        self.scan_indexes(self.index_ops_obj.get_defer_index_list())
        indexes_not_created = self.index_ops_obj.check_if_indexes_created(index_node, defer_build=True)
        if indexes_not_created:
            self.index_ops_obj.update_errors(f'Expected Created indexes {indexes_not_created} found to be not created')
            self.fail(f'Expected Created indexes {indexes_not_created} found to be not created')

    def test_rebalance_in_indexer_node_while_building(self):

        self.retry_time = self.input.param("retry_time", 10)
        self.num_retries = self.input.param("num_retries", 3)
        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 1000})

        body = {"enabled": "true", "afterTimePeriod": self.retry_time , "maxAttempts" : self.num_retries}
        rest = RestConnection(self.master)
        rest.set_retry_rebalance_settings(body)

        if self.enable_oso:
            for index_node in self.index_nodes:
                rest = RestConnection(index_node)
                rest.set_index_settings({"indexer.build.enableOSO": True})

        self.create_scope_collections(num_scopes=1, num_collections=1, scope_prefix="oso_scope",
                                              collection_prefix="oso_collection")

        self.load_docs(self.start_doc)
        #50
        index_create_tasks = self.create_indexes(num=50, query_def_group="unique", defer_build=True)

        for task in index_create_tasks:
            task.result()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

        self.build_indexes()
        try:
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [self.servers[self.nodes_init]],
                [], services=["index"])

            self.sleep(3)
            self._kill_all_processes_index(self.index_nodes[0])
            rebalance.result()
        except Exception as ex:
            if "Rebalance failed" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        self.check_retry_rebalance_succeeded()

        if not self.wait_for_mutation_processing(self.index_nodes):
            self.fail("some indexes did not process mutations on time")

        self.scan_indexes(self.index_ops_obj.get_create_index_list())

    def build_indexes(self):
        for ks in self.keyspace:
            query = f"build index on {ks} (( select raw name from system:all_indexes where `namespace_id` || ':' || `bucket_id` || '.' || `scope_id` || '.' || `keyspace_id` = '{ks}' and state = 'deferred'))"

            try:
                self.run_cbq_query(query=query, server=self.n1ql_nodes[0])
            except Exception as err:
                self.fail(f'{query} failed with {err}')

class SecondaryIndexingRecoveryTests(BaseSecondaryIndexingTests):

    def setUp(self):
        self.use_replica = True
        super(SecondaryIndexingRecoveryTests, self).setUp()
        self.load_query_definitions = []
        self.initial_index_number = self.input.param("initial_index_number", 10)
        for x in range(self.initial_index_number):
            index_name = "index_name_" + str(x)
            query_definition = QueryDefinition(index_name=index_name, index_fields=["VMs"],
                                               query_template="SELECT * FROM %s ", groups=["simple"],
                                               index_where_clause=" VMs IS NOT NULL ")
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
        doc = {"indexer.settings.persisted_snapshot.moi.interval": 60000}
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
            # check if the nodes in cluster are healthy
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
            # self._create_replica_indexes()
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init],
                [], self.nodes_out_list)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            rebalance.result()
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            # check if the nodes in cluster are healthy
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
            # self._create_replica_indexes()
            rebalance = self.cluster.async_rebalance(
                self.servers[:self.nodes_init], self.nodes_in_list,
                self.nodes_out_list, services=self.services_in)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            rebalance.result()
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            # check if the nodes in cluster are healthy
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
            # check if the nodes in cluster are healthy
            msg = "Cluster not in Healthy state"
            self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
            log.info("==== Cluster in healthy state ====")
            self.nodes_out_dist = "index:1"
            self.services_in = ["index"]
            self.nodes_in_list = [extra_nodes[1]]
            self.generate_map_nodes_out_dist()
            # self._create_replica_indexes()
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                     self.nodes_in_list,
                                                     self.nodes_out_list, services=self.services_in)
            rebalance.result()
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            # check if the nodes in cluster are healthy
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
            # self._create_replica_indexes()
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
            # check if the nodes in cluster are healthy
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
        self.get_dgm_for_plasma()
        kvOps_tasks = self._run_kvops_tasks()
        try:
            self.use_replica = False
            self._create_replica_indexes()
            self.targetProcess = self.input.param("targetProcess", 'memcached')
            for node in self.nodes_out_list:
                remote = RemoteMachineShellConnection(node)
                if self.targetProcess == "memcached":
                    remote.kill_memcached()
                else:
                    remote.terminate_process(process_name=self.targetProcess)
            self.sleep(60)
            mid_recovery_tasks = self.async_run_operations(phase="in_between")
            self._run_tasks([kvOps_tasks, mid_recovery_tasks])
            # check if the nodes in cluster are healthy
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
            # check if the nodes in cluster are healthy
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
            # check if the nodes in cluster are healthy
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
            failover_task = self.cluster.async_failover([self.master],
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
            # check if the nodes in cluster are healthy
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
            failover_task = self.cluster.async_failover(
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
            # check if the nodes in cluster are healthy
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
            # check if the nodes in cluster are healthy
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
            # check if the nodes in cluster are healthy
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
        # check if the nodes in cluster are healthy
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
        # Flush the bucket
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
        # check if the nodes in cluster are healthy
        msg = "Cluster not in Healthy state"
        self.assertTrue(self.wait_until_cluster_is_healthy(), msg)
        log.info("==== Cluster in healthy state ====")
        self.sleep(180)
        self._check_all_bucket_items_indexed()
        post_recovery_tasks = self.async_run_operations(phase="after")
        self.sleep(180)
        self._run_tasks([post_recovery_tasks])

    def test_robust_rollback_handling_in_failure_scenario(self):
        """
        MB-36582
        TODO:
        "https://issues.couchbase.com/browse/MB-37586
        https://issues.couchbase.com/browse/MB-37588
        Will wait on the stats to be available
        https://issues.couchbase.com/browse/MB-37594
        """
        data_nodes = self.get_kv_nodes()
        self.assertTrue(len(data_nodes) >= 3, "Can't run this with less than 3 KV nodes")
        bucket_name = self.buckets[0].name
        index_name = list(self.get_index_map()[bucket_name].keys())[0]
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                      get_all_nodes=False)
        rest = RestConnection(index_node)
        # Change indexer snapshot for a recovery point
        doc = {"indexer.settings.persisted_snapshot.moi.interval": 60000}
        rest.set_index_settings(doc)

        # Deleting bucket as there is no easy way in testrunner to crate index before loading data
        for bucket in self.buckets:
            self.cluster.bucket_delete(self.master, bucket=bucket)

        # Create default bucket
        default_params = self._create_bucket_params(
            server=self.master, size=self.bucket_size,
            replicas=self.num_replicas, bucket_type=self.bucket_type,
            enable_replica_index=self.enable_replica_index,
            eviction_policy=self.eviction_policy, lww=self.lww,
            maxttl=self.maxttl, compression_mode=self.compression_mode)
        self.cluster.create_default_bucket(default_params)

        # loading data to bucket
        gens_load = self.generate_docs(num_items=self.docs_per_day)
        self.load(gens_load, flag=self.item_flag, batch_size=self.batch_size, op_type="create", verify_data=False)

        # creating Index
        query_definition = QueryDefinition(index_name=index_name, index_fields=["VMs"],
                                           query_template="SELECT * FROM %s ", groups=["simple"],
                                           index_where_clause=" VMs IS NOT NULL ")
        self.load_query_definitions.append(query_definition)
        self.create_index(bucket="default", query_definition=query_definition)

        node_b, node_c = (None, None)
        for node in data_nodes:
            if node.ip == self.master.ip:
                continue
            if not node_b:
                node_b = node
            else:
                node_c = node
                break
        # Blocking Node C from Node B
        try:
            self.block_incoming_network_from_node(node_b, node_c)

            # Killing Memcached on Node C so that disk snapshots have vbuuid not available with Node B
            for _ in range(2):
                # Killing memcached on node C
                num_snapshot = rest.get_index_stats()[bucket_name][index_name]["num_commits"]
                remote_client = RemoteMachineShellConnection(node_c)
                remote_client.kill_memcached()
                remote_client.disconnect()

                sleep_count = 0
                while sleep_count < 10:
                    self.sleep(10, "Waiting for Disk Snapshot/s to be available")
                    new_num_snapshot = rest.get_index_stats()[bucket_name][index_name]["num_commits"]
                    if new_num_snapshot > num_snapshot:
                        self.log.info("New Disk Snapshot is available")
                        break
                    sleep_count += 1

            # Restarting Indexer to clear in-memory snapshots
            remote_client = RemoteMachineShellConnection(index_node)
            remote_client.execute_command("kill -9 $(ps aux | pgrep 'indexer')")
            self.sleep(timeout=10, message="Allowing time for indexer to restart")

            # Fail over Node C so that replica takes over on Node B
            self.cluster.failover(servers=self.servers, failover_nodes=[node_c])
            self.sleep(timeout=30, message="Waiting for rollback to kick in")

            # Get rollback count
            num_rollback = rest.get_num_rollback_stat(bucket="default")
            self.assertEqual(num_rollback, 1, "Failed to rollback in failure scenario")
            # Todo: add validation that the rollback has happened from snapshot not from Zero
        finally:
            self.resume_blocked_incoming_network_from_node(node_b, node_c)

    def test_discard_disk_snapshot_after_kv_persisted(self):
        """
        MB-36554
        Todo: https://issues.couchbase.com/browse/MB-37586
        Will wait on the stats to be available
        https://issues.couchbase.com/browse/MB-37587
        """
        data_nodes = self.get_kv_nodes()
        self.assertTrue(len(data_nodes) == 2, "This test require a cluster of 2 nodes")
        bucket_name = self.buckets[0].name
        index_name = list(self.get_index_map()[bucket_name])[0]
        index_node = self.get_nodes_from_services_map(service_type="index",
                                                      get_all_nodes=False)
        rest = RestConnection(index_node)
        # Change indexer snapshot for a recovery point
        doc = {"indexer.settings.persisted_snapshot.moi.interval": 60000}
        rest.set_index_settings(doc)

        # Deleting bucket as there is no easy way in testrunner to crate index before loading data
        for bucket in self.buckets:
            self.cluster.bucket_delete(self.master, bucket=bucket)

        # Create default bucket
        default_params = self._create_bucket_params(
            server=self.master, size=self.bucket_size,
            replicas=self.num_replicas, bucket_type=self.bucket_type,
            enable_replica_index=self.enable_replica_index,
            eviction_policy=self.eviction_policy, lww=self.lww,
            maxttl=self.maxttl, compression_mode=self.compression_mode)
        self.cluster.create_default_bucket(default_params)

        # loading data to bucket
        gens_load = self.generate_docs(num_items=self.docs_per_day)
        self.load(gens_load, flag=self.item_flag, batch_size=self.batch_size, op_type="create", verify_data=False)

        # creating Index
        query_definition = QueryDefinition(index_name=index_name, index_fields=["VMs"],
                                           query_template="SELECT * FROM %s ", groups=["simple"],
                                           index_where_clause=" VMs IS NOT NULL ")
        self.load_query_definitions.append(query_definition)
        self.create_index(bucket="default", query_definition=query_definition)

        # Blocking node B firewall
        node_b, node_c = data_nodes
        try:
            self.block_incoming_network_from_node(node_b, node_c)

            # Performing doc mutation
            num_snapshot = rest.get_index_stats()[bucket_name][index_name]["num_commits"]
            gens_load = self.generate_docs(self.docs_per_day * 2)
            self.load(gens_load, flag=self.item_flag, verify_data=False, batch_size=self.batch_size)

            sleep_count = 0
            while sleep_count < 10:
                self.sleep(10, "Waiting for Disk Snapshot/s to be available")
                new_num_snapshot = rest.get_index_stats()[bucket_name][index_name]["num_commits"]
                if new_num_snapshot > num_snapshot:
                    self.log.info("New Disk Snapshot is available")
                    break
                sleep_count += 1

            # Performing doc mutation
            num_snapshot = rest.get_index_stats()[bucket_name][index_name]["num_commits"]
            gens_load = self.generate_docs(self.docs_per_day * 3)
            self.load(gens_load, flag=self.item_flag, verify_data=False, batch_size=self.batch_size)

            sleep_count = 0
            while sleep_count < 10:
                self.sleep(10, "Waiting for Disk Snapshot/s to be available")
                new_num_snapshot = rest.get_index_stats()[bucket_name][index_name]["num_commits"]
                if new_num_snapshot > num_snapshot:
                    self.log.info("New Disk Snapshot is available")
                    break
                sleep_count += 1
            # resume the communication between node B and node C
        finally:
            self.resume_blocked_incoming_network_from_node(node_b, node_c)

        # TODO: Need to add validation based on stat that the Disk Snapshot has catch up and extra snapshots are deleted
        # Meanwhile we will validate based on the item_count
        self.sleep(timeout=2 * 60, message="Giving some time to indexer to recover after resuming communication "
                                           "between node A and node B")
        item_count_after_checking_kv_persisted_seq_num = rest.get_index_stats()[bucket_name][index_name]["items_count"]
        self.assertEqual(item_count_after_checking_kv_persisted_seq_num, self.docs_per_day * 3 * 2016,
                         "Indexer failed to index all the items in bucket.\nExpected indexed item {}"
                         "\n Actual indexed item {}".format(item_count_after_checking_kv_persisted_seq_num,
                                                            self.docs_per_day * 3 * 2016))

    def test_rollback_to_zero_preceded_by_rollback_from_disk_snapshot(self):
        """
        MB-36444
        """
        bucket_name = self.buckets[0].name
        index_name = list(self.get_index_map()[bucket_name])[0]
        data_nodes = self.get_kv_nodes()
        self.assertTrue(len(data_nodes) >= 3, "Can't run this with less than 3 KV nodes")
        # Blocking node B firewall
        node_b, node_c = (None, None)
        for node in data_nodes:
            if node.ip == self.master.ip:
                continue
            if not node_b:
                node_b = node
            else:
                node_c = node
                break
        try:
            # Blocking communication between Node B and Node C
            index_node = self.get_nodes_from_services_map(service_type="index")
            conn = RestConnection(index_node)
            self.block_incoming_network_from_node(node_b, node_c)

            # Doing some mutation which replica on Node C won't see
            gens_load = self.generate_docs(num_items=self.docs_per_day * 2)
            self.load(gens_load, flag=self.item_flag, batch_size=self.batch_size, op_type="create", verify_data=False)

            # Failing over Node C
            self.cluster.failover(servers=self.servers, failover_nodes=[node_c])

            sleep_count = 0
            while sleep_count < 15:
                num_rollback = conn.get_num_rollback_stat(bucket=bucket_name)
                if num_rollback == 1:
                    self.log.info("Indexer has rolled back from disk snapshot")
                    break
                self.sleep(10, "Waiting for rollback to disk snapshot")
                sleep_count += 1
            self.assertNotEqual(sleep_count, 15, "Rollback to disk snapshot didn't happen")

            # Change indexer snapshot for a recovery point
            doc = {"indexer.settings.persisted_snapshot.moi.interval": 60000}
            conn.set_index_settings(doc)

            # Doing some mutation so that two new disk snapshots are generated
            num_snapshot = conn.get_index_stats()[bucket_name][index_name]["num_commits"]
            gens_load = self.generate_docs(num_items=self.docs_per_day * 3)
            self.load(gens_load, flag=self.item_flag, batch_size=self.batch_size, op_type="create", verify_data=False)

            sleep_count = 0
            while sleep_count < 10:
                self.sleep(10, "Waiting for Disk Snapshot/s to be available")
                new_num_snapshot = conn.get_index_stats()[bucket_name][index_name]["num_commits"]
                if new_num_snapshot > num_snapshot:
                    self.log.info("New Disk Snapshot is available")
                    break
                sleep_count += 1
            self.assertNotEqual(sleep_count, 10, "No new Disk Snapshot is available")

            num_snapshot = conn.get_index_stats()[bucket_name][index_name]["num_commits"]
            gens_load = self.generate_docs(num_items=self.docs_per_day * 4)
            self.load(gens_load, flag=self.item_flag, batch_size=self.batch_size, op_type="create", verify_data=False)

            sleep_count = 0
            while sleep_count < 10:
                self.sleep(10, "Waiting for Disk Snapshot/s to be available")
                new_num_snapshot = conn.get_index_stats()[bucket_name][index_name]["num_commits"]
                if new_num_snapshot > num_snapshot:
                    self.log.info("New Disk Snapshot is available")
                    break
                sleep_count += 1
            self.assertNotEqual(sleep_count, 10, "No new Disk Snapshot is available")

            # Performing full recovery for fail over Node C
            self.resume_blocked_incoming_network_from_node(node_b, node_c)
            conn.set_recovery_type(otpNode='ns_1@' + node_c.ip, recoveryType="full")
            self.cluster.rebalance(self.servers, [], [])

            # Blocking communication between Node B and Node C
            self.block_incoming_network_from_node(node_b, node_c)

            # Doing some mutation which replica on Node C won't see
            gens_load = self.generate_docs(num_items=self.docs_per_day * 5)
            self.load(gens_load, flag=self.item_flag, batch_size=self.batch_size, op_type="create", verify_data=False)

            # Killing memcached on node C
            remote_client = RemoteMachineShellConnection(node_c)
            remote_client.kill_memcached()
            remote_client.disconnect()

            # Failing over Node C
            num_rollback = conn.get_num_rollback_stat(bucket=bucket_name)
            self.cluster.failover(servers=self.servers, failover_nodes=[node_c])
            sleep_count = 0
            while sleep_count < 10:
                self.sleep(10, "Waiting for Disk Snapshot/s to be available")
                new_num_rollback = conn.get_num_rollback_stat(bucket=bucket_name)
                if new_num_rollback == num_rollback + 1:
                    self.log.info("Rollbacked to Disk Snapshot")
                    break
                sleep_count += 1
            self.assertNotEqual(sleep_count, 10, "Indexer failed to rollback")
            # Todo: add the assert to check the rollback happened from disk snapshot not from zero
        finally:
            self.resume_blocked_incoming_network_from_node(node_b, node_c)

    def test_restart_timestamp_calculation_for_rollback(self):
        """
        MB-35880
        Case B:
        Can't reproduce it consistently
        """
        data_nodes = self.get_kv_nodes()
        self.assertTrue(len(data_nodes) >= 3, "Can't run this with less than 3 KV nodes")
        # Deleting bucket as there is no easy way in testrunner to crate index before loading data
        for bucket in self.buckets:
            self.cluster.bucket_delete(self.master, bucket=bucket)

        # Create default bucket
        default_params = self._create_bucket_params(
            server=self.master, size=self.bucket_size,
            replicas=self.num_replicas, bucket_type=self.bucket_type,
            enable_replica_index=self.enable_replica_index,
            eviction_policy=self.eviction_policy, lww=self.lww,
            maxttl=self.maxttl, compression_mode=self.compression_mode)
        self.cluster.create_default_bucket(default_params)

        # creating Index idx_0
        query_definition = QueryDefinition(index_name="idx_0", index_fields=["VMs"], query_template="SELECT * FROM %s ",
                                           groups=["simple"], index_where_clause=" VMs IS NOT NULL ")
        self.load_query_definitions.append(query_definition)
        self.create_index(bucket="default", query_definition=query_definition)

        # loading data to bucket
        gens_load = self.generate_docs(num_items=self.docs_per_day)
        self.load(gens_load, flag=self.item_flag, batch_size=self.batch_size, op_type="create", verify_data=False)

        # creating few more indexes
        for item in range(1, 4):
            query_definition = QueryDefinition(index_name="idx_{0}".format(item), index_fields=["VMs"],
                                               query_template="SELECT * FROM %s ", groups=["simple"],
                                               index_where_clause=" VMs IS NOT NULL ")
            self.load_query_definitions.append(query_definition)
            self.create_index(bucket="default", query_definition=query_definition)

        # Checking item_count in all indexes
        # Check whether all the indexes have indexed all the items
        all_items_indexed_flag = False
        index_node = self.get_nodes_from_services_map(service_type="index")
        for i in range(20):
            result_list = []
            rest = RestConnection(index_node)
            for item in range(4):
                indexed_item = rest.get_index_stats()["default"]["idx_{0}".format(item)]["items_count"]
                result_list.append(indexed_item == self.docs_per_day * 2016)
            if len([item for item in result_list if item]) == 4:
                all_items_indexed_flag = True
                break
            self.sleep(10)
        if not all_items_indexed_flag:
            self.fail("Failed to index all the items in the bucket.")
        data_nodes = self.get_kv_nodes()
        node_b, node_c = (None, None)
        for node in data_nodes:
            if node.ip == self.master.ip:
                continue
            if not node_b:
                node_b = node
            else:
                node_c = node
                break

        try:
            # Blocking communication between Node B and Node C
            self.block_incoming_network_from_node(node_b, node_c)

            # Mutating docs so that replica on Node C don't see changes on Node B
            gens_load = self.generate_docs(num_items=self.docs_per_day)
            self.load(gens_load, flag=self.item_flag, batch_size=self.batch_size, op_type="create", verify_data=False)

            # killing Memcached on Node B
            remote_client = RemoteMachineShellConnection(node_b)
            remote_client.kill_memcached()
            remote_client.disconnect()

            # Failing over Node B
            self.cluster.failover(servers=self.servers, failover_nodes=[node_b])
            self.sleep(timeout=10, message="Allowing indexer to rollback")

            # Validating that indexer has indexed item after rollback and catch up with items in bucket
            for item in range(4):
                indexed_item = rest.get_index_stats()["default"]["idx_{0}".format(item)]["items_count"]
                self.assertEqual(indexed_item, self.docs_per_day * 2016, "Index {} has failed to index items after"
                                                                         " rollback")
        finally:
            self.resume_blocked_incoming_network_from_node(node_b, node_c)

    def test_recover_index_from_in_memory_snapshot(self):
        """
        MB-32102
        MB-35663
        """
        bucket_name = self.buckets[0].name
        index_name = list(self.get_index_map()[bucket_name])[0]
        # Blocking node B firewall
        data_nodes = self.get_kv_nodes()
        self.assertTrue(len(data_nodes) >= 3, "Can't run this with less than 3 KV nodes")
        node_b, node_c = (None, None)
        for node in data_nodes:
            if node.ip == self.master.ip:
                continue
            if not node_b:
                node_b = node
            else:
                node_c = node
                break
        # get num_rollback stats before triggering in-memory recovery
        index_node = self.get_nodes_from_services_map(service_type="index")
        conn = RestConnection(index_node)
        num_rollback_before_recovery = conn.get_num_rollback_stat(bucket=bucket_name)
        try:
            self.block_incoming_network_from_node(node_b, node_c)

            # killing Memcached on Node B
            remote_client = RemoteMachineShellConnection(node_b)
            remote_client.kill_memcached()
            remote_client.disconnect()

            # Failing over Node B
            self.cluster.failover(servers=self.servers, failover_nodes=[node_b])
        finally:
            # resume the communication between node B and node C
            self.resume_blocked_incoming_network_from_node(node_b, node_c)
        # get num_rollback stats after in-memory recovery of indexes
        num_rollback_after_recovery = conn.get_num_rollback_stat(bucket=bucket_name)
        self.assertEqual(num_rollback_before_recovery, num_rollback_after_recovery,
                         "Recovery didn't happen from in-memory snapshot")
        self.log.info("Node has recovered from in-memory snapshots")
        # Loading few more docs so that indexer will index updated as well as new docs
        gens_load = self.generate_docs(num_items=self.docs_per_day * 2)
        self.load(gens_load, flag=self.item_flag, batch_size=self.batch_size, op_type="create", verify_data=False)

        use_index_query = "select Count(*) from {0} USE INDEX (`{1}`)".format(bucket_name, index_name)
        result = self.n1ql_helper.run_cbq_query(query=use_index_query, server=self.n1ql_node,
                                                scan_consistency="request_plus")["results"][0]["$1"]
        expected_result = self.docs_per_day * 2 * 2016
        if self.dgm_run:
            self.assertTrue(result > expected_result, "Indexer hasn't recovered properly from in-memory as"
                                                       " indexes haven't catch up with "
                                                       "request_plus/consistency_request")
        else:
            self.assertEqual(result, expected_result, "Indexer hasn't recovered properly from in-memory as"
                                                      " indexes haven't catch up with "
                                                      "request_plus/consistency_request")
        self.log.info("Indexer continues to index as expected")

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
                                                     self.node_port)]
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
            host = "{0}:{1}".format(index_node.ip, self.node_port)
            for index in index_map.values():
                for keys, vals in index.items():
                    if vals["hosts"] == host:
                        lost_indexes.append(keys)
        log.info("Lost Indexes: {0}".format(lost_indexes))
        return lost_indexes

    def _run_kvops_tasks(self):
        tasks_ops = []
        if self.doc_ops:
            tasks_ops = self.async_run_doc_ops()
        return tasks_ops

    def _run_tasks(self, tasks_list):
        for tasks in tasks_list:
            for task in tasks:
                task.result()
