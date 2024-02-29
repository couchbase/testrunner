import random
import os

from tasks.task import NodesFailureTask

from lib.membase.api.rest_client import RestConnection
from tasks.task import ConcurrentIndexCreateTask

from tasks.task import SDKLoadDocumentsTask
from .base_gsi import BaseSecondaryIndexingTests
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats
import threading
from couchbase_helper.query_definitions import SQLDefinitionGenerator
from lib.couchbase_helper.documentgenerator import SDKDataLoader
from remote.remote_util import RemoteMachineShellConnection
from tasks.taskmanager import TaskManager
import math

class ConCurIndexOps(BaseSecondaryIndexingTests):
    def __init__(self):
        self.all_indexes_state = {"created": [], "deleted": [], "defer_build": []}
        self.errors = []
        self.test_fail = False
        self._lock_queue = threading.Lock()
        self.errors_lock_queue = threading.Lock()
        self.create_index_lock = threading.Lock()
        self.ignore_failure = False
        self.stop_create_index = False

    def update_errors(self, error_map):
        with self.errors_lock_queue:
            if not self.ignore_failure:
                self.errors.append(error_map)

    def update_ignore_failure_flag(self, flag):
        with self.errors_lock_queue:
            self.ignore_failure = flag

    def get_ignore_failure_flag(self, flag):
        with self.errors_lock_queue:
            return self.ignore_failure

    def update_stop_create_index(self, flag):
        with self.create_index_lock:
            self.stop_create_index = flag

    def get_stop_create_index(self):
        with self.create_index_lock:
            return self.stop_create_index

    def get_errors(self):
        return self.errors

    def get_delete_index_list(self):
        return self.all_indexes_state["deleted"]

    def get_create_index_list(self):
        return self.all_indexes_state["created"]

    def get_defer_index_list(self):
        return self.all_indexes_state["defer_build"]

    def all_indexes_metadata(self, index_meta=None, operation="create", defer_build=False):
        with self._lock_queue:
            if operation is "create" and defer_build:
                self.all_indexes_state["defer_build"].append(index_meta)
            elif operation is "create":
                self.all_indexes_state["created"].append(index_meta)
            elif operation is "delete" and defer_build:
                try:
                    index = self.all_indexes_state["defer_build"].\
                        pop(random.randrange(len(self.all_indexes_state["defer_build"])))
                    self.all_indexes_state["deleted"].append(index)
                except Exception as e:
                    index = None
                return index
            elif operation is "delete":
                try:
                    index = self.all_indexes_state["created"].\
                        pop(random.randrange(len(self.all_indexes_state["created"])))
                except Exception as e:
                    index = None
                return index
            elif operation is "scan":
                try:
                    index = random.choice(self.all_indexes_state["created"])
                except Exception as e:
                    index = None
                return index
            elif operation is "build":
                try:
                    index = self.all_indexes_state["defer_build"].\
                        pop(random.randrange(len(self.all_indexes_state["defer_build"])))
                except Exception as e:
                    index = None
                return index

    def build_complete_add_to_create(self, index):
        with self._lock_queue:
            if index is not None:
                self.all_indexes_state["created"].append(index)

    def add_to_deleted(self, index):
        with self._lock_queue:
            if index is not None:
                self.all_indexes_state["deleted"].append(index)

class PlasmaCollectionsTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(PlasmaCollectionsTests, self).setUp()
        self.log.info("==============  PlasmaCollectionsTests setup has started ==============")
        self.rest.delete_all_buckets()
        self.num_scopes = self.input.param("num_scopes", 5)
        self.num_collections = self.input.param("num_collections", 10)
        self.test_timeout = self.input.param("test_timeout", 60)
        self.test_bucket = self.input.param('test_bucket', 'test_bucket')
        self.system_failure = self.input.param('system_failure', 'disk_failure')
        self.bucket_size = self.input.param('bucket_size', 256)
        self.drop_sleep = self.input.param('drop_sleep', 30)
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.cli_rest = CollectionsRest(self.master)
        self.stat = CollectionsStats(self.master)
        self.scope_prefix = 'test_scope'
        self.collection_prefix = 'test_collection'
        self.run_cbq_query = self.n1ql_helper.run_cbq_query
        self.batch_size = self.input.param("batch_size", 50000)
        self.all_indexes = []
        self._lock_queue = threading.Lock()
        self.run_tasks = False
        self.tasks = []
        self.batch_size = self.input.param("batch_size", 100000)
        self.start_doc = self.input.param("start_doc", 1)
        self.num_items_in_collection = self.input.param("num_items_in_collection", 100000)
        self.percent_create = self.input.param("percent_create", 100)
        self.percent_update = self.input.param("percent_update", 0)
        self.percent_delete = self.input.param("percent_delete", 0)
        self.all_collections = self.input.param("all_collections", False)
        self.dataset_template = self.input.param("dataset_template", "Employee")
        self.num_of_indexes = self.input.param("num_of_indexes", 1000)
        self.failure_timeout = self.input.param("failure_timeout", 60)
        self.failure_recover_sleep = self.input.param("failure_recover_sleep", 600)
        self.index_ops_obj = ConCurIndexOps()
        self.sweep_interval = self.input.param("sweep_interval", 120)
        self.compact_sleep_duration = self.input.param("compact_sleep_duration", 300)
        self.moi_snapshot_interval = self.input.param("moi_snapshot_interval", 300000)
        self.dgm_check_timeout = self.input.param("dgm_check_timeout", 1800)
        self.concur_drop_indexes = self.input.param("concur_drop_indexes", True)
        self.concur_scan_indexes = self.input.param("concur_scan_indexes", True)
        self.concur_create_indexes = self.input.param("concur_create_indexes", True)
        self.concur_build_indexes = self.input.param("concur_build_indexes", True)
        self.concur_system_failure = self.input.param("concur_system_failure", True)

        self.simple_create_index = self.input.param("simple_create_index", False)
        self.simple_drop_index = self.input.param("simple_drop_index", False)
        self.simple_scan_index = self.input.param("simple_scan_index", False)
        self.simple_kill_indexer = self.input.param("simple_kill_indexer", False)
        self.simple_kill_memcached = self.input.param("simple_kill_memcached", False)
        self.num_pre_indexes = self.input.param("num_pre_indexes", 50)
        self.num_failure_iteration = self.input.param("num_failure_iteration", None)
        self.failure_map = {"disk_failure": {"failure_task": "induce_disk_failure",
                                             "recover_task": "recover_disk_failure",
                                             "expected_failure": ["Terminate Request due to server termination",
                                                                  "Build Already In Progress", "Timeout 1ms exceeded"]},
                            "disk_full": {"failure_task": "induce_disk_full",
                                          "recover_task": "recover_disk_full_failure",
                                          "expected_failure": ["Terminate Request due to server termination",
                                                               "Build Already In Progress", "Timeout 1ms exceeded",
                                                               "There is no available index service that can process "
                                                               "this request at this time",
                                                               "Create index or Alter replica cannot proceed "
                                                               "due to network partition, node failover or "
                                                               "indexer failure"]},
                            "restart_couchbase": {"failure_task": "stop_couchbase",
                                                "recover_task": "start_couchbase",
                                                "expected_failure": ["Terminate Request due to server termination",
                                                                     "There is no available index service that can process "
                                                                     "this request at this time",
                                                                     "Build Already In Progress", "Timeout 1ms exceeded",
                                                                     "Create index or Alter replica cannot proceed "
                                                                     "due to network partition, node failover or "
                                                                     "indexer failure"]},
                            "net_packet_loss": {"failure_task": "induce_net_packet_loss",
                                                "recover_task": "disable_net_packet_loss",
                                                "expected_failure": []},
                            "network_delay": {"failure_task": "induce_network_delay",
                                                "recover_task": "disable_network_delay",
                                                "expected_failure": []},
                            "disk_readonly": {"failure_task": "induce_disk_readonly",
                                                "recover_task": "disable_disk_readonly",
                                                "expected_failure": ["Terminate Request due to server termination",
                                                                     "There is no available index service that can process "
                                                                     "this request at this time",
                                                                     "Build Already In Progress", "Timeout 1ms exceeded"]},
                            "limit_file_limits": {"failure_task": "induce_limit_file_limits",
                                                "recover_task": "disable_limit_file_limits",
                                                "expected_failure": []},
                            "limit_file_size_limit": {"failure_task": "induce_limit_file_size_limit",
                                                "recover_task": "disable_limit_file_size_limit",
                                                "expected_failure": ["Terminate Request due to server termination"]},
                            "extra_files_in_log_dir": {"failure_task": "induce_extra_files_in_log_dir",
                                                "recover_task": "disable_extra_files_in_log_dir",
                                                "expected_failure": []},
                            "dummy_file_in_log_dir": {"failure_task": "induce_dummy_file_in_log_dir",
                                                "recover_task": "disable_dummy_file_in_log_dir",
                                                "expected_failure": []},
                            "empty_files_in_log_dir": {"failure_task": "induce_empty_files_in_log_dir",
                                                "recover_task": "disable_empty_files_in_log_dir",
                                                "expected_failure": []},
                            "shard_json_corruption": {"failure_task": "shard_json_corruption",
                                                       "recover_task": None,
                                                       "expected_failure": []},
                            "enable_firewall": {"failure_task": "induce_enable_firewall",
                                                "recover_task": "disable_firewall",
                                                "expected_failure": ["There is no available index service that can process "
                                                                     "this request at this time",
                                                                     "Build Already In Progress", "Timeout 1ms exceeded",
                                                                     "Create index or Alter replica cannot proceed "
                                                                     "due to network partition, node failover or "
                                                                     "indexer failure"]},
                            "limit_file_limits_desc": {"failure_task": "induce_limit_file_limits_desc",
                                                "recover_task": "disable_limit_file_limits_desc",
                                                "expected_failure": []},
                            "stress_cpu": {"failure_task": "stress_cpu",
                                                "recover_task": None,
                                                "expected_failure": ["Terminate Request due to server termination",
                                                                     "There is no available index service that can process "
                                                                     "this request at this time",
                                                                     "Build Already In Progress", "Timeout 1ms exceeded",
                                                                     "Create index or Alter replica cannot proceed "
                                                                     "due to network partition, node failover or "
                                                                     "indexer failure"]},
                            "stress_ram": {"failure_task": "stress_ram",
                                                "recover_task": None,
                                                "expected_failure": ["Terminate Request due to server termination",
                                                                     "There is no available index service that can process "
                                                                     "this request at this time",
                                                                     "Build Already In Progress", "Timeout 1ms exceeded",
                                                                     "Create index or Alter replica cannot proceed "
                                                                     "due to network partition, node failover or "
                                                                     "indexer failure"]}}

        self.log.info("Setting indexer memory quota to 256 MB and other settings...")
        self.index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        self.n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        self.data_nodes = self.get_kv_nodes()
        for index_node in self.index_nodes:
            rest = RestConnection(index_node)
            rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=256)
            rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": self.moi_snapshot_interval})
            rest.set_index_settings({"indexer.settings.persisted_snapshot_init_build.moi.interval": self.moi_snapshot_interval})
            rest.set_index_settings({"indexer.metadata.compaction.sleepDuration": self.compact_sleep_duration})
            rest.set_index_settings({"indexer.plasma.mainIndex.evictSweepInterval": self.sweep_interval})
            rest.set_index_settings({"indexer.plasma.backIndex.evictSweepInterval": self.sweep_interval})
        self.disk_location = RestConnection(self.index_nodes[0]).get_index_path()
        self.key_prefix=f'bigkeybigkeybigkeybigkeybigkeybigkey_{self.master.ip}_'

        self.log.info("==============  PlasmaCollectionsTests setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  PlasmaCollectionsTests tearDown has started ==============")
        super(PlasmaCollectionsTests, self).tearDown()
        try:
            self.reset_data_mount_point(self.index_nodes)
        except Exception as err:
            self.log.info(str(err))
        self.log.info("==============  PlasmaCollectionsTests tearDown has completed ==============")

    def _prepare_collection_for_indexing(self, num_scopes=1, num_collections=1):
        self.keyspace = []
        self.cli_rest.create_scope_collection_count(scope_num=num_scopes, collection_num=num_collections,
                                                    scope_prefix=self.scope_prefix,
                                                    collection_prefix=self.collection_prefix,
                                                    bucket=self.test_bucket)
        self.scopes = self.cli_rest.get_bucket_scopes(bucket=self.test_bucket)
        self.collections = list(set(self.cli_rest.get_bucket_collections(bucket=self.test_bucket)))
        self.scopes.remove('_default')
        self.collections.remove('_default')
        self.sleep(10)
        for s_item in self.scopes:
            for c_item in self.collections:
                self.keyspace.append(f'default:{self.test_bucket}.{s_item}.{c_item}')

    def check_if_indexes_created(self, index_list, defer_build=False):
        indexes_not_created = []
        for index in index_list:
            index_created, status = self.check_if_index_created(index["name"], defer_build)
            if not index_created:
                indexes_not_created.append({"name": index["name"], "status": status})
        return indexes_not_created

    def check_if_indexes_not_created(self, index_list, defer_build=False):
        indexes_created = []
        for index in index_list:
            index_created, status = self.check_if_index_created(index["name"], defer_build)
            if index_created:
                indexes_created.append({"name": index["name"], "status": status})
        return indexes_created

    def check_if_index_recovered(self, index_list):
        indexes_not_recovered = []
        for index in index_list:
            recovered, index_count, collection_itemcount = self._verify_collection_count_with_index_count(index["query_def"])
            if not recovered:
                error_map = {"index_name": index["name"], "index_count": index_count, "bucket_count": collection_itemcount}
                indexes_not_recovered.append(error_map)
        if not indexes_not_recovered:
            self.log.info("All indexes recovered")

        return indexes_not_recovered

    def check_if_indexes_deleted(self, index_list):
        indexes_not_deleted = []
        for index in index_list:
            index_created, status = self.check_if_index_created(index["name"])
            if index_created:
                indexes_not_deleted.append({"name": index["name"], "status": status})
        return indexes_not_deleted

    def verify_index_ops_obj(self):
        indexes_not_created = self.check_if_indexes_created(self.index_ops_obj.get_create_index_list())
        if indexes_not_created:
            self.index_ops_obj.update_errors(f'Expected Created indexes {indexes_not_created} found to be not created')
            self.log.info(f'Expected Created indexes {indexes_not_created} found to be not created')
        if not self.wait_for_mutation_processing(self.index_nodes):
            self.index_ops_obj.update_errors("Some indexes mutation not processed")
        indexes_not_recovered = self.check_if_index_recovered(self.index_ops_obj.get_create_index_list())
        if indexes_not_recovered:
            self.index_ops_obj.update_errors(f'Some Indexes not recovered {indexes_not_recovered}')
            self.log.info(f'Some Indexes not recovered {indexes_not_recovered}')
        indexes_not_deleted = self.check_if_indexes_deleted(self.index_ops_obj.get_delete_index_list())
        if indexes_not_deleted:
            self.index_ops_obj.update_errors(f'Expected Deleted indexes {indexes_not_deleted} found to be not deleted')
            self.log.info(f'Expected Deleted indexes {indexes_not_deleted} found to be not deleted')
        indexes_not_defer_build = self.check_if_indexes_created(index_list=self.index_ops_obj.get_defer_index_list(), defer_build=True)
        if indexes_not_defer_build:
            self.index_ops_obj.update_errors(f'Expected defer build indexes {indexes_not_defer_build} '
                                             f'found to be not in defer_build state')
            self.log.info(f'Expected defer build indexes {indexes_not_defer_build} '
                          f'found to be not in defer_build state')

    def create_indexes(self, num=0, defer_build=False, itr=0, expected_failure=[]):
        query_definition_generator = SQLDefinitionGenerator()
        index_create_tasks = []
        if self.system_failure in self.failure_map.keys():
            expected_failure = self.failure_map[self.system_failure]["expected_failure"]
        self.log.info(threading.currentThread().getName() + " Started")
        if len(self.keyspace) < num:
            num_indexes_collection = math.ceil(num / len(self.keyspace))
        else:
            num_indexes_collection = 1
        for collection_keyspace in self.keyspace:
            if self.run_tasks:
                collection_name = collection_keyspace.split('.')[-1]
                scope_name = collection_keyspace.split('.')[-2]
                query_definitions = query_definition_generator. \
                    generate_employee_data_query_definitions(index_name_prefix='idx_' +
                                                                               scope_name + "_"
                                                                               + collection_name,
                                                             keyspace=collection_keyspace)
                server = random.choice(self.n1ql_nodes)
                index_create_task = ConcurrentIndexCreateTask(server, self.test_bucket, scope_name,
                                          collection_name, query_definitions,
                                          self.index_ops_obj, self.n1ql_helper, num_indexes_collection, defer_build,
                                                              itr, expected_failure)
                self.index_create_task_manager.schedule(index_create_task)
                index_create_tasks.append(index_create_task)
        self.log.info(threading.currentThread().getName() + " Completed")

        return index_create_tasks

    def build_indexes(self):
        self.sleep(5)
        self.log.info(threading.currentThread().getName() + " Started")
        while self.run_tasks:
            index_to_build = self.index_ops_obj.all_indexes_metadata(operation="build")
            if index_to_build:
                query_def = index_to_build["query_def"]
                build_query = query_def.generate_build_query(namespace=query_def.keyspace)
                try:
                    server = random.choice(self.n1ql_nodes)
                    self.run_cbq_query(query=build_query, server=server)
                    self.index_ops_obj.build_complete_add_to_create(index_to_build)
                except Exception as err:
                    if "Build Already In Progress" not in str(err):
                        error_map = {"query": build_query, "error": str(err)}
                        self.index_ops_obj.update_errors(error_map)
            self.sleep(5)
        self.log.info(threading.currentThread().getName() + " Completed")

    def scan_indexes(self):
        self.sleep(5)
        self.log.info(threading.currentThread().getName() + " Started")
        while self.run_tasks:
            index_to_scan = self.index_ops_obj.all_indexes_metadata(operation="scan")
            if index_to_scan:
                self.log.info(f'Processing index: {index_to_scan["name"]}')
                query_def = index_to_scan["query_def"]
                query = query_def.generate_query(bucket=query_def.keyspace)
                try:
                    server = random.choice(self.n1ql_nodes)
                    self.run_cbq_query(query=query, server=server)
                except Exception as err:
                    if "No index available on keyspace" not in str(err):
                        error_map = {"query": query, "error": str(err)}
                        self.index_ops_obj.update_errors(error_map)
        self.log.info(threading.currentThread().getName() + " Completed")

    def drop_indexes(self, drop_sleep, defer_build=None):
        self.sleep(10)
        self.log.info(threading.currentThread().getName() + " Started")
        while self.run_tasks:
            if not defer_build:
                defer_build = random.choice([True, False])
            index_to_delete = self.index_ops_obj.all_indexes_metadata(operation="delete", defer_build=defer_build)
            if index_to_delete:
                query_def = index_to_delete["query_def"]
                drop_query = query_def.generate_index_drop_query(namespace=query_def.keyspace)
                try:
                    server = random.choice(self.n1ql_nodes)
                    self.run_cbq_query(query=drop_query, server=server)
                    self.index_ops_obj.add_to_deleted(index_to_delete)
                except Exception as err:
                    if "the operation will automaticaly retry after cluster is back to normal" not in str(err) \
                            and "Index Not Found" not in str(err):
                        error_map = {"query": drop_query, "error": str(err)}
                        self.index_ops_obj.update_errors(error_map)
            self.sleep(drop_sleep)
        self.log.info(threading.currentThread().getName() + " Completed")

    def compare_indexes_count(self, indexes_count_before, indexes_count_after):
        indexes_with_data_loss = {}
        for index in indexes_count_after:
            if index in indexes_count_before and indexes_count_after[index] < indexes_count_before[index]:
                indexes_with_data_loss[index] = {"failure_iteration": self.failure_iteration,
                                                 "indexes_count_before": indexes_count_before[index],
                                                 "indexes_count_after": indexes_count_after[index]}
        if indexes_with_data_loss:
            self.index_ops_obj.update_errors(indexes_with_data_loss)


    def load_docs(self):
        sdk_data_loader = SDKDataLoader(start_seq_num=self.start_doc, num_ops=self.num_items_in_collection,
                                        percent_create=self.percent_create,
                                        percent_update=self.percent_update, percent_delete=self.percent_delete,
                                        all_collections=self.all_collections, timeout=1800,
                                        json_template=self.dataset_template,
                                        key_prefix=self.key_prefix, shuffle_docs=True)
        for bucket in self.buckets:
            _task = SDKLoadDocumentsTask(self.master, bucket, sdk_data_loader)
            self.sdk_loader_manager.schedule(_task)


    def schedule_system_failure(self):
        self.log.info(threading.currentThread().getName() + " Started")
        self.sleep(10)
        self.failure_iteration = 1
        while self.run_tasks:
            if self.num_failure_iteration and self.failure_iteration > self.num_failure_iteration:
                self.log.info("Reached number of failure iterations")
                self.run_tasks = False
                break
            disk_location = RestConnection(self.index_nodes[0]).get_index_path()
            #indexes_count_before = self.get_server_indexes_count(self.index_nodes)
            self.index_ops_obj.update_ignore_failure_flag(True)
            system_failure_task = NodesFailureTask(self.master, self.index_nodes, self.system_failure, 300, 0, False, 3,
                                                   disk_location=disk_location, failure_timeout=self.failure_timeout)
            self.system_failure_task_manager.schedule(system_failure_task)
            try:
                system_failure_task.result()
            except Exception as e:
                self.log.info("Exception: {}".format(e))

            self.sleep(300, "wait for 5 mins after system failure for service to recover")
            self.index_ops_obj.update_ignore_failure_flag(False)
            self.sleep(self.failure_recover_sleep, "wait for {} secs more before collecting index count".format(self.failure_recover_sleep))

            #indexes_count_after = self.get_server_indexes_count(self.index_nodes)
            #self.compare_indexes_count(indexes_count_before, indexes_count_after)
            #self.log.info(indexes_count_before)
            #self.log.info(indexes_count_after)

            self.failure_iteration += 1

        self.log.info(threading.currentThread().getName() + " Completed")

    def induce_schedule_system_failure(self, failure_task):
        if failure_task:
            self.log.info(threading.currentThread().getName() + " Started")
            self.sleep(10)

            system_failure_task = NodesFailureTask(self.master, self.index_nodes, failure_task, 300, 0, False, 3,
                                                   disk_location=self.disk_location, failure_timeout=self.failure_timeout)
            self.system_failure_task_manager.schedule(system_failure_task)
            try:
                system_failure_task.result()
            except Exception as e:
                self.log.info("Exception: {}".format(e))

            self.log.info(threading.currentThread().getName() + " Completed")

    def get_num_compaction_per_node_initialized(self, initial_meta_store_size):
        num_compaction_per_node = {}
        for k in initial_meta_store_size:
            num_compaction_per_node[k['nodeip']] = 0
        return num_compaction_per_node

    def get_initial_meta_store_size(self, nodeip, initial_meta_store_size):
        for n in initial_meta_store_size:
            if n['nodeip'] == nodeip:
                return n['metastore_size']

    def verify_fdb_compaction(self):
        self.log.info(threading.currentThread().getName() + " Started")
        self.sleep(10)
        initial_meta_store_size = self.get_size_of_metastore_file()
        num_compaction_per_node = self.get_num_compaction_per_node_initialized(initial_meta_store_size)
        self.log.info(f'initial_meta_store_size: {initial_meta_store_size}')
        while self.run_tasks:
            meta_store_size = self.get_size_of_metastore_file()
            self.log.info(f'meta_store_size: {meta_store_size}')
            for k in meta_store_size:
                if int(k['metastore_size']) < int(self.get_initial_meta_store_size(k['nodeip'], initial_meta_store_size)):
                    num_compaction_per_node[k['nodeip']] += 1
            initial_meta_store_size = meta_store_size
            self.sleep(5)
        self.log.info(f'Number of autocompaction of fdb : {num_compaction_per_node}')

        for v in num_compaction_per_node.values():
            if v < 1:
                self.test_fail = True

        self.log.info(threading.currentThread().getName() + " Completed")

    def test_system_failure_create_drop_indexes(self):
        self.test_fail = False
        self.errors = []
        self.index_create_task_manager = TaskManager(
            "index_create_task_manager")
        self.index_create_task_manager.start()
        self.system_failure_task_manager = TaskManager(
            "system_failure_detector_thread")
        self.system_failure_task_manager.start()
        self.sdk_loader_manager = TaskManager(
            "sdk_loader_manager")
        self.sdk_loader_manager.start()
        if self.num_failure_iteration:
            self.test_timeout = self.failure_timeout * len(self.index_nodes)

        self._prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections)
        self.run_tasks = True

        index_create_tasks = self.create_indexes(num=self.num_pre_indexes)
        for task in index_create_tasks:
            task.result()

        load_doc_thread = threading.Thread(name="load_doc_thread",
                                           target=self.load_docs)
        load_doc_thread.start()

        self.sleep(60, "sleeping for 60 sec for index to start processing docs")

        if not self.check_if_indexes_in_dgm():
            self.log.error("indexes not in dgm even after {}".format(self.dgm_check_timeout))

        if self.concur_create_indexes:
            create_thread = threading.Thread(name="create_thread",
                                             target=self.create_indexes,
                                             args=(self.num_of_indexes, "", 30))
            self.tasks.append(create_thread)

        if self.concur_drop_indexes:
            drop_thread = threading.Thread(name="drop_thread",
                                           target=self.drop_indexes,
                                           args=[self.drop_sleep])
            self.tasks.append(drop_thread)

        if self.concur_build_indexes:
            build_index_thread = threading.Thread(name="build_index_thread",
                                                  target=self.build_indexes)
            self.tasks.append(build_index_thread)

        if self.concur_scan_indexes:
            scan_thread = threading.Thread(name="scan_thread",
                                           target=self.scan_indexes)

            self.tasks.append(scan_thread)

        if self.concur_system_failure:
            system_failure_thread = threading.Thread(name="system_failure_thread",
                                                     target=self.schedule_system_failure)
            self.tasks.append(system_failure_thread)

        for task in self.tasks:
            task.start()

        self.tasks.append(load_doc_thread)

        self.sleep(self.test_timeout)

        self.run_tasks = False

        self.index_ops_obj.update_stop_create_index(True)
        self.kill_loader_process()
        self.sdk_loader_manager.shutdown(True)
        self.index_create_task_manager.shutdown(True)
        self.system_failure_task_manager.shutdown(True)

        for task in self.tasks:
            task.join()

        self.wait_until_indexes_online()
        self.sleep(600, "sleep for 10 mins before validation")
        self.verify_index_ops_obj()

        self.n1ql_helper.drop_all_indexes_on_keyspace()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))


    def test_system_failure_create_drop_indexes_simple(self):
        self.test_fail = False
        self.concur_system_failure = self.input.param("concur_system_failure", False)
        self.errors = []
        self.index_create_task_manager = TaskManager(
            "index_create_task_manager")
        self.index_create_task_manager.start()
        self.system_failure_task_manager = TaskManager(
            "system_failure_detector_thread")
        self.system_failure_task_manager.start()
        self.sdk_loader_manager = TaskManager(
            "sdk_loader_manager")
        self.sdk_loader_manager.start()
        if self.num_failure_iteration:
            self.test_timeout = self.failure_timeout * len(self.index_nodes)

        self._prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections)
        self.run_tasks = True

        index_create_tasks = self.create_indexes(num=self.num_pre_indexes)
        for task in index_create_tasks:
            task.result()

        load_doc_thread = threading.Thread(name="load_doc_thread",
                                           target=self.load_docs)
        load_doc_thread.start()

        self.sleep(60, "sleeping for 60 sec for index to start processing docs")

        if not self.check_if_indexes_in_dgm():
            self.log.error("indexes not in dgm even after {}".format(self.dgm_check_timeout))

        if self.concur_system_failure:
            system_failure_thread = threading.Thread(name="system_failure_thread",
                                                     target=self.induce_schedule_system_failure,
                                                     args=[self.failure_map[self.system_failure]["failure_task"]])
            system_failure_thread.start()
            self.sleep(20)
        else:
            self.induce_schedule_system_failure(self.failure_map[self.system_failure]["failure_task"])
            self.sleep(90, "sleeping for  mins for mutation processing during system failure ")

        if self.simple_create_index:
            index_create_tasks = self.create_indexes(num=1,defer_build=False,itr=300,
                                                     expected_failure=self.failure_map[self.system_failure]["expected_failure"])
            for task in index_create_tasks:
                task.result()
            self.sleep(60, "sleeping for 1 min after creation of indexes")

        if self.simple_drop_index:
            task_thread = threading.Thread(name="drop_thread",
                                           target=self.drop_indexes,
                                           args=(2, False))
            task_thread.start()
            self.sleep(15, "sleeping for 15 sec")
            self.run_tasks = False
            task_thread.join()

        if self.simple_scan_index:
            self.run_tasks = True
            task_thread = threading.Thread(name="scan_thread",
                                           target=self.scan_indexes)
            task_thread.start()
            self.sleep(30, "sleeping for 10 sec")
            self.run_tasks = False
            task_thread.join()

        if self.simple_kill_indexer:
            remote = RemoteMachineShellConnection(self.index_nodes[0])
            remote.terminate_process(process_name="indexer")
            self.sleep(60, "sleeping for 60 sec for indexer to come back")

        if self.simple_kill_memcached:
            remote = RemoteMachineShellConnection(self.data_nodes[1])
            remote.kill_memcached()
            self.sleep(60, "sleeping for 60 sec for memcached to come back")

        if self.concur_system_failure:
            system_failure_thread.join()
        else:
            self.induce_schedule_system_failure(self.failure_map[self.system_failure]["recover_task"])
        self.index_ops_obj.update_stop_create_index(True)
        self.kill_loader_process()
        self.sdk_loader_manager.shutdown(True)
        self.index_create_task_manager.shutdown(True)
        self.system_failure_task_manager.shutdown(True)

        self.wait_until_indexes_online()
        self.sleep(120, "sleep for 120 secs before validation")
        self.verify_index_ops_obj()

        self.n1ql_helper.drop_all_indexes_on_keyspace()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

    def test_kill_indexer_create_drop_indexes_simple(self):
        self.test_fail = False
        self.concur_system_failure = self.input.param("concur_system_failure", False)
        self.errors = []
        self.index_create_task_manager = TaskManager(
            "index_create_task_manager")
        self.index_create_task_manager.start()
        self.system_failure_task_manager = TaskManager(
            "system_failure_detector_thread")
        self.system_failure_task_manager.start()
        self.sdk_loader_manager = TaskManager(
            "sdk_loader_manager")
        self.sdk_loader_manager.start()
        if self.num_failure_iteration:
            self.test_timeout = self.failure_timeout * len(self.index_nodes)

        self._prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections)
        self.run_tasks = True

        index_create_tasks = self.create_indexes(num=self.num_pre_indexes)
        for task in index_create_tasks:
            task.result()

        load_doc_thread = threading.Thread(name="load_doc_thread",
                                           target=self.load_docs)
        load_doc_thread.start()

        self.sleep(60, "sleeping for 60 sec for index to start processing docs")

        if not self.check_if_indexes_in_dgm():
            self.log.error("indexes not in dgm even after {}".format(self.dgm_check_timeout))

        index_create_tasks = self.create_indexes(itr=300, num=25)

        self.kill_index = True
        index_node = self.get_nodes_from_services_map(service_type="index")
        system_failure_thread = threading.Thread(name="kill_indexer_thread",
                                                 target=self._kill_all_processes_index_with_sleep,
                                                 args=(index_node, 1, 600))
        system_failure_thread.start()

        for task in index_create_tasks:
            task.result()

        self.kill_index = False
        self.index_ops_obj.update_stop_create_index(True)
        self.kill_loader_process()
        self.sdk_loader_manager.shutdown(True)
        self.index_create_task_manager.shutdown(True)
        self.system_failure_task_manager.shutdown(True)
        system_failure_thread.join()

        self.wait_until_indexes_online()
        self.sleep(120, "sleep for 120 secs before validation")
        self.verify_index_ops_obj()

        self.n1ql_helper.drop_all_indexes_on_keyspace()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

    def test_shard_json_corruption(self):
        self.test_fail = False
        self.concur_system_failure = self.input.param("concur_system_failure", False)
        self.errors = []
        self.index_create_task_manager = TaskManager(
            "index_create_task_manager")
        self.index_create_task_manager.start()
        self.system_failure_task_manager = TaskManager(
            "system_failure_detector_thread")
        self.system_failure_task_manager.start()
        self.sdk_loader_manager = TaskManager(
            "sdk_loader_manager")
        self.sdk_loader_manager.start()
        if self.num_failure_iteration:
            self.test_timeout = self.failure_timeout * len(self.index_nodes)

        self._prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections)
        self.run_tasks = True

        index_create_tasks = self.create_indexes(num=self.num_pre_indexes)
        for task in index_create_tasks:
            task.result()

        load_doc_thread = threading.Thread(name="load_doc_thread",
                                           target=self.load_docs)
        load_doc_thread.start()

        self.sleep(60, "sleeping for 60 sec for index to start processing docs")

        #if not self.check_if_indexes_in_dgm():
            #self.log.error("indexes not in dgm even after {}".format(self.dgm_check_timeout))

        self.kill_loader_process()
        self.wait_for_mutation_processing(self.index_nodes)

        self.induce_schedule_system_failure(self.failure_map[self.system_failure]["failure_task"])
        self.sleep(90, "sleeping for  mins for mutation processing during system failure ")

        remote = RemoteMachineShellConnection(self.index_nodes[0])
        remote.terminate_process(process_name="indexer")
        self.sleep(60, "sleeping for 60 sec for indexer to come back")

        self.index_ops_obj.update_stop_create_index(True)
        self.sdk_loader_manager.shutdown(True)
        self.index_create_task_manager.shutdown(True)
        self.system_failure_task_manager.shutdown(True)

        self.wait_until_indexes_online()
        indexes_created = self.check_if_indexes_not_created(self.index_ops_obj.get_create_index_list())
        if indexes_created:
            self.fail(f'{indexes_created} are not dropped')

        if self.check_if_shard_exists("shard1", self.index_nodes[0]):
            self.fail('shard1 is not cleaned on disk')

    def test_autocompaction_forestdb(self):
        self.run_tasks = True
        self.test_fail = False
        self.index_create_task_manager = TaskManager(
            "index_create_task_manager")
        self.index_create_task_manager.start()

        self._prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections)

        sdk_data_loader = SDKDataLoader(start_seq_num=self.start_doc, num_ops=self.num_items_in_collection,
                                        percent_create=self.percent_create,
                                        percent_update=self.percent_update, percent_delete=self.percent_delete,
                                        all_collections=self.all_collections, timeout=self.test_timeout,
                                        json_template=self.dataset_template)

        self.data_ops_javasdk_loader_in_batches(sdk_data_loader, self.batch_size)

        create_thread = threading.Thread(name="create_thread",
                                         target=self.create_indexes,
                                         args=(self.num_of_indexes, False))
        self.tasks.append(create_thread)

        drop_thread = threading.Thread(name="drop_thread",
                                       target=self.drop_indexes,
                                       args=[self.drop_sleep])
        self.tasks.append(drop_thread)

        verify_fdb_compaction = threading.Thread(name="verify_fdb_compaction",
                                                 target=self.verify_fdb_compaction)
        self.tasks.append(verify_fdb_compaction)

        self.run_tasks = True

        for task in self.tasks:
            task.start()

        self.sleep(self.test_timeout)

        self.run_tasks = False

        for task in self.tasks:
            task.join()

        self.index_ops_obj.update_stop_create_index(True)
        self.index_create_task_manager.shutdown(True)


        if self.test_fail:
            self.fail("Auto compaction did not trigger for expected number of times")
