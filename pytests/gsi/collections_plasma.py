import random
import copy

from tasks.task import NodesFailureTask

from lib.membase.api.rest_client import RestConnection
from tasks.task import ConcurrentIndexCreateTask
from .base_gsi import BaseSecondaryIndexingTests
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats
import threading
from couchbase_helper.query_definitions import SQLDefinitionGenerator
from lib.couchbase_helper.documentgenerator import SDKDataLoader
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
                    self.all_indexes_state["deleted"].append(index)
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

class PlasmaCollectionsTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(PlasmaCollectionsTests, self).setUp()
        self.log.info("==============  PlasmaCollectionsTests setup has started ==============")
        self.rest.delete_all_buckets()
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_collections = self.input.param("num_collections", 1)
        self.test_timeout = self.input.param("test_timeout", 1)
        self.test_bucket = self.input.param('test_bucket', 'test_bucket')
        self.system_failure = self.input.param('system_failure', 'disk_failure')
        self.bucket_size = self.input.param('bucket_size', 100)
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
        self.batch_size = self.input.param("batch_size", 10000)
        self.start_doc = self.input.param("start_doc", 1)
        self.num_items_in_collection = self.input.param("num_items_in_collection", 100000)
        self.percent_create = self.input.param("percent_create", 100)
        self.percent_update = self.input.param("percent_update", 0)
        self.percent_delete = self.input.param("percent_delete", 0)
        self.all_collections = self.input.param("all_collections", False)
        self.dataset_template = self.input.param("dataset_template", "Employee")
        self.num_of_indexes = self.input.param("num_of_indexes", 1000)
        self.index_ops_obj = ConCurIndexOps()
        self.compact_sleep_duration = self.input.param("compact_sleep_duration", 300)

        self.log.info("==============  PlasmaCollectionsTests setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  PlasmaCollectionsTests tearDown has started ==============")
        super(PlasmaCollectionsTests, self).tearDown()
        self.log.info("==============  PlasmaCollectionsTests tearDown has completed ==============")

    def suite_tearDown(self):
        pass

    def suite_setUp(self):
        pass

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
        self.sleep(5)
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

    def create_indexes(self, num=0, defer_build=False, itr=0):
        query_definition_generator = SQLDefinitionGenerator()
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
                server = random.choice(self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True))
                index_create_task = ConcurrentIndexCreateTask(server, self.test_bucket, scope_name,
                                          collection_name, query_definitions,
                                          self.index_ops_obj, self.n1ql_helper, num_indexes_collection, defer_build, itr)
                self.index_create_task_manager.schedule(index_create_task)
                #index_create_task.result()
        self.log.info(threading.currentThread().getName() + " Completed")

    def build_indexes(self):
        self.sleep(5)
        self.log.info(threading.currentThread().getName() + " Started")
        while self.run_tasks:
            index_to_build = self.index_ops_obj.all_indexes_metadata(operation="build")
            if index_to_build:
                query_def = index_to_build["query_def"]
                build_query = query_def.generate_build_query(namespace=query_def.keyspace)
                try:
                    server = random.choice(self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True))
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
                    server = random.choice(self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True))
                    self.run_cbq_query(query=query, server=server)
                except Exception as err:
                    if "No index available on keyspace" not in str(err):
                        error_map = {"query": query, "error": str(err)}
                        self.index_ops_obj.update_errors(error_map)
        self.log.info(threading.currentThread().getName() + " Completed")

    def drop_indexes(self, drop_sleep):
        self.sleep(10)
        self.log.info(threading.currentThread().getName() + " Started")
        while self.run_tasks:
            defer_build = random.choice([True, False])
            index_to_delete = self.index_ops_obj.all_indexes_metadata(operation="delete", defer_build=defer_build)
            if index_to_delete:
                query_def = index_to_delete["query_def"]
                drop_query = query_def.generate_index_drop_query(namespace=query_def.keyspace)
                try:
                    server = random.choice(self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True))
                    self.run_cbq_query(query=drop_query, server=server)
                except Exception as err:
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

    def schedule_system_failure(self):
        self.log.info(threading.currentThread().getName() + " Started")
        self.sleep(10)
        self.failure_iteration = 1
        while self.run_tasks:
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            disk_location = RestConnection(index_nodes[0]).get_index_path()
            indexes_count_before = self.get_server_indexes_count(index_nodes)
            self.index_ops_obj.update_ignore_failure_flag(True)
            system_failure_task = NodesFailureTask(self.master, index_nodes, self.system_failure, 300, 0, False, 3,
                                                   disk_location=disk_location)
            self.system_failure_task_manager.schedule(system_failure_task)
            try:
                system_failure_task.result()
            except Exception as e:
                self.log.info("Exception: {}".format(e))

            self.sleep(300, "wait for 2.5 mins after system failure for service to recover")
            self.index_ops_obj.update_ignore_failure_flag(False)
            self.sleep(150, "wait for 2.5 mins more before collecting index count")

            indexes_count_after = self.get_server_indexes_count(index_nodes)
            self.compare_indexes_count(indexes_count_before, indexes_count_after)
            self.log.info(indexes_count_before)
            self.log.info(indexes_count_after)

            self.failure_iteration += 1

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

    def test_sharding_create_drop_indexes(self):
        self.test_fail = False
        self.errors = []
        self.index_create_task_manager = TaskManager(
            "index_create_task_manager")
        self.index_create_task_manager.start()
        self._prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections)
        sdk_data_loader = SDKDataLoader(start_seq_num=self.start_doc, num_ops=self.num_items_in_collection,
                                        percent_create=self.percent_create,
                                        percent_update=self.percent_update, percent_delete=self.percent_delete,
                                        all_collections=self.all_collections, timeout=self.test_timeout,
                                        json_template=self.dataset_template)

        data_load_process = threading.Thread(name="data_load_process",
                                             target=self.data_ops_javasdk_loader_in_batches,
                                             args=(sdk_data_loader, self.batch_size))
        self.tasks.append(data_load_process)

        create_thread = threading.Thread(name="create_thread",
                                         target=self.create_indexes,
                                         args=(self.num_of_indexes, ""))
        self.tasks.append(create_thread)

        drop_thread = threading.Thread(name="drop_thread",
                                       target=self.drop_indexes,
                                       args=[self.drop_sleep])
        self.tasks.append(drop_thread)

        build_index_thread = threading.Thread(name="build_index_thread",
                                              target=self.build_indexes)
        self.tasks.append(build_index_thread)

        scan_thread = threading.Thread(name="scan_thread",
                                       target=self.scan_indexes)

        self.tasks.append(scan_thread)

        self.run_tasks = True

        for task in self.tasks:
            task.start()

        self.sleep(self.test_timeout)

        self.run_tasks = False

        self.index_ops_obj.update_stop_create_index(True)
        self.index_create_task_manager.shutdown(True)

        for task in self.tasks:
            task.join()

        self.sleep(10, "sleep for 10 secs before validation as indexStatus API needs some time for update")

        self.verify_index_ops_obj()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

    def test_system_failure_create_drop_indexes(self):
        self.test_fail = False
        self.errors = []
        self.index_create_task_manager = TaskManager(
            "index_create_task_manager")
        self.index_create_task_manager.start()
        self.system_failure_task_manager = TaskManager(
            "system_failure_detector_thread")
        self.system_failure_task_manager.start()
        self._prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections)
        self.run_tasks = True

        self.create_indexes(num=30)

        sdk_data_loader = SDKDataLoader(start_seq_num=self.start_doc, num_ops=self.num_items_in_collection,
                                        percent_create=self.percent_create,
                                        percent_update=self.percent_update, percent_delete=self.percent_delete,
                                        all_collections=self.all_collections, timeout=self.test_timeout,
                                        json_template=self.dataset_template)

        self.data_ops_javasdk_loader_in_batches(sdk_data_loader, self.batch_size)
        #self.schedule_system_failure()

        create_thread = threading.Thread(name="create_thread",
                                         target=self.create_indexes,
                                         args=(self.num_of_indexes, "", 30))
        self.tasks.append(create_thread)

        drop_thread = threading.Thread(name="drop_thread",
                                       target=self.drop_indexes,
                                       args=[self.drop_sleep])
        self.tasks.append(drop_thread)

        build_index_thread = threading.Thread(name="build_index_thread",
                                              target=self.build_indexes)
        self.tasks.append(build_index_thread)

        scan_thread = threading.Thread(name="scan_thread",
                                       target=self.scan_indexes)

        self.tasks.append(scan_thread)

        system_failure_thread = threading.Thread(name="system_failure_thread",
                                                 target=self.schedule_system_failure)
        self.tasks.append(system_failure_thread)

        for task in self.tasks:
            task.start()

        self.sleep(self.test_timeout)

        self.run_tasks = False

        self.index_ops_obj.update_stop_create_index(True)
        self.index_create_task_manager.shutdown(True)
        self.system_failure_task_manager.shutdown(True)

        for task in self.tasks:
            task.join()

        self.sleep(5, "sleep for 5 secs before validation")

        self.verify_index_ops_obj()

        self.n1ql_helper.drop_all_indexes_on_keyspace()

        if self.index_ops_obj.get_errors():
            self.fail(str(self.index_ops_obj.get_errors()))

    def test_autocompaction_forestdb(self):
        self.run_tasks = True
        self.test_fail = False
        self.index_create_task_manager = TaskManager(
            "index_create_task_manager")
        self.index_create_task_manager.start()
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for indexer_node in indexer_nodes:
            rest = RestConnection(indexer_node)
            rest.set_index_settings({"indexer.metadata.compaction.sleepDuration": self.compact_sleep_duration})

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
