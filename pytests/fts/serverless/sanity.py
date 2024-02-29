from TestInput import TestInputServer, TestInputSingleton
from lib import global_vars
from lib.Cb_constants.CBServer import CbServer
from lib.SystemEventLogLib.Events import EventHelper
from lib.capella.utils import CapellaCredentials
from pytests.fts.fts_callable import FTSCallable
from pytests.serverless.serverless_basetestcase import ServerlessBaseTestCase
from deepdiff import DeepDiff
import time
import random
import threading
from datetime import datetime
from prettytable import PrettyTable
from collections import defaultdict, deque

class FTSElixirSanity(ServerlessBaseTestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self._num_of_docs_per_collection = self.input.param("num_of_docs_per_collection", 10000)
        CbServer.use_https = True
        CbServer.capella_run = True
        CbServer.capella_credentials = CapellaCredentials(self.input.capella)
        self.index_replicas = self.input.param("index_replicas", 1)
        self.num_index_partitions = self.input.param("num_partitions", 1)
        self.num_databases = self.input.param("num_databases", 1)
        self.num_indexes = self.input.param("num_indexes", 1)
        self.HWM_limit = self.input.param("HWM_limit", 0.9)
        self.LWM_limit = self.input.param("LWM_limit", 0.75)
        self.UWM_limit = self.input.param("UWM_limit", 0.3)
        self.scaling_time = self.input.param("scaling_time", 30)
        self.polling_time = self.input.param("polling_time", 2)
        self.num_queries = self.input.param("num_queries", 50)
        self.query_workload_after_creation = self.input.param("query_workload_after_creation", False)
        self.query_workload_parallel_creation = self.input.param("query_workload_parallel_creation", True)
        self.autoscaling_with_queries = self.input.param("autoscaling_with_queries", False)
        self.ignore_cas_mismatach = self.input.param("ignore_cas_mismatach", False)

        self.stop_queries = False
        self.stop_monitoring_stats = False
        self.memory_queue = defaultdict(lambda: deque(maxlen=int(self.scaling_time/self.polling_time)))
        self.cpu_queue = defaultdict(lambda: deque(maxlen=int(self.scaling_time/self.polling_time)))
        self.running_avg_memory = {}
        self.running_avg_cpu = {}
        self.current_num_fts_nodes = 2
        self.max_num_fts_nodes = 2
        self.autoscaling_validations = {'scale_out': False, 'scale_in': False}
        self.scale_out_log_count = 0
        self.scale_in_log_count = 0
        self.autorebalance_log_count = 0
        self.start_node = None
        self.total_query_count = 0
        global_vars.system_event_logs = EventHelper()
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    @staticmethod
    def define_index_parameters_collection_related(container_type="bucket", scope=None, collection=None):
        if container_type == 'bucket':
            _type = "emp"
        else:
            index_collections = []
            if type(collection) is list:
                _type = []
                for c in collection:
                    _type.append(f"{scope}.{c}")
                    index_collections.append(c)
            else:
                _type = f"{scope}.{collection}"
                index_collections.append(collection)
        return _type

    def construct_plan_params(self):
        plan_params = {}
        plan_params['numReplicas'] = 0
        if self.index_replicas:
            plan_params['numReplicas'] = self.index_replicas
        plan_params['indexPartitions'] = self.num_index_partitions
        plan_params['indexPartitions'] = self.num_index_partitions
        return plan_params

    def init_input_servers(self, database):
        server = TestInputServer()
        server.ip = database.nebula
        server.port = '8091'
        server.services = "kv,n1ql,fts"
        server.rest_username = database.access_key
        server.rest_password = database.secret_key
        self.input.servers = [server]

    def test_sanity(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True, is_elixir=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()
            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=scope_name, collections=[collection_name], no_check=False)

            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            docs_indexed = fts_idx.get_indexed_doc_count()
            container_doc_count = self._num_of_docs_per_collection
            self.log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
            errors = []
            if docs_indexed == 0:
                errors.append(f"No docs were indexed for index {fts_idx.name}")
            if docs_indexed != container_doc_count:
                errors.append(f"Bucket doc count = {container_doc_count}, index doc count={docs_indexed}")

            fts_callable.delete_all()

    # Create custom map and update definition
    def create_custom_map_index_and_update_defn(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, doc_template="Employee",
                                num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True, is_elixir=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()
            fts_idx = fts_callable.create_fts_index("custom-idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=scope_name, collections=[collection_name], no_check=False)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            self.log.info("Editing custom index with new map...")
            fts_idx.generate_new_custom_map(seed=fts_idx.cm_id + 10, collection_index=True, type_mapping=_type)
            fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
            fts_idx.update()
            docs_indexed = fts_idx.get_indexed_doc_count()
            container_doc_count = self._num_of_docs_per_collection
            self.log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
            errors = []
            if docs_indexed == 0:
                errors.append(f"No docs were indexed for index {fts_idx.name}")
            if docs_indexed != container_doc_count:
                errors.append(f"Bucket doc count = {container_doc_count}, index doc count={docs_indexed}")
            fts_callable.delete_all()

    # Run fts rest based query and expect results
    def run_fts_rest_based_queries(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, doc_template="emp",
                                num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True, is_elixir=True)
            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()
            index = fts_callable.create_fts_index("index", source_type='couchbase',
                                                  source_name=database.id, index_type='fulltext-index',
                                                  index_params=None, plan_params=plan_params,
                                                  source_params=None, source_uuid=None, collection_index=True,
                                                  _type=_type, analyzer="standard",
                                                  scope=scope_name, collections=[collection_name], no_check=False)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            if fts_callable.es:
                fts_callable.create_es_index_mapping(index.es_custom_map,
                                                     index.index_definition)
            fts_callable.run_query_and_compare(index, self.num_queries)
            fts_callable.delete_all()

    # DB with width 'x' and weight 'y', create FTS index, verify replicas are in 2 different nodes and server group
    def override_database_verify_nodes(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            self.override_width_and_weight(database.id, 2, 60)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True, is_elixir=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()
            idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                source_name=database.id, index_type='fulltext-index',
                                                index_params=None, plan_params=plan_params,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="standard",
                                                scope=scope_name, collections=[collection_name], no_check=False)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            index_resp = fts_callable.check_if_index_exists(idx.name, bucket_name=database.id, scope_name=scope_name, node_def=True)
            self.log.info(f"Total no. of index's node and server group : {len(index_resp)}")
            self.assertEqual(len(index_resp), 2)
            fts_callable.delete_all()

    # Create fts index on multiple collections
    def create_index_multiple_collections(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            collection_arr = []
            for i in range(5):
                collection_name = f'db_{counter}_collection_{i}'
                self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
                self.load_databases(load_all_databases=False, num_of_docs=self._num_of_docs_per_collection,
                                    database_obj=database, scope=scope_name, collection=collection_name)
                collection_arr.append(collection_name)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_arr)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_arr, collection_index=True, is_elixir=True)
            plan_params = self.construct_plan_params()
            fts_callable.create_fts_index("idx", source_type='couchbase',
                                          source_name=database.id, index_type='fulltext-index',
                                          index_params=None, plan_params=plan_params,
                                          source_params=None, source_uuid=None, collection_index=True,
                                          _type=_type, analyzer="standard",
                                          scope=scope_name, collections=[collection_arr], no_check=False)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            fts_callable.delete_all()

    # Delete collections and verify all indexes get deleted
    def delete_collections_check_index_delete(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            collection_arr = []
            for i in range(2):
                collection_name = f'db_{counter}_collection_{i}'
                self.create_collection(database_obj=database, scope_name=scope_name,
                                       collection_name=collection_name)
                self.load_databases(load_all_databases=False, num_of_docs=self._num_of_docs_per_collection,
                                    database_obj=database, scope=scope_name, collection=collection_name)
                collection_arr.append(collection_name)
            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_arr)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_arr, collection_index=True, is_elixir=True)
            plan_params = self.construct_plan_params()
            idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                source_name=database.id, index_type='fulltext-index',
                                                index_params=None, plan_params=plan_params,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="standard",
                                                scope=scope_name, collections=[collection_arr], no_check=False)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            resp = fts_callable.check_if_index_exists(idx.name, bucket_name=database.id, scope_name=scope_name, node_def=True)
            if resp:
                self.log.info(f"Index Definition before collection deletion : {resp}")
            for collection_name in collection_arr:
                self.delete_collection(database, scope_name, collection_name)
            self.log.info(f"Waiting for collection to get permanently deleted")
            time.sleep(100)
            resp = fts_callable.check_if_index_exists(idx.name, bucket_name=database.id, scope_name=scope_name, node_def=True)
            if not resp:
                self.log.info("Index not found !")
            else:
                self.log.error(f"Index Definition still exists after collection deletion : {resp}")
                self.fail("Index Definition still exists after collection deletion")

            fts_callable.delete_all()

    # Delete some database and check if only those fts indexes are deleted.
    def delete_some_database_check_index_delete(self):
        self.provision_databases(self.num_databases)
        indexes_arr = []
        random_database_list = [0] if list(range(0, self.num_databases - 1)) == [] else list(range(0, self.num_databases - 1))
        random_variable = 1 if len(random_database_list) == 1 else random.randint(1, len(random_database_list) - 1)
        database_random_indexes = random.sample(random_database_list, random_variable)
        print(database_random_indexes, "Random databases chosen for deletion")
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, doc_template="Employee",
                                num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True,  is_elixir=True)
            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()
            for i in range(random.randint(0, 2)):
                idx = fts_callable.create_fts_index(f"index_{i}", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
                if counter in database_random_indexes:
                    indexes_arr.append(
                        [fts_callable, idx.name, database.id, scope_name])

        index_count = 0
        for i, index in enumerate(indexes_arr):
            if index[0].check_if_index_exists(index[1], bucket_name=index[2], scope_name=index[3], node_def=True):
                self.log.info(f"{i}.Index {index[1]} -> Present before deletion")

        for index in indexes_arr:
            self.delete_database(index[2])

        for index in indexes_arr:
            if index[0].check_if_index_exists(index[1], bucket_name=index[2], scope_name=index[3], node_def=True):
                self.fail(f"Index {index[1]} exists even after DB deletion")
            else:
                self.log.info(f" Index {index[1]} -> Not Found after deletion")
                index_count += 1
        self.assertEqual(len(indexes_arr), index_count, "Index exists even after DB deletion")

    # Delete all database and check if fts indexes are deleted.
    def delete_all_database_check_index_delete(self):
        self.provision_databases(self.num_databases)
        indexes_arr = []
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, doc_template="Employee",
                                num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True,  is_elixir=True)
            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()
            for i in range(random.randint(1, 3)):
                idx = fts_callable.create_fts_index(f"index_{i}", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
                indexes_arr.append([fts_callable, idx.name, database.id, scope_name])

        index_count = 0
        for i, index in enumerate(indexes_arr):
            if index[0].check_if_index_exists(index[1], bucket_name=index[2], scope_name=index[3], node_def=True):
                self.log.info(f"{i}. Index {index[1]} -> Present before deletion")
        self.delete_all_database()
        for index in indexes_arr:
            if index[0].check_if_index_exists(index[1], bucket_name=index[2], scope_name=index[3], node_def=True):
                self.fail(f"Index {index[1]} exists even after DB deletion")
            else:
                self.log.info(f" Index {index[1]} -> Not Found after deletion")
                index_count += 1
        self.assertEqual(len(indexes_arr), index_count, "Index exists even after DB deletion")

    # Create a FTS index and delete.And recreate the same index name.
    def recreate_index_same_name(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False,
                                num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True, is_elixir=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()

            idx = fts_callable.create_fts_index("index", source_type='couchbase',
                                                source_name=database.id, index_type='fulltext-index',
                                                index_params=None, plan_params=plan_params,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="standard",
                                                scope=scope_name, collections=[collection_name], no_check=False)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            fts_callable.delete_fts_index(idx.name)
            try:
                fts_callable.create_fts_index("index", source_type='couchbase',
                                              source_name=database.id, index_type='fulltext-index',
                                              index_params=None, plan_params=plan_params,
                                              source_params=None, source_uuid=None, collection_index=True,
                                              _type=_type, analyzer="standard",
                                              scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
                print("Recreating FTS Index passed!")
                fts_callable.delete_all()
            except Exception as e:
                fts_callable.delete_all()
                AssertionError(str(e),
                               f"rest_create_index: error creating index: , err: manager_api: cannot create/update index because an index with the same name already exists: index")
                self.fail("Recreating fts index with the same name failed")

    # Verify that you will not be able to create indexes with more than 1 partition and 1 replica (In both rest and UI)
    def create_simple_default_index_partition_check(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, doc_template="Employee",
                                num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True, is_elixir=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()
            try:
                fts_callable.create_fts_index("index", source_type='couchbase',
                                              source_name=database.id, index_type='fulltext-index',
                                              index_params=None, plan_params=plan_params,
                                              source_params=None, source_uuid=None, collection_index=True,
                                              _type=_type, analyzer="standard",
                                              scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
                self.fail(
                    "Testcase failed: support for indexes with 1 active + 1 replica partitions only in serverless mode")
            except Exception as e:
                print(
                    "Testcase Passed : support for indexes with 1 active + 1 replica partitions only in serverless mode")
                AssertionError(str(e),
                               "limitIndexDef: support for indexes with 1 active + 1 replica partitions only in serverless mode")
            fts_callable.delete_all()

    # Creating more than 20 indexes per bucket should fail
    def create_max_20index_per_bucket(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, doc_template="Employee",
                                num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True, is_elixir=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()

            for i in range(1, 21):
                fts_callable.create_fts_index(f"index_{i}", source_type='couchbase',
                                              source_name=database.id, index_type='fulltext-index',
                                              index_params=None, plan_params=plan_params,
                                              source_params=None, source_uuid=None, collection_index=True,
                                              _type=_type, analyzer="standard",
                                              scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            # 21st fts-index
            try:
                fts_callable.create_fts_index(f"index_{21}", source_type='couchbase',
                                              source_name=database.id, index_type='fulltext-index',
                                              index_params=None, plan_params=plan_params,
                                              source_params=None, source_uuid=None, collection_index=True,
                                              _type=_type, analyzer="standard",
                                              scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
                self.fail(
                    "Testcase failed: support for indexes with 1 active + 1 replica partitions only in serverless mode")
            except Exception as e:
                print(
                    "Testcase Passed : support for indexes with 1 active + 1 replica partitions only in serverless mode")
                AssertionError(str(e),
                               "Testcase Passed : support for indexes with 1 active + 1 replica partitions only in serverless mode")

            fts_callable.delete_all()

    def create_alias_index_and_validate_query(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, doc_template="emp",
                                num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True, is_elixir=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()

            index = fts_callable.create_fts_index("index", source_type='couchbase',
                                                  source_name=database.id, index_type='fulltext-index',
                                                  index_params=None, plan_params=plan_params,
                                                  source_params=None, source_uuid=None, collection_index=True,
                                                  _type=_type, analyzer="standard",
                                                  scope=scope_name, collections=[collection_name], no_check=False)
            alias = fts_callable.create_alias([index], bucket=database.id, scope=scope_name)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)

            hits, _, _, _ = index.execute_query(fts_callable.sample_query,
                                                zero_results_ok=False)
            hits2, _, _, _ = alias.execute_query(fts_callable.sample_query,
                                                 zero_results_ok=False)
            if hits != hits2:
                self.fail("Index query yields {0} hits while alias on same index yields only {1} hits".format(hits, hits2))
            else:
                self.log.info(f"Hits by FTS Index : {hits}, Hits by FTS Alias Index : {hits2}")

            fts_callable.delete_all()

    def check_scale_out_condition(self, stats):
        """
            Returns True if scale out condition is satisfied.
        """
        mem_util = int(float(self.LWM_limit) * int(stats[0]['limits:memoryBytes']))
        cpu_util = int(float(self.LWM_limit) * 100)
        lim_util, lim_cpu = True, True
        for running_avg_memory in self.running_avg_memory.values():
            if running_avg_memory < mem_util:
                lim_util = False

        for running_avg_cpu in self.running_avg_cpu.values():
            if running_avg_cpu < cpu_util:
                lim_cpu = False

        return lim_cpu | lim_util

    def check_scale_in_condition(self, stats):
        """
            Returns True if scale in condition is satisfied.
        """
        mem_util = int(float(self.UWM_limit) * int(stats[0]['limits:memoryBytes']))
        cpu_util = int(float(self.UWM_limit) * 100)
        for running_avg_memory in self.running_avg_memory.values():
            if running_avg_memory > mem_util:
                return False

        for running_avg_cpu in self.running_avg_cpu.values():
            if running_avg_cpu > cpu_util:
                return False
        return True

    def check_auto_rebalance_condition(self, stats):
        """
            Returns True if auto rebalance condition is satisfied.
        """
        mem_util = int(float(self.HWM_limit) * int(stats[0]['limits:memoryBytes']))
        cpu_util = int(float(self.HWM_limit) * 100)
        for running_avg_memory in self.running_avg_memory.values():
            if running_avg_memory >= mem_util:
                return True

        for running_avg_cpu in self.running_avg_cpu.values():
            if running_avg_cpu >= cpu_util:
                return True
        return False

    def get_fts_stats(self, print_str=None, print_stats=True):
        """
            Print FTS Stats and returns stats along with Current FTS Nodes
        """
        fts_nodes = self.get_fts_nodes()
        creds = self.get_bypass_credentials()
        stats,  cfg_stats = FTSCallable(self.input.servers).get_fts_autoscaling_stats(fts_nodes, creds)
        if print_stats:
            self.prettyPrintStats(stats, fts_nodes, print_str)

        if cfg_stats is not None:
            self.print_fts_cfg_stats(cfg_stats)
        return stats, fts_nodes

    def get_fts_defrag_stats(self, print_str):
        """
            Print FTS Stats and returns stats along with Current FTS Nodes
        """
        resp = FTSCallable(self.input.servers).get_fts_defrag_stats(self.start_node, self.get_bypass_credentials())
        count = 0
        myTable = PrettyTable(["S.No", "FTS Nodes", "util_memoryBytes", "util_cpuPercent", "util_diskByte"])
        for node, stat in resp.items():
            myTable.add_row([count+1, node, stat['memoryBytes'], stat['cpuPercent'], stat['diskBytes']])
        if print_str:
            self.log.info(print_str)
        print(myTable)

    def prettyPrintStats(self, stats, fts_nodes, print_str=None):
        myTable = PrettyTable(["S.No", "FTS Nodes", "lim_memoryBytes", "util_memoryBytes", "util_cpuPercent", "util_diskByte"])
        for count, stat in enumerate(stats):
            myTable.add_row([count+1, fts_nodes[count], stat['limits:memoryBytes'], stat['utilization:memoryBytes'], stat['utilization:cpuPercent'], stat['utilization:diskBytes']])
        if print_str:
            self.log.info(print_str)
        print(myTable)

    def prettyPrintRunningAverage(self, print_str=None):
        myTable = PrettyTable(["S.No", "FTS Node", "30 Min Mem Avg", "30 Min CPU Avg"])
        for count, node in enumerate(self.memory_queue):
            myTable.add_row([count+1, node, self.running_avg_memory[node], self.running_avg_cpu[node]])
        if print_str is None:
            self.log.info("FTS Running Average Stats")
        else:
            self.log.info(print_str)
        print(myTable)

    def print_fts_cfg_stats(self, cfg_stats):
        definitions = cfg_stats['nodeDefsWanted']['nodeDefs']
        myTable = PrettyTable(["S.No", "FTS Node", "nodeDefID"])
        for count, nodeDef in enumerate(definitions):
            myTable.add_row([count+1, definitions[nodeDef]['hostPort'], nodeDef])
        self.log.info("FTS CFG STATS")
        print(myTable)

    def verify_HWM_index_rejection(self, stats):
        """
           Returns True if HWM is responsible for index rejection
        """
        mem_util = int(float(self.HWM_limit) * int(stats[0]['limits:memoryBytes']))
        cpu_util = int(float(self.HWM_limit) * 100)
        count = 0
        for stat in stats:
            if stat['utilization:memoryBytes'] >= mem_util or stat['utilization:cpuPercent'] >= cpu_util:
                count += 1

        if len(stats) - count < 2:
            return True

        return False

    def exempt_index_rejection_due_to_failed_stats(self, stats):
        for stat in stats:
            if stat['utilization:memoryBytes'] == 0 or stat['utilization:cpuPercent'] == 0:
                return True
        return False

    def generate_summary(self, summary, fts_stats, fts_nodes, variables, name="Default", time_taken=0):
        """
             Generates tabulated report for autoscaling test
         """
        mem_arr = []
        for i in range(len(fts_stats)):
            node_name = str(fts_nodes[i]).strip(".sandbox.nonprod-project-avengers.com")
            mem_util = (str(int(fts_stats[i]['utilization:memoryBytes'])/pow(10, 9)) + "GB").strip()
            mem_arr.append(f"{node_name} - {mem_util}")
        obj = {
            "Detail": name,
            "Time": str(datetime.now().replace(microsecond=0)),
            "Memory Status": mem_arr,
            "Moving Average Mem": variables['moving_average_mem'],
            "Moving Average CPU": variables['moving_average_cpu'],
            "Scale Out Count ": variables['scale_out_count'],
            "Scale Out Time": time_taken,
            "HWM Index Rejection": True if variables['HWM_hit'] == 1 else False
        }
        summary.append(obj)

    def start_query_workload(self, fts_callable, index, num_queries):
        while not self.stop_queries:
            fts_callable.run_query_and_compare(index, num_queries)
            self.total_query_count += num_queries

    def monitor_fts_stats(self):
        _, nodes = self.get_fts_stats(print_stats=False)
        self.start_node = nodes[0]
        while not self.stop_monitoring_stats:
            self.get_fts_stats("---FTS STATISTICS---")
            time.sleep(30)

    def monitor_fts_defrag(self):
        if self.start_node is None:
            _, nodes = self.get_fts_stats(print_stats=False)
            self.start_node = nodes[0]
        while not self.stop_monitoring_stats:
            self.get_fts_defrag_stats("---FTS DEFRAG STATISTICS---")
            time.sleep(int(self.polling_time*60))

    def validate_scale_out(self, fts_nodes):
        if len(fts_nodes) == self.current_num_fts_nodes + 2:
            self.max_num_fts_nodes = len(fts_nodes)
            self.current_num_fts_nodes = len(fts_nodes)
            self.autoscaling_validations['scale_out'] = True
            return True
        else:
            return False

    def validate_scale_in(self, fts_nodes):
        if len(fts_nodes) == self.current_num_fts_nodes - 2:
            self.current_num_fts_nodes = len(fts_nodes)
            self.autoscaling_validations['scale_in'] = True
            return True
        else:
            return False

    """
        Following validations have been covered in the autoscaling test 
            1. Scale Out failed
            2. scale out happened when it wasn't required
            3. Scale in failed
            4. AutoRebalance Failed
            5. Index {index.name} created even though HWM had been satisfied
            6. Index Creation Failed 
            7. Query failed abruptly, reason : 
    """
    def monitor_running_average_and_scaling(self):
        while not self.stop_monitoring_stats:
            invalid_stat_present = False
            start_time = time.time()
            fts_stats, fts_nodes = self.get_fts_stats(print_stats=False)
            for i in range(len(fts_nodes)):
                util_mem = fts_stats[i]['utilization:memoryBytes']
                util_cpu = fts_stats[i]['utilization:cpuPercent']
                if util_cpu == 0 or util_mem == 0:
                    invalid_stat_present = True
                self.memory_queue[fts_nodes[i]].append(util_mem)
                self.cpu_queue[fts_nodes[i]].append(util_cpu)

                self.running_avg_memory[fts_nodes[i]] = sum(self.memory_queue[fts_nodes[i]])/len(self.memory_queue[fts_nodes[i]])
                self.running_avg_cpu[fts_nodes[i]] = sum(self.cpu_queue[fts_nodes[i]])/len(self.cpu_queue[fts_nodes[i]])

            self.prettyPrintRunningAverage()

            # validate scale out
            if self.check_scale_out_condition(fts_stats) or self.scale_out_log_count > 0:
                if self.scale_out_log_count == 0:
                    self.log.info("Scale Out Satisfied using Running Average")
                self.scale_out_log_count += 1
                with self.subTest("validate scale out"):
                    if self.validate_scale_out(fts_nodes):
                        self.log.info("Scale out successful")
                        self.scale_out_log_count = 0
                    else:
                        if self.scale_out_log_count == 5:
                            self.log.critical("Scale out failed")
                            self.prettyPrintRunningAverage("--SCALE OUT FAIL STATS--")
                            self.fail(f"Scale Out failed")
            else:
                if not invalid_stat_present:
                    with self.subTest("verify scale out before running average"):
                        if self.validate_scale_out(fts_nodes):
                            self.log.critical("scale out happened when it wasn't required")
                            self.prettyPrintRunningAverage("EARLY SCALE OUT STATS")
                            self.fail(f"Scale Out happened less than scaling time")

            # verify scale in
            if self.check_scale_in_condition(fts_stats) and self.autoscaling_validations['scale_out']:
                self.scale_in_log_count += 1
                with self.subTest("Verify Scale In"):
                    if self.validate_scale_in(fts_nodes):
                        self.log.info("Scale in successful")
                        self.scale_in_log_count = 0
                    else:
                        if self.scale_in_log_count == 5:
                            self.log.critical("Scale in failed")
                            self.prettyPrintRunningAverage("SCALE IN FAIL STATS")
                            self.fail(f"Scale in failed")

            if self.check_auto_rebalance_condition(fts_stats):
                self.autorebalance_log_count += 1
                with self.subTest("Verify AutoRebalance"):
                    rebalance_status = FTSCallable(self.input.servers).get_fts_rebalance_status(self.start_node)
                    if rebalance_status == "none" and self.autorebalance_log_count == 5:
                        self.log.critical("AutoRebalance Failed")
                        self.prettyPrintRunningAverage("-- AUTOREBALANCE FAIL STATS--")
                        self.fail(f"BUG : AutoRebalance Failed..")
                    else:
                        self.log.info("PASS : AutoRebalance Passed")
                        self.autorebalance_log_count = 0

            end_time = time.time()
            time_taken = end_time - start_time
            total_sleep_time = max(0, int(int(self.polling_time*60) - time_taken))
            time.sleep(int(total_sleep_time))

    def create_fts_indexes_for_autoscaling(self, all_info, index_no, db_no, index_arr):
        fts_stats, fts_nodes = self.get_fts_stats(print_stats=False)
        index = None
        try:
            plan_params = self.construct_plan_params()
            index = all_info[db_no]['fts_callable'].create_fts_index(f"counter_{db_no + 1}_idx_{index_no + 1}", source_type='couchbase',
                                                                     source_name=all_info[db_no]['database'].id, index_type='fulltext-index',
                                                                     index_params=None, plan_params=plan_params,
                                                                     source_params=None, source_uuid=None, collection_index=True,
                                                                     _type=all_info[db_no]['type'], analyzer="standard",
                                                                     scope=all_info[db_no]['scope_name'], collections=[all_info[db_no]['collection_name']], no_check=False)
            all_info[db_no]['fts_callable'].wait_for_indexing_complete(self._num_of_docs_per_collection, complete_wait=False, idx=index)
            index_arr.append([all_info[db_no]['fts_callable'], index])
            if self.query_workload_parallel_creation:
                query_thread = threading.Thread(target=self.start_query_workload, kwargs={'fts_callable': all_info[db_no]['fts_callable'], 'index': index, 'num_queries': self.num_queries})
                query_thread.start()

            if not self.exempt_index_rejection_due_to_failed_stats(fts_stats):
                with self.subTest("Verifying Index Rejection for HWM"):
                    if self.verify_HWM_index_rejection(fts_stats):
                        fts_stats_2, _ = self.get_fts_stats(print_stats=False)
                        if self.verify_HWM_index_rejection(fts_stats_2):
                            self.log.critical(f"Index {index.name} created even though HWM had been satisfied")
                            self.prettyPrintStats(fts_stats, fts_nodes, f"STATS before Index {index.name} creation (index created even though HWM had been satisfied)")
                            self.prettyPrintRunningAverage(f"Running stats before Index {index.name} creation (index created even though HWM had been satisfied)")
                            self.prettyPrintStats(fts_stats_2, fts_nodes, f"STATS after Index {index.name} creation (index created even though HWM had been satisfied)")
                            self.prettyPrintRunningAverage(f"Running stats after Index {index.name} creation (index created even though HWM had been satisfied))")
                            self.fail(f"BUG : Index {index.name} created even though HWM had been satisfied")
        except Exception as e:
            if str(e).find("limiting/throttling: the request has been rejected according to regulator") != -1:
                self.log.info("Index creation reject exempted due to limiting/throttling")
            elif str(e).find("RetryOnCASMismatch: too many tries") != -1 and self.ignore_cas_mismatach:
                self.log.critical(f"Index Creation Failure ignored due to CAS Mismatch : {str(e)}")
            else:
                with self.subTest("Index Creation Fail Check"):
                    index_check = False
                    for i in range(2):
                        if i == 1:
                            fts_stats, fts_nodes = self.get_fts_stats(print_stats=False)
                        if self.verify_HWM_index_rejection(fts_stats):
                            self.assertTrue("limitIndexDef: Cannot accommodate index request" in str(e))
                            self.log.info(f"PASS : HWM Index Rejection Passed : {str(e)}")
                            index_check = True
                            break
                    if not index_check:
                        self.log.critical(f"Index Creation Failed - for index {index.name} \n ERROR : {str(e)}")
                        self.prettyPrintStats(fts_stats, fts_nodes,"Index Creation Failed")
                        self.prettyPrintStats(fts_stats, fts_nodes, f"STATS before Index {index.name} creation failed")
                        self.prettyPrintRunningAverage(f"Running stats before Index {index.name} creation failed")
                        self.prettyPrintStats(fts_stats_2, fts_nodes, f"STATS after Index {index.name} creation failed")
                        self.prettyPrintRunningAverage(f"Running stats after Index {index.name} creation failed")
                        self.fail(f"BUG : Index {index.name} Creation Failed")

    def test_fts_autoscaling(self):
        self.provision_databases(self.num_databases, None, self.new_dataplane_id)

        for counter, database in enumerate(self.databases.values()):
            self.init_input_servers(database)
            break

        monitor_stat_thread = threading.Thread(target=self.monitor_fts_stats)
        monitor_avg_thread = threading.Thread(target=self.monitor_running_average_and_scaling)
        monitor_defrag_stat_thread = threading.Thread(target=self.monitor_fts_defrag)

        monitor_stat_thread.start()
        monitor_avg_thread.start()
        monitor_defrag_stat_thread.start()
        time.sleep(120)

        all_info = []
        index_arr = []
        # Create 20 databases with scopes and collections
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name,
                                doc_template="emp")
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True,  is_elixir=True, reduce_query_logging=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            all_info.append({'database': database, 'type': _type, 'scope_name': scope_name,
                             'collection_name': collection_name, 'fts_callable': fts_callable})

        # Launch a thread in each database to create an index
        for index in range(self.num_indexes):
            thread_arr = []
            for db in range(self.num_databases):
                thread = threading.Thread(target=self.create_fts_indexes_for_autoscaling,
                                          kwargs={'all_info': all_info, 'index_no': index, 'db_no': db,
                                                  'index_arr': index_arr})
                thread.start()
                time.sleep(2)
                thread_arr.append(thread)

            for thread in thread_arr:
                thread.join()

        if self.query_workload_after_creation:
            thread_2_array = []
            for index in index_arr:
                thread2 = threading.Thread(target=index[0].run_query_and_compare,
                                           kwargs={'index': index[1], 'num_queries': self.num_queries})
                thread2.start()
                thread_2_array.append(thread2)

            for thread2 in thread_2_array:
                thread2.join()

        self.stop_queries = True

        self.log.info("Sleeping 60 minutes to observe scale in")
        time.sleep(3600)

        self.stop_monitoring_stats = True
        time.sleep(100)
        self.log.info(f" {self.total_query_count} ran during this test.")
        self.log.info("============== collecting cbcollect logs... ==========================")
        self.collect_log_on_dataplane_nodes()

        if not self.autoscaling_validations['scale_out']:
            self.fail("Scale out wasn't observed in this test")

        if not self.autoscaling_validations['scale_in']:
            self.fail("Scale in wasn't observed in this test")

        self.trigger_log_collect = False

    def test_n1ql_search(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True, is_elixir=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()
            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="standard",
                                                    scope=scope_name, collections=[collection_name], no_check=False)

            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)

            docs_indexed = fts_idx.get_indexed_doc_count()
            container_doc_count = self._num_of_docs_per_collection
            self.log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
            # Test the two string options, then test a full text search
            with self.subTest('search_string'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: search_string")
                explain_result = self.run_query(database,
                                                f'EXPLAIN SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE SEARCH(t1.country, "+Slovakia +(Slovak +Republic)") ORDER BY t1.streetAddress limit 5')
                self.assertTrue('IndexFtsSearch' in str(explain_result), f"The query is not using an fts search! please check explain {explain_result}")
                self.assertTrue('idx' in str(explain_result), f"The query is not using the fts index! please check explain {explain_result}")

                n1ql_search_result = self.run_query(database,
                                                    f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE SEARCH(t1.country, "+Slovakia +(Slovak +Republic)") ORDER BY t1.streetAddress limit 5')
                expected_result = self.run_query(database,
                                                 f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE t1.country = "Slovakia (Slovak Republic)" ORDER BY t1.streetAddress limit 5')
                # Ensure that the n1ql equivalent returns results, then compare the search to the n1ql equivalent
                self.assertFalse(expected_result['metrics']['resultCount'] == 0, "This query should return results!")
                diffs = DeepDiff(n1ql_search_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            with self.subTest('search_string_option2'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: search_string_option2")
                explain_result = self.run_query(database,
                                                f'EXPLAIN SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE SEARCH(t1, "country:\\"Slovakia (Slovak Republic)\\"") ORDER BY t1.streetAddress limit 5')
                self.assertTrue('IndexFtsSearch' in str(explain_result), f"The query is not using an fts search! please check explain {explain_result}")
                self.assertTrue('idx' in str(explain_result), f"The query is not using the fts index! please check explain {explain_result}")
                n1ql_search_result = self.run_query(database,
                                                    f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE SEARCH(t1, "country:\\"Slovakia (Slovak Republic)\\"") ORDER BY t1.streetAddress limit 5')
                expected_result = self.run_query(database,
                                                 f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE t1.country = "Slovakia (Slovak Republic)" ORDER BY t1.streetAddress limit 5')
                # Ensure that the n1ql equivalent returns results, then compare the search to the n1ql equivalent
                self.assertFalse(expected_result['metrics']['resultCount'] == 0, "This query should return results!")
                diffs = DeepDiff(n1ql_search_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            with self.subTest('full_text_search'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: full_text_search")
                n1ql_search_query = {"explain": False, "fields": ["*"],"highlight": {},
                                     "query": {"match": "Algeria",
                                               "field": "country",
                                               "analyzer": "standard"},
                                     "sort": [{"by" : "field",
                                               "field" : "streetAddress"}]
                                     }
                explain_result = self.run_query(database,
                                                f'EXPLAIN SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE SEARCH(t1, {n1ql_search_query}) and t1.age > 50 ORDER BY t1.streetAddress')
                self.assertTrue('IndexFtsSearch' in str(explain_result), f"The query is not using an fts search! please check explain {explain_result}")
                self.assertTrue('idx' in str(explain_result), f"The query is not using the fts index! please check explain {explain_result}")

                n1ql_search_result = self.run_query(database,
                                                    f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE SEARCH(t1, {n1ql_search_query}) and t1.age > 50 ORDER BY t1.streetAddress')
                expected_result = self.run_query(database,
                                                 f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE t1.country = "Algeria" and t1.age > 50 ORDER BY t1.streetAddress')
                # Ensure that the n1ql equivalent returns results, then compare the search to the n1ql equivalent
                self.assertFalse(expected_result['metrics']['resultCount'] == 0, "This query should return results!")
                diffs = DeepDiff(n1ql_search_result['results'], expected_result['results'])
                if diffs:
                    self.assertTrue(False, diffs)
            fts_callable.delete_all()

    def test_n1ql_flex(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            self.init_input_servers(database)
            fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope_name,
                                       collections=collection_name, collection_index=True, is_elixir=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()
            fts_idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                    source_name=database.id, index_type='fulltext-index',
                                                    index_params=None, plan_params=plan_params,
                                                    source_params=None, source_uuid=None, collection_index=True,
                                                    _type=_type, analyzer="keyword",
                                                    scope=scope_name, collections=[collection_name], no_check=False)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            docs_indexed = fts_idx.get_indexed_doc_count()
            container_doc_count = self._num_of_docs_per_collection
            self.log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
            with self.subTest('flex using fts'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: flex using fts")
                explain_result = self.run_query(database,
                                                f'EXPLAIN SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 USE INDEX (USING FTS, USING GSI) WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                self.assertTrue('IndexFtsSearch' in str(explain_result), f"The query is not using an fts search! please check explain {explain_result}")
                self.assertTrue('idx' in str(explain_result), f"The query is not using the fts index! please check explain {explain_result}")
                flex_result = self.run_query(database,
                                             f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 USE INDEX (USING FTS, USING GSI) WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                expected_result = self.run_query(database,
                                                 f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                self.assertFalse(expected_result['metrics']['resultCount'] == 0, "This query should return results!")
                diffs = DeepDiff(flex_result['results'], expected_result['results'])
                if diffs:
                    self.assertTrue(False, diffs)
            with self.subTest('flex using gsi'):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: flex using gsi")
                # Create a gsi index
                self.run_query(database, f"CREATE INDEX idx1 on {scope_name}.{collection_name}(country,streetAddress)")
                time.sleep(20)
                try:
                    explain_result = self.run_query(database,
                                                    f'EXPLAIN SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 USE INDEX (USING FTS, USING GSI) WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                    # We expect the gsi index and the fts index both to be used
                    self.assertTrue('IndexFtsSearch' in str(explain_result), f"The query should be using the fts search! please check explain {explain_result}")
                    self.assertTrue(f'idx1' in str(explain_result), f"The query is not using the gsi index! please check explain {explain_result}")
                    self.assertTrue('idx' in str(explain_result), f"The query is not using the fts index! please check explain {explain_result}")
                    flex_result = self.run_query(database,
                                                 f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 USE INDEX (USING FTS, USING GSI) WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                    expected_result = self.run_query(database,
                                                     f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                    self.assertFalse(expected_result['metrics']['resultCount'] == 0, "This query should return results!")
                    diffs = DeepDiff(flex_result['results'], expected_result['results'])
                    if diffs:
                        self.assertTrue(False, diffs)
                finally:
                    self.run_query(database, f'DROP INDEX idx1 on {scope_name}.{collection_name}')
            with self.subTest("flex using gsi keyword"):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: flex using gsi keyword")
                self.run_query(database, f"CREATE INDEX idx1 on {scope_name}.{collection_name}(country,streetAddress)")
                time.sleep(20)
                try:
                    explain_result = self.run_query(database,
                                                    f'EXPLAIN SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 USE INDEX (USING GSI) WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                    # We expect the gsi index and the fts index both to be used
                    self.assertTrue(f'idx1' in str(explain_result), f"The query is not using the gsi index! please check explain {explain_result}")
                    flex_result = self.run_query(database,
                                                 f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 USE INDEX (USING GSI) WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                    expected_result = self.run_query(database,
                                                     f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                    self.assertFalse(expected_result['metrics']['resultCount'] == 0, "This query should return results!")
                    diffs = DeepDiff(flex_result['results'], expected_result['results'])
                    if diffs:
                        self.assertTrue(False, diffs)
                finally:
                    self.run_query(database, f'DROP INDEX idx1 on {scope_name}.{collection_name}')
            with self.subTest("flex using fts keyword"):
                self.log.info("="*40)
                self.log.info("===== SUBTEST: flex using fts keyword")
                self.run_query(database, f"CREATE INDEX idx1 on {scope_name}.{collection_name}(country,streetAddress)")
                time.sleep(20)
                try:
                    explain_result = self.run_query(database,
                                                    f'EXPLAIN SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 USE INDEX (USING FTS) WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                    # We expect the gsi index and the fts index both to be used
                    self.assertTrue('IndexFtsSearch' in str(explain_result), f"The query should be using the fts search! please check explain {explain_result}")
                    self.assertTrue('idx' in str(explain_result), f"The query is not using the fts index! please check explain {explain_result}")
                    flex_result = self.run_query(database,
                                                 f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 USE INDEX (USING FTS) WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                    expected_result = self.run_query(database,
                                                     f'SELECT t1.age,t1.city,t1.country,t1.firstName,t1.lastName,t1.streetAddress,t1.suffix,t1.title from {scope_name}.{collection_name} as t1 WHERE t1.country = "Algeria" ORDER BY t1.streetAddress limit 5')
                    self.assertFalse(expected_result['metrics']['resultCount'] == 0, "This query should return results!")
                    diffs = DeepDiff(flex_result['results'], expected_result['results'])
                    if diffs:
                        self.assertTrue(False, diffs)
                finally:
                    self.run_query(database, f'DROP INDEX idx1 on {scope_name}.{collection_name}')
            fts_callable.delete_all()

    def fts_security_test(self):
        """
            Scenario : Single user creates 2 DBs/tenants and user cannot create indexes on one DB with auth of another DB
        """
        self.provision_databases(count=2)
        db1, creds1, type1, scope1, collection1 = None, None, None, None, None
        db2, creds2, type2, scope2, collection2 = None, None, None, None, None

        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, num_of_docs=self._num_of_docs_per_collection,
                                database_obj=database, scope=scope_name, collection=collection_name)
            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            if counter == 0:
                db1, type1, scope1, collection1 = database, _type, scope_name, collection_name
            else:
                db2, type2, scope2, collection2 = database, _type, scope_name, collection_name

        self.log.info(f"Credentials of DB1 -> {db1.id, db1.access_key, db1.secret_key}")
        self.log.info(f"Credentials of DB2 -> {db2.id, db2.access_key, db2.secret_key}")

        for count in range(2):
            plan_params = self.construct_plan_params()
            if count == 0:
                # Get the FTSIndex Object with the correct set of credentials
                self.init_input_servers(db1)
                fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope1,
                                           collections=collection1, collection_index=True, is_elixir=True)
                FTSIndex = fts_callable.generate_FTSIndex_info("idx", source_type='couchbase',
                                                                  source_name=db1.id, index_type='fulltext-index',
                                                                  index_params=None, plan_params=plan_params,
                                                                  source_params=None, source_uuid=None, collection_index=True,
                                                                  _type=type1, scope=scope1, collections=[collection1])

                # Change all the credentials to credentials of second DB
                self.init_input_servers(db2)
                fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope1,
                                           collections=collection1, collection_index=True, is_elixir=True)
                try:
                    # Create FTS Index with wrong updated credentials
                    fts_callable.create_fts_index("idx", source_type='couchbase',
                                                  source_name=db1.id, index_type='fulltext-index',
                                                  index_params=None, plan_params=plan_params,
                                                  source_params=None, source_uuid=None, collection_index=True,
                                                  _type=type1, analyzer="standard",
                                                  scope=scope1, collections=[collection1], no_check=False,
                                                  FTSIndex=FTSIndex)
                    self.fail("Security Breach : Index created with credentials of different database")
                except Exception as err:
                    self.log.error({str(err)})
                    self.assertTrue('Error creating index: b\'{"message":"Forbidden. User needs one of the following permissions","permissions":["write permission for the source."]}\'' in str(err))
            else:
                self.init_input_servers(db2)
                fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope2,
                                           collections=collection2, collection_index=True, is_elixir=True)
                # Get the FTSIndex Object with the correct set of credentials
                FTSIndex = fts_callable.generate_FTSIndex_info("idx", source_type='couchbase',
                                                                  source_name=db2.id, index_type='fulltext-index',
                                                                  index_params=None, plan_params=plan_params,
                                                                  source_params=None, source_uuid=None, collection_index=True,
                                                                  _type=type2, scope=scope2, collections=[collection2])

                # Change all the credentials to credentials of second DB
                self.init_input_servers(db1)
                fts_callable = FTSCallable(self.input.servers, es_validate=False, es_reset=False, scope=scope2,
                                           collections=collection2, collection_index=True, is_elixir=True)
                try:
                    # Create FTS Index with wrong updated credentials
                    fts_callable.create_fts_index("idx", source_type='couchbase',
                                                  source_name=db2.id, index_type='fulltext-index',
                                                  index_params=None, plan_params=plan_params,
                                                  source_params=None, source_uuid=None, collection_index=True,
                                                  _type=type2, analyzer="standard",
                                                  scope=scope2, collections=[collection2], no_check=False, FTSIndex=FTSIndex)
                    self.fail("Security Breach : Index created with credentials of different database")
                except Exception as err:
                    self.log.error({str(err)})
                    self.assertTrue('Error creating index: b\'{"message":"Forbidden. User needs one of the following permissions","permissions":["write permission for the source."]}\'' in str(err))
            fts_callable.delete_all()
