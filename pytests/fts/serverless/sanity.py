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
from collections import deque

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
        self.num_queries = self.input.param("num_queries", 20)
        self.query_workload_after_creation = self.input.param("query_workload_after_creation",False)
        self.query_workload_parallel_creation = self.input.param("query_workload_parallel_creation",True)
        self.autoscaling_with_queries = self.input.param("autoscaling_with_queries", False)
        self.stop_queries = False
        global_vars.system_event_logs = EventHelper()
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

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
                                       collections=collection_name, collection_index=True)

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
                                       collections=collection_name, collection_index=True)

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
                                       collections=collection_name, collection_index=True)
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
                                       collections=collection_name, collection_index=True)

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
            index_resp = fts_callable.check_if_index_exists(idx.name, node_def=True)
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
                                       collections=collection_arr, collection_index=True)
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
                                       collections=collection_arr, collection_index=True)
            plan_params = self.construct_plan_params()
            idx = fts_callable.create_fts_index("idx", source_type='couchbase',
                                                source_name=database.id, index_type='fulltext-index',
                                                index_params=None, plan_params=plan_params,
                                                source_params=None, source_uuid=None, collection_index=True,
                                                _type=_type, analyzer="standard",
                                                scope=scope_name, collections=[collection_arr], no_check=False)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            resp = fts_callable.check_if_index_exists(idx.name, index_def=True)
            if resp:
                self.log.info(f"Index Definition before collection deletion : {resp}")
            for collection_name in collection_arr:
                self.delete_collection(database, scope_name, collection_name)
            self.log.info(f"Waiting for collection to get permanently deleted")
            time.sleep(100)
            resp = fts_callable.check_if_index_exists(idx.name, index_def=True)
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
        database_random_indexes = random.sample(range(0, self.num_databases - 1),
                                                random.randint(1, self.num_databases))
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
                                       collections=collection_name, collection_index=True)
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
                        [fts_callable, idx.name, database.id])

        index_count = 0
        for i, index in enumerate(indexes_arr):
            if index[0].check_if_index_exists(index[1]):
                self.log.info(f"{i}.Index {index[1]} -> Present before deletion")

        for index in indexes_arr:
            self.delete_database(index[2])

        for index in indexes_arr:
            if index[0].check_if_index_exists(index[1]):
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
                                       collections=collection_name, collection_index=True)
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
                indexes_arr.append([fts_callable, idx.name])

        index_count = 0
        for i, index in enumerate(indexes_arr):
            if index[0].check_if_index_exists(index[1]):
                self.log.info(f"{i}. Index {index[1]} -> Present before deletion")
        self.delete_all_database()
        for index in indexes_arr:
            if index[0].check_if_index_exists(index[1]):
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
                                       collections=collection_name, collection_index=True)

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
                                       collections=collection_name, collection_index=True)

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
                                       collections=collection_name, collection_index=True)

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

    def check_scale_out_condition(self, stats):
        """
            Returns True if scale out condition is satisfied.
        """
        mem_util = int(float(self.LWM_limit) * int(stats[0]['limits:memoryBytes']))
        cpu_util = int(float(self.LWM_limit) * 100)
        lim_util, lim_cpu = True, True
        for stat in stats:
            if stat['utilization:memoryBytes'] < mem_util:
                lim_util = False

            if int(stat['utilization:cpuPercent']) < cpu_util:
                lim_cpu = False

        return lim_cpu | lim_util

    def check_scale_in_condition(self, stats):
        """
            Returns True if scale in condition is satisfied.
        """
        mem_util = int(float(self.UWM_limit) * int(stats[0]['limits:memoryBytes']))
        cpu_util = int(float(self.UWM_limit) * 100)
        for stat in stats:
            if stat['utilization:memoryBytes'] > mem_util or stat['utilization:cpuPercent'] > cpu_util:
                return False
        return True

    def check_auto_rebalance_condition(self, stats):
        """
            Returns True if auto rebalance condition is satisfied.
        """
        try:
            mem_util = int(float(self.HWM_limit) * int(stats[0]['limits:memoryBytes']))
            cpu_util = int(float(self.HWM_limit) * 100)
            for stat in stats:
                if stat['utilization:memoryBytes'] > mem_util:
                    self.log.info(f"HWM CHECK STATUS MEM: {stat['utilization:memoryBytes']}")
                    return True
                if stat['utilization:cpuPercent'] > cpu_util:
                    self.log.info(f"HWM CHECK STATUS CPU: {stat['utilization:cpuPercent']}")
                    return True
            return False
        except:
            print(f"stats not returned in check autoreblance condition : {stats}")

    def get_fts_stats(self, print_str=None):
        """
            Print FTS Stats and returns stats along with Current FTS Nodes
        """
        stats, fts_nodes, http_resp = self.get_all_fts_stats()
        self.prettyPrintStats(stats, fts_nodes, http_resp, print_str)
        return stats, fts_nodes

    def prettyPrintStats(self, stats, fts_nodes, http_resp, print_str=None):
        myTable = PrettyTable(["S.No", "FTS Nodes", "lim_memoryBytes", "util_memoryBytes", "util_cpuPercent", "util_diskByte"])
        for count, stat in enumerate(stats):
            if http_resp:
                print(f"Request Response : {http_resp}")
            try:
                myTable.add_row([count+1, fts_nodes[count], stat['limits:memoryBytes'], stat['utilization:memoryBytes'], stat['utilization:cpuPercent'], stat['utilization:diskBytes']])
            except:
                if fts_nodes[count]:
                    self.log.info(f"Stats not returned for node {fts_nodes[count]}")
                    print(stats, fts_nodes, http_resp)
                    myTable.add_row([count+1, fts_nodes[count], "Stats not returned", "Stats not returned", "Stats not returned", "Stats not returned"])
                    self.print_fts_cfg_stats(fts_nodes[-1])
                else:
                    self.log.info("TESTWARE BUG HERE 2")
                    print(stats, fts_nodes, http_resp)
                    self.print_fts_cfg_stats(fts_nodes[-1])
        if print_str:
            self.log.info(print_str)
        print(myTable)

    def print_fts_cfg_stats(self, fts_node):
        creds = self.get_bypass_credentials()
        definitions = FTSCallable(self.input.servers).get_fts_cfg_stats(fts_node, creds)['nodeDefsWanted']['nodeDefs']
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
            if stat['utilization:memoryBytes'] > mem_util or stat['utilization:cpuPercent'] > cpu_util:
                count += 1

        if len(stats) - count < 2:
            self.log.info(f"HWM CHECK PASS STATUS MEM: {stats}")
            return True

        self.log.info(f"HWM CHECK FAIL STATUS MEM: {stats}")
        return False

    def verify_scale_out(self, variables):
        """
            Returns True if FTS Nodes have scaled out
        """
        fts_stats, fts_nodes = self.get_fts_stats()
        new_node_count = len(fts_nodes)
        if variables['node_count'] == new_node_count:
            return False
        else:
            self.log.info(f"Scale out -> New Count : {new_node_count} , Old Count : {variables['node_count']}")
            variables['node_count'] = new_node_count
            return True

    def verify_scale_in(self, variables, summary):
        """
            Returns True iff Scale-In is not possible or Scale-In was successful
        """
        fts_stats, fts_nodes = self.get_fts_stats()
        self.log.info("Verifying Scale In ...")
        if variables['node_count'] == 2:
            self.generate_summary(summary, fts_stats, fts_nodes, variables, "No Scale-In Required")
            self.log.info("BUG : ScaleIn verification not possible since scale out didn't happen")
            self.fail("Autoscaling Failed since Scaling didn't happen")

        self.delete_all_database(True)

        attempts = 5
        scale_in_status = False
        for i in range(attempts):
            self.log.info(f"Sleeping {int((attempts - i) * 8)} minutes for memory to reduce")
            time.sleep(int((attempts - i) * 8))
            print(f'------------ STATS @ {i+1} Attempt"  ------------')
            fts_stats, fts_nodes = self.get_fts_stats()
            if self.check_scale_in_condition(fts_stats):
                scale_in_status = True
                self.log.info(f"Scale In Satisfied at {i+1}th attempt")
                break

        if not scale_in_status:
            self.generate_summary(summary, fts_stats, fts_nodes, variables, "Scale-In not possible")
            self.log.info("CONDITIONAL BUG : ScaleIn verification not possible since Memory didn't decrease")
            return False

        self.log.info(f"Sleeping {self.scaling_time} to observe Scale In")
        time.sleep(int(self.scaling_time))

        fts_stats, fts_nodes = self.get_fts_stats()
        if len(fts_nodes) < variables['node_count']:
            self.log.info(f"Scale In passed : {fts_stats},{fts_nodes}")
            self.log.info("PASS : Scale In passed !!")
            self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : Scale-In")
            return True

        self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : Scale-In")
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

    def calculate_running_average_LWM(self, stats, queue_sum, queue_cpu, variables):
        try:
            mem_avg = 0
            cpu_avg = 0
            for stat in stats:
                mem_avg += int(stat['utilization:memoryBytes'])
                cpu_avg += int(stat['utilization:cpuPercent'])
            mem_avg /= len(stats)
            cpu_avg /= len(stats)

            variables['moving_avg_sum'] += mem_avg
            variables['moving_avg_cpu_sum'] += cpu_avg

            queue_sum.append([mem_avg, datetime.now()])
            queue_cpu.append([cpu_avg, datetime.now()])

            variables['moving_average_mem'] = variables['moving_avg_sum'] / len(queue_sum)
            variables['moving_average_cpu'] = variables['moving_avg_cpu_sum'] / len(queue_cpu)

            if len(queue_sum) <= 1:
                return

            first = queue_sum[0]
            last = queue_sum[-1]
            if ((last[1]-first[1]).total_seconds()/60) > 30:
                first_avg = queue_sum.popleft()
                variables['moving_avg_sum'] -= first_avg[0]
                variables['moving_average_mem'] = variables['moving_avg_sum'] / len(queue_sum)

            first = queue_cpu[0]
            last = queue_cpu[-1]
            if ((last[1]-first[1]).total_seconds()/60) > 30:
                first_avg = queue_cpu.popleft()
                variables['moving_avg_cpu_sum'] -= first_avg[0]
                variables['moving_average_cpu'] = variables['moving_avg_cpu_sum'] / len(queue_sum)
        except:
            print(f"stats not returned while calculating moving average {stats}")

    def autoscaling_synchronous(self):
        self.delete_all_database(True)
        if self.create_dataplane and self.new_dataplane_id is None:
            self.fail("Failed to provision a new dataplane, ABORTING")

        summary = []
        self.provision_databases(self.num_databases, None, self.new_dataplane_id)
        self.log.info(f"------------ Initial STATS ------------")
        fts_stats, fts_nodes = self.get_fts_stats()
        variables = {'node_count': len(fts_nodes), 'scale_out_time': 0, 'HWM_hit': 0, 'scale_out_status': False,
                     'scale_out_count': 0, 'moving_avg_sum': 0, 'moving_avg_cpu_sum': 0, 'moving_average_mem': 0, 'moving_average_cpu': 0, 'max_no_of_nodes': 2}
        self.generate_summary(summary, fts_stats, fts_nodes, variables, "Initial Summary")

        failout_encounter = False
        index_fail_check = False
        queue_sum = deque()
        queue_cpu = deque()
        flag = True
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
                                       collections=collection_name, collection_index=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()
            for i in range(self.num_indexes):
                try:
                    index = fts_callable.create_fts_index(f"counter_{counter + 1}_idx_{i + 1}", source_type='couchbase',
                                                          source_name=database.id, index_type='fulltext-index',
                                                          index_params=None, plan_params=plan_params,
                                                          source_params=None, source_uuid=None, collection_index=True,
                                                          _type=_type, analyzer="standard",
                                                          scope=scope_name, collections=[collection_name], no_check=False)
                    fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
                    if self.autoscaling_with_queries:
                        fts_callable.run_query_and_compare(index, self.num_queries)

                    if variables['scale_out_count'] != 0 and not index_fail_check:
                        with self.subTest("Verifying Index Rejection for HWM"):
                            fts_stats, fts_nodes = self.get_fts_stats()
                            if self.verify_HWM_index_rejection(fts_stats):
                                if variables['HWM_hit'] == 0:
                                    variables['HWM_hit'] += 1
                                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "HWM HIT-Exempted")
                                    self.log.info("HWM Hit has taken place, exempting first rejection")
                                else:
                                    index_fail_check = True
                                    self.log.info("Index created even though HWM had been satisfied")
                                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : HWM HIT-Index Created")
                                    self.fail("BUG : Index created even though HWM had been satisfied")
                except Exception as e:
                    print("Here in the except block")
                    with self.subTest("Index Creation Check"):
                        fts_stats, fts_nodes = self.get_fts_stats()
                        if self.verify_HWM_index_rejection(fts_stats):
                            # self.assertEqual(str(e), f"rest_create_index: error creating index: , err: manager_api: CreateIndex, Prepare failed, err: limitIndexDef: Cannot accommodate index request: {index_name}, resource utilization over limit(s)")
                            self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : HWM HIT-Index rejected")
                            self.log.info(f"PASS : HWM Index Rejection Passed : {str(e)}")
                        else:
                            self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : Index Creation Failed")
                            self.log.info(f"Index Creation Failed : {str(e)}")
                            self.fail("BUG : Index Creation Failed")

                self.log.info(f"------------ FTS Stats @ DB {counter + 1} Index {i+1} ------------")
                fts_stats, fts_nodes = self.get_fts_stats()

                with self.subTest("Verify AutoRebalance"):
                    if self.check_auto_rebalance_condition(fts_stats):
                        rebalance_status = fts_callable.get_fts_rebalance_status(fts_nodes[0])
                        if rebalance_status is None:
                            self.log.info("PASS : AutoRebalance Failed")
                            self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : AutoRebalance Failed")
                            self.fail(f"BUG : AutoRebalance Failed..")
                        else:
                            self.log.info("PASS : AutoRebalance Passed")
                            self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : AutoRebalance Passed")

                self.calculate_running_average_LWM(fts_stats, queue_sum, queue_cpu, variables)
                try:
                    if not variables['scale_out_status'] and variables['moving_average_mem'] > int(float(self.HWM_limit) * int(fts_stats[0]['limits:memoryBytes'])) or variables['moving_average_cpu'] > int(float(self.HWM_limit) * 100):
                    # Scale out satisfied using moving average
                        self.log.info("Scale Out Satisfied using Moving Average")
                        self.log.info(f"Moving Average: {variables['moving_average']}")
                        self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : Scale Out Satisfied using Moving Average")
                        variables['scale_out_time'] = datetime.now()
                        variables['scale_out_status'] = True
                except:
                    print(f"stats not while validating line 816 {fts_stats, fts_nodes}")

                if not variables['scale_out_status'] and self.check_scale_out_condition(fts_stats) and flag:
                    self.log.info("Scale Out Satisfied")
                    flag = False
                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : Scale Out Satisfied b4 Moving Average")
                    self.prettyPrintStats(fts_stats, fts_nodes, False)
                    # variables['scale_out_time'] = datetime.now()
                    # variables['scale_out_status'] = True

                if variables['scale_out_status']:
                    if divmod((datetime.now() - variables['scale_out_time']).total_seconds(), 60)[0] > int(self.scaling_time):
                        with self.subTest("Scale Out Condition Check after scaling time"):
                            if self.verify_scale_out(variables):
                                time_taken = divmod((datetime.now() - variables['scale_out_time']).total_seconds(), 60)[0]
                                self.log.info(f"PASS : Scale Out Passed")
                                self.log.info(f"Scale Out {variables['scale_out_count']}. Passed after {time_taken} minutes!")
                                self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : Scale Out Passed", time_taken)
                                self.log.info(f"PASS : Scale Out Passed")
                                self.log.info(f"Scale Out Stats: \n {fts_stats}, {fts_nodes}")
                                variables['scale_out_status'] = False
                                variables['scale_out_count'] += 1
                                failout_encounter = False
                            else:
                                if not failout_encounter:
                                    failout_encounter = True
                                    self.log.info(f"FAIL : Scale Out Failed")
                                    self.log.info(f"Scale Out {variables['scale_out_count']} Failed : {fts_stats}, {fts_nodes}")
                                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : Scale Out Failed", self.scaling_time)
                                    self.fail(f"BUG : Scale Out {variables['scale_out_count']}. failed after {self.scaling_time} minutes")
                    else:
                        with self.subTest("Scale Out Condition Check before scaling time"):
                            if self.verify_scale_out(variables):
                                time_taken = divmod((datetime.now() - variables['scale_out_time']).total_seconds(), 60)[0]
                                self.log.info(f"BUG : Scale Out happened before scaling time")
                                self.log.info(f"BUG : Scale Out {variables['scale_out_count']}. happened after {time_taken} minutes!")
                                self.generate_summary(summary, fts_stats, fts_nodes, variables, f"F : Scale Out Passed < {self.scaling_time}", time_taken)
                                self.log.info(f"Scale Out Stats: \n {fts_stats}, {fts_nodes}")
                                variables['scale_out_status'] = False
                                variables['scale_out_count'] += 1
                                failout_encounter = False
                                self.fail(f"Scale Out happened less than scaling time")

            self.log.info(f"------------ FTS Stats @ Database {counter + 1} ------------")
            fts_stats, fts_nodes = self.get_fts_stats()
            self.generate_summary(summary, fts_stats, fts_nodes, variables, f"INFO - DB-{counter+1}")

            myTable = PrettyTable(summary[0].keys())
            for json_obj in summary:
                myTable.add_row(json_obj.values())
            self.log.info("Summary Table")
            print(myTable)

        with self.subTest("Scale In Condition Check"):
            if not self.verify_scale_in(variables, summary):
                self.log.info(f"Scale In Failed : {fts_stats}, {fts_nodes}")
                self.fail("BUG : Scale In Failed")

        myTable = PrettyTable(summary[0].keys())
        for json_obj in summary:
            myTable.add_row(json_obj.values())
        self.log.info("Summary Table")
        print(myTable)

        if not variables['scale_out_status']:
            self.fail("Autoscaling Failed since Scaling didn't happen")

    def start_query_workload(self, fts_callable, index, num_queries):
        while not self.stop_queries:
            fts_callable.run_query_and_compare(index, num_queries)
        time.sleep(100)

    def create_index_and_validate_autoscaling(self, all_info, index_no, db_no, index_arr, variables, summary, queue_sum, queue_cpu):
        plan_params = self.construct_plan_params()
        try:
            # Create an FTS Index
            index = all_info[db_no]['fts_callable'].create_fts_index(f"counter_{db_no + 1}_idx_{index_no + 1}", source_type='couchbase',
                                                  source_name=all_info[db_no]['database'].id, index_type='fulltext-index',
                                                  index_params=None, plan_params=plan_params,
                                                  source_params=None, source_uuid=None, collection_index=True,
                                                  _type=all_info[db_no]['type'], analyzer="standard",
                                                  scope=all_info[db_no]['scope_name'], collections=[all_info[db_no]['collection_name']], no_check=False)
            all_info[db_no]['fts_callable'].wait_for_indexing_complete(self._num_of_docs_per_collection, complete_wait=False)
            index_arr.append([all_info[db_no]['fts_callable'], index])
            if self.query_workload_parallel_creation:
                query_thread = threading.Thread(target=self.start_query_workload, kwargs={'fts_callable': all_info[db_no]['fts_callable'], 'index': index, 'num_queries': self.num_queries })
                query_thread.start()

            # After creating an index check if HWM has hit, if it has hit and index still got created (since we are here), then this is potentially a bug.
            if not variables['index_fail_check']:
                with self.subTest("Verifying Index Rejection for HWM"):
                    fts_stats, fts_nodes = self.get_fts_stats()
                    if self.verify_HWM_index_rejection(fts_stats):
                        if variables['HWM_hit'] == 0:
                            variables['HWM_hit'] += 1
                            self.generate_summary(summary, fts_stats, fts_nodes, variables, "HWM HIT-Exempted")
                            self.log.critical("HWM Hit has taken place, exempting first rejection")
                        else:
                            variables['index_fail_check'] = True
                            self.log.critical("Index created even though HWM had been satisfied")
                            self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : HWM HIT-Index Created")
                            self.fail("BUG : Index created even though HWM had been satisfied")
        except Exception as e:
            print("Here in the except block")
            with self.subTest("Index Creation Check"):
                fts_stats, fts_nodes = self.get_fts_stats()
                if self.verify_HWM_index_rejection(fts_stats):
                    self.assertTrue("limitIndexDef: Cannot accommodate index request" in str(e))
                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : HWM HIT-Index rejected")
                    self.log.info(f"PASS : HWM Index Rejection Passed : {str(e)}")
                else:
                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : Index Creation Failed")
                    self.log.critical(f"Index Creation Failed : {str(e)}")
                    self.fail("BUG : Index Creation Failed")

        fts_stats, fts_nodes = self.get_fts_stats(f"------------ FTS Stats @ DB {db_no + 1} Index {index_no+1} ------------")

        with self.subTest("Verify AutoRebalance"):
            if self.check_auto_rebalance_condition(fts_stats):
                rebalance_status = all_info[db_no]['fts_callable'].get_fts_rebalance_status(fts_nodes[0])
                if rebalance_status == "none":
                    self.log.critical("FAIL : AutoRebalance Failed")
                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : AutoRebalance Failed")
                    self.fail(f"BUG : AutoRebalance Failed..")
                else:
                    self.log.info("PASS : AutoRebalance Passed")
                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : AutoRebalance Passed")

        self.calculate_running_average_LWM(fts_stats, queue_sum, queue_cpu, variables)

        if variables['moving_average_mem'] > int(float(self.LWM_limit) * int(fts_stats[0]['limits:memoryBytes'])) or variables['moving_average_cpu'] > int(float(self.LWM_limit) * 100):
            # Scale out satisfied using moving average sum or moving avg cpu
            self.log.info("Scale Out Satisfied using Moving Average")
            self.log.info(f"Moving Average: {variables['moving_average_mem']} | {variables['moving_average_cpu']}")
            self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : Scale Out Satisfied using Moving Average")
            variables['scale_out_time'] = datetime.now()
            variables['scale_out_status'] = True

        if not variables['scale_out_status'] and self.check_scale_out_condition(fts_stats) and variables['flag']:
            self.log.critical("Scale Out Satisfied b4 Moving Average")
            variables['flag'] = False
            self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : Scale Out Satisfied b4 Moving Average")
            self.prettyPrintStats(fts_stats, fts_nodes, False)
            # variables['scale_out_time'] = datetime.now()
            # variables['scale_out_status'] = True

        if variables['scale_out_status']:
            if divmod((datetime.now() - variables['scale_out_time']).total_seconds(), 60)[0] > int(self.scaling_time):
                with self.subTest("Scale Out Condition Check after scaling time"):
                    if self.verify_scale_out(variables):
                        time_taken = divmod((datetime.now() - variables['scale_out_time']).total_seconds(), 60)[0]
                        self.log.info(f"PASS : Scale Out Passed")
                        self.log.info(f"Scale Out {variables['scale_out_count']}. Passed after {time_taken} minutes!")
                        self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : Scale Out Passed", time_taken)
                        self.log.info(f"PASS : Scale Out Passed")
                        self.log.info(f"Scale Out Stats: \n {fts_stats}, {fts_nodes}")
                        variables['scale_out_status'] = False
                        variables['scale_out_count'] += 1
                        variables['failout_encounter'] = False
                    else:
                        if not variables['failout_encounter']:
                            variables['failout_encounter'] = True
                            self.log.critical(f"FAIL : Scale Out Failed")
                            self.log.critical(f"Scale Out {variables['scale_out_count']} Failed : {fts_stats}, {fts_nodes}")
                            self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : Scale Out Failed", self.scaling_time)
                            self.fail(f"BUG : Scale Out {variables['scale_out_count']}. failed after {self.scaling_time} minutes")
            else:
                with self.subTest("Scale Out Condition Check before scaling time"):
                    if self.verify_scale_out(variables):
                        time_taken = divmod((datetime.now() - variables['scale_out_time']).total_seconds(), 60)[0]
                        self.log.critical(f"BUG : Scale Out happened before scaling time")
                        self.log.critical(f"BUG : Scale Out {variables['scale_out_count']}. happened after {time_taken} minutes!")
                        self.generate_summary(summary, fts_stats, fts_nodes, variables, f"F : Scale Out Passed < {self.scaling_time}", time_taken)
                        self.log.critical(f"Scale Out Stats: \n {fts_stats}, {fts_nodes}")
                        variables['scale_out_status'] = False
                        variables['scale_out_count'] += 1
                        variables['failout_encounter'] = False
                        self.fail(f"Scale Out happened less than scaling time")

    def autoscaling_concurrent(self):
        self.delete_all_database(True)

        summary = []
        self.provision_databases(self.num_databases, None, self.new_dataplane_id)
        fts_stats, fts_nodes = self.get_fts_stats("------------ Initial STATS ------------")
        variables = {'node_count': len(fts_nodes), 'scale_out_time': 0, 'HWM_hit': 0, 'scale_out_status': False,
                     'scale_out_count': 0, 'moving_avg_sum': 0, 'moving_avg_cpu_sum': 0, 'moving_average_mem': 0, 'moving_average_cpu': 0, 'index_fail_check': False,
                     'failout_encounter': False, 'flag': True}
        self.generate_summary(summary, fts_stats, fts_nodes, variables, "Initial Summary")

        queue_sum = deque()
        queue_cpu = deque()
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
                                       collections=collection_name, collection_index=True)

            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            all_info.append({'database': database, 'type': _type, 'scope_name': scope_name, 'collection_name': collection_name, 'fts_callable': fts_callable})

        # Launch a thread in each database to create an index
        for i in range(self.num_indexes):
            thread_arr = []
            for j in range(self.num_databases):
                thread = threading.Thread(target=self.create_index_and_validate_autoscaling, kwargs={'all_info': all_info, 'index_no': i, 'db_no' : j,
                                                                    'index_arr': index_arr, 'variables': variables, 'summary': summary,
                                                                    'queue_sum': queue_sum, 'queue_cpu': queue_cpu})
                thread.start()
                time.sleep(2)
                thread_arr.append(thread)

            for thread in thread_arr:
                thread.join()

        fts_stats, fts_nodes = self.get_fts_stats("------------ FTS DB FINISH STATS  ------------")
        self.generate_summary(summary, fts_stats, fts_nodes, variables, f"FTS DB FINISH STATS")

        myTable = PrettyTable(summary[0].keys())
        for json_obj in summary:
            myTable.add_row(json_obj.values())
        self.log.info("Summary Table")
        print(myTable)

        if self.query_workload_after_creation:
            thread_2_array = []
            for index in index_arr:
                thread2 = threading.Thread(target=index[0].run_query_and_compare, kwargs={'index': index[1], 'num_queries': self.num_queries })
                thread2.start()
                thread_2_array.append(thread2)

            for thread2 in thread_2_array:
                thread2.join()

            fts_stats, fts_nodes = self.get_fts_stats("------------ FTS DB QUERY FINISH STATS  ------------")
            self.generate_summary(summary, fts_stats, fts_nodes, variables, f"FTS DB QUERY FINISH STATS")
        self.stop_queries = True
        myTable = PrettyTable(summary[0].keys())
        for json_obj in summary:
            myTable.add_row(json_obj.values())
        self.log.info("Summary Table")
        print(myTable)

        with self.subTest("Scale In Condition Check"):
            if not self.verify_scale_in(variables, summary):
                self.log.info(f"Scale In Failed : {fts_stats}, {fts_nodes}")
                self.fail("BUG : Scale In Failed")

            myTable = PrettyTable(summary[0].keys())
            for json_obj in summary:
                myTable.add_row(json_obj.values())
            self.log.info("Summary Table")
            print(myTable)

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
                                       collections=collection_name, collection_index=True)

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
                                       collections=collection_name, collection_index=True)

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
            fts_callable.delete_all()

