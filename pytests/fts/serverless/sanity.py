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
import pprint
from datetime import datetime
from prettytable import PrettyTable

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
        self.HWM_limit = self.input.param("HWM_limit", 0.8)
        self.LWM_limit = self.input.param("LWM_limit", 0.5)
        self.UWM_limit = self.input.param("UWM_limit", 0.3)
        self.scaling_time = self.input.param("scaling_time", 30)
        self.num_queries = self.input.param("num_queries", 20)
        self.autoscaling_with_queries = self.input.param("autoscaling_with_queries", False)
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
            fts_callable.create_fts_index("idx", source_type='couchbase',
                                          source_name=database.id, index_type='fulltext-index',
                                          index_params=None, plan_params=plan_params,
                                          source_params=None, source_uuid=None, collection_index=True,
                                          _type=_type, analyzer="standard",
                                          scope=scope_name, collections=[collection_name], no_check=False)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            index_resp = fts_callable.check_if_index_exists(database.id + "." + scope_name + ".idx", node_def=True)
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
            fts_callable.create_fts_index("idx", source_type='couchbase',
                                          source_name=database.id, index_type='fulltext-index',
                                          index_params=None, plan_params=plan_params,
                                          source_params=None, source_uuid=None, collection_index=True,
                                          _type=_type, analyzer="standard",
                                          scope=scope_name, collections=[collection_arr], no_check=False)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            resp = fts_callable.check_if_index_exists(database.id + "." + scope_name + ".idx", index_def=True)
            if resp:
                self.log.info(f"Index Definition before collection deletion : {resp}")
            for collection_name in collection_arr:
                self.delete_collection(database, scope_name, collection_name)
            self.log.info(f"Waiting for collection to get permanently deleted")
            time.sleep(100)
            resp = fts_callable.check_if_index_exists(database.id + "." + scope_name + ".idx", index_def=True)
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
                fts_callable.create_fts_index(f"index_{i}", source_type='couchbase',
                                              source_name=database.id, index_type='fulltext-index',
                                              index_params=None, plan_params=plan_params,
                                              source_params=None, source_uuid=None, collection_index=True,
                                              _type=_type, analyzer="standard",
                                              scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
                if counter in database_random_indexes:
                    indexes_arr.append(
                        [fts_callable, database.id + "." + scope_name + ".index_" + str(i), database.id])

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
                fts_callable.create_fts_index(f"index_{i}", source_type='couchbase',
                                              source_name=database.id, index_type='fulltext-index',
                                              index_params=None, plan_params=plan_params,
                                              source_params=None, source_uuid=None, collection_index=True,
                                              _type=_type, analyzer="standard",
                                              scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
                indexes_arr.append([fts_callable, database.id + "." + scope_name + ".index_" + str(i)])

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

            fts_callable.create_fts_index("index", source_type='couchbase',
                                          source_name=database.id, index_type='fulltext-index',
                                          index_params=None, plan_params=plan_params,
                                          source_params=None, source_uuid=None, collection_index=True,
                                          _type=_type, analyzer="standard",
                                          scope=scope_name, collections=[collection_name], no_check=False)
            fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
            fts_callable.delete_fts_index(database.id + "." + scope_name + ".index")
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
                               f"rest_create_index: error creating index: , err: manager_api: cannot create/update index because an index with the same name already exists: {database.id}.{scope_name}.index")
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
        mem_util = int(float(self.UWM_limit) * int(stats[0]['limits:memoryBytes']))
        cpu_util = int(float(self.UWM_limit) * 100)
        for stat in stats:
            if stat['utilization:memoryBytes'] > mem_util or stat['utilization:cpuPercent'] > cpu_util:
                return False
        return True

    def check_HWM_condition(self, stats):
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

    def get_fts_stats(self):
        """
            Print FTS Stats and returns stats along with Current FTS Nodes
        """
        stats, fts_nodes, http_resp = self.get_all_fts_stats()
        myTable = PrettyTable(["S.No", "FTS Nodes", "lim_memoryBytes", "util_memoryBytes", "util_cpuPercent", "util_diskByte"])
        for count, stat in enumerate(stats):
            if http_resp:
                print(f"Request Response : {http_resp}")
            myTable.add_row([count+1, fts_nodes[count], stat['limits:memoryBytes'], stat['utilization:memoryBytes'], stat['utilization:cpuPercent'], stat['utilization:diskBytes']])
        print(myTable)
        return stats, fts_nodes

    def verify_HWM_index_rejection(self, stats):
        """
           Returns true if HWM is actually the reason for index rejection
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
            Returns True if FTS has scaled out
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
            Returns True if Scale In not possible or Scale In was successful
        """
        fts_stats, fts_nodes = self.get_fts_stats()
        self.log.info("Verifying Scale In ...")
        if len(fts_nodes) == 2:
            self.generate_summary(summary, fts_stats, fts_nodes, variables, "No Scale-In Required")
            self.log.info("CONDITIONAL BUG : ScaleIn verification not possible since scale out didn't happen")
            return True

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
        mem_arr = []
        for i in range(len(fts_stats)):
            node_name = str(fts_nodes[i]).strip(".sandbox.nonprod-project-avengers.com")
            mem_util = (str(int(fts_stats[i]['utilization:memoryBytes'])/pow(10, 9)) + "GB").strip()
            mem_arr.append(f"{node_name} - {mem_util}")
        obj = {
            "Detail": name,
            "Time": str(datetime.now().replace(microsecond=0)),
            "Memory Status": mem_arr,
            "Scale Out Count ": variables['scale_out_count'],
            "Scale Out Time": time_taken,
            "HWM Index Rejection": True if variables['HWM_hit'] == 1 else False
        }
        summary.append(obj)

    def autoscaling_synchronous(self):
        self.delete_all_database(True)
        if self.create_dataplane and self.new_dataplane_id is None:
            self.fail("Failed to provision a new dataplane, ABORTING")

        summary = []
        self.provision_databases(self.num_databases, None, self.new_dataplane_id)
        self.log.info(f"------------ Initial STATS ------------")
        fts_stats, fts_nodes = self.get_fts_stats()
        variables = {'node_count': len(fts_nodes), 'scale_out_time': 0, 'HWM_hit': 0, 'scale_out_status': False,
                     'scale_out_count': 0}
        self.generate_summary(summary, fts_stats, fts_nodes, variables, "Initial Summary")

        failout_encounter = False
        index_fail_check = False

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
                                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "HWM Exempt HIT")
                                    self.log.info("HWM Hit has taken place, exempting first rejection")
                                else:
                                    index_fail_check = True
                                    self.log.info("Index created even though HWM had been satisfied")
                                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : HWM HIT,Index Created")
                                    self.fail("BUG : Index created even though HWM had been satisfied")
                except Exception as e:
                    print("Here in the except block")
                    with self.subTest("Index Creation Check"):
                        fts_stats, fts_nodes = self.get_fts_stats()
                        if self.verify_HWM_index_rejection(fts_stats):
                            # self.assertEqual(str(e), f"rest_create_index: error creating index: , err: manager_api: CreateIndex, Prepare failed, err: limitIndexDef: Cannot accommodate index request: {index_name}, resource utilization over limit(s)")
                            self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : HWM HIT")
                            self.log.info(f"PASS : HWM Index Rejection Passed : {e}")
                            with self.subTest("Scale In Condition Check"):
                                if not self.verify_scale_in(variables, summary):
                                    self.fail("BUG : Scale In Failed")
                            return
                        else:
                            self.log.info(f"Index Creation Failed : {e}")
                            self.fail("BUG : Index Creation Failed")

                self.log.info(f"------------ FTS Stats @ DB {counter + 1} Index {i+1} ------------")
                fts_stats, fts_nodes = self.get_fts_stats()

                if not variables['scale_out_status'] and self.check_scale_out_condition(fts_stats):
                    self.log.info("Scale Out Satisfied")
                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "P : Scale Out Condition Satisfied")
                    self.log.info(fts_stats)
                    variables['scale_out_time'] = datetime.now()
                    variables['scale_out_status'] = True

                if variables['scale_out_status']:
                    if divmod((datetime.now() - variables['scale_out_time']).total_seconds(), 60)[0] > int(self.scaling_time):
                        with self.subTest("Scale Out Condition Check after scaling time"):
                            if self.verify_scale_out(variables):
                                time_taken = divmod((datetime.now() - variables['scale_out_time']).total_seconds(), 60)[0]
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
                                    self.log.info(f"Scale Out Failed -> {variables['scale_out_count']}  : {fts_stats}, {fts_nodes}")
                                    self.generate_summary(summary, fts_stats, fts_nodes, variables, "F : Scale Out Failed", self.scaling_time)
                                    self.fail(f"BUG : Scale Out failed -> {variables['scale_out_count']} within {self.scaling_time} minutes")
                    else:
                        with self.subTest("Scale Out Condition Check before scaling time"):
                            if self.verify_scale_out(variables):
                                time_taken = divmod((datetime.now() - variables['scale_out_time']).total_seconds(), 60)[0]
                                self.log.info(f"BUG : Scale Out {variables['scale_out_count']}. happened after {time_taken} minutes!")
                                self.generate_summary(summary, fts_stats, fts_nodes, variables, f"F : Scale Out Passed < {self.scaling_time}", time_taken)
                                self.log.info(f"Scale Out Stats: \n {fts_stats}, {fts_nodes}")
                                variables['scale_out_status'] = False
                                variables['scale_out_count'] += 1
                                failout_encounter = False
                                self.fail(f"Scale Out Happened less than scaling time")

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
                self.assertTrue(f'{database.id}.{scope_name}.idx' in str(explain_result), f"The query is not using the fts index! please check explain {explain_result}")

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
                self.assertTrue(f'{database.id}.{scope_name}.idx' in str(explain_result), f"The query is not using the fts index! please check explain {explain_result}")
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
                self.assertTrue(f'{database.id}.{scope_name}.idx' in str(explain_result), f"The query is not using the fts index! please check explain {explain_result}")

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
                self.assertTrue(f'{database.id}.{scope_name}.idx' in str(explain_result), f"The query is not using the fts index! please check explain {explain_result}")
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
                    self.assertTrue(f'{database.id}.{scope_name}.idx' in str(explain_result), f"The query is not using the fts index! please check explain {explain_result}")
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
