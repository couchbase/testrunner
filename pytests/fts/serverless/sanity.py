from TestInput import TestInputServer, TestInputSingleton
from lib import global_vars
from lib.Cb_constants.CBServer import CbServer
from lib.SystemEventLogLib.Events import EventHelper
from lib.capella.utils import CapellaCredentials
from pytests.fts.fts_callable import FTSCallable
from pytests.serverless.serverless_basetestcase import ServerlessBaseTestCase
import time
import random


class FTSElixirSanity(ServerlessBaseTestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.num_of_docs_per_collection = 100000
        CbServer.use_https = True
        CbServer.capella_run = True
        CbServer.capella_credentials = CapellaCredentials(self.input.capella)
        self.index_replicas = self.input.param("index_replicas", 1)
        self.num_index_partitions = self.input.param("num_partitions", 1)
        self.num_databases = self.input.param("num_databases", 1)
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
            self.load_databases(load_all_databases=False, num_of_docs=self.num_of_docs_per_collection,
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

            fts_callable.wait_for_indexing_complete(self.num_of_docs_per_collection)

            docs_indexed = fts_idx.get_indexed_doc_count()
            container_doc_count = self.num_of_docs_per_collection
            self.log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
            errors = []
            if docs_indexed == 0:
                errors.append(f"No docs were indexed for index {fts_idx.name}")
            if docs_indexed != container_doc_count:
                errors.append(f"Bucket doc count = {container_doc_count}, index doc count={docs_indexed}")

            fts_callable.delete_all()

    def create_custom_map_index_and_update_defn(self):
        self.provision_databases(self.num_databases)
        for counter, database in enumerate(self.databases.values()):
            self.cleanup_database(database_obj=database)
            scope_name = f'db_{counter}_scope_{random.randint(0, 1000)}'
            collection_name = f'db_{counter}_collection_{random.randint(0, 1000)}'
            self.create_scope(database_obj=database, scope_name=scope_name)
            self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
            self.load_databases(load_all_databases=False, doc_template="Employee",
                                num_of_docs=self.num_of_docs_per_collection,
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
            fts_callable.wait_for_indexing_complete(self.num_of_docs_per_collection)
            self.log.info("Editing custom index with new map...")
            fts_idx.generate_new_custom_map(seed=fts_idx.cm_id + 10, collection_index=True, type_mapping=_type)
            fts_idx.index_definition['uuid'] = fts_idx.get_uuid()
            fts_idx.update()
            docs_indexed = fts_idx.get_indexed_doc_count()
            container_doc_count = self.num_of_docs_per_collection
            self.log.info(f"Docs in index {fts_idx.name}={docs_indexed}, kv docs={container_doc_count}")
            errors = []
            if docs_indexed == 0:
                errors.append(f"No docs were indexed for index {fts_idx.name}")
            if docs_indexed != container_doc_count:
                errors.append(f"Bucket doc count = {container_doc_count}, index doc count={docs_indexed}")

            fts_callable.delete_all()

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
                                num_of_docs=self.num_of_docs_per_collection,
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
            fts_callable.wait_for_indexing_complete(self.num_of_docs_per_collection)
            fts_callable.delete_fts_index(database.id + "." + scope_name + ".index")
            try:
                fts_callable.create_fts_index("index", source_type='couchbase',
                                              source_name=database.id, index_type='fulltext-index',
                                              index_params=None, plan_params=plan_params,
                                              source_params=None, source_uuid=None, collection_index=True,
                                              _type=_type, analyzer="standard",
                                              scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self.num_of_docs_per_collection)
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
                                num_of_docs=self.num_of_docs_per_collection,
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
                fts_callable.wait_for_indexing_complete(self.num_of_docs_per_collection)
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
                                num_of_docs=self.num_of_docs_per_collection,
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
                fts_callable.wait_for_indexing_complete(self.num_of_docs_per_collection)
            # 21st fts-index
            try:
                fts_callable.create_fts_index(f"index_{21}", source_type='couchbase',
                                              source_name=database.id, index_type='fulltext-index',
                                              index_params=None, plan_params=plan_params,
                                              source_params=None, source_uuid=None, collection_index=True,
                                              _type=_type, analyzer="standard",
                                              scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self.num_of_docs_per_collection)
                self.fail(
                    "Testcase failed: support for indexes with 1 active + 1 replica partitions only in serverless mode")
            except Exception as e:
                print(
                    "Testcase Passed : support for indexes with 1 active + 1 replica partitions only in serverless mode")
                AssertionError(str(e),
                               "Testcase Passed : support for indexes with 1 active + 1 replica partitions only in serverless mode")

            fts_callable.delete_all()