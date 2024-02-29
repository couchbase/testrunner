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

class FTSPauseResume(ServerlessBaseTestCase):
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
        self.num_tenant_to_pause = self.input.param("num_tenant_to_pause", 1)
        self.num_tenant_to_resume = self.input.param("num_tenant_to_resume", 1)
        self.paused_indexes = []
        self.paused_databases = []
        self.active_indexes = []
        self.resumed_indexes = []
        self.all_fts_data = []
        self.sample_query = {"match": "Safiya Morgan", "field": "name"}
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

    def fts_indexes_functionality_status(self, indexes):
        """
            Returns True iff
                1. We are able to fetch index definition for given index
                2. We are able to run FTS query on given index
        """
        functionality_works = False
        if not isinstance(indexes, list):
            indexes = [indexes]

        for index in indexes:
            status, resp = index.get_index_defn()
            if status:
                functionality_works = True
            else:
                self.log.error(
                    f"Not able to fetch index definition for index {index.name} \n Status : {status} \n Response {resp}")
            try:
                hits, _, _, _ = index.execute_query(self.sample_query,
                                                    zero_results_ok=False)
            except Exception as err:
                functionality_works = False
                self.log.error(f"FTS query functionality failure {err}")

        return functionality_works

    def verify_fts_functionality(self, paused_indexes=None, resumed_indexes=None, active_indexes=None):
        if active_indexes is None:
            active_indexes = []
        if resumed_indexes is None:
            resumed_indexes = []
        if paused_indexes is None:
            paused_indexes = []

        if len(paused_indexes) != 0:
            if not isinstance(paused_indexes, list):
                paused_indexes = [paused_indexes]
            for index_list in paused_indexes:
                for index in index_list:
                    fts_functionality_status = self.fts_indexes_functionality_status(index)
                    if fts_functionality_status:
                        self.fail(f"FTS functionality working even after pause for index {index.name}")
                    else:
                        self.log.info(
                            f"FTS functionality for index {index.name} not working since it's source bucket was paused")

        if len(resumed_indexes) != 0:
            if not isinstance(resumed_indexes, list):
                resumed_indexes = [resumed_indexes]
            for index_list in resumed_indexes:
                for index in index_list:
                    fts_functionality_status = self.fts_indexes_functionality_status(index)
                    if not fts_functionality_status:
                        self.fail(f"FTS functionality not working for index {index.name} after resume")
                    else:
                        self.log.info("FTS functionality working after resume operation")

        if len(active_indexes) != 0:
            if not isinstance(active_indexes, list):
                active_indexes = [active_indexes]
            for index_list in active_indexes:
                for index in index_list:
                    fts_functionality_status = self.fts_indexes_functionality_status(index)
                    if fts_functionality_status:
                        self.log.info("FTS functionality working for active indexes")
                    else:
                        self.fail(f"FTS functionality not working for active index - {index.name} after pausing some bucket")

    def setup_databases_with_fts(self):
        for counter, database in enumerate(self.databases.values()):
            if database in self.paused_databases:
                continue
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
            self.all_fts_data.append({})
            self.all_fts_data[counter]['database'] = database
            self.all_fts_data[counter]['fts_callable'] = fts_callable
            _type = self.define_index_parameters_collection_related(container_type="collection", scope=scope_name,
                                                                    collection=collection_name)
            plan_params = self.construct_plan_params()
            all_indexes = []
            for i in range(self.num_indexes):
                fts_idx = fts_callable.create_fts_index(f"idx_{i}", source_type='couchbase',
                                                        source_name=database.id, index_type='fulltext-index',
                                                        index_params=None, plan_params=plan_params,
                                                        source_params=None, source_uuid=None, collection_index=True,
                                                        _type=_type, analyzer="standard",
                                                        scope=scope_name, collections=[collection_name], no_check=False)
                fts_callable.wait_for_indexing_complete(self._num_of_docs_per_collection)
                all_indexes.append(fts_idx)
            self.all_fts_data[counter]['indexes'] = all_indexes

    def test_pause_resume(self):
        self.provision_databases(self.num_databases)
        self.setup_databases_with_fts()
        for counter, database in enumerate(self.databases.values()):
            if counter < self.num_tenant_to_pause and counter < len(self.databases):
                self.log.info(f"Starting pause operation for database : {database.id}")
                self.paused_indexes.append(self.all_fts_data[counter]['indexes'])
                self.api.pause_operation(database_id=database.id, state='paused')
            else:
                self.active_indexes.append(self.all_fts_data[counter]['indexes'])

        self.verify_fts_functionality(paused_indexes=self.paused_indexes, active_indexes=self.active_indexes)
        for counter, database in enumerate(self.databases.values()):
            if counter < self.num_tenant_to_resume and counter < len(self.databases):
                self.log.info(f"Starting resume operation for database : {database.id}")
                self.api.resume_operation(database_id=database.id, state='healthy')
                self.resumed_indexes.append(self.all_fts_data[counter]['indexes'])
                self.paused_indexes.remove(self.all_fts_data[counter]['indexes'])

        self.verify_fts_functionality(paused_indexes=self.paused_indexes, resumed_indexes=self.resumed_indexes,
                                      active_indexes=self.active_indexes)
        for fts_data in self.all_fts_data:
            fts_callable = fts_data['fts_callable']
            fts_callable.delete_all()

    def pause_then_create_db_same_name_then_resume(self):
        self.provision_databases(1, seed=1)
        self.setup_databases_with_fts()

        for counter, database in enumerate(self.databases.values()):
            self.log.info(f"Starting pause operation for database : {database.id}")
            self.paused_indexes.append(self.all_fts_data[counter]['indexes'])
            self.api.pause_operation(database_id=database.id, state='paused')
            self.paused_databases.append(database)

        self.verify_fts_functionality(paused_indexes=self.paused_indexes, active_indexes=self.active_indexes)

        self.provision_databases(1, seed=1)
        self.setup_databases_with_fts()

        for counter, database in enumerate(self.databases.values()):
            self.log.info(f"Starting resume operation for database : {database.id}")
            self.api.resume_operation(database_id=database.id, state='healthy')
            self.resumed_indexes.append(self.all_fts_data[counter]['indexes'])
            self.paused_indexes.remove(self.all_fts_data[counter]['indexes'])

        self.verify_fts_functionality(paused_indexes=self.paused_indexes, resumed_indexes=self.resumed_indexes,
                                      active_indexes=self.active_indexes)

        for fts_data in self.all_fts_data:
            fts_callable = fts_data['fts_callable']
            fts_callable.delete_all()
