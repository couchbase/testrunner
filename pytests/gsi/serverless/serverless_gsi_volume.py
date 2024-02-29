"""
serverless_gsi_volume.py: This test runs Volume test for GSI/Query

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = 29/03/23 1:36 pm

"""
import json
import math
import pprint
import random
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from json import loads
from string import digits, ascii_letters
from threading import Event, Lock

from gsi.serverless.base_gsi_serverless import BaseGSIServerless
from membase.api.serverless_rest_client import ServerlessRestConnection as RestConnection
from table_view import TableView


class ServerlessGSIVolume(BaseGSIServerless):
    def setUp(self):
        super(ServerlessGSIVolume, self).setUp()
        self.log.info("==============  ServerlessGSIVolume serverless setup has started ==============")
        self.num_lvt = self.input.param("num_lvt", self.num_of_tenants // 2)
        self.num_hvt = self.input.param("num_hvt", self.num_of_tenants // 3)
        self.num_svt = self.input.param("num_svt", self.num_of_tenants // 6)
        self.lvt_scopes = self.input.param("lvt_scopes", 2)
        self.hvt_scopes = self.input.param("hvt_scopes", 3)
        self.svt_scopes = self.input.param("svt_scopes", 3)
        self.lvt_collections = self.input.param("lvt_collections", 2)
        self.hvt_collections = self.input.param("hvt_collections", 1)
        self.svt_collections = self.input.param("svt_collections", 1)
        self.lvt_weightage = self.input.param("lvt_weightage", 1)
        self.hvt_weightage = self.input.param("hvt_weightage", 2)
        self.svt_weightage = self.input.param("svt_weightage", 0)
        self.database_prefix = self.input.param("database_prefix", "tenant")
        self.scope_prefix = self.input.param("scope_prefix", "test_scope")
        self.collection_prefix = self.input.param("collection_prefix", "test_collection")
        self.doc_prefix = self.input.param("doc_prefix", "doc_")
        self.initial_doc_count = self.input.param("initial_doc_count", 10000)
        self.lvt_data_load = self.input.param("lvt_data_load", 10000)
        self.hvt_data_load = self.input.param("hvt_data_load", 100000)
        self.svt_data_load = self.input.param("svt_data_load", 0)
        self.query_result_limit = self.input.param("query_result_limit", 100)
        self.num_queries_in_parallel = self.input.param("num_queries_in_parallel", 50)
        self.test_run_duration = self.input.param("test_run_duration", 24 * 60 * 60)
        self.stats_dump_timer = self.input.param("stats_dump_timer", 300)
        self.new_tenant_iter = self.input.param("new_tenant_iter", 5)
        self.new_tenants_count = self.input.param("self.new_tenants_count", 3)
        self.cb_collect_duration = self.input.param("cb_collect_duration", 1800)
        self.pause_iter = self.input.param("pause_iter", 10)
        self.svt_ids, self.hvt_ids, self.lvt_ids, self.paused_ids = set(), set(), set(), set()
        self.query_event = Event()
        self.create_indexes_event = Event()
        self.stats_dump_event = Event()
        self.overflown_tenants_for_indexes = set()
        self.query_run_stats_dict = {"SELECT": {"failed_count": 0, "total_count": 0, "success_count": 0},
                                     "DROP": {"failed_count": 0, "total_count": 0, "success_count": 0},
                                     "CREATE": {"failed_count": 0, "total_count": 0, "success_count": 0},
                                     "BUILD": {"failed_count": 0, "total_count": 0, "success_count": 0}}
        self.lock = Lock()
        self.index_scale_up = False
        self.index_scale_down = False
        self.gsi_cooling = False
        self.gsi_auto_rebl = False
        self.n1ql_cooling = False
        self.index_nodes_in_cluster = len(self.get_nodes_from_services_map(service='index', rest_info=self.dp_obj))
        self.kv_nodes_in_cluster = len(self.get_nodes_from_services_map(service='kv', rest_info=self.dp_obj))
        self.n1ql_nodes_in_cluster = len(self.get_nodes_from_services_map(service='n1ql', rest_info=self.dp_obj))
        self.last_5_mins = dict()
        self.stats_dump_event.set()
        self.query_load = {'lvt': {}, 'hvt': {}, 'svt': {}}
        self.drop_queries_dict = dict()
        for _, stats in self.get_all_index_node_usage_stats(print_stats=False).items():
            self.mem_quota = stats["memory_quota"]
            self.units_quota = stats["units_quota"]
            break
        self.log.info("==============  ServerlessGSIVolume serverless setup has started ==============")

    def tearDown(self):
        self.log.info("==============  ServerlessGSIVolume serverless tearDown has started ==============")
        super(ServerlessGSIVolume, self).tearDown()
        self.log.info("==============  ServerlessGSIVolume serverless tearDown has completed ==============")

    def create_indexes_for_tenant(self, database, db_id, weightage, skip_primary=False):
        """
        @summary: Create Indexes for a given tenant according to weightage passed
        """
        create_queries = []
        build_queries = []
        for scope in database.collections:
            for collection in database.collections[scope]:
                namespace = f"`{database.id}`.{scope}.{collection}"
                for _ in range(weightage):
                    definition_list = self.gsi_util_obj.get_index_definition_list(dataset=self.dataset,
                                                                                  skip_primary=skip_primary)
                    queries = self.gsi_util_obj.get_create_index_list(definition_list=definition_list,
                                                                      namespace=namespace, defer_build=True)
                    drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=definition_list,
                                                                         namespace=namespace)
                    if db_id in self.drop_queries_dict:
                        self.drop_queries_dict[db_id].extend(drop_queries)
                    else:
                        self.drop_queries_dict[db_id] = drop_queries
                    build_query = self.gsi_util_obj.get_build_indexes_query(definition_list=definition_list,
                                                                            namespace=namespace)
                    create_queries.extend(queries)
                    build_queries.append(build_query)

                    # Adding select queries
                    if db_id in self.lvt_ids:
                        if db_id not in self.query_load['lvt']:
                            values = self.gsi_util_obj.get_select_queries(definition_list=self.definition_list,
                                                                          namespace=namespace,
                                                                          limit=self.query_result_limit)
                            self.query_load['lvt'][db_id] = set(values)
                    elif db_id in self.hvt_ids:
                        if db_id not in self.query_load['hvt']:
                            values = self.gsi_util_obj.get_select_queries(definition_list=self.definition_list,
                                                                          namespace=namespace,
                                                                          limit=self.query_result_limit)
                            self.query_load['hvt'][db_id] = set(values)
                    elif db_id in self.svt_ids:
                        if db_id not in self.query_load['svt']:
                            values = self.gsi_util_obj.get_select_queries(definition_list=self.definition_list,
                                                                          namespace=namespace,
                                                                          limit=self.query_result_limit)
                            self.query_load['svt'][db_id] = set(values)
        self.run_bulk_queries(queries=create_queries, database=database, db_id=db_id, batch_size=5, query_type='CREATE')
        self.run_bulk_queries(queries=build_queries, database=database, db_id=db_id, batch_size=5, query_type='BUILD')

    def create_indexes_for_tenants(self, databases_dict=None, continous_run=True, skip_primary=False):
        """
        @summary: This method generates indexes for tenant based on the defined weightage for tenant category.
        If tenant have any no previous index load then new set of select queries would be populated and then query_load
        variable will be amended for that variable.

        """
        while self.create_indexes_event.is_set():
            if databases_dict is None:
                databases_dict = self.databases
            with ThreadPoolExecutor() as executor:
                try:
                    tasks = []
                    for db_id, database in databases_dict.items():
                        if db_id in self.lvt_ids:
                            weightage = self.lvt_weightage
                        elif db_id in self.hvt_ids:
                            weightage = self.hvt_weightage
                        elif db_id in self.svt_ids:
                            weightage = self.svt_weightage
                        else:
                            weightage = 1
                        task = executor.submit(self.create_indexes_for_tenant, database, db_id, weightage, skip_primary)
                        tasks.append(task)

                    for task in tasks:
                        task.result()

                    # Monitoring building of indexes
                    self.monitor_build_indexes_for_tenants()

                    # Re-attempting building of indexes in case of build query failure
                    index_node = random.choice(self.get_nodes_from_services_map(service='index', rest_info=self.dp_obj))
                    rest = RestConnection(rest_username=self.dp_obj.admin_username,
                                          rest_password=self.dp_obj.admin_password, rest_srv=index_node)
                    index_status = rest.get_indexer_metadata()['status']

                    index_build_dict = {}
                    for index in index_status:
                        index_namespace = f"`{index['bucket']}`.{index['scope']}.{index['collection']}"
                        if index["status"] == 'Created':
                            if index_namespace in index_build_dict:
                                index_build_dict[index_namespace].add(f"`{index['indexName']}`")
                            else:
                                index_build_dict[index_namespace] = {f"`{index['indexName']}`"}
                    if index_build_dict:
                        for index_namespace, name_list in index_build_dict.items():
                            bucket, _, _ = index_namespace.split('.')
                            bucket = bucket.strip('`')
                            build_indexes_string = ', '.join(name_list)
                            build_query = f'BUILD INDEX ON {index_namespace} ({build_indexes_string})'
                            self.run_bulk_queries(queries=[build_query], database=self.databases[bucket], db_id=bucket,
                                                  batch_size=1, query_type="BUILD")
                        self.monitor_build_indexes_for_tenants()
                    if not continous_run:
                        break
                except Exception as err:
                    self.create_indexes_event.clear()
                    self.log.fatal(f"Exception occurred during Index creation - {err}")

    def monitor_build_indexes_for_tenants(self, timeout=1800, num_retry=2):
        start = datetime.now()
        if num_retry == 0:
            self.log.critical("Indexes Building is taking unusually longer time. Check Index logs")
            return

        while datetime.now() < start + timedelta(seconds=timeout):
            try:
                index_node = random.choice(self.get_nodes_from_services_map(service='index', rest_info=self.dp_obj))
                rest = RestConnection(rest_username=self.dp_obj.admin_username,
                                      rest_password=self.dp_obj.admin_password, rest_srv=index_node)
                index_status = rest.get_indexer_metadata()['status']

                state_table = TableView(self.log.info)
                state_table.set_headers(["Index Name", "Bucket", "Scope", "Collection", "State"])
                index_state_dict = {index["name"]: index["status"] for index in index_status
                                    if index["status"] != "Ready"}
                if index_state_dict:
                    values = index_state_dict.values()
                    if all(map(lambda x: x == 'Created', values)):
                        self.log.info("All Remaining indexes are in Created state")
                        break
                    for index in index_status:
                        if index["name"] in index_state_dict:
                            state_table.add_row([index["name"], index["bucket"], index["scope"],
                                                 index["collection"], index["status"]])
                    state_table.display("Index Building Status")
                    self.sleep(60 * 4, "Waiting for indexes to be online")
                else:
                    self.log.info("All Indexes are UP and READY!!!")
                    break
            except Exception as err:
                self.log.critical(f"Error occurred during Index Building: {err}")
        else:
            self.log.info(f"All Indexes didn't build in {timeout} seconds. Running the polling again")
            self.monitor_build_indexes_for_tenants(timeout=timeout * 2, num_retry=num_retry - 1)

    def run_bulk_queries(self, queries, database, db_id, batch_size=20, query_type="SELECT"):
        """
        @summary: Run queries in parallel for a given batch size.
        It also tracks the pass/fail status for give query type
        """
        query_type = query_type.upper()
        num_batches = math.ceil(len(queries) / batch_size)
        with ThreadPoolExecutor() as executor:
            for batch_counter in range(num_batches):
                start = batch_counter * batch_size
                end = start + batch_size
                batch_queries = queries[start:end]
                tasks = []
                query_node = random.choice(self.get_all_query_nodes(rest_info=self.dp_obj))
                for query in batch_queries:
                    task = executor.submit(self.run_query, database=database, query=query,
                                           print_billing_info=False, query_node=query_node)
                    tasks.append(task)
                    self.query_run_stats_dict[query_type]["total_count"] += 1
                for task in tasks:
                    try:
                        task.result()
                        self.query_run_stats_dict[query_type]["success_count"] += 1
                    except Exception as err:
                        self.query_run_stats_dict[query_type]["failed_count"] += 1
                        err_limit = 'Limit for number of indexes that can be' \
                                    ' created per bucket has been reached.'
                        err_msg = "'code': 12016, 'msg': 'Index Not Found"
                        if err_limit in str(err):
                            self.overflown_tenants_for_indexes.add(db_id)
                        elif err_msg in str(err):
                            continue

    def drop_indexes_from_overflown_tenants(self):
        """
        @summary: Drop indexes for all the tenants for which max indexes limit is reached. It tracks the tenants from
        self.overflown_tenants_for_indexes variable.
        """
        while self.create_indexes_event.is_set():
            try:
                with ThreadPoolExecutor() as executor:
                    if self.overflown_tenants_for_indexes:
                        tasks = []
                        for db_id in self.overflown_tenants_for_indexes:
                            num_drop_queries = random.randint(5, 20)
                            drop_queries = random.sample(self.drop_queries_dict[db_id], k=num_drop_queries)
                            database = self.databases[db_id]
                            task = executor.submit(self.run_bulk_queries, drop_queries, database, db_id,
                                                   num_drop_queries, "DROP")
                            tasks.append(task)

                        for task in tasks:
                            task.result()
                        self.overflown_tenants_for_indexes.remove(db_id)
            except Exception as err:
                self.log.critical(err)

    def run_select_query_load(self, run_time=0):
        """
        @summary: This method run the query load continuously for give tenants until the query event is set to false
        This run certain no. of queries in parallel select queries based on tenant category weightage and then divide
        the no. equally among all the dbs in that category. Random queries are selected and executed over and over.
        """
        total_weightage = self.hvt_weightage + self.lvt_weightage + self.svt_weightage
        hvt_tenant_weightage = math.ceil(self.num_queries_in_parallel * self.hvt_weightage /
                                         total_weightage / len(self.hvt_ids))
        lvt_tenant_weightage = math.ceil(self.num_queries_in_parallel * self.lvt_weightage /
                                         total_weightage / len(self.lvt_ids))
        svt_tenant_weightage = math.ceil(self.num_queries_in_parallel * self.svt_weightage /
                                         total_weightage / len(self.svt_ids))
        select_queries = {}
        while self.query_event.is_set():
            try:
                for db_id, db_queries in self.query_load['lvt'].items():
                    db_queries = list(db_queries)
                    if len(db_queries) < lvt_tenant_weightage:
                        queries = db_queries * math.ceil(lvt_tenant_weightage / len(db_queries))
                    else:
                        queries = random.sample(db_queries, k=lvt_tenant_weightage)
                    select_queries[db_id] = queries
                for db_id, db_queries in self.query_load['hvt'].items():
                    db_queries = list(db_queries)
                    if len(db_queries) < hvt_tenant_weightage:
                        queries = db_queries * math.ceil(hvt_tenant_weightage / len(db_queries))
                    else:
                        queries = random.sample(db_queries, k=hvt_tenant_weightage)
                    select_queries[db_id] = queries
                for db_id, db_queries in self.query_load['svt'].items():
                    db_queries = list(db_queries)
                    if len(db_queries) < svt_tenant_weightage:
                        queries = db_queries * math.ceil(svt_tenant_weightage / len(db_queries))
                    else:
                        queries = random.sample(db_queries, k=svt_tenant_weightage)
                    select_queries[db_id] = queries

                start_time = datetime.now()
                total_queries = 0
                with ThreadPoolExecutor() as executor:
                    tasks = []
                    for db_id, queries in select_queries.items():
                        database = self.databases[db_id]
                        task = executor.submit(self.run_bulk_queries, queries, database,
                                               db_id, self.num_queries_in_parallel)
                        tasks.append(task)
                        total_queries += len(queries)
                    for task in tasks:
                        task.result()
                execution_time = (datetime.now() - start_time).total_seconds()
                self.log.info(f"Total execution time for {len(total_queries)} Select Queries: {execution_time} ")

                if run_time > 0:
                    curr_time = datetime.now()
                    if curr_time > start_time + timedelta(minutes=run_time):
                        break
            except Exception as err:
                self.query_event.clear()
                self.log.critical(err)

    def generate_doc_load_for_tenant_category(self, tenant_type, load_volume, ops_rate=10000):
        """
        @summary: Generate uniform load for each collection in given tenant category
        """
        if tenant_type == 'lvt':
            self.log.info("Generating dataload for LVT tenants")
            databases = [database for db_id, database in self.databases.items() if db_id in self.lvt_ids]
            db_ids = [db_id for db_id, database in self.databases.items() if db_id in self.lvt_ids]
            self.log.info(f"Total no. of LVT tenants: {len(databases)}")
            self.log.info(f"Total no. of doc insertion/collection: {load_volume}")
            self.log.info(f"LVT tenants: {db_ids}")
        elif tenant_type == 'svt':
            self.log.info("Generating dataload for SVT tenants")
            databases = [database for db_id, database in self.databases.items() if db_id in self.svt_ids]
            db_ids = [db_id for db_id, database in self.databases.items() if db_id in self.svt_ids]
            self.log.info(f"Total no. of SVT tenants: {len(databases)}")
            self.log.info(f"Total no. of doc insertion/collection: {load_volume}")
            self.log.info(f"SVT tenants: {db_ids}")
        else:
            self.log.info("Generating dataload for HVT tenants")
            databases = [database for db_id, database in self.databases.items() if db_id in self.hvt_ids]
            db_ids = [db_id for db_id, database in self.databases.items() if db_id in self.hvt_ids]
            self.log.info(f"Total no. of HVT tenants: {len(databases)}")
            self.log.info(f"Total no. of doc insertion/collection: {load_volume}")
            self.log.info(f"HVT tenants: {db_ids}")
        random_string = ''.join(random.choices(ascii_letters + digits, k=10))
        key_prefix = f'{self.doc_prefix}{random_string}'
        with ThreadPoolExecutor() as executor:
            tasks = []
            for database in databases:
                task = executor.submit(self.load_data_new_doc_loader, databases=[database], doc_start=0,
                                       dataset=self.dataset, doc_end=load_volume, key_prefix=key_prefix,
                                       ops_rate=ops_rate)
                tasks.append(task)
            for task in tasks:
                task.result()

    def populate_tenant_category(self, db_ids=None, lvt_weightage=0, hvt_weightage=0, svt_weightage=0):
        """
        @summary: Distributes tenant into LVT, HVT and SVT according to weightage passed
        """
        if db_ids is None:
            db_ids = list(self.databases.keys())
        if lvt_weightage == 0:
            lvt_weightage = self.num_lvt
        if hvt_weightage == 0:
            hvt_weightage = self.num_hvt
        if svt_weightage == 0:
            svt_weightage = self.num_svt
        self.lvt_ids.update(db_ids[:lvt_weightage])
        self.hvt_ids.update(db_ids[lvt_weightage:lvt_weightage + hvt_weightage])
        self.svt_ids.update(db_ids[lvt_weightage + hvt_weightage: lvt_weightage + hvt_weightage + svt_weightage])

    def add_tenants_during_test_run(self):
        with ThreadPoolExecutor() as executor:
            old_db_ids = set(self.databases.keys())
            self.provision_databases(count=self.new_tenants_count,
                                     start_num=self.num_of_tenants,
                                     seed=self.database_prefix)
            new_db_ids = list(set(self.databases.keys()) - old_db_ids)
            self.log.info(f"New Tenants Ids - {new_db_ids}")
            self.populate_tenant_category(db_ids=new_db_ids, lvt_weightage=1,
                                          hvt_weightage=1, svt_weightage=1)
            self.num_of_tenants += self.new_tenants_count
            self.log.info(f"No. of tenants in cluster: {self.num_of_tenants}")

            new_dbs = [self.databases[db_id] for db_id in new_db_ids]
            lvt_dbs = [new_dbs[0]]
            self.num_of_scopes_per_db = self.lvt_scopes
            self.num_of_collections_per_scope = self.lvt_collections
            self.create_scopes_collections(databases=lvt_dbs)

            # Creating  scopes and collections for HVT DBs
            hvt_dbs = [new_dbs[1]]
            self.num_of_scopes_per_db = self.hvt_scopes
            self.num_of_collections_per_scope = self.hvt_collections
            self.create_scopes_collections(databases=hvt_dbs)

            # Creating  scopes and collections for SVT DBs
            svt_dbs = [new_dbs[2]]
            self.num_of_scopes_per_db = self.svt_scopes
            self.num_of_collections_per_scope = self.svt_collections
            self.create_scopes_collections(databases=svt_dbs)

            tasks = []
            for database in new_dbs:
                task = executor.submit(self.load_data_new_doc_loader, databases=[database],
                                       dataset=self.dataset,
                                       doc_start=0, doc_end=self.initial_doc_count,
                                       key_prefix=self.doc_prefix)
                tasks.append(task)

            new_db_dict = {db_id: self.databases[db_id] for db_id in new_db_ids}
            if not self.create_indexes_event.is_set():
                self.create_indexes_event.set()
            create_index_tasks = executor.submit(self.create_indexes_for_tenants, database_dict=new_db_dict)
            tasks.append(create_index_tasks)
            return tasks

    def check_cluster_state(self, timer=5):
        """
        @summary: Check if cluster is in balanced state or not
        """
        while self.stats_dump_event.is_set():
            try:
                state = self.get_cluster_balanced_state()
                if not state:
                    self.log.critical(f"Dataplane State {self.dataplane_id}: {state}")
            except Exception as err:
                self.log.critical(err)
            self.sleep(timer, f"Will dump cluster state stats after {timer}")

    def dump_cluster_nodes_info(self, timer=60):
        """
        @summary: Dumps the no. of nodes for each service in cluster
        """
        while self.stats_dump_event.is_set():
            try:
                table = TableView(self.log.info)
                table.set_headers(["KV Nodes", "Index Nodes", "Query Node"])
                self.index_nodes_in_cluster = len(self.get_nodes_from_services_map(service='index',
                                                                                   rest_info=self.dp_obj))
                self.kv_nodes_in_cluster = len(self.get_nodes_from_services_map(service='kv',
                                                                                rest_info=self.dp_obj))
                self.n1ql_nodes_in_cluster = len(self.get_nodes_from_services_map(service='n1ql',
                                                                                  rest_info=self.dp_obj))
                table.add_row([self.kv_nodes_in_cluster, self.index_nodes_in_cluster, self.n1ql_nodes_in_cluster])
                table.display("Cluster stats")
            except Exception as err:
                self.log.critical(err)
            self.sleep(timer)

    def dump_num_indexes_for_each_tenant(self, timer=600):
        while self.stats_dump_event.is_set():
            try:
                index_dict = {}
                consolidated_dict = {}
                table = TableView(self.log.info)
                table.set_headers(["Node", "Bucket", "scope", "collection", "num_of_indexes"])
                consolidated_table = TableView(self.log.info)
                consolidated_table.set_headers(["Node", "Bucket", "num_of_indexes"])
                index_node = random.choice(self.get_nodes_from_services_map(service='index', rest_info=self.dp_obj))
                rest = RestConnection(rest_username=self.dp_obj.admin_username,
                                      rest_password=self.dp_obj.admin_password, rest_srv=index_node)
                index_status = rest.get_indexer_metadata()['status']
                for index in index_status:
                    node = index["hosts"][0].split(":")[0]
                    bucket = index["bucket"]
                    scope = index["scope"]
                    collection = index["collection"]
                    key = f'{bucket}`$`{scope}`$`{collection}`$`{node}'  # used `$` as delimiter
                    consolidated_key = f'{bucket}`$`{node}'
                    if key in index_dict:
                        index_dict[key] += 1
                    else:
                        index_dict[key] = 1
                    if consolidated_key in consolidated_dict:
                        consolidated_dict[consolidated_key] += 1
                    else:
                        consolidated_dict[consolidated_key] = 1

                display_list = []
                for key, value in index_dict.items():
                    bucket, scope, collection, node = key.split("`$`")
                    display_list.append([node, bucket, scope, collection, value])
                display_list = sorted(display_list, key=lambda x: x[1])
                for item in display_list:
                    node, bucket, scope, collection, count = item
                    table.add_row([node, bucket, scope, collection, count])
                table.display("Tenant Index count stats")

                consolidated_list = []
                for key, value in consolidated_dict.items():
                    bucket, node = key.split("`$`")
                    consolidated_list.append([node, bucket, value])
                consolidated_list = sorted(consolidated_list, key=lambda x: x[1])
                for item in consolidated_list:
                    node, bucket, total_indexes = item
                    consolidated_table.add_row([node, bucket, total_indexes])
                consolidated_table.display("Tenant consolidated Index count stats")
            except Exception as err:
                self.log.critical(f"Error occurred during index counting for tenants - {err}")
            self.sleep(timer, f"Will dump Index stats after {timer}")

    def dump_gsi_threshold_stats(self, timer=600):
        """
        @summary: dumping GSI threshold stats after every timer interval
        """
        while self.stats_dump_event.is_set():
            try:
                table = TableView(self.log.info)
                table.set_headers(["Node",
                                   "memory_quota",
                                   "memory_used_actual",
                                   "num_indexes",
                                   "num_tenants",
                                   "units_quota",
                                   "units_used_actual"])
                result = self.get_all_index_node_usage_stats(print_stats=False)
                for node, value in result.items():
                    table.add_row([node,
                                   value["memory_quota"],
                                   value["memory_used_actual"],
                                   value["num_indexes"],
                                   value["num_tenants"],
                                   value["units_quota"],
                                   value["units_used_actual"]])
                table.display("Index Threshold Stats")
            except Exception as err:
                self.log.critical(err)
            self.sleep(timer, f"Will dump Index stats after {timer}")

    def dump_query_threshold_stats(self, timer=600):
        """
        @summary: dumping GSI threshold stats after every timer interval
        """
        while self.stats_dump_event.is_set():
            self.n1ql_nodes_below30 = 0
            self.n1ql_nodes_above60 = 0
            self.scale_up_n1ql = False
            self.scale_down_n1ql = False
            try:
                n1ql_table = TableView(self.log.info)
                n1ql_table.set_headers(["Dataplane",
                                        "Node",
                                        "load_factor",
                                        "queued",
                                        "active"])
                result = self.get_all_query_node_usage_stats()
                for node, content in result.items():
                    n1ql_table.add_row([
                        self.dataplane_id,
                        node,
                        str(content["load_factor.value"]),
                        str(content["queued_requests.value"]),
                        str(content["active_requests.value"])
                    ])
                    if content["load_factor.value"] >= 60:
                        self.n1ql_nodes_above60 += 1
                    elif content["load_factor.value"] < 30:
                        self.n1ql_nodes_below30 += 1
                n1ql_table.display("Query Node Stats")
                self.log.info(f"N1QL Nodes Above 60: {self.n1ql_nodes_above60}")
                self.log.info(f"N1QL Nodes Below 30: {self.n1ql_nodes_below30}")
            except Exception as err:
                self.log.critical(err)
            query_nodes = self.get_all_query_nodes(rest_info=self.dp_obj)
            if self.scale_down_n1ql is False and self.scale_up_n1ql is False and self.n1ql_cooling is False:
                if self.n1ql_nodes_above60 == len(query_nodes):
                    self.scale_up_n1ql = True
                elif self.n1ql_nodes_below30 == len(query_nodes) and len(query_nodes) >= 4:
                    self.scale_down_n1ql = True
            self.sleep(timer, f"Will dump query stats after {timer}")

    def dump_query_stats(self, timer=600):
        """
        @summary: dumping GSI threshold stats after every timer interval
        """
        while self.stats_dump_event.is_set():
            try:
                table = TableView(self.log.info)
                table.set_headers(["query_type", "total_count", "failed_count", "success_count"])
                stats = self.query_run_stats_dict
                for query_type, result in stats.items():
                    table.add_row([query_type, result["total_count"], result["failed_count"], result["success_count"]])

                table.display("Query Stats")
            except Exception as err:
                self.log.critical(err)
            self.sleep(timer, f"Will dump  Query stats after {timer}")

    def dump_index_node_stats(self, timer=600):
        """
        @summary: Dump stats for index nodes
        """
        while self.stats_dump_event.is_set():
            self.nodes_below_LWM = 0
            self.nodes_above_LWM = 0
            self.nodes_above_HWM = 0
            self.scale_down_nodes = 0
            table = TableView(self.log.info)
            table.set_headers(["Node",
                               "num_tenants",
                               "num_indexes",
                               "memory_used_actual",
                               "units_used_actual/units_quota",
                               "5 min Avg Mem",
                               "5 min Avg Units"])
            try:
                indexer_node_stats = self.get_all_index_node_usage_stats(print_stats=False)
                for node, value in indexer_node_stats.items():
                    if node not in self.last_5_mins:
                        self.last_5_mins[node] = deque([(value['memory_used_percentage'],
                                                         value['units_used_percentage'])],
                                                       maxlen=5)
                    else:
                        self.last_5_mins[node].append((value['memory_used_percentage'],
                                                       value['units_used_percentage']))
                    avg_mem_used = round(sum([consumption[0] for consumption in self.last_5_mins[node]]) /
                                         len(self.last_5_mins[node]), 2)
                    avg_units_used = round(sum([consumption[1] for consumption in self.last_5_mins[node]]) /
                                           len(self.last_5_mins[node]), 2)
                    if avg_mem_used > 70 or avg_units_used > 50:
                        self.nodes_above_HWM += 1
                    elif avg_mem_used > 45 or avg_units_used > 36:
                        self.nodes_above_LWM += 1
                    elif avg_mem_used < 40 or avg_units_used < 32:
                        self.nodes_below_LWM += 1

                    table.add_row([
                        node,
                        str(indexer_node_stats[node]["num_tenants"]),
                        str(indexer_node_stats[node]["num_indexes"]),
                        str(indexer_node_stats[node]["memory_used_actual"]),
                        f'{indexer_node_stats[node]["units_used_actual"]} / {indexer_node_stats[node]["units_quota"]}',
                        f'{avg_mem_used}%',
                        f'{avg_units_used}%'
                    ])
                table.display("Index Node Stats")
                self.log.info(f"GSI - Nodes below LWM: {self.nodes_below_LWM}")
                self.log.info(f"GSI - Nodes above LWM: {self.nodes_above_LWM}")
                self.log.info(f"GSI - Nodes above HWM: {self.nodes_above_HWM}")
            except Exception as err:
                self.log.critical(err)
            self.sleep(timer, f"Will dump Index stats after {timer}")

    def dump_defrag_stats(self, timer=600):
        """
        @summary: Dump defrag API stats
        """
        while self.stats_dump_event.is_set():
            defrag_table = TableView(self.log.info)
            defrag_table.set_headers(["Node",
                                      "num_tenants",
                                      "num_index_repaired",
                                      "memory_used_actual",
                                      "units_used_actual"])
            try:
                result = self.get_defrag_response(rest_info=self.dp_obj)
                for node, gsi_stat in result.items():
                    defrag_table.add_row([node,
                                          gsi_stat["num_tenants"],
                                          gsi_stat["num_index_repaired"],
                                          gsi_stat["memory_used_actual"],
                                          gsi_stat["units_used_actual"]
                                          ])
                defrag_table.display("Defrag Stats")
                if not self.index_scale_down and not self.index_scale_up and \
                        not self.gsi_auto_rebl and not self.gsi_cooling:
                    num_tenant_0 = 0
                    nodes_below_15_tenants = 0
                    nodes_below_lwm_defrag = 0
                    for node, gsi_stat in result.items():
                        if gsi_stat["num_tenants"] == 0:
                            num_tenant_0 += 1
                        elif gsi_stat["num_tenants"] <= 15 \
                                and gsi_stat["memory_used_actual"] / self.mem_quota < 40 \
                                and gsi_stat["units_used_actual"] / self.units_quota < 32:
                            nodes_below_15_tenants += 1
                        if gsi_stat["memory_used_actual"] / self.mem_quota < 40 \
                                and gsi_stat["units_used_actual"] / self.units_quota < 32:
                            nodes_below_lwm_defrag += 1
                        if gsi_stat["num_index_repaired"] > 0:
                            self.log.info(f"{node} have indexes to be repaired")
                            self.gsi_auto_rebl = True
                            self.log.info("GSI - Auto-Rebalance should trigger in a while as num_index_repaired > 0")
                            continue
                    self.log.info("Nodes below LWM from the defrag API: %s" % nodes_below_lwm_defrag)
                    index_nodes = self.get_nodes_from_services_map(service='index', rest_info=self.dp_obj)
                    if num_tenant_0 > 0 and nodes_below_15_tenants == len(index_nodes) - num_tenant_0 and \
                            len(index_nodes) - num_tenant_0 >= 2:
                        self.index_scale_down = True
                        self.scale_down_nodes = num_tenant_0
                        self.log.info("GSI - Scale DOWN should trigger in a while")
                    if self.nodes_above_HWM > 1 and self.nodes_below_LWM > 1:
                        if nodes_below_lwm_defrag == index_nodes:
                            self.gsi_auto_rebl = True
                            self.log.info("GSI - Auto-Rebalance should trigger in a while")
                        elif len(index_nodes) < 10:
                            self.index_scale_up = True
                            self.log.info("(RULE2) GSI - Scale UP should trigger in a while")
                    if self.nodes_above_LWM == len(index_nodes) or self.nodes_above_HWM == len(index_nodes):
                        if len(index_nodes) < 10:
                            self.index_scale_up = True
                            self.log.info("(RULE1) GSI - Scale UP should trigger in a while")
            except Exception as e:
                self.log.critical(e)
            self.sleep(timer, f"Will dump Defrag stats after {timer}")

    def check_ebs_scaling(self, timer=120):
        """
        1. check current disk used
        2. If disk used > 50% check for EBS scale on all nodes for that service
        """
        while self.stats_dump_event.is_set():
            table = TableView(self.log.info)
            table.set_headers(["Node",
                               "Path",
                               "TotalDisk",
                               "UsedDisk",
                               "% Disk Used"])
            kv_nodes = self.get_nodes_from_services_map(service='kv', rest_info=self.dp_obj)
            try:
                for node in kv_nodes:
                    data = RestConnection(rest_username=self.dp_obj.admin_username,
                                          rest_password=self.dp_obj.admin_password,
                                          rest_srv=node).get_nodes_self()
                    for storage in data.availableStorage:
                        if "cb" in storage.path:
                            table.add_row([
                                node.ip,
                                storage.path,
                                data.storageTotalDisk,
                                data.storageUsedDisk,
                                storage.usagePercent])
                            if storage.usagePercent > 90:
                                self.log.critical("Disk did not scale while\
                                    it is approaching full!!!")
                table.display("EBS  Stats")
            except Exception as err:
                self.log.critical(err)
            self.sleep(timer, f"Will check for EBS Scaling after {timer}")

    def check_memory_management(self, timer=120):
        """
        1. Check the database disk used
        2. Cal DGM based on ram/disk used and if it is < 1% wait for tunable
        """
        disk_list = [0, 50, 100, 150, 200, 250, 300, 350, 400, 450]
        memory = [256, 256, 384, 512, 640, 768, 896, 1024, 1152, 1280]

        while self.stats_dump_event.is_set():
            try:
                table = TableView(self.log.info)
                table.set_headers(["Node", "Bucket",
                                   "Total Ram(MB)",
                                   "Total Data(GB)",
                                   "Logical Data",
                                   "Items"])
                logical_data = defaultdict(int)
                kv_nodes = self.get_nodes_from_services_map(service='kv', rest_info=self.dp_obj)
                for node in kv_nodes:
                    _, stats = RestConnection(rest_username=self.dp_obj.admin_username,
                                              rest_password=self.dp_obj.admin_password,
                                              rest_srv=node).query_prometheus(
                        "kv_logical_data_size_bytes")
                    if stats["status"] == "success":
                        stats = [stat for stat in stats["data"]["result"] if stat["metric"]["state"] == "active"]
                        for stat in stats:
                            logical_data[stat["metric"]["bucket"]] += int(stat["value"][1])
                    for bucket in self.databases:
                        data = self.dp_rest.get_bucket_json(bucket)
                        ram_mb = data["quota"]["rawRAM"] / (1024 * 1024)
                        data_gb = data["basicStats"]["diskUsed"] / (1024 * 1024 * 1024)
                        items = data["basicStats"]["itemCount"]
                        logical_ddata_gb = logical_data[bucket] / (1024 * 1024 * 1024)
                        table.add_row([node, bucket, ram_mb, data_gb, logical_ddata_gb, items])
                        for i, disk in enumerate(disk_list):
                            if disk > logical_ddata_gb:
                                start = datetime.now()
                                while datetime.now() < start + timedelta(1200) and \
                                        ram_mb != memory[i - 1]:
                                    self.log.info(f"Wait for bucket: {bucket}"
                                                  f", Expected: {memory[i - 1]}, Actual: {ram_mb}")
                                    self.sleep(5)
                                    data = self.dp_rest.get_bucket_json(bucket)
                                    ram_mb = data["quota"]["rawRAM"] / (1024 * 1024)
                                    continue
                                if ram_mb != memory[i - 1]:
                                    self.log.critical(
                                        f"bucket: {bucket}, Expected: {memory[i - 1]}, Actual: {ram_mb}")
                                break
                table.display("Bucket Memory Statistics")
            except Exception as err:
                self.log.critical(err)
            self.sleep(timer, f"Will check for Memory stats after {timer}")

    def check_cluster_scaling(self, dataplane_id=None, service="kv", state="scaling"):
        """
        @summary: Checks the CP jobs for any scaling operation
        """
        self.lock.acquire()
        dataplane_id = dataplane_id or self.dataplane_id
        try:
            self.log.info("Dataplane Jobs:")
            resp = json.loads(self.api.serverless_api.get_dataplane_job_info(dataplane_id).content)
            self.log.info(pprint.pformat(resp))
            self.log.info("Scaling Records:")
            resp = json.loads(self.api.serverless_api.get_all_scaling_records(dataplane_id).content)
            self.log.info(pprint.pformat(resp))
        except Exception as err:
            self.log.info(f"Scaling records are empty {err}")
        dataplane_state = "healthy"
        try:
            resp = json.loads(self.api.serverless_api.get_serverless_dataplane_info(dataplane_id).content)
            dataplane_state = resp["couchbase"]["state"]
        except Exception as err:
            self.log.critical(err)
        scaling_timeout = 5 * 60 * 60
        while dataplane_state == "healthy" and scaling_timeout >= 0:
            dataplane_state = "healthy"
            try:
                resp = json.loads(self.api.serverless_api.get_serverless_dataplane_info(dataplane_id).content)
                dataplane_state = resp["couchbase"]["state"]
            except Exception as err:
                self.log.critical(err)
            self.log.info(f"Cluster state is: {dataplane_state}. Target: {state} for {service}")
            self.sleep(2)
            scaling_timeout -= 2

        scaling_timeout = 10 * 60 * 60
        while dataplane_state != "healthy" and scaling_timeout >= 0:
            dataplane_state = state
            try:
                resp = json.loads(self.api.serverless_api.get_serverless_dataplane_info(dataplane_id).content)
                dataplane_state = resp["couchbase"]["state"]
            except Exception as err:
                self.log.critical(err)
            self.log.info(f"Cluster state is: {dataplane_state}. Target: {state} for {service}")
            self.sleep(2)
            scaling_timeout -= 2

        self.log.info("Dataplane Jobs:")
        resp = json.loads(self.api.serverless_api.get_dataplane_job_info(dataplane_id).content)
        self.log.info(pprint.pformat(resp))
        self.log.info("Scaling Records:")
        resp = json.loads(self.api.serverless_api.get_all_scaling_records(dataplane_id).content)
        self.log.info(pprint.pformat(resp))
        self.sleep(10)
        self.lock.release()

    def check_kv_scaling(self, timer=60):
        """
        @summary: Checks for KV scaling
        """
        self.log.info("KV - Scale operation should trigger in a while.")
        while self.stats_dump_event.is_set():
            if self.check_jobs_entry("kv", "scalingService"):
                self.check_cluster_scaling(service="kv", state="scaling")
                break
            self.sleep(timer)

    def check_jobs_entry(self, service, operation):
        """
        @summary: Check CP jobs for a given operation for a given service and returns bool
        """
        jobs = loads(self.api.serverless_api.get_dataplane_job_info(self.dataplane_id).content)
        for job in jobs["clusterJobs"]:
            if job.get("payload"):
                if job["payload"].get("tags"):
                    self.log.info(f'{job["payload"].get("tags")}, {job["status"]}')
                    for details in job["payload"].get("tags"):
                        if details["key"] == operation and \
                                details["value"] == service and \
                                job["status"] == "processing":
                            return True
        return False

    def check_n1ql_scaling(self, timer=60):
        """
        @summary: Check for query scaling
        """
        self.n1ql_cooling_start = datetime.now()

        while self.stats_dump_event.is_set():
            try:
                if self.n1ql_cooling and self.n1ql_cooling_start + timedelta(900) > datetime.now():
                    self.log.info(f"N1QL is in cooling period for 15 mins after auto-scaling:"
                                  f" {self.n1ql_cooling_start + timedelta(900) - datetime.now()} pending")
                    self.sleep(60)
                    continue
                self.log.info("N1QL - Check for scale operation.")
                if self.scale_up_n1ql or self.scale_down_n1ql:
                    self.log.info("N1QL - Scale operation should trigger in a while.")
                    _time = datetime.now() + timedelta(30 * 60)
                    while _time > datetime.now():
                        if self.check_jobs_entry("n1ql", "scalingService"):
                            self.check_cluster_scaling(service="N1QL", state="scaling")
                            self.n1ql_cooling = True
                            self.n1ql_cooling_start = datetime.now()
                            break
                        self.sleep(10)
                    self.scale_up_n1ql, self.scale_down_n1ql = False, False
                    self.n1ql_nodes_below30 = 0
                    self.n1ql_nodes_above60 = 0
            except Exception as err:
                self.log.critical(err)
            self.sleep(timer)

    def check_gsi_scaling(self):
        """
        @summary: Checks for GSI scaling based on no. of tenants in a sub-cluster
        """
        while self.stats_dump_event.is_set():
            tenants = list()
            index_nodes = self.get_nodes_from_services_map(service='index', rest_info=self.dp_obj)
            for node in index_nodes:
                rest = RestConnection(rest_username=self.dp_obj.admin_username,
                                      rest_password=self.dp_obj.admin_password,
                                      rest_srv=node)
                resp = rest.get_all_index_stats()
                tenants.append(resp["num_tenants"])
            gsi_scaling = True
            for tenant in tenants:
                if tenant < 20:
                    gsi_scaling = False
                    break
            if gsi_scaling:
                prev_gsi_nodes = self.index_nodes_in_cluster
                self.log.info("Step: Test GSI Auto-Scaling due to num of GSI tenants per sub-cluster")
                self.check_cluster_scaling(service="gsi")
                curr_gsi_nodes = self.get_nodes_from_services_map(service='index', rest_info=self.dp_obj)
                if curr_gsi_nodes != prev_gsi_nodes + 2:
                    self.log.critical("GSI Auto-scaling didn't happen")

    def check_index_auto_scaling_rebl(self, timer=60):
        """
        @summary: Checks for Index auto-scaling due to LWM/HWM and auto-reblance of tenants
        """
        self.gsi_cooling_start = datetime.now()
        while self.stats_dump_event.is_set():
            try:
                if self.gsi_cooling and self.gsi_cooling_start + timedelta(900) > datetime.now():
                    self.log.info(f"GSI is in cooling period for 15 mins after auto-scaling:"
                                  f" {self.gsi_cooling_start + timedelta(900) - datetime.now()} pending")
                    self.sleep(60)
                    continue
                self.gsi_cooling = False
                self.log.info("Index - Check for LWM/HWM scale/defrag operation.")
                if self.index_scale_down or self.index_scale_up:
                    self.log.info("Index - Scale operation should trigger in a while.")
                    _time = datetime.now() + timedelta(30 * 60)
                    while _time > datetime.now():
                        if self.check_jobs_entry("index", "scalingService"):
                            self.check_cluster_scaling(service="GSI", state="scaling")
                            self.gsi_cooling = True
                            self.gsi_cooling_start = datetime.now()
                            self.index_scale_down, self.index_scale_up = False, False
                            break
                        self.log.critical("Index scalingService not found in /jobs")
                        self.sleep(10)
                elif self.gsi_auto_rebl:
                    self.log.info("Index - Rebalance operation should trigger in a while.")
                    _time = datetime.now() + timedelta(30 * 60)
                    while _time > datetime.now():
                        if self.check_jobs_entry("index", "rebalancingService"):
                            self.check_cluster_scaling(service="GSI", state="rebalancing")
                            self.gsi_cooling = True
                            self.gsi_cooling_start = datetime.now()
                            self.gsi_auto_rebl = False
                            break
                        self.log.critical("Index rebalancingService not found in /jobs")
                        self.sleep(10)
            except Exception as err:
                self.log.critical(err)
            self.sleep(timer)

    def running_pause_on_tenants(self):
        self.log.info("Pausing one of the Stale tenant")
        if not self.svt_ids:
            self.log.info("No Stale Tenants to Pause")
            return
        pause_tenant_db = self.svt_ids.pop()
        self.log.info(f"Pausing tenant: {pause_tenant_db}")
        try:
            self.api.pause_operation(database_id=pause_tenant_db, state='paused')
            self.paused_ids = pause_tenant_db
        except Exception as err:
            self.svt_ids.add(pause_tenant_db)
            self.log.fatal(f"Exception occurred during Pausing tenant: {pause_tenant_db}")
            self.log.error(err)

    def running_resume_on_tenants(self):
        self.log.info("Resume on the Paused tenant")
        if not self.paused_ids:
            self.log.info("No Tenants to Resume")
            return
        resume_tenant_db = self.paused_ids.pop()
        self.log.info(f"Resuming tenant: {resume_tenant_db}")
        try:
            self.api.resume_operation(database_id=resume_tenant_db, state="healthy")
            self.svt_ids.add(resume_tenant_db)
        except Exception as err:
            self.paused_ids.add(resume_tenant_db)
            self.log.fatal(f"Exception occurred during Resuming tenant: {resume_tenant_db}")
            self.log.error(err)

    def volume_test(self):
        with ThreadPoolExecutor() as executor:
            try:
                executor.submit(self.dump_gsi_threshold_stats, timer=self.stats_dump_timer)
                executor.submit(self.dump_index_node_stats, timer=self.stats_dump_timer)
                executor.submit(self.dump_query_threshold_stats, timer=self.stats_dump_timer)
                executor.submit(self.dump_query_stats, timer=self.stats_dump_timer)
                executor.submit(self.dump_defrag_stats, timer=self.stats_dump_timer)
                executor.submit(self.dump_cluster_nodes_info, timer=self.stats_dump_timer)
                executor.submit(self.check_cluster_state, timer=self.stats_dump_timer)
                executor.submit(self.check_ebs_scaling, timer=self.stats_dump_timer)
                executor.submit(self.dump_num_indexes_for_each_tenant, timer=self.stats_dump_timer)
                executor.submit(self.check_memory_management, timer=self.stats_dump_timer)
                executor.submit(self.check_kv_scaling)
                executor.submit(self.check_n1ql_scaling)
                executor.submit(self.check_gsi_scaling)
                executor.submit(self.check_index_auto_scaling_rebl)

                self.provision_databases(count=self.num_of_tenants, seed=self.database_prefix)
                self.populate_tenant_category()
                self.query_event.set()
                self.create_indexes_event.set()

                # Initial provisioning of cluster
                # creating scopes and collection in each of the databases
                # Creating  scopes and collections for LVT DBs
                lvt_dbs = [self.databases[db_id] for db_id in self.lvt_ids]
                self.num_of_scopes_per_db = self.lvt_scopes
                self.num_of_collections_per_scope = self.lvt_collections
                self.create_scopes_collections(databases=lvt_dbs)

                # Creating  scopes and collections for HVT DBs
                hvt_dbs = [self.databases[db_id] for db_id in self.hvt_ids]
                self.num_of_scopes_per_db = self.hvt_scopes
                self.num_of_collections_per_scope = self.hvt_collections
                self.create_scopes_collections(databases=hvt_dbs)

                # Creating  scopes and collections for SVT DBs
                svt_dbs = [self.databases[db_id] for db_id in self.svt_ids]
                self.num_of_scopes_per_db = self.svt_scopes
                self.num_of_collections_per_scope = self.svt_collections
                self.create_scopes_collections(databases=svt_dbs)

                # creating indexes for each collection and generating initial doc load
                # 1 batch for each LVT and SVT and 2 batches for HVT

                tasks = []
                for database in self.databases.values():
                    task = executor.submit(self.load_data_new_doc_loader, databases=[database], dataset=self.dataset,
                                           doc_start=0, doc_end=self.initial_doc_count, key_prefix=self.doc_prefix)
                    tasks.append(task)

                for task in tasks:
                    task.result()

                # setting svt weightage to 1 to have initial index load if svt weightage is -
                if self.svt_weightage == 0:
                    self.svt_weightage = 1
                    self.create_indexes_for_tenants(databases_dict=self.databases, continous_run=False)
                    self.svt_weightage = 0
                else:
                    self.create_indexes_for_tenants(databases_dict=self.databases, continous_run=False)
                # setting index and query event for dumping stats after every periodically
                self.run_select_query_load(run_time=2)

                # Starting Progressive Iterations
                # Starting the time for Volume test
                curr_time = datetime.now()
                logs_dump_time = datetime.now()
                end_time = curr_time + timedelta(seconds=self.test_run_duration)
                iteration_count = 1

                executor.submit(self.create_indexes_for_tenants, skip_primary=True)
                executor.submit(self.run_select_query_load)
                executor.submit(self.drop_indexes_from_overflown_tenants)
                # Running continuous query_load
                force_kill = False
                while curr_time < end_time:
                    if force_kill:
                        break
                    # starting the iteration
                    self.log.info("*" * 50)
                    self.log.info(f"Starting iteration no.: {iteration_count}")
                    self.log.info("*" * 50)
                    iteration_start_time = datetime.now()
                    if not self.query_event.is_set():
                        self.query_event.set()
                        executor.submit(self.run_select_query_load)
                    if not self.create_indexes_event.is_set():
                        self.create_indexes_event.set()
                        executor.submit(self.create_indexes_for_tenants, skip_primary=True)

                    # add more tenants every time below condition satisfy
                    add_tenant_task = None
                    if iteration_count % self.new_tenant_iter == 0:
                        self.log.info("Adding new tenants to cluster")
                        add_tenant_task = executor.submit(self.add_tenants_during_test_run)

                    # Populating data load for tenants
                    lvt_task = executor.submit(self.generate_doc_load_for_tenant_category, ops_rate=1000,
                                               tenant_type='lvt', load_volume=self.lvt_data_load)
                    if self.svt_data_load > 0:
                        svt_task = executor.submit(self.generate_doc_load_for_tenant_category, ops_rate=100,
                                                   tenant_type='svt', load_volume=self.svt_data_load)

                    hvt_task = executor.submit(self.generate_doc_load_for_tenant_category, ops_rate=1000,
                                               tenant_type='hvt', load_volume=self.hvt_data_load)

                    try:
                        lvt_task.result()
                    except Exception as err:
                        self.log.critical(f"Exception in LVT data load {err}")
                    try:
                        if self.svt_data_load > 0:
                            svt_task.result()
                    except Exception as err:
                        self.log.critical(f"Exception in SVT data load {err}")
                    try:
                        hvt_task.result()
                    except Exception as err:
                        self.log.critical(f"Exception in HVT data load {err}")

                    if iteration_count % self.new_tenant_iter == 0:
                        try:
                            add_tenant_task.result()
                        except Exception as err:
                            self.log.critical(f"Exception while adding new tenants: {err}")

                    # Running cbcollect periodically for given time duration
                    if datetime.now() > logs_dump_time + timedelta(seconds=self.cb_collect_duration):
                        executor.submit(self.collect_log_on_dataplane_nodes)
                        logs_dump_time = datetime.now()

                    if iteration_count % self.pause_iter == 0:
                        self.running_pause_on_tenants()

                    if iteration_count % (self.pause_iter + 2) == 0:
                        self.running_resume_on_tenants()

                    curr_time = datetime.now()
                    self.log.info("*" * 50)
                    self.log.info(f"Ending iteration no.: {iteration_count}")
                    self.log.info("*" * 50)
                    iteration_execution_time = (datetime.now() - iteration_start_time).total_seconds()
                    self.log.info(f"Iteration Execution time : {iteration_execution_time}")
                    if iteration_count % self.new_tenant_iter == 0:
                        self.sleep(15*60, "Cool Down period for all Tenant")
                    iteration_count += 1

            except Exception as err:
                raise Exception(f"Unsuccessful Test run {err}")
            finally:
                self.query_event.clear()
                self.create_indexes_event.clear()
                self.stats_dump_event.clear()
                self.log.info("GSI Volume Test Finished.")

    def test_delete_all_dbs(self):
        self.delete_all_database(all_db=True, dataplane_id=self.dataplane_id)
