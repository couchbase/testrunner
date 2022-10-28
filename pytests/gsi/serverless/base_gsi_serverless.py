import logging
import math
import random
import string

import requests
import time

from gsi.serverless.base_query_serverless import QueryBaseServerless
from membase.api.serverless_rest_client import ServerlessRestConnection as RestConnection
from TestInput import TestInputServer
from couchbase_helper.documentgenerator import SDKDataLoader
from serverless.gsi_utils import GSIUtils

log = logging.getLogger(__name__)


class BaseGSIServerless(QueryBaseServerless):

    def setUp(self):
        super(BaseGSIServerless, self).setUp()
        self.total_doc_count = self.input.param("total_doc_count", 100000)
        self.missing_index = self.input.param("missing_index", False)
        self.array_index = self.input.param("array_index", False)
        self.partitioned_index = self.input.param("partitioned_index", False)
        self.defer_build = self.input.param("defer_build", False)
        self.gsi_util_obj = GSIUtils(self.run_query)

    def tearDown(self):
        super(BaseGSIServerless, self).tearDown()

    def create_index(self, database_obj, query_statement, use_sdk=False):
        self.log.info(f"Creating index on DB with ID: {database_obj.id}. Index statement:{query_statement}")
        resp = self.run_query(database=database_obj, query=query_statement, use_sdk=use_sdk)
        if 'errors' in resp:
            self.log.error(f"Error while creating index {resp['errors']}")
            raise Exception(f"{resp['errors']}")
        return resp

    def drop_index(self, database_obj, index_name, use_sdk=False):
        query_statement = f'DROP INDEX  {index_name} on _default'
        self.log.info(
            f"Deleting index {index_name} on DB with ID: {database_obj.id}. Index statement:{query_statement}")
        resp = self.run_query(database=database_obj, query=query_statement, use_sdk=use_sdk)
        if 'errors' in resp:
            self.log.error(f"Error while creating index {resp['errors']}")
            raise Exception(f"{resp['errors']}")
        return resp

    def get_indexer_metadata(self, database_obj, indexer_node):
        endpoint = "https://{}:18091/indexStatus".format(indexer_node)
        resp = requests.get(endpoint, auth=(database_obj.admin_username, database_obj.admin_password), verify=False)
        resp.raise_for_status()
        indexer_metadata = resp.json()['indexes']
        self.log.debug(f"Indexer metadata {indexer_metadata}")
        return indexer_metadata

    def get_resident_host_for_index(self, index_name, database_obj, indexer_node):
        index_metadata = self.get_indexer_metadata(database_obj=database_obj, indexer_node=indexer_node)
        for index in index_metadata:
            if index['index'] == index_name:
                self.log.debug(f"Index metadata for index {index_name} is {index}")
                return index['hosts']

    def get_count_of_indexes_for_tenant(self, database_obj):
        query = "select * from system:indexes"
        results = self.run_query(query=query, database=database_obj)['results']
        return len(results)

    def wait_until_indexes_online(self, database, index_name, keyspace, timeout=20):
        rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
        indexer_node = None
        nodes_obj = rest_obj.get_all_dataplane_nodes()
        self.log.debug(f"Dataplane nodes object {nodes_obj}")
        for node in nodes_obj:
            if 'index' in node['services']:
                indexer_node = node['hostname'].split(":")[0]
                self.log.info(f"Will use this indexer node to obtain metadata {indexer_node}")
                break
        index_online, index_replica_online = False, False
        time_now = time.time()
        index_name = f"{index_name}".rstrip("`").lstrip("`")
        while time.time() - time_now < timeout * 60:
            index_metadata = self.get_indexer_metadata(database_obj=database, indexer_node=indexer_node)
            for index in index_metadata:
                if index['index'] == index_name and keyspace in index['definition'] and index['status'] == 'Ready':
                    index_online = True
                if index['index'] == f"{index_name} (replica 1)" and keyspace in index['definition'] \
                        and index['status'] == 'Ready':
                    index_replica_online = True
            if index_online and index_replica_online:
                break
            time.sleep(30)
        if not index_online:
            raise Exception(f"Index {index_name} on database {database} not online despite waiting for {timeout} mins")
        if not index_replica_online:
            raise Exception(
                f"Replica of {index_name} not online on database {database} despite waiting for {timeout} mins")

    def check_if_index_exists(self, database_obj, index_name):
        query = f"select * from system:indexes where name={index_name}"
        results = self.run_query(query=query, database=database_obj)['results']
        return len(results) > 0

    def prepare_all_databases(self, databases=None, doc_template="Employee", num_of_tenants=5, total_doc_count=100000,
                              batch_size=10000, serverless_run=True):
        self.log.debug("In prepare_all_databases method. Will load all the databases with variable num of documents")
        databases = self.databases if not databases else databases
        self.create_scopes_collections(databases=databases.values())
        heavy_load_tenant_count = math.ceil(num_of_tenants * 0.6)
        light_load_tenant_count = num_of_tenants - heavy_load_tenant_count
        self.doc_count_tenant_weights, count_heavy_tenants, count_light_tenants = [], heavy_load_tenant_count, light_load_tenant_count
        weight_heavy_tenants, weight_light_tenants = 80, 20
        for _ in range(heavy_load_tenant_count):
            tenant_weight = math.floor(weight_heavy_tenants / count_heavy_tenants)
            weight_heavy_tenants = weight_heavy_tenants - tenant_weight
            count_heavy_tenants = count_heavy_tenants - 1
            self.doc_count_tenant_weights.append(math.floor(tenant_weight * total_doc_count / 100))
        for _ in range(light_load_tenant_count):
            tenant_weight = math.floor(weight_light_tenants / count_light_tenants)
            weight_light_tenants = weight_light_tenants - tenant_weight
            count_light_tenants = count_light_tenants - 1
            self.doc_count_tenant_weights.append(math.floor(tenant_weight * total_doc_count / 100))
        self.log.info(f"No of heavily loaded tenants:{heavy_load_tenant_count} "
                      f"No. of lightly loaded tenants:{light_load_tenant_count}. "
                      f"Doc count for each of the databases {self.doc_count_tenant_weights}")
        for database in self.databases.values():
            namespaces = []
            for scope in database.collections:
                collection_lists = [f'`{database.id}`.{scope}.{collection}' for collection in database.collections[scope]]
                namespaces.extend(collection_lists)
            self.gsi_util_obj.index_operations_during_phases(namespaces=namespaces, dataset=doc_template,
                                                             capella_run=True, database=database)
            database.namespaces = namespaces
        self.populate_data(doc_template=doc_template, serverless_run=serverless_run, batch_size=batch_size,
                           databases=databases)

    def create_scopes_collections(self, databases):
        for database in databases:
            scope_coll_dict, coll_count = {}, 0
            for item in range(self.num_of_scopes_per_db):
                scope_name = f'scope_num_{item}_{random.randint(0, 1000)}'
                self.create_scope(database_obj=database, scope_name=scope_name)
                collection_list = []
                for counter in range(self.num_of_collections_per_scope):
                    collection_name = f'collection_num_{item}_{random.randint(0, 1000)}'
                    self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
                    coll_count += 1
                    collection_list.append(collection_name)
                    if scope_name in database.collections:
                        database.collections[scope_name].append(collection_name)
                    else:
                        database.collections[scope_name] = [collection_name]
                scope_coll_dict[scope_name] = collection_list
            self.log.info(f"Scope and collection list:{scope_coll_dict}. No. of collections: {coll_count}")

    def populate_data(self, batch_size=10000, databases=None, doc_template='default', serverless_run=True,
                      output=False):
        tasks = []
        databases = self.databases if not databases else databases
        for index, database in enumerate(databases.values()):
            database.doc_count = self.doc_count_tenant_weights[index]
            server = TestInputServer()
            server.ip = database.srv
            collection_count = 0
            for scope in database.collections:
                collection_count += len(database.collections[scope])
            key_prefix = f'doc_{"".join(random.choices(string.ascii_uppercase + string.digits, k=7))}_'
            for scope in database.collections:
                num_of_docs = int(self.doc_count_tenant_weights[index] / collection_count)
                collection_list = database.collections[scope]
                for collection in collection_list:
                    for start in range(0, num_of_docs, batch_size):
                        end = start + batch_size
                        if end >= num_of_docs:
                            end = num_of_docs
                        kv_gen = SDKDataLoader(num_ops=end - start, percent_create=100,
                                               start_seq_num=start + 1,
                                               scope=scope, collection=collection, json_template=doc_template,
                                               get_sdk_logs=True, username=database.access_key,
                                               password=database.secret_key, timeout=1000,
                                               start=start, end=end, key_prefix=key_prefix,
                                               output=output, capella=serverless_run)
                        tasks.append(self.cluster.async_load_gen_docs(server, database.id, kv_gen, pause_secs=1,
                                                                      timeout_secs=300))
        for task in tasks:
            if task:
                task.result()

    def get_index_settings(self, indexer_node, database_obj):
        endpoint = "https://{}:18091/settings?internal=ok".format(indexer_node)
        resp = requests.get(endpoint, auth=(database_obj.admin_username, database_obj.admin_password), verify=False)
        resp.raise_for_status()
        result = resp.json()
        self.log.debug(f"settings {result}")
        return result

    def get_index_stats(self, indexer_node, database_obj):
        endpoint = "https://{}:19102/stats".format(indexer_node)
        resp = requests.get(endpoint, auth=(database_obj.admin_username, database_obj.admin_password), verify=False)
        resp.raise_for_status()
        index_stats = resp.json()
        self.log.debug(f"Indexer metadata {index_stats}")
        return index_stats

    def populate_data_till_threshold(self, database_obj, doc_template='default', threshold=50):
        index_nodes = self.get_nodes_from_services_map(database=database_obj, service="index")
        node_stats = {}
        for index_node in index_nodes:
            node_stats[index_node] = self.get_index_stats(indexer_node=index_node, database_obj=database_obj)
        self.populate_data(doc_template=doc_template)

    def get_all_index_node_usage_stats(self, database):
        index_nodes = self.get_nodes_from_services_map(database=database, service="index")
        nodes = {}
        for node in index_nodes:
            nodes[node] = self.get_index_stats(indexer_node=node, database_obj=database)
        return nodes

