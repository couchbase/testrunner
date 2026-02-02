import logging
import random
import string
from threading import Event

import requests
import time
import copy
import os
import subprocess

from gsi.serverless.base_query_serverless import QueryBaseServerless
from membase.api.serverless_rest_client import ServerlessRestConnection as RestConnection
from TestInput import TestInputServer
from lib.Cb_constants.CBServer import CbServer
from couchbase_helper.documentgenerator import SDKDataLoader
from serverless.gsi_utils import GSIUtils
from lib.capella.utils import ServerlessDataPlane
from concurrent.futures import ThreadPoolExecutor

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
        self.num_of_load_cycles = self.input.param("num_of_load_cycles", 10000)
        self.num_of_index_creation_batches = self.input.param("num_of_index_creation_batches", 1)
        self.dataset = self.input.param("dataset", "Hotel")
        self.region = self.input.capella['region']
        self.s3_bucket = self.input.param("s3_bucket", "gsi-onprem-test")
        self.storage_prefix = self.input.param("storage_prefix", None)
        self.definition_list = self.gsi_util_obj.get_index_definition_list(dataset=self.dataset)
        if self.aws_access_key_id:
            from serverless.s3_utils import S3Utils
            self.s3_utils_obj = S3Utils(aws_access_key_id=self.aws_access_key_id,
                                        aws_secret_access_key=self.aws_secret_access_key,
                                        s3_bucket=self.s3_bucket, region=self.region)
        CbServer.capella_run = True

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

    def get_indexer_metadata(self, indexer_node, rest_info):
        admin_username, admin_password = rest_info.admin_username, rest_info.admin_password
        endpoint = "https://{}:18091/indexStatus".format(indexer_node)
        resp = requests.get(endpoint, auth=(admin_username, admin_password), verify=False)
        resp.raise_for_status()
        indexer_metadata = resp.json()['indexes']
        self.log.debug(f"Indexer metadata {indexer_metadata}")
        return indexer_metadata

    def get_resident_host_for_index(self, index_name, indexer_node, rest_info):
        index_metadata = self.get_indexer_metadata(indexer_node=indexer_node, rest_info=rest_info)
        for index in index_metadata:
            if index['index'] == index_name:
                # changing log level to debug temporarily. TODO remove this once stabilised
                self.log.info(f"Index metadata for index {index_name} is {index}")
                return index['hosts']

    def get_resident_host_for_tenant(self, tenant, indexer_node, rest_info):
        rest_obj = RestConnection(rest_info.admin_username, rest_info.admin_password, rest_info.rest_host)
        nodes_obj = rest_obj.get_all_dataplane_nodes()
        self.log.debug(f"Dataplane nodes object {nodes_obj}")
        for node in nodes_obj:
            if 'index' in node['services']:
                indexer_node = node['hostname'].split(":")[0]
                self.log.info(f"Will use this indexer node to obtain metadata {indexer_node}")
                break
        index_metadata = self.get_indexer_metadata(indexer_node=indexer_node, rest_info=rest_info)
        index_hosts = set()
        for index in index_metadata:
            if index['bucket'] == tenant:
                index_hosts.add(index['hosts'][0])
                if len(list(index_hosts)) == 2:
                    return list(index_hosts)

    def get_all_tenants_on_host(self, host, rest_info):
        index_metadata = self.get_indexer_metadata(indexer_node=host, rest_info=rest_info)
        tenants = set()
        for index in index_metadata:
            if index['hosts'][0].split(":")[0] == host:
                tenants.add(index['bucket'])
        return list(tenants)

    def get_count_of_indexes_for_tenant(self, database_obj):
        query = "select * from system:indexes"
        results = self.run_query(query=query, database=database_obj)['results']
        return len(results)

    def wait_until_indexes_online(self, rest_info, index_name, keyspace, timeout=20, use_rest=True):
        if use_rest:
            rest_obj = RestConnection(rest_info.admin_username, rest_info.admin_password, rest_info.rest_host)
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
                index_metadata = self.get_indexer_metadata(rest_info=rest_info, indexer_node=indexer_node)
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
                raise Exception(f"Index {index_name}  not online despite waiting for {timeout} mins")
            if not index_replica_online:
                raise Exception(
                    f"Replica of {index_name} not online despite waiting for {timeout} mins")
        else:
            index_online = False
            time_now = time.time()
            index_name = f"{index_name}".rstrip("`").lstrip("`")
            database_name = keyspace.split(".")[0].replace("`", "")
            database = self.databases[database_name]
            while time.time() - time_now < timeout * 60:
                index_metadata = self.run_query(database=database, query="select * from system:indexes")
                for index in index_metadata['results']:
                    if index['indexes']['name'] == index_name and index['indexes']['state'] == 'online':
                        index_online = True
                if index_online:
                    break
                time.sleep(30)
            if not index_online:
                raise Exception(f"Index {index_name}  not online despite waiting for {timeout} mins")

    def check_if_index_exists(self, database_obj, index_name):
        query = f"select * from system:indexes"
        results = self.run_query(query=query, database=database_obj)['results']
        for result in results:
            if result['indexes']['name'] == index_name and result['indexes']['bucket_id'] == database_obj.id:
                return True
        return False

    def prepare_all_databases(self, databases=None):
        self.log.debug("In prepare_all_databases method. Will load all the databases with variable num of documents")
        databases = self.databases if not databases else databases
        self.create_scopes_collections(databases=databases.values())
        for database in self.databases.values():
            namespaces = []
            for scope in database.collections:
                collection_lists = [f'`{database.id}`.{scope}.{collection}' for collection in database.collections[scope]]
                namespaces.extend(collection_lists)
            self.gsi_util_obj.index_operations_during_phases(namespaces=namespaces, dataset=self.dataset,
                                                             capella_run=True, database=database,
                                                             num_of_batches=self.num_of_index_creation_batches,
                                                             defer_build_mix=True)
            database.namespaces = namespaces

    def prepare_databases(self):
        for database in self.databases.values():
            namespaces = []
            for scope in database.collections:
                collection_lists = [f'`{database.id}`.{scope}.{collection}' for collection in database.collections[scope]]
                namespaces.extend(collection_lists)
            self.gsi_util_obj.index_operations_during_phases(namespaces=namespaces, dataset=self.dataset,
                                                             capella_run=True, database=database,
                                                             num_of_batches=self.num_of_index_creation_batches,
                                                             defer_build_mix=True)
            database.namespaces = namespaces
        doc_end = 0
        time_before = time.time()
        self.log.info(f"Timestamp before data load:{time_before}")
        for i in range(self.num_of_load_cycles):
            if self.use_new_doc_loader:
                doc_start = doc_end
                doc_end = doc_start + self.total_doc_count
                self.log.info(f"populate_data_until_threshold iteration {i}. doc_start {doc_start} doc_end {doc_end}")
                self.load_data_new_doc_loader(databases=self.databases.values(), doc_start=doc_start, doc_end=doc_end)
            else:
                self.populate_data(doc_template=self.dataset, serverless_run=self.capella_run, batch_size=10000,
                                   databases=self.databases)
            self.get_all_index_node_usage_stats()
        time_after = time.time()
        self.log.info(f"Timestamp after data load:{time_after}. Time taken {time_after - time_before}")

    def populate_data_until_threshold(self, databases, use_new_doc_loader=False, doc_template=None,
                                      batch_size=10000, serverless_run=True):
        if not doc_template:
            doc_template = self.dataset
        doc_end = 0
        for i in range(self.num_of_load_cycles):
            if use_new_doc_loader:
                doc_start = doc_end
                doc_end = doc_start + self.total_doc_count
                self.log.info(f"populate_data_until_threshold iteration {i}. doc_start {doc_start} doc_end {doc_end}")
                self.load_data_new_doc_loader(databases=databases, doc_start=doc_start, doc_end=doc_end)
            else:
                self.populate_data(doc_template=doc_template, serverless_run=serverless_run, batch_size=batch_size,
                                   databases=databases)
            all_node_stats = self.get_all_index_node_usage_stats()
            self.log.info(f"Index nodes list: {all_node_stats.keys()}")
            for node in all_node_stats.keys():
                mem_quota = all_node_stats[node]['memory_quota']
                memory_used_actual = all_node_stats[node]['memory_used_actual']
                units_quota = all_node_stats[node]['units_quota']
                units_used_actual = all_node_stats[node]['units_used_actual']
                num_tenants = all_node_stats[node]['num_tenants']
                self.log.info(f"Index stats for {node} are memory_quota: {mem_quota}. "
                              f"Memory used actual {memory_used_actual}."
                              f"units_quota {units_quota} "
                              f"units_used_actual {units_used_actual} num of tenants {num_tenants} ")
                self.log.info(f"Memory used ratio: {memory_used_actual/mem_quota}. \n "
                              f"Units used ratio : {units_used_actual/units_quota} \n Num. of tenants:{num_tenants}")

    def create_scopes_collections(self, databases, scope_collection_map=None):
        self.log.info("Creating scopes and collections")
        for database in databases:
            scope_coll_dict, coll_count = {}, 0
            if not scope_collection_map:
                for item in range(self.num_of_scopes_per_db):
                    scope_name = f'scope_num_{item}_{random.randint(0, 1000)}'
                    self.log.info(f"Will create scope with name {scope_name}")
                    self.create_scope(database_obj=database, scope_name=scope_name)
                    collection_list = []
                    for counter in range(self.num_of_collections_per_scope):
                        collection_name = f'collection_num_{item}_{random.randint(0, 1000)}'
                        self.log.info(f"Will create collection with name {scope_name}")
                        self.create_collection(database_obj=database, scope_name=scope_name, collection_name=collection_name)
                        coll_count += 1
                        collection_list.append(collection_name)
                        if scope_name in database.collections:
                            database.collections[scope_name].append(collection_name)
                        else:
                            database.collections[scope_name] = [collection_name]
                    scope_coll_dict[scope_name] = collection_list
                self.log.info(f"Scope and collection list:{scope_coll_dict}. No. of collections: {coll_count}")
            else:
                for scope in scope_collection_map.keys():
                    self.create_scope(database_obj=database, scope_name=scope)
                    for collection in scope_collection_map[scope]:
                        self.create_collection(database_obj=database, scope_name=scope,
                                               collection_name=collection)
                database.collections = scope_collection_map


    def populate_data(self, batch_size=10000, databases=None, doc_template='default', serverless_run=True,
                      output=False):
        tasks = []
        databases = self.databases if not databases else databases
        for index, database in enumerate(databases):
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

    def get_index_settings(self, indexer_node, rest_info):
        endpoint = "https://{}:18091/settings?internal=ok".format(indexer_node)
        resp = requests.get(endpoint, auth=(rest_info.admin_username, rest_info.admin_password), verify=False)
        resp.raise_for_status()
        result = resp.json()
        self.log.debug(f"settings {result}")
        return result

    def get_index_stats(self, indexer_node, rest_info):
        endpoint = "https://{}:19102/stats".format(indexer_node)
        resp = requests.get(endpoint, auth=(rest_info.admin_username, rest_info.admin_password), verify=False)
        resp.raise_for_status()
        index_stats = resp.json()
        self.log.debug(f"Indexer metadata {index_stats}")
        return index_stats

    def populate_data_till_threshold(self, database_obj, doc_template='default', threshold=50):
        rest_info = self.create_rest_info_obj(username=database_obj.admin_username, password=database_obj.admin_password,
                                              rest_host=database_obj.rest_host)
        index_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
        node_stats = {}
        for index_node in index_nodes:
            node_stats[index_node] = self.get_index_stats(indexer_node=index_node, rest_info=rest_info)
        self.populate_data(doc_template=doc_template)

    def get_all_index_node_usage_stats(self, print_stats=True):
        node_wise_stats = {}
        for dataplane in self.dataplanes.values():
            if not dataplane:
                dataplane = ServerlessDataPlane(self.dataplane_id)
                rest_api_info = self.api.get_access_to_serverless_dataplane_nodes(dataplane_id=self.dataplane_id)
                dataplane.populate(rest_api_info)
            rest_info = self.create_rest_info_obj(dataplane.admin_username, dataplane.admin_password, dataplane.rest_host)
            index_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
            nodes = {}
            for node in index_nodes:
                nodes[node] = self.get_index_stats(indexer_node=node,
                                                   rest_info=rest_info)
            if print_stats:
                self.log.info(f"Index nodes list: {nodes.keys()}")
            for node in nodes.keys():
                mem_quota = nodes[node]['memory_quota']
                memory_used_actual = nodes[node]['memory_used_actual']
                units_quota = nodes[node]['units_quota']
                units_used_actual = nodes[node]['units_used_actual']
                num_tenants = nodes[node]['num_tenants']
                num_indexes = nodes[node]['num_indexes']
                memory_used_percentage = memory_used_actual / mem_quota * 100
                units_used_percentage = units_used_actual / units_quota * 100
                if print_stats:
                    self.log.info(f"Index stats for {node} are memory_quota: {mem_quota}. "
                                  f"Memory used actual {memory_used_actual}."
                                  f"units_quota {units_quota} "
                                  f"units_used_actual {units_used_actual} num of tenants {num_tenants} ")
                    self.log.info(f"Memory used %: {memory_used_percentage} . \n "
                                  f"Units used % : {units_used_percentage} "
                                  f"\n Num. of tenants:{num_tenants}")
                node_wise_stats.update({node: {"memory_quota": mem_quota,
                                               "memory_used_actual": memory_used_actual,
                                               "units_quota": units_quota,
                                               "units_used_actual": units_used_actual,
                                               "num_tenants": num_tenants,
                                               "num_indexes": num_indexes,
                                               "memory_used_percentage": memory_used_percentage,
                                               "units_used_percentage": units_used_percentage}})
        return node_wise_stats

    def scale_up_index_subcluster(self, dataplane, wait_for_rebalance_completion=True):
        rest_info = self.create_rest_info_obj(username=dataplane.admin_username,
                                              password=dataplane.admin_password,
                                              rest_host=dataplane.rest_host)
        index_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
        num_nodes = len(index_nodes)
        self.log.info(f"Number of indexer nodes in the DP: {num_nodes} on dataplane with ID {dataplane.id}")
        self.update_specs(dataplane_id=dataplane.id, new_count=num_nodes+2, service='index',
                          wait_for_rebalance_completion=wait_for_rebalance_completion)

    def scale_down_index_subcluster(self, dataplane, wait_for_rebalance_completion=True):
        rest_info = self.create_rest_info_obj(username=dataplane.admin_username,
                                              password=dataplane.admin_password,
                                              rest_host=dataplane.rest_host)
        index_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
        num_nodes = len(index_nodes)
        self.log.info(f"Number of indexer nodes in the DP: {num_nodes}")
        self.log.info(f"Will remove 2 new indexer nodes")
        if num_nodes == 2:
            self.log.error("Cannot scale down the index subcluster further since it only has 2 nodes")
            raise Exception("Cannot scale down since there are only 2 nodes")
        self.update_specs(dataplane_id=dataplane.id, new_count=num_nodes-2, service='index',
                          wait_for_rebalance_completion=wait_for_rebalance_completion)

    def reset_dataplane(self, dataplane_id):
        resp = self.api.get_dataplane_debug_info(dataplane_id=dataplane_id)
        current_specs = resp['couchbase']['specs']
        new_specs = copy.deepcopy(current_specs)
        for counter, spec in enumerate(current_specs):
            if spec['services'][0]['type'] == "index" and spec['count'] != 2:
                new_specs[counter]['count'] = 2
            if spec['services'][0]['type'] == "n1ql" and spec['count'] != 2:
                new_specs[counter]['count'] = 2
            if spec['services'][0]['type'] == "kv" and spec['count'] != 3:
                new_specs[counter]['count'] = 3
        self.log.info(f"Update_specs will be run on {dataplane_id}")
        self.log.info(f"Will use this config to update specs: {new_specs}")
        self.api.modify_cluster_specs(dataplane_id=dataplane_id, specs=new_specs)

    def load_data_across_all_tenants(self, databases, doc_start, doc_end):
        cur_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', ))
        pom_path = os.path.join(cur_dir, r"magma_loader/DocLoader")
        os.chdir(pom_path)
        cmd_list = []
        for database in databases:
            for scope in database.collections:
                for collection in database.collections[scope]:
                    command = f"mvn compile exec:java -Dexec.cleanupDaemonThreads=false " \
                              f"-Dexec.mainClass='couchbase.test.sdk.Loader' -Dexec.args='-n {database.srv} " \
                              f"-user {database.access_key} -pwd {database.secret_key} -b {database.id} " \
                              f"-p 11207 -create_s {doc_start} -create_e {doc_end} " \
                              f"-cr 100 -up 0 -rd 0 -workers 1 -docSize 1024 " \
                              f"-scope {scope} -collection {collection}'"
                    self.log.info(
                        f"Will run this command {command} to load data for db {database.id} scope {scope} collection {collection}")
                    cmd_list.append(command)
        tasks = []
        with ThreadPoolExecutor() as executor:
            for command in cmd_list:
                task = executor.submit(subprocess.Popen, args=command, shell=True,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                tasks.append(task)
            for task in tasks:
                task.result()

    def run_parallel_workloads(self, event, total_doc_count=None):
        if total_doc_count is None:
            total_doc_count = self.total_doc_count
        i = 0
        while not event.is_set():
            with ThreadPoolExecutor() as executor:
                tasks = []
                task = executor.submit(self.load_data_new_doc_loader, databases=self.databases.values(), doc_start=0,
                                       doc_end=total_doc_count, update_rate=100, create_rate=0)
                tasks.append(task)
                print(f"Iteration number {i}")
                for counter, database in enumerate(self.databases.values()):
                    for scope in database.collections:
                        for collection in database.collections[scope]:
                            select_query_list = self.gsi_util_obj.get_select_queries(definition_list=self.definition_list,
                                                                                     namespace=f"`{database.id}`.{scope}.{collection}")
                            for select_query in select_query_list:
                                task = executor.submit(self.run_query, database=database, query=select_query)
                                tasks.append(task)
                for task in tasks:
                    task.result()
                i += 1
        self.log.info("Workload function complete. Exiting")

    def get_all_index_names(self, database, scope=None, collection=None):
        response = self.run_query(database=database, query="select * from system:indexes")['results']
        name_list = []
        for item in response:
            if scope is not None and collection is not None:
                if item['indexes']['scope_id'] == scope and item['indexes']['keyspace_id'] == collection:
                    name_list.append(item['indexes']['name'])
            else:
                name_list.append(item['indexes']['name'])
        return name_list

    def get_fast_rebalance_config(self, indexer_node, rest_info):
        """
        returns a tuple of storage_scheme, bucket_name, storage_prefix
        """
        admin_username, admin_password = rest_info.admin_username, rest_info.admin_password
        endpoint = "https://{}:19102/settings".format(indexer_node)
        resp = requests.get(endpoint, auth=(admin_username, admin_password), verify=False)
        resp.raise_for_status()
        indexer_settings = resp.json()
        self.log.debug(f"Indexer metadata {indexer_settings}")
        bucket_name = indexer_settings["indexer.settings.rebalance.blob_storage_bucket"]
        storage_prefix = indexer_settings["indexer.settings.rebalance.blob_storage_prefix"]
        storage_scheme = indexer_settings["indexer.settings.rebalance.blob_storage_scheme"]
        return storage_scheme, bucket_name, storage_prefix

    def drop_all_indexes_on_tenant(self, database):
        indexes = self.run_query(database, f'SELECT `namespace`, name,\
                    CASE WHEN bucket_id is missing THEN keyspace_id ELSE bucket_id END as `bucket`,\
                    CASE WHEN scope_id is missing THEN "_default" ELSE scope_id END as `scope`,\
                    CASE WHEN bucket_id is missing THEN "_default" ELSE keyspace_id END as `collection`\
                    FROM system:indexes')
        for index in indexes['results']:
            if index['bucket'] == database.id:
                self.run_query(database, f"DROP INDEX {index['namespace']}:`{index['bucket']}`.`{index['scope']}`."
                                         f"`{index['collection']}`.`{index['name']}`")

    def get_index_status(self, rest_info, index_name, keyspace):
        rest_obj = RestConnection(rest_info.admin_username, rest_info.admin_password, rest_info.rest_host)
        nodes_obj = rest_obj.get_all_dataplane_nodes()
        self.log.debug(f"Dataplane nodes object {nodes_obj}")
        indexer_node = None
        for node in nodes_obj:
            if 'index' in node['services']:
                indexer_node = node['hostname'].split(":")[0]
                break
        if indexer_node is not None:
            index_metadata = self.get_indexer_metadata(rest_info=rest_info, indexer_node=indexer_node)
            for index in index_metadata:
                if index['index'] == index_name and keyspace in index['definition']:
                    return index['status']
        else:
            raise Exception("Unable to fetch index node from the dataplane nodes object")

    # TODO refactor all get_indexer_metadata calls to get_index_metadata
    def get_index_metadata(self, rest_info):
        admin_username, admin_password = rest_info.admin_username, rest_info.admin_password
        rest_obj = RestConnection(rest_info.admin_username, rest_info.admin_password, rest_info.rest_host)
        nodes_obj = rest_obj.get_all_dataplane_nodes()
        self.log.debug(f"Dataplane nodes object {nodes_obj}")
        indexer_node = None
        for node in nodes_obj:
            if 'index' in node['services']:
                indexer_node = node['hostname'].split(":")[0]
                break
        endpoint = "https://{}:18091/indexStatus".format(indexer_node)
        resp = requests.get(endpoint, auth=(admin_username, admin_password), verify=False)
        resp.raise_for_status()
        indexer_metadata = resp.json()['indexes']
        self.log.debug(f"Indexer metadata {indexer_metadata}")
        return indexer_metadata

    def build_all_indexes(self, databases):
        build_index_query_template = "build index on keyspacename (( select raw name from system:all_indexes where " \
                                     "`using`='gsi' and '`' || `bucket_id` || '`.`' || `scope_id` || '`.`' || " \
                                     "`keyspace_id` || '`' = 'keyspacename' and state = 'deferred'))"
        with ThreadPoolExecutor() as executor:
            tasks = []
            for database in databases:
                for scope in database.collections:
                    for collection in database.collections[scope]:
                        keyspace = f"`{database.id}`.`{scope}`.`{collection}`"
                        query = build_index_query_template.replace("keyspacename", keyspace)
                        self.log.info(f"Build index query for db {database.id} scope {scope} collection {collection} is {query}")
                        task = executor.submit(self.run_query, database=database, query=query)
                        tasks.append(task)
            for task in tasks:
                try:
                    task.result()
                except:
                    pass

    def are_all_indexes_online(self, rest_info):
        rest_obj = RestConnection(rest_info.admin_username, rest_info.admin_password, rest_info.rest_host)
        nodes_obj = rest_obj.get_all_dataplane_nodes()
        self.log.debug(f"Dataplane nodes object {nodes_obj}")
        indexer_node_list = []
        for node in nodes_obj:
            if 'index' in node['services']:
                indexer_node = node['hostname'].split(":")[0]
                self.log.info(f"Will use this indexer node to obtain metadata {indexer_node}")
                indexer_node_list.append(indexer_node)
        for indexer_node in indexer_node_list:
            index_metadata = self.get_indexer_metadata(rest_info=rest_info, indexer_node=indexer_node)
            for index in index_metadata:
                if index['status'] == 'Ready':
                    continue
                self.log.info(f"Index {index['index']} is not yet ready. Index state is {index['status']}")
                return False
            return True

    def get_index_metadata_stats(self, rest_info, return_only_persisted=False):
        if return_only_persisted:
            stats_required = ["last_known_scan_time"]
        else:
            stats_required = ['items_count',
                              'key_size_distribution',
                              'raw_data_size',
                              'backstore_raw_data_size',
                              'arrkey_size_distribution',
                              "last_known_scan_time"]
        indexer_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="index")
        result = {}
        for node in indexer_nodes:
            stat_data = self.get_index_stats(node, rest_info=rest_info)
            for stat, value in stat_data.items():
                for req_stat in stats_required:
                    if f':{req_stat}' in stat:
                        result[stat] = value
        return result

    def get_defrag_response(self, rest_info):
        rest_obj = RestConnection(rest_info.admin_username, rest_info.admin_password, rest_info.rest_host)
        nodes_obj = rest_obj.get_all_dataplane_nodes()
        self.log.debug(f"Dataplane nodes object {nodes_obj}")
        indexer_node = None
        for node in nodes_obj:
            if 'index' in node['services']:
                indexer_node = node['hostname'].split(":")[0]
                break
        if indexer_node is not None:
            api = "https://{}:18091/pools/default/services/index/defragmented".format(indexer_node)
            resp = requests.get(api, auth=(rest_info.admin_username, rest_info.admin_password), verify=False)
            resp.raise_for_status()
            return resp.json()
        else:
            raise Exception("Unable to fetch index node from the dataplane nodes object")

    def get_cluster_balanced_state(self):
        """
        @summary: returns cluster's balanced state
        """
        kv_node = self.get_nodes_from_services_map(service='kv', rest_info=self.dp_obj)[0]
        try:
            content = RestConnection(rest_username=self.dp_obj.admin_username,
                                     rest_password=self.dp_obj.admin_password,
                                     rest_srv=kv_node).get_pools_default()
            return content["balanced"]
        except Exception as err:
            self.log.critical(f"{self.dataplane_id} /pools/default has failed!!! -  {err}")
