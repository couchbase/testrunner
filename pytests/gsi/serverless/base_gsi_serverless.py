import logging
import requests
import time

from gsi.serverless.base_query_serverless import QueryBaseServerless
from membase.api.serverless_rest_client import ServerlessRestConnection as RestConnection

log = logging.getLogger(__name__)


class BaseGSIServerless(QueryBaseServerless):

    def setUp(self):
        super(BaseGSIServerless, self).setUp()
        self.num_of_docs_per_collection = 10000
        self.missing_index = self.input.param("missing_index", False)
        self.array_index = self.input.param("array_index", False)
        self.partitioned_index = self.input.param("partitioned_index", False)
        self.defer_build = self.input.param("defer_build", False)

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
        self.log.info(f"Deleting index {index_name} on DB with ID: {database_obj.id}. Index statement:{query_statement}")
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
        while time.time() - time_now < timeout*60:
            index_metadata = self.get_indexer_metadata(database_obj=database, indexer_node=indexer_node)
            for index in index_metadata:
                if index['index'] == index_name and keyspace in index['definition'] and index['status'] == 'Ready' :
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
            raise Exception(f"Replica of {index_name} not online on database {database} despite waiting for {timeout} mins")

    def check_if_index_exists(self, database_obj, index_name):
        query = f"select * from system:indexes where name={index_name}"
        results = self.run_query(query=query, database=database_obj)['results']
        return len(results) > 0
