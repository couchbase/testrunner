from typing import Dict
import unittest
import requests
from TestInput import TestInputSingleton
from lib.capella.utils import CapellaAPI, CapellaCredentials, ServerlessDatabase, ServerlessDataPlane
import lib.capella.utils as capella_utils
import logger
from tasks.task import CreateServerlessDatabaseTask
from tasks.taskmanager import TaskManager
import logging
from couchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator, QueryOptions
from couchbase.bucket import Bucket
from couchbase_helper.documentgenerator import SDKDataLoader
from membase.api.serverless_rest_client import ServerlessRestConnection as RestConnection
from couchbase_helper.cluster import Cluster as Cluster_helper
from TestInput import TestInputServer
import time
from membase.api.exception import CBQError
import ast, json


class ServerlessBaseTestCase(unittest.TestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.log = logger.Logger.get_logger()
        self.log.setLevel(logging.DEBUG)
        self.api = CapellaAPI(CapellaCredentials(self.input.capella))
        self.task_manager = TaskManager("task_manager")
        self.task_manager.start()
        self.databases: Dict[str, ServerlessDatabase] = {}
        self.dataplanes: Dict[str, ServerlessDataPlane] = {}
        self.sdk_clusters: Dict[str, Cluster] = {}
        self.cluster = Cluster_helper()
        self.num_of_tenants = self.input.param("num_of_tenants", 1)
        self.drop_indexes = self.input.param("drop_indexes", False)
        self.num_of_scopes_per_db = self.input.param("num_of_scopes_per_db", 2)
        self.num_of_collections_per_scope = self.input.param("num_of_collections_per_scope", 2)
        self.trigger_log_collect = self.input.param("trigger_log_collect", False)
        self.multitenant_run = True if self.num_of_tenants > 1 else False
        self.use_sdk = self.input.param("use_sdk", False)
        self.num_of_indexes_per_tenant = self.input.param("num_of_indexes_per_tenant", 20)
        self.create_bypass_user = self.trigger_log_collect or self.input.param("create_bypass_user", False)
        self.skip_import_sample = self.input.param("skip_import_sample", True)
        self.log.info(f"Create bypass user {self.create_bypass_user}")
        self.log.info(f"Trigger log collect {self.trigger_log_collect}")
        self.capella_run = self.input.param("capella_run", False)
        self.num_of_docs_per_collection = self.input.param("num_of_docs_per_collection", 10000)

    def tearDown(self):
        if self._testMethodName not in ['suite_tearDown', 'suite_setUp'] and self.trigger_log_collect:
            test_failed = self.has_test_failed()
            if test_failed:
                self.log.info(f"Test failure: {self._testMethodName}. Will trigger a log collect via REST API")
                for dataplane in self.dataplanes:
                    rest_obj = RestConnection(rest_username=self.dataplanes[dataplane].admin_username,
                                              rest_password=self.dataplanes[dataplane].admin_password,
                                              rest_srv=self.dataplanes[dataplane].rest_host)
                    cb_collect_list = rest_obj.collect_logs(test_name=self._testMethodName)
                    self.log.info(f"Test failure. Cbcollect info list {cb_collect_list}")
        self.delete_all_database()
        self.task_manager.shutdown(force=True)

    def has_test_failed(self):
        if hasattr(self._outcome, 'errors'):
            # Python 3.4 - 3.10
            result = self.defaultTestResult()
            self._feedErrorsToResult(result, self._outcome.errors)
        else:
            # Python 3.11+
            result = self._outcome.result
        ok = all(test != self for test, text in result.errors + result.failures)
        if ok:
            return False
        else:
            self.log.info('Errors/failures seen during test execution of {}. Errors {} and Failures {}'.format(
                self._testMethodName,
                result.errors,
                result.failures))
            return True

    def create_database(self):
        task = self.create_database_async()
        return task.result()

    def create_database_async(self):
        config = capella_utils.create_serverless_config(self.input, self.skip_import_sample)
        task = CreateServerlessDatabaseTask(api=self.api, config=config, databases=self.databases,
                                            dataplanes=self.dataplanes, create_bypass_user=self.create_bypass_user)
        self.task_manager.schedule(task)
        return task

    def get_sdk_bucket(self, database_id) -> Bucket:
        cluster = self.get_sdk_cluster(database_id)
        return cluster.bucket(database_id)

    def get_sdk_cluster(self, database_id) -> Cluster:
        cluster = self.sdk_clusters.get(database_id)
        if cluster:
            return cluster
        else:
            database = self.databases[database_id]
            skip_tls_verify = "" if database.srv.endswith(
                "cloud.couchbase.com") else "?tls_verify=none"
            cluster = Cluster("couchbases://{}{}".format(database.srv, skip_tls_verify),
                              ClusterOptions(PasswordAuthenticator(database.access_key, database.secret_key)))
            self.sdk_clusters[database_id] = cluster
            return cluster

    def load_databases(self, load_all_databases=False, database_obj=None, doc_template="Person", num_of_docs=1000,
                       scope="_default", collection="_default"):
        """
        load_all_databases: for multi-tenant tests, do you want data load to happen to all DBs?
        database_obj : database obj
        doc_template: The json template for the data load
        num_of_docs: No of documents. For a multi-tenant DB, this will populate <num_of_docs> docs to each DB
        """
        tasks = []
        if load_all_databases:
            databases_to_be_loaded = self.databases.values()
        else:
            databases_to_be_loaded = [database_obj]
        for database in databases_to_be_loaded:
            kv_gen = SDKDataLoader(num_ops=num_of_docs, percent_create=100, percent_update=0, percent_delete=0,
                                   load_pattern="uniform", start_seq_num=1, key_prefix="doc_", key_suffix="_",
                                   scope=scope, collection=collection, json_template=doc_template, doc_expiry=0,
                                   fields_to_update=None,
                                   doc_size=500, get_sdk_logs=False, username=database.access_key,
                                   password=database.secret_key, timeout=1000,
                                   start=0, end=0, op_type="create", all_collections=False, es_compare=False,
                                   es_host=None, es_port=None,
                                   es_login=None, es_password=None, output=False, upd_del_shift=0, shuffle_docs=False,
                                   capella=True)
            server = TestInputServer()
            server.ip = database.srv
            tasks.append(self.cluster.async_load_gen_docs(server, database.id, kv_gen, pause_secs=1,
                                                          timeout_secs=300))
        for task in tasks:
            if task:
                task.result()

    def provision_databases(self, count=1):
        self.log.info(f'PROVISIONING {count} DATABASES ...')
        tasks = []
        for _ in range(0, count):
            task = self.create_database_async()
            tasks.append(task)
        for task in tasks:
            task.result()

    def delete_all_database(self, count=1):
        for database_id in self.databases:
            try:
                self.log.info("deleting serverless database {}".format(
                    {"database_id": database_id}))
                self.api.delete_serverless_database(database_id)
            except Exception as e:
                msg = {
                    "database_id": database_id,
                    "error": str(e)
                }
                self.log.warn("failed to delete database {}".format(msg))
        for database_id in self.databases:
            self.api.wait_for_database_deleted(database_id)
            self.log.info("serverless database deleted {}".format(
                {"database_id": database_id}))

    def delete_database(self, database_id):
        try:
            self.log.info("deleting serverless database {}".format(
                {"database_id": database_id}))
            self.api.delete_serverless_database(database_id)
        except Exception as e:
            msg = {
                "database_id": database_id,
                "error": str(e)
            }
            self.log.warn("failed to delete database {}".format(msg))
        self.api.wait_for_database_deleted(database_id)
        self.log.info("serverless database deleted {}".format(
            {"database_id": database_id}))

    def override_width_and_weight(self, database_id, width, weight):
        override = {"width": width, "weight": weight}
        return self.api.override_width_and_weight(database_id, override)

    def run_query(self, database, query, query_params=None, use_sdk=False, query_node=None, **kwargs):
        """
        By default runs against nebula endpoint. In case, you need to use a specific query node,
        the query node hostname needs to be passed. This also requires create_bypass_user to be True
        **kwargs:
        username - if you want to run the queries with a custom username
        password - if you want to run the queries with a custom password
        Eg: self.run_query(database_obj, query="select 1", username= 'Administrator', password='password')
        """
        username, password = None, None
        if use_sdk:
            cluster = self.get_sdk_cluster(database.id)
            row_iter = cluster.query(query, QueryOptions(query_context=f"default:{database.id}"))
            return row_iter.rows()
        else:
            if not query_params:
                query_params = {'query_context': f"default:{database.id}"}
                for key, value in kwargs.items():
                    if key == "txtimeout":
                        query_params['txtimeout'] = value
                    if key == "txnid":
                        query_params['txid'] = ''"{0}"''.format(value)
                    if key == "scan_consistency":
                        query_params['scan_consistency'] = value
                    if key == "scan_vector":
                        query_params['scan_vector'] = str(value).replace("'", '"')
                    if key == "timeout":
                        query_params['timeout'] = value
                    if key == "username":
                        username = value
                    if key == "password":
                        password = value
            if not query_node:
                api = f"https://{database.nebula}:18093/query/service"
                verify = True
            else:
                api = f"https://{query_node}:18093/query/service"
                verify = False
            if username is None and password is None:
                auth = (database.access_key, database.secret_key)
            else:
                auth = (username, password)
            query_params["statement"] = query
            self.log.info(f'EXECUTE QUERY against {api}: Query: {query} using creds: {auth}')
            self.log.debug(f"Run Query statement payload {query_params}")
            resp = requests.post(api, params=query_params, auth=auth, timeout=120, verify=verify)
            resp.raise_for_status()
            if 'billingUnits' in resp.json().keys():
                self.log.info(f"BILLING UNITS from query: {resp.json()['billingUnits']}")
            if 'errors' in resp.json():
                self.log.error(f"Error from query execution: {resp.json()['errors']}")
                raise CBQError(resp.json(), database.nebula)
            return resp.json()

    def cleanup_database(self, database_obj):
        self.run_query(query="DROP scope sample if exists", database=database_obj)

    def create_scope(self, database_obj, scope_name, use_sdk=False):
        if use_sdk:
            pass
        else:
            self.run_query(query="Create scope default:`{}`.{}".format(database_obj.id, scope_name),
                           database=database_obj)

    def create_collection(self, database_obj, scope_name, collection_name, use_sdk=False):
        if use_sdk:
            pass
        else:
            resp = self.run_query(query="Create collection default:`{}`.{}.{}".format(database_obj.id, scope_name, collection_name), database=database_obj)
            self.log.info(f"Response for collection creation : {resp}")

    def delete_collection(self, database_obj, scope_name, collection_name, use_sdk=False):
        if use_sdk:
            pass
        else:
            resp = self.run_query(query="DROP COLLECTION default:`{}`.{}.{}".format(database_obj.id, scope_name, collection_name), database=database_obj)
            self.log.info(f"Response for collection deletion : {resp}")

    def drop_all_indexes(self, database):
        indexes = self.run_query(database, f'SELECT `namespace`, name,\
            CASE WHEN bucket_id is missing THEN keyspace_id ELSE bucket_id END as `bucket`,\
            CASE WHEN scope_id is missing THEN "_default" ELSE scope_id END as `scope`,\
            CASE WHEN bucket_id is missing THEN "_default" ELSE keyspace_id END as `collection`\
            FROM system:indexes')
        for index in indexes['results']:
            result = self.run_query(database, f"DROP INDEX {index['namespace']}:`{index['bucket']}`.`{index['scope']}`.`{index['collection']}`.`{index['name']}`")

    def check_index_status(self, database, name, timeout = 30, desired_state='online'):
        current_state = 'offline'
        stop = time.time() + timeout
        while (current_state != desired_state and time.time() < stop):
            result = self.run_query(database, f'SELECT raw state FROM system:indexes WHERE name = "{name}"')
            current_state = result['results'][0]
            self.log.info(f'INDEX {name} state: {current_state}')
        if current_state != desired_state:
            self.fail(f'INDEX {name} state: {current_state}, fail to reach state: {desired_state} within timeout: {timeout}')

    def get_nodes_from_services_map(self, database, service):
        rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
        service_nodes = []
        nodes_obj = rest_obj.get_all_dataplane_nodes()
        self.log.debug(f"Dataplane nodes object {nodes_obj}")
        for node in nodes_obj:
            if service in node['services']:
                node = node['hostname'].split(":")[0]
                service_nodes.append(node)
        return service_nodes

    def process_CBQE(self, s, index=0):
        '''
        return json object {'code':12345, 'msg':'error message'}
        '''
        content = ast.literal_eval(str(s).split("ERROR:")[1])
        json_parsed = json.loads(json.dumps(content))
        return json_parsed['errors'][index]