import copy
import os
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
import subprocess

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
        self.dataplane_id = self.input.capella.get("dataplane_id", None)
        self.use_new_doc_loader = self.input.param("use_new_doc_loader", False)
        self.teardown_all_databases = self.input.param("teardown_all_databases", True)
        self.create_dataplane = self.input.param("create_dataplane", False)
        self.create_dataplane_override = self.input.param("create_dataplane_override", False)
        self.new_dataplane_id = None
        if self._testMethodName not in ['suite_tearDown', 'suite_setUp'] and self.create_dataplane:
            overRide = None
            if self.create_dataplane_override:
                with open('pytests/serverless/config/dataplane_spec_config.json') as f:
                    overRide = json.load(f)
                    overRide['couchbase']['image'] = self.input.capella.get("image")
            self.new_dataplane_id = self.provision_dataplane(overRide)
            if self.new_dataplane_id is not None:
                self.dataplanes[self.new_dataplane_id] = ServerlessDataPlane(self.new_dataplane_id)
                rest_api_info = self.api.get_access_to_serverless_dataplane_nodes(dataplane_id=self.new_dataplane_id)
                self.dataplanes[self.new_dataplane_id].populate(rest_api_info)

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
        if self.teardown_all_databases:
            self.delete_all_database()
        if self.new_dataplane_id is not None:
            self.log.info(f"Deleting dataplane : {self.new_dataplane_id}")
            self.delete_dataplane(self.new_dataplane_id)
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

    def create_database_async(self, seed=None, dataplane_id=None):
        config = capella_utils.create_serverless_config(input=self.input, skip_import_sample=self.skip_import_sample,
                                                        seed=seed, dp_id=dataplane_id)
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
                                   doc_size=1024, get_sdk_logs=False, username=database.access_key,
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

    def provision_databases(self, count=1, seed=None, dataplane_id=None):
        self.log.info(f'PROVISIONING {count} DATABASES ...')
        tasks = []
        for i in range(0, count):
            if seed:
                task = self.create_database_async(seed=f"{seed}-{i}", dataplane_id=dataplane_id)
            else:
                task = self.create_database_async(dataplane_id=dataplane_id)
            tasks.append(task)
        for task in tasks:
            task.result()

    def provision_dataplane(self, overRide=None):
        self.log.info('PROVISIONING DATAPLANE ...')
        return self.api.create_dataplane_wait_for_ready(overRide)

    def delete_all_database(self, all_db=False):
        databases = None
        if all_db:
            databases = self.api.get_databases_id()
            if len(databases) == 0:
                return
        else:
            databases = self.databases

        for database_id in databases:
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
        for database_id in databases:
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

    def delete_dataplane(self, dataplane_id):
        self.api.delete_dataplane(dataplane_id)

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

    def get_nodes_from_services_map(self, service, rest_info):
        rest_obj = RestConnection(rest_info.admin_username, rest_info.admin_password, rest_info.rest_host)
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

    def load_data_new_doc_loader(self, databases, doc_start=0, doc_end=100000, create_rate=100, update_rate=0):
        # will be removed once DocLoader is a testrunner subdmodule
        cur_dir = os.getcwd()
        pom_path = os.path.join(cur_dir, r"magma_loader/DocLoader")
        os.chdir(pom_path)
        try:
            for database in databases:
                for scope in database.collections:
                    for collection in database.collections[scope]:
                        command = f"mvn compile exec:java -Dexec.cleanupDaemonThreads=false " \
                                  f"-Dexec.mainClass='couchbase.test.sdk.Loader' -Dexec.args='-n {database.srv} " \
                                  f"-user {database.access_key} -pwd {database.secret_key} -b {database.id} " \
                                  f"-p 11207 -create_s {doc_start} -create_e {doc_end} " \
                                  f"-cr {create_rate} -up {update_rate} -rd 0 -workers 1 -docSize 1024 " \
                                  f"-scope {scope} -collection {collection}'"
                        self.log.info(f"Will run this command {command} to load data")
                        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                        out, err = process.communicate()
                        if "BUILD SUCCESS" not in str(out):
                            self.log.error(f"Data load failure for bucket {database.id} scope {scope} collection {collection}")
        finally:
            os.chdir(cur_dir)

    def update_specs(self, dataplane_id, new_count=4, service='index', timeout=1200):
        resp = self.api.get_dataplane_deployment_status(dataplane_id=dataplane_id)
        old_count, num_nodes_after = 0, 0
        if "overRide" in resp['couchbaseCluster']:
            current_specs = resp['couchbaseCluster']['overRide']['specs']
        else:
            current_specs = resp['couchbaseCluster']['specs']
        new_specs = copy.deepcopy(current_specs)
        for counter, spec in enumerate(current_specs):
            if spec['services'][0]['type'] == service and spec['count'] != new_count:
                old_count = spec['count']
                new_specs[counter]['count'] = new_count
        self.log.info(f"Will use this config to update specs: {new_specs}")
        self.api.modify_cluster_specs(dataplane_id=dataplane_id, specs=new_specs)
        time.sleep(30)
        time_now = time.time()
        while time_now < time.time() + timeout:
            if not self.dataplanes:
                dataplane = ServerlessDataPlane(dataplane_id=dataplane_id)
            else:
                dataplane = self.dataplanes[self.dataplane_id]
            rest_info = self.create_rest_info_obj(username=dataplane.admin_username,
                                                  password=dataplane.admin_password,
                                                  rest_host=dataplane.rest_host)
            nodes_after_scaling = self.get_nodes_from_services_map(rest_info=rest_info, service=service)
            num_nodes_after = len(nodes_after_scaling)
            if num_nodes_after == new_count:
                break
            time.sleep(30)
        if num_nodes_after != new_count:
            self.log.error(f"Scale operation did not happen despite waiting {timeout} seconds")
            raise Exception("Scaling operations failure")

    def create_rest_info_obj(self, username, password, rest_host):
        class Rest_Info:
            def __init__(self, username, password, rest_host):
                self.admin_username = username
                self.admin_password = password
                self.rest_host = rest_host
        rest_info = Rest_Info(username=username, password=password, rest_host=rest_host)
        return rest_info

    def get_all_fts_stats(self):
        stats_nodes_resp = self.api.get_fts_stats()
        return stats_nodes_resp
