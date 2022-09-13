from typing import Dict
import unittest
import requests
from TestInput import TestInputSingleton
from lib.capella.utils import CapellaAPI, CapellaCredentials, ServerlessDatabase
import lib.capella.utils as capella_utils
import logger
from tasks.task import CreateServerlessDatabaseTask
from tasks.taskmanager import TaskManager
import logging
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.cluster import Cluster as Cluster_helper

class ServerlessBaseTestCase(unittest.TestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.log = logger.Logger.get_logger()
        self.log.setLevel(logging.DEBUG)
        self.api = CapellaAPI(CapellaCredentials(self.input.capella))
        self.task_manager = TaskManager("task_manager")
        self.task_manager.start()
        self.databases: Dict[str, ServerlessDatabase] = {}
        self.sdk_clusters: Dict[str, Cluster] = {}
        self.cluster = Cluster_helper()

    def tearDown(self):
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
        self.task_manager.shutdown(force=True)

    def create_database(self):
        task = self.create_database_async()
        return task.result()

    def create_database_async(self):
        config = capella_utils.create_serverless_config(self.input)
        task = CreateServerlessDatabaseTask(self.api, config, self.databases)
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

    def load_database(self, database, kv_gen=None):
        #Not working yet.
        if not kv_gen:
            kv_gen = SDKDataLoader(num_ops=0, percent_create=0, percent_update=0, percent_delete=0,
                 load_pattern="uniform", start_seq_num=1, key_prefix="doc_", key_suffix="_",
                 scope="_default", collection="_default", json_template="Person", doc_expiry=0, fields_to_update=None,
                 doc_size=500, get_sdk_logs=False, username=database.access_key, password=database.secret_key, timeout=1000,
                 start=0, end=0, op_type="create", all_collections=False, es_compare=False, es_host=None, es_port=None,
                 es_login=None, es_password=None, output=False, upd_del_shift=0 , shuffle_docs=False)
        tasks = self._async_load_all_buckets(database, kv_gen)
        for task in tasks:
            if task:
                task.result()

    def _async_load_all_buckets(self, database, kv_gen):
        tasks = []
        tasks.append(self.cluster.async_load_gen_docs(database.srv, database.id, kv_gen, pause_secs=1,
                                                      timeout_secs=300))
        return tasks

    def run_query(self, database, query, query_params=None, use_sdk=False, **kwargs):
        if use_sdk:
            cluster = self.get_sdk_cluster(database.id)
            row_iter = cluster.query(query)
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
            api = f"https://{database.nebula}:18093/query/service"
            query_params["statement"] = query
            self.log.info(f'EXECUTE QUERY against {database.nebula}: {query}')
            resp = requests.post(api, params=query_params, auth=(database.access_key, database.secret_key))
            resp.raise_for_status()
            return resp.json()