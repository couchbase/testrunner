import ast
import base64
import copy
import json
import logging
import math
import os
import random
import re
import string
import struct
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from string import ascii_letters, digits

import faiss
import numpy as np
import requests
from deepdiff import DeepDiff
from requests.auth import HTTPBasicAuth
from Cb_constants import CbServer
from collection.collections_cli_client import CollectionsCLI
from collection.collections_rest_client import CollectionsRest
from collection.collections_stats import CollectionsStats
from couchbase_helper.cluster import Cluster
from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.query_definitions import SQLDefinitionGenerator
from couchbase_helper.tuq_generators import TuqGenerators
from lib.membase.helper.cluster_helper import ClusterOperationHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import WIN_COUCHBASE_LOGS_PATH, LINUX_COUCHBASE_LOGS_PATH
from membase.api.rest_client import RestConnection, RestHelper
from serverless.gsi_utils import GSIUtils
from tasks.task import ConcurrentIndexCreateTask
from tasks.task import SDKLoadDocumentsTask
from .newtuq import QueryTests

log = logging.getLogger(__name__)


class BaseSecondaryIndexingTests(QueryTests):

    def setUp(self):
        super(BaseSecondaryIndexingTests, self).setUp()
        self.ansi_join = self.input.param("ansi_join", False)
        self.index_lost_during_move_out = []
        self.run_continous_query = False
        self.desired_rr = self.input.param("desired_rr", 10)
        self.verify_using_index_status = self.input.param("verify_using_index_status", False)
        self.use_replica_when_active_down = self.input.param("use_replica_when_active_down", True)
        self.use_where_clause_in_index = self.input.param("use_where_clause_in_index", False)
        self.scan_consistency = self.input.param("scan_consistency", "request_plus")
        self.scan_vector_per_values = self.input.param("scan_vector_per_values", None)
        self.alter_index_error = self.input.param("alter_index_error", None)
        self.timeout_for_index_online = self.input.param("timeout_for_index_online", 600)
        self.verify_query_result = self.input.param("verify_query_result", True)
        self.verify_explain_result = self.input.param("verify_explain_result", True)
        self.defer_build = self.input.param("defer_build", True)
        self.build_index_after_create = self.input.param("build_index_after_create", True)
        self.run_query_with_explain = self.input.param("run_query_with_explain", True)
        self.run_query = self.input.param("run_query", True)
        self.graceful = self.input.param("graceful", False)
        self.groups = self.input.param("groups", "all").split(":")
        self.use_rest = self.input.param("use_rest", False)
        self.plasma_dgm = self.input.param("plasma_dgm", False)
        self.bucket_name = self.input.param("bucket_name", "default")
        self.num_scopes = self.input.param("num_scopes", 1)
        self.index_load_three_pass = self.input.param("index_load_three_pass", "shard_capacity")
        self.num_collections = self.input.param("num_collections", 1)
        self.item_to_delete = self.input.param('item_to_delete', None)
        self.test_bucket = self.input.param('test_bucket', 'test_bucket')
        self.wait_for_scheduled_index = self.input.param('wait_for_scheduled_index', True)
        self.scheduled_index_create_rebal = self.input.param('scheduled_index_create_rebal', False)
        self.enable_dgm = self.input.param('enable_dgm', False)
        self.rest = RestConnection(self.master)
        self.collection_rest = CollectionsRest(self.master)
        self.collection_cli = CollectionsCLI(self.master)
        self.stat = CollectionsStats(self.master)
        self.scope_prefix = self.input.param('scope_prefix', 'test_scope')
        self.collection_prefix = self.input.param('collection_prefix', 'test_collection')
        self.initial_index_num = self.input.param("initial_index_num", 20)
        self.initial_index_batches = self.input.param("initial_index_batches", 2)
        self.run_cbq_query = self.n1ql_helper.run_cbq_query
        self.num_of_docs_per_collection = self.input.param('num_of_docs_per_collection', 10000)
        self.deploy_node_info = None
        self.server_grouping = self.input.param("server_grouping", None)
        self.partition_fields = self.input.param('partition_fields', None)
        self.partitoned_index = self.input.param('partitioned_index', False)
        self.json_template = self.input.param("json_template", "Person")
        self.magma_flask_host = self.input.param("magma_flask_host", '172.23.121.85')
        self.magma_flask_port = self.input.param("magma_flask_port", 5000)
        if self.partition_fields:
            self.partition_fields = self.partition_fields.split(',')
        self.num_partition = self.input.param('num_partition', 8)
        self.transfer_batch_size = self.input.param("transfer_batch_size", 20)
        self.rebalance_timeout = self.input.param("rebalance_timeout", 600)
        self.missing_field_desc = self.input.param("missing_field_desc", False)
        self.num_of_tenants = self.input.param("num_of_tenants", 10)
        self.aws_access_key_id = self.input.param("aws_access_key_id", None)
        self.aws_secret_access_key = self.input.param("aws_secret_access_key", None)
        self.region = self.input.param("region", "us-west-1")
        self.s3_bucket = self.input.param("s3_bucket", "gsi-onprem-test")
        self.download_vector_dataset = self.input.param("download_vector_dataset", False)
        self.vector_backup_filename = self.input.param("vector_backup_filename", "backup_zips/100K_car.zip")
        self.storage_prefix = self.input.param("storage_prefix", None)
        self.index_batch_weight = self.input.param("index_batch_weight", 1)
        self.server_group_map = {}
        self.password = self.input.membase_settings.rest_password
        self.username = self.input.membase_settings.rest_username
        self.cancel_rebalance = self.input.param("cancel_rebalance", False)
        self.fail_rebalance = self.input.param("fail_rebalance", False)
        self.rebalance_type = self.input.param("rebalance_type", None)
        self.use_shard_based_rebalance = self.input.param("use_shard_based_rebalance", False)
        self.use_cbo = self.input.param("use_cbo", False)
        self.xattr_indexes = self.input.param("xattr_indexes", False)
        self.create_query_node_pattern = r"create .*?index (.*?) on .*?nodes':.*?\[(.*?)].*?$"
        # current value of n1ql_feat_ctrl disables sequential scan. To enable it set value to 0x4c
        self.n1ql_feat_ctrl = self.input.param("n1ql_feat_ctrl", "16460")
        if self.aws_access_key_id:
            from serverless.s3_utils import S3Utils
            self.s3_utils_obj = S3Utils(aws_access_key_id=self.aws_access_key_id,
                                        aws_secret_access_key=self.aws_secret_access_key,
                                        s3_bucket=self.s3_bucket, region=self.region)
        if not self.use_rest:
            query_definition_generator = SQLDefinitionGenerator()
            if self.dataset == "default" or self.dataset == "employee":
                self.query_definitions = query_definition_generator.generate_employee_data_query_definitions()
            if self.dataset == "simple":
                self.query_definitions = query_definition_generator.generate_simple_data_query_definitions()
            if self.dataset == "sabre":
                self.query_definitions = query_definition_generator.generate_sabre_data_query_definitions()
            if self.dataset == "bigdata":
                self.query_definitions = query_definition_generator.generate_big_data_query_definitions()
            if self.dataset == "array":
                self.query_definitions = query_definition_generator.generate_airlines_data_query_definitions()
            self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.ops_map = self._create_operation_map()
        self.find_nodes_in_list()
        self.generate_map_nodes_out_dist()
        self.memory_create_list = []
        self.memory_drop_list = []
        self.skip_cleanup = self.input.param("skip_cleanup", False)
        self.index_loglevel = self.input.param("index_loglevel", None)
        self.data_model = self.input.param("data_model", "sentence-transformers/all-MiniLM-L6-v2")
        self.vector_dim = self.input.param("vector_dim", "384")
        self.dimension = self.input.param("dimension", None)
        self.trainlist = self.input.param("trainlist", None)
        self.description = self.input.param("description", None)
        self.similarity = self.input.param("similarity", "L2_SQUARED")
        self.scan_nprobes = self.input.param("scan_nprobes", 100)
        self.scan_limit = self.input.param("scan_limit", 100)
        self.quantization_algo_color_vector = self.input.param("quantization_algo_color_vector", "PQ3x8")
        self.quantization_algo_description_vector = self.input.param("quantization_algo_description_vector", "PQ32x8")
        self.gsi_util_obj = GSIUtils(self.run_cbq_query)
        self.dimension = self.input.param("dimension", None)
        self.trainlist = self.input.param("trainlist", None)
        self.description = self.input.param("description", None)
        self.similarity = self.input.param("similarity", "L2_SQUARED")
        self.scan_limit = self.input.param("scan_limit", 100)
        self.base64 = self.input.param("base64", False)
        self.use_magma_server = self.input.param("use_magma_server", False)
        self.bhive_index = self.input.param("bhive_index", False)
        self.namespaces = []
        if self.index_loglevel:
            self.set_indexer_logLevel(self.index_loglevel)
        if self.dgm_run and hasattr(self, "gens_load"):
            self._load_doc_data_all_buckets(gen_load=self.gens_load)
        self.gsi_thread = Cluster()
        self.defer_build = self.defer_build and self.use_gsi_for_secondary
        self.num_index_replica = self.input.param("num_index_replica", 0)
        self.redistribute_nodes = self.input.param("redistribute_nodes", False)
        if self.capella_run:
            if self.num_index_replica == 0:
                self.num_index_replica = 1
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        if self.use_https:
            self.node_port = '18091'
        else:
            self.node_port = '8091'
        if index_node:
            self.index_rest = RestConnection(index_node)
            # New settings for schedule Indexes
            self.index_rest.set_index_settings({"queryport.client.waitForScheduledIndex": self.wait_for_scheduled_index})
            self.index_rest.set_index_settings({"indexer.allowScheduleCreateRebal": self.scheduled_index_create_rebal})
            if self.use_shard_based_rebalance:
                self.enable_shard_based_rebalance()

        # Disabling CBO and Sequential Scans for GSI tests

        if not self.capella_run:
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
            query_rest = RestConnection(query_node)
            cb_version = float(query_rest.get_major_version())
            # if the builds are greater than 7.6, trinity variable is set to True and sequential scans are disabled
            trinity = False
            if cb_version >= 7.6:
                trinity = True
            api = f"{query_rest.baseUrl}settings/querySettings"
            data = f'queryUseCBO={str(self.use_cbo).lower()}'
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            auth = HTTPBasicAuth(self.rest.username, self.rest.password)
            response = requests.request("POST", api, headers=headers, data=data, auth=auth, verify=False)
            if response.status_code != 200:
                self.fail(f"Disabling sequential scan failed. {response.content}")
            self.log.info(f"{data} set")
            self.log.info(response.text)
            if not trinity:
                self.n1ql_feat_ctrl = 76
            api = f"{query_rest.query_baseUrl}admin/settings"
            headers = {
                'Content-Type': 'application/json',
            }
            data = json.dumps({"n1ql-feat-ctrl": self.n1ql_feat_ctrl})
            response = requests.request("POST", api, headers=headers, data=data, auth=auth, verify=False)
            if response.status_code != 200:
                self.fail(f"Disabling sequential scan failed. {response.content}")
            self.log.info(f"{data} set")
            self.log.info(response.text)

    def tearDown(self):
        super(BaseSecondaryIndexingTests, self).tearDown()

    def _create_server_groups(self):
        if self.server_grouping:
            server_groups = self.server_grouping.split(":")
            self.log.info("server groups : %s", server_groups)

            zones = list(self.rest.get_zone_names().keys())

            if not self.rest.is_zone_exist("Group 1"):
                self.rest.add_zone("Group 1")

            # Delete Server groups
            for zone in zones:
                if zone != "Group 1":
                    nodes_in_zone = self.rest.get_nodes_in_zone(zone)
                    if nodes_in_zone:
                        self.rest.shuffle_nodes_in_zones(list(nodes_in_zone.keys()),
                                                         zone, "Group 1")
                    self.rest.delete_zone(zone)

            zones = list(self.rest.get_zone_names().keys())
            source_zone = zones[0]

            # Create Server groups
            for i in range(1, len(server_groups) + 1):
                server_grp_name = "ServerGroup_" + str(i)
                if not self.rest.is_zone_exist(server_grp_name):
                    self.rest.add_zone(server_grp_name)

            # Add nodes to Server groups
            i = 1
            for server_grp in server_groups:
                server_list = []
                server_grp_name = "ServerGroup_" + str(i)
                i += 1
                nodes_in_server_group = server_grp.split("-")
                for node in nodes_in_server_group:
                    self.rest.shuffle_nodes_in_zones(
                        [self.servers[int(node)].ip], source_zone,
                        server_grp_name)
                    server_list.append(self.servers[int(node)].ip + ":" + self.servers[int(node)].port)
                self.server_group_map[server_grp_name] = server_list
            self.rest.delete_zone("Group 1")

    def create_index(self, bucket, query_definition, deploy_node_info=None, desc=None):
        create_task = self.async_create_index(bucket, query_definition, deploy_node_info, desc=desc)
        create_task.result()
        if self.build_index_after_create:
            if self.defer_build:
                build_index_task = self.async_build_index(bucket, [f'`{query_definition.index_name}`'])
                self.wait_until_indexes_online(defer_build=self.defer_build)
                build_index_task.result()
            check = self.n1ql_helper.is_index_ready_and_in_list(bucket, query_definition.index_name,
                                                                server=self.n1ql_node)
            self.assertTrue(check, "index {0} failed to be created".format(query_definition.index_name))

    def run_scans_and_return_results(self, select_queries):
        query_result = {}
        n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        for query in select_queries:
            query_result[query] = self.run_cbq_query(query=query, scan_consistency='request_plus',
                                                     server=n1ql_server)['results']
        return query_result

    def create_scope_collections(self, num_scopes=1, num_collections=1, scope_prefix=None,
                                 collection_prefix=None, test_bucket=None):
        if not collection_prefix:
            collection_prefix = self.collection_prefix
        if not scope_prefix:
            scope_prefix = self.scope_prefix
        if not test_bucket:
            test_bucket = self.test_bucket
        self.cli_rest.create_scope_collection_count(scope_num=num_scopes, collection_num=num_collections,
                                                    scope_prefix=scope_prefix,
                                                    collection_prefix=collection_prefix,
                                                    bucket=test_bucket)
        self.update_keyspace_list(test_bucket)

    def update_keyspace_list(self, bucket, default=False, system=False):
        #initialize self.keyspace before using this method
        self.keyspace = []
        scopes = self.cli_rest.get_bucket_scopes(bucket=bucket)
        if not default:
            scopes.remove('_default')
        if not system and '_system' in scopes:
            scopes.remove('_system')
        for s_item in scopes:
            collections = self.cli_rest.get_scope_collections(bucket=bucket, scope=s_item)
            for c_item in collections:
                self.keyspace.append(f'default:{bucket}.{s_item}.{c_item}')
        self.keyspace = list(set(self.keyspace))

    def create_indexes(self, num=0, defer_build=False, itr=0, expected_failure=[], index_name_prefix="idx_",
                       query_def_group="plasma_test"):
        query_definition_generator = SQLDefinitionGenerator()
        index_create_tasks = []
        self.log.info(threading.currentThread().getName() + " Started")
        if len(self.keyspace) < num:
            num_indexes_collection = math.ceil(num / len(self.keyspace))
        else:
            num_indexes_collection = 1
        for collection_keyspace in self.keyspace:
            if self.run_tasks:
                collection_name = collection_keyspace.split('.')[-1]
                scope_name = collection_keyspace.split('.')[-2]
                query_definitions = query_definition_generator. \
                    generate_employee_data_query_definitions(index_name_prefix=index_name_prefix +
                                                                               scope_name + "_"
                                                                               + collection_name,
                                                             keyspace=collection_keyspace)
                server = random.choice(self.n1ql_nodes)
                index_create_task = ConcurrentIndexCreateTask(server, self.test_bucket, scope_name,
                                                              collection_name, query_definitions,
                                                              self.index_ops_obj, self.n1ql_helper, num_indexes_collection, defer_build,
                                                              itr, expected_failure, query_def_group)
                self.index_create_task_manager.schedule(index_create_task)
                index_create_tasks.append(index_create_task)
        self.log.info(threading.currentThread().getName() + " Completed")

        return index_create_tasks

    def load_docs(self, start_doc, key_prefix="bigkeybigkeybigkeybigkeybigkeybigkeybigkeybigkeyb_"):
        load_tasks = self.async_load_docs(start_doc, key_prefix=key_prefix)

        for task in load_tasks:
            task.result()

    def async_load_docs(self, start_doc, key_prefix="bigkeybigkeybigkeybigkeybigkeybigkeybigkeybigkeyb_", num_ops=None):
        load_tasks = []
        if not num_ops:
            num_ops = self.num_items_in_collection
        sdk_data_loader = SDKDataLoader(start_seq_num=start_doc, num_ops=self.num_items_in_collection,
                                        percent_create=self.percent_create,
                                        percent_update=self.percent_update, percent_delete=self.percent_delete,
                                        all_collections=self.all_collections, timeout=1800,
                                        json_template=self.dataset_template,
                                        key_prefix=key_prefix)
        for bucket in self.buckets:
            _task = SDKLoadDocumentsTask(self.master, bucket, sdk_data_loader)
            self.sdk_loader_manager.schedule(_task)
            load_tasks.append(_task)

        return load_tasks

    def async_create_index(self, bucket, query_definition, deploy_node_info=None, desc=None):
        index_where_clause = None
        if self.use_where_clause_in_index:
            index_where_clause = query_definition.index_where_clause
        self.query = query_definition.generate_index_create_query(namespace=bucket,
                                                                  use_gsi_for_secondary=self.use_gsi_for_secondary,
                                                                  deploy_node_info=deploy_node_info,
                                                                  defer_build=self.defer_build,
                                                                  index_where_clause=index_where_clause,
                                                                  num_replica=self.num_index_replica, desc=desc)
        create_index_task = self.gsi_thread.async_create_index(server=self.n1ql_node, bucket=bucket,
                                                               query=self.query, n1ql_helper=self.n1ql_helper,
                                                               index_name=query_definition.index_name,
                                                               defer_build=self.defer_build)
        return create_index_task

    def create_index_using_rest(self, bucket, query_definition, exprType='N1QL', deploy_node_info=None, desc=None):
        ind_content = query_definition.generate_gsi_index_create_query_using_rest(bucket=bucket,
                                                                                  deploy_node_info=deploy_node_info,
                                                                                  defer_build=None,
                                                                                  index_where_clause=None,
                                                                                  gsi_type=self.gsi_type,
                                                                                  desc=desc)

        log.info("Creating index {0}...".format(query_definition.index_name))
        return self.rest.create_index_with_rest(ind_content)

    def async_build_index(self, bucket, index_list=None):
        if not index_list:
            index_list = []
        self.query = self.n1ql_helper.gen_build_index_query(bucket=bucket, index_list=index_list)
        self.log.info(self.query)
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        build_index_task = self.gsi_thread.async_build_index(server=n1ql_node, bucket=bucket,
                                                             query=self.query, n1ql_helper=self.n1ql_helper)
        return build_index_task

    def async_monitor_index(self, bucket, index_name=None):
        monitor_index_task = self.gsi_thread.async_monitor_index(server=self.n1ql_node, bucket=bucket,
                                                                 n1ql_helper=self.n1ql_helper, index_name=index_name,
                                                                 timeout=self.timeout_for_index_online)
        return monitor_index_task

    def multi_create_index(self, buckets=None, query_definitions=None, deploy_node_info=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = "{0}:{1}".format(bucket.name, query_definition.index_name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    self.create_index(bucket.name, query_definition, deploy_node_info)

    def restore_couchbase_bucket(self, backup_filename, skip_default_scope=True):
        filename = backup_filename.split('/')[-1]
        self.s3_utils_obj.download_file(object_name=backup_filename,
                                        filename=filename)
        remote_client = RemoteMachineShellConnection(self.master)
        type = remote_client.extract_remote_info().distribution_type.lower()
        remote_client.copy_file_local_to_remote(src_path=filename, des_path=filename)
        repo = filename.split('.')[0]
        remote_client.execute_command("rm -rf backup")
        if type == 'windows':
            couchbase_root_dir = '"C:\\Program Files\\Couchbase\\Server\\bin\\cbbackupmgr"'
        else:
            couchbase_root_dir = "/opt/couchbase/bin/cbbackupmgr"
        backup_config_cmd = f"{couchbase_root_dir} config --archive backup/ --repo {repo}"
        out = remote_client.execute_command(backup_config_cmd)
        self.log.info(out)
        if "failed" in out[0]:
            self.fail(out)
        self.log.info("unzip the backup repo before restoring it")
        unzip_cmd = f"unzip -o {filename}"
        remote_client.execute_command(command=unzip_cmd)
        restore_cmd = f"{couchbase_root_dir} restore --archive backup --repo {repo} " \
                      f"--cluster couchbase://127.0.0.1 --username {self.username} --password {self.password} " \
                      f"-auto-create-buckets "
        restore_out = remote_client.execute_command(restore_cmd)
        self.log.debug(restore_out)

        self.buckets = self.rest.get_buckets()
        self.namespaces = []
        for bucket in self.buckets:
            scopes = self.rest.get_bucket_scopes(bucket=bucket)
            for scope in scopes:
                if scope == '_default' and skip_default_scope:
                    continue
                collections = self.rest.get_scope_collections(bucket=bucket, scope=scope)
                for collection in collections:
                    self.namespaces.append(f"default:{bucket}.{scope}.{collection}")
        self.log.info("namespaces created successfully")
        self.log.info(self.namespaces)

    def validate_scans_for_recall_and_accuracy(self, select_query, similarity="L2_SQUARED", scan_consitency=False):
        faiss_query = self.convert_to_faiss_queries(select_query=select_query)
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        vector_field, vector = self.extract_vector_field_and_query_vector(select_query)
        query_res_faiss = self.run_cbq_query(query=faiss_query, server=n1ql_node)['results']
        list_of_vectors_to_be_indexed_on_faiss = []
        if self.xattr_indexes:
            vector_field = vector_field.split('.')[-1]
        for v in query_res_faiss:
            result = v[vector_field]
            if self.base64:
                result = self.decode_base64_to_float(result)
            list_of_vectors_to_be_indexed_on_faiss.append(result)

        # Todo: Add a provision to convert base64 encoded embeddings to float32 embeddings

        faiss_db = self.load_data_into_faiss(vectors=np.array(list_of_vectors_to_be_indexed_on_faiss, dtype="float32"), similarity=similarity)

        ann = self.search_ann_in_faiss(query=vector, faiss_db=faiss_db, limit=self.scan_limit)

        faiss_closest_vectors = []
        for idx in ann:
            # self.log.info(f'for idx {idx} vector is {list_of_vectors_to_be_indexed_on_faiss[idx]}')
            if self.base64:
                value = self.encode_floats_to_base64(list_of_vectors_to_be_indexed_on_faiss[idx])
            else:
                value = list_of_vectors_to_be_indexed_on_faiss[idx]
            faiss_closest_vectors.append(value)
        if scan_consitency:
            gsi_query_res = self.run_cbq_query(query=select_query, server=self.n1ql_node, scan_consistency=self.scan_consistency)['results']
        else:
            gsi_query_res = self.run_cbq_query(query=select_query, server=self.n1ql_node)['results']
        gsi_query_vec_list = []

        for v in gsi_query_res:
            gsi_query_vec_list.append(v[vector_field])

        self.log.info(f'len of qv list is {len(gsi_query_vec_list)}')
        self.log.info(f'len of faiss list is {len(faiss_closest_vectors)}')

        #todo will comment this out post the resloving of https://issues.couchbase.com/browse/MB-62783
        # self.assertNotEqual(len(gsi_query_vec_list), 0, f"vector list is not equal for query {self.gen_query_without_entire_qvec(select_query)} gsi query results are {gsi_query_res}")

        recall = accuracy = 0

        for ele in gsi_query_vec_list:
            if ele in faiss_closest_vectors:
                recall += 1

        for qv, faiss in zip(gsi_query_vec_list, faiss_closest_vectors):
            if qv == faiss:
                accuracy += 1

        accuracy = accuracy / self.scan_limit
        recall = recall / self.scan_limit
        faiss_db.reset()

        redacted_select_query = self.gen_query_without_entire_qvec(query=select_query)
        self.log.info(f"accuracy for the query : {select_query} is {accuracy * 100} % ")
        self.log.info(f"recall for the query is : {select_query} is {recall * 100} %")
        return redacted_select_query, recall, accuracy

    def gen_query_without_entire_qvec(self, query):
        # Regex pattern to match the array of numbers
        array_pattern = r'\[[-+eE\d.,\s]+\]'

        # Replace the matched array with 'qvec'
        modified_query = re.sub(array_pattern, 'qvec', query)

        return modified_query

    def extract_vector_field_and_query_vector(self, query):
        if self.base64:
            pattern = r"ANN\(.*?\s*\((.*?),\s*.*?\),\s*(\[.*?\])"
        else:
            pattern = r"ANN\(\s*(.*?),\s*(\[(?:-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?(?:,\s*)?)+\])"
        matches = re.search(pattern, query)

        if matches:
            vector_field = matches.group(1)
            vector = matches.group(2)
            # self.log.info(f"vector_field: {vector_field}")
            self.log.info(f"Vector : {vector}")
            vector = ast.literal_eval(vector)
            return vector_field, vector
        else:
            raise Exception("no fields extracted")

    def get_per_index_codebook_memory_usage(self):
        stats_map = self.get_index_stats(perNode=True)
        index_code_book_memory_map = {}
        for node in stats_map:
            for ks in stats_map[node]:
                for index in stats_map[node][ks]:
                    if "primary" in index:
                        continue
                    index_code_book_memory_map[index] = stats_map[node][ks][index]['codebook_mem_usage']
        return index_code_book_memory_map, sum(index_code_book_memory_map.values())

    def validate_shard_seggregation(self, shard_index_map):
        index_categories = ['scalar', 'vector', 'bhive']
        for shard, indices in shard_index_map.items():
            categories_found = set()

            for index in indices:
                for category in index_categories:
                    if category in index:
                        categories_found.add(category)

            # To check if exactly more than one category is found for this shard
            self.assertEqual(len(categories_found), 1, f"More than category of index found for the shard specific shard {indices}. The shard mapping to index is {shard_index_map}")

    def get_shards_index_map(self):
        index_node = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(index_node)
        metadata = rest.get_indexer_metadata()
        shard_index_map = {}
        for index_metadata in metadata['status']:
            if 'alternateShardIds' in index_metadata:
                for host in index_metadata['alternateShardIds']:
                    for partition in index_metadata['alternateShardIds'][host]:
                        shards = index_metadata['alternateShardIds'][host][partition][0][:-2]
                        if shards in shard_index_map:
                            shard_index_map[shards].append(index_metadata['indexName'])
                        else:
                            shard_index_map[shards] = [index_metadata['indexName']]

        for key, value in shard_index_map.items():
            shard_index_map[key] = list(set(shard_index_map[key]))
        return shard_index_map

    def _return_maps(self, perNode=False, map_from_index_nodes=False):
        if map_from_index_nodes:
            index_map = self.get_index_map_from_index_endpoint()
        else:
            index_map = self.get_index_map()
        stats_map = self.get_index_stats(perNode=perNode)
        return index_map, stats_map

    def multi_create_index_using_rest(self, buckets=None, query_definitions=None, deploy_node_info=None):
        self.index_id_map = {}
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            if bucket not in list(self.index_id_map.keys()):
                self.index_id_map[bucket] = {}
            for query_definition in query_definitions:
                id_map = self.create_index_using_rest(bucket=bucket, query_definition=query_definition,
                                                      deploy_node_info=deploy_node_info)
                self.index_id_map[bucket][query_definition] = id_map["id"]

    def async_multi_create_index(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        create_index_tasks = []
        self.index_lost_during_move_out = []
        self.log.info(self.index_nodes_out)
        index_node_count = 0
        for query_definition in query_definitions:
            index_info = "{0}".format(query_definition.index_name)
            if index_info not in self.memory_create_list:
                self.memory_create_list.append(index_info)
                if index_node_count < len(self.index_nodes_out):
                    node_index = index_node_count
                    self.deploy_node_info = ["{0}:{1}".format(self.index_nodes_out[index_node_count].ip,
                                                              self.node_port)]
                    if query_definition.index_name not in self.index_lost_during_move_out:
                        self.index_lost_during_move_out.append(query_definition.index_name)
                    index_node_count += 1
                for bucket in buckets:
                    create_index_tasks.append(self.async_create_index(bucket.name,
                                                                      query_definition,
                                                                      deploy_node_info=self.deploy_node_info))
                # self.sleep(3)
        if self.defer_build and self.build_index_after_create:
            index_list = []
            for task in create_index_tasks:
                task.result()
            for query_definition in query_definitions:
                if query_definition.index_name not in index_list:
                    index_list.append(query_definition.index_name)
            for bucket in self.buckets:
                build_index_task = self.async_build_index(bucket, index_list)
                build_index_task.result()
            monitor_index_tasks = []
            for index_name in index_list:
                for bucket in self.buckets:
                    monitor_index_tasks.append(self.async_monitor_index(bucket.name, index_name))
            return monitor_index_tasks
        else:
            return create_index_tasks

    def multi_drop_index_using_rest(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                self.drop_index_using_rest(bucket, query_definition)

    def multi_drop_index(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_drop_query(namespace=bucket.name)
                index_create_info = "{0}:{1}".format(bucket.name, query_definition.index_name)
                if index_info not in self.memory_drop_list:
                    self.memory_drop_list.append(index_info)
                    self.drop_index(bucket.name, query_definition)
                if index_create_info in self.memory_create_list:
                    self.memory_create_list.remove(index_create_info)

    def reset_data_mount_point(self, nodes):
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            shell.execute_command('umount -l /data; '
                                  'mkdir -p /usr/disk-img; dd if=/dev/zero '
                                  'of=/usr/disk-img/disk-quota.ext4 count=10485760; '
                                  '/sbin/mkfs -t ext4 -q /usr/disk-img/disk-quota.ext4 -F; '
                                  'mount -o loop,rw,usrquota,grpquota /usr/disk-img/disk-quota.ext4 /data; '
                                  'umount -l /data; fsck.ext4 /usr/disk-img/disk-quota.ext4 -y; '
                                  'chattr +i /data; mount -o loop,rw,usrquota,grpquota /usr/disk-img/disk-quota.ext4 '
                                  '/data; rm -rf /data/*; chmod -R 777 /data')

    def _run_queries_continously(self, select_queries):
        while self.run_continous_query:
            tasks_list = self.gsi_util_obj.aysnc_run_select_queries(select_queries=select_queries,
                                                                    query_node=self.query_node)
            for task in tasks_list:
                task.result()

    def get_size_of_metastore_file(self):
        indexer_nodes = self.get_nodes_from_services_map(
            service_type="index", get_all_nodes=True)
        metastore_size_on_nodes = []
        for node in indexer_nodes:
            data_dir = RestConnection(node).get_index_path()
            shell = RemoteMachineShellConnection(node)
            metastore_size, err = shell.execute_command(
                "find {0}/@2i -name MetadataStore.[0-9]* | xargs du -sb|awk '{{print $1}}'".
                    format(data_dir))

            metastore_size = metastore_size[0]
            metastore_size_on_nodes.append({"nodeip": node.ip, "metastore_size": metastore_size})

        return metastore_size_on_nodes

    def async_multi_drop_index(self, buckets=None, query_definitions=None, pre_cc=False):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        drop_index_tasks = []
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_drop_query(namespace=bucket.name, pre_cc=pre_cc)
                if index_info not in self.memory_drop_list:
                    self.memory_drop_list.append(index_info)
                    drop_index_tasks.append(self.async_drop_index(bucket.name, query_definition, pre_cc=pre_cc))
        return drop_index_tasks

    def alter_index_replicas(self, index_name, namespace="default", action='replica_count', num_replicas=1,
                             set_error=False, nodes=None, replica_id=1, expect_failure=False):
        error = []
        if action == 'move':
            alter_index_query = f'ALTER INDEX {index_name} on {namespace} ' \
                f'WITH {{"action": "move", "nodes":{nodes}}}'
        elif action == 'replica_count':
            if nodes is not None:
                alter_index_query = f'ALTER INDEX {index_name} on {namespace} ' \
                                    f' WITH {{"action":"replica_count","num_replica": {num_replicas}, "nodes":{nodes}}}'
            else:
                alter_index_query = f'ALTER INDEX {index_name} on {namespace} ' \
                    f' WITH {{"action":"replica_count","num_replica": {num_replicas}}}'
        elif action == 'drop_replica':
            alter_index_query = f'ALTER INDEX {index_name} ON {namespace} ' \
                f'WITH {{"action":"drop_replica","replicaId": {replica_id}}}'
        else:
            # Negative case consideration
            alter_index_query = f'ALTER INDEX {index_name} on {namespace} ' \
                ' WITH {{"action":"replica_count"}}'

        try:
            self.n1ql_helper.run_cbq_query(query=alter_index_query, server=self.n1ql_node)
        except Exception as ex:
            error.append(str(ex))
            self.log.error(str(ex))

        if error:
            if expect_failure:
                self.log.info("alter index replica count failed as expected")
                self.log.info("Error : %s", error)
                if set_error:
                    self.alter_index_error = error
            else:
                self.log.info("Error : %s", error)
                self.fail("alter index failed to change the number of replicas")
        else:
            self.log.info("alter index started successfully")
        return error

    def drop_index(self, bucket, query_definition, verify_drop=True):
        try:
            self.query = query_definition.generate_index_drop_query(namespace=bucket,
                                                                    use_gsi_for_secondary=self.use_gsi_for_secondary,
                                                                    use_gsi_for_primary=self.use_gsi_for_primary)
            actual_result = self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)
            if verify_drop:
                check = self.n1ql_helper._is_index_in_list(bucket, query_definition.index_name, server=self.n1ql_node)
                self.assertFalse(check, "index {0} failed to be deleted".format(query_definition.index_name))
        except Exception as ex:
            self.log.info(ex)
            query = "select * from system:indexes"
            actual_result = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
            self.log.info(actual_result)

    def drop_index_using_rest(self, bucket, query_definition, verify_drop=True):
        self.log.info("Dropping index: {0}...".format(query_definition.index_name))
        self.rest.drop_index_with_rest(self.index_id_map[bucket][query_definition])
        if verify_drop:
            check = self.n1ql_helper._is_index_in_list(bucket, query_definition.index_name, server=self.n1ql_node)
            self.assertFalse(check, "Index {0} failed to be deleted".format(query_definition.index_name))
            del (self.index_id_map[bucket][query_definition])

    def async_drop_index(self, bucket, query_definition, pre_cc=False):
        self.query = query_definition.generate_index_drop_query(namespace=bucket,
                                                                use_gsi_for_secondary=self.use_gsi_for_secondary,
                                                                use_gsi_for_primary=self.use_gsi_for_primary,
                                                                pre_cc=pre_cc)
        drop_index_task = self.gsi_thread.async_drop_index(server=self.n1ql_node, bucket=bucket, query=self.query,
                                                           n1ql_helper=self.n1ql_helper,
                                                           index_name=query_definition.index_name)
        return drop_index_task

    def query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket=bucket)
        actual_result = self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)
        self.log.info(actual_result)
        if self.verify_explain_result:
            check = self.n1ql_helper.verify_index_with_explain(actual_result, query_definition.index_name)
            self.assertTrue(check, "Index %s not found" % (query_definition.index_name))

    def async_query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket=bucket)
        query_with_index_task = self.gsi_thread.async_n1ql_query_verification(server=self.n1ql_node, bucket=bucket,
                                                                              query=self.query,
                                                                              n1ql_helper=self.n1ql_helper,
                                                                              is_explain_query=True,
                                                                              index_name=query_definition.index_name,
                                                                              verify_results=self.verify_explain_result)
        return query_with_index_task

    def multi_query_using_index_with_explain(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                self.query_using_index_with_explain(bucket.name,
                                                    query_definition)

    def async_multi_query_using_index_with_explain(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        async_query_with_explain_tasks = []
        for bucket in buckets:
            for query_definition in query_definitions:
                async_query_with_explain_tasks.append(self.async_query_using_index_with_explain(bucket.name,
                                                                                                query_definition))
        return async_query_with_explain_tasks

    def query_using_index(self, bucket, query_definition, expected_result=None, scan_consistency=None,
                          scan_vector=None, verify_results=True):
        if not scan_consistency:
            scan_consistency = self.scan_consistency
        self.gen_results.query = query_definition.generate_query(bucket=bucket)
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result=False)
        self.query = self.gen_results.query
        log.info("Query : {0}".format(self.query))
        msg, check = self.n1ql_helper.run_query_and_verify_result(query=self.query, server=self.n1ql_node, timeout=500,
                                                                  expected_result=expected_result,
                                                                  scan_consistency=scan_consistency,
                                                                  scan_vector=scan_vector,
                                                                  verify_results=verify_results)
        self.assertTrue(check, msg)

    def async_query_using_index(self, bucket, query_definition, expected_result=None, scan_consistency=None,
                                scan_vector=None):
        self.gen_results.query = query_definition.generate_query(bucket=bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        if expected_result == None:
            expected_result = self.gen_results.generate_expected_result(print_expected_result=False)
        self.query = self.gen_results.query
        query_with_index_task = self.gsi_thread.async_n1ql_query_verification(
            server=self.n1ql_node, bucket=bucket,
            query=self.query, n1ql_helper=self.n1ql_helper,
            expected_result=expected_result, index_name=query_definition.index_name,
            scan_consistency=scan_consistency, scan_vector=scan_vector,
            verify_results=self.verify_query_result)
        return query_with_index_task

    def query_using_index_with_emptyset(self, bucket, query_definition):
        self.gen_results.query = query_definition.generate_query(bucket=bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        self.query = self.gen_results.query
        actual_result = self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)
        self.assertTrue(len(actual_result["results"]) == 0, "Result is not empty {0}".format(actual_result["results"]))

    def multi_query_using_index_with_emptyresult(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                self.query_using_index_with_emptyset(bucket.name, query_definition)

    def multi_query_using_index(self, buckets=None, query_definitions=None,
                                expected_results=None, scan_consistency=None,
                                scan_vectors=None, verify_results=True):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            scan_vector = None
            if scan_vectors != None:
                scan_vector = scan_vectors[bucket.name]
            for query_definition in query_definitions:
                if expected_results:
                    expected_result = expected_results[query_definition.index_name]
                else:
                    expected_result = None
                self.query_using_index(bucket=bucket.name, query_definition=query_definition,
                                       expected_result=expected_result, scan_consistency=scan_consistency,
                                       scan_vector=scan_vector, verify_results=verify_results)

    def async_multi_query_using_index(self, buckets=[], query_definitions=[], expected_results={},
                                      scan_consistency=None, scan_vectors=None):
        multi_query_tasks = []
        for bucket in buckets:
            scan_vector = None
            if scan_vectors != None:
                scan_vector = scan_vectors[bucket.name]
            for query_definition in query_definitions:
                if expected_results:
                    multi_query_tasks.append(self.async_query_using_index(bucket.name, query_definition,
                                                                          expected_results[query_definition.index_name],
                                                                          scan_consistency=scan_consistency,
                                                                          scan_vector=scan_vector))
                else:
                    multi_query_tasks.append(self.async_query_using_index(bucket.name, query_definition, None,
                                                                          scan_consistency=scan_consistency,
                                                                          scan_vector=scan_vector))
        return multi_query_tasks

    def async_check_and_run_operations(self, buckets=[], initial=False, before=False, after=False,
                                       in_between=False, scan_consistency=None, scan_vectors=None):
        # self.verify_query_result = True
        # self.verify_explain_result = True
        if initial:
            self._set_query_explain_flags("initial")
            self.log.info(self.ops_map["initial"])
            return self.async_run_multi_operations(buckets=buckets,
                                                   create_index=self.ops_map["initial"]["create_index"],
                                                   drop_index=self.ops_map["initial"]["drop_index"],
                                                   query=self.ops_map["initial"]["query_ops"],
                                                   query_with_explain=self.ops_map["initial"]["query_explain_ops"],
                                                   scan_consistency=scan_consistency,
                                                   scan_vectors=scan_vectors)
        if before:
            self._set_query_explain_flags("before")
            self.log.info(self.ops_map["before"])
            return self.async_run_multi_operations(buckets=buckets,
                                                   create_index=self.ops_map["before"]["create_index"],
                                                   drop_index=self.ops_map["before"]["drop_index"],
                                                   query=self.ops_map["before"]["query_ops"],
                                                   query_with_explain=self.ops_map["before"]["query_explain_ops"],
                                                   scan_consistency=scan_consistency,
                                                   scan_vectors=scan_vectors)
        if in_between:
            self._set_query_explain_flags("in_between")
            self.log.info(self.ops_map["initial"])
            return self.async_run_multi_operations(buckets=buckets,
                                                   create_index=self.ops_map["in_between"]["create_index"],
                                                   drop_index=self.ops_map["in_between"]["drop_index"],
                                                   query=self.ops_map["in_between"]["query_ops"],
                                                   query_with_explain=self.ops_map["in_between"]["query_explain_ops"],
                                                   scan_consistency=scan_consistency,
                                                   scan_vectors=scan_vectors)
        if after:
            self._set_query_explain_flags("after")
            self.log.info(self.ops_map["initial"])
            return self.async_run_multi_operations(buckets=buckets,
                                                   create_index=self.ops_map["after"]["create_index"],
                                                   drop_index=self.ops_map["after"]["drop_index"],
                                                   query=self.ops_map["after"]["query_ops"],
                                                   query_with_explain=self.ops_map["after"]["query_explain_ops"],
                                                   scan_consistency="request_plus",
                                                   scan_vectors=scan_vectors)

    def run_multi_operations(self, buckets=[], query_definitions=[], expected_results={},
                             create_index=False, drop_index=False, query_with_explain=False, query=False,
                             scan_consistency=None, scan_vectors=None):
        try:
            if create_index:
                self.multi_create_index(buckets, query_definitions)
            if query_with_explain:
                self.multi_query_using_index_with_explain(buckets, query_definitions)
            if query:
                self.multi_query_using_index(buckets, query_definitions,
                                             expected_results, scan_consistency=scan_consistency,
                                             scan_vectors=scan_vectors)
        except Exception as ex:
            self.log.info(ex)
            raise
        finally:
            if drop_index and not self.skip_cleanup:
                self.multi_drop_index(buckets, query_definitions)

    def async_run_multi_operations(self, buckets=None, query_definitions=None, expected_results=None,
                                   create_index=False, drop_index=False, query_with_explain=False, query=False,
                                   scan_consistency=None, scan_vectors=None):
        tasks = []
        if not query_definitions:
            query_definitions = self.query_definitions
        try:
            if create_index:
                tasks += self.async_multi_create_index(buckets, query_definitions)
            if query_with_explain:
                tasks += self.async_multi_query_using_index_with_explain(buckets, query_definitions)
            if query:
                tasks += self.async_multi_query_using_index(buckets, query_definitions, expected_results,
                                                            scan_consistency=scan_consistency,
                                                            scan_vectors=scan_vectors)
            if drop_index:
                tasks += self.async_multi_drop_index(self.buckets, query_definitions)
        except Exception as ex:
            self.log.info(ex)
            raise
        return tasks

    def async_run_operations(self, phase, buckets=None, query_definitions=None, expected_results=None,
                             scan_consistency=None, scan_vectors=None):
        if not buckets:
            buckets = self.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        if not scan_consistency:
            scan_consistency = self.scan_consistency
        tasks = []
        operation_map = self.generate_operation_map(phase)
        self.log.info("=== {0}: {1} ===".format(phase.upper(), operation_map))
        nodes_out = []
        if isinstance(self.nodes_out_dist, str):
            for service in self.nodes_out_dist.split("-"):
                nodes_out.append(service.split(":")[0])
        if operation_map:
            try:
                if "create_index" in operation_map:
                    if ("index" in nodes_out or "n1ql" in nodes_out) and phase == "in_between":
                        tasks = []
                    else:
                        tasks += self.async_multi_create_index(buckets, query_definitions)
                if "query_with_explain" in operation_map:
                    if "n1ql" in nodes_out and phase == "in_between":
                        tasks = []
                    else:
                        tasks += self.async_multi_query_using_index_with_explain(buckets, query_definitions)
                if "query" in operation_map:
                    if "n1ql" in nodes_out and phase == "in_between":
                        tasks = []
                    else:
                        tasks += self.async_multi_query_using_index(buckets, query_definitions, expected_results,
                                                                    scan_consistency=scan_consistency,
                                                                    scan_vectors=scan_vectors)
                if "drop_index" in operation_map:
                    pre_cc = False
                    index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
                    cb_version = RestConnection(index_node).get_nodes_version()
                    if '7.0' not in cb_version:
                        pre_cc = True
                    if "index" in nodes_out or "n1ql" in nodes_out:
                        if phase == "in_between":
                            tasks = []
                    else:
                        tasks += self.async_multi_drop_index(self.buckets, query_definitions, pre_cc=pre_cc)
            except Exception as ex:
                log.info(ex)
                raise
        return tasks

    def run_full_table_scan_using_rest(self, bucket, query_definition, verify_result=False):
        expected_result = []
        actual_result = []
        full_scan_query = f"SELECT * FROM {bucket.name}"
        if query_definition.index_where_clause:
            full_scan_query += f" WHERE {query_definition.index_where_clause}"
        self.gen_results.query = full_scan_query
        temp = self.gen_results.generate_expected_result(print_expected_result=False)
        for item in temp:
            expected_result.append(list(item.values()))
        if self.scan_consistency == "request_plus":
            body = {"stale": "False"}
        else:
            body = {"stale": "ok"}
        content = self.rest.full_table_scan_gsi_index_with_rest(self.index_id_map[bucket][query_definition], body)
        if verify_result:
            doc_id_list = []
            for item in content:
                if item["docid"] not in doc_id_list:
                    for doc in self.full_docs_list:
                        if doc["_id"] == item["docid"]:
                            actual_result.append([doc])
                            doc_id_list.append(item["docid"])
            diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

            #self.assertEqual(len(sorted(actual_result)), len(sorted(expected_result)),
            #                 "Actual Items {0} are not equal to expected Items {1}".
            #                 format(len(sorted(actual_result)), len(sorted(expected_result))))
            #msg = "The number of rows match but the results mismatch, please check"
            #if sorted(actual_result) != sorted(expected_result):
            #    raise Exception(msg)

    def run_lookup_gsi_index_with_rest(self, bucket, query_definition):
        pass

    def run_range_scan_with_rest(self, bucket, query_definition):
        pass

    def gen_scan_vector(self, use_percentage=1.0, use_random=False):
        servers = self.get_kv_nodes(servers=self.servers[:self.nodes_init])
        sequence_bucket_map = self.get_vbucket_seqnos(servers, self.buckets)
        scan_vectors = {}
        if use_percentage == 1.0:
            for bucket in self.buckets:
                scan_vector = []
                self.log.info("analyzing for bucket {0}".format(bucket.name))
                map = sequence_bucket_map[bucket.name]
                for i in range(1024):
                    key = "vb_" + str(i)
                    value = [int(map[key]["abs_high_seqno"]), map[key]["uuid"]]
                    scan_vector.append(value)
                scan_vectors[bucket.name] = scan_vector
        else:
            for bucket in self.buckets:
                scan_vector = {}
                total = int(self.vbuckets * use_percentage)
                vbuckets_number_list = list(range(0, total))
                if use_random:
                    vbuckets_number_list = random.sample(range(0, self.vbuckets), total)
                self.log.info("analyzing for bucket {0}".format(bucket.name))
                map = sequence_bucket_map[bucket.name]
                for key in list(map.keys()):
                    vb = int(key.split("vb_")[1])
                    if vb in vbuckets_number_list:
                        value = [int(map[key]["abs_high_seqno"]), map[key]["uuid"]]
                        scan_vector[str(vb)] = value
                scan_vectors[bucket.name] = scan_vector
        return scan_vectors

    def check_missing_and_extra(self, actual, expected):
        missing = []
        extra = []
        for item in actual:
            if not (item in expected):
                extra.append(item)
        for item in expected:
            if not (item in actual):
                missing.append(item)
        return missing, extra

    def _verify_results(self, actual_result, expected_result, missing_count=1, extra_count=1):
        actual_result = self._gen_dict(actual_result)
        expected_result = self._gen_dict(expected_result)
        if len(actual_result) != len(expected_result):
            missing, extra = self.check_missing_and_extra(actual_result, expected_result)
            self.log.error("Missing items: %s.\n Extra items: %s" % (missing[:missing_count], extra[:extra_count]))
            self.fail("Results are incorrect.Actual num %s. Expected num: %s.\n" % (
                len(actual_result), len(expected_result)))
        if self.max_verify is not None:
            actual_result = actual_result[:self.max_verify]
            expected_result = expected_result[:self.max_verify]

        msg = "Results are incorrect.\n Actual first and last 100:  %s.\n ... \n %s" + \
              "Expected first and last 100: %s.\n  ... \n %s"
        self.assertTrue(actual_result == expected_result,
                        msg % (actual_result[:100], actual_result[-100:],
                               expected_result[:100], expected_result[-100:]))

    def verify_index_absence(self, query_definitions, buckets):
        server = self.get_nodes_from_services_map(service_type="n1ql")
        for bucket in buckets:
            for query_definition in query_definitions:
                check = self.n1ql_helper._is_index_in_list(bucket.name, query_definition.index_name, server=server)
                self.assertFalse(check, " {0} was not absent as expected".format(query_definition.index_name))

    def _gen_dict(self, result):
        result_set = []
        if result != None and len(result) > 0:
            for val in result:
                for key in list(val.keys()):
                    result_set.append(val[key])
        return result_set

    def _verify_index_map(self):
        if not self.verify_using_index_status:
            return
        index_map = self.get_index_map()
        index_bucket_map = self.n1ql_helper.gen_index_map(self.n1ql_node)
        msg = "difference in index map found, expected {0} \n actual {1}".format(index_bucket_map, index_map)
        self.assertTrue(len(list(index_map.keys())) == len(self.buckets),
                        "numer of buckets mismatch :: " + msg)
        for bucket in self.buckets:
            self.assertTrue((bucket.name in list(index_map.keys())),
                            " bucket name not present in index map {0}".format(index_map))
        for bucket_name in list(index_bucket_map.keys()):
            self.assertTrue(len(list(index_bucket_map[bucket_name].keys())) == len(list(index_map[bucket_name].keys())),
                            "number of indexes mismatch ::" + msg)
            for index_name in list(index_bucket_map[bucket_name].keys()):
                msg1 = "index_name {0} not found in {1}".format(index_name, list(index_map[bucket_name].keys()))
                self.assertTrue(index_name in list(index_map[bucket_name].keys()), msg1 + " :: " + msg)

    def _verify_primary_index_count(self):
        bucket_map = self.get_buckets_itemCount()
        for bucket, value in bucket_map.items():
            bucket_map[bucket] = value - self.stat.get_collection_item_count_cumulative(bucket, CbServer.system_scope,
                                                                                        CbServer.query_collection,
                                                                                        self.get_kv_nodes())
        count = 0
        while not self._verify_items_count() and count < 15:
            self.log.info("All Items Yet to be Indexed...")
            self.sleep(5)
            count += 1
        self.assertTrue(self._verify_items_count(), "All Items didn't get Indexed...")
        self.log.info("All the documents are indexed...")
        # self.sleep(10)
        index_bucket_map = self.n1ql_helper.get_index_count_using_primary_index(self.buckets, self.n1ql_node)
        self.log.info(bucket_map)
        self.log.info(index_bucket_map)
        for bucket_name in list(bucket_map.keys()):
            actual_item_count = index_bucket_map[bucket_name]
            expected_item_count = bucket_map[bucket_name]
            self.assertTrue(str(actual_item_count) == str(expected_item_count),
                            "Bucket {0}, mismatch in item count for index :{1} : expected {2} != actual {3} ".format
                            (bucket_name, "primary", expected_item_count, actual_item_count))

    def _verify_items_count(self):
        """
        Compares Items indexed count is sample
        as items in the bucket.
        """

        index_map = self.get_index_stats()
        self.log.info(index_map)
        self.log.info("************************")
        self.log.info(list(index_map.keys()))
        for key, value in index_map.items():
            if 'num_docs_pending' in key or 'num_docs_queued' in key:
                self.log.info(f"{key}:{value}")
                if value:
                    return False
        return True

    def _verify_items_count_collections(self, index_node):
        """
        Compares Items indexed count is sample
        as items in the bucket.
        """
        index_rest = RestConnection(index_node)
        index_map = index_rest.get_index_stats()
        for keyspace in list(index_map.keys()):
            self.log.info("Keyspace: {0}".format(keyspace))
            for index_name, index_val in index_map[keyspace].items():
                if index_val["index_state"] != 0:
                    self.log.info("Index: {0}".format(index_name))
                    self.log.info("number of docs pending: {0}".format(index_val["num_docs_pending"]))
                    self.log.info("number of docs queued: {0}".format(index_val["num_docs_queued"]))
                    if index_val["num_docs_pending"] or index_val["num_docs_queued"]:
                        return False
        return True

    def get_persistent_snapshot_count(self, index_node, index_name):
        """
        Returns the number of persistent snapshots for a MOI index for a specific node
        """
        index_rest = RestConnection(index_node)
        index_map = index_rest.get_index_stats()
        for keyspace in list(index_map.keys()):
            self.log.info("Keyspace: {0}".format(keyspace))
            for idx_name, idx_val in index_map[keyspace].items():
                if idx_name == index_name:
                    return idx_val["num_snapshots"]


    def _get_items_count_collections(self, index_node, keyspace, index_name):
        """
        Compares Items indexed count is sample
        as items in the bucket.
        """
        try:
            index_rest = RestConnection(index_node)
            index_map = index_rest.get_index_stats()
            item_count = index_map[keyspace][index_name]["items_count"]
        except Exception as e:
            self.log.info(e)
            item_count = -1
        return item_count

    def _get_index_map(self, index_node):
        """
        Compares Items indexed count is sample
        as items in the bucket.
        """
        try:
            index_rest = RestConnection(index_node)
            index_map = index_rest.get_index_stats()
        except Exception as e:
            self.log.info(e)
            index_map = None
        return index_map

    def verify_type_fields(self, num_hotels=0, num_airports=0, num_airlines=0, expected_failure=False):
        retries = 3
        query = "select * from `{0}` where type = {1} order by meta().id"
        hotel_query = query.format(self.bucket_name, "'hotel'")
        hotels_validated = False
        airlines_validated = False
        airports_validated = False
        for i in range(0, retries):
            hotel_results = self.n1ql_helper.run_cbq_query(query=hotel_query, server=self.n1ql_node)
            if hotel_results['metrics']['resultCount'] == num_hotels:
                hotels_validated = True
                break
            else:
                time.sleep(1)
        self.log.info("Actual results: {0}".format(str(hotel_results['metrics']['resultCount'])))
        self.assertTrue(hotels_validated)

        airline_query = query.format(self.bucket_name, "'airline'")
        for i in range(0, retries):
            airline_results = self.n1ql_helper.run_cbq_query(query=airline_query, server=self.n1ql_node)
            if airline_results['metrics']['resultCount'] == num_airlines:
                airlines_validated = True
                break
            else:
                time.sleep(1)
        self.log.info("Actual results: {0}".format(str(airline_results['metrics']['resultCount'])))
        self.assertTrue(airlines_validated)

        airport_query = query.format(self.bucket_name, "'airport'")
        for i in range(0, retries):
            airport_results = self.n1ql_helper.run_cbq_query(query=airport_query, server=self.n1ql_node)
            if airport_results['metrics']['resultCount'] == num_airports:
                airports_validated = True
                break
            else:
                time.sleep(1)
        self.log.info("Actual results: {0}".format(str(airport_results['metrics']['resultCount'])))
        self.assertTrue(airports_validated)

    def _verify_bucket_count_with_index_count(self, query_definitions=None, buckets=None):
        """
        :param bucket:
        :param index:
        :return:
        """
        count = 0
        if not query_definitions:
            query_definitions = self.query_definitions
        if not buckets:
            buckets = self.buckets
        while not self._verify_items_count() and count < 15:
            self.log.info("All Items Yet to be Indexed...")
            self.sleep(10)
            count += 1
        if not self._verify_items_count():
            raise Exception("All Items didn't get Indexed...")
        bucket_map = self.get_buckets_itemCount()
        for bucket in buckets:
            bucket_count = bucket_map[bucket.name] - self.stat.get_collection_item_count_cumulative(bucket,
                                                                                                    CbServer.system_scope,
                                                                                                    CbServer.query_collection,
                                                                                                    self.get_kv_nodes())
            for query in query_definitions:
                index_count = self.n1ql_helper.get_index_count_using_index(bucket,
                                                                           query.index_name, self.n1ql_node)
                self.assertTrue(int(index_count) == int(bucket_count),
                                "Bucket {0}, mismatch in item count for index :{1} : expected {2} != actual {3} ".format
                                (bucket.name, query.index_name, bucket_count, index_count))
        self.log.info("Items Indexed Verified with bucket count...")

    def verify_compression_stat(self, query_definitions):
        all_index_comp_stat_verified = False
        is_any_stat_negative = False
        indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                         get_all_nodes=True)
        for node in indexer_nodes:
            indexer_rest = RestConnection(node)
            content = indexer_rest.get_index_storage_stats()
            for index in list(content.values()):
                for index_name, stats in index.items():
                    comp_stat_verified = False
                    is_stat_negative = False
                    for query in query_definitions:
                        if index_name == query.index_name:
                            self.log.info(f'num_rec_compressed for index {index_name} found to be '
                                          f'{stats["MainStore"]["num_rec_compressed"]} and num_rec_swapin found to be '
                                          f'{stats["MainStore"]["num_rec_swapin"]}')
                            if stats["MainStore"]["num_rec_compressed"] > 0:
                                comp_stat_verified = True
                            if stats["MainStore"]["num_rec_compressed"] < 0:
                                is_stat_negative = True
                    all_index_comp_stat_verified = all_index_comp_stat_verified or comp_stat_verified
                    is_any_stat_negative = is_any_stat_negative and is_stat_negative

            if is_any_stat_negative:
                all_index_comp_stat_verified = False
        return all_index_comp_stat_verified

    def wait_for_mutation_processing(self, index_nodes):
        count = 0
        all_nodes_mutation_processed = True
        for index_node in index_nodes:
            while not self._verify_items_count_collections(index_node) and count < 90:
                self.log.info("All Items Yet to be Indexed...")
                self.sleep(10)
                count += 1
            if not self._verify_items_count_collections(index_node):
                self.log.error("All Items didn't get Indexed...")
                mutation_processed = False
            else:
                self.log.info("All Items are indexed")
                mutation_processed = True

            all_nodes_mutation_processed = all_nodes_mutation_processed and mutation_processed
        return all_nodes_mutation_processed

    def check_if_shard_exists(self, shard_dir, index_node):
        data_dir = RestConnection(index_node).get_index_path()
        shell = RemoteMachineShellConnection(index_node)
        shard_abs_dirpath = f'{data_dir}/@2i/shards/{shard_dir}'
        return shell.check_directory_exists(shard_abs_dirpath)

    def check_if_index_recovered(self, index_list):
        indexes_not_recovered = []
        for index in index_list:
            recovered, index_count, collection_itemcount = self._verify_collection_count_with_index_count(index["query_def"])
            if not recovered:
                error_map = {"index_name": index["name"], "index_count": index_count, "bucket_count": collection_itemcount}
                indexes_not_recovered.append(error_map)
        if not indexes_not_recovered:
            self.log.info("All indexes recovered")

    def _verify_collection_count_with_index_count(self, query_def):
        recovered = False
        index_name = query_def.index_name
        keyspace = query_def.keyspace
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        collection_itemcount = self.n1ql_helper.get_collection_itemCount(keyspace, n1ql_node)
        index_count = self.n1ql_helper.get_index_count_using_index_collection(keyspace, index_name, n1ql_node)
        if int(index_count) == int(collection_itemcount):
            recovered = True
            self.log.info(f'Collection item count : {collection_itemcount}, '
                          f'item count for index {index_name}: {index_count}')
        else:
            self.log.error(f'Collection item count : {collection_itemcount}, '
                           f'item count for index {index_name}: {index_count}')
        return recovered, index_count, collection_itemcount

    def _verify_index_count_less_than_collection(self, index_node, query_def):
        recovered = False
        index_name = query_def.index_name
        keyspace = query_def.keyspace
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        collection_itemcount = self.n1ql_helper.get_collection_itemCount(keyspace, n1ql_node)

        index_count = self._get_items_count_collections(index_node, keyspace, index_name)
        if int(index_count) != -1 and int(index_count) < int(collection_itemcount):
            recovered = True
        elif index_count == -1 or not int(index_count):
            recovered = -1
        self.log.info(f'Collection item count : {collection_itemcount}, '
                      f'item count for index {index_name}: {index_count}')

        return recovered, index_count, collection_itemcount

    def check_index_recovered_snapshot(self, index_list, index_node, max_time=60):
        indexes_not_recovered = []
        stat_time = time.time()
        end_time = stat_time + max_time
        list_of_index = copy.deepcopy(index_list)
        while time.time() < end_time:
            if not list_of_index:
                break

            for index in list_of_index:
                snap_recovered, index_count, collection_itemcount = \
                    self._verify_index_count_less_than_collection(index_node, index["query_def"])
                if snap_recovered == -1:
                    break
                if not snap_recovered:
                    error_map = {"index_name": index["name"], "index_count": index_count, "bucket_count": collection_itemcount}
                    indexes_not_recovered.append(error_map)
                else:
                    list_of_index.remove(index)
        if indexes_not_recovered:
            self.log.info(f'Indexes {indexes_not_recovered} not recovered')
            return False
        else:
            return True

    def check_if_index_count_zero(self, index_list, index_nodes):
        index_map = None
        index_item_count_map = {}
        for index_node in index_nodes:
            index_map = self._get_index_map(index_node)
            for index in index_list:
                index_count = index_map[index["query_def"].keyspace][index["query_def"].index_name]["items_count"]
                self.log.info("Index count on index {0} : {1}".format(index["query_def"].index_name, index_count))
                if index["query_def"].index_name not in index_item_count_map:
                    index_item_count_map[index["query_def"].index_name] = index_count
                else:
                    index_item_count_map[index["query_def"].index_name] += index_count

        for index in index_item_count_map:
            if index_item_count_map[index] != 0:
                return False
        return True

    def _kill_all_processes_index(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.execute_command("pkill indexer")

    def _kill_projector_process(self, server):
        shell = RemoteMachineShellConnection(server)
        shell.execute_command("pkill projector")

    def kill_loader_process(self):
        self.log.info("killing java doc loader process")
        os.system(f'pgrep -f .*{self.key_prefix}*')
        os.system(f'pkill -f .*{self.key_prefix}*')

    def kill_pillow_fight(self):
        self.log.info("killing java doc loader process")
        os.system(f'pkill cbc-pillowfight')

    def _kill_all_processes_index_with_sleep(self, server, sleep_time, timeout=1200):
        self.log.info(threading.currentThread().getName() + " Started")
        endtime = time.time() + timeout
        while self.kill_index and time.time() < endtime:
            shell = RemoteMachineShellConnection(server)
            shell.execute_command("pkill indexer")
            self.sleep(sleep_time)
        self.log.info(threading.currentThread().getName() + " Completed")

    def _check_all_bucket_items_indexed(self, query_definitions=None, buckets=None):
        """
        :param bucket:
        :param index:
        :return:
        """
        count = 0
        while not self._verify_items_count() and count < 15:
            self.log.info("All Items Yet to be Indexed...")
            self.sleep(10)
            count += 1
        self.assertTrue(self._verify_items_count(), "All Items didn't get Indexed...")

    def _create_operation_map(self):
        map_initial = {"create_index": False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        map_before = {"create_index": False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        map_in_between = {"create_index": False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        map_after = {"create_index": False, "query_ops": False, "query_explain_ops": False, "drop_index": False}
        initial = self.input.param("initial", "")
        for op_type in initial.split(":"):
            if op_type != '':
                map_initial[op_type] = True
        before = self.input.param("before", "")
        for op_type in before.split(":"):
            if op_type != '':
                map_before[op_type] = True
        in_between = self.input.param("in_between", "")
        for op_type in in_between.split(":"):
            if op_type != '':
                map_in_between[op_type] = True
        after = self.input.param("after", "")
        for op_type in after.split(":"):
            if op_type != '':
                map_after[op_type] = True
        return {"initial": map_initial, "before": map_before, "in_between": map_in_between, "after": map_after}

    def generate_operation_map(self, phase):
        operation_map = []
        self.verify_query_result = False
        self.verify_explain_result = False
        ops = self.input.param(phase, "")
        for type in ops.split("-"):
            for op_type in type.split(":"):
                if "verify_query_result" in op_type:
                    self.verify_query_result = True
                    continue
                if "verify_explain_result" in op_type:
                    self.verify_explain_result = True
                    continue
                if op_type != '':
                    operation_map.append(op_type)
        return operation_map

    def _query_explain_in_async(self):
        tasks = self.async_run_multi_operations(buckets=self.buckets,
                                                query_definitions=self.query_definitions,
                                                create_index=False, drop_index=False,
                                                query_with_explain=self.run_query_with_explain,
                                                query=False, scan_consistency=self.scan_consistency)
        for task in tasks:
            task.result()
        tasks = self.async_run_multi_operations(buckets=self.buckets,
                                                query_definitions=self.query_definitions,
                                                create_index=False, drop_index=False,
                                                query_with_explain=False, query=self.run_query,
                                                scan_consistency=self.scan_consistency)
        for task in tasks:
            task.result()

    def _set_query_explain_flags(self, phase):
        if ("query_ops" in list(self.ops_map[phase].keys())) and self.ops_map[phase]["query_ops"]:
            self.ops_map[phase]["query_explain_ops"] = True
        if ("do_not_verify_query_result" in list(self.ops_map[phase].keys())) and self.ops_map[phase][
            "do_not_verify_query_result"]:
            self.verify_query_result = False
            self.ops_map[phase]["query_explain_ops"] = False
        if ("do_not_verify_explain_result" in list(self.ops_map[phase].keys())) and self.ops_map[phase][
            "do_not_verify_explain_result"]:
            self.verify_explain_result = False
            self.ops_map[phase]["query_explain_ops"] = False
        self.log.info(self.ops_map)

    def fail_if_no_buckets(self):
        buckets = False
        for a_bucket in self.buckets:
            buckets = True
        if not buckets:
            self.fail('FAIL: This test requires buckets')

    def block_incoming_network_from_node(self, node1, node2):
        shell = RemoteMachineShellConnection(node1)
        shell.execute_command("nft add table ip filter")
        shell.execute_command("nft add chain ip filter INPUT '{ type filter hook input priority 0; }'")
        self.log.info("Adding {0} into iptables rules on {1}".format(
            node1.ip, node2.ip))
        command = f"nft 'add rule ip filter INPUT ip saddr {node2.ip} counter reject'"
        shell.execute_command(command)

    def resume_blocked_incoming_network_from_node(self, node1, node2):
        shell = RemoteMachineShellConnection(node1)
        shell.execute_command("nft add table ip filter")
        shell.execute_command("nft add chain ip filter INPUT '{ type filter hook input priority 0; }'")
        self.log.info(f"Removing iptables rules on {node1.ip} for {node2.ip}")
        command = "nft flush ruleset"
        shell.execute_command(command)

    def block_outgoing_network_from_node(self, node1, node2):
        shell = RemoteMachineShellConnection(node1)
        shell.execute_command("nft add table ip filter")
        shell.execute_command("nft add chain ip filter INPUT '{ type filter hook input priority 0; }'")
        self.log.info("Adding {0} into iptables rules on {1}".format(
            node1.ip, node2.ip))
        command = f"nft 'add rule ip filter OUTPUT ip saddr {node2.ip} counter reject'"
        shell.execute_command(command)

    def unblock_indexer_port(self, node, port=9104):
        shell = RemoteMachineShellConnection(node)
        self.log.info(f"Unblocking port 9104 on {node}")
        try:
            shell.execute_command(f"iptables -D INPUT -p tcp --destination-port {port} -j DROP")
        except:
            pass
        try:
            shell.execute_command(f"iptables -D OUTPUT -p tcp --destination-port {port} -j DROP")
        except:
            pass
        try:
            shell.execute_command(f"iptables -S")
        except:
            pass

    def block_indexer_port(self, node, port=9104):
        shell = RemoteMachineShellConnection(node)
        self.log.info(f"Unblocking port 9104 on {node}")
        shell.execute_command(f"iptables -A INPUT -p tcp --destination-port {port} -j DROP")
        shell.execute_command(f"iptables -A OUTPUT -p tcp --destination-port {port} -j DROP")
        shell.execute_command(f"iptables -S")

    def set_indexer_logLevel(self, loglevel="info"):
        """
        :param loglevel:
        Possible Values
            -- info
            -- debug
            -- warn
            -- verbose
            -- Silent
            -- Fatal
            -- Error
            -- Timing
            -- Trace
        """
        self.log.info("Setting indexer log level to {0}".format(loglevel))
        server = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(server)
        status = rest.set_indexer_params("logLevel", loglevel)

    def wait_until_cluster_is_healthy(self):
        master_node = self.master
        if self.targetMaster:
            if len(self.servers) > 1:
                master_node = self.servers[1]
        rest = RestConnection(master_node)
        is_cluster_healthy = False
        count = 0
        while not is_cluster_healthy and count < 10:
            count += 1
            cluster_nodes = rest.node_statuses()
            for node in cluster_nodes:
                if node.status != "healthy":
                    is_cluster_healthy = False
                    log.info("Node {0} is in {1} state...".format(node.ip,
                                                                  node.status))
                    self.sleep(5)
                    break
                else:
                    is_cluster_healthy = True
        return is_cluster_healthy

    def get_server_indexes_count(self, index_nodes):
        server_index_count = {}
        for server in index_nodes:
            rest = RestConnection(server)
            server_index_count.update(rest.get_indexes_count())

        return server_index_count

    def compute_cluster_avg_rr_index(self):
        index_rr_percentages = []
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for node in index_nodes:
            rest = RestConnection(node)
            all_stats = rest.get_all_index_stats()
            index_rr_percentages.append(all_stats['avg_resident_percent'])
            self.log.info(f"Index avg_resident_ratio for node {node.ip} is {all_stats['avg_resident_percent']}")
        avg_avg_rr = sum(index_rr_percentages) / len(index_rr_percentages)
        return avg_avg_rr

    def dataload_till_rr(self, namespaces, rr=100, json_template='Hotel', batch_size=10000, timeout=1800):
        """
        Run this method only when no Index scans are being served, otherwise reaching to desired rr won't be possible
        """
        start_time = time.time()
        while self.compute_cluster_avg_rr_index() > rr:
            for namespace in namespaces:
                _, keyspace = namespace.split(':')
                bucket, scope, collection = keyspace.split('.')
                key_prefix = ''.join(random.choices(ascii_letters + digits, k=10))
                gen_create = SDKDataLoader(num_ops=100000, percent_create=100, key_prefix=key_prefix,
                                           percent_update=0, percent_delete=0, scope=scope,
                                           collection=collection, json_template=json_template,
                                           output=True, username=self.username, password=self.password)
                if self.use_magma_loader:
                    task = self.cluster.async_load_gen_docs(self.master, bucket=bucket,
                                                            generator=gen_create, pause_secs=1,
                                                            timeout_secs=300, use_magma_loader=True)
                    task.result()
                else:
                    tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=self.gen_create,
                                                                    batch_size=batch_size, dataset=json_template)
                    for task in tasks:
                        task.result()
            if time.time() - start_time < timeout:
                self.sleep(30, "Giving some time to Resident Ratio to settle down")
            else:
                self.log.info(f"Can't reach desired Resident Ratio in {timeout} secs.")

    def index_creation_till_rr(self, rr=100, timeout=1800):
        start_time = time.time()
        characters = string.ascii_letters + string.digits
        while self.compute_cluster_avg_rr_index() > rr:
            for namespace in self.namespaces:
                random_sequence = ''.join(random.choices(characters, k=5))
                definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                          prefix='test_' + random_sequence,
                                                                          similarity=self.similarity, train_list=None,
                                                                          scan_nprobes=self.scan_nprobes,
                                                                          array_indexes=False,
                                                                          limit=self.scan_limit,
                                                                          quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                          quantization_algo_description_vector=self.quantization_algo_description_vector)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replica)

            for query in create_queries:
                if self.compute_cluster_avg_rr_index() > rr:
                    self.run_cbq_query(query)
                else:
                    break
                self.wait_until_indexes_online()
                if time.time() - start_time < timeout:
                    self.sleep(60, "Giving some time to Resident Ratio to settle down")
                else:
                    self.log.info(f"Can't reach desired Resident Ratio in {timeout} secs.")


    def wait_until_indexes_online(self, timeout=600, defer_build=False, check_paused_index=False, schedule_index=False):
        rest = RestConnection(self.master)
        init_time = time.time()
        check = False
        timed_out = False
        while not check:
            index_status = rest.get_index_status()
            next_time = time.time()
            if index_status == {}:
                self.log.error("No indexes are present, this check does not apply!")
                break
            else:
                for index_info in index_status.values():
                    for index_state in index_info.values():
                        if defer_build:
                            if index_state["status"] == "Ready" or index_state["status"] == "Created":
                                check = True
                            else:
                                check = False
                                time.sleep(1)
                                break
                        elif check_paused_index:
                            if index_state["status"] == "Paused" or index_state["status"] == "Ready":
                                check = True
                            else:
                                check = False
                                time.sleep(1)
                                break
                        elif schedule_index:
                            if index_state["status"] == "Ready" or index_state["status"] == "Scheduled for Creation":
                                check = True
                            else:
                                check = False
                                time.sleep(1)
                                break
                        else:
                            if index_state["status"] == "Ready":
                                check = True
                            else:
                                check = False
                                time.sleep(1)
                                break
                    if next_time - init_time > timeout:
                        timed_out = True
                        check = next_time - init_time > timeout
        if timed_out:
            check = False
        return check

    def check_if_indexes_in_dgm(self):
        def check_if_indexes_in_dgm_node(node, indexes_in_dgm):
            indexer_rest = RestConnection(node)
            node_in_dgm = False
            content = indexer_rest.get_index_storage_stats()
            try:
                tot_indexes = len(list(content.values())[0].values())
                index_in_dgm = 0
                for index in list(content.values()):
                    for key, stats in index.items():
                        stats = dict(stats)
                        rr = stats["MainStore"]["resident_ratio"]
                        if rr != 0 and rr < 1.00:
                            index_in_dgm += 1
                            if f'{node}_{index_in_dgm}' not in indexes_in_dgm:
                                indexes_in_dgm.append(f'{node}_{index_in_dgm}')
                                self.log.info("Index : {} , resident_ratio : {}".format(key, rr))
                if index_in_dgm > (tot_indexes/3):
                    node_in_dgm = True
            except IndexError as e:
                node_in_dgm = True
            return node_in_dgm

        indexes_in_dgm = []
        all_nodes_in_dgm = False
        endtime = time.time() + self.dgm_check_timeout
        while not all_nodes_in_dgm and time.time() < endtime:
            all_nodes_in_dgm = True
            for node in self.index_nodes:
                all_nodes_in_dgm = all_nodes_in_dgm and check_if_indexes_in_dgm_node(node, indexes_in_dgm)

        return all_nodes_in_dgm

    def check_if_index_created(self, index, defer_build=False):
        index_created = False
        status = None
        rest = RestConnection(self.master)
        index_status = rest.get_index_status()
        for index_info in index_status.values():
            for k, v in index_info.items():
                if k == index:
                    status = v["status"]
                    if status == "Ready" or status == "Building" or (defer_build and status == "Created"):
                        index_created = True

        return index_created, status

    def get_indexes_not_online(self):
        rest = RestConnection(self.master)
        index_list = []
        index_status = rest.get_index_status()
        for index_info in index_status.values():
            for k, v in index_info.items():
                if v["status"] != "Ready":
                    index_list.append(k)
        return index_list

    def wait_until_specific_index_online(self, index_name='', timeout=600, defer_build=False):
        rest = RestConnection(self.master)
        init_time = time.time()
        check = False
        timed_out = False
        while not check:
            index_status = rest.get_index_status()
            next_time = init_time
            for index_info in list(index_status.values()):
                for idx_name in list(index_info.keys()):
                    if idx_name == index_name:
                        for index_state in list(index_info.values()):
                            if defer_build:
                                if index_state["status"] == "Created":
                                    check = True
                                else:
                                    check = False
                                    time.sleep(1)
                                    next_time = time.time()
                                    break
                            else:
                                if index_state["status"] == "Ready":
                                    check = True
                                else:
                                    check = False
                                    time.sleep(1)
                                    next_time = time.time()
                                    break
            if next_time - init_time > timeout:
                timed_out = True
                check = next_time - init_time > timeout
        if timed_out:
            check = False
        return check

    def verify_index_in_index_map(self, index_name='', timeout=600):
        rest = RestConnection(self.master)
        init_time = time.time()
        check = False
        seen = False
        next_time = init_time
        timed_out = False
        while not check and not timed_out:
            index_status = rest.get_index_status()
            log.info(index_status)
            for index_info in list(index_status.values()):
                if seen:
                    break
                for idx_name in list(index_info.keys()):
                    if idx_name == index_name:
                        seen = True
                        break
                    else:
                        check = False
                        time.sleep(1)
                        next_time = time.time()
            if seen:
                check = True
            if next_time - init_time > timeout:
                timed_out = True
                check = False
        return check

    def get_dgm_for_plasma(self, indexer_nodes=None, memory_quota=256):
        """
        Internal Method to create OOM scenario
        :return:
        """

        def validate_disk_writes(indexer_nodes=None):
            if not indexer_nodes:
                indexer_nodes = self.get_nodes_from_services_map(
                    service_type="index", get_all_nodes=True)
            for node in indexer_nodes:
                indexer_rest = RestConnection(node)
                content = indexer_rest.get_index_storage_stats()
                for index in list(content.values()):
                    for stats in list(index.values()):
                        if stats["MainStore"]["resident_ratio"] >= 1.00:
                            return False
            return True

        def kv_mutations(self, docs=1):
            if not docs:
                docs = self.docs_per_day
            gens_load = self.generate_docs(docs)
            self.full_docs_list = self.generate_full_docs_list(gens_load)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.load(gens_load, buckets=self.buckets, flag=self.item_flag,
                      verify_data=False, batch_size=self.batch_size)

        if self.gsi_type != "plasma":
            return
        if not self.plasma_dgm:
            return
        log.info("Trying to get all indexes in DGM...")
        log.info("Setting indexer memory quota to {0} MB...".format(memory_quota))
        node = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(node)
        rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=memory_quota)
        cnt = 0
        docs = 50 + self.docs_per_day
        while cnt < 100:
            if validate_disk_writes(indexer_nodes):
                log.info("========== DGM is achieved ==========")
                return True
            kv_mutations(self, docs)
            self.sleep(30)
            cnt += 1
            docs += 20
        return False

    def reboot_node(self, node):
        self.log.info("Rebooting node '{0}'....".format(node.ip))
        shell = RemoteMachineShellConnection(node)
        if shell.extract_remote_info().type.lower() == 'windows':
            o, r = shell.execute_command("shutdown -r -f -t 0")
        elif shell.extract_remote_info().type.lower() == 'linux':
            o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        shell.disconnect()
        # wait for restart and warmup on all node
        self.sleep(self.wait_timeout * 3)
        # disable firewall on these nodes
        self.stop_firewall_on_node(node)
        # wait till node is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert([node], self,
                                                             wait_if_warmup=True)

    def prepare_tenants(self, data_load=True, index_creations=True, query_node=None, json_template=None,
                        bucket_prefix='test_bucket_', bucket_num_offset=0, num_buckets=None, defer_build_mix=False, random_bucket_name=None):
        if not json_template:
            json_template = self.json_template
        if not num_buckets:
            num_buckets = self.num_of_tenants
        for i in range(num_buckets):
            self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            if random_bucket_name:
                bucket_name = f"{bucket_prefix}{bucket_num_offset + i}" + "".join(
                    random.choices(string.ascii_lowercase + string.digits, k=5))
            else:
                bucket_name = f"{bucket_prefix}{bucket_num_offset + i}"
            self.cluster.create_standard_bucket(name=bucket_name, port=11222, bucket_params=self.bucket_params)
            self.sleep(10)
            self.collection_rest.create_scope_collection_count(scope_num=self.num_scopes,
                                                               collection_num=self.num_collections,
                                                               scope_prefix=self.scope_prefix,
                                                               collection_prefix=self.collection_prefix,
                                                               bucket=bucket_name)
        self.buckets = self.rest.get_buckets()
        scopes = [f'{self.scope_prefix}_{num + 1}' for num in range(self.num_scopes)]
        collections = [f'{self.collection_prefix}_{num + 1}' for num in range(self.num_collections)]
        for bucket in self.buckets:
            for scope in scopes:
                for collection in collections:
                    namespace = f'default:{bucket}.{scope}.{collection}'
                    if namespace not in self.namespaces:
                        self.namespaces.append(namespace)

        if data_load:
            self.load_data_on_all_tenants(scopes=scopes, collections=collections, json_template=json_template)

        if index_creations:
            try:
                if self.gsi_util_obj:
                    pass
            except AttributeError as err:
                from serverless.gsi_utils import GSIUtils
                self.gsi_util_obj = GSIUtils(self.run_cbq_query)
            if query_node:
                self.query_node = query_node
            self.gsi_util_obj.index_operations_during_phases(namespaces=self.namespaces, dataset=json_template,
                                                             query_node=self.query_node, capella_run=False,
                                                             num_of_batches=self.index_batch_weight,
                                                             defer_build_mix=defer_build_mix)
            self.wait_until_indexes_online(defer_build=defer_build_mix)

    def get_persisted_stats(self, index_node=None):
        persisted_stats_list = ['last_known_scan_time',
                                # 'avg_scan_rate',
                                # 'num_rows_scanned',
                                # 'num_rollbacks',
                                # 'num_rollbacks_to_zero'
                                ]
        if index_node is None:
            index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)
        stats = self.index_rest.get_all_index_stats()
        result = {}
        for stat, value in stats.items():
            if '_system:_query' in stat:
                continue
            for persisted_stat in persisted_stats_list:
                if f':{persisted_stat}' in stat:
                    result[stat] = value
        return result

    def get_rebalance_related_stats(self, index_node=None):
        reblance_related_stats = ['items_count',
                                  'key_size_distribution',
                                  'raw_data_size',
                                  'backstore_raw_data_size',
                                  'arrkey_size_distribution']
        if index_node is None:
            index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)[0]
        self.index_rest = RestConnection(index_node)
        stats = self.index_rest.get_all_index_stats()
        result = {}
        for stat, value in stats.items():
            if '_system:_query' in stat:
                continue
            for reb_stat in reblance_related_stats:
                if f':{reb_stat}' in stat:
                    result[stat] = value
        return result

    def create_S3_config(self):
        COUCHBASE_AWS_HOME = '/home/couchbase/.aws'
        aws_cred_file = ('[default]\n'
                         f'aws_access_key_id={self.aws_access_key_id}\n'
                         f'aws_secret_access_key={self.aws_secret_access_key}')
        aws_conf_file = ('[default]\n'
                         f'region={self.region}\n'
                         'output=json')
        for node in self.servers:
            shell = RemoteMachineShellConnection(node)
            shell.execute_command(f"rm -rf {COUCHBASE_AWS_HOME}")
            shell.execute_command(f"mkdir -p {COUCHBASE_AWS_HOME}")

            shell.create_file(remote_path=f'{COUCHBASE_AWS_HOME}/credentials', file_data=aws_cred_file)
            shell.create_file(remote_path=f'{COUCHBASE_AWS_HOME}/config', file_data=aws_conf_file)

            # adding validation that the file is created and content is available.
            self.log.info("Printing content of .aws directory")
            self.log.info(shell.execute_command(f"ls -l {COUCHBASE_AWS_HOME}"))
            self.log.info("Printing content of config file")
            self.log.info(shell.execute_command(f"cat {COUCHBASE_AWS_HOME}/config"))
            self.log.info("Printing content of credentials file")
            self.log.info(shell.execute_command(f"cat {COUCHBASE_AWS_HOME}/credentials"))

        if self.storage_prefix is None:
            self.storage_prefix = 'indexing_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
            # checking if folder exist and deleting it
            result = self.s3_utils_obj.check_s3_folder_exist(folder=self.storage_prefix)
            if result:
                self.s3_utils_obj.delete_s3_folder(folder=self.storage_prefix)
        # create a folder in S3 bucket
        self.s3_utils_obj.create_s3_folder(folder=self.storage_prefix)

        fast_rebalance_config = {"indexer.settings.rebalance.blob_storage_bucket": self.s3_bucket,
                                 "indexer.settings.rebalance.blob_storage_prefix": self.storage_prefix,
                                 "indexer.settings.rebalance.blob_storage_scheme": "s3"}
        self.index_rest.set_index_settings(fast_rebalance_config)

    def load_data_on_all_tenants(self, scopes, collections, json_template=None, batch_size=10000, num_docs=None,
                                 percent_create=100, percent_update=0, percent_delete=0, key_prefix='doc_',
                                 output=False, start=0):
        if not json_template:
            json_template = self.json_template
        if not num_docs:
            num_docs = self.num_of_docs_per_collection
        tasks = []
        for bucket in self.buckets:
            for s_item in scopes:
                for c_item in collections:
                    if self.use_magma_loader:
                        gen_create = SDKDataLoader(num_ops=num_docs, percent_create=percent_create,
                                                   key_prefix=key_prefix, doc_size=1000, workers=10, ops_rate=10000,
                                                   percent_update=percent_update, percent_delete=percent_delete,
                                                   scope=s_item, collection=c_item, json_template=json_template,
                                                   output=output, start_seq_num=start)
                        tasks.append(self.cluster.async_load_gen_docs(self.master, bucket, gen_create,
                                                                      pause_secs=1, timeout_secs=300,
                                                                      dataset=json_template))

                    else:
                        if self.json_template == 'Magma':
                            self.fail("Magma dataset doesn't work with java_sdk_client. Use use_magma_loader=true")
                        tasks = []
                        for batch_start in range(0, num_docs, batch_size):
                            batch_end = batch_start + batch_size
                            if batch_end >= num_docs:
                                batch_end = num_docs
                            batch_ops = batch_end - batch_start
                            gen_create = SDKDataLoader(num_ops=batch_ops, percent_create=percent_create,
                                                       key_prefix=key_prefix, doc_size=1000,
                                                       percent_update=percent_update, percent_delete=percent_delete,
                                                       scope=s_item, collection=c_item, json_template=json_template,
                                                       output=output, start_seq_num=batch_start, password=self.password)
                            tasks.append(self.cluster.async_load_gen_docs(self.master, bucket, gen_create,
                                                                          pause_secs=1, timeout_secs=300,
                                                                          dataset=json_template))
                    namespace = f'default:{bucket}.{s_item}.{c_item}'
                    if namespace not in self.namespaces:
                        self.namespaces.append(namespace)
        for task in tasks:
            task.result()

    def load_docs_via_magma_server(self, server, bucket, gen):
        url = f'http://{self.magma_flask_host}:{self.magma_flask_port}/run'
        params = {
            'ip' : f'{server.ip}',
            'user' : f'{gen.username}',
            'password' : f'{gen.password}',
            'bucket' : f'{bucket}',
            'create_s' : f'{gen.create_start}',
            'create_e' : f'{gen.create_end}',
            'update_s' : f'{gen.update_start}',
            'update_e' : f'{gen.update_end}',
            'mutate' : f'{gen.mutate}',
            'dim' : f'{gen.dim}',
            'cr' : f'{gen.percent_create}',
            'up' : f'{gen.percent_update}',
            'rd' : f'{gen.percent_delete}',
            'docSize' : f'{gen.doc_size}',
            'keyPrefix' : f'{gen.key_prefix}',
            'scope' : f'{gen.scope}',
            'collection' : f'{gen.collection}',
            'valueType' : f'{gen.json_template}',
            'ops' : f'{gen.ops_rate}',
            'workers' : f'{gen.workers}',
            'timeout' : f'{gen.timeout}',
            'debug' : f'{gen.get_sdk_logs}'
        }

        response = requests.post(url, data=params)
        self.log.info(f"Response from magma server is {response.content}")
        if response.status_code != 200:
            raise Exception(f'exception is {response.content}')
        else:
            self.log.info(f'Doc loading is sucessful on bucket {bucket}, scope {gen.scope}, collection {gen.collection}')

    def prepare_collection_for_indexing(self, num_scopes=1, num_collections=1, num_of_docs_per_collection=1000,
                                        indexes_before_load=False, json_template="Person", batch_size=10**4,
                                        bucket_name=None, key_prefix='doc_', load_default_coll=False, base64=False,
                                        model="sentence-transformers/all-MiniLM-L6-v2"):
        if not bucket_name:
            bucket_name = self.test_bucket
        pre_load_idx_pri = None
        pre_load_idx_gsi = None
        self.collection_rest.create_scope_collection_count(scope_num=num_scopes, collection_num=num_collections,
                                                           scope_prefix=self.scope_prefix,
                                                           collection_prefix=self.collection_prefix,
                                                           bucket=bucket_name)
        scopes = [f'{self.scope_prefix}_{scope_num + 1}' for scope_num in range(num_scopes)]
        self.sleep(10, "Allowing time after collection creation")
        if load_default_coll:
            scopes.append("_default")
        if num_of_docs_per_collection > 0:
            for s_item in scopes:
                if load_default_coll and s_item == '_default':
                    collections = ['_default']
                else:
                    collections = [f'{self.collection_prefix}_{coll_num + 1}' for coll_num in range(num_collections)]
                for c_item in collections:
                    self.namespaces.append(f'default:{bucket_name}.{s_item}.{c_item}')
                    if indexes_before_load:
                        pre_load_idx_pri = QueryDefinition(index_name='pre_load_idx_pri')
                        pre_load_idx_gsi = QueryDefinition(index_name='pre_load_idx_gsi', index_fields=['firstName'])
                        query = pre_load_idx_pri.generate_primary_index_create_query(namespace=self.namespaces[0])
                        self.run_cbq_query(query=query)
                        query = pre_load_idx_gsi.generate_index_create_query(namespace=self.namespaces[0])
                        self.run_cbq_query(query=query)

                    self.gen_create = SDKDataLoader(num_ops=num_of_docs_per_collection, percent_create=100,
                                                    percent_update=0, percent_delete=0, scope=s_item,
                                                    collection=c_item, json_template=json_template,
                                                    output=True, username=self.username, password=self.password,
                                                    key_prefix=key_prefix, base64=base64, model=model)
                    if self.use_magma_loader:
                        task = self.cluster.async_load_gen_docs(self.master, bucket=bucket_name,
                                                                generator=self.gen_create,
                                                                use_magma_loader=True)
                        task.result()
                    else:
                        tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=self.gen_create,
                                                                        batch_size=batch_size, dataset=json_template)
                        for task in tasks:
                            task.result()
                    if self.dgm_run:
                        self._load_doc_data_all_buckets(gen_load=self.gen_create)
        return pre_load_idx_pri, pre_load_idx_gsi

    def delete_bucket_scope_collection(self, delete_item, server, bucket, scope='test_scope_1',
                                       collection='test_collection_1', timeout=None):
        if not server:
            server = self.servers[0]
        if not bucket:
            bucket = self.test_bucket
        if not delete_item:
            delete_item = self.item_to_delete
        if delete_item == 'bucket':
            self.log.info(f"Deleting bucket: {bucket}")
            return self.cluster.bucket_delete(server=server, bucket=bucket, timeout=timeout)
        elif delete_item == 'scope':
            self.log.info(f"Deleting Scope: {scope}")
            return self.collection_rest.delete_scope(bucket=bucket, scope=scope)
        elif delete_item == 'collection':
            self.log.info(f"Deleting Collection: {collection}")
            return self.collection_rest.delete_collection(bucket=bucket, scope=scope, collection=collection)

    def get_indexer_mem_quota(self, indexer_node=None):
        """
        Get Indexer memory Quota
        :param indexer_node:
        """
        if not indexer_node:
            indexer_node = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(indexer_node)
        content = rest.cluster_status()
        return int(content['indexMemoryQuota'])

    def update_master_node(self):
        for server in self.servers:
            try:
                rest = RestConnection(server)
                master = rest.get_orchestrator()
                break
            except Exception as err:
                pass
        for server in self.servers:
            if server.ip == master:
                self.master = server
                break
        return server

    def get_nodes_uuids(self):
        if self.use_https:
            port = '18091'
            connection_protocol = 'https'
        else:
            port = '8091'
            connection_protocol = 'http'

        api = f'{connection_protocol}://{self.master.ip}:{port}/pools/default'
        status, content, header = self.rest.urllib_request(api)
        if not status:
            raise Exception(content)
        results = json.loads(content)['nodes']
        node_ip_uuid_map = {}
        for node in results:
            ip = node['hostname'].split(':')[0]
            node_ip_uuid_map[ip] = node['nodeUUID']
        return node_ip_uuid_map

    def get_mutation_vectors(self):
        self.log.info("Grepping for 'MutationResult' in java_sdk_loader.log")
        out = subprocess.check_output(['wc', '-l', 'java_sdk_loader.log'], universal_newlines=True).strip().split()[0]
        if eval(out) != 0:
            out = subprocess.check_output(['grep', 'MutationResult', 'java_sdk_loader.log'],
                                          universal_newlines=True).strip().split('\n')
            return set(out)
        else:
            return set()

    def convert_mutation_vector_to_scan_vector(self, mvectors):
        vectors = re.findall(r'.*?vbID=(.*?), vbUUID=(.*?), seqno=(.*?),', str(mvectors))
        scan_vector = {}
        for vector in vectors:
            vector = list(vector)
            scan_vector[str(vector[0])] = [int(vector[2]), str(vector[1])]
        return scan_vector


    def validate_smart_batching_during_rebalance(self, rebalance_task):
        while self.rest._rebalance_progress() == 100 and rebalance_task.state != 'CHECKING':
            progress = self.rest._rebalance_progress()
            state = rebalance_task.state
            self.log.info(f'Progress:{progress}')
            self.log.info(f'state:{state}')
            self.sleep(5)
        self.sleep(5)
        # Validating no. of parallel concurrent build
        start_time = time.time()
        while self.rest._rebalance_progress() < 100:
            indexer_metadata = self.index_rest.get_indexer_metadata()['status']
            moving_indexes_count = 0
            # self.log.info(indexer_metadata)
            for index in indexer_metadata:
                if index['status'] == 'Moving' and index['completion'] > 0 and index['completion'] < 100:
                    moving_indexes_count += 1
            self.log.info(f"No. of Indexes in Moving State: {moving_indexes_count}")
            if moving_indexes_count > self.transfer_batch_size:
                self.fail("No. of parallel index builds are more than 'transfer batch size'")

            curr_time = time.time()
            if curr_time - start_time > self.rebalance_timeout:
                self.fail("Rebalance got stuck or it's taking longer than expect. Please check the logs")
            self.sleep(5)

        result = rebalance_task.result()
        self.log.info(result)
        rebalance_status = RestHelper(self.rest).rebalance_reached()
        self.assertTrue(rebalance_status, "rebalance failed, stuck or did not complete")

    def get_all_indexes_in_the_cluster(self):
        index_name_list = []
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(indexer_nodes[0])
        index_status = rest.get_indexer_metadata()
        for index_info in index_status['status']:
            index_name_list.append(index_info['name'])
        return index_name_list

    def encode_floats_to_base64(self, float_array):
        # Pack the float array into bytes
        byte_data = struct.pack(f'{len(float_array)}f', *float_array)
        # Encode the byte data to a base64 string
        base64_encoded = base64.b64encode(byte_data).decode('utf-8')
        return base64_encoded

    def decode_base64_to_float(self, value):
        byte_data = base64.b64decode(value)
        num_floats = len(byte_data) // struct.calcsize('f')
        # Unpack bytes into floats
        result = list(struct.unpack(f'{num_floats}f', byte_data))
        return result

    def convert_to_faiss_queries(self, select_query):
        # Regular expression to remove ORDER BY clause
        pattern = r'\bORDER\s+BY\b.*'

        # Remove everything after 'ORDER BY'
        query_with_no_order_by = re.sub(pattern, '', select_query, flags=re.IGNORECASE).strip()

        return query_with_no_order_by

    def get_nodes_in_cluster_after_upgrade(self, master_node=None):
        rest = None
        if master_node == None:
            rest = RestConnection(self.master)
        else:
            rest = RestConnection(master_node)
        nodes = rest.node_statuses()
        server_set = []
        for node in nodes:
            for server in self.input.servers:
                if server.ip == node.ip:
                    server_set.append(server)
        return server_set

    def validate_node_placement_with_nodes_clause(self, create_queries):
        indexer_node = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(indexer_node)
        indexer_metadata = rest.get_indexer_metadata()['status']

        index_map = {}
        for index_query in create_queries:
            out = re.search(self.create_query_node_pattern, index_query, re.IGNORECASE)


            index_name, nodes = out.groups()
            nodes = [node.strip("' ") for node in nodes.split(',')]
            index_map[index_name.strip('`')] = nodes

        for idx in indexer_metadata:
            if idx['scope'] == '_system':
                continue
            idx_name = idx["indexName"]
            if idx_name not in index_map:
                continue
            host = idx['hosts'][0]
            self.assertTrue(host in index_map[idx_name], "Index is not hosted on specified Node")


    def validate_shard_affinity(self, specific_indexes=None, node_in=None, provisioned=True):
        if not self.capella_run:
            if not self.is_shard_based_rebalance_enabled(provisioned=provisioned, node_in=node_in):
                self.log.info("Skipping validating shard affinity since shard based rebalance is disabled")
                return
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = None
        for node in indexer_nodes:
            rest = RestConnection(node)
            if float(rest.get_major_version()) >= 7.6:
                break
        index_resp = rest.get_indexer_metadata(return_system_query_scope=False)
        mixed_mode = False
        node_versions_set = set()
        for node in self.get_nodes_in_cluster_after_upgrade():
            rest = RestConnection(node)
            node_versions_set.add(rest.get_complete_version())
        if len(list(node_versions_set)) > 1:
            mixed_mode = True
        if 'status' not in index_resp:
            raise Exception(f"No index metadata seen in response {index_resp}")
        indexer_metadata = index_resp['status']
        if self.gsi_type == 'memory_optimized':
            self.log.info("Validating that none of the indexes have alternate shards for MOI type indexes")
            shard_id_list = self.fetch_shard_id_list()
            # TODO uncomment after https://issues.couchbase.com/browse/MB-58480 is fixed
            if shard_id_list:
                raise Exception(f"Alternate shard ID seen for MOI type indexes. Shard ID list {shard_id_list}")
        else:
            for index_metadata in indexer_metadata:
                if not specific_indexes:
                    if not index_metadata['alternateShardIds'] and not mixed_mode:
                        self.log.error(f"Indexer metadata {indexer_metadata}")
                        raise Exception(
                            f"Alternate shard ID value None for index {index_metadata['name']} with definition {index_metadata['definition']}")
                else:
                    if not index_metadata['alternateShardIds'] and not mixed_mode and index_metadata['name'] in specific_indexes:
                        self.log.error(f"Indexer metadata {indexer_metadata}")
                        raise Exception(
                            f"Alternate shard ID value None for index {index_metadata['name']} with definition {index_metadata['definition']}")
                # 2nd field of alternate shard ID should be the replica ID validation
                if not specific_indexes:
                    for host in index_metadata['alternateShardIds']:
                        for partition in index_metadata['alternateShardIds'][host]:
                            shard_list = index_metadata['alternateShardIds'][host][partition]
                            for shard in shard_list:
                                shard_replica_id = int(shard.split("-")[1])
                                replica_id = int(index_metadata['replicaId'])
                                if shard_replica_id != replica_id:
                                    self.log.error(f"Indexer metadata {indexer_metadata}")
                                    self.log.error(f"shard_replica_id  {shard_replica_id} and replica_id {replica_id}")
                                    raise Exception(f"Alternate shard ID and replica ID mismatch for index "
                                                    f"{index_metadata['name']} with definition {index_metadata['definition']} and definition ID {index_metadata['defnId']}. Alt shard ID {shard}.")
                else:
                    if index_metadata['name'] in specific_indexes:
                        for host in index_metadata['alternateShardIds']:
                            for partition in index_metadata['alternateShardIds'][host]:
                                shard_list = index_metadata['alternateShardIds'][host][partition]
                                for shard in shard_list:
                                    shard_replica_id = int(shard.split("-")[1])
                                    replica_id = int(index_metadata['replicaId'])
                                    if shard_replica_id != replica_id:
                                        self.log.error(f"Indexer metadata {indexer_metadata}")
                                        raise Exception(f"Alternate shard ID and replica ID mismatch for index "
                                                        f"{index_metadata['name']} with definition {index_metadata['definition']}. Alt shard ID {shard}.")
                if not specific_indexes:
                    # only one alternate shard ID for primary index validation
                    if 'isPrimary' in index_metadata:
                        for host in index_metadata['alternateShardIds']:
                            for partition in index_metadata['alternateShardIds'][host]:
                                if len(index_metadata['alternateShardIds'][host][partition]) != 1:
                                    self.log.error(f"Indexer metadata {indexer_metadata}")
                                    raise Exception(
                                        f"There are more than 1 instances of alternate shard ID for primary index "
                                        f"{index_metadata['name']} with definition {index_metadata['definition']}")
                    # only 2 alternate shard IDs for secondary index validation
                    else:
                        for host in index_metadata['alternateShardIds']:
                            for partition in index_metadata['alternateShardIds'][host]:
                                if len(index_metadata['alternateShardIds'][host][partition]) != 2:
                                    self.log.error(f"Indexer metadata {indexer_metadata}")
                                    raise Exception(
                                        f"There are more than 1 instances of alternate shard ID for primary index "
                                        f"{index_metadata['name']} with definition {index_metadata['definition']}")

                else:
                    # only one alternate shard ID for primary index validation
                    if 'isPrimary' in index_metadata and index_metadata['name'] in specific_indexes:
                        for host in index_metadata['alternateShardIds']:
                            for partition in index_metadata['alternateShardIds'][host]:
                                if len(index_metadata['alternateShardIds'][host][partition]) != 1:
                                    self.log.error(f"Indexer metadata {indexer_metadata}")
                                    raise Exception(
                                        f"There are more than 1 instances of alternate shard ID for primary index "
                                        f"{index_metadata['name']} with definition {index_metadata['definition']}")
                    # only 2 alternate shard IDs for secondary index validation
                    else:
                        if index_metadata['name'] in specific_indexes:
                            for host in index_metadata['alternateShardIds']:
                                for partition in index_metadata['alternateShardIds'][host]:
                                    if len(index_metadata['alternateShardIds'][host][partition]) != 2:
                                        self.log.error(f"Indexer metadata {indexer_metadata}")
                                        raise Exception(
                                            f"There are more than 1 instances of alternate shard ID for primary index "
                                            f"{index_metadata['name']} with definition {index_metadata['definition']}")
            self.log.info("All indexes have 2 alternate shard IDs and all primary indexes have 1 alt shard ID.")
            # replicas sharing slot IDs and instance IDs validation
            if not mixed_mode:
                self.validate_replica_indexes(index_map=indexer_metadata)
                self.log.info("All replicas share slot IDs")
            # shard IDs unique to host validation
            self.validate_shard_unique_to_host(index_map=indexer_metadata, specific_index=specific_indexes)
            self.log.info("Shards unique to host validation successful")

    def fetch_shard_id_list(self, return_system_query_scope=False):
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = None
        for node in indexer_nodes:
            rest = RestConnection(node)
            if float(rest.get_major_version()) >= 7.6:
                break
        if not rest:
            raise Exception("No 7.6 index nodes in the cluster")
        index_resp = rest.get_indexer_metadata(return_system_query_scope=return_system_query_scope)
        if 'status' not in index_resp:
            self.log.error(f"No metadata seen in response. {index_resp}")
            return []
        index_map = index_resp['status']
        shards_list_all = []
        for index_metadata in index_map:
            if 'alternateShardIds' in index_metadata:
                for host in index_metadata['alternateShardIds']:
                    for partition in index_metadata['alternateShardIds'][host]:
                        shard_list = index_metadata['alternateShardIds'][host][partition]
                        for shard in shard_list:
                            if shard not in shards_list_all:
                                shards_list_all.append(shard)
        return sorted(shards_list_all)

    def validate_alternate_shard_ids_presence(self, node_in=None):
        if not self.is_shard_based_rebalance_enabled(provisioned=True, node_in=node_in):
            self.log.info("Skipping validating alternate shard id presence since shard based rebalance is disabled")
            return
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for node in indexer_nodes:
            rest = RestConnection(node)
            if float(rest.get_major_version()) >= 7.6:
                rest = RestConnection(node)
                index_resp = rest.get_indexer_metadata(return_system_query_scope=False)
                index_map = index_resp['status']
                for index_metadata in index_map:
                    host_ip_list = [host.split(":")[0] for host in index_metadata['hosts']]
                    if node.ip in host_ip_list and index_metadata['alternateShardIds'] is None:
                        raise Exception(
                            f"Index {index_metadata['name']} present on 7.6 node {node.ip} but lacks alternate shard ID")

    def perform_continuous_kv_mutations(self, event, timeout=1800, num_docs_mutating=10000):
        collection_namespaces = self.namespaces
        time_now = time.time()
        while not event.is_set() and time.time() - time_now < timeout:
            for namespace in collection_namespaces:
                _, keyspace = namespace.split(':')
                bucket, scope, collection = keyspace.split('.')
                self.gen_create = SDKDataLoader(num_ops=num_docs_mutating, percent_create=100,
                                                percent_update=10, percent_delete=0, scope=scope,
                                                collection=collection, json_template=self.json_template,
                                                output=True, username=self.username, password=self.password)
                if self.use_magma_loader:
                    task = self.cluster.async_load_gen_docs(self.master, bucket=bucket,
                                                            generator=self.gen_create, pause_secs=1,
                                                            timeout_secs=300, use_magma_loader=True)
                    task.result()
                else:
                    tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=self.gen_create,
                                                                    batch_size=10**4, dataset=self.json_template)
                    for task in tasks:
                        task.result()
                time.sleep(10)

    def validate_shard_unique_to_host(self, index_map, specific_index=None):
        shard_host_map = {}
        for index_metadata in index_map:
            if not specific_index:
                for host in index_metadata['alternateShardIds']:
                    for partition in index_metadata['alternateShardIds'][host]:
                        shard_list = index_metadata['alternateShardIds'][host][partition]
                        host_ip = host
                        for shard in shard_list:
                            shard_entry = shard + "-partition-" + partition
                            if shard in shard_host_map and shard_host_map[shard] != host_ip:
                                raise Exception(f"Shard {shard} is shared between {shard_host_map[shard]} and {host_ip}")
                            else:
                                shard_host_map[shard_entry] = host_ip
            else:
                if index_metadata['name'] in specific_index:
                    for host in index_metadata['alternateShardIds']:
                        for partition in index_metadata['alternateShardIds'][host]:
                            shard_list = index_metadata['alternateShardIds'][host][partition]
                            host_ip = host
                            for shard in shard_list:
                                shard_entry = shard + "-partition-" + partition
                                if shard in shard_host_map and shard_host_map[shard] != host_ip:
                                    raise Exception(f"Shard {shard} is shared between {shard_host_map[shard]} and {host_ip}")
                                else:
                                    shard_host_map[shard_entry] = host_ip

    def validate_replica_indexes(self, index_map):
        replicas_dict = {}
        for index in index_map:
            if index['numReplica'] > 0:
                if index['defnId'] in replicas_dict:
                    replicas_dict[index['defnId']].append(index)
                else:
                    replicas_dict[index['defnId']] = [index]
        # Replicas share shard ID validation
        for replica_num, replica in enumerate(replicas_dict):
            replica_shards_dict = {}
            for index, index_metadata in enumerate(replicas_dict[replica]):
                replica_shards_dict[replicas_dict[replica][index]['replicaId']] = set()
                for item in replicas_dict[replica][index]['alternateShardIds']:
                    for shard_items_list in replicas_dict[replica][index]['alternateShardIds'][item].values():
                        for shard_id in shard_items_list:
                            if shard_id.split("-")[0] not in replica_shards_dict[replicas_dict[replica][index]['replicaId']]:
                                replica_shards_dict[replicas_dict[replica][index]['replicaId']].add(shard_id.split("-")[0])
            self.log.debug(f"Replicas shard dict is {replica_shards_dict}\n\n for defn ID {replicas_dict[replica]} \n")
            len_max = -1
            for item in replica_shards_dict:
                if len(replica_shards_dict[item]) > len_max:
                    len_max = len(replica_shards_dict[item])
                    all_possible_shards_replica_id = item
            for item in replica_shards_dict:
                if item != all_possible_shards_replica_id:
                    for shard in replica_shards_dict[item]:
                        if shard not in replica_shards_dict[all_possible_shards_replica_id]:
                            self.log.info(f"Index metadata is {index_map}")
                            raise Exception(f"Replicas don't share slot ID for replica ID {replica_shards_dict}")
        # Replicas reside on different hosts validation
        for replica in replicas_dict:
            for index, index_metadata in enumerate(replicas_dict[replica]):
                host_ip = index_metadata['hosts']
                for index_metadata_2 in replicas_dict[replica][index+1:]:
                    if index_metadata_2['numPartition'] == 1:
                        if index_metadata_2['hosts'] == host_ip:
                            self.log.error(f"Index_metadata {index_metadata} being compared against index_metadata_2 {index_metadata_2}")
                            self.log.info(f"Index metadata is {index_map}")
                            raise Exception("Replicas reside on the same host.")

                    else:
                        for host in index_metadata['partitionMap']:
                            for partition in index_metadata['partitionMap'][host]:
                                if host in index_metadata_2['partitionMap']:
                                    if partition in index_metadata_2['partitionMap'][host]:
                                        self.log.info(f"Index metadata is {index_map}")
                                        raise AssertionError(
                                            f"Partition replicas reside on the same host. Metadata 1 {index_metadata} Metadata 2 {index_metadata_2}")
    def check_gsi_logs_for_shard_transfer(self):
        """ Checks if file transfer based rebalance is triggered.
        """
        count = 0
        log_string = "ShardTransferToken(v2) generated token.*BuildSource: Peer"
        log_validated = False
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if not indexer_nodes:
            return None
        for server in indexer_nodes:
            shell = RemoteMachineShellConnection(server)
            _, dir = RestConnection(server).diag_eval(
                'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
            indexer_log = str(dir) + '/indexer.log*'
            count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                               format(log_string, indexer_log))
            if isinstance(count, list):
                count = int(count[0])
            else:
                count = int(count)
            shell.disconnect()
            if count > 0:
                self.log.info(f"===== File transfer based rebalance triggered "
                              f"as validated from the log on {server.ip}=====. The no. of occurrences - {count}")
                log_validated = True
                break
        if not log_validated:
            self.log.info(f"===== File transfer based rebalance not triggered."
                          f"No log lines matching string {log_string} seen on any of the indexer nodes.")
        return log_validated

    def enable_shard_based_rebalance(self, provisioned=False):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        cluster_profile = rest.get_cluster_config_profile()
        if provisioned:
            rest.set_shard_affinity_provisoned_mode({"indexer.default.enable_shard_affinity": True})
        else:
            rest.set_index_settings({"indexer.settings.enable_shard_affinity": True})

    def disable_shard_based_rebalance(self, provisioned=False):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        if provisioned:
            rest.set_shard_affinity_provisoned_mode({"indexer.default.enable_shard_affinity": False})
        else:
            rest.set_index_settings({"indexer.settings.enable_shard_affinity": False})

    def enable_corrupt_index_backup(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.settings.enable_corrupt_index_backup": True})

    def enable_shard_seggregation(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.planner.use_shard_dealer": True})

    def disable_shard_seggregation(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.planner.use_shard_dealer": False})

    def enable_corrupt_index_backup(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.settings.enable_corrupt_index_backup": True})

    def enable_planner(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"queryport.client.usePlanner": True})

    def disable_planner(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"queryport.client.usePlanner": False})

    def enable_honour_nodes_in_definition(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.planner.honourNodesInDefn": True})

    def disable_honour_nodes_in_definition(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.settings.enable_shard_affinity": False})

    def enable_redistribute_indexes(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.settings.rebalance.redistribute_indexes": True})

    def disable_redistribute_indexes(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.settings.rebalance.redistribute_indexes": False})

    def is_shard_based_rebalance_enabled(self, provisioned=False, node_in=None):
        shard_affinity = False
        if self.gsi_type == 'plasma':
            if node_in:
                rest = RestConnection(node_in)
            else:
                indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
                rest = RestConnection(indexer_node)
            if provisioned:
                shard_affinity = rest.get_indexer_internal_stats()["indexer.default.enable_shard_affinity"]
            else:
                shard_affinity = rest.get_index_settings()["indexer.settings.enable_shard_affinity"]
        return shard_affinity

    def set_max_instances_per_shard(self, instance_count):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.plasma.maxInstancePerShard": instance_count})

    def set_max_shards_per_tenant(self, shard_count=2000):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.plasma.shardLimitPerTenant": shard_count})

    def set_shard_tenant_multiplier(self, multiplier=5):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.plasma.shardTenantMultiplier": multiplier})

    def set_minimum_instances_per_shard(self, instance_count):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.plasma.minNumShard": instance_count})

    def set_maximum_disk_usage_per_shard(self, disk_size=268435456000):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.plasma.maxDiskUsagePerShard": disk_size})

    def set_flush_buffer_quota(self, quota=4.0):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.plasma.flushBufferQuota": quota})

    def set_flush_buffer_size(self, size=1048576):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.plasma.flushBufferSize": size})

    def set_persisted_snapshot_interval(self, interval=600000):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": interval})

    def find_unique_slot_id_per_node(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        data=rest.get_indexer_metadata()['status']

        ip_to_slot_id = {}

        # Iterate over each dictionary in the list
        for entry in data:
            # Iterate over the nested alternateShardIds dictionary
            for ip, shard_ids in entry["alternateShardIds"].items():
                # Check if the IP address already exists in the mapping
                if ip in ip_to_slot_id:
                    # Extend the list of shard IDs for the existing IP address
                    ip_to_slot_id[ip].extend(shard_ids.values())
                else:
                    # Create a new entry in the mapping for the IP address
                    ip_to_slot_id[ip] = list(shard_ids.values())
        for key, value in ip_to_slot_id.items():
            ip_to_slot_id[key] = [str(item).split("-")[0] for sublist in value for item in sublist]
            ip_to_slot_id[key] = list(set(ip_to_slot_id[key]))

        return ip_to_slot_id


    def fetch_total_shards_limit(self):
        indexer_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        rest = RestConnection(indexer_node)
        settings = rest.get_indexer_internal_stats()
        max_shard_count = settings['indexer.plasma.shardLimitPerTenant']
        flush_buffer_quota = settings['indexer.plasma.flushBufferQuota']
        mem_quota = self.get_indexer_mem_quota()
        shard_limit = math.floor(mem_quota * 0.9 * flush_buffer_quota / 100)
        if shard_limit % 2 != 0:
            shard_limit += 1
        return min(max_shard_count, shard_limit)

    def perform_ddl_operations_during_rebalance(self):
        query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        create_queries = []
        for namespace in self.namespaces:
            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace, defer_build=True,
                                                              num_replica=self.num_index_replica)
            for query in queries:
                create_queries.append(query)
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(indexer_nodes[0])
        index_resp = rest.get_indexer_metadata(return_system_query_scope=False)
        if 'status' not in index_resp:
            raise Exception(f"No index metadata seen in response {index_resp}")
        index_map = index_resp['status']
        replicas_dict = {}
        for index in index_map:
            if index['numReplica'] > 0:
                if index['defnId'] in replicas_dict:
                    replicas_dict[index['defnId']].append(index)
                else:
                    replicas_dict[index['defnId']] = [index]
        replicas_dict_items_list = random.choices(list(replicas_dict.keys()), k=6)
        update_queries = []
        for replicas_dict_item in replicas_dict_items_list:
            item = replicas_dict[replicas_dict_item]
            replica_index = random.choice(item)
            bucket, name, replicaID = replica_index['bucket'], replica_index['indexName'], replica_index[
                'replicaId']
            scope, collection = replica_index['scope'], replica_index['collection']
            query_type = random.choice(['alter', 'drop'])
            if query_type == 'alter':
                query = f'ALTER INDEX default:`{bucket}`.`{scope}`.`{collection}`.`{name}` WITH {{"action":"drop_replica","replicaId": {replicaID}}}'
            else:
                query = f'DROP INDEX default:`{bucket}`.`{scope}`.`{collection}`.`{name}`'
            update_queries.append(query)
        queries_all = create_queries + update_queries
        random.shuffle(queries_all)
        random.shuffle(queries_all)
        for query in queries_all:
            if self.rest._rebalance_status_and_progress()[0] != 'running':
                self.log.info("Rebalance has completed. Will exit the ddl loop")
                return
            try:
                self.run_cbq_query(query=query, server=query_node)
            except Exception as err:
                self.log.error(f"Error occurred during DDL operation as expected: {err}. Query: {query}")
            else:
                if self.rest._rebalance_status_and_progress()[0] == 'running':
                    raise Exception(f"DDL operations went through successfully "
                                    f"with no exceptions even though rebalance is running. Query is {query}")

    def perform_rebalance_during_ddl(self, nodes_in_list, nodes_out_list, services_in):
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        tasks = []
        replica_count = random.randint(0, 2)
        query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
        all_queries = []
        for namespace in self.namespaces:
            create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace,
                                                              num_replica=replica_count,
                                                              randomise_replica_count=True)
            all_queries.extend(create_queries)
        with ThreadPoolExecutor() as executor:
            for query in all_queries:
                tasks.append(executor.submit(self.run_cbq_query, query=query, server=query_node))
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], nodes_in_list, nodes_out_list,
                                                     services=services_in, cluster_config=self.cluster_config)
        self.log.info(f"Rebalance task triggered. Sleeping for 60 seconds until the rebalance starts")
        time.sleep(60)
        try:
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance did not fail despite DDL operations")
            rebalance.result()
        except Exception as ex:
            if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
        else:
            self.fail("rebalance did not fail despite build index being in progress")
        self.log.info("Sleeping for five mins waiting for all the indexes to get built")
        time.sleep(300)

    def corrupt_plasma_metadata(self, node):
        rest = RestConnection(node)
        storage_dir = rest.get_indexer_internal_stats()['indexer.storage_dir']
        shard_path = os.path.join(storage_dir, 'shards')
        shell = RemoteMachineShellConnection(node)
        shards_list = shell.execute_command(f'ls {shard_path}')[0]
        self.log.info(f"The shards are {shards_list}")
        shard_chosen = random.choice(shards_list)
        shard_abs_path = os.path.join(shard_path, shard_chosen)
        self.log.info(f"The chosen shard for metadata corruption is {shard_chosen}")
        shell.execute_command(f'touch {shard_abs_path}/error')

    def fetch_plasma_shards_list(self):
        json_resp = self.index_rest.get_index_aggregate_metadata()
        shards = set()
        for each_item in json_resp['result']['metadata']:
            for item in each_item['topologies']:
                if item["scope"] == '_system':
                    continue
                for defn in item['definitions']:
                    for instance in defn['instances']:
                        for partition in instance['partitions']:
                            for shard in partition['shardIds']:
                                shards.add(shard)
        return list(shards)

    def find_max_plasma_shards_allowed(self):
        memory_quota = self.get_indexer_mem_quota()
        plasma_quota = .9 * memory_quota
        max_shards = 5 * plasma_quota * .04
        return min(max_shards, 2000)

    def load_data_into_faiss(self, vectors, dim=None, similarity="L2_SQUARED"):
        if dim is None:
            dim = len(vectors[0])

        if similarity == "L2_SQUARED" or similarity == "L2" or similarity == "EUCLIDEAN_SQUARED" or similarity == "EUCLIDEAN":
            index = faiss.IndexFlatL2(dim)
        elif similarity == "DOT":
            index = faiss.IndexFlatIP(dim)
        else:
            faiss.normalize_L2(vectors)
            index = faiss.IndexFlatIP(dim)

        # Might not need it.
        #faiss.normalize_L2(vectors)
        index.add(vectors)
        return index

    def search_ann_in_faiss(self, query, faiss_db, limit=100, n_probe=None):
        _vector = np.array([query], dtype="float32")
        # Might not need it.
        #faiss.normalize_L2(_vector)
        if n_probe is None:
            n_probe = faiss_db.ntotal
        dist, ann = faiss_db.search(_vector, k=n_probe)
        return ann[0][:limit]

    def gen_table_view(self, query_stats_map, message="query stats"):
        from table_view import TableView
        table = TableView(self.log.info)
        table.set_headers(['Query', 'Recall', 'Accuracy'])
        for query in query_stats_map:
            table.add_row([query, query_stats_map[query][0], query_stats_map[query][1]])
        table.display(message=message)

    def display_recall_and_accuracy_stats(self, select_queries, message="query stats", stats_assertion=True,
                                          similarity="L2_SQUARED"):
        query_stats_map = {}
        for query in select_queries:
            # this is to ensure that select queries run primary indexes are not tested for recall and accuracy
            if "ANN" not in query:
                continue
            redacted_query, recall, accuracy = self.validate_scans_for_recall_and_accuracy(select_query=query,
                                                                                           similarity=similarity)
            query_stats_map[redacted_query] = [recall, accuracy]
        self.gen_table_view(query_stats_map=query_stats_map, message=message)
        if stats_assertion:
            for query in query_stats_map:
                self.assertGreaterEqual(query_stats_map[query][0] * 100, 70,
                                        f"recall for query {query} is less than threshold 70")
                # uncomment the below code snippet to do assertions for accuracy
                # self.assertGreaterEqual(query_stats_map[query][1] * 100, 70,
                #                         f"accuracy for query {query} is less than threshold 70")
    def validate_error_msg_and_doc_count_in_cbcollect(self, node, error_message):
        shell = RemoteMachineShellConnection(node)
        if shell.extract_remote_info().type.lower() == 'windows':
            log_path = WIN_COUCHBASE_LOGS_PATH
        elif shell.extract_remote_info().type.lower() == 'linux':
            log_path = LINUX_COUCHBASE_LOGS_PATH

        cmd = f"cd {log_path}; grep -r '{error_message}' *"
        output, error = shell.execute_command(cmd)

        if len(output) == 0:
            return False
        return True

    def get_item_counts_from_kv(self):
        """
        Gets all indexes and their definitions using N1QL system:indexes
        Returns a list of dictionaries containing index name and actual count from executing query
        """
        query = "SELECT bucket_id, scope_id, keyspace_id, name, index_key, `condition` FROM system:indexes"
        result = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)

        simplified_results = []
        # Add select query field for each index
        for index in result['results']:
            # Build the keyspace string
            bucket = index['bucket_id']
            scope = index.get('scope_id', '_default')
            collection = index.get('keyspace_id', '_default')
            keyspace = f"`{bucket}`.`{scope}`.`{collection}`"

            where_clause = ""
            if index.get('condition'):
                where_clause = f" WHERE {index['condition']}"

            count_query = f"SELECT COUNT(*) FROM {keyspace} {where_clause}"

            # Execute the count query
            try:
                count_result = self.n1ql_helper.run_cbq_query(query=count_query, server=self.n1ql_node)
                count = count_result['results'][0]['$1']
            except Exception as e:
                self.log.error(f"Error executing count query for index {index['name']}: {str(e)}")
                count = 0

            # Add only name and count to results
            simplified_results.append({
                "name": f"default:{bucket}.{scope}.{collection}.{index['name']}",
                "count": count
            })

        return simplified_results

    def get_item_counts_from_index_stats(self):
        """
        Gets item count for all indexes using the /stats endpoint
        Returns a list of dictionaries containing index name and aggregated items_count
        Handles partitioned indexes by summing counts across nodes
        """
        index_counts_map = {}  # Use map to aggregate counts
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for node in indexer_nodes:
            rest = RestConnection(node)
            stats = rest.get_index_stats()
            for index_path, index_stats in stats.items():
                for key, value in index_stats.items():
                    if '_system:' in index_path:
                        continue
                    index_name = key
                    items_count = value.get('items_count', 0)
                    if f"{index_path}.{index_name}" in index_counts_map:
                        index_counts_map[f"{index_path}.{index_name}"] += items_count
                    else:
                        index_counts_map[f"{index_path}.{index_name}"] = items_count

        # Convert map to list format
        index_counts = [
            {"name": name, "count": count}
            for name, count in index_counts_map.items()
        ]
        return index_counts

    def compare_item_counts_between_kv_and_gsi(self):
        kv_map = self.get_item_counts_from_kv()
        index_map = self.get_item_counts_from_index_stats()
        self.log.info(f"kv_map : {kv_map}")
        self.log.info(f"index_map : {index_map}")
        error_obj = []
        for kv_dict in kv_map:
            index_name_in_kv_dict, index_count_in_kv_dict = kv_dict['name'], kv_dict['count']
            for index_dict in index_map:
                if index_name_in_kv_dict == index_dict ['name']:
                    _, index_count_in_index_dict = index_dict['name'], index_dict['count']
                    break
            if index_count_in_index_dict != index_count_in_kv_dict:
                error = {}
                self.log.error(f"Counts are index map {index_count_in_index_dict} kv map {index_count_in_kv_dict} for index {index_name_in_kv_dict}")
                error['error'] = f"Counts for index {index_name_in_kv_dict} don't match. index map count {index_count_in_index_dict} kv map count {index_count_in_kv_dict}"
                error_obj.append(error)
        if error_obj:
            raise Exception(f"Counts don't match for the following indexes: {error_obj}")

    def get_all_array_index_names(self):
        """Returns a list of all array indexes in the cluster
        """
        array_indexes = []
        index_metadata = self.index_rest.get_indexer_metadata()['status']
        for index in index_metadata:
            # Check if index definition contains array indexing syntax like ALL or DISTINCT
            if 'definition' in index and ('ALL' in index['definition'] or 'DISTINCT' in index['definition']):
                array_indexes.append(index['indexName'])
        return array_indexes

    def get_storage_stats_map(self, node):
        """
        Fetches index storage stats from /stats/storage endpoint
        Args:
            node: Node to fetch stats from
        Returns:
            List of dictionaries containing storage stats for each index
        """
        rest = RestConnection(node)
        api = f"/api/v1/stats/storage"
        status, content, _ = rest.http_request(api)
        if not status:
            raise Exception(f"Error getting storage stats: {content}")
        storage_stats = json.loads(content)
        index_stats = []
        # Process and format the storage stats
        for index_path, stats in storage_stats.items():
            if '_system:' in index_path:
                continue
            for index_name, index_stats in stats.items():
                stat_obj = {
                    "name": f"{index_path}:{index_name}",
                    "mainstore_count": index_stats.get("MainStore", {}).get("items_count", 0),
                    "backstore_count": index_stats.get("BackStore", {}).get("items_count", 0)
                }
                index_stats.append(stat_obj)
        return index_stats

    def backstore_mainstore_check(self):
        # Get all index nodes in the cluster
        idx_node_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        # Exclude all array indexes
        ignore_count_index_list = self.get_all_array_index_names()
        errors = []
        for node in idx_node_list:
            self.log.info(f"Checking stats for node {node}")
            indexer_rest = RestConnection(node)
            content = indexer_rest.get_index_storage_stats()
            for index in list(content.values()):
                for index_name, stats in index.items():
                    self.log.info(f"checking for index {index_name}")
                    if index_name in ignore_count_index_list or "BackStore" not in stats:
                        continue
                    if stats["MainStore"]["item_count"] != stats["BackStore"]["item_count"]:
                        self.log.info(f"Index map as seen during backstore_mainstore_check is {stats}")
                        self.log.error(f"Item count mismatch in backstore and mainstore for {index_name}")
                        errors_obj = dict()
                        errors_obj["type"] = "mismatch in backstore and mainstore"
                        errors_obj["index_name"] = index_name
                        errors_obj["mainstore_count"] = stats["MainStore"]["item_count"]
                        errors_obj["backstore_count"] = stats["BackStore"]["item_count"]
                        errors.append(errors_obj)
        if len(errors) > 0:
            return errors
        else:
            self.log.info("backstore_mainstore_check passed. No discrepancies seen.")

    def check_storage_directory_cleaned_up(self, threshold_gb=0.025):
        """
        Checks if storage directory is cleaned up
        Args:
            threshold_gb (float): Maximum allowed storage space in GB (default: 0.025 GB or ~25MB)
        """
        idx_node_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for node in idx_node_list:
            rest = RestConnection(node)
            storage_dir = rest.get_indexer_internal_stats()['indexer.storage_dir']
            used_space = self.get_storage_directory_used_space(node, storage_dir)
            if used_space > threshold_gb:
                self.log.error(f"Storage directory {storage_dir} on {node} is not empty. Used space: {used_space} GB (threshold: {threshold_gb} GB)")
                raise Exception(f"Storage directory {storage_dir} on {node} is not empty. Used space: {used_space} GB (threshold: {threshold_gb} GB)")

    def get_storage_directory_used_space(self, node, storage_dir):
        """
        Fetches used space for storage directory
        Returns:
            Used space in GB
        """
        shell = RemoteMachineShellConnection(node)
        try:
            # Use du command to get specific directory size in MB
            output, error = shell.execute_command(f"du -s --block-size=1M {storage_dir} | cut -f1")
            if error or not output:
                self.log.error(f"Error getting disk space for {storage_dir}: {error}")
            used_space_mb = int(output[0])  # Size in MB
            used_space = used_space_mb / 1024.0  # Convert MB to GB
        except Exception as e:
            self.log.error(f"Error getting storage directory used space: {str(e)}")
            used_space = None
        finally:
            shell.disconnect()
        return used_space

    def drop_all_indexes(self):
        """
        Drops all indexes in the cluster except system indexes
        """
        self.log.info("Dropping all indexes...")
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        query = "SELECT bucket_id, scope_id, keyspace_id, name FROM system:indexes WHERE `using`='gsi'"
        result = self.n1ql_helper.run_cbq_query(query=query, server=query_node)
        for index in result['results']:
            try:
                bucket = index['bucket_id']
                scope = index.get('scope_id', '_default')
                collection = index.get('keyspace_id', '_default')
                index_name = index['name']
                if bucket == '_system':
                    continue
                drop_query = f"DROP INDEX `{index_name}` ON default:`{bucket}`.`{scope}`.`{collection}`"
                self.log.info(f"Executing: {drop_query}")
                self.n1ql_helper.run_cbq_query(query=drop_query, server=query_node)
            except Exception as e:
                self.log.error(f"Error dropping index {index_name}: {str(e)}")
        self.log.info("Finished dropping indexes")

    def validate_replica_indexes_item_counts(self):
        """
        Gets item counts for all replica indexes and validates counts match across replicas.
        Handles both regular and partitioned indexes.
        Returns:
            List of dictionaries containing any mismatches found between replica counts
        """
        errors = []
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(indexer_nodes[0])
        index_metadata = rest.get_indexer_metadata()['status']

        # Group indexes by definition ID to compare replicas
        defn_groups = {}
        for index in index_metadata:
            if '_system:' in index['bucket']:
                continue
            defn_id = index['defnId']
            if defn_id not in defn_groups:
                defn_groups[defn_id] = []
            defn_groups[defn_id].append(index)

        # Compare counts across replicas
        for defn_id, replicas in defn_groups.items():
            if len(replicas) <= 1:  # Skip non-replica indexes
                continue

            # Get counts for each replica
            replica_counts = {}
            for replica in replicas:
                index_name = replica['name']
                bucket = replica['bucket']
                scope = replica.get('scope', '_default')
                collection = replica.get('collection', '_default')
                replica_id = replica['replicaId']
                is_partitioned = 'partitionId' in replica

                # Initialize count for this replica
                if replica_id not in replica_counts:
                    replica_counts[replica_id] = {
                        'name': index_name,
                        'count': 0,
                        'nodes': set(),
                        'is_partitioned': is_partitioned
                    }

                # Get stats from all nodes for partitioned indexes, or hosting node for non-partitioned
                for node in indexer_nodes:
                    rest = RestConnection(node)
                    stats = rest.get_index_stats()
                    index_key = f"default:{bucket}.{scope}.{collection}"

                    if index_key in stats:
                        # For partitioned indexes, need to look for partition suffixes
                        matching_indexes = []
                        for stat_index in stats[index_key]:
                            if is_partitioned:
                                # Match base name for partitioned indexes
                                if stat_index.startswith(index_name + " "):
                                    matching_indexes.append(stat_index)
                            elif stat_index == index_name:
                                matching_indexes.append(stat_index)

                        # Sum up counts across all partitions/replicas on this node
                        for matching_index in matching_indexes:
                            if matching_index in stats[index_key]:
                                count = stats[index_key][matching_index].get('items_count', 0)
                                replica_counts[replica_id]['count'] += count
                                replica_counts[replica_id]['nodes'].add(node.ip)

            # Compare counts between replicas
            if len(replica_counts) > 1:
                base_count = None
                for replica_id, info in replica_counts.items():
                    if base_count is None:
                        base_count = info['count']
                    elif info['count'] != base_count:
                        error = {
                            'definition_id': defn_id,
                            'index_name': replicas[0]['name'],  # Base index name
                            'is_partitioned': info['is_partitioned'],
                            'mismatches': replica_counts,
                            'error': f"Replica counts don't match for index {replicas[0]['name']}"
                        }
                        self.log.error(f"Count mismatch found for index {replicas[0]['name']}: {replica_counts}")
                        errors.append(error)
                        break

                # Log the successful comparison
                self.log.info(f"Index {replicas[0]['name']} counts: {replica_counts}")
        return errors

    def validate_no_pending_mutations(self, timeout=1800):
        """
        Validates that there are no pending mutations by checking num_docs_pending stats.
        Retries for specified timeout period.

        Args:
            timeout (int): Maximum time in seconds to wait for pending mutations to clear

        Returns:
            bool: True if no pending mutations found, False otherwise
        """
        end_time = time.time() + timeout
        while time.time() < end_time:
            index_stats = self.index_rest.get_index_stats()
            pending_mutations = []

            for bucket, indexes in index_stats.items():
                for index, stats in indexes.items():
                    for key, val in stats.items():
                        if 'num_docs_pending' in key and val > 0:
                            pending_mutations.append(f"{bucket}.{index}.{key}:{val}")

            if not pending_mutations:
                return True

            self.log.info(f"Docs still pending: {pending_mutations}")
            time.sleep(60)

        self.log.error(f"Mutations still pending after {timeout} seconds")
        return False

    def validate_memory_released(self, timeout=300, threshold_ratio=0.1):
        """
        Validates that memory has been properly freed up after index operations
        across all index nodes.

        Args:
            timeout (int): Maximum time in seconds to wait for memory to be released
            threshold_ratio (float): Maximum acceptable ratio of memory_rss/memory_total
                                (e.g., 0.1 means RSS should be below 10% of total)
        Returns:
            bool: True if memory has been properly released on all nodes, False otherwise
        """
        end_time = time.time() + timeout
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        while time.time() < end_time:
            high_memory_nodes = []

            for node in indexer_nodes:
                rest = RestConnection(node)
                index_stats = rest.get_all_index_stats()

                # Get memory stats for this node
                memory_rss = index_stats.get('memory_rss', 0)
                memory_total = index_stats.get('memory_total', 0)

                if memory_total > 0:  # Avoid division by zero
                    ratio = memory_rss / memory_total
                    if ratio > threshold_ratio:
                        high_memory_nodes.append((node.ip, ratio * 100))

            if not high_memory_nodes:
                return True

            self.log.info(f"Memory still high on nodes: {', '.join([f'{ip}:{ratio:.1f}%' for ip, ratio in high_memory_nodes])}")
            time.sleep(5)

        self.log.error(f"Memory not properly released on nodes {high_memory_nodes} after {timeout} seconds")
        return False

    def validate_cpu_normalized(self, timeout=300, threshold_ratio=0.1):
        """
        Validates that CPU usage has returned to normal levels after index operations
        across all index nodes.

        Args:
            timeout (int): Maximum time in seconds to wait for CPU to normalize
            threshold_percent (float): Maximum acceptable CPU utilization percentage
                                    (e.g., 10 means CPU should be below 10%)
        Returns:
            bool: True if CPU usage is below threshold on all nodes, False otherwise
        """
        end_time = time.time() + timeout
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        while time.time() < end_time:
            high_cpu_nodes = []

            for node in indexer_nodes:
                rest = RestConnection(node)
                index_stats = rest.get_all_index_stats()

                # Get CPU stats for this node
                cpu_utilization = index_stats.get('cpu_utilization', 0)
                num_cpu_core = index_stats.get('num_cpu_core', 0)
                if num_cpu_core > 0:
                    utilization_ratio = cpu_utilization / (num_cpu_core * 100)

                if utilization_ratio > threshold_ratio:
                    high_cpu_nodes.append((node.ip, cpu_utilization))

            if not high_cpu_nodes:
                return True

            self.log.info(f"CPU still high on nodes: {', '.join([f'{ip}:{cpu:.1f}%' for ip, cpu in high_cpu_nodes])}")
            time.sleep(5)

        self.log.error(f"CPU usage remained high on nodes {high_cpu_nodes} after {timeout} seconds")
        return False

class ConCurIndexOps():
    def __init__(self):
        self.all_indexes_state = {"created": [], "deleted": [], "defer_build": []}
        self.errors = []
        self.test_fail = False
        self._lock_queue = threading.Lock()
        self.errors_lock_queue = threading.Lock()
        self.create_index_lock = threading.Lock()
        self.ignore_failure = False
        self.stop_create_index = False

    def update_errors(self, error_map):
        with self.errors_lock_queue:
            if not self.ignore_failure:
                self.errors.append(error_map)

    def update_ignore_failure_flag(self, flag):
        with self.errors_lock_queue:
            self.ignore_failure = flag

    def get_ignore_failure_flag(self, flag):
        with self.errors_lock_queue:
            return self.ignore_failure

    def update_stop_create_index(self, flag):
        with self.create_index_lock:
            self.stop_create_index = flag

    def get_stop_create_index(self):
        with self.create_index_lock:
            return self.stop_create_index

    def get_errors(self):
        return self.errors

    def get_delete_index_list(self):
        return self.all_indexes_state["deleted"]

    def get_create_index_list(self):
        return self.all_indexes_state["created"]

    def get_defer_index_list(self):
        return self.all_indexes_state["defer_build"]

    def all_indexes_metadata(self, index_meta=None, operation="create", defer_build=False):
        with self._lock_queue:
            if operation == "create" and defer_build:
                self.all_indexes_state["defer_build"].append(index_meta)
            elif operation == "create":
                self.all_indexes_state["created"].append(index_meta)
            elif operation == "delete" and defer_build:
                try:
                    index = self.all_indexes_state["defer_build"]. \
                        pop(random.randrange(len(self.all_indexes_state["defer_build"])))
                    self.all_indexes_state["deleted"].append(index)
                except Exception as e:
                    index = None
                return index
            elif operation == "delete":
                try:
                    index = self.all_indexes_state["created"]. \
                        pop(random.randrange(len(self.all_indexes_state["created"])))
                    self.all_indexes_state["deleted"].append(index)
                except Exception as e:
                    index = None
                return index
            elif operation == "scan":
                try:
                    index = random.choice(self.all_indexes_state["created"])
                except Exception as e:
                    index = None
                return index
            elif operation == "build":
                try:
                    index = self.all_indexes_state["defer_build"]. \
                        pop(random.randrange(len(self.all_indexes_state["defer_build"])))
                except Exception as e:
                    index = None
                return index

    def build_complete_add_to_create(self, index):
        with self._lock_queue:
            if index is not None:
                self.all_indexes_state["created"].append(index)

    def check_if_indexes_created(self, index_node, defer_build=False):
        indexes_not_created = []
        if defer_build:
            index_list = self.all_indexes_state["defer_build"]
        else:
            index_list = self.all_indexes_state["created"]
        for index in index_list:
            index_created, status = self.check_if_index_created(index_node, index["name"], defer_build)
            if not index_created:
                indexes_not_created.append({"name": index["name"], "status": status})
        return indexes_not_created

    @staticmethod
    def check_if_index_created(index_node, index, defer_build=False):
        index_created = False
        status = None
        rest = RestConnection(index_node)
        index_status = rest.get_index_status()
        for index_info in index_status.values():
            for k, v in index_info.items():
                if k == index:
                    status = v["status"]
                    if status == "Ready" or status == "Building" or (defer_build and status == "Created"):
                        index_created = True

        return index_created, status
