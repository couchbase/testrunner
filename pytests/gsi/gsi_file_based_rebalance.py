import math
import random
import time
import os
from sentence_transformers import SentenceTransformer
from membase.api.rest_client import RestConnection, RestHelper
from concurrent.futures import ThreadPoolExecutor
from couchbase_helper.documentgenerator import SDKDataLoader
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.query_tests_helper import QueryHelperTests
from .base_gsi import BaseSecondaryIndexingTests, log

from threading import Event
from deepdiff import DeepDiff


class FileBasedRebalance(BaseSecondaryIndexingTests, QueryHelperTests):
    def setUp(self):
        super().setUp()
        self.rest = RestConnection(self.servers[0])
        self.n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
        self.create_primary_index = False
        self.retry_time = self.input.param("retry_time", 300)
        self.sleep_time = self.input.param("sleep_time", 1)
        self.num_retries = self.input.param("num_retries", 1)
        self.build_index = self.input.param("build_index", False)
        self.rebalance_all_at_once = self.input.param("rebalance_all_at_once", False)
        self.continuous_mutations = self.input.param("continuous_mutations", False)
        self.replica_repair = self.input.param("replica_repair", False)
        self.chaos_action = self.input.param("chaos_action", None)
        self.topology_change = self.input.param("topology_change", True)
        self.capella_rebalance = self.input.param("capella_rebalance", "None")
        self.stress_factor = self.input.param("stress_factor", 0.5)
        self.disable_shard_affinity = self.input.param("disable_shard_affinity", False)
        self.index_resident_ratio = int(self.input.param("index_resident_ratio", 50))
        self.disk_full = self.input.param("disk_full", False)
        self.rand = random.randint(1, 1000000000)
        self.alter_index = self.input.param("alter_index", None)
        self.use_nodes_clause = self.input.param("use_nodes_clause", False)
        self.skip_default = self.input.param("skip_default", True)
        self.encoder = SentenceTransformer(self.data_model, device="cpu")
        self.encoder.cpu()
        self.gsi_util_obj.set_encoder(encoder=self.encoder)
        if self.ansi_join:
            self.rest.load_sample("travel-sample")
            self.sleep(10)
        self.enable_shard_based_rebalance()
        self.NUM_DOCS_POST_REBALANCE = 10 ** 5

    def tearDown(self):
        if self._testMethodName not in ['suite_tearDown', 'suite_setUp']:
            super(FileBasedRebalance, self).tearDown()

    def test_gsi_rebalance_out_indexer_node(self):
        self.install_tools()
        if self.bhive_index:
            self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        else:
            self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                                bucket_params=self.bucket_params)
            self.buckets = self.rest.get_buckets()
            self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=True)
            time.sleep(10)
        skip_array_index_item_count, scan_results_check = False, True
        with ThreadPoolExecutor() as executor_main:
            try:
                event = Event()
                if self.continuous_mutations:
                    future = executor_main.submit(self.perform_continuous_kv_mutations, event)
                    skip_array_index_item_count = True
                    scan_results_check = False
                select_queries = set()
                query_node = self.get_nodes_from_services_map(service_type="n1ql")
                index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                create_queries = []
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    deploy_nodes = None
                    if self.use_nodes_clause:
                        nodes_list = random.sample(index_nodes, k=replica_count + 1)
                        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in nodes_list]
                    for namespace in self.namespaces:
                        query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                                  prefix='test_',
                                                                                  similarity=self.similarity, train_list=None,
                                                                                  scan_nprobes=self.scan_nprobes,
                                                                                  array_indexes=False,
                                                                                  limit=self.scan_limit, is_base64=self.base64,
                                                                                  quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                                  quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                                  bhive_index=self.bhive_index)
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace, limit=self.scan_limit))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True,
                                                                          deploy_node_info=deploy_nodes,
                                                                          bhive_index=self.bhive_index)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                             query_node=query_node)
                        create_queries.extend(queries)

                self.wait_until_indexes_online()
                if self.use_nodes_clause:
                    self.validate_node_placement_with_nodes_clause(create_queries=create_queries)
                self.validate_shard_affinity()
                nodes_out = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                nodes_out_list = nodes_out[:2]
                if self.rebalance_all_at_once:
                    # rebalance out all nodes at once
                    self.rebalance_and_validate(nodes_out_list=nodes_out_list,
                                                swap_rebalance=False,
                                                skip_array_index_item_count=skip_array_index_item_count,
                                                select_queries=select_queries,
                                                scan_results_check=scan_results_check)
                else:
                    # rebalance out 1 node after another
                    for count, node in enumerate(nodes_out_list):
                        self.log.info(f"Running rebalance number {count}")
                        self.rebalance_and_validate(nodes_out_list=[node], swap_rebalance=False,
                                                    skip_array_index_item_count=skip_array_index_item_count,
                                                    select_queries=select_queries,
                                                    scan_results_check=scan_results_check)
                map_after_rebalance, stats_map_after_rebalance = self._return_maps()
                self.run_post_rebalance_operations(map_after_rebalance=map_after_rebalance,
                                                   stats_map_after_rebalance=stats_map_after_rebalance)
            finally:
                event.set()
                if self.continuous_mutations:
                    future.result()

    def test_gsi_rebalance_in_indexer_node(self):
        self.install_tools()
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for indexer_node in indexer_nodes:
            rest = RestConnection(indexer_node)
            rest.set_index_settings({"indexer.settings.rebalance.redistribute_indexes": True})
        if self.bhive_index:
            self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        else:
            self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                                bucket_params=self.bucket_params)
            self.buckets = self.rest.get_buckets()
            self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=True)
            time.sleep(10)
        skip_array_index_item_count, scan_results_check = False, True
        with ThreadPoolExecutor() as executor_main:
            try:
                event = Event()
                if self.continuous_mutations:
                    future = executor_main.submit(self.perform_continuous_kv_mutations, event)
                    skip_array_index_item_count = True
                    scan_results_check = False
                select_queries = set()
                query_node = self.get_nodes_from_services_map(service_type="n1ql")
                index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                create_queries = []
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    deploy_nodes = None
                    if self.use_nodes_clause:
                        nodes_list = random.sample(index_nodes, k=replica_count + 1)
                        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in nodes_list]
                    for namespace in self.namespaces:
                        query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                                  prefix='test_',
                                                                                  similarity=self.similarity, train_list=None,
                                                                                  scan_nprobes=self.scan_nprobes,
                                                                                  array_indexes=False,
                                                                                  limit=self.scan_limit, is_base64=self.base64,
                                                                                  quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                                  quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                                  bhive_index=self.bhive_index)
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace, limit=self.scan_limit))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True,
                                                                          deploy_node_info=deploy_nodes,
                                                                          bhive_index=self.bhive_index)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                             query_node=query_node)
                        create_queries.extend(queries)

                self.wait_until_indexes_online()
                if self.use_nodes_clause:
                    self.validate_node_placement_with_nodes_clause(create_queries=create_queries)
                self.validate_shard_affinity()
                nodes_in_list = self.servers[self.nodes_init:]
                if self.topology_change:
                    if self.rebalance_all_at_once:
                        # rebalance all nodes at once
                        services_in = ["index"] * len(nodes_in_list)
                        self.rebalance_and_validate(nodes_out_list=[],
                                                    nodes_in_list=nodes_in_list,
                                                    swap_rebalance=False,
                                                    skip_array_index_item_count=skip_array_index_item_count,
                                                    services_in=services_in,
                                                    select_queries=select_queries,
                                                    scan_results_check=scan_results_check
                                                    )
                    else:
                        services_in = ["index"]
                        # rebalance in 1 node after another
                        for count, node in enumerate(nodes_in_list):
                            self.log.info(f"Running rebalance number {count}")
                            self.rebalance_and_validate(nodes_out_list=[], nodes_in_list=[node],
                                                        swap_rebalance=False,
                                                        skip_array_index_item_count=skip_array_index_item_count,
                                                        services_in=services_in,
                                                        select_queries=select_queries,
                                                        scan_results_check=scan_results_check
                                                        )
                else:
                    self.rebalance_and_validate(nodes_out_list=[], nodes_in_list=[],
                                                swap_rebalance=False,
                                                skip_array_index_item_count=skip_array_index_item_count,
                                                select_queries=select_queries,
                                                scan_results_check=scan_results_check
                                                )
                map_after_rebalance, stats_map_after_rebalance = self._return_maps()
                self.run_post_rebalance_operations(map_after_rebalance=map_after_rebalance,
                                                   stats_map_after_rebalance=stats_map_after_rebalance)
            finally:
                event.set()
                if self.continuous_mutations:
                    future.result()

    def test_gsi_swap_rebalance(self):
        self.install_tools()
        if self.bhive_index:
            self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        else:
            self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                                bucket_params=self.bucket_params)
            self.buckets = self.rest.get_buckets()
            self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=True)
            time.sleep(10)
        skip_array_index_item_count, scan_results_check = False, True
        with ThreadPoolExecutor() as executor_main:
            try:
                event = Event()
                if self.continuous_mutations:
                    future = executor_main.submit(self.perform_continuous_kv_mutations, event)
                    skip_array_index_item_count = True
                    scan_results_check = False
                select_queries = set()
                create_queries = []
                query_node = self.get_nodes_from_services_map(service_type="n1ql")
                index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    deploy_nodes = None
                    if self.use_nodes_clause:
                        nodes_list = random.sample(index_nodes, k=replica_count + 1)
                        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in nodes_list]
                    for namespace in self.namespaces:
                        query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                                  prefix='test_',
                                                                                  similarity=self.similarity, train_list=None,
                                                                                  scan_nprobes=self.scan_nprobes,
                                                                                  array_indexes=False,
                                                                                  limit=self.scan_limit, is_base64=self.base64,
                                                                                  quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                                  quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                                  bhive_index=self.bhive_index)
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace, limit=self.scan_limit))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True,
                                                                          deploy_node_info=deploy_nodes,
                                                                          bhive_index=self.bhive_index)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                             query_node=query_node)
                        create_queries.extend(queries)
                self.wait_until_indexes_online()
                if self.use_nodes_clause:
                    self.validate_node_placement_with_nodes_clause(create_queries=create_queries)
                self.validate_shard_affinity()
                nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                to_remove_nodes = nodes_out_list[:2]
                to_add_nodes = self.servers[self.nodes_init:]
                if self.rebalance_all_at_once:
                    # rebalance out all nodes at once
                    services_in = ["index"] * len(to_add_nodes)
                    self.rebalance_and_validate(nodes_out_list=to_remove_nodes,
                                                nodes_in_list=to_add_nodes,
                                                swap_rebalance=True,
                                                skip_array_index_item_count=skip_array_index_item_count,
                                                services_in=services_in, select_queries=select_queries,
                                                scan_results_check=scan_results_check)
                else:
                    # rebalance out 1 node after another
                    services_in = ["index"]
                    for i in range(len(to_add_nodes)):
                        self.log.info(f"Running rebalance number {i}")
                        self.rebalance_and_validate(nodes_out_list=[to_remove_nodes[i]],
                                                    nodes_in_list=[to_add_nodes[i]],
                                                    swap_rebalance=True,
                                                    skip_array_index_item_count=skip_array_index_item_count,
                                                    services_in=services_in, select_queries=select_queries,
                                                    scan_results_check=scan_results_check)
                map_after_rebalance, stats_map_after_rebalance = self._return_maps()
                self.run_post_rebalance_operations(map_after_rebalance=map_after_rebalance,
                                                   stats_map_after_rebalance=stats_map_after_rebalance)
            finally:
                event.set()
                if self.continuous_mutations:
                    future.result()

    def test_gsi_failover_indexer_node(self):
        self.install_tools()
        if self.bhive_index:
            self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        else:
            self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                                bucket_params=self.bucket_params)
            self.buckets = self.rest.get_buckets()
            self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=True)
            time.sleep(10)
        skip_array_index_item_count, scan_results_check = False, True
        with ThreadPoolExecutor() as executor_main:
            try:
                event = Event()
                if self.continuous_mutations:
                    future = executor_main.submit(self.perform_continuous_kv_mutations, event)
                    skip_array_index_item_count = True
                    scan_results_check = False
                select_queries = set()
                create_queries = []
                query_node = self.get_nodes_from_services_map(service_type="n1ql")
                index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    deploy_nodes = None
                    if self.use_nodes_clause:
                        nodes_list = random.sample(index_nodes, k=replica_count + 1)
                        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in nodes_list]
                    for namespace in self.namespaces:
                        query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                                  prefix='test_',
                                                                                  similarity=self.similarity, train_list=None,
                                                                                  scan_nprobes=self.scan_nprobes,
                                                                                  array_indexes=False,
                                                                                  limit=self.scan_limit, is_base64=self.base64,
                                                                                  quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                                  quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                                  bhive_index=self.bhive_index)
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace, limit=self.scan_limit))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True,
                                                                          deploy_node_info=deploy_nodes,
                                                                          bhive_index=self.bhive_index)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                             query_node=query_node)
                        create_queries.extend(queries)
                self.wait_until_indexes_online()
                if self.use_nodes_clause:
                    self.validate_node_placement_with_nodes_clause(create_queries=create_queries)
                self.validate_shard_affinity()
                nodes_out = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                nodes_out_list = nodes_out[:2]
                if self.rebalance_all_at_once:
                    # failover all nodes at once
                    self.rebalance_and_validate(failover_nodes_list=nodes_out_list,
                                                swap_rebalance=False,
                                                skip_array_index_item_count=skip_array_index_item_count,
                                                select_queries=select_queries,
                                                scan_results_check=scan_results_check
                                                )
                else:
                    # failover 1 node after another
                    for count, node in enumerate(nodes_out_list):
                        self.log.info(f"Running rebalance number {count}")
                        self.rebalance_and_validate(failover_nodes_list=[node], swap_rebalance=False,
                                                    skip_array_index_item_count=skip_array_index_item_count,
                                                    select_queries=select_queries,
                                                    scan_results_check=scan_results_check
                                                    )
                map_after_rebalance, stats_map_after_rebalance = self._return_maps()
                self.run_post_rebalance_operations(map_after_rebalance=map_after_rebalance,
                                                   stats_map_after_rebalance=stats_map_after_rebalance)
            finally:
                event.set()
                if self.continuous_mutations:
                    future.result()

    def test_gsi_rebalance_toggle_flag(self):
        self.install_tools()
        self.disable_shard_based_rebalance()
        if self.bhive_index:
            self.restore_couchbase_bucket(backup_filename=self.vector_backup_filename, skip_default_scope=self.skip_default)
        else:
            self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                                bucket_params=self.bucket_params)
            self.buckets = self.rest.get_buckets()
            self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, load_default_coll=True)
            time.sleep(10)
        skip_array_index_item_count, scan_results_check = False, True
        with ThreadPoolExecutor() as executor_main:
            try:
                event = Event()
                if self.continuous_mutations:
                    future = executor_main.submit(self.perform_continuous_kv_mutations, event)
                    skip_array_index_item_count = True
                    scan_results_check = False
                select_queries = set()
                create_queries = []
                query_node = self.get_nodes_from_services_map(service_type="n1ql")
                index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    deploy_nodes = None
                    if self.use_nodes_clause:
                        nodes_list = random.sample(index_nodes, k=replica_count + 1)
                        deploy_nodes = [f"{node.ip}:{node.port}" for node in nodes_list]
                    for namespace in self.namespaces:
                        query_definitions = self.gsi_util_obj.get_index_definition_list(dataset=self.json_template,
                                                                                  prefix='test_',
                                                                                  similarity=self.similarity, train_list=None,
                                                                                  scan_nprobes=self.scan_nprobes,
                                                                                  array_indexes=False,
                                                                                  limit=self.scan_limit, is_base64=self.base64,
                                                                                  quantization_algo_color_vector=self.quantization_algo_color_vector,
                                                                                  quantization_algo_description_vector=self.quantization_algo_description_vector,
                                                                                  bhive_index=self.bhive_index)
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace, limit=self.scan_limit))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True,
                                                                          deploy_node_info=deploy_nodes,
                                                                          bhive_index=self.bhive_index)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                             query_node=query_node)
                        create_queries.extend(queries)
                self.wait_until_indexes_online()
                if self.use_nodes_clause:
                    self.validate_node_placement_with_nodes_clause(create_queries=create_queries)
                nodes_out = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                nodes_out_list = nodes_out[:2]
                if self.rebalance_all_at_once:
                    # rebalance out all nodes at once
                    self.rebalance_and_validate(nodes_out_list=nodes_out_list,
                                                swap_rebalance=False,
                                                skip_array_index_item_count=skip_array_index_item_count,
                                                select_queries=select_queries,
                                                scan_results_check=scan_results_check)
                    # TODO uncomment after https://issues.couchbase.com/browse/MB-58942 fix
                    # shard_id_list = self.fetch_shard_id_list()
                    # if shard_id_list:
                    #     raise AssertionError("Alternate shard IDs generated for
                    #     indexes despite the flag being set to False")
                else:
                    # rebalance out 1 node after another
                    for count, node in enumerate(nodes_out_list):
                        self.log.info(f"Running rebalance number {count}")
                        self.rebalance_and_validate(nodes_out_list=[node], swap_rebalance=False,
                                                    skip_array_index_item_count=skip_array_index_item_count,
                                                    select_queries=select_queries,
                                                    scan_results_check=scan_results_check)
                        # TODO uncomment after https://issues.couchbase.com/browse/MB-58942 fix
                        # shard_id_list = self.fetch_shard_id_list()
                        # if shard_id_list:
                        #     raise AssertionError("Alternate shard IDs generated for
                        #     indexes despite the flag being set to False")
                self.enable_shard_based_rebalance()
                self.enable_redistribute_indexes()
                nodes_in_list = nodes_out_list
                if self.rebalance_all_at_once:
                    # rebalance all nodes at once
                    services_in = ["index"] * len(nodes_in_list)
                    self.rebalance_and_validate(nodes_out_list=[],
                                                nodes_in_list=nodes_in_list,
                                                swap_rebalance=False,
                                                skip_array_index_item_count=skip_array_index_item_count,
                                                services_in=services_in,
                                                select_queries=select_queries,
                                                scan_results_check=scan_results_check,
                                                skip_shard_validations=True
                                                )
                else:
                    services_in = ["index"]
                    # rebalance in 1 node after another
                    for count, node in enumerate(nodes_in_list):
                        self.log.info(f"Running rebalance number {count}")
                        self.rebalance_and_validate(nodes_out_list=[], nodes_in_list=[node],
                                                    swap_rebalance=False,
                                                    skip_array_index_item_count=skip_array_index_item_count,
                                                    services_in=services_in,
                                                    select_queries=select_queries,
                                                    scan_results_check=scan_results_check,
                                                    skip_shard_validations=True
                                                    )
            finally:
                event.set()
                if self.continuous_mutations:
                    future.result()

    def test_gsi_rebalance_indexer_node_capella(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template,
                                             load_default_coll=True)
        time.sleep(10)
        skip_array_index_item_count, scan_results_check = False, True
        with ThreadPoolExecutor() as executor_main:
            try:
                event = Event()
                if self.continuous_mutations:
                    future = executor_main.submit(self.perform_continuous_kv_mutations, event)
                    skip_array_index_item_count = True
                    scan_results_check = False
                select_queries = set()
                query_node = self.get_nodes_from_services_map(service_type="n1ql")
                index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                create_queries = []
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    deploy_nodes = None
                    if self.use_nodes_clause:
                        nodes_list = random.sample(index_nodes, k=replica_count + 1)
                        deploy_nodes = [f"{node.ip}:{self.node_port}" for node in nodes_list]
                    for namespace in self.namespaces:
                        query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          deploy_node_info=deploy_nodes,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                             query_node=query_node)
                        create_queries.extend(queries)

                self.wait_until_indexes_online()
                if self.use_nodes_clause:
                    self.validate_node_placement_with_nodes_clause(create_queries=create_queries)
                self.validate_shard_affinity()
                nodes_out = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                nodes_out_list = nodes_out[:2]
                if self.rebalance_all_at_once:
                    # rebalance out all nodes at once
                    self.rebalance_and_validate(nodes_out_list=nodes_out_list,
                                                swap_rebalance=False,
                                                skip_array_index_item_count=skip_array_index_item_count,
                                                select_queries=select_queries,
                                                scan_results_check=scan_results_check,
                                                capella_rebalance=self.capella_rebalance)
                else:
                    # rebalance out 1 node after another
                    for count, node in enumerate(nodes_out_list):
                        self.log.info(f"Running rebalance number {count}")
                        self.rebalance_and_validate(nodes_out_list=[node], swap_rebalance=False,
                                                    skip_array_index_item_count=skip_array_index_item_count,
                                                    select_queries=select_queries,
                                                    scan_results_check=scan_results_check,
                                                    capella_rebalance=self.capella_rebalance)
                map_after_rebalance, stats_map_after_rebalance = self._return_maps()
                self.run_post_rebalance_operations(map_after_rebalance=map_after_rebalance,
                                                   stats_map_after_rebalance=stats_map_after_rebalance)
            except Exception as e:
                raise Exception(e)
            finally:
                event.set()
                if self.continuous_mutations:
                    future.result()

    def test_plasma_shard_size_limit(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template, load_default_coll=True)
        self.set_flush_buffer_quota(0.5)
        # 100 MB
        self.set_maximum_disk_usage_per_shard(109715200)
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        allowed_num_shards = self.fetch_total_shards_limit() * len(indexer_nodes)
        time.sleep(10)
        select_queries = set()
        create_queries = []
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for _ in range(3):
            replica_count = 0
            deploy_nodes = None
            if self.use_nodes_clause:
                nodes_list = random.sample(index_nodes, k=replica_count + 1)
                deploy_nodes = [f"{node.ip}:{node.port}" for node in nodes_list]
            for namespace in self.namespaces:
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                           namespace=namespace))
                queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                  namespace=namespace, deploy_node_info=deploy_nodes)
                self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                     query_node=query_node)
                create_queries.extend(queries)
                shards_list = self.fetch_plasma_shards_list()
                if len(shards_list) > allowed_num_shards:
                    raise Exception(
                        f"The total number of shards {shards_list} has exceeded the allowed limit {allowed_num_shards}")
        self.wait_until_indexes_online()
        if self.use_nodes_clause:
            self.validate_node_placement_with_nodes_clause(create_queries=create_queries)
        self.validate_shard_affinity()
        if self.disable_shard_affinity:
            shards_list_before = self.fetch_plasma_shards_list()
            self.disable_shard_based_rebalance()
            for namespace in self.namespaces:
                queries = [f'create index idx on {namespace}(name)']
                self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
                shards_list_after = self.fetch_plasma_shards_list()
                if sorted(shards_list_before) == sorted(shards_list_after):
                    raise Exception(
                        "Indexes with alternate shard ID share shards with indexes with no alternate shard ID")

    def test_recovery_from_disk_snapshot(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        snapshot_interval = 180
        self.set_persisted_snapshot_interval(interval=snapshot_interval * 1000)
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template, load_default_coll=True)
        time.sleep(10)
        select_queries = set()
        create_queries = []
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for _ in range(self.initial_index_batches):
            replica_count = random.randint(1, 2)
            deploy_nodes = None
            if self.use_nodes_clause:
                nodes_list = random.sample(index_nodes, k=replica_count + 1)
                deploy_nodes = [f"{node.ip}:{node.port}" for node in nodes_list]
            for namespace in self.namespaces:
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                           namespace=namespace))
                queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                  namespace=namespace,
                                                                  num_replica=replica_count,
                                                                  randomise_replica_count=True,deploy_node_info=deploy_nodes)
                self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
                create_queries.extend(queries)
        self.wait_until_indexes_online()
        if self.use_nodes_clause:
            self.validate_node_placement_with_nodes_clause(create_queries=create_queries)
        self.validate_shard_affinity()
        time.sleep(60)
        to_add_nodes, to_remove_nodes, services_in = [], [], None
        if self.rebalance_type in ['rebalance_in', 'rebalance_swap']:
            to_add_nodes = self.servers[self.nodes_init:]
            services_in = ["index"] * len(to_add_nodes)
            if self.rebalance_type == 'rebalance_in':
                self.enable_redistribute_indexes()
        if self.rebalance_type in ['rebalance_out', 'rebalance_swap']:
            nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            to_remove_nodes = nodes_out_list[:2]
        # rebalance out all nodes at once
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add=to_add_nodes,
                                                 to_remove=to_remove_nodes, services=services_in,
                                                 cluster_config=self.cluster_config)
        time.sleep(3)
        status, _ = self.rest._rebalance_status_and_progress()
        while status != 'running':
            time.sleep(1)
            status, _ = self.rest._rebalance_status_and_progress()
        time.sleep(10)
        reached = RestHelper(self.rest).rebalance_reached()
        rebalance.result()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        for item in self.namespaces:
            s_item, c_item = item.split(":")[1].split(".")[1], item.split(":")[1].split(".")[2]
            self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection * 2, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=s_item,
                                            collection=c_item, json_template=self.json_template,
                                            output=True, username=self.username, password=self.password,
                                            start=self.num_of_docs_per_collection + 1,
                                            end=2 * self.num_of_docs_per_collection)
            tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=self.gen_create,
                                                            batch_size=self.num_of_docs_per_collection,
                                                            dataset=self.json_template)
            for task in tasks:
                task.result()
        time.sleep(snapshot_interval + 60)
        _, stats_map_before_restart = self._return_maps()
        snapshots_created_indexes = {}
        for host in stats_map_before_restart:
            for bucket in stats_map_before_restart[host]:
                for index in stats_map_before_restart[host][bucket]:
                    if stats_map_before_restart[host][bucket][index]["num_disk_snapshots"] >= 1:
                        snapshots_created_indexes[f'{bucket}.{index}'] = stats_map_before_restart[host][bucket][index]
        time.sleep(10)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for server in index_nodes:
            self._kill_all_processes_index(server)
        time.sleep(10)
        _, stats_map_after_restart = self._return_maps()
        for host in stats_map_after_restart:
            for bucket in stats_map_after_restart[host]:
                for index in stats_map_after_restart[host][bucket]:
                    if index in snapshots_created_indexes:
                        if stats_map_after_restart[host][bucket][index]['num_docs_indexed'] == 0 and \
                                stats_map_after_restart[host][bucket][index]['items_count'] != \
                                snapshots_created_indexes[f'{bucket}.{index}']['items_count']:
                            # TODO remove this and find a better way to find partitioned indexes
                            if "partitioned" not in index:
                                raise AssertionError(f"Mismatch in item count and num_docs_indexed for index {index}. "
                                                     f"Before restart {snapshots_created_indexes[index]['items_count']}"
                                                     f"After restart {stats_map_after_restart[host][bucket][index]['items_count']}")

    def test_gsi_rebalance_with_memory_cpu_stress(self):
        self.install_tools()
        nodes_dict = None
        with ThreadPoolExecutor() as executor_main:
            try:
                event, future = Event(), None
                self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                                replicas=self.num_replicas,
                                                                bucket_type=self.bucket_type,
                                                                enable_replica_index=self.enable_replica_index,
                                                                eviction_policy=self.eviction_policy, lww=self.lww)
                self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                                    bucket_params=self.bucket_params)
                self.buckets = self.rest.get_buckets()
                self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                     num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                     json_template=self.json_template, load_default_coll=True)
                time.sleep(10)
                select_queries = set()
                create_queries = []
                query_node = self.get_nodes_from_services_map(service_type="n1ql")
                index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                replica_count = random.randint(1, 2)
                deploy_nodes = None
                if self.use_nodes_clause:
                    nodes_list = random.sample(index_nodes, k=replica_count + 1)
                    deploy_nodes = [f"{node.ip}:{node.port}" for node in nodes_list]
                for namespace in self.namespaces:
                    query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                    select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                               namespace=namespace))
                    queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                      namespace=namespace,
                                                                      num_replica=replica_count,
                                                                      randomise_replica_count=True,deploy_node_info=deploy_nodes)
                    self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                         query_node=query_node)
                    create_queries.extend(queries)
                self.wait_until_indexes_online()
                time.sleep(30)
                if self.use_nodes_clause:
                    self.validate_node_placement_with_nodes_clause(create_queries=create_queries)
                self.validate_shard_affinity()
                to_add_nodes, to_remove_nodes, services_in = [], [], None
                if self.rebalance_type in ['rebalance_in', 'rebalance_swap']:
                    to_add_nodes = self.servers[self.nodes_init:]
                    services_in = ["index"] * len(to_add_nodes)
                    if self.rebalance_type == 'rebalance_in':
                        self.enable_redistribute_indexes()
                if self.rebalance_type in ['rebalance_out', 'rebalance_swap']:
                    nodes_out_list = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                    to_remove_nodes = nodes_out_list[:2]
                future_query = executor_main.submit(self.run_query_load, select_queries, event)
                index_resident_ratio = self.index_resident_ratio
                self.log.info(f"Target index RR is {index_resident_ratio}")
                time.sleep(120)
                self.load_until_index_dgm(resident_ratio=index_resident_ratio)
                if self.disk_full:
                    nodes_dict = self.fill_up_disk(disk_fill_percent=80)
                # commenting out temporarily to stabilise tests
                # executor_main.submit(self.perform_continuous_kv_mutations, event)
                executor_main.submit(self.run_stress_tool, self.stress_factor, 1800)
                # rebalance all nodes at once
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], to_add=to_add_nodes,
                                                         to_remove=to_remove_nodes, services=services_in,
                                                         cluster_config=self.cluster_config)
                time.sleep(3)
                status, _ = self.rest._rebalance_status_and_progress()
                while status != 'running':
                    time.sleep(1)
                    status, _ = self.rest._rebalance_status_and_progress()
                time.sleep(10)
                future_disk_usage = executor_main.submit(self.run_disk_usage_tool, event)
                reached = RestHelper(self.rest).rebalance_reached(retry_count=500)
                rebalance.result()
                self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            finally:
                event.set()
                if nodes_dict:
                    self.delete_dummy_files(nodes_dict)
                if future_query:
                    future_query.result()
                if future_disk_usage:
                    future_disk_usage.result()
                self.kill_stress_tool()

    def test_drop_duplicate_indexes_on_rebalance(self):
        """
        https://issues.couchbase.com/browse/MB-58379
        Create index idx1 on node n1. Failover n1. Create idx1 on node n2. Add back n1.
        Rebalance. Duplicate indexes are dropped.
        """
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template, load_default_coll=False)
        time.sleep(10)
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        namespace = self.namespaces[0]
        nodes = random.choice(indexer_nodes)
        if self.use_nodes_clause:
            queries = [
                f'create index idx on {namespace}(name) USING GSI WITH {{\'nodes\':[\'{nodes.ip}:{nodes.port}\'], \'defer_build\': False}}']
        else:
            queries = [f'create index idx on {namespace}(name)']
        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
        self.wait_until_indexes_online()
        if self.use_nodes_clause:
            self.validate_node_placement_with_nodes_clause(create_queries=queries)
        self.validate_shard_affinity()
        time.sleep(60)
        rest = RestConnection(indexer_nodes[0])
        index_resp = rest.get_indexer_metadata()
        if 'status' not in index_resp:
            raise Exception(f"No index metadata seen in response {index_resp}")
        index_map = index_resp['status']
        index_list, host_ip_to_failover, host_to_failover = [], None, None
        for index in index_map:
            index_list.append(index['name'])
            host_ip_to_failover = index['hosts'][0]
        self.log.info(f"Index list after rebalance {index_list}")
        host_ip_to_failover = host_ip_to_failover.split(":")[0]
        self.log.info(f"host_ip_to_failover {host_ip_to_failover}")
        for item in self.servers:
            if item.ip == host_ip_to_failover:
                host_to_failover = item
        failover_nodes_list = [host_to_failover]
        self.log.info(f"Running failover task for node {failover_nodes_list}")
        failover_task = self.cluster.async_failover([self.master], failover_nodes=failover_nodes_list,
                                                    graceful=False)
        failover_task.result()
        time.sleep(10)
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
        rest = RestConnection(self.master)
        node_obj = None
        nodes_all = rest.node_statuses()
        for node in nodes_all:
            if node.ip == host_ip_to_failover:
                node_obj = node
        self.log.info(f"Running add back task for node {node_obj.ip}")
        rest.add_back_node(node_obj.id)
        time.sleep(10)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [],
                                                 cluster_config=self.cluster_config)
        time.sleep(1)
        status, _ = self.rest._rebalance_status_and_progress()
        while status != 'running':
            time.sleep(1)
            status, _ = self.rest._rebalance_status_and_progress()
        time.sleep(10)
        reached = RestHelper(self.rest).rebalance_reached(retry_count=500)
        rebalance.result()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rest = RestConnection(indexer_nodes[0])
        time.sleep(5)
        index_resp = rest.get_indexer_metadata()
        if 'status' not in index_resp:
            raise Exception(f"No index metadata seen in response {index_resp}")
        index_map = index_resp['status']
        index_list_after, host_to_failover = [], None
        for index in index_map:
            index_list_after.append(index['name'])
        self.log.info(f"Index list after rebalance {index_list_after}")
        self.wait_until_indexes_online()
        self.validate_shard_affinity()
        if len(index_list_after) != 1:
            raise Exception("Duplicate indexes not dropped after rebalance")

    def test_partition_repair_on_rebalance(self):
        """
        https://issues.couchbase.com/browse/MB-57378
        When a node containing partitions is failed over, partition repair should take place
        """
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template, load_default_coll=False)
        time.sleep(10)
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        deploy_nodes = ",".join([f"\'{node.ip}:{node.port}\'" for node in indexer_nodes])
        namespace = self.namespaces[0]
        if self.use_nodes_clause:
            queries = [
                f'create index idx on {namespace}(name) PARTITION BY HASH (meta().id) WITH {{\'nodes\':[{deploy_nodes}], \'defer_build\': False}}']
        else:
            queries = [
                f'create index idx on {namespace}(name) PARTITION BY HASH (meta().id)']

        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
        self.wait_until_indexes_online()
        if self.use_nodes_clause:
            self.validate_node_placement_with_nodes_clause(create_queries=queries)
        self.validate_shard_affinity()
        time.sleep(60)
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        time.sleep(5)
        rest = RestConnection(indexer_nodes[0])
        index_resp = rest.get_indexer_metadata()
        if 'status' not in index_resp:
            raise Exception(f"No index metadata seen in response {index_resp}")
        index_map = index_resp['status']
        index_list, host_ip_to_failover, host_to_failover = [], None, None
        for index in index_map:
            index_list.append(index['name'])
            host_ip_to_failover = index['hosts'][0]
        self.log.info(f"Index list after rebalance {index_list}")
        host_ip_to_failover = host_ip_to_failover.split(":")[0]
        self.log.info(f"host_ip_to_failover {host_ip_to_failover}")
        for item in self.servers:
            if item.ip == host_ip_to_failover:
                host_to_failover = item
        failover_nodes_list = [host_to_failover]
        self.log.info(f"Running failover task for node {failover_nodes_list}")
        failover_task = self.cluster.async_failover([self.master], failover_nodes=failover_nodes_list,
                                                    graceful=False)
        failover_task.result()
        time.sleep(10)
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [], [],
                                                 cluster_config=self.cluster_config)
        time.sleep(1)
        status, _ = self.rest._rebalance_status_and_progress()
        while status != 'running':
            time.sleep(1)
            status, _ = self.rest._rebalance_status_and_progress()
        time.sleep(10)
        reached = RestHelper(self.rest).rebalance_reached()
        rebalance.result()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(indexer_nodes[0])
        time.sleep(5)
        index_resp = rest.get_indexer_metadata()
        if 'status' not in index_resp:
            raise Exception(f"No index metadata seen in response {index_resp}")
        index_map = index_resp['status']
        index_list_after, host_to_failover = [], None
        for index in index_map:
            index_list_after.append(index['name'])
        self.log.info(f"Index list after rebalance {index_list_after}")
        self.wait_until_indexes_online()
        if self.use_nodes_clause:
            self.validate_node_placement_with_nodes_clause(create_queries=queries)
        self.validate_shard_affinity()
        if len(index_list_after) != 1:
            raise Exception("Duplicate indexes not dropped after rebalance")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance=map_before_rebalance,
                                                      map_after_rebalance=map_after_rebalance,
                                                      stats_map_before_rebalance=stats_map_before_rebalance,
                                                      stats_map_after_rebalance=stats_map_after_rebalance,
                                                      nodes_in=[],
                                                      nodes_out=failover_nodes_list,
                                                      swap_rebalance=False,
                                                      use_https=False,
                                                      item_count_increase=False,
                                                      per_node=True,
                                                      skip_array_index_item_count=False,
                                                      indexes_changed=False)

    def test_non_symmetric_sibling_shard_token_generation(self):
        """
        https://issues.couchbase.com/browse/MB-58224
        Create idx1 with 2 replicas. Create idx2 with 2 replicas. Drop replica ID 0 of idx1.
        Failover node that has the dropped replica. Add a new node and swap rebalance a new node.
        Check that replica repair takes place.
        """
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template, load_default_coll=False)
        time.sleep(10)
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        num_replica = 2
        random_index_nodes = random.sample(indexer_nodes, num_replica+1)
        deploy_nodes = ",".join([f"\'{node.ip}:{node.port}\'" for node in random_index_nodes])
        namespace = self.namespaces[0]
        if self.use_nodes_clause:
            queries = [f'create index idx on {namespace}(name) with {{\'nodes\':[{deploy_nodes}], \'num_replica\': {num_replica}}}',
                       f'create index idx2 on {namespace}(country) with {{\'nodes\':[{deploy_nodes}], \'num_replica\': {num_replica}]}}']
        else:
            queries = [f'create index idx on {namespace}(name) with {{\'num_replica\': {num_replica}}}',
                       f'create index idx2 on {namespace}(country) with {{\'num_replica\': {num_replica}]}}']
        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
        time.sleep(60)
        self.wait_until_indexes_online()
        if self.use_nodes_clause:
            self.validate_node_placement_with_nodes_clause(create_queries=queries)
        self.validate_shard_affinity()
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        query_drop_replica = f'ALTER INDEX {namespace}.idx WITH {{"action":"drop_replica","replicaId": {0}}}'
        self.run_cbq_query(query=query_drop_replica, server=query_node)
        time.sleep(5)
        rest = RestConnection(indexer_nodes[0])
        index_resp = rest.get_indexer_metadata()
        if 'status' not in index_resp:
            raise Exception(f"No index metadata seen in response {index_resp}")
        index_map = index_resp['status']
        host_index_map = {}
        for index in index_map:
            if index['hosts'][0].split(":")[0] in host_index_map:
                host_index_map[index['hosts'][0].split(":")[0]] += 1
            else:
                host_index_map[index['hosts'][0].split(":")[0]] = 1
        index_list, host_ip_to_failover, host_to_failover = [], None, None
        for host in host_index_map:
            if host_index_map[host] != 2:
                host_ip_to_failover = host
        self.log.info(f"host_ip_to_failover {host_ip_to_failover}")
        for item in self.servers:
            if item.ip == host_ip_to_failover:
                host_to_failover = item
        failover_nodes_list = [host_to_failover]
        self.log.info(f"Running failover task for node {failover_nodes_list}")
        failover_task = self.cluster.async_failover([self.master], failover_nodes=failover_nodes_list,
                                                    graceful=False)
        failover_task.result()
        time.sleep(10)
        to_add_nodes = self.servers[self.nodes_init:self.nodes_init + 1]
        rebalance = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=to_add_nodes,
                                                 to_remove=[], services=['index'], cluster_config=self.cluster_config)
        time.sleep(1)
        status, _ = self.rest._rebalance_status_and_progress()
        while status != 'running':
            time.sleep(1)
            status, _ = self.rest._rebalance_status_and_progress()
        time.sleep(10)
        reached = RestHelper(self.rest).rebalance_reached()
        rebalance.result()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        time.sleep(60)
        self.wait_until_indexes_online()
        self.validate_shard_affinity()
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance=map_before_rebalance,
                                                      map_after_rebalance=map_after_rebalance,
                                                      stats_map_before_rebalance=stats_map_before_rebalance,
                                                      stats_map_after_rebalance=stats_map_after_rebalance,
                                                      nodes_in=[],
                                                      nodes_out=failover_nodes_list,
                                                      swap_rebalance=True,
                                                      use_https=False,
                                                      item_count_increase=False,
                                                      per_node=True,
                                                      skip_array_index_item_count=False,
                                                      indexes_changed=False)
        if not self.check_gsi_logs_for_shard_transfer():
            raise Exception("Shard based rebalance not triggered")

    def test_disable_features_to_maintain_shard_affinity(self):
        """
        https://issues.couchbase.com/browse/MB-57860
        Create idx1 with nodes clause. It should fail
        Create idx2 and alter idx2 with move clause. It should fail
        Create idx3, idx4, idx5.... with queryport.client.usePlanner disabled. Indexes should not be placed in
        round-robin fashion.
        """
        if self.cb_version[:4] > "7.6.2":
            self.skipTest("Not applicable in 7.6.2")
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template, load_default_coll=False)
        time.sleep(10)
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        namespace = self.namespaces[0]
        ip, port = indexer_nodes[0].ip, indexer_nodes[0].port
        query = f'create index idx on {namespace}(name) with {{"nodes": ["{ip}:{port}"]}}'
        message = 'defining `nodes` in with clause is not allowed to maintain index grouping (index-shard affinity)'
        try:
            self.run_cbq_query(query=query, server=query_node)
        except Exception as err:
            if message in str(err):
                self.log.info(f"Error raised {str(err)} as expected while running a query with nodes clause")
            else:
                raise Exception(f"Message not seen as expected. Message seen {message}. Message expected {str(err)}")
        time.sleep(10)
        query = f'create index idx on {namespace}(name)'
        self.run_cbq_query(query=query, server=query_node)
        time.sleep(10)
        self.wait_until_indexes_online()
        rest = RestConnection(indexer_nodes[0])
        index_resp = rest.get_indexer_metadata()
        if 'status' not in index_resp:
            raise Exception(f"No index metadata seen in response {index_resp}")
        index_map = index_resp['status']
        for index in index_map:
            ip_host = index['hosts'][0].split(":")[0]
            break
        for node in indexer_nodes:
            if ip_host != node.ip:
                ip_alter = node.ip
                port_alter = node.port
                break
        query = f'alter index idx on {namespace} with {{"action": "move", "nodes": ["{ip_alter}:{port_alter}"]}}'
        message_alter_idx = "move index is disabled"
        try:
            self.run_cbq_query(query=query, server=query_node)
        except Exception as err:
            if message_alter_idx in str(err):
                self.log.info(f"Error raised {str(err)} as expected while running a query with nodes clause")
            else:
                raise Exception(
                    f"Message not seen as expected. Message seen {message_alter_idx}. Message expected {str(err)}")
        self.disable_planner()
        time.sleep(3)
        query = f'create index idx3 on {namespace}(name)'
        message_planner_disabled = "round robin planner is disabled"
        try:
            self.run_cbq_query(query=query, server=query_node)
        except Exception as err:
            if message_planner_disabled in str(err):
                self.log.info(f"Error raised {str(err)} as expected while running a query with planner disabled")
            else:
                raise Exception(
                    f"Message not seen as expected. Message seen {message_alter_idx}. Message expected {str(err)}")
        self.disable_shard_based_rebalance()
        time.sleep(1)
        query = f'create index idx3 on {namespace}(name)'
        self.run_cbq_query(query=query, server=query_node)
        self.wait_until_indexes_online()

    def test_disable_move_node_replicas_to_maintain_shard_affinity(self):
        if self.cb_version[:5] < "7.6.6":
            self.skipTest("Not applicable less than 7.6.6")
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template, load_default_coll=False)
        time.sleep(10)
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        namespace = self.namespaces[0]
        query = f'create index idx on {namespace}(name)'
        self.run_cbq_query(query=query, server=query_node)
        time.sleep(10)
        self.wait_until_indexes_online()
        deploy_nodes = ",".join([f"\'{node.ip}:{node.port}\'" for node in indexer_nodes])

        query = f'ALTER INDEX idx on {namespace} WITH {{"action": "replica_count" ,"num_replica": {self.num_index_replica}, "nodes": [{deploy_nodes}]}}'
        message_alter_idx = "clause is disabled with alter index as file based rebalance (shard affinity) is enabled"
        try:
            self.run_cbq_query(query=query, server=query_node)
        except Exception as err:
            if message_alter_idx in str(err):
                self.log.info(f"Error raised {str(err)} as expected while running a query with nodes clause")
            else:
                raise Exception(
                    f"Message not seen as expected. Message seen {message_alter_idx}. Message expected {str(err)}")


    def test_with_nodes_full_capacity(self):
        if self.cb_version[:4] < "7.6.2":
            self.skipTest("Not applicable in 7.6.2")
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template, load_default_coll=True)
        select_queries = set()
        self.set_flush_buffer_quota(0.5)
        # 100 MB
        self.set_maximum_disk_usage_per_shard(109715200)
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        allowed_num_shards = self.fetch_total_shards_limit() * len(indexer_nodes)
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        nodes_in = self.servers[self.nodes_init]
        create_queries = []
        replica_count = 1
        for _ in range(2):
            for namespace in self.namespaces:
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                           namespace=namespace))
                queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                  namespace=namespace,
                                                                  num_replica=replica_count,
                                                                  randomise_replica_count=True)
                create_queries = queries[:]
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace,
                                                     query_node=query_node)
                create_queries.extend(queries)

        shards_list = self.fetch_plasma_shards_list()
        if len(shards_list) > allowed_num_shards:
            raise Exception(
                f"The total number of shards {shards_list} has exceeded the allowed limit {allowed_num_shards}")
        if len(shards_list) < allowed_num_shards:
            for namespace in self.namespaces:
                queries = [f'create index idx on {namespace}(name)']
                self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
                shards_list = self.fetch_plasma_shards_list()
                if len(shards_list) == allowed_num_shards:
                    break


        self.wait_until_indexes_online()
        self.validate_shard_affinity()
        self.sleep(10)
        slot_id_map_before_with_query = self.find_unique_slot_id_per_node()

        services_in = ["index"]
        rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [nodes_in], [],
                                                 cluster_config=self.cluster_config, services=services_in)

        time.sleep(3)
        status, _ = self.rest._rebalance_status_and_progress()
        while status != 'running':
            time.sleep(1)
            status, _ = self.rest._rebalance_status_and_progress()
        time.sleep(10)
        reached = RestHelper(self.rest).rebalance_reached()
        rebalance.result()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        indexer_nodes_post_rebalance = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        num_replica = 2
        random_index_nodes = random.sample(indexer_nodes_post_rebalance, num_replica + 1)
        deploy_nodes = ",".join([f"\'{node.ip}:{node.port}\'" for node in random_index_nodes])
        if self.use_nodes_clause:
            index_query_on_all_three_nodes = f"create index idx1 on {self.namespaces[0]}(name) with {{\"nodes\":[{deploy_nodes}], \"num_replica\":{num_replica}}}"
            self.run_cbq_query(query=index_query_on_all_three_nodes)
            self.wait_until_indexes_online()

        else:

            query = f"create index idx1 on {self.namespaces[0]}(name) with {{\"num_replica\":{num_replica}}}"
            self.run_cbq_query(query=query)
            self.wait_until_indexes_online()

        slot_id_map_post_with_query = self.find_unique_slot_id_per_node()
        new_slot_id = slot_id_map_post_with_query[f'{nodes_in.ip}:{self.node_port}']
        for node in slot_id_map_before_with_query:
            if new_slot_id in slot_id_map_before_with_query[node]:
                raise Exception(
                    f"The new index is sharing existing shard id shard list before {slot_id_map_before_with_query}, after {slot_id_map_post_with_query}")

        shard_list_before = self.fetch_plasma_shards_list()
        index_query_on_all_three_nodes = f"create index idx2 on {self.namespaces[0]}(name) with {{\"nodes\":[{deploy_nodes}], \"num_replica\":{num_replica}}}"
        self.run_cbq_query(query=index_query_on_all_three_nodes)
        self.wait_until_indexes_online()
        shard_list_after = self.fetch_plasma_shards_list()
        self.assertEqual(len(shard_list_before), len(shard_list_after),
                         f"new shards created before : {shard_list_before}  after {shard_list_after}")

        shard_list_before = self.fetch_plasma_shards_list()
        query = f"create index idx3 on {self.namespaces[0]}(name) with {{\"num_replica\":{num_replica}}}"
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        shard_list_after = self.fetch_plasma_shards_list()
        self.assertEqual(len(shard_list_before), len(shard_list_after),
                         f"new shards created before : {shard_list_before}  after {shard_list_after}")

        slot_id_map_post_all_queries = self.find_unique_slot_id_per_node()
        counter = 0
        for shard in slot_id_map_post_all_queries[f'{nodes_in.ip}:{self.node_port}']:
            if shard in slot_id_map_post_all_queries[
                f'{indexer_nodes_post_rebalance[0].ip}:{self.node_port}'] and shard in slot_id_map_post_all_queries[
                f'{indexer_nodes_post_rebalance[1].ip}:{self.node_port}']:
                counter += 1
        self.assertEqual(counter, 1,
                         f"new slot id not created for the new node for with clause stats : {slot_id_map_post_with_query}")

    def test_missing_partitions_during_shard_rebalance(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.collection_rest.create_scope_collection_count(scope_num=self.num_scopes, collection_num=self.num_collections,
                                                           scope_prefix=self.scope_prefix,
                                                           collection_prefix=self.collection_prefix,
                                                           bucket=self.test_bucket)
        #creating namespaces and loading docs

        scopes = [f'{self.scope_prefix}_{scope_num + 1}' for scope_num in range(self.num_scopes)]
        self.sleep(10, "Allowing time after collection creation")
        for s_item in scopes:
            collections = [f'{self.collection_prefix}_{coll_num + 1}' for coll_num in range(self.num_collections)]
            for c_item in collections:
                self.namespaces.append(f'default:{self.test_bucket}.{s_item}.{c_item}')
                num_docs = 10000
                if c_item == "test_collection_2":
                    num_docs = 1000000
                self.gen_create = SDKDataLoader(num_ops=num_docs, percent_create=100,
                                                percent_update=0, percent_delete=0, scope=s_item,
                                                collection=c_item, json_template=self.json_template,
                                                output=True, username=self.username, password=self.password)

                task = self.cluster.async_load_gen_docs(self.master, bucket=self.test_bucket,
                                                        generator=self.gen_create, pause_secs=1,
                                                        timeout_secs=300, use_magma_loader=True)
                task.result()
        self.enable_shard_based_rebalance()
        self.sleep(10)
        index_nodes_before_rebalance = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        for query in ['create index idx_1 on test_bucket.test_scope_1.test_collection_1(name) partition by hash(meta().id) with {"num_partition": 64}', f'create index idx_2 on test_bucket.test_scope_1.test_collection_2(city, reviews) with {{"nodes": ["{index_nodes_before_rebalance[0].ip}:8091"]}}']:
            self.run_cbq_query(query=query)

        #swap rebalancing out the second node with the third node
        rebalance = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[self.servers[self.nodes_init]],
                                                 to_remove=[index_nodes_before_rebalance[1]], services=['index'])
        time.sleep(1)
        status, _ = self.rest._rebalance_status_and_progress()
        while status != 'running':
            time.sleep(1)
            status, _ = self.rest._rebalance_status_and_progress()
        time.sleep(10)
        reached = RestHelper(self.rest).rebalance_reached()
        rebalance.result()
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")

        #loading 90k docs into collection 1
        bucket, scope, collection = self.namespaces[0].split('.')
        self.gen_create = SDKDataLoader(num_ops=90000, percent_create=100,
                                        percent_update=0, percent_delete=0, scope=scope,
                                        collection=collection, json_template=self.json_template,
                                        output=True, username=self.username, password=self.password)

        task = self.cluster.async_load_gen_docs(self.master, bucket=self.test_bucket,
                                                    generator=self.gen_create, pause_secs=1,
                                                    timeout_secs=300, use_magma_loader=True)
        task.result()

        result = self.run_cbq_query(query="select count(name) from test_bucket.test_scope_1.test_collection_1 where name is not null;")
        self.assertEqual(result[0]["$1"], 100000, "docs not matching")

    # common methods
    def _return_maps(self):
        index_map = self.get_index_map_from_index_endpoint(return_system_query_scope=False)
        stats_map = self.get_index_stats(perNode=True, return_system_query_scope=False)
        return index_map, stats_map

    def run_operation(self, phase="before", action=None, nodes_in_list=None, nodes_out_list=None, select_queries=None):
        if phase == "before":
            self.run_async_index_operations(operation_type="create_index")
        elif phase == "during":
            self.run_async_index_operations(operation_type="query")
            if action is not None:
                if action == "kill_indexer":
                    indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                    server = random.choice(indexer_nodes)
                    time.sleep(5)
                    self._kill_all_processes_index(server=server)
                elif action == "kill_projector":
                    data_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
                    server = random.choice(data_nodes)
                    self._kill_projector_process(server=server)
                elif action == 'stop_rebalance':
                    self.rest = RestConnection(self.master)
                    self.rest.stop_rebalance()
                elif action == 'ddl_during_rebalance':
                    self.perform_ddl_operations_during_rebalance()
        elif phase == "after":
            n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
            country, address, city, email = f"test_country{random.randint(0, 100)}", f"test_add{random.randint(0, 100)}", \
                f"test_city{random.randint(0, 100)}", f"test_email{random.randint(0, 100)}"
            doc_body = {
                "country": country,
                "address": address,
                "free_parking": False,
                "city": city,
                "type": "Hotel",
                "url": "www.henrietta-hegmann.co",
                "reviews": [
                    {
                        "date": "2023-09-15 08:57:48",
                        "author": "Ms. Selma Schaden",
                        "ratings": {
                            "Value": 1,
                            "Cleanliness": 1,
                            "Overall": 4,
                            "Check in / front desk": 2,
                            "Rooms": 2
                        }
                    },
                    {
                        "date": "2023-09-29 08:57:48",
                        "author": "test_author",
                        "ratings": {
                            "Value": 3,
                            "Cleanliness": 1,
                            "Overall": 1,
                            "Check in / front desk": 1,
                            "Rooms": 2
                        }
                    }
                ],
                "phone": "364-389-9784",
                "price": 1134,
                "avg_rating": 3,
                "free_breakfast": True,
                "name": "Jame Cummings Hotel",
                "public_likes": [
                    "Mr. Brian Grimes",
                    "Linwood Hermann",
                    "Micah Funk",
                    "Micheal Hansen"
                ],
                "email": email
            }
            collection_namespace = self.namespaces[0]
            _, keyspace = collection_namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            insert_query = f'INSERT INTO {collection_namespace} (KEY, VALUE) VALUES ("scan_doc_1", {doc_body})'
            select_query = f'Select country, city from {collection_namespace} where meta().id = "scan_doc_1"'
            count_query = f'Select count(meta().id) from {collection_namespace} where price >= 0'
            gen_create = SDKDataLoader(num_ops=self.NUM_DOCS_POST_REBALANCE, percent_create=100,
                                       percent_update=0, percent_delete=0, scope=scope,
                                       collection=collection, start_seq_num=self.num_of_docs_per_collection + 1,
                                       json_template=self.json_template, password=self.password, username=self.username)
            try:
                with ThreadPoolExecutor() as executor:
                    executor.submit(self._load_all_buckets, self.master, gen_create)
                    executor.submit(self.run_cbq_query, query=insert_query, server=n1ql_server)
                    self.sleep(15, "Giving some time so the mutations start")
                    select_task = executor.submit(self.run_cbq_query, query=select_query,
                                                  scan_consistency='request_plus', server=n1ql_server)
                    count_task = executor.submit(self.run_cbq_query, query=count_query, scan_consistency='request_plus',
                                                 server=n1ql_server)

                    result1 = select_task.result()['results'][0]
                    result2 = count_task.result()['results'][0]['$1']
                print(f"Result1 {result1} Result2 {result2}")
                self.assertEqual(result1, {'city': city, 'country': country},
                                 "scan_doc_1 which was inserted before scan request with request_plus is not in result")
                self.assertTrue(result2 > self.num_of_docs_per_collection + 1,
                                "request plus scan is not able to wait for new inserted docs")
            except Exception as err:
                self.fail(str(err))
            self.run_async_index_operations(operation_type="query")
        else:
            self.run_async_index_operations(operation_type="drop_index")

    def rebalance_and_validate(self, nodes_out_list=None, nodes_in_list=None,
                               swap_rebalance=False, skip_array_index_item_count=False,
                               services_in=None, failover_nodes_list=None, select_queries=None,
                               scan_results_check=False, capella_rebalance=None, skip_shard_validations=False):
        if not nodes_out_list:
            nodes_out_list = []
        if not nodes_in_list:
            nodes_in_list = []
        # TODO remove sleep after MB-58840 fix
        time.sleep(60)
        shard_list_before_rebalance = self.fetch_shard_id_list()
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        self.log.info("Running scans before rebalance")
        nodes_corrupt_list = []
        if scan_results_check and select_queries is not None:
            query_result = self.run_scans_and_return_results(select_queries=select_queries)
        if failover_nodes_list is not None:
            self.log.info(f"Running failover task for node {failover_nodes_list}")
            failover_task = self.cluster.async_failover([self.master], failover_nodes=failover_nodes_list,
                                                        graceful=False)
            failover_task.result()
        if self.replica_repair:
            self.log.info("Replica repair flag is set to True. Will choose a few replicas to be dropped")
            self.drop_replicas()
        if self.chaos_action == 'rebalance_during_ddl':
            self.perform_rebalance_during_ddl(nodes_in_list=nodes_in_list, nodes_out_list=nodes_out_list,
                                              services_in=services_in)
            self.wait_until_indexes_online()
            if nodes_in_list:
                nodes_in_list = []
        elif self.chaos_action == 'block_port':
            for i in range(3):
                nodes_list = nodes_in_list + nodes_out_list
                for node in nodes_list:
                    self.block_indexer_port(node=node)
            time.sleep(30)
        elif self.chaos_action == 'corrupt_plasma_metadata':
            if nodes_out_list:
                node = random.choice(nodes_out_list)
                nodes_corrupt_list.append(node)
            self.log.info(f"Nodes chosen for metadata corruption {nodes_corrupt_list}")
            for node in nodes_corrupt_list:
                self.corrupt_plasma_metadata(node)
        # rebalance operation
        if self.capella_run:
            if capella_rebalance == 'rebalance_in':
                self.capella_api.add_nodes_to_capella_cluster(services=['index'], cluster_id=self.cluster_id,
                                                              nodes_count=len(nodes_out_list))
            elif capella_rebalance == 'rebalance_out':
                self.capella_api.remove_nodes_from_capella_cluster(services=['index'], cluster_id=self.cluster_id,
                                                                   nodes_count=len(nodes_out_list))
            elif capella_rebalance == 'swap_rebalance':
                self.capella_api.scale_up_capella_nodes(services=['index'], cluster_id=self.cluster_id)
        else:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], nodes_in_list, nodes_out_list,
                                                     services=services_in, cluster_config=self.cluster_config)
        self.log.info(f"Rebalance task triggered. Wait in loop until the rebalance starts")
        time.sleep(3)
        try:
            status, _ = self.rest._rebalance_status_and_progress()
            while status != 'running':
                time.sleep(1)
                status, _ = self.rest._rebalance_status_and_progress()
            self.log.info("Rebalance has started running.")
        except:
            self.log.info(f"Rebalance status/progress failure because of chaos action {self.chaos_action}")
        if self.chaos_action and self.chaos_action not in ['kill_projector', 'ddl_during_rebalance',
                                                           'rebalance_during_ddl']:
            self.run_operation(phase="during", action=self.chaos_action)
            time.sleep(3)
            if self.chaos_action != 'stop_rebalance':
                # Rebalance failure check skipped for stop rebalance
                try:
                    rebalance.result()
                except Exception as ex:
                    if "Rebalance failed. See logs for detailed reason. You can try again" not in str(ex):
                        self.fail("rebalance failed with some unexpected error : {0}".format(str(ex)))
                else:
                    self.fail("rebalance did not fail despite chaos action")
                finally:
                    # unblock all the blocked indexer ports if any
                    if self.chaos_action == 'block_port':
                        for i in range(3):
                            for node in nodes_in_list:
                                self.unblock_indexer_port(node=node)
                            for node in nodes_out_list:
                                self.unblock_indexer_port(node=node)
                        time.sleep(30)
                    if self.chaos_action == 'corrupt_plasma_metadata':
                        for node in nodes_corrupt_list:
                            self._kill_all_processes_index(server=node)
                        nodes_in_list = []
                        time.sleep(30)
            self.enable_redistribute_indexes()
            time.sleep(120)
            if nodes_in_list:
                nodes_in_list = []
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], nodes_in_list, nodes_out_list,
                                                     services=services_in, cluster_config=self.cluster_config)
            self.log.info(
                f"Rebalance task re-triggered after chaos action. Wait for 20 seconds until the rebalance starts")
            time.sleep(20)
        self.sleep(10)
        if not self.capella_run:
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            shard_affinity = self.is_shard_based_rebalance_enabled()
        else:
            if not capella_rebalance == 'swap_rebalance':
                try:
                    reached = RestHelper(self.rest).rebalance_reached()
                    self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                except Exception as e:
                    if self.capella_rebalance == 'rebalance_in':
                        retry = 0
                        while retry < 5:
                            self.sleep(30)
                            try:
                                reached = RestHelper(self.rest).rebalance_reached()
                            except Exception as e:
                                continue
                            else:
                                break
                            retry += 1
                    else:
                        raise Exception(str(e))
                self.assertTrue(reached, "rebalance failed, stuck or did not complete")

            else:
                self.capella_api.wait_for_cluster(cluster_id=self.cluster_id, sleep_timer=60)
            servers = self.capella_api.get_nodes_formatted(cluster_id=self.cluster_id, username=self.username,
                                                           password=self.password)
            self.servers = servers
            self.update_master_node()
            shard_affinity = True
        # TODO remove sleep after MB-58840 fix
        time.sleep(60)
        if shard_affinity and not skip_shard_validations:
            self.log.info("Running validations for shard-based rebalance")
            self.log.info("Fetching list of shards after completion of rebalance")
            shard_list_after_rebalance = self.fetch_shard_id_list()
            self.log.info("Compare shard list before and after rebalance.")
            # uncomment after MB-58776 is fixed
            if shard_list_before_rebalance:
                if shard_list_after_rebalance != shard_list_before_rebalance and self.chaos_action not in [
                    'rebalance_during_ddl', 'ddl_during_rebalance']:
                    self.log.error(
                        f"Shards before {shard_list_before_rebalance}. Shards after {shard_list_after_rebalance}")
                    raise AssertionError("Shards missing after rebalance")
                self.log.info(
                    f"Shard list before rebalance {shard_list_before_rebalance}. After rebalance {shard_list_after_rebalance}")
                # uncomment after MB-58776 is fixed
                self.validate_shard_affinity()
                self.sleep(30)
                if not self.capella_run:
                    if not self.check_gsi_logs_for_shard_transfer():
                        raise Exception("Shard based rebalance not triggered")
        else:
            self.log.info("Running validations for MOI type indexes")
            if self.check_gsi_logs_for_shard_transfer():
                raise Exception("Shard based rebalance triggered for MOI type indexes")
        self.log.info("Running scans after rebalance")
        if scan_results_check and select_queries is not None and not self.bhive_index:
            n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
            for query in select_queries:
                post_rebalance_result = self.run_cbq_query(query=query, scan_consistency='request_plus',
                                                           server=n1ql_server)['results']
                diffs = DeepDiff(post_rebalance_result, query_result[query], ignore_order=True)
                if diffs:
                    self.log.error(f"Mismatch in query result before and after rebalance. Select query {query}\n\n. "
                                   f"Result before \n\n {query_result[query]}."
                                   f"Result after \n \n {post_rebalance_result}")
                    raise Exception("Mismatch in query results before and after rebalance")
        map_after_rebalance, stats_map_after_rebalance = self._return_maps()
        self.log.info("Fetch metadata after rebalance")
        if self.chaos_action in ['rebalance_during_ddl', 'ddl_during_rebalance']:
            indexes_changed = True
        else:
            indexes_changed = False
        self.n1ql_helper.verify_indexes_redistributed(map_before_rebalance=map_before_rebalance,
                                                      map_after_rebalance=map_after_rebalance,
                                                      stats_map_before_rebalance=stats_map_before_rebalance,
                                                      stats_map_after_rebalance=stats_map_after_rebalance,
                                                      nodes_in=nodes_in_list,
                                                      nodes_out=nodes_out_list,
                                                      swap_rebalance=swap_rebalance,
                                                      use_https=False,
                                                      item_count_increase=False,
                                                      per_node=True,
                                                      skip_array_index_item_count=skip_array_index_item_count,
                                                      indexes_changed=indexes_changed)

    def drop_replicas(self, num_indexes=5):
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(indexer_nodes[0])
        index_map = rest.get_indexer_metadata()['status']
        replicas_dict = {}
        n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        for index in index_map:
            if index['numReplica'] > 0:
                if index['defnId'] in replicas_dict:
                    replicas_dict[index['defnId']].append(index)
                else:
                    replicas_dict[index['defnId']] = [index]
        replicas_dict_items_list = random.sample(list(replicas_dict.keys()), k=num_indexes)
        for replicas_dict_item in replicas_dict_items_list:
            item = replicas_dict[replicas_dict_item]
            replica_index = random.choice(item)
            bucket, name, replicaID = replica_index['bucket'], replica_index['indexName'], replica_index['replicaId']
            scope, collection = replica_index['scope'], replica_index['collection']
            alter_index_query = f'ALTER INDEX default:`{bucket}`.`{scope}`.`{collection}`.`{name}` WITH {{"action":"drop_replica","replicaId": {replicaID}}}'
            self.log.info(f"drop query to be run {alter_index_query}")
            self.n1ql_helper.run_cbq_query(query=alter_index_query, server=n1ql_node)

    def run_post_rebalance_operations(self, map_after_rebalance, stats_map_after_rebalance):
        if self.gsi_type == 'memory_optimized':
            self.log.info("Skipping post rebalance operations for MOI type indexes")
            return
        self.run_operation(phase="after")
        map_after_rebalance_2, stats_map_after_rebalance_2 = self._return_maps()
        self.n1ql_helper.validate_item_count_data_size(map_before_rebalance=map_after_rebalance,
                                                       map_after_rebalance=map_after_rebalance_2,
                                                       stats_map_before_rebalance=stats_map_after_rebalance,
                                                       stats_map_after_rebalance=stats_map_after_rebalance_2,
                                                       item_count_increase=True,
                                                       per_node=True,
                                                       skip_array_index_item_count=True)

    def install_tools(self):
        nodes = self.servers
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            shell.execute_command(command='apt-get install -qq -y stress-ng > /dev/null && echo 1 || echo 0',
                                  get_pty=True)
            shell.execute_command(command='apt-get install -qq -y stress > /dev/null && echo 1 || echo 0', get_pty=True)
            shell.execute_command(command='apt-get install -qq -y sysstat > /dev/null && echo 1 || echo 0',
                                  get_pty=True)
            shell.execute_command(command='apt-get install -qq -y iptables > /dev/null && echo 1 || echo 0',
                                  get_pty=True)

    def run_stress_tool(self, stress_factor=0.25, timeout=1800):
        nodes_all = self.servers
        shell = RemoteMachineShellConnection(nodes_all[0])
        free_mem_cmd = "free -m | awk 'NR==2 {print $4}'"
        output, error = shell.execute_command(free_mem_cmd)
        free_mem_in_mb = int(output[0])
        ram = math.floor(free_mem_in_mb * stress_factor)
        num_cpu_cmd = "grep -c ^processor /proc/cpuinfo"
        output, error = shell.execute_command(num_cpu_cmd)
        num_cpu = int(output[0])
        cpu = math.floor(num_cpu * stress_factor)
        cmd = f'stress --cpu {cpu} --vm-bytes {ram}M --vm 1 --timeout {timeout} -d 1 & > /dev/null && echo 1 || echo 0'
        self.log.info(f"Will run this command to simulate CPU and memory stress {cmd}")
        with ThreadPoolExecutor() as executor_main:
            for node in nodes_all:
                shell = RemoteMachineShellConnection(node)
                executor_main.submit(shell.execute_command, cmd)

    def fill_up_disk(self, disk_fill_percent=10):
        self.log.info("Will check the disk usage on all indexer nodes")
        nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(nodes[0])
        storage_dir = os.path.dirname((rest.get_indexer_internal_stats()['indexer.storage_dir']))
        cmd_device = f"df -P -T {storage_dir} | tail -n +2 | awk '{{print $1}}'"
        DUMMY_FILE_NAME = 'DUMMY_FILE_DELETE_IF_STILL_PRESENT'
        nodes_paths_dict = {}
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            self.log.info(f"Cmd to find the device {cmd_device}")
            output, error = shell.execute_command(cmd_device)
            device = output[0]
            self.log.info(f"Device {device}")
            empty_space_cmd = f"df -P -T -h {device} | tail -n +2 | awk '{{print $5}}'"
            output, error = shell.execute_command(empty_space_cmd)
            self.log.info(f"Cmd to find empty space {empty_space_cmd}")
            space = float(output[0].rstrip("G"))
            self.log.info(f"Empty space {space}")
            space_to_fill = math.floor(disk_fill_percent * space * 1024 / 100)
            self.log.info(f"space_to_fill is  {space_to_fill}")
            cmd_disk_fill_up = fr"dd if={device} of={storage_dir}/{DUMMY_FILE_NAME} bs=1M count={space_to_fill}"
            self.log.info(f"cmd_disk_fill_up is  {cmd_disk_fill_up}")
            shell.execute_command(cmd_disk_fill_up, timeout=3600)
            nodes_paths_dict[node] = f"rm -f {storage_dir}/{DUMMY_FILE_NAME}"
        return nodes_paths_dict

    def delete_dummy_files(self, nodes_paths_dict):
        for item in nodes_paths_dict:
            shell = RemoteMachineShellConnection(item)
            shell.execute_command(nodes_paths_dict[item])

    def run_disk_usage_tool(self, event, timeout=1800):
        self.log.info("Will check the disk usage on all indexer nodes")
        nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        rest = RestConnection(nodes[0])
        storage_dir = rest.get_indexer_internal_stats()['indexer.storage_dir']
        cmd_device = f"df -P -T {storage_dir} | tail -n +2 | awk '{{print $1}}'"
        time_now = time.time()
        while not event.is_set() and time.time() - time_now < timeout:
            for node in nodes:
                shell = RemoteMachineShellConnection(node)
                output, error = shell.execute_command(cmd_device)
                device = output[0]
                self.log.info(f"The physical device is {device}")
                shell.execute_command(f"iostat -dkxyN 1 1 {device} | grep -v '^$' | tail -n 2")
                output, error = shell.execute_command(f"iostat -dkxyN 1 1 {device} | grep -v '^$' | tail -n 2")
                disk_output = output[0]
                print(f"Disk output as seen on {node.ip} is {disk_output}")
                time.sleep(60)

    def load_until_index_dgm(self, resident_ratio=50):
        rr_achieved, end_num, batch_size = False, self.num_of_docs_per_collection, 10000
        avg_avg_rr = self.compute_cluster_avg_rr_index()
        self.log.info(f"Before load_until_index_dgm: Avg of avg_resident_ratio across the cluster is {avg_avg_rr}")
        if avg_avg_rr > resident_ratio:
            while not rr_achieved:
                for item in self.namespaces:
                    s_item, c_item = item.split(":")[1].split(".")[1], item.split(":")[1].split(".")[2]
                    end_num = end_num + batch_size
                    self.gen_create = SDKDataLoader(num_ops=end_num, percent_create=100,
                                                    percent_update=0, percent_delete=0, scope=s_item,
                                                    collection=c_item, json_template=self.json_template,
                                                    output=True, username=self.username, password=self.password,
                                                    end=end_num, start_seq_num=end_num - batch_size + 1)
                    tasks = self.data_ops_javasdk_loader_in_batches(sdk_data_loader=self.gen_create,
                                                                    batch_size=batch_size,
                                                                    dataset=self.json_template)
                time.sleep(10)
                avg_avg_rr = self.compute_cluster_avg_rr_index()
                self.log.info(f"Avg of avg_resident_ratio across the cluster is {avg_avg_rr}")
                if avg_avg_rr <= resident_ratio:
                    rr_achieved = True
                    self.log.info("Target RR achieved")
                for task in tasks:
                    task.result()
            self.sleep(10)
        else:
            self.log.info(f"The avg RR is already below targeted {resident_ratio}. Not loading any more data")

    def run_query_load(self, select_queries, event, timeout=1800):
        query_tasks = []
        time_now = time.time()
        while not event.is_set() and time.time() - time_now < timeout:
            with ThreadPoolExecutor() as executor_main:
                n1ql_server = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=False)
                for query in select_queries:
                    query_task = executor_main.submit(self.run_cbq_query, query, 10, n1ql_server)
                    query_tasks.append(query_task)
            time.sleep(60)
