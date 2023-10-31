import random
import time

from membase.api.rest_client import RestConnection, RestHelper
from concurrent.futures import ThreadPoolExecutor
from couchbase_helper.documentgenerator import SDKDataLoader
from lib import testconstants
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.fts.fts_base import NodeHelper
from pytests.query_tests_helper import QueryHelperTests
from .base_gsi import BaseSecondaryIndexingTests, log


from threading import Event
from deepdiff import DeepDiff


class FileBasedRebalance(BaseSecondaryIndexingTests, QueryHelperTests,  NodeHelper):
    def setUp(self):
        if self._testMethodName not in ['suite_tearDown', 'suite_setUp']:
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
            # TODO. After adding other tests, check if this can be removed
            # if not self.capella_run:
            #     shell = RemoteMachineShellConnection(self.servers[0])
            #     info = shell.extract_remote_info().type.lower()
            #     if info == 'linux':
            #         if self.nonroot:
            #             self.cli_command_location = testconstants.LINUX_NONROOT_CB_BIN_PATH
            #         else:
            #             self.cli_command_location = testconstants.LINUX_COUCHBASE_BIN_PATH
            #     elif info == 'windows':
            #         self.cmd_ext = ".exe"
            #         self.cli_command_location = testconstants.WIN_COUCHBASE_BIN_PATH_RAW
            #     elif info == 'mac':
            #         self.cli_command_location = testconstants.MAC_COUCHBASE_BIN_PATH
            #     else:
            #         raise Exception("OS not supported.")
            self.rand = random.randint(1, 1000000000)
            self.alter_index = self.input.param("alter_index", None)
            if self.ansi_join:
                self.rest.load_sample("travel-sample")
                self.sleep(10)
            self.enable_shard_based_rebalance()
            self.NUM_DOCS_POST_REBALANCE = 10 ** 5

    def tearDown(self):
        if self._testMethodName not in ['suite_tearDown', 'suite_setUp']:
            super(FileBasedRebalance, self).tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def test_gsi_rebalance_out_indexer_node(self):
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
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                    for namespace in self.namespaces:
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
                self.wait_until_indexes_online()
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
        indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for indexer_node in indexer_nodes:
            rest = RestConnection(indexer_node)
            rest.set_index_settings({"indexer.settings.rebalance.redistribute_indexes": True})
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
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                    for namespace in self.namespaces:
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
                self.wait_until_indexes_online()
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
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                    for namespace in self.namespaces:
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
                self.wait_until_indexes_online()
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
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                    for namespace in self.namespaces:
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
                self.wait_until_indexes_online()
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
        self.disable_shard_based_rebalance()
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
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                    for namespace in self.namespaces:
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
                self.wait_until_indexes_online()
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
                                                scan_results_check=scan_results_check
                                                )
                    self.validate_shard_affinity()
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
                        self.validate_shard_affinity()
                map_after_rebalance, stats_map_after_rebalance = self._return_maps()
                self.run_post_rebalance_operations(map_after_rebalance=map_after_rebalance,
                                                   stats_map_after_rebalance=stats_map_after_rebalance)
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
                for _ in range(self.initial_index_batches):
                    replica_count = random.randint(1, 2)
                    query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                    for namespace in self.namespaces:
                        select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                                   namespace=namespace))
                        queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                          namespace=namespace,
                                                                          num_replica=replica_count,
                                                                          randomise_replica_count=True)
                        self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
                self.wait_until_indexes_online()
                self.validate_shard_affinity()
                nodes_out = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                nodes_out_list = nodes_out[:2]
                if self.rebalance_all_at_once:
                    # rebalance out all nodes at once
                    self.rebalance_and_validate(nodes_out_list=nodes_out_list,
                                                swap_rebalance=False,
                                                skip_array_index_item_count=skip_array_index_item_count,
                                                select_queries=select_queries,
                                                scan_results_check=scan_results_check, capella_rebalance=self.capella_rebalance)
                else:
                    # rebalance out 1 node after another
                    for count, node in enumerate(nodes_out_list):
                        self.log.info(f"Running rebalance number {count}")
                        self.rebalance_and_validate(nodes_out_list=[node], swap_rebalance=False,
                                                    skip_array_index_item_count=skip_array_index_item_count,
                                                    select_queries=select_queries,
                                                    scan_results_check=scan_results_check, capella_rebalance=self.capella_rebalance)
                map_after_rebalance, stats_map_after_rebalance = self._return_maps()
                self.run_post_rebalance_operations(map_after_rebalance=map_after_rebalance,
                                                   stats_map_after_rebalance=stats_map_after_rebalance)
            except Exception as e:
                raise Exception(e)
            finally:
                event.set()
                if self.continuous_mutations:
                    future.result()

    # common methods
    def _return_maps(self):
        index_map = self.get_index_map_from_index_endpoint(return_system_query_scope=False)
        stats_map = self.get_index_stats(perNode=True, return_system_query_scope=False)
        return index_map, stats_map

    def run_operation(self, phase="before", action=None, nodes_in_list=None, nodes_out_list=None):
        if phase == "before":
            self.run_async_index_operations(operation_type="create_index")
        elif phase == "during":
            self.run_async_index_operations(operation_type="query")
            if action is not None:
                if action == "kill_indexer":
                    indexer_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                    server = random.choice(indexer_nodes)
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
                elif action == 'shard_corruption':
                    pass
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
                               scan_results_check=False, capella_rebalance=None):
        if not nodes_out_list:
            nodes_out_list = []
        if not nodes_in_list:
            nodes_in_list = []
        # TODO remove sleep after MB-58840 fix
        time.sleep(60)
        shard_list_before_rebalance = self.fetch_shard_id_list()
        map_before_rebalance, stats_map_before_rebalance = self._return_maps()
        self.log.info("Running scans before rebalance")
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
        elif self.chaos_action == 'block_port':
            for i in range(3):
                nodes_list = nodes_in_list + nodes_out_list
                for node in nodes_list:
                    self.block_indexer_port(node=node)
            time.sleep(30)
        # rebalance operation
        if self.capella_run:
            if capella_rebalance == 'rebalance_in':
                self.capella_api.add_nodes_to_capella_cluster(services=['index'], cluster_id=self.cluster_id, nodes_count=len(nodes_out_list))
            elif capella_rebalance == 'rebalance_out':
                self.capella_api.remove_nodes_from_capella_cluster(services=['index'], cluster_id=self.cluster_id, nodes_count=len(nodes_out_list))
            elif capella_rebalance == 'swap_rebalance':
                self.capella_api.scale_up_capella_nodes(services=['index'], cluster_id=self.cluster_id)
        else:
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], nodes_in_list, nodes_out_list,
                                                     services=services_in, cluster_config=self.cluster_config)
        self.log.info(f"Rebalance task triggered. Wait in loop until the rebalance starts")
        time.sleep(3)
        status, _ = self.rest._rebalance_status_and_progress()
        while status != 'running':
            time.sleep(1)
            status, _ = self.rest._rebalance_status_and_progress()
        self.log.info("Rebalance has started running.")
        if not self.capella_run:
            self.run_operation(phase="during", action=self.chaos_action, nodes_in_list=nodes_in_list,
                               nodes_out_list=nodes_out_list)
        if self.chaos_action and self.chaos_action not in ['kill_projector', 'ddl_during_rebalance', 'rebalance_during_ddl']:
            if self.chaos_action != 'stop_rebalance':
                # Rebalance failure check skipped for stop rebalance
                try:
                    reached = RestHelper(self.rest).rebalance_reached()
                    self.assertTrue(reached, "rebalance failed, stuck or did not complete")
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
            time.sleep(10)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], nodes_in_list, nodes_out_list,
                                                     services=services_in, cluster_config=self.cluster_config)
            self.log.info(f"Rebalance task re-triggered after chaos action. Wait in loop until the rebalance starts")
            time.sleep(3)
            status, _ = self.rest._rebalance_status_and_progress()
            while status != 'running':
                time.sleep(1)
                status, _ = self.rest._rebalance_status_and_progress()
        time.sleep(10)
        if not self.capella_run:
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            rebalance.result()
            self.run_operation(phase="during", action=self.chaos_action)
        if not capella_rebalance == 'swap_rebalance':
            reached = RestHelper(self.rest).rebalance_reached()
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        else:
            self.capella_api.wait_for_cluster(cluster_id=self.cluster_id, sleep_timer=60)
        if self.capella_run:
            servers = self.capella_api.get_nodes_formatted(cluster_id=self.cluster_id, username=self.username,
                                                           password=self.password)
            self.servers = servers
            self.update_master_node()

        if not self.capella_run:
            rebalance.result()
        # TODO remove sleep after MB-58840 fix
        time.sleep(60)
        if not self.capella_run:
            shard_affinity = self.is_shard_based_rebalance_enabled()
        else:
            shard_affinity = True
        if shard_affinity:
            self.log.info("Running validations for shard-based rebalance")
            self.log.info("Fetching list of shards after completion of rebalance")
            shard_list_after_rebalance = self.fetch_shard_id_list()
            self.log.info("Compare shard list before and after rebalance.")
            # uncomment after MB-58776 is fixed
            if shard_list_after_rebalance != shard_list_before_rebalance and self.chaos_action not in ['rebalance_during_ddl', 'ddl_during_rebalance']:
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
        if scan_results_check and select_queries is not None:
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
        self.run_operation(phase="after")
        map_after_rebalance_2, stats_map_after_rebalance_2 = self._return_maps()
        self.n1ql_helper.validate_item_count_data_size(map_before_rebalance=map_after_rebalance,
                                                       map_after_rebalance=map_after_rebalance_2,
                                                       stats_map_before_rebalance=stats_map_after_rebalance,
                                                       stats_map_after_rebalance=stats_map_after_rebalance_2,
                                                       item_count_increase=True,
                                                       per_node=True,
                                                       skip_array_index_item_count=True)

