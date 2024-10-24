"""
gsi_percentage_quota.py: This suite performs test for percentage memory quota changes
https://issues.couchbase.com/browse/MB-57060

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = 18/07/23 3:23 pm

"""
import json
import random
import requests
from concurrent.futures import ThreadPoolExecutor
from couchbase_helper.documentgenerator import SDKDataLoader
from membase.api.rest_client import RestConnection
from table_view import TableView

from .base_gsi import BaseSecondaryIndexingTests


class GSIPercentageQuota(BaseSecondaryIndexingTests):
    def setUp(self):
        super(GSIPercentageQuota, self).setUp()
        self.log.info("==============  ReplicaRepair setup has started ==============")
        buckets = self.rest.get_buckets()
        if buckets:
            for bucket in buckets:
                RestConnection.delete_bucket(self, bucket=bucket.name)
        self.initial_index_batches = self.input.param("initial_index_batches", 3)
        self.resident_ratio = self.input.param("resident_ratio", 100)
        self.scale_up_services = self.input.param("scale_up_services", None)
        self.password = self.input.membase_settings.rest_password
        self.scale_up_services = self.scale_up_services.split(':')
        self.log.info("==============  GSIPercentageQuota setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  GSIPercentageQuota tearDown has started ==============")
        self.log.info("==============  GSIPercentageQuota tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  GSIPercentageQuota tearDown has started ==============")
        super(GSIPercentageQuota, self).tearDown()
        self.log.info("==============  GSIPercentageQuota tearDown has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  GSIPercentageQuota suite_setup has started ==============")
        self.log.info("==============  GSIPercentageQuota suite_setup has completed ==============")

    def gen_node_wise_indexes_dict(self, index_metatdata):
        node_wise_non_partitoned_indexes = {}
        node_wise_partitioned_indexes = {}
        for idx in index_metatdata:
            idx_name = idx['name']
            hosts = [host for host in idx['hosts']]
            is_partitioned = idx['partitioned']
            if is_partitioned:
                partition_map = idx['partitionMap']
                for host, partitions in partition_map.items():
                    host = host.split(':')[0]
                    if host in node_wise_partitioned_indexes:
                        node_wise_partitioned_indexes[host].append((idx_name, sorted(partitions)))
                    else:
                        node_wise_partitioned_indexes[host] = [(idx_name, sorted(partitions))]
            else:
                host = hosts[0].split(':')[0]
                if host in node_wise_non_partitoned_indexes:
                    node_wise_non_partitoned_indexes[host].append(idx_name)
                else:
                    node_wise_non_partitoned_indexes[host] = [idx_name]
        return node_wise_non_partitoned_indexes, node_wise_partitioned_indexes

    def validate_swap_only_index_movement(self, index_metatdata_before_reb, indexer_metatdata_after_reb,
                                          out_node, in_node):
        np_indexes_data_b4, p_indexes_data_b4 = self.gen_node_wise_indexes_dict(index_metatdata_before_reb)
        np_indexes_data_aft, p_indexes_data_aft = self.gen_node_wise_indexes_dict(indexer_metatdata_after_reb)

        expected_swap_indexes = np_indexes_data_b4[out_node]
        actual_swap_indexes = np_indexes_data_aft[in_node]

        expected_pt_swap_indexes = p_indexes_data_b4[out_node]
        actual_pt_swap_indexes = p_indexes_data_aft[in_node]

        if expected_swap_indexes != actual_swap_indexes:
            self.log.error("Swap indexes data not matching")
        if expected_pt_swap_indexes != actual_pt_swap_indexes:
            self.log.error("Partitioned Swap indexes data not matching")
        # todo: to be replaced with above if statements
        # self.assertEqual(expected_swap_indexes, actual_swap_indexes, "Swap indexes data not matching")
        # self.assertEqual(expected_pt_swap_indexes, actual_pt_swap_indexes, "Swap indexes data not matching")


    def stats_validation_in_heterogenous_cluster(self):
        stats_dict = {}
        table = TableView(self.log.info)
        table.set_headers(["Node", "Percentage Memory Quota", "Old Memory Quota", "New Memory Quota"])
        try:
            nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            for node in nodes:
                rest = RestConnection(node)
                user_stats = rest.get_indexer_internal_stats()
                new_stats = rest.get_indexer_stats()
                percentage_memory_quota = user_stats['indexer.settings.percentage_memory_quota']
                old_memory_quota = user_stats['indexer.settings.memory_quota']
                new_memory_quota = new_stats['memory_quota']

                if percentage_memory_quota == 0:
                    self.fail(f"percentage_memory_quota shouldn't be zero for heterogeneous cluster:"
                              f" - {user_stats}")

                if node.ip in stats_dict:
                    stats_dict[node.ip].append({'percentage_memory_quota': percentage_memory_quota,
                                                'old_memory_quota': old_memory_quota,
                                                'new_memory_quota_value': new_memory_quota})
                else:
                    stats_dict[node.ip] = [{'percentage_memory_quota': percentage_memory_quota,
                                            'memory_quota': old_memory_quota,
                                            'new_memory_quota_value': new_memory_quota}]
                table.add_row([node, percentage_memory_quota, old_memory_quota, new_memory_quota])
        except Exception as err:
            self.log.error(f"Error occurred during user_stats validation: {err}")
        finally:
            table.display("Index Memory Quota user_stats:")

    def ddl_during_rebalance(self, query_node=None):
        if query_node is None:
            query_node = self.get_nodes_from_services_map(service_type="n1ql")
        query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
        indexes_name = self.gsi_util_obj.get_indexes_name(query_definitions=query_definitions)
        alter_template = 'ALTER index {} on {} WITH {{"action": "drop_replica", "replicaId": 0}};'
        for namespace in self.namespaces:
            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace, defer_build=True,
                                                              num_replica=self.num_index_replica)
            drop_queries = self.gsi_util_obj.get_drop_index_list(definition_list=query_definitions,
                                                                 namespace=namespace)

            alter_queries = [alter_template.format(idx_name, namespace) for idx_name in indexes_name]
            try:
                self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace,
                                                     query_node=query_node)
            except Exception as err:
                self.log.error(f"Error occurred during DDL: {err}")
            self.wait_until_indexes_online(defer_build=True)
            for query in alter_queries:
                try:
                    self.run_cbq_query(query=query, server=query_node)
                except Exception as err:
                    self.log.error(f"Error occurred during Alter query: {err}")
            for query in drop_queries:
                try:
                    self.run_cbq_query(query=query, server=query_node)
                except Exception as err:
                    self.log.error(f"Error occurred during Drop indexes: {err}")

    def set_exclude_pararms_during_scale_up(self, out_node):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for index_node in index_nodes:
            rest = RestConnection(index_node)
            if index_node.ip == out_node:
                self.log.info(f"Setting excludeNode=in for {index_node.ip}")
                index_setting = "excludeNode=in"
                rest.set_index_planner_settings(index_setting)
            else:
                self.log.info(f"Setting excludeNode=inout for {index_node.ip}")
                index_setting = "excludeNode=inout"
                rest.set_index_planner_settings(index_setting)

    def reset_exclude_params(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for index_node in index_nodes:
            rest = RestConnection(index_node)
            self.log.info(f"Re-setting excludeNode for {index_node.ip}")
            index_setting = "excludeNode="
            rest.set_index_planner_settings(index_setting)

    def monitor_scale_up(self, verify_swap_rebalance=True, select_queries=None, query_node=None):
        out_order = self.capella_api.get_replacement_order(cluster_id=self.cluster_id)

        # checking if there is a delay in deployment job initiation
        if not out_order:
            out_order = self.capella_api.get_replacement_order(cluster_id=self.cluster_id)
        for node in out_order:
            self.master = self.update_master_node()
            self.rest = RestConnection(self.master)
            self.set_exclude_pararms_during_scale_up(out_node=node)

            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            index_node = [i_node for i_node in index_nodes if i_node.ip != node][0]
            rest = RestConnection(index_node)

            metadata_b4_reb = rest.get_indexer_metadata()['status']
            index_nodes_b4_reb = {node.ip for node in index_nodes}

            # checking for rebalance to start
            self.log.info("Waiting for Rebalance to start")
            while self.rest._rebalance_progress_status() != 'running':
                continue
            self.log.info("Rebalance started...")
            if select_queries:
                select_tasks = self.gsi_util_obj.aysnc_run_select_queries(select_queries=select_queries,
                                                                         query_node=query_node)
            # cancel Rebalance Not working
            self.sleep(5)
            if self.cancel_rebalance:
                self.rest.stop_rebalance()

            # checking for rebalance to finish
            self.log.info("Waiting for Rebalance to finish")
            while self.rest._rebalance_progress_status() != 'none':
                continue
            self.log.info("Rebalance finished...")
            self.sleep(5)
            if select_queries:
                for task in select_tasks:
                    task.result()
            metadata_aft_reb = rest.get_indexer_metadata()['status']

            servers = self.capella_api.get_nodes_formatted(cluster_id=self.cluster_id, username=self.username,
                                                           password=self.password)
            for server in servers:
                if server.ip == node:
                    servers.remove(server)
                    break
            self.servers = servers
            new_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            index_nodes_aft_reb = {node.ip for node in new_index_nodes}

            # Validating percentage changes with scalen up
            self.stats_validation_in_heterogenous_cluster()
            # validating the swap only movement of indexes
            if verify_swap_rebalance:
                in_node = list(index_nodes_aft_reb - index_nodes_b4_reb)[0]
                out_node = list(index_nodes_b4_reb - index_nodes_aft_reb)[0]
                self.validate_swap_only_index_movement(index_metatdata_before_reb=metadata_b4_reb,
                                                       indexer_metatdata_after_reb=metadata_aft_reb,
                                                       in_node=in_node, out_node=out_node)
            # self.ddl_during_rebalance()
        self.reset_exclude_params()

    def test_percentage_quota_provision(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template)
        select_queries = set()
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        for _ in range(self.initial_index_batches):
            query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
            for namespace in self.namespaces:
                select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                           namespace=namespace))
                queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                  namespace=namespace,
                                                                  num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
        self.wait_until_indexes_online()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)

        self.dataload_till_rr(namespaces=self.namespaces, rr=self.resident_ratio, json_template=self.json_template)
        self.gsi_util_obj.query_event.set()

        # Todo: Temporary; to be removed after CP changes
        percentage_quota_setting = {"indexer.settings.percentage_memory_quota": 82}
        ddl_during_scale_up_enabled = {"indexer.allowDDLDuringScaleUp": True}
        index_node = index_nodes[0]
        index_rest = RestConnection(index_node)
        self.capella_api.set_capella_index_settings(node=index_node.ip, setting=percentage_quota_setting,
                                                    username=index_node.rest_username,
                                                    password=index_node.rest_password)
        self.capella_api.set_capella_index_settings(node=index_node.ip, setting=ddl_during_scale_up_enabled,
                                                    username=index_node.rest_username,
                                                    password=index_node.rest_password)
        metadata_before_scale_up = index_rest.get_indexer_metadata()['status']
        idx_name_list = {idx['name'] for idx in metadata_before_scale_up}

        self.capella_api.scale_up_capella_nodes(services=self.scale_up_services, cluster_id=self.cluster_id)
        self.sleep(5)
        with ThreadPoolExecutor() as executor:
            select_task = executor.submit(self.gsi_util_obj.run_continous_query_load,
                                          select_queries=select_queries, query_node=query_node)
            monitor_task = executor.submit(self.monitor_scale_up)
            task = executor.submit(self.capella_api.wait_for_cluster, cluster_id=self.cluster_id, sleep_timer=60)
            # Wait for cluster to stable
            task.result()

            # Todo: Temporary; to be removed after CP changes
            percentage_quota_setting = {"indexer.settings.percentage_memory_quota": 0}
            ddl_during_scale_up_disabled = {"indexer.allowDDLDuringScaleUp": False}
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            index_rest = RestConnection(index_nodes[0])
            self.capella_api.set_capella_index_settings(node=index_nodes[0].ip, setting=percentage_quota_setting,
                                                        username=index_nodes[0].rest_username,
                                                        password=index_nodes[0].rest_password)
            self.capella_api.set_capella_index_settings(node=index_nodes[0].ip, setting=ddl_during_scale_up_disabled,
                                                        username=index_nodes[0].rest_username,
                                                        password=index_nodes[0].rest_password)

            self.log.info("Clearing events for all parallel tasks")
            self.gsi_util_obj.query_event.clear()

            monitor_task.result()
            select_task.result()

            self.log.info(f"New Indexer nodes: {index_nodes}")
            metadata_after_scale_up = index_rest.get_indexer_metadata()['status']
            new_idx_name_list = {idx['name'] for idx in metadata_after_scale_up}
            self.assertEqual(len(metadata_before_scale_up), len(metadata_after_scale_up),
                             "Indexer metadata not matching")
            if idx_name_list - new_idx_name_list:
                self.fail(f"Some index/es is/are missing after Scale up: {idx_name_list - new_idx_name_list}")

    def test_replica_repair_during_scale_up(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template)

        select_queries = set()
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        for _ in range(self.initial_index_batches):
            query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
            for namespace in self.namespaces:
                select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                           namespace=namespace))
                queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                  namespace=namespace,
                                                                  num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
        self.wait_until_indexes_online()
        self.dataload_till_rr(namespaces=self.namespaces, rr=self.resident_ratio, json_template=self.json_template)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        index_node = index_nodes[0]
        index_rest = RestConnection(index_node)
        disable_replica_repair = {"indexer.rebalance.disable_replica_repair": True}
        self.capella_api.set_capella_index_settings(node=index_node.ip, setting=disable_replica_repair,
                                                    username=index_node.rest_username,
                                                    password=index_node.rest_password)

        # Removing an index node to introduce replica lost
        metadata_before_remove_node = index_rest.get_indexer_metadata()['status']
        self.capella_api.remove_nodes_from_capella_cluster(services=['index'], cluster_id=self.cluster_id,
                                                           wait_for_healthy_cluster=True)
        metadata_after_remove_node = index_rest.get_indexer_metadata()['status']

        # Adding a new index node with replica repair disabled
        self.capella_api.add_nodes_to_capella_cluster(services=['index'], cluster_id=self.cluster_id,
                                                      wait_for_healthy_cluster=True)
        metadata_after_add_node = index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(metadata_after_remove_node), len(metadata_after_add_node))

        # Todo: Temporary; to be removed after CP changes
        percentage_quota_setting = {"indexer.settings.percentage_memory_quota": 75}
        enable_replica_repair = {"indexer.rebalance.disable_replica_repair": False}
        self.capella_api.set_capella_index_settings(node=index_node.ip, setting=enable_replica_repair,
                                                    username=index_node.rest_username,
                                                    password=index_node.rest_password)
        self.capella_api.set_capella_index_settings(node=index_node.ip, setting=percentage_quota_setting,
                                                    username=index_node.rest_username,
                                                    password=index_node.rest_password)
        metadata_before_scale_up = metadata_after_add_node
        idx_name_list = {idx['name'] for idx in metadata_before_scale_up}

        self.capella_api.scale_up_capella_nodes(services=['index'], cluster_id=self.cluster_id)
        self.sleep(5)
        with ThreadPoolExecutor() as executor:
            select_task = executor.submit(self.gsi_util_obj.run_continous_query_load,
                                          select_queries=select_queries, query_node=query_node)
            stats_validation_task = executor.submit(self.stats_validation_in_heterogenous_cluster)
            monitor_task = executor.submit(self.monitor_scale_up, verify_swap_rebalance=False)
            task = executor.submit(self.capella_api.wait_for_cluster, cluster_id=self.cluster_id, sleep_timer=60)
            # Wait for cluster to become stable
            task.result()

            # Todo: Temporary; to be removed after CP changes
            percentage_quota_setting = {"indexer.settings.percentage_memory_quota": 0}
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            index_rest = RestConnection(index_nodes[0])
            self.capella_api.set_capella_index_settings(node=index_nodes[0].ip, setting=percentage_quota_setting,
                                                        username=index_nodes[0].rest_username,
                                                        password=index_nodes[0].rest_password)

            self.log.info("Clearing events for all parallel tasks")
            self.gsi_util_obj.query_event.clear()
            self.stats_check_event.clear()
            self.gsi_util_obj.query_event.clear()

            monitor_task.result()
            select_task.result()
            stats_validation_task.result()

            self.log.info(f"New Indexer nodes: {index_nodes}")
            metadata_after_scale_up = index_rest.get_indexer_metadata()['status']
            new_idx_name_list = {idx['name'] for idx in metadata_after_scale_up}
            self.assertEqual(len(metadata_before_remove_node), len(metadata_after_scale_up),
                             "Indexer metadata not matching")
            if idx_name_list - new_idx_name_list:
                self.fail(f"Some index/es is/are missing after Scale up: {idx_name_list - new_idx_name_list}")

    def test_continous_scaling(self):
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template=self.json_template)
        select_queries = set()
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        for _ in range(self.initial_index_batches):
            query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
            for namespace in self.namespaces:
                select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                           namespace=namespace))
                queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                  namespace=namespace,
                                                                  num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
        self.wait_until_indexes_online()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for next_compute in CAPELLA_AWS_COMPUTES_ORDER[1:]:
            if 'm5' not in next_compute:
                continue

            self.dataload_till_rr(namespaces=self.namespaces, rr=self.resident_ratio, json_template=self.json_template)

            # Todo: Temporary; to be removed after CP changes
            percentage_quota_setting = {"indexer.settings.percentage_memory_quota": 80}
            ddl_during_scale_up_enabled = {"indexer.allowDDLDuringScaleUp": True}
            index_node = index_nodes[0]
            index_rest = RestConnection(index_node)
            self.capella_api.set_capella_index_settings(node=index_node.ip, setting=percentage_quota_setting,
                                                        username=index_node.rest_username,
                                                        password=index_node.rest_password)
            self.capella_api.set_capella_index_settings(node=index_node.ip, setting=ddl_during_scale_up_enabled,
                                                        username=index_node.rest_username,
                                                        password=index_node.rest_password)
            metadata_before_scale_up = index_rest.get_indexer_metadata()['status']
            idx_name_list = {idx['name'] for idx in metadata_before_scale_up}

            self.capella_api.scale_up_capella_nodes(services=self.scale_up_services, cluster_id=self.cluster_id)
            self.sleep(5)
            with ThreadPoolExecutor() as executor:
                monitor_task = executor.submit(self.monitor_scale_up, select_queries=select_queries,
                                               query_node=query_node)
                task = executor.submit(self.capella_api.wait_for_cluster, cluster_id=self.cluster_id, sleep_timer=60)
                # Wait for cluster to stable
                task.result()

                # Todo: Temporary; to be removed after CP changes
                percentage_quota_setting = {"indexer.settings.percentage_memory_quota": 0}
                ddl_during_scale_up_disabled = {"indexer.allowDDLDuringScaleUp": False}
                index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                index_rest = RestConnection(index_nodes[0])
                self.capella_api.set_capella_index_settings(node=index_nodes[0].ip, setting=percentage_quota_setting,
                                                            username=index_nodes[0].rest_username,
                                                            password=index_nodes[0].rest_password)
                self.capella_api.set_capella_index_settings(node=index_nodes[0].ip, setting=ddl_during_scale_up_disabled,
                                                            username=index_nodes[0].rest_username,
                                                            password=index_nodes[0].rest_password)

                monitor_task.result()

                self.log.info(f"New Indexer nodes: {index_nodes}")
                metadata_after_scale_up = index_rest.get_indexer_metadata()['status']
                new_idx_name_list = {idx['name'] for idx in metadata_after_scale_up}
                self.assertEqual(len(metadata_before_scale_up), len(metadata_after_scale_up),
                                 "Indexer metadata not matching")
                if idx_name_list - new_idx_name_list:
                    self.fail(f"Some index/es is/are missing after Scale up: {idx_name_list - new_idx_name_list}")