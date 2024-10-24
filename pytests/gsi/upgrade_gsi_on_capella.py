"""
upgrade_gsi_on_capella.py:

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = 08/08/23 1:49 pm

"""
import json
from concurrent.futures import ThreadPoolExecutor
from couchbase_helper.documentgenerator import SDKDataLoader
from membase.api.rest_client import RestConnection

from .base_gsi import BaseSecondaryIndexingTests


class UpgradeGSIOnCapella(BaseSecondaryIndexingTests):
    def setUp(self):
        super(UpgradeGSIOnCapella, self).setUp()
        self.log.info("==============  UpgradeGSIOnCapella setup has started ==============")
        buckets = self.rest.get_buckets()
        if buckets:
            for bucket in buckets:
                RestConnection.delete_bucket(self, bucket=bucket.name)
        self.count_query = "SELECT COUNT(*) FROM {} WHERE price IS NOT NULL"
        self.log.info("==============  UpgradeGSIOnCapella setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  UpgradeGSIOnCapella tearDown has started ==============")
        self.log.info("==============  UpgradeGSIOnCapella tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  UpgradeGSIOnCapella tearDown has started ==============")
        super(UpgradeGSIOnCapella, self).tearDown()
        self.log.info("==============  UpgradeGSIOnCapella tearDown has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  UpgradeGSIOnCapella suite_setup has started ==============")
        self.log.info("==============  UpgradeGSIOnCapella suite_setup has completed ==============")

    def validate_index_master(self):
        out_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        out_kv_nodes = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)
        out_n1ql_nodes = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)
        index_nodes_ip = [node.ip for node in out_index_nodes]
        kv_nodes_ip = [node.ip for node in out_kv_nodes]
        n1ql_nodes_ip = [node.ip for node in out_n1ql_nodes]
        index_node = None
        replacement_order = self.capella_api.get_replacement_order(cluster_id=self.cluster_id)
        index_nodes_replacement_order = []
        for node in replacement_order:
            if node in index_nodes_ip:
                index_nodes_replacement_order.append(node)
        for idx_node in out_index_nodes:
            if idx_node.ip == index_nodes_replacement_order[-1]:
                index_node = idx_node
                break
        index_rest = RestConnection(index_node)
        # self.log.info("Waiting for Index Rebalance to start")
        for node in replacement_order:
            while self.rest._rebalance_progress_status() != 'running':
                continue
            if node in kv_nodes_ip:
                self.log.info(f"Upgrading KV node {node}")
            elif node in index_nodes_ip:
                self.log.info(f"Upgrading Index node {node}")
                status = True
                if status:
                    count = 0
                    while count < 120:
                        rebalance_tokens = index_rest.list_indexer_rebalance_tokens(server=index_node)
                        self.sleep(1, 'waiting for tokens')
                        if rebalance_tokens is None or 'rebalancetoken' not in rebalance_tokens:
                            count = count + 1
                        else:
                            break
                    if count >= 120:
                        self.fail('Did not get the params rebalance token')
                parsed_output = json.loads(rebalance_tokens)
                index_rebalance_master = parsed_output['rebalancetoken']['MasterIP']
                self.log.info(f"New Rebalance Master is :{index_rebalance_master}")
                if index_rebalance_master in index_nodes_ip:
                    self.fail("Higher version Index node is not selected as rebalance master")
            elif node in n1ql_nodes_ip:
                self.log.info(f"Upgrading N1QL node {node}")
            else:
                self.log.info("Upgrading other services")
            while self.rest._rebalance_progress_status() == 'running':
                continue

    def post_upgrade_operations(self, select_queries):
        # Running select queries
        query_node = self.get_nodes_from_services_map(service_type="n1ql")
        tasks = self.gsi_util_obj.aysnc_run_select_queries(select_queries=select_queries,
                                                           scan_consistency='request_plus',
                                                           query_node=query_node)
        for task in tasks:
            task.result()

        # creating few Indexes
        for namespace in self.namespaces:
            query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace, defer_build_mix=True,
                                                              num_replica=self.num_index_replica)
            self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)

            _, keyspace = namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            self.gen_create = SDKDataLoader(num_ops=self.num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope,
                                            collection=collection, json_template=self.json_template,
                                            output=True, username=self.username, password=self.password,
                                            workers=5)
            task = self.cluster.async_load_gen_docs(self.master, bucket=bucket,
                                                    generator=self.gen_create, pause_secs=1,
                                                    timeout_secs=300)
            task.result()

            # Validating num docs for each collection
            for namespace in self.namespaces:
                query = self.count_query.format(namespace)
                result = self.run_cbq_query(query=query, server=query_node,
                                            scan_consistency='request_plus')['results'][0]['$1']
                self.assertEqual(result, self.num_of_docs_per_collection * 2)

    def test_upgrade_nodes(self):
        self.update_master_node()
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
        for namespace in self.namespaces:
            query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
            select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                       namespace=namespace))
            queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                              namespace=namespace,
                                                              num_replica=self.num_index_replica)
            self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
        self.wait_until_indexes_online()
        self.dataload_till_rr(namespaces=self.namespaces, rr=self.resident_ratio, json_template=self.json_template)

        # Validating num docs for each collection
        for namespace in self.namespaces:
            query = self.count_query.format(namespace)
            result = self.run_cbq_query(query=query, server=query_node)['results'][0]['$1']
            self.assertEqual(result, self.num_of_docs_per_collection)

        # Starting cluster upgrade
        self.capella_api.upgrade_cluster(cluster_id=self.cluster_id, ami_version=self.upgrade_ami,
                                         wait_for_healthy_cluster=False)
        with ThreadPoolExecutor() as executor:
            task = executor.submit(self.validate_index_master)
            executor.submit(self.capella_api.wait_for_cluster, cluster_id=self.cluster_id, sleep_timer=60, timeout=3600)
            task.result()
            # Todo:
            # Add a method to generate KV mutation and Index scans. As nodes are going out Rest connections would fails
            # so we need to update server list and maintain new connections
        self.servers = self.capella_api.get_nodes_formatted(cluster_id=self.cluster_id, username=self.username,
                                                            password=self.password)
        self.update_master_node()
        self.post_upgrade_operations(select_queries=select_queries)
