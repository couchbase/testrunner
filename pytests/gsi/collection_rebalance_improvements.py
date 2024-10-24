"""
collection_rebalance_improvements.py: These test validate the index build improvement during rebalance.
https://issues.couchbase.com/browse/MB-57057

__author__ = "Yash Dodderi"
__maintainer = "Yash Dodderi"
__email__ = "yash.dodderi@couchbase.com"
__git_user__ = "yash-dodderi7"
__created_on__ = 17/07/23 4:24 pm

"""

from membase.api.capella_rest_client import RestConnection as RestConnectionCapella
from membase.api.rest_client import RestConnection
from .base_gsi import BaseSecondaryIndexingTests
from couchbase_helper.documentgenerator import SDKDataLoader
import random
import string
from concurrent.futures import ThreadPoolExecutor
from remote.remote_util import RemoteMachineShellConnection
from threading import Thread
from random import randrange


class RebalanceImprovement(BaseSecondaryIndexingTests):
    def setUp(self):
        super(RebalanceImprovement, self).setUp()
        self.log.info("==============  RebalanceImprovement setup has started ==============")
        if self.capella_run:
            buckets = self.rest.get_buckets()
            if buckets:
                for bucket in buckets:
                    RestConnectionCapella.delete_bucket(self, bucket=bucket.name)

        else:
            self.rest.delete_all_buckets()
        self.password = self.input.membase_settings.rest_password
        self.buckets = self.rest.get_buckets()
        if not self.capella_run:
            self._create_server_groups()
            self.cb_version = float(self.cb_version.split('-')[0][0:3])
            self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                                bucket_params=self.bucket_params)
        self.run_continous_query = False
        self.failover_method = self.input.param("failover_method", "cancel_rebalance")
        self.num_index_batches = self.input.param("num_index_batches", 1)
        self.num_of_docs_per_collection_mutation = self.input.param("num_of_docs_per_collection_mutation", 1000)
        self.query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)[0]
        self.log.info("==============  RebalanceImprovement setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  RebalanceImprovement tearDown has started ==============")
        self.log.info("==============  RebalanceImprovement tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  RebalanceImprovement tearDown has started ==============")
        super(RebalanceImprovement, self).tearDown()
        self.log.info("==============  RebalanceImprovement tearDown has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  RebalanceImprovement suite_setup has started ==============")
        self.log.info("==============  RebalanceImprovement suite_setup has completed ==============")

    def _run_queries_continously(self, select_queries, scan_consistency=None):
        while self.run_continous_query:
            tasks_list = self.gsi_util_obj.aysnc_run_select_queries(select_queries=select_queries,
                                                                    query_node=self.query_node,
                                                                    scan_consistency=scan_consistency)

            for task in tasks_list:
                task.result()

    def kv_mutations(self, num_of_docs_per_collection=1000):
        task_list = []
        for namespace in self.namespaces:
            _, keyspace = namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            key_prefix = 'doc_' + "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
            self.gen_create = SDKDataLoader(num_ops=num_of_docs_per_collection, percent_create=100,
                                            percent_update=0, percent_delete=0, scope=scope,
                                            collection=collection, json_template='Hotel', key_prefix=key_prefix,
                                            output=True)
            task = self.cluster.async_load_gen_docs(self.master, bucket=bucket,
                                                    generator=self.gen_create, pause_secs=1,
                                                    timeout_secs=300, use_magma_loader=True)
            task_list.append(task)

        for task in task_list:
            task.result()

    def gen_num_of_indexed_docs(self):
        no_of_docs_indexed = []
        for namespace in self.namespaces:
            _, keyspace = namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            price_count_query_before_rebalance = f'SELECT COUNT(*) FROM `{bucket}`.`{scope}`.`{collection}` WHERE price is NOT NULL'
            value = self.run_cbq_query(query=price_count_query_before_rebalance, scan_consistency='request_plus')
            no_of_docs_indexed.append(value['results'][0]['$1'])
        return no_of_docs_indexed

    def verify_index_distribution(self, indexer_metadata):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        index_map = {key.ip: [] for key in index_nodes}
        for data in indexer_metadata:
            index_on_node = data['hosts'][0].split(':')[0]
            if index_on_node not in index_map:
                index_map[index_on_node] = [data['name']]
            else:
                index_map[index_on_node].append(data['name'])
        for node in index_map:
            self.assertGreater(len(index_map[node]), 0, f'index not present in nodes : {node}')

    def validate_batch_size_and_queries(self, node=None, index_nodes_b4_rebalance=None, rebalance_fail=False):
        if rebalance_fail:
            try:
                while self.rest._rebalance_progress_status() != 'running':
                    continue
            except Exception as err:
                self.log.info(f'Error : {err}')
        else:
            while self.rest._rebalance_progress_status() != 'running':
                continue

        if node is None:
            self.servers = self.capella_api.get_nodes_formatted(cluster_id=self.cluster_id, username=self.username,
                                                                password=self.password)
            index_nodes_aft_reb = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
            b4_idx_nodes_ip = {node.ip for node in index_nodes_b4_rebalance}
            aft_idx_nodes_ip = {node.ip for node in index_nodes_aft_reb}
            node_ip = list(aft_idx_nodes_ip - b4_idx_nodes_ip)[0]
            for node in index_nodes_aft_reb:
                if node.ip == node_ip:
                    break
            else:
                self.fail(f"No new index node added to the cluster.{index_nodes_aft_reb}")
        while self.rest._rebalance_progress_status() == 'running':
            index_rest_out = RestConnection(node)
            stat_data_out = index_rest_out.get_index_stats()
            metadata_out = index_rest_out.get_indexer_metadata()['status']
            num_scans = {key: value for key, value in stat_data_out.items() if key.lower().endswith('num_requests')}
            indexes_building = {key: value for key, value in stat_data_out.items() if
                                key.lower().endswith('index_state')}
            for index, num_requests in num_scans.items():
                if num_requests > 0:
                    if self.num_collections > 1 or self.num_scopes > 1:
                        self.run_continous_query = False
                    self.log.warning(
                        f'Node {node} is serving scans when its getting rebalanced in index : {index} num scans : {num_requests}')
                    if self.rest._rebalance_progress_status() == 'running':
                        self.sleep(60)
                        if self.rest._rebalance_progress_status() == 'running':
                            self.log.warning(f'Node {node} is serving scans when its getting rebalanced in index : {index} num scans : {num_requests}')
            namespaces = set()
            index_names_list = []
            for index in indexes_building:
                if indexes_building[index] == 2:
                    split_key = index.split(':')
                    namespace = f'{split_key[0]}.{split_key[1]}.{split_key[2]}'
                    index_name = f'{split_key[3]}'
                    # self.log.info(f'namespaces : {namespace}')
                    # self.log.info(f'index name list : {index_names_list}')
                    namespaces.add(namespace)
                    index_names_list.append(index_name)
            if len(list(namespaces)) > 1 and len(index_names_list) > 20:
                if self.num_collections > 1 or self.num_scopes > 1:
                    self.run_continous_query = False

                self.fail(
                    f'namespace : {list(namespaces)} index_names_list : {index_names_list}  index meta data : {metadata_out}')

    def validate_scans_post_rebalance(self, select_queries_list, node, batches=1, query_node=None):
        
        self.log.info('Running scans post rebalance')
        for batch in range(batches):
            tasks = self.gsi_util_obj.aysnc_run_select_queries(select_queries=select_queries_list,
                                                               query_node=query_node,
                                                               scan_consistency="request_plus")

            for task in tasks:
                task.result()

        index_rest_out = RestConnection(node)
        stat_data_out = index_rest_out.get_index_stats()
        num_scans = {key: value for key, value in stat_data_out.items() if key.lower().endswith('num_requests')}
        no_index_scan_count = 0
        for index, num_requests in num_scans.items():
            if num_requests > 0:
                self.log.info(f'index : {index} num scans : {num_requests}')
                no_index_scan_count += 1
        if no_index_scan_count == 0:
            self.fail(f'Node {node} is not serving scans post rebalanced')

    def test_swap_rebalance_by_one(self):
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        select_queries_list = []

        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
                select_queries = self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                      namespace=namespace, limit=10)
                for query in select_queries:
                    select_queries_list.append(query)
        select_queries_list = list(set(select_queries_list))
        self.wait_until_indexes_online()
        for namespace in self.namespaces:
            deffered_index_query = f"CREATE INDEX `idx_deffered` ON {namespace}(url) USING GSI  WITH {{'defer_build': True, 'num_replica': 1}}"
            self.run_cbq_query(query=deffered_index_query)
        indexder_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']
        no_of_docs_indexed_before_rebalanced = self.gen_num_of_indexed_docs()

        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        services_in = ['index']
        self.log.info(f'nodes out list : {self.nodes_out_list}')
        mutation_batch = 0
        for idx, index_node in enumerate(index_nodes):
            out_node = index_node
            self.log.info(f'node being balanced out {out_node}')
            in_node = self.servers[self.nodes_init + idx]
            mutation_batch += 1
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [in_node],
                                                     [out_node], services=services_in)
            self.run_continous_query = True
            thread = Thread(target=self._run_queries_continously, args=[select_queries_list, 'request_plus'])
            kv_mutations_thread = Thread(target=self.kv_mutations, args=[self.num_of_docs_per_collection_mutation])
            thread.start()
            kv_mutations_thread.start()
            self.validate_batch_size_and_queries(node=in_node)

            rebalance.result()
            self.run_continous_query = False
            self.sleep(10)
            self.validate_scans_post_rebalance(select_queries_list=select_queries_list, node=in_node)
        self.sleep(60)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        new_index_rest = RestConnection(index_node)
        no_of_docs_indexed_after_rebalanced = self.gen_num_of_indexed_docs()
        indexder_metadata_after_rebalance = new_index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexder_metadata_before_rebalance), len(indexder_metadata_after_rebalance),
                         'Probable drop of some indexes')

        for i in range(len(no_of_docs_indexed_before_rebalanced)):
            expected_doc_count = no_of_docs_indexed_before_rebalanced[
                                     i] + mutation_batch * self.num_of_docs_per_collection_mutation
            self.assertEqual(no_of_docs_indexed_after_rebalanced[i], expected_doc_count,
                             'mutations are not reflected in the indexes')
        self.verify_index_distribution(indexer_metadata=indexder_metadata_after_rebalance)

    def test_rebalance_in_one(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.index_rest.set_index_settings(redistribute)
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        select_queries_list = []

        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
                select_queries = self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                      namespace=namespace, limit=10)

                for query in select_queries:
                    select_queries_list.append(query)
        select_queries_list = list(set(select_queries_list))
        self.wait_until_indexes_online()
        for namespace in self.namespaces:
            deffered_index_query = f"CREATE INDEX `idx_deffered` ON {namespace}(url) USING GSI  WITH {{'defer_build': True, 'num_replica': 1}}"
            self.run_cbq_query(query=deffered_index_query)
        indexder_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']
        no_of_docs_indexed_before_rebalanced = self.gen_num_of_indexed_docs()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        services_in = ['index']
        mutation_batch = 0
        for idx, index_node in enumerate(index_nodes):
            in_node = self.servers[self.nodes_init + idx]
            self.log.info(f'node being balanced out {in_node}')
            mutation_batch += 1
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [in_node],
                                                     [], services=services_in)
            self.run_continous_query = True
            thread = Thread(target=self._run_queries_continously, args=[select_queries_list, 'request_plus'])
            kv_mutations_thread = Thread(target=self.kv_mutations, args=[self.num_of_docs_per_collection_mutation])
            thread.start()
            kv_mutations_thread.start()
            self.validate_batch_size_and_queries(node=in_node)

            rebalance.result()
            self.run_continous_query = False
            self.sleep(10)
            self.validate_scans_post_rebalance(select_queries_list=select_queries_list, node=in_node)
        self.sleep(60)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        new_index_rest = RestConnection(index_node)
        no_of_docs_indexed_after_rebalanced = self.gen_num_of_indexed_docs()
        indexder_metadata_after_rebalance = new_index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexder_metadata_before_rebalance), len(indexder_metadata_after_rebalance),
                         'Probable drop of some indexes')
        #self.get_cbcollect_info(self.servers)
        for i in range(len(no_of_docs_indexed_before_rebalanced)):
            expected_doc_count = no_of_docs_indexed_before_rebalanced[
                                     i] + mutation_batch * self.num_of_docs_per_collection_mutation
            self.assertEqual(no_of_docs_indexed_after_rebalanced[i], expected_doc_count,
                             'mutations are not reflected in the indexes')
        self.verify_index_distribution(indexer_metadata=indexder_metadata_after_rebalance)

    def test_replica_repair_swap_rebalance_node(self):
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        select_queries_list = []

        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
                select_queries = self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                      namespace=namespace, limit=10)

                for query in select_queries:
                    select_queries_list.append(query)
        select_queries_list = list(set(select_queries_list))
        self.wait_until_indexes_online()
        for namespace in self.namespaces:
            deffered_index_query = f"CREATE INDEX `idx_deffered` ON {namespace}(url) USING GSI  WITH {{'defer_build': True, 'num_replica': 1}}"
            self.run_cbq_query(query=deffered_index_query)
        mutation_batch = 1
        indexder_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']
        no_of_docs_indexed_before_rebalanced = self.gen_num_of_indexed_docs()
        services_in = ['index']
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        replica_repair_enable = {"indexer.rebalance.disable_replica_repair": True}
        self.index_rest.set_index_settings(replica_repair_enable)
        index_node_A = index_nodes[0]
        index_node_B = index_nodes[1]
        no_of_indexes_node_A_before = len(RestConnection(index_node_A).get_indexer_metadata()['status'])

        rebalance_out_B = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                       [index_node_B], services=services_in)
        rebalance_out_B.result()
        no_of_indexes_node_A_after = len(RestConnection(index_node_A).get_indexer_metadata()['status'])
        self.assertLess(no_of_indexes_node_A_after, no_of_indexes_node_A_before,
                        'Replica indexes were not dropped inspite of dropping a node')
        index_node_C = self.servers[self.nodes_init + 0]
        index_node_D = self.servers[self.nodes_init + 1]

        rebalance_in_C = self.cluster.async_rebalance(self.servers[:self.nodes_init], [index_node_C],
                                                      [], services=services_in)
        rebalance_in_C.result()
        replica_repair_disable = {"indexer.rebalance.disable_replica_repair": False}
        self.index_rest.set_index_settings(replica_repair_disable)
        index_node_rest_A = RestConnection(index_node_A)
        index_node_rest_C = RestConnection(index_node_C)
        index_setting_A = "excludeNode=in"
        index_setting_C = "excludeNode=out"
        index_node_rest_A.set_index_planner_settings(index_setting_A)
        index_node_rest_C.set_index_planner_settings(index_setting_C)

        final_swap_rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [index_node_D],
                                                            [index_node_A], services=services_in)
        self.run_continous_query = True
        thread = Thread(target=self._run_queries_continously, args=[select_queries_list, 'request_plus'])
        kv_mutations_thread = Thread(target=self.kv_mutations, args=[self.num_of_docs_per_collection_mutation])
        thread.start()
        kv_mutations_thread.start()
        self.validate_batch_size_and_queries(node=index_node_D)

        final_swap_rebalance.result()
        self.run_continous_query = False
        self.sleep(10)
        self.validate_scans_post_rebalance(select_queries_list=select_queries_list, node=index_node_D)
        self.sleep(60)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        new_index_rest = RestConnection(index_node)
        no_of_docs_indexed_after_rebalanced = self.gen_num_of_indexed_docs()
        indexder_metadata_after_rebalance = new_index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexder_metadata_before_rebalance), len(indexder_metadata_after_rebalance),
                         'Probable drop of some indexes')
        for i in range(len(no_of_docs_indexed_before_rebalanced)):
            expected_doc_count = no_of_docs_indexed_before_rebalanced[
                                     i] + mutation_batch * self.num_of_docs_per_collection_mutation
            self.assertEqual(no_of_docs_indexed_after_rebalanced[i], expected_doc_count,
                             'mutations are not reflected in the indexes')
        self.verify_index_distribution(indexer_metadata=indexder_metadata_after_rebalance)

    def test_replica_repair_rebalance_in(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.index_rest.set_index_settings(redistribute)
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        select_queries_list = []

        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
                select_queries = self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                      namespace=namespace, limit=10)

                for query in select_queries:
                    select_queries_list.append(query)
        select_queries_list = list(set(select_queries_list))
        self.wait_until_indexes_online()
        for namespace in self.namespaces:
            deffered_index_query = f"CREATE INDEX `idx_deffered` ON {namespace}(url) USING GSI  WITH {{'defer_build': True, 'num_replica': 1}}"
            self.run_cbq_query(query=deffered_index_query)
        indexder_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']
        no_of_docs_indexed_before_rebalanced = self.gen_num_of_indexed_docs()
        services_in = ['index']
        mutation_batch = 1
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        replica_repair_enable = {"indexer.rebalance.disable_replica_repair": True}
        self.index_rest.set_index_settings(replica_repair_enable)
        index_node_A = index_nodes[0]
        index_node_B = index_nodes[1]
        no_of_indexes_node_A_before = len(RestConnection(index_node_A).get_indexer_metadata()['status'])

        rebalance_out_B = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                       [index_node_B], services=services_in)
        rebalance_out_B.result()
        no_of_indexes_node_A_after = len(RestConnection(index_node_A).get_indexer_metadata()['status'])
        self.assertLess(no_of_indexes_node_A_after, no_of_indexes_node_A_before,
                        'Replica indexes were not dropped inspite of dropping a node')

        index_node_C = self.servers[self.nodes_init + 0]
        index_node_D = self.servers[self.nodes_init + 1]

        rebalance_in_C = self.cluster.async_rebalance(self.servers[:self.nodes_init], [index_node_C],
                                                      [], services=services_in)
        rebalance_in_C.result()
        replica_repair_disable = {"indexer.rebalance.disable_replica_repair": False}
        self.index_rest.set_index_settings(replica_repair_disable)
        index_node_rest_A = RestConnection(index_node_A)
        index_node_rest_C = RestConnection(index_node_C)
        index_setting_A = "excludeNode=in"
        index_setting_C = "excludeNode=out"
        index_node_rest_A.set_index_planner_settings(index_setting_A)
        index_node_rest_C.set_index_planner_settings(index_setting_C)

        final_rebalance_in = self.cluster.async_rebalance(self.servers[:self.nodes_init], [index_node_D],
                                                          [], services=services_in)
        self.run_continous_query = True
        thread = Thread(target=self._run_queries_continously, args=[select_queries_list, 'request_plus'])
        kv_mutations_thread = Thread(target=self.kv_mutations, args=[self.num_of_docs_per_collection_mutation])
        thread.start()
        kv_mutations_thread.start()
        self.validate_batch_size_and_queries(node=index_node_D)

        final_rebalance_in.result()
        self.run_continous_query = False
        self.sleep(10)
        self.validate_scans_post_rebalance(select_queries_list=select_queries_list, node=index_node_D)
        self.sleep(60)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        new_index_rest = RestConnection(index_node)
        no_of_docs_indexed_after_rebalanced = self.gen_num_of_indexed_docs()
        indexder_metadata_after_rebalance = new_index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexder_metadata_before_rebalance), len(indexder_metadata_after_rebalance),
                         'Probable drop of some indexes')
        for i in range(len(no_of_docs_indexed_before_rebalanced)):
            expected_doc_count = no_of_docs_indexed_before_rebalanced[
                                     i] + mutation_batch * self.num_of_docs_per_collection_mutation
            self.assertEqual(no_of_docs_indexed_after_rebalanced[i], expected_doc_count,
                             'mutations are not reflected in the indexes')
        self.verify_index_distribution(indexer_metadata=indexder_metadata_after_rebalance)

    def test_rebalance_in_out_one(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": False}
        self.index_rest.set_index_settings(redistribute)
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        select_queries_list = []

        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
                select_queries = self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                      namespace=namespace, limit=10)

                for query in select_queries:
                    select_queries_list.append(query)
        select_queries_list = list(set(select_queries_list))
        self.wait_until_indexes_online()
        for namespace in self.namespaces:
            deffered_index_query = f"CREATE INDEX `idx_deffered` ON {namespace}(url) USING GSI  WITH {{'defer_build': True, 'num_replica': 1}}"
            self.run_cbq_query(query=deffered_index_query)
        indexder_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']
        no_of_docs_indexed_before_rebalanced = self.gen_num_of_indexed_docs()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        services_in = ['index']
        self.log.info(f'nodes out list : {self.nodes_out_list}')
        mutation_batch = 0
        for idx, index_node in enumerate(index_nodes):
            out_node = index_node
            self.log.info(f'node being balanced out {out_node}')
            in_node = self.servers[self.nodes_init + idx]
            mutation_batch += 1
            rebalance_in = self.cluster.async_rebalance(self.servers[:self.nodes_init], [in_node],
                                                        [], services=services_in)
            rebalance_in.result()
            rebalance_out = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                         [out_node], services=services_in)

            self.run_continous_query = True
            thread = Thread(target=self._run_queries_continously, args=[select_queries_list, 'request_plus'])
            kv_mutations_thread = Thread(target=self.kv_mutations, args=[self.num_of_docs_per_collection_mutation])
            thread.start()
            kv_mutations_thread.start()
            self.validate_batch_size_and_queries(node=in_node)

            rebalance_out.result()
            self.run_continous_query = False
            self.sleep(10)
            self.validate_scans_post_rebalance(select_queries_list=select_queries_list, node=in_node)
        self.sleep(60)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        new_index_rest = RestConnection(index_node)
        no_of_docs_indexed_after_rebalanced = self.gen_num_of_indexed_docs()
        indexder_metadata_after_rebalance = new_index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexder_metadata_before_rebalance), len(indexder_metadata_after_rebalance),
                         'Probable drop of some indexes')
        for i in range(len(no_of_docs_indexed_before_rebalanced)):
            expected_doc_count = no_of_docs_indexed_before_rebalanced[
                                     i] + mutation_batch * self.num_of_docs_per_collection_mutation
            self.assertEqual(no_of_docs_indexed_after_rebalanced[i], expected_doc_count,
                             'mutations are not reflected in the indexes')
        self.verify_index_distribution(indexer_metadata=indexder_metadata_after_rebalance)

    def test_fail_swap_rebalance(self):
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        select_queries_list = []

        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
                select_queries = self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                      namespace=namespace, limit=10)

                for query in select_queries:
                    select_queries_list.append(query)
        select_queries_list = list(set(select_queries_list))
        self.wait_until_indexes_online()
        for namespace in self.namespaces:
            deffered_index_query = f"CREATE INDEX `idx_deffered` ON {namespace}(url) USING GSI  WITH {{'defer_build': True, 'num_replica': 1}}"
            self.run_cbq_query(query=deffered_index_query)
        indexder_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']
        no_of_docs_indexed_before_rebalanced = self.gen_num_of_indexed_docs()

        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        services_in = ['index']
        self.log.info(f'nodes out list : {self.nodes_out_list}')
        mutation_batch = 0
        for idx, index_node in enumerate(index_nodes):
            out_node = index_node
            remote = RemoteMachineShellConnection(out_node)
            self.log.info(f'node being balanced out {out_node}')
            in_node = self.servers[self.nodes_init + idx]
            mutation_batch += 1
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [in_node],
                                                     [out_node], services=services_in)
            while self.rest._rebalance_progress_status() != 'running':
                continue
            self.run_continous_query = True
            with ThreadPoolExecutor() as excecutor:
                task = excecutor.submit(self.validate_batch_size_and_queries, node=in_node, rebalance_fail=True)
                self.sleep(10)
                if self.failover_method == "index_kill":
                    for i in range(10):
                        self.log.info('index process stopped')
                        remote.terminate_process(process_name="indexer")
                else:
                    self.rest.stop_rebalance()
                excecutor.submit(self._run_queries_continously, select_queries=select_queries_list,
                                 scan_consistency='request_plus')
                excecutor.submit(self.kv_mutations, num_of_docs_per_collection=self.num_of_docs_per_collection_mutation)

                err = None
                try:
                    rebalance.result()
                except Exception as ex:
                    err = str(ex)
                    self.run_continous_query = False
                    self.log.info(err)
                self.run_continous_query = False
                if err is None and self.failover_method != "cancel_rebalance":
                    self.fail('rebalance could not be failed')
                try:
                    task.result()
                except Exception as e:
                    self.fail(f'test failed due to {str(e)}')

            self.sleep(75)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                     [out_node], services=services_in)
            rebalance.result()
            self.sleep(10)
            self.validate_scans_post_rebalance(select_queries_list=select_queries_list, node=in_node)
        self.sleep(60)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        new_index_rest = RestConnection(index_node)
        no_of_docs_indexed_after_rebalanced = self.gen_num_of_indexed_docs()
        indexder_metadata_after_rebalance = new_index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexder_metadata_before_rebalance), len(indexder_metadata_after_rebalance),
                         'Probable drop of some indexes')

        for i in range(len(no_of_docs_indexed_before_rebalanced)):
            expected_doc_count = no_of_docs_indexed_before_rebalanced[
                                     i] + mutation_batch * self.num_of_docs_per_collection_mutation
            self.assertEqual(no_of_docs_indexed_after_rebalanced[i], expected_doc_count,
                             'mutations are not reflected in the indexes')
        self.verify_index_distribution(indexer_metadata=indexder_metadata_after_rebalance)

    def test_fail_rebalance_in(self):
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.index_rest.set_index_settings(redistribute)
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        select_queries_list = []

        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
                select_queries = self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                      namespace=namespace, limit=10)

                for query in select_queries:
                    select_queries_list.append(query)
        select_queries_list = list(set(select_queries_list))
        self.wait_until_indexes_online()
        for namespace in self.namespaces:
            deffered_index_query = f"CREATE INDEX `idx_deffered` ON {namespace}(url) USING GSI  WITH {{'defer_build': True, 'num_replica': 1}}"
            self.run_cbq_query(query=deffered_index_query)
        indexder_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']
        no_of_docs_indexed_before_rebalanced = self.gen_num_of_indexed_docs()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        services_in = ['index']
        mutation_batch = 0
        for idx, index_node in enumerate(index_nodes):
            out_node = index_node
            remote = RemoteMachineShellConnection(out_node)
            in_node = self.servers[self.nodes_init + idx]
            self.log.info(f'node being balanced out {in_node}')
            mutation_batch += 1
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [in_node],
                                                     [], services=services_in)
            while self.rest._rebalance_progress_status() != 'running':
                continue
            self.run_continous_query = True
            with ThreadPoolExecutor() as excecutor:
                task = excecutor.submit(self.validate_batch_size_and_queries, node=in_node, rebalance_fail=True)
                self.sleep(10)
                if self.failover_method == "index_kill":
                    for i in range(10):
                        self.log.info('index process stopped')
                        remote.terminate_process(process_name="indexer")
                else:
                    self.rest.stop_rebalance()
                excecutor.submit(self._run_queries_continously, select_queries=select_queries_list,
                                 scan_consistency='request_plus')
                excecutor.submit(self.kv_mutations, num_of_docs_per_collection=self.num_of_docs_per_collection_mutation)
                err = None
                try:
                    rebalance.result()
                except Exception as ex:
                    err = str(ex)
                    self.run_continous_query = False
                    self.log.info(err)
                self.run_continous_query = False
                if err is None and self.failover_method != "cancel_rebalance":
                    self.fail('rebalance could not be failed')
                try:
                    task.result()
                except Exception as e:
                    if 'is serving scans when its getting rebalanced in index' in str(e):
                        self.fail(f'test failed due to {str(e)}')

            self.sleep(75)
            rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                     [], services=services_in)
            rebalance.result()
            self.sleep(10)
            self.validate_scans_post_rebalance(select_queries_list=select_queries_list, node=in_node)
        self.sleep(60)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        new_index_rest = RestConnection(index_node)
        no_of_docs_indexed_after_rebalanced = self.gen_num_of_indexed_docs()
        indexder_metadata_after_rebalance = new_index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexder_metadata_before_rebalance), len(indexder_metadata_after_rebalance),
                         'Probable drop of some indexes')
        for i in range(len(no_of_docs_indexed_before_rebalanced)):
            expected_doc_count = no_of_docs_indexed_before_rebalanced[
                                     i] + mutation_batch * self.num_of_docs_per_collection_mutation
            self.assertEqual(no_of_docs_indexed_after_rebalanced[i], expected_doc_count,
                             'mutations are not reflected in the indexes')
        self.verify_index_distribution(indexer_metadata=indexder_metadata_after_rebalance)

    def test_fail_rebalance_in_out(self):
        self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             json_template='Hotel')
        select_queries_list = []

        for namespace in self.namespaces:
            for index in range(self.num_index_batches):
                prefix = f"idx_{index}"
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition(index_name_prefix=prefix)
                create_queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                         namespace=namespace,
                                                                         num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=create_queries, database=namespace)
                select_queries = self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                      namespace=namespace, limit=10)

                for query in select_queries:
                    select_queries_list.append(query)
        select_queries_list = list(set(select_queries_list))
        self.wait_until_indexes_online()
        for namespace in self.namespaces:
            deffered_index_query = f"CREATE INDEX `idx_deffered` ON {namespace}(url) USING GSI  WITH {{'defer_build': True, 'num_replica': 1}}"
            self.run_cbq_query(query=deffered_index_query)
        indexder_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']
        no_of_docs_indexed_before_rebalanced = self.gen_num_of_indexed_docs()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        services_in = ['index']
        self.log.info(f'nodes out list : {self.nodes_out_list}')
        mutation_batch = 0
        for idx, index_node in enumerate(index_nodes):
            out_node = index_node
            remote = RemoteMachineShellConnection(out_node)
            self.log.info(f'node being balanced out {out_node}')
            in_node = self.servers[self.nodes_init + idx]
            mutation_batch += 1
            rebalance_in = self.cluster.async_rebalance(self.servers[:self.nodes_init], [in_node],
                                                        [], services=services_in)
            rebalance_in.result()
            rebalance_out = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                         [out_node], services=services_in)
            while self.rest._rebalance_progress_status() != 'running':
                continue
            self.run_continous_query = True
            with ThreadPoolExecutor() as excecutor:
                task = excecutor.submit(self.validate_batch_size_and_queries, node=in_node, rebalance_fail=True)
                self.sleep(10)
                if self.failover_method == "index_kill":
                    for i in range(10):
                        self.log.info('index process stopped')
                        remote.terminate_process(process_name="indexer")
                else:
                    self.rest.stop_rebalance()
                excecutor.submit(self._run_queries_continously,select_queries=select_queries_list, scan_consistency='request_plus')
                excecutor.submit(self.kv_mutations,num_of_docs_per_collection=self.num_of_docs_per_collection_mutation)
                err = None
                try:
                    rebalance_out.result()
                except Exception as ex:
                    err = str(ex)
                    self.run_continous_query = False
                    self.log.info(err)
                self.run_continous_query = False
                if err is None and self.failover_method != "cancel_rebalance":
                    self.fail('rebalance could not be failed')
                try:
                    task.result()
                except Exception as e:
                    self.fail(f'test failed due to {str(e)}')

            self.sleep(75)
            rebalance_out = self.cluster.async_rebalance(self.servers[:self.nodes_init], [],
                                                         [out_node], services=services_in)
            rebalance_out.result()
            self.sleep(10)
            self.validate_scans_post_rebalance(select_queries_list=select_queries_list, node=in_node)
        self.sleep(60)
        index_node = self.get_nodes_from_services_map(service_type="index", get_all_nodes=False)
        new_index_rest = RestConnection(index_node)
        no_of_docs_indexed_after_rebalanced = self.gen_num_of_indexed_docs()
        indexder_metadata_after_rebalance = new_index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexder_metadata_before_rebalance), len(indexder_metadata_after_rebalance),
                         'Probable drop of some indexes')
        for i in range(len(no_of_docs_indexed_before_rebalanced)):
            expected_doc_count = no_of_docs_indexed_before_rebalanced[
                                     i] + mutation_batch * self.num_of_docs_per_collection_mutation
            self.assertEqual(no_of_docs_indexed_after_rebalanced[i], expected_doc_count,
                             'mutations are not reflected in the indexes')
        self.verify_index_distribution(indexer_metadata=indexder_metadata_after_rebalance)

    # This is a capella test
    def test_rebalance_in_on_capella_cluster(self):
        self.num_buckets = self.input.param("num_buckets", 1)
        self.master = self.update_master_node()
        self.rest = RestConnection(self.master)
        for item in range(self.num_buckets):
            bucket_name = f'test_bucket_{item + 1}'
            self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                            replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                            enable_replica_index=self.enable_replica_index,
                                                            eviction_policy=self.eviction_policy, lww=self.lww)
            self.cluster.create_standard_bucket(name=bucket_name, port=11222,
                                                bucket_params=self.bucket_params)
            self.prepare_collection_for_indexing(num_scopes=self.num_scopes, num_collections=self.num_collections,
                                                 num_of_docs_per_collection=self.num_of_docs_per_collection,
                                                 json_template=self.json_template, bucket_name=bucket_name)
        self.buckets = self.rest.get_buckets()
        select_queries = set()
        query_node = self.get_nodes_from_services_map(service_type="n1ql")

        for namespace in self.namespaces:
            for batch in range(self.num_index_batches):
                query_definitions = self.gsi_util_obj.generate_hotel_data_index_definition()
                select_queries.update(self.gsi_util_obj.get_select_queries(definition_list=query_definitions,
                                                                           namespace=namespace, limit=10))
                queries = self.gsi_util_obj.get_create_index_list(definition_list=query_definitions,
                                                                  namespace=namespace,
                                                                  num_replica=self.num_index_replica)
                self.gsi_util_obj.create_gsi_indexes(create_queries=queries, database=namespace, query_node=query_node)
                defer_query = (f"CREATE INDEX defer_idx_{batch} on {namespace}(url) "
                               f"with {{'defer_build': true, 'num_replica': 1}}")
                self.run_cbq_query(query=defer_query, server=query_node)
        self.wait_until_indexes_online(defer_build=True)
        self.dataload_till_rr(namespaces=self.namespaces, rr=50, json_template=self.json_template)
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        index_node = index_nodes[0]
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.capella_api.set_capella_index_settings(node=index_node.ip, setting=redistribute,
                                                    username=index_node.rest_username,
                                                    password=index_node.rest_password)
        # Starting the rebalance-in with redistribution enabled
        with ThreadPoolExecutor() as executor:
            task = executor.submit(self.capella_api.add_nodes_to_capella_cluster, services=["index"],
                                   cluster_id=self.cluster_id)
            self.validate_batch_size_and_queries(index_nodes_b4_rebalance=index_nodes)
            task.result()
        new_index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        for node in new_index_nodes:
            if node not in index_nodes:
                break
        else:
            self.fail("No new Index node was added")
        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        self.validate_scans_post_rebalance(select_queries_list=select_queries, node=node, query_node=query_node)
        self.verify_index_distribution(indexer_metadata=indexer_metadata)
