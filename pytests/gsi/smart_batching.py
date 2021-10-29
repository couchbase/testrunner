"""smart_batching.py: "This class test smart batching improvemnt for gsi indexer nodes during rebalance. MB-33546"

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "15/09/21 03:45 pm"

"""
import random
import time

from gsi.base_gsi import BaseSecondaryIndexingTests
from gsi.collections_concurrent_indexes import powerset
from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestHelper


class SmartBatching(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SmartBatching, self).setUp()
        self.log.info("==============  SmartBatching setup has started ==============")
        self.rest.delete_all_buckets()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.initial_index_num = self.input.param("initial_index_num", 20)
        self.transfer_batch_size = self.input.param("transfer_batch_size", 3)
        self.rebalance_timeout = self.input.param("rebalance_timeout", 600)
        self.use_defer_build = self.input.param("use_defer_build", False)

        self.add_nodes_num = self.input.param("add_nodes_num", 1)
        self.remove_nodes_num = self.input.param("remove_nodes_num", 1)
        self.index_field_set = powerset(['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                                         'suffix', 'filler1', 'phone', 'zipcode'])
        index_transfer_batch_size = {"indexer.rebalance.transferBatchSize": self.transfer_batch_size}
        self.index_rest.set_index_settings(index_transfer_batch_size)
        redistribute = {"indexer.settings.rebalance.redistribute_indexes": True}
        self.index_rest.set_index_settings(redistribute)
        self.log.info("==============  SmartBatching setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  SmartBatching tearDown has started ==============")
        super(SmartBatching, self).tearDown()
        self.log.info("==============  SmartBatching tearDown has completed ==============")

    def suite_tearDown(self):
        pass

    def suite_setUp(self):
        pass

    def _validate_smart_batching_during_rebalance(self, rebalance_task):
        while self.rest._rebalance_progress() == 100 and rebalance_task.state != 'CHECKING':
            progress = self.rest._rebalance_progress()
            state = rebalance_task.state
            print(f'Progress:{progress}')
            print(f'state:{state}')
            self.sleep(5)
        self.sleep(5)
        # Validating no. of parallel concurrent build
        start_time = time.time()
        while self.rest._rebalance_progress() < 100:
            indexer_metadata = self.index_rest.get_indexer_metadata()['status']
            moving_indexes_count = 0
            # self.log.info(indexer_metadata)
            for index in indexer_metadata:
                if index['status'] == 'Moving':
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

    def test_batching_for_rebalance_in_indexer_node(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 2 index nodes")

        self.prepare_collection_for_indexing(num_scopes=self.scope_num, num_collections=self.collection_num,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection)

        for collection_namespace in self.namespaces:
            for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
                if self.use_defer_build:
                    defer_build = random.choice([True, False])
                else:
                    defer_build = False
                idx = f'idx_{item}'
                index_gen = QueryDefinition(index_name=idx, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=defer_build,
                                                              num_replica=self.num_replicas)
                try:
                    self.run_cbq_query(query=query)
                except Exception as err:
                    error = 'Build Already In Progress'
                    if error not in str(err):
                        self.fail(err)
                self.wait_until_indexes_online(defer_build=True)

        indexer_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']
        # Reblance-in indexer node/s
        add_nodes = self.servers[self.nodes_init:self.nodes_init + self.add_nodes_num]
        services = ['index'] * self.add_nodes_num
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                      to_remove=[], services=services)

        self.sleep(60)
        self._validate_smart_batching_during_rebalance(rebalance_task)
        indexer_metadata_after_rebalance = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexer_metadata_after_rebalance),
                         self.initial_index_num * (self.num_replicas + 1) * self.scope_num * self.collection_num)
        indexes_distribution = {}
        for index in indexer_metadata_after_rebalance:
            status_after_reb = index['status']
            index_name = index['indexName']
            for idx in indexer_metadata_before_rebalance:
                if idx['indexName'] == index_name:
                    status = idx['status']
                    if status_after_reb != status:
                        self.log.info(f"Indexer Metadata before Rebalance {indexer_metadata_before_rebalance}")
                        self.log.info(f"Indexer Metadata after Rebalance {indexer_metadata_after_rebalance}")
                        self.log.info(f"Expected: {status}, Actual: {status_after_reb}")
                        self.fail(f"Index Status is not matching after rebalance for index: {index_name}")
            host = index['hosts'][0]
            if host in indexes_distribution:
                indexes_distribution[host].append(index_name)
            else:
                indexes_distribution[host] = [index_name]
        for host in indexes_distribution:
            self.log.info(f"No. of Indexes on {host}: {len(indexes_distribution[host])}")

    def test_batching_for_rebalance_out_indexer_node(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.fail("Need at least 3 index nodes")

        self.prepare_collection_for_indexing(num_scopes=self.scope_num, num_collections=self.collection_num,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection)

        for collection_namespace in self.namespaces:
            for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
                if self.use_defer_build:
                    defer_build = random.choice([True, False])
                else:
                    defer_build = False
                idx = f'idx_{item}'
                index_gen = QueryDefinition(index_name=idx, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=defer_build,
                                                              num_replica=self.num_replicas)
                try:
                    self.run_cbq_query(query=query)
                except Exception as err:
                    error = 'Build Already In Progress'
                    if error not in str(err):
                        self.fail(err)
                self.wait_until_indexes_online(defer_build=True)

        indexer_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']
        # Reblance-out indexer node/s
        remove_nodes = index_nodes[1:1 + self.remove_nodes_num]
        services = ['index'] * self.remove_nodes_num
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=[],
                                                      to_remove=remove_nodes, services=services)
        self._validate_smart_batching_during_rebalance(rebalance_task)
        indexer_metadata_after_rebalance = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexer_metadata_after_rebalance),
                         self.initial_index_num * (self.num_replicas + 1) * self.scope_num * self.collection_num)
        indexes_distribution = {}
        for index in indexer_metadata_after_rebalance:
            status_after_reb = index['status']
            index_name = index['indexName']
            for idx in indexer_metadata_before_rebalance:
                if idx['indexName'] == index_name:
                    status = idx['status']
                    if status_after_reb != status:
                        self.log.info(f"Indexer Metadata before Rebalance {indexer_metadata_before_rebalance}")
                        self.log.info(f"Indexer Metadata after Rebalance {indexer_metadata_after_rebalance}")
                        self.log.info(f"Expected: {status}, Actual: {status_after_reb}")
                        self.fail(f"Index Status is not matching after rebalance for index: {index_name}")
            host = index['hosts'][0]
            if host in indexes_distribution:
                indexes_distribution[host].append(index_name)
            else:
                indexes_distribution[host] = [index_name]
        for host in indexes_distribution:
            self.log.info(f"No. of Indexes on {host}: {len(indexes_distribution[host])}")

    def test_batching_for_swap_rebalance_indexer_node(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 2 index nodes")

        self.prepare_collection_for_indexing(num_scopes=self.scope_num, num_collections=self.collection_num,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection)

        for collection_namespace in self.namespaces:
            for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
                if self.use_defer_build:
                    defer_build = random.choice([True, False])
                else:
                    defer_build = False
                idx = f'idx_{item}'
                index_gen = QueryDefinition(index_name=idx, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=defer_build,
                                                              num_replica=self.num_replicas)
                try:
                    self.run_cbq_query(query=query)
                except Exception as err:
                    error = 'Build Already In Progress'
                    if error not in str(err):
                        self.fail(err)
                self.wait_until_indexes_online(defer_build=True)

        indexer_metadata_before_rebalance = self.index_rest.get_indexer_metadata()['status']
        # Swap Reblance indexer node/s
        add_nodes = self.servers[self.nodes_init:self.nodes_init + self.add_nodes_num]
        remove_nodes = index_nodes[1:1 + self.remove_nodes_num]
        services = ['index'] * self.remove_nodes_num
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                      to_remove=remove_nodes, services=services)
        self.sleep(30)
        self._validate_smart_batching_during_rebalance(rebalance_task)
        indexer_metadata_after_rebalance = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexer_metadata_after_rebalance),
                         self.initial_index_num * (self.num_replicas + 1) * self.scope_num * self.collection_num)
        indexes_distribution = {}
        for index in indexer_metadata_after_rebalance:
            status_after_reb = index['status']
            index_name = index['indexName']
            for idx in indexer_metadata_before_rebalance:
                if idx['indexName'] == index_name:
                    status = idx['status']
                    if status_after_reb != status:
                        self.log.info(f"Indexer Metadata before Rebalance {indexer_metadata_before_rebalance}")
                        self.log.info(f"Indexer Metadata after Rebalance {indexer_metadata_after_rebalance}")
                        self.log.info(f"Expected: {status}, Actual: {status_after_reb}")
                        self.fail(f"Index Status is not matching after rebalance for index: {index_name}")
            host = index['hosts'][0]
            if host in indexes_distribution:
                indexes_distribution[host].append(index_name)
            else:
                indexes_distribution[host] = [index_name]
        for host in indexes_distribution:
            self.log.info(f"No. of Indexes on {host}: {len(indexes_distribution[host])}")

    def test_batching_for_recovering_from_failover_node(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 2:
            self.fail("Need at least 2 index nodes")

        self.prepare_collection_for_indexing(num_scopes=self.scope_num, num_collections=self.collection_num,
                                             num_of_docs_per_collection=self.num_of_docs_per_collection)

        for collection_namespace in self.namespaces:
            for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
                if self.use_defer_build:
                    defer_build = random.choice([True, False])
                else:
                    defer_build = False
                idx = f'idx_{item}'
                if self.partitoned_index:
                    partition_fields = index_field
                    index_gen = QueryDefinition(index_name=idx, index_fields=index_field,
                                                partition_by_fields=partition_fields)
                else:
                    index_gen = QueryDefinition(index_name=idx, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=defer_build,
                                                              num_replica=self.num_replicas,
                                                              num_partition=self.num_partition)

                try:
                    self.run_cbq_query(query=query)
                except Exception as err:
                    error = 'Build Already In Progress'
                    if error not in str(err):
                        self.fail(err)
                self.wait_until_indexes_online(defer_build=True)

        indexer_metadata_before_failover = self.index_rest.get_indexer_metadata()['status']
        node_out = index_nodes[1]
        failover_task = self.cluster.async_failover(self.servers[:self.nodes_init], [node_out],
                                                    self.graceful, wait_for_pending=120)

        failover_task.result()
        # Reblance-in indexer node/s
        add_nodes = self.servers[self.nodes_init:self.nodes_init + self.add_nodes_num]
        services = ['index'] * self.add_nodes_num
        rebalance_task = self.cluster.async_rebalance(servers=self.servers[:self.nodes_init], to_add=add_nodes,
                                                      to_remove=[], services=services)

        self.sleep(30)
        self._validate_smart_batching_during_rebalance(rebalance_task)
        indexer_metadata_after_failover = self.index_rest.get_indexer_metadata()['status']
        self.assertEqual(len(indexer_metadata_after_failover),
                         self.initial_index_num * (self.num_replicas + 1) * self.scope_num * self.collection_num)
        if not self.partitoned_index:
            indexes_distribution = {}
            for index in indexer_metadata_before_failover:
                status_before_reb = index['status']
                index_name = index['name']
                host = index['hosts'][0]
                if node_out.ip in host:
                    for idx in indexer_metadata_after_failover:
                        if idx['name'] == index_name:
                            curr_host = idx['hosts'][0]
                            status = idx['status']
                            if status_before_reb != status:
                                self.log.info(f"Indexer Metadata before Rebalance {indexer_metadata_before_failover}")
                                self.log.info(f"Indexer Metadata after Rebalance {indexer_metadata_after_failover}")
                                self.log.info(f"Expected: {status}, Actual: {status_before_reb}")
                                self.fail(f"Index Status is not matching after rebalance for index: {index_name}")
                            self.assertTrue(curr_host != host)

                if host in indexes_distribution:
                    indexes_distribution[host].append(index_name)
                else:
                    indexes_distribution[host] = [index_name]
            for host in indexes_distribution:
                self.log.info(f"No. of Indexes on {host}: {len(indexes_distribution[host])}")
