"""collections_alter_index.py: description

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "14/09/20 11:45 am" 

"""
import copy
import random

from concurrent.futures import ThreadPoolExecutor

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from .base_gsi import BaseSecondaryIndexingTests


class CollectionsAlterIndex(BaseSecondaryIndexingTests):
    def setUp(self):
        super(CollectionsAlterIndex, self).setUp()
        self.log.info("==============  CollectionsIndexBasics setup has started ==============")
        self.rest.delete_all_buckets()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.log.info("==============  CollectionsIndexBasics setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  CollectionsIndexBasics tearDown has started ==============")
        super(CollectionsAlterIndex, self).tearDown()
        self.log.info("==============  CollectionsIndexBasics tearDown has completed ==============")

    def test_alter_index_for_collections(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.skipTest("Can't run Alter index tests with less than 2 Index nodes")
        self.prepare_collection_for_indexing()
        collection_namespace = self.namespaces[0]
        index_gen1 = QueryDefinition(index_name='idx1', index_fields=['age'])
        index_gen2 = QueryDefinition(index_name='idx2', index_fields=['city'])
        index_gen3 = QueryDefinition(index_name='idx3', index_fields=['country'])
        query = index_gen1.generate_index_create_query(namespace=collection_namespace, num_replica=1)
        self.run_cbq_query(query=query)
        query = index_gen2.generate_index_create_query(namespace=collection_namespace, num_replica=1)
        self.run_cbq_query(query=query)
        query = index_gen3.generate_index_create_query(namespace=collection_namespace, num_replica=1)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        index_info = self.get_index_map()[self.test_bucket]
        idx_name = 'idx2'
        idx2_replica_host, idx2_host = None, None
        for idx in index_info:
            if idx_name == idx:
                idx2_host = index_info[idx]['hosts']
            elif idx_name in idx:
                idx2_replica_host = index_info[idx]['hosts']
            if idx2_replica_host and idx2_host:
                break

        new_index_node = None
        old_index_node = None
        if self.use_https:
            port = '18091'
        else:
            port = '8091'
        for node in index_nodes:
            if node.ip in idx2_replica_host:
                old_index_node = f"{node.ip}:{port}"
            if node.ip not in idx2_host and node.ip not in idx2_replica_host:
                new_index_node = f"{node.ip}:{port}"
            if old_index_node and new_index_node:
                break
        # Just moving idx2 node, while keeping same node for replica
        self.alter_index_replicas(index_name='idx2', namespace=collection_namespace, action='move',
                                  nodes=[old_index_node, new_index_node])
        self.sleep(10)
        self.wait_until_indexes_online()
        index_info = self.get_index_map()[self.test_bucket]
        idx2_new_host = None
        for idx in index_info:
            if idx_name == idx:
                idx2_new_host = index_info[idx]['hosts']
            elif idx_name in idx:
                self.assertEqual(idx2_replica_host, index_info[idx]['hosts'], "Replica host also has been changed")

    def test_alter_index_with_multiple_collections_with_same_index_name(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.skipTest("Can't run Alter index tests with less than 2 Index nodes")
        self.prepare_collection_for_indexing(num_collections=2)
        collection_namespace1, collection_namespace2 = self.namespaces
        index_gen1 = QueryDefinition(index_name='idx1', index_fields=['age'])
        index_gen2 = QueryDefinition(index_name='idx2', index_fields=['city'])
        index_gen3 = QueryDefinition(index_name='idx3', index_fields=['country'])
        query = index_gen1.generate_index_create_query(namespace=collection_namespace1, num_replica=1)
        self.run_cbq_query(query=query)
        query = index_gen1.generate_index_create_query(namespace=collection_namespace2, num_replica=1)
        self.run_cbq_query(query=query)
        query = index_gen2.generate_index_create_query(namespace=collection_namespace1, num_replica=1)
        self.run_cbq_query(query=query)
        query = index_gen2.generate_index_create_query(namespace=collection_namespace2, num_replica=1)
        self.run_cbq_query(query=query)
        query = index_gen3.generate_index_create_query(namespace=collection_namespace1, num_replica=1)
        self.run_cbq_query(query=query)
        query = index_gen3.generate_index_create_query(namespace=collection_namespace2, num_replica=1)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        # Just moving idx2 node of collection1, while keeping same node for replica
        self.sleep(20)
        index_info = self.rest.get_indexer_metadata()['status']
        index_info_without_idx2 = copy.deepcopy(index_info)
        idx_name = 'idx2'
        _, keyspace = collection_namespace1.split(':')
        bucket, scope, collection = keyspace.split('.')
        idx2_host, idx2_replica_host = None, None
        for item in index_info:
            if item['name'] == idx_name and item['collection'] == collection:
                idx2_host = item['hosts'][0]
                index_info_without_idx2.remove(item)
            elif idx_name in item['name'] and item['collection'] == collection:
                idx2_replica_host = item['hosts'][0]
                index_info_without_idx2.remove(item)

            if idx2_host and idx2_replica_host:
                break

        new_index_node = None
        old_index_node = None
        if self.use_https:
            port = '18091'
        else:
            port = '8091'
        for node in index_nodes:
            if node.ip in idx2_replica_host:
                old_index_node = f"{node.ip}:{port}"
            if node.ip not in idx2_host and node.ip not in idx2_replica_host:
                new_index_node = f"{node.ip}:{port}"
            if old_index_node and new_index_node:
                break
        self.sleep(30, "Giving some time before trying alter index")
        self.log.info(f"Current hosts for index idx2: {idx2_host} and idx2(replica 1): {old_index_node}")
        self.log.info(f"New hosts should be for index idx2: {new_index_node} and idx2(replica 1): {old_index_node}")
        self.alter_index_replicas(index_name='idx2', namespace=collection_namespace1, action='move',
                                  nodes=[old_index_node, new_index_node])
        self.sleep(10)
        self.wait_until_indexes_online()

        # Validating host switch for index idx2 of test_collection_1
        index_info_after_alter_index = self.rest.get_indexer_metadata()['status']
        index_info_after_alter_index_without_idx2 = copy.deepcopy(index_info_after_alter_index)

        idx2_host_after_alter_index, idx2_replica_host_after_alter_index = None, None
        for item in index_info_after_alter_index:
            if item['name'] == idx_name and item['collection'] == collection:
                idx2_host_after_alter_index = item['hosts'][0]
                index_info_after_alter_index_without_idx2.remove(item)
            elif idx_name in item['name'] and item['collection'] == collection:
                idx2_replica_host_after_alter_index = item['hosts'][0]
                index_info_after_alter_index_without_idx2.remove(item)

            if idx2_host_after_alter_index and idx2_replica_host_after_alter_index:
                break

        self.log.info(f"Current hosts for index idx2: {idx2_host_after_alter_index} "
                      f"and idx2(replica 1): {idx2_replica_host_after_alter_index}")
        self.assertEqual(new_index_node, idx2_host_after_alter_index, "Replica host also has been changed."
                                                                      " May be increase sleep as index metadata might"
                                                                      " not be updated in time")
        self.assertEqual(idx2_replica_host, idx2_replica_host_after_alter_index, "Index idx2 hasn't been moved")
        self.assertEqual(index_info_without_idx2, index_info_after_alter_index_without_idx2)

    def test_alter_index_with_num_partition(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.skipTest("Can't run Alter index tests with less than 2 Index nodes")
        new_replica = 1
        self.prepare_collection_for_indexing()
        collection_namespace = self.namespaces[0]
        idx1, idx2 = 'idx1', 'idx2'
        index_gen1 = QueryDefinition(index_name=idx1, index_fields=['age'], partition_by_fields=['meta().id'])
        index_gen2 = QueryDefinition(index_name=idx2, index_fields=['city'], partition_by_fields=['meta().id'])
        query = index_gen1.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        query = index_gen2.generate_index_create_query(namespace=collection_namespace)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        self.sleep(5)

        index_metadata = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_metadata), 2, "No. of indexes are not matching.")
        for index in index_metadata:
            self.assertTrue(index['partitioned'], f"{index} is not a partitioned index")
            self.assertEqual(index['numPartition'], 8, "No. of partitions are not matching")
            self.assertEqual(index['numReplica'], 0, "No. of replicas are not matching")

        self.alter_index_replicas(namespace=collection_namespace, index_name=idx1, num_replicas=new_replica)
        self.alter_index_replicas(namespace=collection_namespace, index_name=idx2, num_replicas=new_replica)
        self.sleep(10)
        self.wait_until_indexes_online()

        index_metadata = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_metadata), 4, "No. of indexes are not matching.")
        for index in index_metadata:
            self.assertTrue(index['partitioned'], f"{index} is not a partitioned index")
            self.assertEqual(index['numPartition'], 8, "No. of partitions are not matching")
            self.assertEqual(index['numReplica'], new_replica, "No. of replicas are not matching")

    def test_alter_index_drop_index(self):
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        if len(index_nodes) < 3:
            self.skipTest("Can't run Alter index tests with less than 2 Index nodes")
        initial_replica = 2
        idx1, idx2 = 'idx1', 'idx2'
        self.prepare_collection_for_indexing()
        collection_namespace = self.namespaces[0]
        index_gen1 = QueryDefinition(index_name='idx1', index_fields=['age'])
        index_gen2 = QueryDefinition(index_name='idx2', index_fields=['city'])
        query = index_gen1.generate_index_create_query(namespace=collection_namespace, num_replica=initial_replica)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()
        query = index_gen2.generate_index_create_query(namespace=collection_namespace, num_replica=initial_replica)
        self.run_cbq_query(query=query)
        self.wait_until_indexes_online()

        index_metadata = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_metadata), 6, "No. of indexes are not matching.")
        for index in index_metadata:
            self.assertEqual(index['numReplica'], initial_replica, "No. of replicas are not matching")

        self.alter_index_replicas(namespace=collection_namespace, index_name=idx1, action='drop_replica', replica_id=2)
        self.sleep(10)
        self.alter_index_replicas(namespace=collection_namespace, index_name=idx2, action='drop_replica', replica_id=1)
        self.sleep(10)
        self.wait_until_indexes_online()

        index_metadata = self.rest.get_indexer_metadata()['status']
        self.assertEqual(len(index_metadata), 4, "No. of indexes are not matching.")
        for index in index_metadata:
            if index['indexName'] == idx1:
                self.assertTrue(index['replicaId'] != 2, f"Dropped wrong replica Id for index{idx2}")
            elif index['indexName'] == idx2:
                self.assertTrue(index['replicaId'] != 1, f"Dropped wrong replica Id for index{idx2}")
