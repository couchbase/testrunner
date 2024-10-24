"""gsi_free_tier.py: These tests validate free-tier limits tests

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "10/27/21 11:22 am"

"""
from concurrent.futures import ThreadPoolExecutor
from couchbase_helper.query_definitions import QueryDefinition
from .base_gsi import BaseSecondaryIndexingTests
from .collections_concurrent_indexes import powerset


class GSIFreeTier(BaseSecondaryIndexingTests):
    def setUp(self):
        super(GSIFreeTier, self).setUp()
        self.log.info("==============  GSIFreeTier setup has started ==============")
        self.gsi_tier_limit = self.input.param("gsi_tier_limit", 5)
        self.rest.delete_all_buckets()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection,
                                             num_scopes=self.num_scopes, num_collections=self.num_collections)
        self.index_field_set = powerset(['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                                         'suffix', 'filler1', 'phone', 'zipcode'])
        self.rest.set_internalSetting('enforceLimits', True)
        self.updated_tier_limit = self.input.param("updated_tier_limit", None)
        self.drop_indexes = self.input.param("drop_indexes", False)
        self.alter_index_instances = self.input.param("alter_index_instances", False)
        self.log.info("==============  GSIFreeTier setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  GSIFreeTier tearDown has started ==============")
        super(GSIFreeTier, self).tearDown()
        self.log.info("==============  GSIFreeTier tearDown has completed ==============")

    def test_free_tier_limit(self):
        index_gen_list = []
        prev_scope = None
        index_instance_counter = 0
        for collection_namespace in self.namespaces:
            _, keyspace = collection_namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            self.index_rest.set_gsi_tier_limit(bucket=bucket, scope=scope, limit=self.gsi_tier_limit)
            if prev_scope != scope:
                index_instance_counter = 0
            limit_updated_flag = False
            indexes_list = []
            for item, index_field in zip(range(self.gsi_tier_limit), self.index_field_set):
                idx = f'idx_{item}'
                if self.partitoned_index:
                    partition_fields = index_field
                    index_gen = QueryDefinition(index_name=idx, index_fields=index_field,
                                                partition_by_fields=partition_fields)
                else:
                    index_gen = QueryDefinition(index_name=idx, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build,
                                                              num_replica=self.num_index_replica,
                                                              num_partition=self.num_partition)
                index_gen_list.append(index_gen)
                try:
                    self.run_cbq_query(query=query)
                    indexes_list.append(idx)
                    if self.defer_build:
                        build_query = index_gen.generate_build_query(namespace=collection_namespace)
                        self.run_cbq_query(build_query)
                    self.wait_until_indexes_online()

                    # Increasing the counter only after successful index creation
                    if self.partitoned_index:
                        index_instance_counter += self.num_partition
                    else:
                        index_instance_counter += self.num_index_replica + 1
                except Exception as err:
                    expected_err = 'Limit for number of indexes that can be created per scope has been reached'
                    if expected_err in str(err):
                        self.log.info("Last query tried to create indexes which were beyond GSI Tier limits")
                        self.log.info(f"GSI Free Tier Limit: {self.gsi_tier_limit}")
                        self.log.info(f"Index instances limit available:{self.gsi_tier_limit - index_instance_counter}")
                        prev_scope = scope
                        if self.updated_tier_limit and not limit_updated_flag:
                            self.index_rest.set_gsi_tier_limit(bucket=bucket, scope=scope,
                                                               limit=self.updated_tier_limit)
                            limit_updated_flag = True
                            self.gsi_tier_limit = self.updated_tier_limit
                        elif self.drop_indexes and not limit_updated_flag:
                            drop_query = index_gen_list[0].generate_index_drop_query(namespace=collection_namespace)
                            self.run_cbq_query(query=drop_query)
                            limit_updated_flag = True
                            indexer_metadata = self.index_rest.get_indexer_metadata()['status']
                            self.log.info(f"No. of Index instances after Index Drop: {len(indexer_metadata)}")
                            if self.partitoned_index:
                                index_instance_counter -= self.num_partition
                            else:
                                index_instance_counter -= self.num_index_replica + 1
                        elif self.alter_index_instances and not limit_updated_flag:
                            for index_name in indexes_list:
                                index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
                                query = f'Alter index {index_name} on {collection_namespace} ' \
                                        f'with {{"action": "replica_count", "num_replica": {len(index_nodes) - 1}}}'
                                try:
                                    self.run_cbq_query(query=query)
                                except Exception as err:
                                    if expected_err in str(err):
                                        self.log.info("Last query tried to alter indexes"
                                                      " which were beyond GSI Tier limits")
                                        self.log.info(f"GSI Free Tier Limit: {self.gsi_tier_limit}")
                                        self.log.info(f"Index instances limit available:"
                                                      f" {self.gsi_tier_limit - index_instance_counter}")
                                        limit_updated_flag = True
                                        break
                                    else:
                                        self.fail(err)
                                else:
                                    indexer_metadata = self.index_rest.get_indexer_metadata()['status']
                                    self.log.info(f"Index instances limit available:"
                                                  f" {self.gsi_tier_limit - len(indexer_metadata)}")
                                    self.fail("Alter Index changes the index instance beyondGSI Tier limits")
                        else:
                            break
                    else:
                        self.fail(err)

    def test_free_tier_limit_with_concurrent_indexes(self):
        query_list = []
        for collection_namespace in self.namespaces:
            _, keyspace = collection_namespace.split(':')
            bucket, scope, collection = keyspace.split('.')
            self.index_rest.set_gsi_tier_limit(bucket=bucket, scope=scope, limit=self.gsi_tier_limit)
            for item, index_field in zip(range(self.gsi_tier_limit), self.index_field_set):
                idx = f'idx_{item}'
                if self.partitoned_index:
                    partition_fields = index_field
                    index_gen = QueryDefinition(index_name=idx, index_fields=index_field,
                                                partition_by_fields=partition_fields)
                else:
                    index_gen = QueryDefinition(index_name=idx, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace,
                                                              defer_build=self.defer_build,
                                                              num_replica=self.num_index_replica,
                                                              num_partition=self.num_partition)
                query_list.append(query)
        with ThreadPoolExecutor() as executor:
            tasks = []
            for query in query_list:
                task = executor.submit(self.run_cbq_query, query=query)
                tasks.append(task)
            for task in tasks:
                try:
                    result = task.result()
                    self.log.info(result)
                except Exception as err:
                    expected_err_1 = 'Limit for number of indexes that can be created per scope has been reached'
                    concurrent_index_err_1 = 'The index is scheduled for background creation.'
                    concurrent_index_err_2 = 'Index creation will be retried in background'
                    concurrent_index_err_3 = 'Build Already In Progress.'

                    if concurrent_index_err_1 in str(err) or concurrent_index_err_2 in str(err) or\
                            concurrent_index_err_3 in str(err):
                        pass
                    elif expected_err_1 in str(err):
                        self.log.info("Last query tried to create indexes which were beyond GSI Tier limits")
                        self.log.info(err)
                    else:
                        self.fail(err)
