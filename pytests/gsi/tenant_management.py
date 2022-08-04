"""tenant_management.py: "This class test cluster affinity  for GSI"

__author__ = "Hemant Rajput"
__maintainer = "Hemant Rajput"
__email__ = "Hemant.Rajput@couchbase.com"
__git_user__ = "hrajput89"
__created_on__ = "08/04/22 12:27 pm"

"""
import random

from gsi.base_gsi import BaseSecondaryIndexingTests
from gsi.collections_concurrent_indexes import powerset
from couchbase_helper.query_definitions import QueryDefinition


class TenantManagement(BaseSecondaryIndexingTests):
    def setUp(self):
        super(TenantManagement, self).setUp()
        self.log.info("==============  TenantManagement setup has started ==============")
        self.rest.delete_all_buckets()
        self.bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222,
                                            bucket_params=self.bucket_params)
        self.buckets = self.rest.get_buckets()
        self.use_defer_build = self.input.param("use_defer_build", False)
        self.index_field_set = powerset(['age', 'city', 'country', 'title', 'firstName', 'lastName', 'streetAddress',
                                         'suffix', 'filler1', 'phone', 'zipcode'])

    def tearDown(self):
        self.log.info("==============  TenantManagement tearDown has started ==============")
        super(TenantManagement, self).tearDown()
        self.log.info("==============  TenantManagement tearDown has completed ==============")

    def suite_tearDown(self):
        pass

    def suite_setUp(self):
        pass

    def test_cluster_affinity(self):
        query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)[0]
        self._create_server_groups()
        self.prepare_collection_for_indexing(num_of_docs_per_collection=self.num_of_docs_per_collection)
        for collection_namespace in self.namespaces:
            for item, index_field in zip(range(self.initial_index_num), self.index_field_set):
                if self.use_defer_build:
                    defer_build = random.choice([True, False])
                else:
                    defer_build = False
                idx = f'idx_{item}_{collection_namespace.split(":")[1].replace(".","_")}'
                index_gen = QueryDefinition(index_name=idx, index_fields=index_field)
                query = index_gen.generate_index_create_query(namespace=collection_namespace, defer_build=defer_build,
                                                              num_replica=self.num_replicas)
                try:
                    self.run_cbq_query(query=query, server=query_node)
                except Exception as err:
                    error = 'Build Already In Progress'
                    if error not in str(err):
                        self.fail(err)
                self.wait_until_indexes_online(defer_build=True)

        indexer_metadata = self.index_rest.get_indexer_metadata()['status']
        idx_host_list = set()
        for idx in indexer_metadata:
            idx_host = idx['hosts'][0]
            idx_host_list.add(idx_host)

        self.assertEqual(len(idx_host_list), 2, "Indexes are hosted on more than 2 node of sub-cluster")
