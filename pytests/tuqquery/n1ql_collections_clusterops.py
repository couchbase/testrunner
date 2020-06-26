from .tuq import QueryTests
from membase.helper.bucket_helper import BucketOperationHelper
from collection.collections_n1ql_client import CollectionsN1QL
from lib.membase.api.rest_client import RestHelper
from lib.collection.collections_cli_client import CollectionsCLI


class QueryCollectionsClusteropsTests(QueryTests):

    def setUp(self):
        super(QueryCollectionsClusteropsTests, self).setUp()
        self.log.info("==============  QueryCollectionsClusteropsTests setup has started ==============")
        self.log_config_info()
        self.collections_helper = CollectionsN1QL(self.master)
        self.cli_helper = CollectionsCLI(self.master)
        self.bucket_params = self._create_bucket_params(server=self.master, size=100, replicas=0,
                                                        bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy="fullEviction", lww=self.lww)

        self.log.info("==============  QueryCollectionsClusteropsTests setup has completed ==============")

    def suite_setUp(self):
        self.log.info("==============  QueryCollectionsClusteropsTests suite_setup has started ==============")
        super(QueryCollectionsClusteropsTests, self).suite_setUp()
        self.log.info("==============  QueryCollectionsClusteropsTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryCollectionsClusteropsTests tearDown has started ==============")

        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
        super(QueryCollectionsClusteropsTests, self).tearDown()
        self.log.info("==============  QueryCollectionsClusteropsTests tearDown has completed ==============")

    def suite_tearDown(self):
        self.log.info("==============  QueryCollectionsClusteropsTests suite_tearDown has started ==============")
        super(QueryCollectionsClusteropsTests, self).suite_tearDown()
        self.log.info("==============  QueryCollectionsClusteropsTests suite_tearDown has completed ==============")

    def test_create_scope_collection_inactive_kv(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"
        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        failover_result = self.cluster.failover(servers=self.servers, failover_nodes=[self.servers[1]], graceful=False)

        scope_created = self.cli_helper.create_scope(bucket=bucket_name, scope=scope_name)
        self.assertTrue(scope_created, "Cannot create scope")
        collection_created = self.cli_helper.create_collection(bucket=bucket_name, scope="_default", collection=collection_name)
        self.assertTrue(collection_created, "Cannot create collection for cluster with one kv node failed over.")

    def test_create_scope_collection_rebalance_kv(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        rebalance_result = self.cluster.async_rebalance(self.servers, [], [self.servers[1]])

        try:
            scope_created = self.cli_helper.create_scope(bucket=bucket_name, scope=scope_name)
            self.assertTrue(scope_created, "Cannot create scope during rebalance")
            collection_created = self.cli_helper.create_collection(bucket=bucket_name, scope="_default", collection=collection_name)
            self.assertTrue(collection_created, "Cannot create collection during rebalance.")
        finally:
            #wait until rebalance is done
            RestHelper(self.rest).rebalance_reached(retry_count=150)
            time_limit = 100
            while time_limit > 0:
                if RestHelper(self.rest).is_cluster_rebalanced():
                    break
                else:
                    time_limit = time_limit - 1
                    self.sleep(10, "Waiting for rebalance finish.")

    def test_drop_scope_collection_inactive_kv(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)

        scope_created = self.cli_helper.create_scope(bucket=bucket_name, scope=scope_name)
        self.assertTrue(scope_created, "Cannot create scope.")
        collection_created = self.cli_helper.create_collection(bucket=bucket_name, scope=scope_name, collection=collection_name)
        self.assertTrue(collection_created, "Cannot create collection.")

        failover_result = self.cluster.failover(servers=self.servers, failover_nodes=[self.servers[1]], graceful=False)
        self.sleep(10, "Waiting for failover.")
        collection_dropped = self.cli_helper.delete_collection(bucket=bucket_name, scope=scope_name, collection=collection_name)
        self.assertTrue(collection_dropped, "Cannot drop collection for cluster with one kv node failed over.")
        scope_dropped= self.cli_helper.delete_scope(scope=scope_name, bucket=bucket_name)
        self.assertTrue(scope_dropped, "Cannot drop scope for cluster with one kv node failed over.")


    def test_drop_scope_collection_rebalance_kv(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        scope_created = self.cli_helper.create_scope(bucket=bucket_name, scope=scope_name)
        self.assertTrue(scope_created, "Cannot create scope")
        collection_created = self.cli_helper.create_collection(bucket=bucket_name, scope="_default", collection=collection_name)
        self.assertTrue(collection_created, "Cannot create collection")

        rebalance_result = self.cluster.async_rebalance(self.servers, [], [self.servers[1]])
        try:
            collection_dropped = self.cli_helper.delete_collection(bucket=bucket_name, scope="_default", collection=collection_name)
            self.assertTrue(collection_dropped, "Cannot drop collection during rebalance.")
            scope_dropped = self.cli_helper.delete_scope(scope=scope_name, bucket=bucket_name)
            self.assertTrue(scope_dropped, "Cannot drop scope during rebalance")
        finally:
            #wait until rebalance is done
            RestHelper(self.rest).rebalance_reached(retry_count=150)
            time_limit = 100
            while time_limit > 0:
                if RestHelper(self.rest).is_cluster_rebalanced():
                    break
                else:
                    time_limit = time_limit - 1
                    self.sleep(10, "Waiting for rebalance finish.")

    def test_create_collection_after_failover(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        scope_created = self.cli_helper.create_scope(bucket=bucket_name, scope=scope_name)
        self.assertTrue(scope_created, "Cannot create scope")

        failover_result = self.cluster.failover(servers=self.servers, failover_nodes=[self.servers[1]], graceful=False)

        collection_created = self.cli_helper.create_collection(bucket=bucket_name, scope="_default", collection=collection_name)
        self.assertTrue(collection_created, "Cannot create collection for cluster with one kv node failed over.")

    def test_create_collection_diring_rebalance_kv(self):
        bucket_name = "bucket1"
        scope_name = "scope1"
        collection_name = "collection1"

        self.cluster.create_standard_bucket(bucket_name, 11222, self.bucket_params)
        scope_created = self.cli_helper.create_scope(bucket=bucket_name, scope=scope_name)
        self.assertTrue(scope_created, "Cannot create scope during rebalance")

        rebalance_result = self.cluster.async_rebalance(self.servers, [], [self.servers[1]])
        try:
            collection_created = self.cli_helper.create_collection(bucket=bucket_name, scope="_default", collection=collection_name)
            self.assertTrue(collection_created, "Cannot create collection during rebalance.")
        finally:
            #wait until rebalance is done
            RestHelper(self.rest).rebalance_reached(retry_count=150)
            time_limit = 100
            while time_limit > 0:
                if RestHelper(self.rest).is_cluster_rebalanced():
                    break
                else:
                    time_limit = time_limit - 1
                    self.sleep(10, "Waiting for rebalance finish.")
