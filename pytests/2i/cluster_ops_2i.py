from .base_2i import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper

class SecondaryIndexingClusterOpsTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingClusterOpsTests, self).setUp()
        server = self.get_nodes_from_services_map(service_type = "n1ql")
        self.rest = RestConnection(server)

    def tearDown(self):
        super(SecondaryIndexingClusterOpsTests, self).tearDown()

    def test_remove_bucket_and_query(self):
    	#Initialization operation
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index=True, drop_index = False,
            query_with_explain = self.run_query_with_explain, query = self.run_query)
        #Remove bucket and recreate it
        for bucket in self.buckets:
        	self.rest.delete_bucket(bucket.name)
        #Verify the result set is empty
        self.verify_index_absence(query_definitions = self.query_definitions, buckets = self.buckets)

    def test_change_bucket_properties(self):
    	#Initialization operation
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = True, drop_index = False,
            query_with_explain = True, query = True)

        #Change Bucket Properties
        for bucket in self.buckets:
            self.rest.change_bucket_props(bucket,
                      ramQuotaMB=None,
                      authType=None,
                      saslPassword=None,
                      replicaNumber=0,
                      proxyPort=None,
                      replicaIndex=None,
                      flushEnabled=False)

        #Run query and query explain
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = False, drop_index = True,
            query_with_explain = True, query = True)

    def test_flush_bucket_and_query(self):
        #Initialization operation
        self.run_multi_operations(buckets=self.buckets,
            query_definitions=self.query_definitions,
            create_index=True, drop_index=False,
            query_with_explain=True, query=True)
        #Remove bucket and recreate it
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket.name)
        rollback_exception = True
        query_try_count = 0
        while rollback_exception and query_try_count < 10:
            self.sleep(5)
            query_try_count += 1
            #Query and bucket with empty result set
            try:
                self.multi_query_using_index_with_emptyresult(
                    query_definitions=self.query_definitions, buckets=self.buckets)
                rollback_exception = False
            except Exception as ex:
                msg = "Indexer rollback"
                if msg not in str(ex):
                    rollback_exception = False
                    self.log.info(ex)
                    raise
        self.assertFalse(rollback_exception, "Indexer still in rollback after 50 secs.")

    def test_delete_create_bucket_and_query(self):
    	#Initialization operation
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = True, drop_index = False,
            query_with_explain = self.run_query_with_explain, query = self.run_query)
        #Remove bucket and recreate it
        for bucket in self.buckets:
        	self.rest.delete_bucket(bucket.name)
        self.sleep(2)
        #Flush bucket and recreate it
        self._bucket_creation()
        self.sleep(2)
        ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
        #Verify the result set is empty
        self.verify_index_absence(query_definitions = self.query_definitions, buckets = self.buckets)
        index_map = self.get_index_stats()
        self.assertTrue(len(index_map) == 0, "Index Stats still show {0}".format(index_map))

    def test_data_loss(self):
        #Initialization operation
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = True, drop_index = False,
            query_with_explain = False, query = False)
        self._verify_bucket_count_with_index_count()
        try:
            servr_out = self.servers[1:self.nodes_init]
            failover_task = self.cluster.async_failover([self.master],
                        failover_nodes = servr_out, graceful=False)
            failover_task.result()
            rebalance = self.cluster.async_rebalance(self.servers[:1],
                                    [], servr_out)
            rebalance.result()
            # get the items in the index and check if the data loss is reflected correctly
            self.sleep(2)
        except Exception as ex:
            raise
        finally:
            self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = False, drop_index = True,
            query_with_explain = False, query = False)

    def test_tombstone_removal_impact(self):
        #Initialization operation
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = True, drop_index = False,
            query_with_explain = False, query = False)
        self.sleep(20)
        self._verify_bucket_count_with_index_count()
        try:
            # Run operations expiry and deletion
            self.run_doc_ops()
            tasks = []
            # Run auto-compaction to remove the tomb stones
            for bucket in self.buckets:
                tasks.append(self.cluster.async_compact_bucket(self.master, bucket))
            for task in tasks:
                task.result()
            self.sleep(10)
            # run compaction and analyze results
            self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions,
                create_index = True, drop_index = False, query_with_explain = True, query = True)
        except Exception as ex:
            self.log.info(str(ex))
            raise
        finally:
            self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = False, drop_index = True,
            query_with_explain = False, query = False)

    def test_maxttl_setting(self):
        """
        Load data, create index, check if all docs are indexed
        Wait until maxttl has elapsed, check if all docs are deleted
        and the deletes are indexed
        :return:
        """
        maxttl = int(self.input.param("maxttl", None))
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = True, drop_index = False,
            query_with_explain = False, query = False)
        self.sleep(20)
        self._verify_bucket_count_with_index_count()
        self.sleep(maxttl, "waiting for docs to be expired automatically per maxttl rule")
        self._expiry_pager(self.master)
        self.sleep(60, "wait for expiry pager to run on all nodes...")
        for bucket in self.buckets:
            items = RestConnection(self.master).get_active_key_count(bucket)
            self.log.info("Docs in source bucket is {0} after maxttl has elapsed".format(items))
            if items != 0:
                self.fail("Docs in source bucket is not 0 after maxttl has elapsed")
        self._verify_bucket_count_with_index_count()
