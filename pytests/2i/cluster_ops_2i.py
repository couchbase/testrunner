from base_2i import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection
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
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = True, drop_index = False,
            query_with_explain = True, query = True)
        #Remove bucket and recreate it
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket.name)
        self.sleep(2)
        #Query and bucket with empty result set
        self.multi_query_using_index_with_emptyresult(query_definitions = self.query_definitions,
             buckets = self.buckets)

    def test_delete_create_bucket_and_query(self):
    	#Initialization operation
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = self.run_create_index, drop_index = False,
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
        except Exception, ex:
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
        self._verify_bucket_count_with_index_count()
        try:
            # Run operations expiry and deletion
            self.run_doc_ops()
            tasks = []
            # Run auto-compaction to remove the tomb stones
            for bucket in self.buckets:
                tasks.append(self.cluster.async_compact_bucket(self.master,bucket))
            for task in tasks:
                task.result()
            self.sleep(2)
            # run compaction and analyze results
            self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions,
                create_index = True, drop_index = False, query_with_explain = True, query = True)
        except Exception, ex:
            raise
        finally:
            self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = False, drop_index = True,
            query_with_explain = False, query = False)