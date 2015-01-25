from base_2i import BaseSecondaryIndexingTests
from membase.api.rest_client import RestConnection, RestHelper

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
            create_index = self.run_create_index, drop_index = False,
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
        self.sleep(60)
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
        self.sleep(60)
        #Flush bucket and recreate it
        self._bucket_creation()
        self.sleep(60)
        #Query and bucket with empty result set
        self.multi_query_using_index_with_emptyresult(
            query_definitions = self.query_definitions, buckets = self.buckets)
        #Verify the result set is empty
        self.verify_index_absence(query_definitions = self.query_definitions, buckets = self.buckets)



