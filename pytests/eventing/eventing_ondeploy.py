from pytests.eventing.eventing_constants import HANDLER_CODE_ONDEPLOY
from pytests.eventing.eventing_base import EventingBaseTest
import logging
import json

log = logging.getLogger()

class EventingOndeploy(EventingBaseTest):
    def setUp(self):
        super(EventingOndeploy, self).setUp()
        handler_code = self.input.param('handler_code', None)
        if handler_code == 'ondeploy_failure':
            self.handler_code = HANDLER_CODE_ONDEPLOY.ONDEPLOY_FAILURE
        elif handler_code == 'ondeploy_failure_timeout':
            self.handler_code = HANDLER_CODE_ONDEPLOY.ONDEPLOY_FAILURE_TIMEOUT

    def tearDown(self):
        super(EventingOndeploy, self).tearDown()

    def test_eventing_ondeploy_bucket_operations(self):
        '''
        Basic Bucket Operations
        Advanced Bucket Accessors (Advanced GET/INSERT/UPSERT/REPLACE/DELETE)
        Bucket Cache
        Subdoc Operations
        '''
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_ADVANCED_BUCKET_OP)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        # One doc added for each operation. Expected doc count is 4
        self.verify_doc_count_collections("default.scope0.collection1", 4)
        self.undeploy_and_delete_function(body)

    def test_eventing_ondeploy_curl_timers_timeout(self):
        '''
        CURL Request
        Timeout
        Timers
        '''
        #self.load_data_to_collection(self.docs_per_day * self.num_docs, "default.scope0.collection0")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_CURL)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        # One doc added for each operation. Expected doc count is 3
        self.verify_doc_count_collections("default.scope0.collection1", 3)
        self.undeploy_and_delete_function(body)

    def test_eventing_ondeploy_function_xattrs(self):
        '''
        Checking xattrs attribute
        '''
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_XATTRS)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        # One doc added for each operation. Expected doc count is 1
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self.undeploy_and_delete_function(body)

    def test_eventing_ondeploy_source_bucket_mutation(self):
        '''
        Checking mutations on the source bucket
        '''
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_SOURCE_BUCKET_MUTATION)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        # One doc added for each operation. Expected doc count is 1
        self.verify_doc_count_collections("default.scope0.collection0", 1)
        self.undeploy_and_delete_function(body)

    def test_eventing_ondeploy_n1ql(self):
        '''
        N1QL Query
        '''
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_N1QL)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        # One doc added for each query. Expected doc count is 1
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self.undeploy_and_delete_function(body)

    def test_eventing_ondeploy_analytics(self):
        '''
        Analytics Query
        '''
        #load travel-sample bucket
        self.load_sample_buckets(self.server,"travel-sample")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_ANALYTICS)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        # One doc added for each query. Expected doc count is 1
        self.verify_doc_count_collections("default.scope0.collection1", 1)
        self.undeploy_and_delete_function(body)

    def test_eventing_ondeploy_function_pause_resume(self):
        '''
        Ondeploy triggered when an eventing function is resumed
        '''
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_BASIC_BUCKET_OP)
        # 1st doc added when the function is deployed
        self.deploy_function(body)
        self.pause_function(body)
        self.resume_function(body) #ondpely would be triggered
        # 2nd doc added when the function is resumed
        # Expected doc count = 2
        self.verify_doc_count_collections("default.scope0.collection1", 2)
        self.undeploy_and_delete_function(body)

    def test_eventing_ondeploy_default_params_check(self):
        '''
        Checking if the default value of on_deploy_timeout is 60
        '''
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_BASIC_BUCKET_OP)
        self.deploy_function(body)
        stats = self.rest.get_function_details(self.function_name, self.function_scope)
        stats_dict = json.loads(stats)
        print("Checking if the default value of on_deploy_timeout is 60.")
        assert stats_dict['settings']['on_deploy_timeout'] == 60, f"Invalid timeout value: {stats_dict['settings']['on_deploy_timeout']}. Expected 60."
        self.undeploy_and_delete_function(body)

    def test_eventing_ondeploy_failure_check(self):
        '''
        error in Ondeploy due to:
            - incorrect javascript code
            - small timeout value
        '''
        # Error in Javascript code/small timeout value, function should not be deployed
        body = self.create_save_function_body(self.function_name, self.handler_code)
        try:
            self.deploy_function(body)
            self.wait_for_handler_state(self.function_name, "deployed")
        except Exception as e:
            print(f"Error while waiting for handler state to be deployed: {str(e)}")
        self.delete_function(body)

    def test_eventing_ondeploy_dropping_function_scope_when_handler_is_deployed(self):
        '''
        Dropping the function scope and checking if the function is getting undeployed
        '''
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_BASIC_BUCKET_OP)
        self.deploy_function(body)
        # drop function scope
        self.rest.delete_bucket(self.src_bucket_name)
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body)

    def test_eventing_ondeploy_dropping_metadata_keyspace_when_handler_is_deployed(self):
        '''
        Dropping the metadata keyspace and checking if the function is getting undeployed
        '''
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ONDEPLOY.ONDEPLOY_BASIC_BUCKET_OP)
        self.deploy_function(body)
        # drop metadata keyspace
        self.rest.delete_bucket(self.metadata_bucket_name)
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body)