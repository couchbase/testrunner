from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_CURL
from pytests.eventing.eventing_base import EventingBaseTest
import logging, json, os

log = logging.getLogger()


class EventingSettings(EventingBaseTest):
    def setUp(self):
        super(EventingSettings, self).setUp()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.gens_load = self.generate_docs(self.docs_per_day)
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == 'bucket_op_with_cron_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMERS
        elif handler_code == 'n1ql_op_with_timers':
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITH_TIMERS
        elif handler_code == 'source_bucket_mutation':
            self.handler_code = HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION
        elif handler_code == 'source_bucket_mutation_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_WITH_TIMERS
        elif handler_code == 'bucket_op_curl_get':
            self.handler_code = HANDLER_CODE_CURL.BUCKET_OP_WITH_CURL_GET
        elif handler_code == 'timer_op_curl_post':
            self.handler_code = HANDLER_CODE_CURL.TIMER_OP_WITH_CURL_POST
        else:
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE

    def tearDown(self):
        super(EventingSettings, self).tearDown()

    def test_eventing_with_non_default_setting_values(self):
        # No of docs should be a multiple of sock_batch_size for eventing to process all mutations
        # doc size will 2016, so setting sock_batch_size as 8 so all the docs gets processed
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              sock_batch_size=8, worker_count=8, cpp_worker_thread_count=8,
                                              checkpoint_interval=5000, tick_duration=2000)
        self.deploy_function(body)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, 2*self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # This sleep is intentionally added, undeploy takes some time to cleanup the eventing-consumer's
        self.sleep(60)
        # Ensure that all consumers are cleaned up
        # This step is added because of MB-26846
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_with_changing_log_level_repeatedly(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body['settings']['deployment_status']=True
        body['settings']['processing_status']=True
        # dynamically change the log level
        # currently this is the only setting that can be dynamically modified when a function is deployed
        for i in range(5):
            for log_level in ['TRACE', 'INFO', 'ERROR', 'WARNING', 'DEBUG']:
                body['settings']['log_level'] = log_level
                log.info("Changing log level to {0}".format(log_level))
                self.rest.set_settings_for_function(body['appname'], body['settings'], self.function_scope)
                self.sleep(5)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, 2*self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # This sleep is intentionally added, undeploy takes some time to cleanup the eventing-consumer's
        self.sleep(60)
        # Ensure that all consumers are cleaned up
        # This step is added because of MB-26846
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_bindings_and_description_change_propagate_after_function_is_deployed(self):
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        # remove alias before deploying
        del body['depcfg']['buckets']
        # deploy a function without any alias
        self.deploy_function(body)
        # undeploy the function
        self.undeploy_function(body)
        # This is an important sleep, without this undeploy doesn't finish properly and subsequent deploy hangs
        self.sleep(30)
        # Add an alias and change the description
        body['settings']['description'] = "Adding a new description"
        body['depcfg']['buckets'] = []
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name})
        if self.is_sbm:
            body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name,"access": "rw"})
        # For new alias values to propagate we need to deploy the function again.
        self.deploy_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, 2*self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_bindings_and_description_change_propagate_after_function_is_resumed(self):
        # load documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        # remove alias before deploying
        del body['depcfg']['buckets']
        # deploy a function without any alias
        self.rest.create_function(body['appname'],body)
        self.deploy_function(body)
        # make sure no doc in destination bucket
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        # pause the function
        self.pause_function(body)
        #update bucket settings
        body1=self.rest.get_function_details(body['appname'], self.function_scope)
        body1=json.loads(body1)
        del body1['settings']['dcp_stream_boundary']
        body1['settings']['description'] = "Adding a new description"
        body1['depcfg']['buckets'] = []
        body1['depcfg']['buckets'].append({"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name})
        self.rest.update_function(body['appname'], body1, self.function_scope)
        # update all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='update')
        # For new alias values to propagate we need to deploy the function again.
        self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name,  self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_handler_change_then_function_is_resumed(self):
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        # deploy a function without any alias
        self.deploy_function(body)
        #verify the documents
        self.verify_eventing_results(self.function_name,  self.docs_per_day * 2016, skip_stats_validation=True)
        # pause the function
        self.pause_function(body)
        # This is an important sleep, without this undeploy doesn't finish properly and subsequent deploy hangs
        self.sleep(30)
        # update bucket settings
        body1 = self.rest.get_function_details(body['appname'], self.function_scope)
        body1 = json.loads(body1)
        del body1['settings']['dcp_stream_boundary']
        body1['settings']['description'] = "Adding a new description"
        body1['depcfg']['buckets'] = []
        body1['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name,"access": "rw"})
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION)
        fh = open(abs_file_path, "r")
        body1['appcode'] = fh.read()
        self.rest.update_function(body['appname'], body1, self.function_scope)
        # update all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='update')
        # For new alias values to propagate we need to deploy the function again.
        self.resume_function(body)
        self.is_sbm=True
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name,  self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    #MB-31146
    def test_default_log_level(self):
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        #remove log level
        del body['settings']['log_level']
        # deploy a function without any alias
        self.deploy_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, 2*self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_timer_context_max_size_less_than_20b(self):
        try:
            body = self.create_save_function_body(self.function_name, self.handler_code)
            body['settings']['timer_context_size']=19
            self.rest.create_function(body['appname'], body, self.function_scope)
        except Exception as e:
            self.log.info(e)
            assert "ERR_INVALID_REQUEST" in str(e) and "timer_context_size value should be greater than or equal to 20" in str(e), True

    def test_timer_context_max_size_greater_than_20mb(self):
        try:
            body = self.create_save_function_body(self.function_name, self.handler_code)
            body['settings']['timer_context_size']=200000000
            self.rest.create_function(body['appname'], body, self.function_scope)
        except Exception as e:
            self.log.info(e)
            assert "ERR_INVALID_REQUEST" in str(e) and "timer_context_size value should be less than or equal to 2.097152e+07" in str(e), True

    def test_timer_context_max_size_less_than_handler_size(self):
        body = self.create_save_function_body(self.function_name, "handler_code/timer_context_size.js")
        body['settings']['timer_context_size'] = 20
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_timer_context_max_size_value_empty(self):
        try:
            body = self.create_save_function_body(self.function_name, self.handler_code)
            body['settings']['timer_context_size']=None
            self.rest.create_function(body['appname'], body, self.function_scope)
        except Exception as e:
            self.log.info(e)
            assert "ERR_INVALID_REQUEST" in str(e) and "timer_context_size expected interger value" in str(e), True

    def test_bindings_to_non_existent_buckets(self):
        try:
            body = self.create_save_function_body(self.function_name, self.handler_code)
            body['depcfg']['buckets'].append({"alias": "new_bucket", "bucket_name": "new_bucket", "access": "rw"})
            self.rest.create_function(body['appname'], body, self.function_scope)
        except Exception as e:
            self.log.info(e)
            assert "ERR_BUCKET_MISSING" in str(e), True

    def test_update_bindings_via_config_api_call(self):
        # load documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        # remove bucket bindings
        del body['depcfg']['buckets']
        body['depcfg']['buckets'] = []
        body['depcfg']['buckets'].append(
            {"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name, "scope_name": "_default",
             "collection_name": "_default", "access": "rw"})
        body['depcfg']['metadata_scope'] = "_default"
        body['depcfg']['metadata_collection'] = "_default"
        body['depcfg']['source_scope'] = "_default"
        body['depcfg']['source_collection'] = "_default"
        # update handler config via new api call
        self.rest.update_eventing_config_per_function(body['depcfg'], self.function_name, self.function_scope)
        log.info(self.rest.get_eventing_config_per_function(self.function_name, self.function_scope))
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_create_function_without_settings(self):
        try:
            body = self.create_save_function_body(self.function_name, self.handler_code)
            del body['settings']
            self.rest.create_function(body['appname'], body, self.function_scope)
        except Exception as e:
            self.log.info(e)
            assert "ERR_INVALID_CONFIG" in str(e) and "processing_status is required" in str(e), True
