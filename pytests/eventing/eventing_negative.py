import json

from couchbase_helper.tuq_helper import N1QLHelper
from lib.couchbase_helper.documentgenerator import BlobGenerator, JsonDocGenerator, JSONNonDocGenerator
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from couchbase.bucket import Bucket
from pytests.eventing.eventing_constants import HANDLER_CODE, HANDLER_CODE_ERROR
from pytests.eventing.eventing_base import EventingBaseTest
import logging

from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection

log = logging.getLogger()


class EventingNegative(EventingBaseTest):
    def setUp(self):
        super(EventingNegative, self).setUp()
        self.buckets = self.rest.get_buckets()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.gens_load = self.generate_docs(self.docs_per_day)

    def tearDown(self):
        super(EventingNegative, self).tearDown()

    def test_delete_function_when_function_is_in_deployed_state_and_which_is_already_deleted(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # Try deleting a function which is still in deployed state
        try:
            self.delete_function(body)
        except Exception as ex:
            log.info("output from delete API before undeploying function: {0}".format(str(ex)))
            message = "Skipping delete request from primary store for app: {0} as it hasn't been undeployed".format(
                self.function_name)
            if message not in str(ex):
                self.fail("Function delete succeeded even when function was in deployed state")
        self.undeploy_and_delete_function(body)
        try:
            # Try deleting a function which is already deleted
            self.delete_function(body)
        except Exception as ex:
            message = "App: {0} not deployed".format(self.function_name)
            if message not in str(ex):
                self.fail("Function delete succeeded even when function was in deployed state")

    def test_deploy_function_where_source_metadata_and_destination_buckets_dont_exist(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        # delete source, metadata and destination buckets
        for bucket in self.buckets:
            self.rest.delete_bucket(bucket.name)
        try:
            self.rest.deploy_function(body['appname'], body, self.function_scope)
        except Exception as ex:
            if "ERR_BUCKET_MISSING" not in str(ex):
                self.fail("Function save/deploy succeeded even when src/dst/metadata buckets doesn't exist")

    def test_deploy_function_where_source_and_metadata_buckets_are_same(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        # set both src and metadata bucket as same
        body['depcfg']['metadata_bucket'] = self.src_bucket_name
        try:
            self.rest.create_function(self.function_name,body)
        except Exception as ex:
            if "ERR_SRC_MB_SAME" not in str(ex):
                self.fail("Eventing function allowed both source and metadata bucket to be same")

    def test_src_metadata_and_dst_bucket_flush_when_eventing_is_processing_mutations(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        if self.pause_resume:
            self.pause_function(body)
        # flush source, metadata and destination buckets when eventing is processing_mutations
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket.name)
        # Undeploy and delete the function. In case of flush functions are not undeployed automatically
        self.undeploy_and_delete_function(body)
        # check if all the eventing-consumers are cleaned up
        # Validation of any issues like panic will be taken care by teardown method
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_src_metadata_and_dst_bucket_flush_for_source_bucket_mutation(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # flush source, metadata and destination buckets when eventing is processing_mutations
        for bucket in self.buckets:
            self.rest.flush_bucket(bucket.name)
        # Undeploy and delete the function. In case of flush functions are not undeployed automatically
        self.undeploy_and_delete_function(body)
        # check if all the eventing-consumers are cleaned up
        # Validation of any issues like panic will be taken care by teardown method
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    # See MB-30377
    def test_src_metadata_and_dst_bucket_delete_when_eventing_is_processing_mutations(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        if self.pause_resume:
            self.pause_function(body)
        # delete source, metadata and destination buckets when eventing is processing_mutations
        for bucket in self.buckets:
                self.log.info("deleting bucket: %s", bucket.name)
                self.rest.delete_bucket(bucket.name)
        # Wait for function to get undeployed automatically
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body)
        self.sleep(60)
        # check if all the eventing-consumers are cleaned up
        # Validation of any issues like panic will be taken care by teardown method
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    # See MB-30377
    def test_src_metadata_and_dst_bucket_delete_for_source_bucket_mutation(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        if self.pause_resume:
            self.pause_function(body)
        # delete source, metadata and destination buckets when eventing is processing_mutations
        for bucket in self.buckets:
            self.log.info("deleting bucket: %s", bucket.name)
            self.rest.delete_bucket(bucket.name)
        # Wait for function to get undeployed automatically
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body)
        self.sleep(60)
        # check if all the eventing-consumers are cleaned up
        # Validation of any issues like panic will be taken care by teardown method
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    # MB-30377
    def test_src_bucket_delete_when_eventing_is_processing_mutations(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # delete source, metadata and destination buckets when eventing is processing_mutations
        for bucket in self.buckets:
            if bucket.name == "src_bucket":
                self.log.info("deleting bucket: %s", bucket.name)
                self.rest.delete_bucket(bucket.name)
        # Wait for function to get undeployed automatically
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body)
        self.sleep(10)
        # check if all the eventing-consumers are cleaned up
        # Validation of any issues like panic will be taken care by teardown method
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")
        self.skip_metabucket_check = True

    # MB-29533 and MB-31545
    def test_metadata_bucket_delete_when_eventing_is_processing_mutations(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # delete source, metadata and destination buckets when eventing is processing_mutations
        for bucket in self.buckets:
            if bucket.name == "metadata":
                self.log.info("deleting bucket: %s", bucket.name)
                self.rest.delete_bucket(bucket.name)
        # Wait for function to get undeployed automatically
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body)
        self.sleep(60)
        # check if all the eventing-consumers are cleaned up
        # Validation of any issues like panic will be taken care by teardown method
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")
        self.skip_metabucket_check=True

    def test_undeploy_when_function_is_still_in_bootstrap_state(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        self.deploy_function(body, wait_for_bootstrap=False)
        body1 = {"count": 1}
        # Set retry to 1
        self.rest.set_eventing_retry(body['appname'], body1, self.function_scope)
        try:
            # Try undeploying the function when it is still bootstrapping
            self.undeploy_function(body)
        except Exception as ex:
            if "ERR_APP_NOT_DEPLOYED" not in str(ex):
                self.fail("Function undeploy succeeded even when function was in bootstrapping state")

    def test_function_where_handler_code_takes_more_time_to_execute_than_execution_timeout(self):
        # Note to Self : Never use SDK's unless you really have to. It is difficult to upgrade or maintain correct
        # sdk versions on the slaves. Scripts will be notoriously unreliable when you run on jenkins slaves.
        num_docs = 10
        values = ['1', '10']
        # create 10 non json docs on source bucket
        gen_load_non_json = JSONNonDocGenerator('non_json_docs', values, start=0, end=num_docs)
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_non_json, self.buckets[0].kvs[1],
                                   'create', compression=self.sdk_compression)
        # create a function which sleeps for 5 secs and set execution_timeout to 1s
        body = self.create_save_function_body(self.function_name, HANDLER_CODE_ERROR.EXECUTION_TIME_MORE_THAN_TIMEOUT,
                                              execution_timeout=10)
        # deploy the function
        self.deploy_function(body)
        # This is intentionally added so that we wait for some mutations to process and we decide none are processed
        self.sleep(100)
        # No docs should be present in dst_bucket as the all the function executions should have timed out
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        eventing_nodes = self.get_nodes_from_services_map(service_type="eventing", get_all_nodes=True)
        exec_timeout_count = 0
        for eventing_node in eventing_nodes:
            rest_conn = RestConnection(eventing_node)
            out = rest_conn.get_all_eventing_stats()
            # get sum of all timeout_count
            exec_timeout_count += out[0]["failure_stats"]["timeout_count"]
        # check whether all the function executions timed out and is equal to number of docs created
        if exec_timeout_count != num_docs:
            self.fail("Not all event executions timed out : Expected : {0} Actual : {1}".format(num_docs,
                                                                                                exec_timeout_count))
        self.undeploy_and_delete_function(body)

    def test_syntax_error(self):
        try:
            body = self.create_save_function_body(self.function_name, HANDLER_CODE.SYNTAX_ERROR)
        except Exception as e:
            if "ERR_HANDLER_COMPILATION" not in str(e):
                self.fail("Deployment is expected to be failed but no message of failure")

    def test_read_binary_data_from_the_function(self):
        gen_load_binary = BlobGenerator('binary1000000', 'binary', self.value_size, start=1,
                                        end=2016 * self.docs_per_day + 1)
        gen_load_json = JsonDocGenerator('binary', op_type="create", end=2016 * self.docs_per_day)
        # load binary data on dst bucket and non json on src bucket with identical keys so that we can read them
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_json, self.buckets[0].kvs[1], "create",
                                   exp=0, flag=0, batch_size=1000, compression=self.sdk_compression)
        self.cluster.load_gen_docs(self.master, self.dst_bucket_name, gen_load_binary, self.buckets[0].kvs[1], "create",
                                   exp=0, flag=0, batch_size=1000, compression=self.sdk_compression)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.READ_BUCKET_OP_ON_DST)
        self.deploy_function(body)
        # wait for some time so that exception_count increases
        # This is because we can't read binary data from handler code
        self.sleep(60)
        stats = self.rest.get_all_eventing_stats()
        bucket_op_exception_count = stats[0]["failure_stats"]["bucket_op_exception_count"]
        self.undeploy_and_delete_function(body)
        log.info("stats : {0}".format(json.dumps(stats, sort_keys=True, indent=4)))
        if bucket_op_exception_count == 0:
            self.fail("Reading binary data succeeded from handler code")

    def test_deploy_function_name_with_more_than_100_chars(self):
        # create a string of more than 100 chars
        function_name = "a" * 101
        try:
            body = self.create_save_function_body(function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        except Exception as e:
            if "Function name length must be less than 100" not in str(e):
                self.fail("Deployment is expected to be failed but succeeded with function name more than 100 chars")

    def test_deploy_function_name_with_special_chars(self):
        # create a string with space and other special chars
        err_msg = 'Function name can only start with characters in range A-Z, a-z, 0-9 and can only contain characters in range A-Z, a-z, 0-9, underscore and hyphen'
        try:
            body = self.create_save_function_body("abc@$", HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        except Exception as e:
            if err_msg not in str(e):
                self.fail("Deployment is expected to be failed when space is present in function name expected err message {}".format(err_msg))

    def test_deploy_function_invalid_alias_name(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        # Use an invalid alias
        body['depcfg']['buckets'].append({"alias": "908!@#$%%^&&**", "bucket_name": self.dst_bucket_name})
        try:
            self.rest.create_function(body['appname'], body, self.function_scope)
        except Exception as e:
            if "ERR_INVALID_CONFIG" not in str(e):
                log.info(str(e))
                self.fail("Deployment is expected to be failed but succeeded with function name more than 100 chars")

    def test_deploy_function_with_prefix_length_greater_than_16_chars(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        # Use an user_prefix greater than 16 chars
        body['settings']['user_prefix'] = "eventingeventingeventingeventingeventingeventingeventingeventingeventing"
        try:
            self.rest.create_function(body['appname'], body, self.function_scope)
        except Exception as e:
            if "ERR_INVALID_CONFIG" not in str(e):
                log.info(str(e))
                self.fail("Deployment is expected to be failed but succeeded with user_prefix greater than 16 chars")

    def test_pause_when_function_not_deployed(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        body['settings']['deployment_status'] = False
        body['settings']['processing_status'] = False
        self.rest.create_function(body['appname'], body, self.function_scope)
        try:
            self.pause_function(body)
            self.fail("application is paused even before deployment")
        except Exception as e:
            if "ERR_APP_NOT_BOOTSTRAPPED" not in str(e):
                log.info(str(e))
                self.fail("Not correct exception thrown")

    def test_pause_when_function_is_deploying(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body,wait_for_bootstrap=False)
        try:
            self.pause_function(body)
            self.fail("application is paused even before deployment")
        except Exception as e:
            if "ERR_APP_NOT_BOOTSTRAPPED" not in str(e):
                log.info(str(e))
                self.fail("Not correct exception thrown")
        self.wait_for_handler_state(body['appname'],"deployed")
        self.undeploy_and_delete_function(body)

    def test_delete_when_resume_in_progress(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        self.pause_function(body)
        self.resume_function(body, wait_for_resume=False)
        try:
            self.delete_function(body)
            self.fail("application is paused even before deployment")
        except Exception as e:
            if "ERR_APP_NOT_UNDEPLOYED" not in str(e):
                log.info(str(e))
                self.fail("Not correct exception thrown")
        self.wait_for_handler_state(body['appname'],"deployed")
        self.undeploy_and_delete_function(body)

    def test_delete_paused_handler(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        self.pause_function(body)
        try:
            self.delete_function(body)
            self.fail("application is paused even before deployment")
        except Exception as e:
            if "ERR_APP_NOT_UNDEPLOYED" not in str(e):
                log.info(str(e))
                self.fail("Not correct exception thrown")
        self.undeploy_function(body)

    def test_n1ql_DML_with_source_bucket(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_SOURCE_INSERT)
        try:
            self.deploy_function(body)
            self.fail("application is deployed for insert on source bucket")
        except Exception as e:
            log.info(str(e))
            if "ERR_INTER_BUCKET_RECURSION" not in str(e):
                self.fail("Not correct exception thrown")

    def test_n1ql_with_wrong_query(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                 batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, 'handler_code/n1ql_op_negative.js', worker_count=3)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016,skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, on_delete=True,skip_stats_validation=True)
        self.undeploy_and_delete_function(body)


    def test_n1ql_without_n1ql_node(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                 batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, 'handler_code/n1ql_op_no_n1ql.js', worker_count=3)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016,skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, on_delete=True,skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        self.skip_metabucket_check=True

    def test_n1ql_without_index_node(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                 batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, 'handler_code/n1ql_op_without_index.js', worker_count=3)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", self.docs_per_day * self.num_docs)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_doc_count_collections("dst_bucket._default._default", 0)
        self.undeploy_and_delete_function(body)

    def test_n1ql_max_connection(self):
        self.load_sample_buckets(self.server, "travel-sample")
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                 batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, 'handler_code/n1ql_op_connection.js', worker_count=1)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016,skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, on_delete=True,skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    #MB-32127
    def test_field_boundary_update_for_deployed_function(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3,dcp_stream_boundary="from_now")
        self.deploy_function(body)
        update_body={"deployment_status":True, "processing_status":True, "dcp_stream_boundary":"everything"}
        try:
            self.rest.set_settings_for_function(self.function_name,update_body, self.function_scope)
        except Exception as e:
            if "ERR_INVALID_CONFIG" not in str(e):
                raise Exception("Feed boundary updated when app is deployed")
        self.undeploy_and_delete_function(body)

    #MB-31140
    def test_eventing_error_type(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, 'handler_code/eventing_error.js', worker_count=1)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)


    #MB-31140
    def test_n1ql_error_type(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, 'handler_code/n1ql_error.js', worker_count=1)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    #MB-31140
    def test_curl_error_type(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, 'handler_code/curl_error.js', worker_count=1)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    #MB-35750
    def test_non_json(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, 'handler_code/non_json.js', worker_count=1)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016*3, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    # MB-31140
    def test_kv_error_type(self):
        #kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        self.skip_metabucket_check=True
        body = self.create_save_function_body(self.function_name, 'handler_code/kv_error.js', worker_count=1,execution_timeout=3)
        self.deploy_function(body)
        try:
            # partition the kv node when its processing mutations
            self.drop_data_to_bucket_from_eventing(self.servers[1])
            self.sleep(10)
            # load some data
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.sleep(700)
        except Exception:
            self.reset_firewall(self.servers[1])
        finally:
            self.reset_firewall(self.servers[1])
        matched, count=self.check_word_count_eventing_log(self.function_name,"KVError:",2016)
        if count == 0:
            raise Exception("No KV error shown up")
        on_update_success=self.get_stats_value(self.function_name, "execution_stats.on_update_success")
        dcp_mutation_msg_counter=self.get_stats_value(self.function_name, "execution_stats.dcp_mutation_msg_counter")
        self.log.info("execution_stats.on_update_success: {}".format(on_update_success))
        self.log.info("execution_stats.on_update_success: {}".format(dcp_mutation_msg_counter))
        if not (count == on_update_success == dcp_mutation_msg_counter):
            raise Exception("Kv error don't match with stats")


    def test_src_collection_delete_when_eventing_is_processing_mutations(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # delete source collection
        self.collection_rest.delete_collection(bucket="src_bucket",scope="src_bucket",collection="src_bucket")
        # Wait for function to get undeployed automatically
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body)
        self.sleep(60)
        # check if all the eventing-consumers are cleaned up
        # Validation of any issues like panic will be taken care by teardown method
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_src_scope_delete_when_eventing_is_processing_mutations(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # delete source collection
        self.collection_rest.delete_scope(bucket="src_bucket",scope="src_bucket")
        # Wait for function to get undeployed automatically
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body)
        self.sleep(60)
        # check if all the eventing-consumers are cleaned up
        # Validation of any issues like panic will be taken care by teardown method
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")


    def test_metadata_collection_delete_when_eventing_is_processing_mutations(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # delete source collection
        self.collection_rest.delete_collection(bucket="metadata",scope="metadata",collection="metadata")
        # Wait for function to get undeployed automatically
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body)
        self.sleep(60)
        # check if all the eventing-consumers are cleaned up
        # Validation of any issues like panic will be taken care by teardown method
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_metadata_scope_delete_when_eventing_is_processing_mutations(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket.src_bucket.src_bucket")
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # delete source collection
        self.collection_rest.delete_scope(bucket="metadata",scope="metadata")
        # Wait for function to get undeployed automatically
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body)
        self.sleep(60)
        # check if all the eventing-consumers are cleaned up
        # Validation of any issues like panic will be taken care by teardown method
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_src_bucket_deletion_when_function_is_in_deploying_state(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body, wait_for_bootstrap=False)
        # delete src bucket
        for bucket in self.buckets:
            if bucket.name == "src_bucket":
                self.log.info("deleting bucket: %s", bucket.name)
                self.rest.delete_bucket(bucket.name)
        # Wait for function to get undeployed automatically
        self.wait_for_handler_state(body['appname'], "undeployed")
        # Delete the function
        self.delete_function(body)
        self.sleep(10)
        # check if all the eventing-consumers are cleaned up
        # Validation of any issues like panic will be taken care by teardown method
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")
