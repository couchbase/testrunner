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
        if self.create_functions_buckets:
            self.bucket_size = 100
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=self.num_replicas)
            self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.expiry = 3
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == 'bucket_op_with_cron_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMERS
        elif handler_code == 'n1ql_op_with_timers':
            # index is required for delete operation through n1ql
            self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
            self.n1ql_helper = N1QLHelper(shell=self.shell,
                                          max_verify=self.max_verify,
                                          buckets=self.buckets,
                                          item_flag=self.item_flag,
                                          n1ql_port=self.n1ql_port,
                                          full_docs_list=self.full_docs_list,
                                          log=self.log, input=self.input,
                                          master=self.master,
                                          use_rest=True
                                          )
            self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)
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
        try:
            self.cleanup_eventing()
        except:
            # This is just a cleanup API. Ignore the exceptions.
            pass
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
        # dynamically change the log level
        # currently this is the only setting that can be dynamically modified when a function is deployed
        for i in range(5):
            for log_level in ['TRACE', 'INFO', 'ERROR', 'WARNING', 'DEBUG']:
                body['settings']['log_level'] = log_level
                log.info("Changing log level to {0}".format(log_level))
                self.rest.set_settings_for_function(body['appname'], body['settings'])
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
        self.deploy_function(body)
        # make sure no doc in destination bucket
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        # pause the function
        self.pause_function(body)
        #update bucket settings
        body1=self.rest.get_function_details(body['appname'])
        body1=json.loads(body1)
        del body1['settings']['dcp_stream_boundary']
        body1['settings']['description'] = "Adding a new description"
        body1['depcfg']['buckets'] = []
        body1['depcfg']['buckets'].append({"alias": self.dst_bucket_name, "bucket_name": self.dst_bucket_name})
        self.rest.update_function(body['appname'], body1)
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
        body1 = self.rest.get_function_details(body['appname'])
        body1 = json.loads(body1)
        del body1['settings']['dcp_stream_boundary']
        body1['settings']['description'] = "Adding a new description"
        body1['depcfg']['buckets'] = []
        body1['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name,"access": "rw"})
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION)
        fh = open(abs_file_path, "r")
        body1['appcode'] = fh.read()
        self.rest.update_function(body['appname'], body1)
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