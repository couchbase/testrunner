from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest
import logging

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
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
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
        for i in xrange(5):
            for log_level in ['TRACE', 'INFO', 'ERROR', 'WARNING', 'DEBUG']:
                body['settings']['log_level'] = log_level
                log.info("Changing log level to {0}".format(log_level))
                self.rest.set_settings_for_function(body['appname'], body['settings'])
                self.sleep(5)
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
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
        self.verify_eventing_results(self.function_name,  self.docs_per_day * 2016, skip_stats_validation=True)
        # delete json documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)