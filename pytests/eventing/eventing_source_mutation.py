import copy
import json
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest
import logging

log = logging.getLogger()

class EventingSourceMutation(EventingBaseTest):
    def setUp(self):
        super(EventingSourceMutation, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=700)
        if self.create_functions_buckets:
            self.replicas = self.input.param("replicas", 0)
            self.bucket_size = 100
            # This is needed as we have increased the context size to 93KB. If this is not increased the metadata
            # bucket goes into heavy DGM
            self.metadata_bucket_size = 400
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=self.replicas)
            bucket_params_meta = self._create_bucket_params(server=self.server, size=self.metadata_bucket_size,
                                                       replicas=self.replicas)
            self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.src_bucket = RestConnection(self.master).get_buckets()
            self.cluster.create_standard_bucket(name=self.dst_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params_meta)
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
        elif handler_code == 'n1ql_op_without_timers':
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
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITHOUT_TIMERS
        elif handler_code == 'source_bucket_mutation':
            self.handler_code = HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION
        else:
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        force_disable_new_orchestration = self.input.param('force_disable_new_orchestration', False)
        if force_disable_new_orchestration:
            self.rest.diag_eval("ns_config:set(force_disable_new_orchestration, true).")

    def tearDown(self):
        try:
            self.print_go_routine_dump_from_all_eventing_nodes()
        except:
            # This is just a go routine dump API. Ignore the exceptions.
            pass
        try:
            self.print_eventing_stats_from_all_eventing_nodes()
        except:
            # This is just a stats API. Ignore the exceptions.
            pass
        super(EventingSourceMutation, self).tearDown()

    def test_inter_handler_recursion(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                              worker_count=3)
        self.deploy_function(body)
        body1 = self.create_save_function_body(self.function_name+"_2", HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                              worker_count=3)
        try:
            self.deploy_function(body1)
        except Exception as ex:
            if "Inter Handler Recursion Error" in str(ex):
                pass
            else:
                raise Exception("No inter handler recursion observed")
        self.undeploy_function(body)
        self.deploy_function(body1)
        try:
            self.deploy_function(body)
        except Exception as ex:
            if "Inter Handler Recursion Error" in str(ex):
                pass
            else:
                raise Exception("No inter handler recursion observed")


    def test_pause_resume_with_source_bucket_mutation(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION, worker_count=3)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.pause_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(60)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")
        self.gens_load = self.generate_docs(self.docs_per_day*2)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size*2)
        self.resume_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016*4, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)