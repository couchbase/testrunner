import copy
import json
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest
import logging
from couchbase.bucket import Bucket

log = logging.getLogger()

class EventingSourceMutation(EventingBaseTest):
    def setUp(self):
        super(EventingSourceMutation, self).setUp()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.gens_load = self.generate_docs(self.docs_per_day)
        force_disable_new_orchestration = self.input.param('force_disable_new_orchestration', False)
        if force_disable_new_orchestration:
            self.rest.diag_eval("ns_config:set(force_disable_new_orchestration, true).")

    def tearDown(self):
        super(EventingSourceMutation, self).tearDown()

    def test_inter_handler_recursion(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                              worker_count=3)
        self.deploy_function(body)
        body1 = self.create_save_function_body(self.function_name+"_2", HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                              worker_count=3)
        try:
            self.deploy_function(body1,wait_for_bootstrap=False)
            raise Exception("Handler deployed even with inter handler recursion")
        except Exception as ex:
            self.log.info("Exception Thrown {}".format(ex))
            if "ERR_INTER_FUNCTION_RECURSION" in str(ex):
                pass
            else:
                self.fail("No inter handler recursion observed")
        self.undeploy_function(body)
        self.deploy_function(body1)
        try:
            self.deploy_function(body)
        except Exception as ex:
            if "ERR_INTER_FUNCTION_RECURSION" in str(ex):
                pass
            else:
                raise Exception("No inter handler recursion observed")
        self.undeploy_function(body1)


    def test_pause_resume_with_source_bucket_mutation(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION, worker_count=3)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016*2, skip_stats_validation=True)
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
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016*5, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    #MB-32516
    def test_indistinguishable_mutation(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                              worker_count=3)
        self.deploy_function(body)
        url = 'couchbase://{ip}/{name}'.format(ip=self.master.ip, name=self.src_bucket_name)
        bucket = Bucket(url, username="cbadminbucket", password="password")
        bucket.insert('customer123', {'some': 'value'})
        self.verify_eventing_results(self.function_name,2,skip_stats_validation=True)
        self.pause_function(body)
        bucket.replace('customer123', {'some': 'value1'})
        bucket.replace('customer123', {'some': 'value'})
        self.bucket_compaction()
        self.resume_function(body)
        self.verify_eventing_results(self.function_name,3,skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    #MB-34912
    def cycle_check_bypass(self):
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
            if "ERR_INTER_FUNCTION_RECURSION" in str(ex):
                pass
            else:
                raise Exception("No inter handler recursion observed")
        self.rest.allow_interbucket_recursion()
        try:
            self.deploy_function(body1)
        except Exception as ex:
            if "ERR_INTER_FUNCTION_RECURSION" in str(ex):
                raise Exception("Inter handler recursion observed even for allow_interbucket_recursion=true")
        self.undeploy_and_delete_function(body)
        self.undeploy_and_delete_function(body1)

    def test_runtime_recursion(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, 'handler_code/runtime_recursion_sbm.js', worker_count=3)
        self.deploy_function(body)
        matched, count=self.check_word_count_eventing_log(self.function_name,"Run time recursion from SBM for id:",1,return_count_only=True)
        if count > 1:
            self.fail("Seeing runtime recusrion in logs {}".format(count))
        self.undeploy_and_delete_function(body)


    def test_inter_handler_recursion_named_collections(self):
        body = self.create_function_with_collection(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                              src_namespace="src_bucket.src_bucket.src_bucket",
                                                    collection_bindings=["src_bucket.src_bucket.src_bucket.src_bucket.rw"])
        self.deploy_function(body)
        body1 = self.create_function_with_collection(self.function_name+"_2", HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                                     src_namespace="src_bucket.src_bucket.src_bucket",
                                                     collection_bindings=["src_bucket.src_bucket.src_bucket.src_bucket.rw"])
        try:
            self.deploy_function(body1,wait_for_bootstrap=False)
            raise Exception("Handler deployed even with inter handler recursion")
        except Exception as ex:
            self.log.info("Exception Thrown {}".format(ex))
            if "ERR_INTER_FUNCTION_RECURSION" in str(ex):
                pass
            else:
                self.fail("No inter handler recursion observed")
        self.undeploy_function(body)
        self.deploy_function(body1)
        try:
            self.deploy_function(body)
        except Exception as ex:
            if "ERR_INTER_FUNCTION_RECURSION" in str(ex):
                pass
            else:
                raise Exception("No inter handler recursion observed")
        self.undeploy_function(body1)

    def test_sbm_via_n1ql(self):
        try:
            body = self.create_save_function_body(self.function_name, "handler_code/sbm_with_n1ql.js", worker_count=3)
            self.deploy_function(body)
        except Exception as e:
            self.log.info(e)
            assert "ERR_INTER_BUCKET_RECURSION" in str(e), True
        finally:
            self.delete_function(body)

    # MB-53968
    def test_mutation_loss_during_deploy_undeploy_cycle_with_source_bucket_mutations(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, "handler_code/ABO/insert_sbm.js")
        self.deploy_function(body)
        self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs * 2)
        self.undeploy_function(body)
        self.deploy_function(body)
        self.verify_doc_count_collections("src_bucket._default._default", self.docs_per_day * self.num_docs * 4)
        self.undeploy_and_delete_function(body)
