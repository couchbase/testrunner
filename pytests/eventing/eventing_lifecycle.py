import json
import os

from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, EXPORTED_FUNCTION
from pytests.eventing.eventing_base import EventingBaseTest, log


class EventingLifeCycle(EventingBaseTest):
    def setUp(self):
        super(EventingLifeCycle, self).setUp()
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

    def tearDown(self):
        super(EventingLifeCycle, self).tearDown()

    def test_function_deploy_undeploy_in_a_loop_for_bucket_operations(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        for i in xrange(1, 5):
            self.cluster.bucket_flush(self.master, self.dst_bucket_name)
            self.deploy_function(body)
            self.undeploy_function(body)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_function_deploy_undeploy_in_a_loop_for_n1ql_operations(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE)
        for i in xrange(1, 5):
            self.cluster.bucket_flush(self.master, self.dst_bucket_name)
            self.deploy_function(body)
            self.undeploy_function(body)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_function_deploy_undeploy_in_a_loop_for_doc_timers(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        for i in xrange(1, 5):
            self.cluster.bucket_flush(self.master, self.dst_bucket_name)
            self.deploy_function(body)
            self.undeploy_function(body)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_function_pause_resume_in_a_loop_for_bucket_operations(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE)
        self.deploy_function(body)
        for i in xrange(1, 10):
            self.pause_function(body)
            # This sleep in intentionally put in a function
            self.sleep(5, "sleep for some seconds after pausing the function")
            self.resume_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_function_pause_resume_in_a_loop_for_n1ql_operations(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.N1QL_INSERT_ON_UPDATE)
        self.deploy_function(body)
        for i in xrange(1, 10):
            self.pause_function(body)
            # This sleep in intentionally put in a function
            self.sleep(5, "sleep for some seconds after pausing the function")
            self.resume_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_function_pause_resume_in_a_loop_for_doc_timers(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_DOC_TIMER)
        self.deploy_function(body)
        for i in xrange(1, 10):
            self.pause_function(body)
            # This sleep in intentionally put in a function
            self.sleep(5, "sleep for some seconds after pausing the function")
            self.resume_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)

    def test_export_function(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMER)
        self.deploy_function(body)
        # export the function that we have created
        output = self.rest.export_function(self.function_name)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # Validate that exported function data matches with the function that we created
        self.assertTrue(output["appname"] == self.function_name, msg="Function name mismatch from the exported function")
        self.assertTrue(output["appcode"] == body["appcode"], msg="Handler code mismatch from the exported function")
        self.assertTrue(cmp(output["settings"], body["settings"]) == 0,
                        msg="Settings mismatch from the exported function")
        self.undeploy_and_delete_function(body)

    def test_import_function(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # read the exported function
        script_dir = os.path.dirname(__file__)
        abs_file_path = os.path.join(script_dir, EXPORTED_FUNCTION.N1QL_INSERT_ON_UPDATE_WITH_CRON_TIMER)
        fh = open(abs_file_path, "r")
        body = json.loads(fh.read())
        # import the previously exported function
        # we don't have specific API for import, we reuse the API's
        self.rest.save_function("test_import_function", body)  # we have hardcoded function name as it's imported
        self.rest.deploy_function("test_import_function", body)  # we have hardcoded function name as it's imported
        self.wait_for_bootstrap_to_complete("test_import_function")  # we have hardcoded function name as it's imported
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results("test_import_function", self.docs_per_day * 2016)
        self.undeploy_and_delete_function(body)
