import json
import os
import logging
import re

from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, EXPORTED_FUNCTION, HANDLER_CODE_CURL
from pytests.eventing.eventing_base import EventingBaseTest, log

log = logging.getLogger()


class EventingTimers(EventingBaseTest):
    def setUp(self):
        super(EventingTimers, self).setUp()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.handler_code = self.input.param('handler_code', None)

    def tearDown(self):
        super(EventingTimers, self).tearDown()

    def test_null_refrence(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, "handler_code/null_timer_refrence.js",
                                              worker_count=1)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_different_context_type(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code, worker_count=1)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_timer_creation_error(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code, worker_count=1)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_timer_execution_time(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code, worker_count=1)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True)
        self.undeploy_and_delete_function(body)

    def test_multiple_timers_with_same_callback_reference(self):
        self.load_data_to_collection(self.docs_per_day * self.num_docs, "src_bucket._default._default")
        body = self.create_save_function_body(self.function_name, self.handler_code, worker_count=1)
        self.deploy_function(body)
        self.verify_doc_count_collections("dst_bucket._default._default", 1)
        self.undeploy_and_delete_function(body)
