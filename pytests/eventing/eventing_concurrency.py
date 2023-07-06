from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE, EXPORTED_FUNCTION
from pytests.eventing.eventing_base import EventingBaseTest
import logging

log = logging.getLogger()


class EventingConcurrency(EventingBaseTest):
    def setUp(self):
        super(EventingConcurrency, self).setUp()
        self._create_buckets(self.master, [self.dst_bucket_name1], bucket_size = self.bucket_size)
        self.buckets = self.rest.get_buckets()
        self.src_bucket = self.rest.get_bucket_by_name(self.src_bucket_name)
        self.gens_load = self.generate_docs(self.docs_per_day)

    def tearDown(self):
        super(EventingConcurrency, self).tearDown()

    def test_function_with_handler_code_with_bucket_operations_to_multiple_buckets(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.MULTIPLE_BUCKET_OPS_ON_UPDATE,
                                              worker_count=3, multi_dst_bucket=True)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, timeout=1200)
        self.dst_bucket_name = self.dst_bucket_name1
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, bucket=self.dst_bucket_name1,
                                     timeout=1200)
        self.undeploy_and_delete_function(body)

    def test_function_with_handler_code_with_multiple_timer_operations_with_bucket_operations_to_multiple_buckets(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name,
                                              HANDLER_CODE.MULTIPLE_TIMER_OPS_OF_DIFFERENT_TYPE_ON_UPDATE,
                                              worker_count=3, multi_dst_bucket=True)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True, timeout=1200)
        self.dst_bucket_name = self.dst_bucket_name1
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True,
                                     bucket=self.dst_bucket_name1, timeout=1200)
        self.undeploy_and_delete_function(body)

    def test_function_with_handler_code_with_multiple_timer_operations_of_the_same_type(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name,
                                              HANDLER_CODE.MULTIPLE_TIMER_OPS_OF_SAME_TYPE_ON_UPDATE,
                                              worker_count=3, multi_dst_bucket=True)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True, timeout=1200)
        self.dst_bucket_name = self.dst_bucket_name1
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True,
                                     bucket=self.dst_bucket_name1, timeout=1200)
        self.undeploy_and_delete_function(body)

    def test_multiple_functions_at_the_same_time(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # deploy the first function
        body = self.create_save_function_body(self.function_name,
                                              HANDLER_CODE.BUCKET_OPS_ON_UPDATE,
                                              worker_count=3)
        self.deploy_function(body)
        # deploy the second function
        body1 = self.create_save_function_body(self.function_name + "_1",
                                               HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMER_WITH_SECOND_BUCKET,
                                               worker_count=3)
        # this is required to deploy multiple functions at the same time
        del body1['depcfg']['buckets'][0]
        body1['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1})
        self.rest.create_function(body1['appname'], body1)
        self.deploy_function(body1)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, doc_timer_events=True, timeout=1200)
        self.dst_bucket_name = self.dst_bucket_name1
        self.verify_eventing_results(self.function_name + "_1", self.docs_per_day * 2016, doc_timer_events=True,
                                     bucket=self.dst_bucket_name1, timeout=1200)
        self.undeploy_and_delete_function(body)
        self.undeploy_and_delete_function(body1)

    def test_function_with_handler_code_which_has_multiple_bindings_to_same_bucket(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name,
                                              HANDLER_CODE.MULTIPLE_ALIAS_BINDINGS_FOR_SAME_BUCKET,
                                              worker_count=3)
        # create an another alias for the same bucket
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_xdcr_and_indexing_with_eventing(self):
        rest_src = RestConnection(self.servers[0])
        rest_dst = RestConnection(self.servers[2])
        try:
            rest_src.remove_all_replications()
            rest_src.remove_all_remote_clusters()
            rest_src.add_remote_cluster(self.servers[2].ip, self.servers[2].port, self.servers[0].rest_username,
                                        self.servers[0].rest_password, "C2")
            rest_dst.create_bucket(bucket=self.src_bucket_name, ramQuotaMB=100)
            self.sleep(30)
            # setup xdcr relationship
            repl_id = rest_src.start_replication('continuous', self.src_bucket_name, "C2")
            if repl_id is not None:
                self.log.info("Replication created successfully")
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
            # deploy function
            self.deploy_function(body)
            # Wait for eventing to catch up with all the update mutations and verify results
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
            stats_xdcr_dst = rest_dst.get_bucket_stats(self.src_bucket_name)
            index_bucket_map = self.n1ql_helper.get_index_count_using_primary_index(self.buckets, self.n1ql_server)
            actual_count = index_bucket_map[self.src_bucket_name]
            log.info("No of docs in xdcr destination bucket : {0}".format(stats_xdcr_dst["curr_items"]))
            log.info("No of docs indexed by primary index: {0}".format(actual_count))
            if stats_xdcr_dst["curr_items"] != self.docs_per_day * 2016:
                self.fail("xdcr did not replicate all documents, actual : {0} expected : {1}".format(
                    stats_xdcr_dst["curr_items"], self.docs_per_day * 2016))
            if actual_count != self.docs_per_day * 2016:
                self.fail("Not all the items were indexed, actual : {0} expected : {1}".format(
                    actual_count, self.docs_per_day * 2016))
            # delete all documents
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
            # Wait for eventing to catch up with all the delete mutations and verify results
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
            stats_xdcr_dst = rest_dst.get_bucket_stats(self.src_bucket_name)
            index_bucket_map = self.n1ql_helper.get_index_count_using_primary_index(self.buckets, self.n1ql_server)
            actual_count = index_bucket_map[self.src_bucket_name]
            log.info("No of docs in xdcr destination bucket : {0}".format(stats_xdcr_dst["curr_items"]))
            log.info("No of docs indexed by primary index: {0}".format(actual_count))
            if stats_xdcr_dst["curr_items"] != 0:
                self.fail("xdcr did not replicate all documents, actual : {0} expected : {1}".format(
                    stats_xdcr_dst["curr_items"], 0))
            if actual_count != 0:
                self.fail("Not all the items were indexed, actual : {0} expected : {1}".format(actual_count, 0))
            self.undeploy_and_delete_function(body)
        finally:
            self.n1ql_helper.drop_primary_index(using_gsi=True, server=self.n1ql_server)
            rest_dst.delete_bucket()
