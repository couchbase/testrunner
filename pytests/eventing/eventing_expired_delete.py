from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest, log
from membase.helper.cluster_helper import ClusterOperationHelper


class EventingExpired(EventingBaseTest):
    def setUp(self):
        super(EventingExpired, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=700)
        if self.create_functions_buckets:
            self.bucket_size = 200
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=0)
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
        query = "create primary index on {}".format(self.src_bucket_name)
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)

    def tearDown(self):
        super(EventingExpired, self).tearDown()


    def test_expired_mutation_pause_resume(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, exp=1)
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 10, bucket=self.src_bucket_name)
        body = self.create_save_function_body(self.function_name, "handler_code/bucket_op_expired.js", worker_count=3)
        self.deploy_function(body)
        self.pause_function(body)
        self.sleep(5)
        self.resume_function(body)
        # Wait for eventing to catch up with all the expiry mutations and verify results
        self.verify_eventing_results(self.function_name, 0, on_delete=True,skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_expired_type(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, "handler_code/expired_type.js")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_expired_value_for_delete(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, "handler_code/expired_type.js")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_expired_with_cancel_timer(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, exp=60)
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 10, bucket=self.src_bucket_name)
        body = self.create_save_function_body(self.function_name, "handler_code/cancel_timer_with_expiry.js")
        self.deploy_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)