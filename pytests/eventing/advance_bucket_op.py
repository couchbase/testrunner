from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest, log
from membase.helper.cluster_helper import ClusterOperationHelper


class AdvanceBucketOp(EventingBaseTest):
    def setUp(self):
        super(AdvanceBucketOp, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=900)
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
            self.cluster.create_standard_bucket(name=self.dst_bucket_name1, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.cluster.create_standard_bucket(name=self.metadata_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                                bucket_params=bucket_params)
            self.buckets = RestConnection(self.master).get_buckets()
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.handler_code = self.input.param('handler_code',None)
        self.expiry = 3
        query = "create primary index on {}".format(self.src_bucket_name)
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        query = "create primary index on {}".format(self.dst_bucket_name)
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        query = "create primary index on {}".format(self.metadata_bucket_name)
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)

    def tearDown(self):
        super(AdvanceBucketOp, self).tearDown()

    def test_advance_bucket_op(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                 batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name, "access": "rw"})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_advance_bucket_op_with_expiry(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                 batch_size=self.batch_size,exp=120)
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 3, bucket=self.src_bucket_name)
        body = self.create_save_function_body(self.function_name,self.handler_code)
        body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name, "access": "rw"})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_increment_decrement(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name, "access": "rw"})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 + 1, skip_stats_validation=True)
        self.verify_counter(self.docs_per_day * 2016)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the deletes mutations and verify results
        self.verify_eventing_results(self.function_name, 1, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def verify_counter(self,count):
        query = "select count from dst_bucket"
        counter = self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        self.log.info(counter["results"][0]["count"])
        if counter["results"][0]["count"] != count:
            self.fail("counter value is not as expected {0} , actual value {1}".format(count,counter["results"][0]["count"]))

    def test_set_expiry(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_different_datatype(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1, "access": "rw"})
        self.rest.create_function(body['appname'],body)
        self.deploy_function(body)
        query = "insert into src_bucket (KEY, VALUE) VALUES (\"data\",\"doc created\")"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        self.verify_eventing_results(self.function_name,1, skip_stats_validation=True)
        self.verify_eventing_results(self.function_name,1, skip_stats_validation=True,bucket=self.dst_bucket_name1)
        self.undeploy_and_delete_function(body)

    def test_different_datatype_insert(self):
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        query = "insert into src_bucket (KEY, VALUE) VALUES (\"data\",\"doc created\")"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        self.verify_eventing_results(self.function_name,7, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_incorrect_param_type(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        body['depcfg']['buckets'].append(
            {"alias": self.dst_bucket_name1, "bucket_name": self.dst_bucket_name1, "access": "rw"})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,
                                     bucket=self.dst_bucket_name1)
        self.undeploy_and_delete_function(body)

    def test_cas_errors(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_incorrect_bindings(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        body['depcfg']['buckets'].append(
            {"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name, "access": "rw"})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_increment_decrement_error(self):
        query = "insert into src_bucket (KEY, VALUE) VALUES (\"counter\",{\"count\": \"abc\"})"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        body = self.create_save_function_body(self.function_name, self.handler_code,dcp_stream_boundary="from_now")
        body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name,
                                          "access": "rw"})
        self.rest.create_function(body['appname'], body, self.function_scope)
        self.deploy_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the deletes mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)