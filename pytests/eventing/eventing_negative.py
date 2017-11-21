from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest, log


class EventingNegative(EventingBaseTest):
    def setUp(self):
        super(EventingNegative, self).setUp()
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
        super(EventingNegative, self).tearDown()

    def test_delete_function_when_function_is_in_deployed_state_and_which_is_already_deleted(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the create mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # Try deleting a function which is still in deployed state
        output, _ = self.delete_function(body)
        log.info("output from delete API before undeploying function: {0}".format(output))
        message = "Skipping delete request from temp store for app: {0} as it hasn't been undeployed".format(
            self.function_name)
        if message not in output:
            self.fail("Function delete succeeded even when function was in deployed state")
        self.undeploy_and_delete_function(body)
        # Try deleting a function which is already deleted
        message = "App: {0} not deployed".format(self.function_name)
        output, _ = self.delete_function(body)
        log.info("output from delete API after deleting function: {0}".format(output))
        if message not in output:
            self.fail("Function delete succeeded even when function was in deployed state")

    def test_deploy_function_where_source_metadata_and_destination_buckets_dont_exist(self):
        # delete source, metadata and destination buckets
        for bucket in self.buckets:
            self.rest.delete_bucket(bucket.name)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        content = self.rest.save_function(body['appname'], body)
        content1 = self.rest.deploy_function(body['appname'], body)
        log.info(content)
        log.info(content1)
        # Need meaningful error message
        # once MB-26935 is fixed, validate the message here

    def test_deploy_function_where_source_and_metadata_buckets_are_same(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        # set both src and metadatat bucket as same
        body['depcfg']['metadata_bucket'] = self.src_bucket_name
        self.rest.save_function(body['appname'], body)
        # Try to deploy the function
        content1 = self.rest.deploy_function(body['appname'], body)
        if "Source bucket same as metadata bucket" not in content1:
            self.fail("Eventing function allowed both source and metadata bucket to be same")
        self.undeploy_and_delete_function(body)

    def test_eventing_with_memcached_buckets(self):
        # delete existing couchbase buckets which will be created as part of setup
        for bucket in self.buckets:
            self.rest.delete_bucket(bucket.name)
        # create memcached bucket with the same name
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas)
        tasks = []
        for bucket in self.buckets:
            tasks.append(self.cluster.async_create_memcached_bucket(name=bucket.name,
                                                                    port=STANDARD_BUCKET_PORT + 1,
                                                                    bucket_params=bucket_params))
        for task in tasks:
            task.result()
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_ON_UPDATE, worker_count=3)
        self.rest.save_function(body['appname'], body)
        content1 = self.rest.deploy_function(body['appname'], body)
        if "Source bucket is memcached, should be either couchbase or ephemeral" not in content1:
            self.fail("Eventing function allowed both source and metadata bucket to be memcached buckets")
