from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest
import logging

log = logging.getLogger()


class EventingBucket(EventingBaseTest):
    def setUp(self):
        super(EventingBucket, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=500)
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
        super(EventingBucket, self).tearDown()

    def test_eventing_with_ephemeral_buckets_with_lww_enabled(self):
        # delete existing couchbase buckets which will be created as part of setup
        for bucket in self.buckets:
            self.rest.delete_bucket(bucket.name)
        # create ephemeral buckets with the same name
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas,
                                                   bucket_type='ephemeral', eviction_policy='noEviction', lww=True)
        tasks = []
        for bucket in self.buckets:
            tasks.append(self.cluster.async_create_standard_bucket(name=bucket.name, port=STANDARD_BUCKET_PORT + 1,
                                                                   bucket_params=bucket_params))
        for task in tasks:
            task.result()
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_where_destination_bucket_is_in_dgm(self):
        # push the destination bucket to dgm
        total_items = self.push_to_dgm(self.dst_bucket_name, 20)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body)
        # load documents on the source bucket
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name,
                                     total_items + self.docs_per_day * 2016,  # add the data we have already created
                                     skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name,
                                     total_items,   # Since it will only delete the items created by eventing
                                     skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_where_source_bucket_is_in_dgm(self):
        # push the source bucket to dgm
        self.push_to_dgm(self.src_bucket_name, 50)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body)
        # load documents on the source bucket
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name,
                                     self.docs_per_day * 2016,  # since we have deployed with dcp boundary from_now
                                     skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_where_metadata_bucket_is_in_dgm(self):
        # push the metadata bucket to dgm
        self.push_to_dgm(self.metadata_bucket_name, 50)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body)
        # load documents on the source bucket
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name,
                                     self.docs_per_day * 2016,  # since we have deployed with dcp boundary from_now
                                     )
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_where_destination_bucket_is_in_warmup_state(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        # push the metadata bucket to dgm
        items_from_dgm = self.push_to_dgm(self.dst_bucket_name, 50)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body)
        try:
            # load documents on the source bucket
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
            self.kill_memcached_service(kv_node)
        except:
            pass
        # wait for bucket to come out of warm up state
        self.sleep(60)
        # print the stats couple of times to get the latest data
        for i in xrange(0, 5):
            self.print_execution_and_failure_stats()
            self.sleep(30)
        # delete the function
        self.undeploy_and_delete_function(body)
        curr_items = self.bucket_stat('curr_items', self.dst_bucket_name)
        no_of_eventing_ops_missed = items_from_dgm + self.docs_per_day * 2016 - curr_items
        log.info("No of docs mismatch because the bucket was in warmup state : {0}".format(no_of_eventing_ops_missed))
        # sleep intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_bucket_compaction_when_eventing_is_processing_mutations(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # start compacting source, metadata and destination buckets
        self.bucket_compaction()
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # start compacting source, metadata and destination buckets
        self.bucket_compaction()
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)


