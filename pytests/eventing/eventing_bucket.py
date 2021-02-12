import copy

from lib.couchbase_helper.stats_tools import StatsCommon
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from lib.couchbase_helper.documentgenerator import JSONNonDocGenerator, BlobGenerator
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest
import logging

log = logging.getLogger()


class EventingBucket(EventingBaseTest):
    def setUp(self):
        super(EventingBucket, self).setUp()
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=700)
        if self.create_functions_buckets:
            self.bucket_size = 100
            self.metadata_bucket_size = 400
            log.info(self.bucket_size)
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                       replicas=self.num_replicas)
            bucket_params_meta = self._create_bucket_params(server=self.server, size=self.metadata_bucket_size,
                                                            replicas=self.num_replicas)
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
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITH_TIMERS
        elif handler_code == 'source_bucket_mutation':
            self.handler_code = HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION
        elif handler_code == 'source_bucket_mutation_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OP_SOURCE_BUCKET_MUTATION_WITH_TIMERS
        else:
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE
        # index is required for delete operation through n1ql
        self.n1ql_node = self.get_nodes_from_services_map(service_type="n1ql")
        self.n1ql_helper = N1QLHelper(shell=self.shell, max_verify=self.max_verify, buckets=self.buckets,
                                      item_flag=self.item_flag, n1ql_port=self.n1ql_port,
                                      full_docs_list=self.full_docs_list, log=self.log, input=self.input,
                                      master=self.master, use_rest=True)
        self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)

    def tearDown(self):
        try:
            self.cleanup_eventing()
        except:
            # This is just a cleanup API. Ignore the exceptions.
            pass
        super(EventingBucket, self).tearDown()

    def test_eventing_with_ephemeral_buckets_with_lww_enabled(self):
        # delete existing couchbase buckets which will be created as part of setup
        for bucket in self.buckets:
            # Having metadata bucket as an ephemeral bucket is a bad idea
            if bucket.name != "metadata":
                self.rest.delete_bucket(bucket.name)
        # create ephemeral buckets with the same name
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas,
                                                   bucket_type='ephemeral', eviction_policy='noEviction', lww=True)
        tasks = []
        for bucket in self.buckets:
            # Having metadata bucket as an ephemeral bucket is a bad idea
            if bucket.name != "metadata":
                tasks.append(self.cluster.async_create_standard_bucket(name=bucket.name, port=STANDARD_BUCKET_PORT + 1,
                                                                       bucket_params=bucket_params))
        for task in tasks:
            task.result()
        try:
            # load data
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size)
        except:
            pass
        self.n1ql_helper.drop_primary_index(server=self.n1ql_node)
        self.n1ql_helper.create_primary_index(server=self.n1ql_node)
        body = self.create_save_function_body(self.function_name, self.handler_code,src_binding=True)
        self.deploy_function(body)
        stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True)
        try:
            # delete all documents
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
        except:
            pass
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_with_with_the_couchbase_buckets_in_heavy_dgm(self):
        gen_load_del = copy.deepcopy(self.gens_load)
        # load some data
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, self.gens_load,
                                                self.buckets[0].kvs[1], 'create', compression=self.sdk_compression)
        body = self.create_save_function_body(self.function_name, self.handler_code,src_binding=True)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        task.result()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete all documents
        task = self.cluster.async_load_gen_docs(self.master, self.src_bucket_name, gen_load_del,
                                                self.buckets[0].kvs[1], 'delete', compression=self.sdk_compression)
        if self.pause_resume:
            self.pause_function(body)
            self.sleep(30)
            self.resume_function(body)
        task.result()
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_where_destination_bucket_is_in_dgm(self):
        # push the destination bucket to dgm
        total_items = self.push_to_dgm(self.dst_bucket_name, 20)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              dcp_stream_boundary="from_now",src_binding=True)
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
                                     total_items,  # Since it will only delete the items created by eventing
                                     skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_where_source_bucket_is_in_dgm(self):
        self.skip_metabucket_check=True
        # push the source bucket to dgm
        self.push_to_dgm(self.src_bucket_name, 50)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              dcp_stream_boundary="from_now")
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # load documents on the source bucket
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True,expected_duplicate=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        if self.pause_resume:
            self.pause_function(body)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,expected_duplicate=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_where_metadata_bucket_is_in_dgm(self):
        # push the metadata bucket to dgm
        self.push_to_dgm(self.metadata_bucket_name, 50)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              dcp_stream_boundary="from_now",src_binding=True)
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
        self.skip_metabucket_check=True

    def test_eventing_where_destination_bucket_is_in_warmup_state(self):
        kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=False)
        # push the metadata bucket to dgm
        items_from_dgm = self.push_to_dgm(self.dst_bucket_name, 50)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              dcp_stream_boundary="from_now",src_binding=True)
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
        for i in range(0, 5):
            self.print_execution_and_failure_stats(self.function_name)
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
        gen_load_copy = copy.deepcopy(self.gens_load)
        if self.is_sbm:
            body = self.create_save_function_body(self.function_name, self.handler_code)
        else:
            body = self.create_save_function_body(self.function_name, self.handler_code, src_binding=True)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # start compacting source, metadata and destination buckets
        self.bucket_compaction()
        # load some data on metadata bucket while eventing is processing mutations
        # metadata bucket can be used for other purposes as well
        task = self.cluster.async_load_gen_docs(self.master, self.metadata_bucket_name, gen_load_copy,
                                                self.buckets[0].kvs[1], "create", compression=self.sdk_compression)
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        task.result()
        if self.pause_resume:
            self.pause_function(body)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # start compacting source, metadata and destination buckets
        self.bucket_compaction()
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        self.skip_metabucket_check=True

    def test_source_and_destination_bucket_interchanged(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        values = ['1', '10']
        gen_load_non_json = JSONNonDocGenerator('non_json_docs', values, start=0, end=2016 * self.docs_per_day)
        gen_load_non_json_del = copy.deepcopy(gen_load_non_json)
        self.cluster.load_gen_docs(self.master, self.dst_bucket_name, gen_load_non_json, self.buckets[0].kvs[1],
                                   'create', compression=self.sdk_compression)
        # deploy the first function
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE,
                                              worker_count=3)
        self.deploy_function(body)
        # deploy the second function
        body1 = self.create_save_function_body(self.function_name + "_1",
                                               HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE_INTERCHAGE,
                                               worker_count=3)
        # this is required to deploy multiple functions at the same time
        del body1['depcfg']['source_bucket']
        body1['depcfg']['source_bucket'] = self.dst_bucket_name
        del body1['depcfg']['buckets'][0]
        body1['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name})
        try:
            self.deploy_function(body1)
        except Exception as ex:
            if "ERR_INTER_BUCKET_RECURSION" in str(ex):
                pass
            else:
                raise Exception("No inter bucket recursion observed")
        self.undeploy_and_delete_function(body)

    def test_eventing_with_ephemeral_buckets_with_eviction_enabled(self):
        # delete src_bucket which will be created as part of setup
        self.rest.delete_bucket(self.src_bucket_name)
        # create source bucket as ephemeral bucket with the same name
        bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas, bucket_type='ephemeral',
                                                   eviction_policy='nruEviction')
        self.cluster.create_standard_bucket(name=self.src_bucket_name, port=STANDARD_BUCKET_PORT + 1,
                                            bucket_params=bucket_params)
        body = self.create_save_function_body(self.function_name, self.handler_code,src_binding=True)
        # deploy function
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        # load data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        try:
            # delete all documents
            self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                      batch_size=self.batch_size, op_type='delete')
        except:
            # since some of the docs are already ejected by eventing, load method will fails, hence ignoring failure
            pass
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
        self.verify_eventing_results(self.function_name, stats_src["curr_items"], skip_stats_validation=True,expected_duplicate=True)
        self.undeploy_and_delete_function(body)
        vb_active_auto_delete_count = StatsCommon.get_stats([self.master], self.src_bucket_name, '',
                                                            'vb_active_auto_delete_count')[self.master]
        if vb_active_auto_delete_count == 0:
            self.fail("No items were ejected from ephemeral bucket")
        else:
            log.info("Number of items auto deleted from ephemeral bucket is {0}".format(vb_active_auto_delete_count))
        # sleep intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    def test_eventing_where_source_and_destination_bucket_are_same(self):
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.SRC_AND_DST_BUCKET_ARE_SAME,
                                              worker_count=3)
        # create an alias so that src bucket is also destination bucket
        del body['depcfg']['buckets'][0]
        body['depcfg']['buckets'].append({"alias": self.dst_bucket_name, "bucket_name": self.src_bucket_name})
        self.deploy_function(body)
        # sleep intentionally added as we are validating no mutations are processed by eventing
        self.sleep(60)
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        # Undeploy and delete the function
        self.undeploy_and_delete_function(body)

    def test_eventing_where_we_read_source_bucket_data_from_handler_code(self):
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_READ_SOURCE_BUCKET,
                                              worker_count=3)
        # create an alias so that src bucket as well so that we can read data from source bucket
        body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name})
        self.deploy_function(body)
        # sleep intentionally added as we are validating no mutations are processed by eventing
        self.sleep(60)
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_eventing_with_different_compression_modes(self):
        compression_mode = self.input.param('compression_mode', 'passive')
        bucket_type = self.input.param('bucket_type', 'membase')
        # delete existing couchbase buckets which will be created as part of setup
        for bucket in self.buckets:
            self.rest.delete_bucket(bucket.name)
        # create buckets with the same name with different compression modes
        if bucket_type != "ephemeral":
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas,
                                                   bucket_type=bucket_type, compression_mode=compression_mode)
        else:
            bucket_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                   replicas=self.num_replicas,
                                                   bucket_type=bucket_type, compression_mode=compression_mode,
                                                   eviction_policy='noEviction')
        tasks = []
        for bucket in self.buckets:
            tasks.append(self.cluster.async_create_standard_bucket(name=bucket.name, port=STANDARD_BUCKET_PORT + 1,
                                                                   bucket_params=bucket_params))
        for task in tasks:
            task.result()
        if self.is_sbm:
            body = self.create_save_function_body(self.function_name, self.handler_code)
        else:
            body = self.create_save_function_body(self.function_name, self.handler_code,src_binding=True)
        stats_src = RestConnection(self.master).get_bucket_stats(bucket=self.src_bucket_name)
        self.deploy_function(body)
        if self.pause_resume:
            self.pause_function(body)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016 * 2, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        if self.pause_resume:
            self.pause_function(body)
        # delete all documents
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        if self.pause_resume:
            self.resume_function(body)
        # Wait for eventing to catch up with all the delete mutations and verify results
        # Wait for eventing to catch up with all the update mutations and verify results after rebalance
        if self.is_sbm:
            self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        else:
            self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        # intentionally added , as it requires some time for eventing-consumers to shutdown
        self.sleep(30)
        self.assertTrue(self.check_if_eventing_consumers_are_cleaned_up(),
                        msg="eventing-consumer processes are not cleaned up even after undeploying the function")

    #MB-31126
    def test_bucket_overhead(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMER,
                                              worker_count=3)
        # create an alias so that src bucket as well so that we can read data from source bucket
        body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name})
        self.deploy_function(body)
        # sleep intentionally added as we are validating no mutations are processed by eventing
        self.sleep(60)
        countMap=self.get_buckets_itemCount()
        initalDoc=countMap["metadata"]
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 2016*self.docs_per_day, skip_stats_validation=True)
        countMap = self.get_buckets_itemCount()
        finalDoc=countMap["metadata"]
        self.undeploy_and_delete_function(body)

    #MB-30973
    def test_cleanup_metadata(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMER,
                                              worker_count=3)
        # create an alias so that src bucket as well so that we can read data from source bucket
        body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name})
        self.deploy_function(body)
        # sleep intentionally added as we are validating no mutations are processed by eventing
        self.sleep(60)
        countMap = self.get_buckets_itemCount()
        initalDoc = countMap["metadata"]
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 2016 * self.docs_per_day, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        countMap = self.get_buckets_itemCount()
        finalDoc = countMap["metadata"]
        assert int(finalDoc) == 0



    def test_source_bucket_mutation_with_read_access(self):
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_WITH_SOURCE_BUCKET_MUTATION,
                                              worker_count=3)
        # create an alias so that src bucket is also destination bucket
        del body['depcfg']['buckets'][0]
        body['depcfg']['buckets'].append({"alias": self.src_bucket_name, "bucket_name": self.src_bucket_name})
        self.deploy_function(body)
        # sleep intentionally added as we are validating no mutations are processed by eventing
        self.sleep(60)
        self.verify_eventing_results(self.function_name, 2016 * self.docs_per_day, skip_stats_validation=True)
        # Undeploy and delete the function
        self.undeploy_and_delete_function(body)

    def test_langague_compatibility_6_0(self):
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name,'handler_code/language_compatibility_6_0.js',
                                              worker_count=3,language_compatibility='6.0.0')
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, 2016 * self.docs_per_day, skip_stats_validation=True)
        # Undeploy and delete the function
        self.undeploy_and_delete_function(body)

    def test_langague_compatibility_6_5(self):
        # load some data
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name,'handler_code/language_compatibility_6_5.js',
                                              worker_count=3)
        self.deploy_function(body)
        self.verify_eventing_results(self.function_name, 2016 * self.docs_per_day, skip_stats_validation=True)
        # Undeploy and delete the function
        self.undeploy_and_delete_function(body)
