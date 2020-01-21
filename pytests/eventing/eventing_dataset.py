# Intentionally adding 1 new line
# coding=utf-8
import copy
import logging
from couchbase.bucket import Bucket
from lib.couchbase_helper.documentgenerator import BlobGenerator, JsonDocGenerator, JSONNonDocGenerator
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.membase.helper.cluster_helper import ClusterOperationHelper
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_base import EventingBaseTest
from pytests.eventing.eventing_constants import HANDLER_CODE
import couchbase.subdocument as SD
from threading import Thread

log = logging.getLogger()


class EventingDataset(EventingBaseTest):
    def setUp(self):
        super(EventingDataset, self).setUp()
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
        handler_code = self.input.param('handler_code', 'bucket_op')
        if handler_code == 'bucket_op':
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE1
        elif handler_code == 'bucket_op_with_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_TIMERS
        elif handler_code == 'bucket_op_with_cron_timers':
            self.handler_code = HANDLER_CODE.BUCKET_OPS_WITH_CRON_TIMERS
        elif handler_code == 'n1ql_op_with_timers':
            self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)
            self.handler_code = HANDLER_CODE.N1QL_OPS_WITH_TIMERS
        else:
            self.handler_code = HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE1

    def tearDown(self):
        super(EventingDataset, self).tearDown()

    def test_functions_where_dataset_has_binary_and_json_data(self):
        gen_load = BlobGenerator('binary', 'binary-', self.value_size, end=2016 * self.docs_per_day)
        # load binary and json data
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load, self.buckets[0].kvs[1], 'create',
                                   exp=0, flag=0, batch_size=1000, compression=self.sdk_compression)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete both binary and json documents
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load, self.buckets[0].kvs[1], 'delete',
                                   exp=0, flag=0, batch_size=1000, compression=self.sdk_compression)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_functions_where_documents_change_from_binary_to_json_data(self):
        gen_load_binary = BlobGenerator('binary1000000', 'binary', self.value_size, start=1,
                                        end=2016 * self.docs_per_day + 1)
        gen_load_json = JsonDocGenerator('binary', op_type="create", end=2016 * self.docs_per_day)
        gen_load_binary_del = copy.deepcopy(gen_load_binary)
        gen_load_json_del = copy.deepcopy(gen_load_json)
        # load binary data
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_binary, self.buckets[0].kvs[1], "create",
                                   exp=0, flag=0, batch_size=1000, compression=self.sdk_compression)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # convert data from binary to json
        # use the same doc-id's as binary to update from binary to json
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_binary_del, self.buckets[0].kvs[1],
                                   'delete', batch_size=1000, compression=self.sdk_compression)
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_json, self.buckets[0].kvs[1], 'create',
                                   batch_size=1000, compression=self.sdk_compression)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete all json docs
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_json_del, self.buckets[0].kvs[1],
                                   'delete', batch_size=1000, compression=self.sdk_compression)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_functions_where_dataset_has_binary_and_non_json_data(self):
        gen_load_binary = BlobGenerator('binary', 'binary-', self.value_size, end=2016 * self.docs_per_day)
        values = ['1', '10']
        gen_load_non_json = JSONNonDocGenerator('non_json_docs', values, start=0, end=2016 * self.docs_per_day)
        gen_load_non_json_del = copy.deepcopy(gen_load_non_json)
        # load binary and non json data
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_binary, self.buckets[0].kvs[1], 'create',
                                   compression=self.sdk_compression)
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_non_json, self.buckets[0].kvs[1],
                                   'create', compression=self.sdk_compression)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete non json documents
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_non_json_del, self.buckets[0].kvs[1],
                                   'delete', compression=self.sdk_compression)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    # See MB-26706
    def test_eventing_where_dataset_has_different_key_types_using_sdk_and_n1ql(self):
        keys = [
            "1324345656778878089435468780879760894354687808797613243456567788780894354687808797613243456567788780894354687808797613243456567788780894354687808797613287808943546878087976132434565677887808943546878087976132434565677887808943546878087976132943546878",
            # max key size
            "1",  # Numeric key, see MB-26706
            "a1",  # Alphanumeric
            "1a",  # Alphanumeric
            "1 a b",  # Alphanumeric with space
            "1.234",  # decimal
            "~`!@  #$%^&*()-_=+{}|[]\:\";\'<>?,./",  # all special characters
            "\xc2\xa1 \xc2\xa2 \xc2\xa4 \xc2\xa5",  # utf-8 encoded characters
            "true",  # boolean key
            "false",  # boolean key
            "True",  # boolean key
            "False",  # boolean key
            "null",  # null key
            "undefined",  # undefined key
            # Check here for javascript builtin objects : https://mzl.la/1zDsM8O
            "NaN",
            "Symbol()",
            "Symbol(42)"
            "Symbol(\'foo\')",
            "isNaN",
            "Error",
            "Function",
            "Infinity",
            "Atomics",
            "Boolean",
            "ArrayBuffer",
            "DataView",
            "Date",
            "Generator {}",
            "InternalError",
            "Intl",
            "Number",
            "Math",
            "Map",
            "Promise",
            "Proxy"
        ]
        url = 'couchbase://{ip}/{name}'.format(ip=self.master.ip, name=self.src_bucket_name)
        bucket = Bucket(url, username="cbadminbucket", password="password")
        for key in keys:
            bucket.upsert(key, "Test with different key values")
        # create a doc using n1ql query
        query = "INSERT INTO  " + self.src_bucket_name + " ( KEY, VALUE ) VALUES ('key11111','from N1QL query')"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        body = self.create_save_function_body(self.function_name, self.handler_code)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, len(keys) + 1, skip_stats_validation=True)
        # delete all the documents with different key types
        for key in keys:
            bucket.remove(key)
        # delete a doc using n1ql query
        self.n1ql_helper.create_primary_index(using_gsi=True, server=self.n1ql_node)
        query = "DELETE FROM " + self.src_bucket_name + " where meta().id='key11111'"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)
        self.n1ql_helper.drop_primary_index(using_gsi=True, server=self.n1ql_node)

    def test_eventing_processes_mutations_when_mutated_through_subdoc_api_and_set_expiry_through_sdk(self):
        # set expiry pager interval
        ClusterOperationHelper.flushctl_set(self.master, "exp_pager_stime", 1, bucket=self.src_bucket_name)
        url = 'couchbase://{ip}/{name}'.format(ip=self.master.ip, name=self.src_bucket_name)
        bucket = Bucket(url, username="cbadminbucket", password="password")
        for docid in ['customer123', 'customer1234', 'customer12345']:
            bucket.insert(docid, {'some': 'value'})
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              dcp_stream_boundary="from_now")
        # deploy eventing function
        self.deploy_function(body)
        # upserting a new sub-document
        bucket.mutate_in('customer123', SD.upsert('fax', '775-867-5309'))
        # inserting a sub-document
        bucket.mutate_in('customer1234', SD.insert('purchases.complete', [42, True, None], create_parents=True))
        # Creating and populating an array document
        bucket.mutate_in('customer12345', SD.array_append('purchases.complete', ['Hello'], create_parents=True))
        self.verify_eventing_results(self.function_name, 3, skip_stats_validation=True)
        for docid in ['customer123', 'customer1234', 'customer12345']:
            # set expiry on all the docs created using sub doc API
            bucket.touch(docid, ttl=5)
        self.sleep(10, "wait for expiry of the documents")
        # Wait for eventing to catch up with all the expiry mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_eventing_processes_mutation_when_xattrs_is_updated(self):
        url = 'couchbase://{ip}/{name}'.format(ip=self.master.ip, name=self.src_bucket_name)
        bucket = Bucket(url, username="cbadminbucket", password="password")
        for docid in ['customer123', 'customer1234', 'customer12345']:
            bucket.upsert(docid, {})
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              dcp_stream_boundary="from_now")
        # deploy eventing function
        self.deploy_function(body)
        # update multiple xattrs and update the documents
        for docid in ['customer123', 'customer1234', 'customer12345']:
            bucket.mutate_in(docid, SD.upsert('my1', {'value': 1}, xattr=True))
            bucket.mutate_in(docid, SD.upsert('my2', {'value': 2}, xattr=True))
            bucket.mutate_in(docid, SD.upsert('fax', '775-867-5309'))
        self.verify_eventing_results(self.function_name, 3, skip_stats_validation=True)
        # add new multiple xattrs , delete old xattrs and delete the documents
        for docid in ['customer123', 'customer1234', 'customer12345']:
            bucket.mutate_in(docid, SD.upsert('my3', {'value': 3}, xattr=True))
            bucket.mutate_in(docid, SD.upsert('my4', {'value': 4}, xattr=True))
            bucket.mutate_in(docid, SD.remove('my3', xattr=True))
            bucket.mutate_in(docid, SD.remove('my4', xattr=True))
            bucket.remove(docid)
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    # See MB-27679
    def test_eventing_with_large_doc_size(self):
        document_size = self.input.param('document_size')
        data_chan_size = self.input.param('data_chan_size', 10)
        worker_queue_cap = self.input.param('worker_queue_cap', 10)
        # generate docs with size >=  1MB , See MB-27679
        gens_load = self.generate_docs_bigdata(self.docs_per_day, document_size=document_size)
        self.load(gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=10)
        body = self.create_save_function_body(self.function_name, self.handler_code,
                                              data_chan_size=data_chan_size, worker_queue_cap=worker_queue_cap)
        thread = Thread(target=self._change_eventing_quota, args=())
        thread.start()
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True,
                                     timeout=1200)
        self.load(gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=10, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True, timeout=1200)
        self.undeploy_and_delete_function(body)
        thread.join()

    def test_eventing_with_unicode_character_in_handler_code(self):
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_UNICODE_CHAR)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def _change_eventing_quota(self):
        for quota in range(256, 512, 4):
            self.rest.set_service_memoryQuota(service='eventingMemoryQuota', memoryQuota=quota)
            self.sleep(10)

    def test_eventing_does_not_use_xattrs(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OPS_WITH_TIMERS,
                                              dcp_stream_boundary="from_now")
        # deploy eventing function
        self.deploy_function(body)
        url = 'couchbase://{ip}/{name}'.format(ip=self.master.ip, name=self.src_bucket_name)
        bucket = Bucket(url, username="cbadminbucket", password="password")
        for docid in ['customer123', 'customer1234', 'customer12345']:
            bucket.upsert(docid, {'a': 1})
        self.verify_eventing_results(self.function_name, 3, skip_stats_validation=True)
        # add new multiple xattrs , delete old xattrs and delete the documents
        for docid in ['customer123', 'customer1234', 'customer12345']:
            r = bucket.mutate_in(docid, SD.get('eventing', xattr=True))
            log.info(r)
            if "Could not execute one or more multi lookups or mutations" not in str(r):
                self.fail("eventing is still using xattrs for timers")
            r = bucket.mutate_in(docid, SD.get('_eventing', xattr=True))
            log.info(r)
            if "Could not execute one or more multi lookups or mutations" not in str(r):
                self.fail("eventing is still using xattrs for timers")
        self.undeploy_and_delete_function(body)


    def test_eventing_crc_and_fiid(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION,
                                              dcp_stream_boundary="from_now")
        # deploy eventing function
        self.deploy_function(body)
        url = 'couchbase://{ip}/{name}'.format(ip=self.master.ip, name=self.src_bucket_name)
        bucket = Bucket(url, username="cbadminbucket", password="password")
        for docid in ['customer123', 'customer1234', 'customer12345']:
            bucket.upsert(docid, {'a': 1})
        self.verify_eventing_results(self.function_name, 3, skip_stats_validation=True)
        # check for fiid and crc
        for docid in ['customer123', 'customer1234', 'customer12345']:
            fiid = bucket.lookup_in(docid, SD.exists('_eventing.fiid', xattr=True))
            self.log.info(fiid.exists('_eventing.fiid'))
            crc = bucket.lookup_in(docid, SD.exists('_eventing.crc', xattr=True))
            self.log.info(crc.exists('_eventing.crc'))
            if not fiid.exists('_eventing.fiid') and not crc.exists('_eventing.crc'):
                self.fail("No fiid : {} or crc : {} found:".format(fiid, crc))
        self.undeploy_function(body)
        for docid in ['customer123', 'customer1234', 'customer12345']:
            fiid = bucket.lookup_in(docid, SD.exists('_eventing.fiid', xattr=True))
            crc = bucket.lookup_in(docid, SD.exists('_eventing.crc', xattr=True))
            if not fiid.exists('_eventing.fiid') and not crc.exists('_eventing.crc'):
                self.fail("No fiid : {} or crc : {} found after undeployment:".format(fiid, crc))

    def test_fiid_crc_with_pause_resume(self):
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.BUCKET_OP_SOURCE_DOC_MUTATION,
                                              dcp_stream_boundary="from_now")
        # deploy eventing function
        self.deploy_function(body)
        url = 'couchbase://{ip}/{name}'.format(ip=self.master.ip, name=self.src_bucket_name)
        bucket = Bucket(url, username="cbadminbucket", password="password")
        for docid in ['customer123', 'customer1234', 'customer12345']:
            bucket.upsert(docid, {'a': 1})
        self.verify_eventing_results(self.function_name, 3, skip_stats_validation=True)
        #get fiid and crc
        fiid_value = bucket.lookup_in('customer123', SD.exists('_eventing.fiid', xattr=True))['_eventing.fiid']
        crc_value = bucket.lookup_in('customer123', SD.exists('_eventing.crc', xattr=True))['_eventing.crc']
        self.log.info("Fiid: {} and CRC: {}".format(fiid_value, crc_value))
        # check for fiid and crc
        for docid in ['customer1234', 'customer12345']:
            fiid = bucket.lookup_in(docid, SD.exists('_eventing.fiid', xattr=True))
            crc = bucket.lookup_in(docid, SD.exists('_eventing.crc', xattr=True))
            if fiid_value != fiid['_eventing.fiid'] or crc_value !=crc['_eventing.crc']:
                self.fail("fiid {} or crc {} values are not same:".format(fiid, crc))
        self.pause_function(body)
        for docid in ['customer12553', 'customer1253', 'customer12531']:
            bucket.upsert(docid, {'a': 1})
        self.resume_function(body)
        self.verify_eventing_results(self.function_name, 6, skip_stats_validation=True)
        for docid in ['customer12553', 'customer1253', 'customer12531', 'customer123', 'customer1234', 'customer12345']:
            fiid = bucket.lookup_in(docid, SD.exists('_eventing.fiid', xattr=True))
            crc = bucket.lookup_in(docid, SD.exists('_eventing.crc', xattr=True))
            if fiid_value != fiid['_eventing.fiid'] or crc_value !=crc['_eventing.crc']:
                self.fail("fiid {} or crc {} values are not same:".format(fiid, crc))
