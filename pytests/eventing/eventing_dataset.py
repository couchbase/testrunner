# Intentionally adding 1 new line
# coding=utf-8
import logger
from lib.couchbase_helper.documentgenerator import BlobGenerator, JsonDocGenerator, JSONNonDocGenerator
from lib.couchbase_helper.tuq_helper import N1QLHelper
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.eventing.eventing_constants import HANDLER_CODE
from pytests.eventing.eventing_base import EventingBaseTest, log
from couchbase.bucket import Bucket

log = logger.Logger.get_logger()


class EventingDataset(EventingBaseTest):
    def setUp(self):
        super(EventingDataset, self).setUp()
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

    def tearDown(self):
        super(EventingDataset, self).tearDown()

    def test_functions_where_dataset_has_binary_and_json_data(self):
        gen_load = BlobGenerator('binary', 'binary-', self.value_size, end=2016 * self.docs_per_day)
        # load binary and json data
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load, self.buckets[0].kvs[1], "create",
                                   exp=0, flag=0, batch_size=1000)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete both binary and json documents
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load, self.buckets[0].kvs[1], "delete",
                                   exp=0, flag=0, batch_size=1000)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size, op_type='delete')
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_functions_where_documents_change_from_binary_to_json_data(self):
        gen_load_binary = BlobGenerator('binary1000000', 'binary', self.value_size, start=1,
                                        end=2016 * self.docs_per_day + 1)
        gen_load_json = JsonDocGenerator('binary', op_type="create", end=2016 * self.docs_per_day)
        # load binary data
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_binary, self.buckets[0].kvs[1], "create",
                                   exp=0, flag=0, batch_size=1000)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # convert data from binary to json
        # use the same doc-id's as binary to update from binary to json
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_binary, self.buckets[0].kvs[1], "delete",
                                   exp=0, flag=0, batch_size=1000)
        self.sleep(60)
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_json, self.buckets[0].kvs[1], "create",
                                   exp=0, flag=0, batch_size=1000)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete all json docs
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_json, self.buckets[0].kvs[1], "delete",
                                   exp=0, flag=0, batch_size=1000)
        # Wait for eventing to catch up with all the delete mutations and verify results
        self.verify_eventing_results(self.function_name, 0, skip_stats_validation=True)
        self.undeploy_and_delete_function(body)

    def test_functions_where_dataset_has_binary_and_non_json_data(self):
        gen_load_binary = BlobGenerator('binary', 'binary-', self.value_size, end=2016 * self.docs_per_day)
        values = ['1', '10']
        gen_load_non_json = JSONNonDocGenerator('non_json_docs', values, start=0, end=2016 * self.docs_per_day)
        # load binary and non json data
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_binary, self.buckets[0].kvs[1], "create",
                                   exp=0, flag=0, batch_size=1000)
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_non_json, self.buckets[0].kvs[1],
                                   "create", exp=0, flag=0, batch_size=1000)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
        self.deploy_function(body)
        # Wait for eventing to catch up with all the update mutations and verify results
        self.verify_eventing_results(self.function_name, self.docs_per_day * 2016, skip_stats_validation=True)
        # delete non json documents
        self.cluster.load_gen_docs(self.master, self.src_bucket_name, gen_load_non_json, self.buckets[0].kvs[1],
                                   "delete", exp=0, flag=0, batch_size=1000)
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
            "\xc2\xa1 \xc2\xa2 \xc2\xa4 \xc2\xa5"  # utf-8 encoded characters
        ]
        url = 'couchbase://{ip}/{name}'.format(ip=self.master.ip, name=self.src_bucket_name)
        bucket = Bucket(url, username="cbadminbucket", password="password")
        for key in keys:
            bucket.upsert(key, "Test with different key values")
        # create a doc using n1ql query
        query = "INSERT INTO  " + self.src_bucket_name + " ( KEY, VALUE ) VALUES ('key11111','from N1QL query')"
        self.n1ql_helper.run_cbq_query(query=query, server=self.n1ql_node)
        body = self.create_save_function_body(self.function_name, HANDLER_CODE.DELETE_BUCKET_OP_ON_DELETE)
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

