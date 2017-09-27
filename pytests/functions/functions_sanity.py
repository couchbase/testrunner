import random

from lib.couchbase_helper.tuq_generators import TuqGenerators
from lib.membase.api.rest_client import RestConnection
from lib.testconstants import STANDARD_BUCKET_PORT
from pytests.functions.functions_base import FunctionsBaseTest, log


class FunctionsSanity(FunctionsBaseTest):
    def setUp(self):
        super(FunctionsSanity, self).setUp()
        if self.create_functions_buckets:
            self.bucket_size = max(100, self.quota / 3)
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
        self.full_docs_list = self.generate_full_docs_list(self.gens_load)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        self.load(self.gens_load, buckets=self.src_bucket, flag=self.item_flag, verify_data=False,
                  batch_size=self.batch_size)
        self.function_name = "Function_{0}".format(random.randint(1, 1000000000))

    def tearDown(self):
        super(FunctionsSanity, self).tearDown()

    def test_mutation_for_dcp_stream_boundary_from_beginning(self):
        body = self.create_save_function_body(self.function_name,
                                              "function OnUpdate(doc, meta) {\n    log('document', doc);\n    dst_bucket[meta.docid] = 'hello world';\n}\nfunction OnDelete(doc) {\n}")
        content = self.rest.save_function(self.function_name, body)
        log.info("saveApp API : {0}".format(content))
        content = self.rest.deploy_function(self.function_name, body)
        log.info("deployApp API : {0}".format(content))
        # Wait for functions to catch up with all the mutations
        # TODO: This is just a hack now. There will be api which will be provided so that we can wait for functions service to comeout of bootstrapping state -> completed
        self.sleep(180)
        stats_src = self.rest.get_bucket_stats(bucket=self.src_bucket_name)
        stats_dst = self.rest.get_bucket_stats(bucket=self.dst_bucket_name)
        # In the event handler code we create 1 doc for each mutation, Since we have deployed the handler code for the dcp_stream_boundary
        # from the beginning number of docs in source bucket and destination bucket should match
        # TODO: There will be a API which will provide cumulative stats for dcp_events_processed, once it is provided we will compare the results with that
        self.assertEqual(stats_src["curr_items"], stats_dst["curr_items"])
