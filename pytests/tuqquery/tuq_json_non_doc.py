from tuqquery.tuq import QueryTests
from view.viewquerytests import StoppableThread
from couchbase_helper.documentgenerator import JSONNonDocGenerator

class JSONNonDocTests(QueryTests):
    def setUp(self):
        try:
            self.skip_generation=True
            super(JSONNonDocTests, self).setUp()
            self.log.info("==============  JSONNonDocTests setup has started ==============")
            self.value_type = self.input.param("value_type", "int")
            self.gens_load = self.gen_docs(type='json_non_docs', values_type=self.value_type)
            for bucket in self.buckets:
                self.cluster.bucket_flush(self.master, bucket=bucket,
                                      timeout=self.wait_timeout * 5)
            self.load(self.gens_load)
        except:
            self.cluster.shutdown()
        self.log.info("==============  JSONNonDocTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(JSONNonDocTests, self).suite_setUp()
        self.log.info("==============  JSONNonDocTests suite_setup has started ==============")
        self.log.info("==============  JSONNonDocTests suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  JSONNonDocTests tearDown has started ==============")
        self.log.info("==============  JSONNonDocTests tearDown has completed ==============")
        super(JSONNonDocTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  JSONNonDocTests suite_tearDown has started ==============")
        self.log.info("==============  JSONNonDocTests suite_tearDown has completed ==============")
        super(JSONNonDocTests, self).suite_tearDown()

    def test_simple_query(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            self.query = "select * from %s" % bucket.name
            actual_result = self.run_cbq_query()
            self.sleep(5, 'wait for index build')
            actual_result = self.run_cbq_query()
            actual_result = [doc[bucket.name] for doc in actual_result['results']]
            expected_result = self.generate_full_docs_list(self.gens_load)
            self._verify_results(actual_result, expected_result)

    def test_int_where(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            self.query = "select v from %s v where v > 300" % bucket.name
            actual_result = self.run_cbq_query()
            self.sleep(5, 'wait for index build')
            actual_result = self.run_cbq_query()
            actual_result = [doc["v"] for doc in actual_result['results']]
            expected_result = self.generate_full_docs_list(self.gens_load)
            expected_result = [doc for doc in expected_result if doc > 300 ]
            self._verify_results(actual_result, expected_result)

    def test_prepared_int_where(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            self.query = "select v from %s v where v > 300" % bucket.name
            self.prepared_common_body()

    def test_string_where(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            self.query = "select v from %s where v = 4" % bucket.name
            actual_result = self.run_cbq_query()
            self.sleep(5, 'wait for index build')
            actual_result = self.run_cbq_query()
            actual_result = [doc["$1"] for doc in actual_result['results']]
            expected_result = self.generate_full_docs_list(self.gens_load)
            expected_result = [doc for doc in expected_result if doc == 4 ]
            self._verify_results(actual_result, expected_result)

    def test_array_where(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            self.query = "SELECT v FROM %s v WHERE ANY num IN v SATISFIES num > 20 end" % bucket.name
            actual_result = self.run_cbq_query()
            self.sleep(5, 'wait for index build')
            actual_result = self.run_cbq_query()
            actual_result = [doc["v"] for doc in actual_result['results']]
            expected_result = self.generate_full_docs_list(self.gens_load)
            expected_result = [doc for doc in expected_result if doc[0] > 20 or doc[1] > 20 ]
            self._verify_results(actual_result, expected_result)

