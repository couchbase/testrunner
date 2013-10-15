from tuqquery.tuq import QueryTests
from viewquerytests import StoppableThread
from couchbase.documentgenerator import JSONNonDocGenerator

class JSONNonDocTests(QueryTests):
    def setUp(self):
        super(JSONNonDocTests, self).setUp()
        self.value_type = self.input.param("value_type", "int")
        self.gens_load = self.generate_docs(self.value_type)
        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket=bucket,
                                  timeout=self.wait_timeout * 5)
        self.load(self.gens_load)

    def suite_setUp(self):
        super(JSONNonDocTests, self).suite_setUp()

    def tearDown(self):
        super(JSONNonDocTests, self).tearDown()

    def suite_tearDown(self):
        super(JSONNonDocTests, self).suite_tearDown()

    def test_simple_query(self):
        for bucket in self.buckets:
            self.query = "select value() from %s" % bucket.name
            actual_result = self.run_cbq_query()
            actual_result = [doc["$1"] for doc in actual_result['resultset']]
            expected_result = self._generate_full_docs_list(self.gens_load)
            self._verify_results(sorted(actual_result), sorted(expected_result))

    def test_int_where(self):
        for bucket in self.buckets:
            self.query = "select value() from %s where value() > 300" % bucket.name
            actual_result = self.run_cbq_query()
            actual_result = [doc["$1"] for doc in actual_result['resultset']]
            expected_result = self._generate_full_docs_list(self.gens_load)
            expected_result = [doc for doc in expected_result if doc > 300 ]
            self._verify_results(sorted(actual_result), sorted(expected_result))

    def test_string_where(self):
        for bucket in self.buckets:
            self.query = "select value() from %s where value() == 'Engineer'" % bucket.name
            actual_result = self.run_cbq_query()
            actual_result = [doc["$1"] for doc in actual_result['resultset']]
            expected_result = self._generate_full_docs_list(self.gens_load)
            expected_result = [doc for doc in expected_result if doc == 'Engineer' ]
            self._verify_results(sorted(actual_result), sorted(expected_result))

    def test_array_where(self):
        for bucket in self.buckets:
            self.query = "SELECT value() FROM %s WHERE ANY num > 20 OVER num IN value() end" % bucket.name
            actual_result = self.run_cbq_query()
            actual_result = [doc["$1"] for doc in actual_result['resultset']]
            expected_result = self._generate_full_docs_list(self.gens_load)
            expected_result = [doc for doc in expected_result if doc[0] > 20 or doc[1] > 20 ]
            self._verify_results(sorted(actual_result), sorted(expected_result))

    def generate_docs(self, values_type, name="tuq", start=0, end=0):
        if end==0:
            end = self.num_items
        if values_type == 'string':
            values = ['Engineer', 'Sales', 'Support']
        elif values_type == 'int':
            values = [100, 200, 300, 400, 500]
        elif values_type == 'array':
            values = [[10, 20], [20, 30], [30, 40]]
        else:
            return []
        generators = [JSONNonDocGenerator(name, values, start=start,end=end)]
        return generators