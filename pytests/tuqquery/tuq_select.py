from .tuq import QueryTests
from membase.api.exception import CBQError

class QuerySelectTests(QueryTests):
    def setUp(self):
        super(QuerySelectTests, self).setUp()
        self.bucket = "default"

    def suite_setUp(self):
        super(QuerySelectTests, self).suite_setUp()

    def tearDown(self):
        super(QuerySelectTests, self).tearDown()

    def suite_tearDown(self):
        super(QuerySelectTests, self).suite_tearDown()

    def test_order(self):
        expected_result = [{"c": 3, "b": 2, "a": "1", "$1": 20}]
        result = self.run_cbq_query('SELECT t.c, t.b, t.a, 10*2 FROM [{"a":"1", "b":2, "c": 3}] t')
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_exclude_single(self):
        expected_result = {"a": 1, "c": 3, "d": 4}
        result = self.run_cbq_query('SELECT * EXCLUDE "b" FROM [{"a": 1, "b":2, "c": 3, "d": 4}] t')
        self.assertEqual(expected_result, result['results'][0]['t'], f"We expected {expected_result} but got {result['results'][0]['t']}")

        result = self.run_cbq_query('SELECT * EXCLUDE b FROM [{"a": 1, "b":2, "c": 3, "d": 4}] t')
        self.assertEqual(expected_result, result['results'][0]['t'], f"We expected {expected_result} but got {result['results'][0]['t']}")

    def test_exclude_not_present(self):
        expected_result = {"a": 1, "b": 2, "c": 3, "d": 4}
        result = self.run_cbq_query('SELECT * EXCLUDE "xyz" FROM [{"a": 1, "b":2, "c": 3, "d": 4}] t')
        self.assertEqual(expected_result, result['results'][0]['t'], f"We expected {expected_result} but got {result['results'][0]['t']}")

    def test_exclude_multiple(self):
        expected_result = {"a": 1, "d": 4}
        result = self.run_cbq_query('SELECT * EXCLUDE "b,c" FROM [{"a": 1, "b":2, "c": 3, "d": 4}] t')
        self.assertEqual(expected_result, result['results'][0]['t'], f"We expected {expected_result} but got {result['results'][0]['t']}")

    def test_exclude_function(self):
        expected_result = {"a": 1, "c": 3, "d": 4}
        result = self.run_cbq_query('SELECT * EXCLUDE lower("b") FROM [{"a":1, "b":2, "c": 3, "d": 4}] t')
        self.assertEqual(expected_result, result['results'][0]['t'], f"We expected {expected_result} but got {result['results'][0]['t']}")

    def test_exclude_udf(self):
        udf = "create or replace function f1() LANGUAGE JAVASCRIPT as 'function f1() {return \"a,b\"}'"
        self.run_cbq_query(udf)
        expected_result = {"c": 3, "d": 4}
        result = self.run_cbq_query('SELECT * EXCLUDE f1() FROM [{"a":1, "b":2, "c": 3, "d": 4}] t')
        self.assertEqual(expected_result, result['results'][0]['t'], f"We expected {expected_result} but got {result['results'][0]['t']}")

    def test_exclude_param(self):
        expected_result = {"a": 1, "c": 3, "d": 4}
        result = self.run_cbq_query('SELECT * EXCLUDE $exb FROM [{"a":1, "b":2, "c": 3, "d": 4}] t', query_params = {'$exb': '"b"'})
        self.assertEqual(expected_result, result['results'][0]['t'], f"We expected {expected_result} but got {result['results'][0]['t']}")

    def test_exclude_negative(self):
        expected_code = 5010
        expected_msg = "Does not evaluate to a string"
        try:
            self.run_cbq_query('SELECT * EXCLUDE 3 FROM [{"a":1, "b":2, "c": 3, "d": 4}] t')
            self.fail("Should have failed")
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], expected_code)
            self.assertEqual(error['reason']['cause']['details'], expected_msg)
