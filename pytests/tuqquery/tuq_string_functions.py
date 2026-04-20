from .tuq import QueryTests
from membase.api.exception import CBQError

class QueryStringFunctionsTests(QueryTests):
    def setUp(self):
        super(QueryStringFunctionsTests, self).setUp()
        self.bucket = "default"
        self.value = self.input.param("value", "Hello World!")

    def suite_setUp(self):
        super(QueryStringFunctionsTests, self).suite_setUp()

    def tearDown(self):
        super(QueryStringFunctionsTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryStringFunctionsTests, self).suite_tearDown()
        
    def test_pad_default(self):
        expected = [{'left': '     Hello', 'right': 'Hello     '}]
        result = self.run_cbq_query('SELECT LPAD("Hello", 10) as `left`, RPAD("Hello", 10) as `right`')
        self.assertEqual(result['results'], expected)

    def test_pad_truncate(self):
        expected = [{'left': 'Hello', 'right': 'Hello'}]
        result = self.run_cbq_query('SELECT LPAD("Hello World!", 5, "+") as `left`, RPAD("Hello World!", 5, "+") as `right`')
        self.assertEqual(result['results'], expected)

    def test_pad_char(self):
        expected = [{'left': self.value.rjust(30, "+"), 'right': self.value.ljust(30, "+")}]
        result = self.run_cbq_query(f'SELECT LPAD("{self.value}", 30, "+") as `left`, RPAD("{self.value}", 30, "+") as `right`')
        self.assertEqual(result['results'], expected)

    def test_pad_string(self):
        expected = [{'left': '^-^-^-^-^-^-^-^-^-Hello World!', 'right': 'Hello World!^-^-^-^-^-^-^-^-^-'}]
        result = self.run_cbq_query(f'SELECT LPAD("{self.value}", 30, "^-") as `left`, RPAD("{self.value}", 30, "^-") as `right`')
        self.assertEqual(result['results'], expected)

    def test_pad_invalid_value(self):
        # null, True, number, json
        expected = [{'left': None, 'right': None}]
        result = self.run_cbq_query(f'SELECT LPAD({self.value}, 10) as `left`, RPAD({self.value}, 10) as `right`')
        self.assertEqual(result['results'], expected)

    def test_pad_empty_string(self):
        expected = [{'left': None, 'right': None}]
        result = self.run_cbq_query(f'SELECT LPAD("{self.value}", 10, "") as `left`, RPAD("{self.value}", 10, "") as `right`')
        self.assertEqual(result['results'], expected)

        expected = [{'left': "".rjust(30, "+"), 'right': "".ljust(30, "+")}]
        result = self.run_cbq_query(f'SELECT LPAD("", 30, "+") as `left`, RPAD("", 30, "+") as `right`')
        self.assertEqual(result['results'], expected)

    def test_pad_invalid_args(self):
        error_code = 3000
        queries = [
            'SELECT LPAD("Hello")',
            'SELECT RPAD("Hello")',
            'SELECT LPAD("Hello", 10, "+", "extra")',
            'SELECT RPAD("Hello", 10, "+", "extra")'
        ]
        for query in queries:
            try:
                self.run_cbq_query(query)
            except CBQError as ex:
                error = self.process_CBQE(ex)
                self.assertEqual(error['code'], error_code)
                self.assertTrue("must be between 2 and 3" in error['msg'])

    def test_pad_missing(self):
        expected = [{}]
        result = self.run_cbq_query(f'SELECT LPAD(T.miss, 10) as `left`, RPAD(T.miss, 10) as `right` FROM [{{"a":1}}] as T')
        self.assertEqual(result['results'], expected)

    def test_array_to_string_basic(self):
        """
        MB-69200: Test array_to_string basic functionality.
        Signature: array_to_string(separator, str1, str2, ...)
        """
        result = self.run_cbq_query("SELECT array_to_string('-', 'a', 'b', 'c') AS result")
        self.assertEqual(result['results'], [{'result': 'a-b-c'}])

        result = self.run_cbq_query("SELECT array_to_string(',', 'x', 'y', 'z') AS result")
        self.assertEqual(result['results'], [{'result': 'x,y,z'}])

        result = self.run_cbq_query("SELECT array_to_string('', 'hello', 'world') AS result")
        self.assertEqual(result['results'], [{'result': 'helloworld'}])

    def test_array_to_string_concat2_synonym(self):
        """
        MB-69200: Verify array_to_string is a synonym for concat2.
        """
        result1 = self.run_cbq_query("SELECT array_to_string('-', 'a', 'b', 'c') AS result")
        result2 = self.run_cbq_query("SELECT concat2('-', 'a', 'b', 'c') AS result")
        self.assertEqual(result1['results'], result2['results'])

        result1 = self.run_cbq_query("SELECT array_to_string('|', '1', '2', '3', '4') AS result")
        result2 = self.run_cbq_query("SELECT concat2('|', '1', '2', '3', '4') AS result")
        self.assertEqual(result1['results'], result2['results'])

    def test_array_to_string_edge_cases(self):
        """
        MB-69200: Test array_to_string with edge cases (NULL, single arg).
        """
        # NULL separator
        result = self.run_cbq_query("SELECT array_to_string(NULL, 'a', 'b') AS result")
        self.assertEqual(result['results'], [{'result': None}])

        # Single string (no joining needed)
        result = self.run_cbq_query("SELECT array_to_string('-', 'only') AS result")
        self.assertEqual(result['results'], [{'result': 'only'}])
