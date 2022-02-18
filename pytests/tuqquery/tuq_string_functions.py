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
