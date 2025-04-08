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
        self.log.info(result['results'])
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

        result = self.run_cbq_query('SELECT t.c, t.b, t.a, 10*2 FROM [{"a":"1", "b":2, "c": 3}] t', query_params = {'sort_projection': False})
        self.log.info(result['results'])
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

        expected_result_sorted = [{"$1": 20, "a": "1", "b": 2, "c": 3}]
        result = self.run_cbq_query('SELECT t.c, t.b, t.a, 10*2 FROM [{"a":"1", "b":2, "c": 3}] t', query_params = {'sort_projection': True})
        self.log.info(result['results'])
        self.assertEqual(expected_result_sorted, result['results'], f"We expected {expected_result_sorted} but got {result['results']}")

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

    def test_urlencode(self):
        result = self.run_cbq_query('SELECT urlencode("a%20b") AS a')
        expected_result = [{"a": "a%2520b"}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urldecode(self):
        result = self.run_cbq_query('SELECT urldecode("a%20b") AS a')
        expected_result = [{"a": "a b"}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urlencode_urldecode(self):
        result = self.run_cbq_query('SELECT urldecode(urlencode("a b")) AS a')
        expected_result = [{"a": "a b"}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urldecode_urlencode(self):
        result = self.run_cbq_query('SELECT urlencode(urldecode("a%20b")) AS a')
        expected_result = [{"a": "a+b"}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urlencode_urldecode_urlencode(self):
        result = self.run_cbq_query('SELECT urlencode(urldecode(urlencode("a%20b"))) AS a')
        expected_result = [{"a": "a%2520b"}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urlencode_null(self):
        result = self.run_cbq_query('SELECT urlencode(NULL) AS a')
        expected_result = [{"a": None}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urldecode_null(self):
        result = self.run_cbq_query('SELECT urldecode(NULL) AS a')
        expected_result = [{"a": None}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")
    
    def test_urlencode_empty(self):
        result = self.run_cbq_query('SELECT urlencode("") AS a')
        expected_result = [{"a": ""}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urldecode_empty(self):
        result = self.run_cbq_query('SELECT urldecode("") AS a')
        expected_result = [{"a": ""}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urlencode_complex(self):
        result = self.run_cbq_query('SELECT urlencode("p1=v/a/l/1&p2=v?a l;2") AS a')
        expected_result = [{"a": "p1%3Dv%2Fa%2Fl%2F1%26p2%3Dv%3Fa+l%3B2"}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urldecode_complex(self):
        result = self.run_cbq_query('SELECT urldecode("p1%3Dv%2Fa%2Fl%2F1%26p2%3Dv%3Fa+l%3B2") AS a')
        expected_result = [{"a": "p1=v/a/l/1&p2=v?a l;2"}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urlencode_number(self):
        result = self.run_cbq_query('SELECT urlencode(123) AS a')
        expected_result = [{"a": None}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urldecode_number(self):
        result = self.run_cbq_query('SELECT urldecode(123) AS a')
        expected_result = [{"a": None}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urlencode_boolean(self):
        result = self.run_cbq_query('SELECT urlencode(TRUE) AS a')
        expected_result = [{"a": None}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_urldecode_boolean(self):
        result = self.run_cbq_query('SELECT urldecode(TRUE) AS a')
        expected_result = [{"a": None}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_basic(self):
        result = self.run_cbq_query('SELECT REDACT(t) FROM [{"a": "abc", "b": "def"}] t')
        expected_result = [{"$1": {"a": "xxx", "b": "xxx"}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_with_pattern(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "a"}) FROM [{"a": "abc", "b": "def"}] t
        ''')
        expected_result = [{"$1": {"a": "xxx", "b": "def"}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_with_custom_mask(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "a", "mask": "*"}) FROM [{"a": "abc", "b": "def"}] t
        ''')
        expected_result = [{"$1": {"a": "***", "b": "def"}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_mask_length(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"strict": true}) FROM [{
                "short": "a",
                "medium": "test",
                "long": "very_long_string",
                "special": "!@#$%^&*()"
            }] t
        ''')
        expected_result = [{"$1": {
            "long": "xxxxxxxxxxxxxxxx",
            "medium": "xxxx",
            "short": "x",
            "special": "xxxxxxxxxx"
        }}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_mask_length_with_custom_mask(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": ".*", "regex": true, "mask": "*"}) FROM [{
                "short": "a",
                "medium": "test",
                "long": "very_long_string"
            }] t
        ''')
        expected_result = [{"$1": {
            "short": "*",
            "medium": "****",
            "long": "****_****_******"
        }}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_mask_length_vs_fixed(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, 
                {"pattern": "var1", "mask": "*"},
                {"pattern": "var2", "mask": "*", "fixedlength": true},
                {"pattern": "var3", "mask": "#"}
            ) FROM [{
                "var1": "short",
                "var2": "medium_length",
                "var3": "another_string"
            }] t
        ''')
        expected_result = [{"$1": {
            "var1": "*****",
            "var2": "*",
            "var3": "#######_######"
        }}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_with_omit(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "a", "omit": true}) FROM [{"a": "abc", "b": "def"}] t
        ''')
        expected_result = [{"$1": {"b": "def"}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_with_exclude(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "a", "exclude": true}) FROM [{"a": "abc", "b": "xxx"}] t
        ''')
        expected_result = [{"$1": {"a": "abc", "b": "xxx"}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_with_regex(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "^a", "regex": true}) FROM [{"a": "abc", "ab": "def", "b": "ghi"}] t
        ''')
        expected_result = [{"$1": {"a": "xxx", "ab": "xxx", "b": "ghi"}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_with_ignorecase(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "A", "ignorecase": true}) FROM [{"a": "abc", "b": "def"}] t
        ''')
        expected_result = [{"$1": {"a": "xxx", "b": "def"}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_with_name(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "a", "name": true}) FROM [{"a": "abc", "b": "def"}] t
        ''')
        expected_result = [{"$1": {"f0000": "xxx", "b": "def"}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_multiple_filters(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, 
                {"pattern": "a", "mask": "*"}, 
                {"pattern": "b", "mask": "#"}
            ) FROM [{"a": "abc", "b": "def"}] t
        ''')
        expected_result = [{"$1": {"a": "***", "b": "###"}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_nested_object(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "a"}) 
            FROM [{"a": "abc", "nested": {"a": "def", "b": "ghi"}}] t
        ''')
        expected_result = [{"$1": {"a": "xxx", "nested": {"a": "xxx", "b": "ghi"}}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_array(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "data"}) 
            FROM [{"data": ["secret", "public", "secret2", "public2"]}] t
        ''')
        expected_result = [{"$1": {"data": ["xxxxxx", "xxxxxx", "xxxxxxx", "xxxxxxx"]}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_array_of_objects(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "password"}) 
            FROM [{
                "users": [
                    {"name": "user1", "password": "pass123"},
                    {"name": "user2", "password": "pass456"}
                ]
            }] t
        ''')
        expected_result = [{"$1": {
            "users": [
                {"name": "user1", "password": "xxxxxxx"},
                {"name": "user2", "password": "xxxxxxx"}
            ]
        }}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_complex_nested(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "secret"}, {"pattern": "password"}) 
            FROM [{
                "id": "doc1",
                "metadata": {
                    "created": "2024-01-01",
                    "secret_key": "abc123"
                },
                "users": [
                    {
                        "name": "user1",
                        "password": "pass123",
                        "data": {
                            "public": "visible",
                            "secret": "hidden"
                        }
                    },
                    {
                        "name": "user2",
                        "password": "pass456",
                        "data": {
                            "public": "visible",
                            "secret": "hidden"
                        }
                    }
                ],
                "settings": {
                    "public_key": "xyz789",
                    "secret_token": "token123"
                }
            }] t
        ''')
        expected_result = [{"$1": {
            "id": "doc1",
            "metadata": {
                "created": "2024-01-01",
                "secret_key": "xxxxxx"
            },
            "settings": {
                "public_key": "xyz789",
                "secret_token": "xxxxxxxx"
            },
            "users": [
                {
                    "data": {
                        "public": "visible",
                        "secret": "xxxxxx"
                    },
                    "name": "user1",
                    "password": "xxxxxxx",
                },
                {
                    "data": {
                        "public": "visible",
                        "secret": "xxxxxx"
                    },
                    "name": "user2",
                    "password": "xxxxxxx",
                }
            ]
        }}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_mixed_types(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {"pattern": "secret"}) 
            FROM [{
                "data": {
                    "string": "secret_text",
                    "number": 12345,
                    "boolean": true,
                    "array": ["secret1", 123, true, "secret2"],
                    "secret_array": ["secret1", 123, true, "secret2"],
                    "nested": {
                        "secret_field": "hidden",
                        "public_field": "visible"
                    },
                    "mixed_array": [
                        "secret_string",
                        {"secret": "hide_this", "public": "show_this"},
                        123,
                        ["nested_secret", "public_data"]
                    ]
                }
            }] t
        ''')
        expected_result = [{"$1": {
            "data": {
                "array": [
                    "secret1",
                    123,
                    True,
                    "secret2"
                ],
                "boolean": True,
                "mixed_array": [
                    "secret_string",
                    {
                        "public": "show_this",
                        "secret": "xxxx_xxxx"
                    },
                    123,
                    [
                        "nested_secret",
                        "public_data"
                    ]
                ],
                "nested": {
                    "public_field": "visible",
                    "secret_field": "xxxxxx"
                },
                "number": 12345,
                "secret_array": [
                    "xxxxxxx",
                    111,
                    True,
                    "xxxxxxx"
                ],
                "string": "secret_text"
            }
        }}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_complex_exclude(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {
                "pattern": "secret|password|key",
                "regex": true,
                "exclude": true
            }) FROM [{
                "id": "doc1",
                "metadata": {
                    "created": "2024-01-01",
                    "secret_key": "abc123",
                    "api_token": "xyz789",
                    "public_key": "pub123"
                },
                "users": [
                    {
                        "name": "user1",
                        "password": "pass123",
                        "email": "user1@example.com",
                        "data": {
                            "public": "visible",
                            "secret": "hidden",
                            "token": "t123",
                            "preferences": {
                                "theme": "dark",
                                "api_key": "k456",
                                "secret_notes": "note123"
                            }
                        }
                    },
                    {
                        "name": "user2",
                        "password": "pass456",
                        "email": "user2@example.com",
                        "data": {
                            "public": "visible",
                            "secret": "hidden",
                            "token": "t456",
                            "preferences": {
                                "theme": "light",
                                "api_key": "k789",
                                "secret_notes": "note456"
                            }
                        }
                    }
                ],
                "settings": {
                    "public_key": "xyz789",
                    "secret_token": "token123",
                    "app_token": "app456",
                    "credentials": {
                        "username": "admin",
                        "password_hash": "hash123",
                        "api_token": "api789"
                    }
                }
            }] t
        ''')
        expected_result = [{"$1": {
            "id": "xxxx",
            "metadata": {
                "api_token": "xxxxxx",
                "created": "1111-11-11",
                "public_key": "pub123",
                "secret_key": "abc123"
            },
            "settings": {
                "app_token": "xxxxxx",
                "credentials": {
                    "api_token": "xxxxxx",
                    "password_hash": "hash123",
                    "username": "xxxxx"
                },
                "public_key": "xyz789",
                "secret_token": "token123"
            },
            "users": [
                {
                    "data": {
                        "preferences": {
                            "api_key": "k456",
                            "secret_notes": "note123",
                            "theme": "xxxx"
                        },
                        "public": "xxxxxxx",
                        "secret": "hidden",
                        "token": "xxxx"
                    },
                    "email": "xxxxx@xxxxxxx.xxx",
                    "name": "xxxxx",
                    "password": "pass123"
                },
                {
                    "data": {
                        "preferences": {
                            "api_key": "k789",
                            "secret_notes": "note456",
                            "theme": "xxxxx"
                        },
                        "public": "xxxxxxx",
                        "secret": "hidden",
                        "token": "xxxx"
                    },
                    "email": "xxxxx@xxxxxxx.xxx",
                    "name": "xxxxx",
                    "password": "pass456"
                }
            ]
        }}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_multiple_excludes(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, {
                "pattern": "^(name|id|public).*",
                "regex": true,
                "exclude": true
            }, {
                "pattern": ".*password.*",
                "regex": true,
                "exclude": true
            }) FROM [{
                "id": "user123",
                "name": "John Doe",
                "username": "jdoe",
                "password": "secret123",
                "password_hint": "favorite color",
                "public_profile": {
                    "display_name": "JD",
                    "avatar": "url://avatar",
                    "bio": "Developer"
                },
                "private_data": {
                    "email": "john@example.com",
                    "phone": "123-456-7890",
                    "backup_password": "backup123",
                    "security_questions": [
                        {
                            "name_of_question": "First question",
                            "question": "First pet name?",
                            "password_reset_answer": "Fluffy"
                        }
                    ]
                }
            }] t
        ''')
        expected_result = [{"$1": {
            "id": "user123",
            "name": "John Doe",
            "password": "xxxxxxxxx",
            "password_hint": "xxxxxxxx xxxxx",
            "private_data": {
                "backup_password": "xxxxxxxxx",
                "email": "xxxx@xxxxxxx.xxx",
                "phone": "xxx-xxx-xxxx",
                "security_questions": [
                    {
                        "name_of_question": "First question",
                        "password_reset_answer": "xxxxxx",
                        "question": "xxxxx xxx xxxx?"
                    }
                ]
            },
            "public_profile": {
                "avatar": "xxx://xxxxxx",
                "bio": "xxxxxxxxx",
                "display_name": "xx"
            },
            "username": "xxxx"
        }}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_filter_order(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, 
                {"pattern": "secret", "mask": "*"},
                {"pattern": "secret", "mask": "#"},
                {"pattern": ".*", "regex": true, "mask": "@"}
            ) FROM [{
                "secret_data": "sensitive",
                "secret_key": "key123",
                "public_data": "visible"
            }] t
        ''')
        expected_result = [{"$1": {
            "secret_data": "********",
            "secret_key": "******",
            "public_data": "@@@@@@@"
        }}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_filter_order_with_exclude(self):
        result = self.run_cbq_query('''
            SELECT REDACT(t, 
                {"pattern": "secret", "mask": "*"},
                {"pattern": "secret", "exclude": true},
                {"pattern": "public", "mask": "#"}
            ) FROM [{
                "secret_data": "sensitive",
                "secret_key": "key123",
                "public_data": "visible"
            }] t
        ''')
        expected_result = [{"$1": {
            "secret_data": "*********",
            "secret_key": "******",
            "public_data": "#######"
        }}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")
    
    def test_redact_string_literal(self):
        result = self.run_cbq_query('''
            SELECT REDACT("secret")
        ''')
        expected_result = [{"$1": "xxxxxx"}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_number_literal(self):
        result = self.run_cbq_query('''
            SELECT REDACT(123)
        ''')
        expected_result = [{"$1": 111}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_array_literal(self):
        result = self.run_cbq_query('''
            SELECT REDACT(["secret", 123])
        ''')
        expected_result = [{"$1": ["xxxxxx", 111]}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_evaluate_simple_statement(self):
        result = self.run_cbq_query('''
            SELECT EVALUATE("SELECT 1+1")
        ''')
        expected_result = [{"$1": [{"$1": 2}]}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_evaluate_with_named_params(self):
        result = self.run_cbq_query('''
            SELECT EVALUATE("SELECT $x + $y", {"x": 5, "y": 3}) 
        ''')
        expected_result = [{"$1": [{"$1": 8}]}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_evaluate_with_positional_params(self):
        result = self.run_cbq_query('''
            SELECT EVALUATE("SELECT $1 + $2", [10, 20])
        ''')
        expected_result = [{"$1": [{"$1": 30}]}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_evaluate_with_subquery(self):
        result = self.run_cbq_query('''
            SELECT EVALUATE("SELECT x FROM [1,2,3] x WHERE x > $threshold", 
                          {"threshold": 1})
        ''')
        expected_result = [{"$1": [{"x": 2}, {"x": 3}]}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_evaluate_invalid_statement(self):
        try:
            self.run_cbq_query('''
                SELECT EVALUATE("INVALID SQL")
            ''')
            self.fail("Expected query to fail with invalid SQL")
        except CBQError as ex:
            self.assertTrue("syntax error" in str(ex).lower())

    def test_evaluate_missing_params(self):
        try:
            self.run_cbq_query('''
                SELECT EVALUATE("SELECT $x + $y")
            ''')
            self.fail("Expected query to fail due to missing parameters")
        except CBQError as ex:
            self.assertTrue("no value for named parameter" in str(ex).lower())

    def test_evaluate_missing_positional_params(self):
        try:
            self.run_cbq_query('''
                SELECT EVALUATE("SELECT $1 + $2", [10])
            ''')
            self.fail("Expected query to fail due to missing positional parameters")
        except CBQError as ex:
            self.assertTrue("no value for positional parameter" in str(ex).lower())

    def test_evaluate_extra_positional_params(self):
        result = self.run_cbq_query('''
            SELECT EVALUATE("SELECT $1 + $2", [10, 20, 30])
        ''')
        expected_result = [{"$1": [{"$1": 30}]}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_evaluate_extra_named_params(self):
        result = self.run_cbq_query('''
            SELECT EVALUATE("SELECT $x + $y", {"x": 10, "y": 20, "z": 30})
        ''')
        expected_result = [{"$1": [{"$1": 30}]}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_evaluate_in_from_clause(self):
        result = self.run_cbq_query('''
            SELECT * FROM (EVALUATE("SELECT $x + $y", {"x": 10, "y": 20})) AS t
        ''')
        expected_result = [{"t": {"$1": 30}}]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_evaluate_privileges(self):
        try:
            # Drop test_user if it exists
            self.run_cbq_query('''
                DROP USER test_user
            ''')
        except CBQError as ex:
            self.log.info(f"Error dropping user: {ex}")
        # create test_user with password password
        self.run_cbq_query('''
            CREATE USER test_user PASSWORD "password"
        ''')
        # grant write privileges to test_user on default bucket
        self.run_cbq_query('''
            GRANT query_update ON `default` TO test_user
        ''')
        # try to evaluate a statement with the user
        try:
            self.run_cbq_query('''
                SELECT EVALUATE("SELECT * FROM `default` LIMIT 1")
            ''', username="test_user", password="password")
            self.fail("Expected query to fail due to insufficient privileges")
        except CBQError as ex:
            self.assertTrue("user does not have credentials" in str(ex).lower())

    def test_evaluate_update_statement(self):
        try:
            self.run_cbq_query('''
                SELECT EVALUATE("UPDATE `default` SET val = $x", {"x": 10})
            ''')
            self.fail("Expected query to fail due to not being readonly")
        except CBQError as ex:
            self.assertTrue("not a readonly request" in str(ex).lower())

    def test_redact_ifmissing(self):
        # redact the id of the parent object if it is missing 
        result = self.run_cbq_query('''
            SELECT {
                "parent": {
                    "id": IFMISSING(REDACT(doc.parent.id, { "pattern": ".*", "regex": true, "mask": "*" }), "******"),
                    "name": doc.parent.name },
                "child": { "id": doc.child.id, "name": doc.child.name }
                } 
            FROM [{"parent":{"name": "joe", "id": "joe-abc"}, "child": {"name": "jim", "id":"jim-123"}}] doc
        ''')
        expected_result = [
        {
            "$1": {
                "child": {
                    "id": "jim-123",
                    "name": "jim"
                },
                "parent": {
                    "id": "***-***",
                    "name": "joe"
                }
            }
        }
        ]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")
        
        result = self.run_cbq_query('''
            SELECT {
                "parent": {
                    "id": IFMISSING(REDACT(doc.parent.id, { "pattern": ".*", "regex": true, "mask": "*" }), "******"),
                    "name": doc.parent.name },
                "child": { "id": doc.child.id, "name": doc.child.name }
                } 
            FROM [{"parent":{"name": "joe"}, "child": {"name": "jim", "id":"jim-123"}}] doc
        ''')
        expected_result = [
        {
            "$1": {
                "child": {
                    "id": "jim-123",
                    "name": "jim"
                },
                "parent": {
                    "id": "******",
                    "name": "joe"
                }
            }
        }
        ]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_ifnull(self):
        result = self.run_cbq_query('''
            SELECT {
                "parent": {
                    "id": IFNULL(REDACT(doc.parent.id, { "pattern": ".*", "regex": true, "mask": "*" }), "******"),
                    "name": doc.parent.name },
                "child": { "id": doc.child.id, "name": doc.child.name }
                } 
            FROM [{"parent":{"name": "joe", "id": "joe-abc"}, "child": {"name": "jim", "id":"jim-123"}}] doc
        ''')
        expected_result = [
        {
            "$1": {
                "child": {
                    "id": "jim-123", 
                    "name": "jim"
                },
                "parent": {
                    "id": "***-***",
                    "name": "joe"
                }
            }
        }
        ]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

        result = self.run_cbq_query('''
            SELECT {
                "parent": {
                    "id": IFNULL(REDACT(doc.parent.id, { "pattern": ".*", "regex": true, "mask": "*" }), "******"),
                    "name": doc.parent.name },
                "child": { "id": doc.child.id, "name": doc.child.name }
                } 
            FROM [{"parent":{"name": "joe", "id": null}, "child": {"name": "jim", "id":"jim-123"}}] doc
        ''')
        expected_result = [
        {
            "$1": {
                "child": {
                    "id": "jim-123",
                    "name": "jim"
                },
                "parent": {
                    "id": "******",
                    "name": "joe"
                }
            }
        }
        ]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_case(self):   
        result = self.run_cbq_query('''
            SELECT {
                "parent": {
                    "id": CASE WHEN REDACT(doc.parent.id) IS NULL THEN "******" ELSE REDACT(doc.parent.id) END,
                    "name": doc.parent.name },
                "child": { "id": doc.child.id, "name": doc.child.name }
            } 
            FROM [{"parent":{"name": "joe", "id": null}, "child": {"name": "jim", "id":"jim-123"}}] doc
        ''')
        expected_result = [
        {
            "$1": {
                "child": {
                    "id": "jim-123",
                    "name": "jim"
                },
                "parent": {
                    "id": "******",
                    "name": "joe"
                }
            }
        }
        ]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")

    def test_redact_field_selection(self):
        result = self.run_cbq_query('''
            SELECT {
                "parent": {
                    "name": doc.parent.name,
                    "id": REDACT(doc.parent.id, {"mask": "*"})
                },
                "child": doc.child
            }
            FROM [{"parent":{"name": "joe", "id": "joe-123"}, "child": {"name": "jim", "id":"jim-123"}}] doc
        ''')
        expected_result = [
        {
            "$1": {
                "child": {
                    "id": "jim-123",
                    "name": "jim"   
                },
                "parent": {
                    "id": "***-***",
                    "name": "joe"
                }
            }
        }
        ]
        self.assertEqual(expected_result, result['results'], f"We expected {expected_result} but got {result['results']}")