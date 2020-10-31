
from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests
import time
from deepdiff import DeepDiff

class QueryUDFTests(QueryTests):

    def setUp(self):
        super(QueryUDFTests, self).setUp()
        self.log.info("==============  QueryUDFTests setup has started ==============")
        self.shell = RemoteMachineShellConnection(self.master)
        self.info = self.shell.extract_remote_info()
        if self.info.type.lower() == 'windows':
            self.curl_path = "%scurl" % self.path
        else:
            self.curl_path = "curl"
        self.named_params = self.input.param("named_params", False)
        self.no_params = self.input.param("no_params", False)
        self.special_chars = self.input.param("special_chars", False)
        self.namespace = self.input.param("namespace", False)
        self.invalid = self.input.param("invalid", False)
        self.reserved_word = self.input.param("reserved_word", False)
        self.replace = self.input.param("replace", False)
        self.log.info("==============  QuerySanityTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryUDFTests, self).suite_setUp()
        self.log.info("==============  QueryUDFTests suite_setup has started ==============")
        if self.analytics:
            self.run_cbq_query(query="CREATE DATASET default on default")
            self.run_cbq_query(query="connect link Local")
        self.log.info("==============  QueryUDFTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryUDFTests tearDown has started ==============")
        self.log.info("==============  QueryUDFTests tearDown has completed ==============")
        super(QueryUDFTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryUDFTests suite_tearDown has started ==============")
        if self.analytics:
            self.run_cbq_query(query="DROP DATASET default ")
        self.log.info("==============  QueryUDFTests suite_tearDown has completed ==============")
        super(QueryUDFTests, self).suite_tearDown()

    '''Test that makes sure parameters work as expected
        -Fixed list of params (can only provide the amount expected, otherwise will error)
            -Extra test to make sure that the list of params can have special character in it 
        -Flexible list of params (can provide ANY number of params regardless of how many are used
        -No params (cannot pass any amount of params)'''
    def test_inline_params(self):
        try:
            try:
                if self.analytics:
                    if self.named_params:
                        if self.special_chars:
                            self.run_cbq_query("CREATE ANALYTICS FUNCTION celsius(deg_) { (`deg_` - 32) * 5/9}")
                        else:
                            self.run_cbq_query("CREATE ANALYTICS FUNCTION celsius(degrees) { (degrees - 32) * 5/9 }")
                    elif self.no_params:
                        self.run_cbq_query("CREATE ANALYTICS FUNCTION celsius() { (10 - 32) * 5/9 }")
                    else:
                        self.run_cbq_query("CREATE ANALYTICS FUNCTION celsius(...) { (args[0] - 32) * 5/9 }")
                else:
                    if self.named_params:
                        if self.special_chars:
                            self.run_cbq_query("CREATE FUNCTION celsius(deg_) LANGUAGE INLINE AS (`deg_` - 32) * 5/9")
                        else:
                            self.run_cbq_query("CREATE FUNCTION celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9")
                    elif self.no_params:
                        self.run_cbq_query("CREATE FUNCTION celsius() LANGUAGE INLINE AS (10 - 32) * 5/9")
                    else:
                        self.run_cbq_query("CREATE FUNCTION celsius(...) LANGUAGE INLINE AS (args[0] - 32) * 5/9")
            except Exception as e:
                self.log.error(str(e))
            try:
                if self.analytics:
                    proper = self.run_cbq_query("SELECT RAW celsius(10)")
                else:
                    proper = self.run_cbq_query("EXECUTE FUNCTION celsius(10)")
                self.assertEqual(proper['results'], [-12.222222222222221])
            except Exception as e:
                self.log.error(str(e))
                if self.no_params:
                    self.assertTrue("Incorrect number of arguments supplied to function celsius" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
                else:
                    self.fail()
            try:
                if self.analytics:
                    too_many = self.run_cbq_query("SELECT RAW celsius(10,15)")
                else:
                    too_many = self.run_cbq_query("EXECUTE FUNCTION celsius(10,15)")
                self.assertEqual(too_many['results'], [-12.222222222222221])
            except Exception as e:
                self.log.error(str(e))
                if self.named_params or self.no_params:
                    self.assertTrue("Incorrect number of arguments supplied to function celsius" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
                else:
                    self.fail()
            try:
                if self.analytics:
                    not_enough = self.run_cbq_query("SELECT RAW celsius()")
                else:
                    not_enough = self.run_cbq_query("EXECUTE FUNCTION celsius()")
                if self.no_params:
                    self.assertEqual(not_enough['results'], [-12.222222222222221])
                else:
                    self.assertEqual(not_enough['results'],[None])
            except Exception as e:
                self.log.error(str(e))
                if self.named_params:
                    self.assertTrue("Incorrect number of arguments supplied to function celsius" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
                else:
                    self.fail()
        finally:
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION celsius")
                else:
                    self.run_cbq_query("DROP FUNCTION celsius")
            except Exception as e:
                self.log.error(str(e))

    def test_inline_drop_function(self):
        try:
            try:
                if self.special_chars:
                    self.run_cbq_query("CREATE FUNCTION `c%.-_`(...) LANGUAGE INLINE AS (args[0] - 32) * 5/9")
                else:
                    self.run_cbq_query("CREATE FUNCTION cels(...) LANGUAGE INLINE AS (args[0] - 32) * 5/9")
            except Exception as e:
                self.log.error(str(e))
            try:
                if self.special_chars:
                    proper = self.run_cbq_query("EXECUTE FUNCTION `c%.-_`(10)")
                else:
                    proper = self.run_cbq_query("EXECUTE FUNCTION cels(10)")
                self.assertEqual(proper['results'], [-12.222222222222221])
            except Exception as e:
                self.log.error(str(e))
            try:
                if self.special_chars:
                    results = self.run_cbq_query("DROP FUNCTION `c%.-_`")
                else:
                    results = self.run_cbq_query("DROP FUNCTION cels")
                self.assertEqual(results['status'], 'success')
            except Exception as e:
                self.log.error(str(e))
                self.fail()
            try:
                if self.special_chars:
                    self.run_cbq_query("EXECUTE FUNCTION `c%.-_`(10)")
                else:
                    self.run_cbq_query("EXECUTE FUNCTION cels(10)")
                self.fail("Query should have error'd, but it did not")
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue('Function not found' in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
        finally:
            try:
                if self.special_chars:
                    self.run_cbq_query("DROP FUNCTION `c%.-_`")
                else:
                    self.run_cbq_query("DROP FUNCTION cels")
            except Exception as e:
                pass

    def test_inline_drop_missing_function(self):
        try:
            self.run_cbq_query(query="DROP FUNCTION func_does_not_exist")
        except Exception as e:
            self.log.error(str(e))
            self.assertTrue('Function not found func_does_not_exist' in str(e), "Error message is wrong {0}".format(str(e)))

    def test_inline_function_syntax(self):
        try:
            try:
                self.run_cbq_query("CREATE FUNCTION celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9")
                self.run_cbq_query("CREATE FUNCTION fahrenheit(...) { (args[0] * 9/5) + 32 }")
            except Exception as e:
                self.log.error(str(e))
                self.fail("Valid syntax creation error'd {0}".format(str(e)))

            try:
                results = self.run_cbq_query("EXECUTE FUNCTION celsius(10)")
                self.assertEqual(results['results'], [-12.222222222222221])
            except Exception as e:
                self.log.error(str(e))
                self.fail()
            try:
                results = self.run_cbq_query("EXECUTE FUNCTION fahrenheit(10)")
                self.assertEqual(results['results'], [50])
            except Exception as e:
                self.log.error(str(e))
                self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION celsius")
            except Exception as e:
                self.log.error(str(e))
            try:
                self.run_cbq_query("DROP FUNCTION fahrenheit")
            except Exception as e:
                self.log.error(str(e))

    def test_inline_function_syntax_scope(self):
        try:
            self.run_cbq_query("CREATE FUNCTION default:default.{0}.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9".format(self.scope))
            results = self.run_cbq_query("EXECUTE FUNCTION default:default.{0}.celsius(10)".format(self.scope))
            self.assertEqual(results['results'], [-12.222222222222221])
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION default:default.{0}.celsius".format(self.scope))
            except Exception as e:
                self.log.error(str(e))

    def test_inline_function_naming(self):
        function_name = ''
        try:
            try:
                if self.special_chars:
                    function_name = '`c%.-_`'
                    results = self.run_cbq_query("CREATE FUNCTION `c%.-_`(deg_) LANGUAGE INLINE AS (`deg_` - 32) * 5/9")
                elif self.namespace:
                    if self.reserved_word:
                        function_name = 'default:type'
                        results = self.run_cbq_query("CREATE FUNCTION default:type(...) LANGUAGE INLINE AS (10 - 32) * 5/9")
                    else:
                        function_name = 'default:celsius'
                        results = self.run_cbq_query("CREATE FUNCTION default:celsius(...) LANGUAGE INLINE AS (10 - 32) * 5/9")
                elif self.reserved_word:
                    results = self.run_cbq_query("CREATE FUNCTION join(...) LANGUAGE INLINE AS (10 - 32) * 5/9")
                elif self.invalid:
                    function_name = '`%.-`'
                    results = self.run_cbq_query("CREATE FUNCTION `%.-`(...) LANGUAGE INLINE AS (10 - 32) * 5/9")
                else:
                    function_name = 'celsius'
                    results = self.run_cbq_query("CREATE FUNCTION celsius(...) LANGUAGE INLINE AS (args[0] - 32) * 5/9")
                self.log.info(results)
                self.assertEqual(results['status'], "success")
            except Exception as e:
                self.log.error(str(e))
                if self.reserved_word and not self.namespace:
                    self.assertTrue('syntax error - at join' in str(e))
                else:
                    self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION {0}".format(function_name))
            except Exception as e:
                self.log.error(str(e))

    def test_inline_create_or_replace(self):
        try:
            if self.analytics:
                self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION func1(degrees) {(degrees - 32) * 5/9}")
                results = self.run_cbq_query("SELECT RAW func1(10)")
                self.assertEqual(results['results'], [-12.222222222222221])
                self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION func1(degree) { (degree * 9/5) + 32 }")
                results = self.run_cbq_query("SELECT RAW func1(10)")
                self.assertEqual(results['results'], [50])
            else:
                self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(...) LANGUAGE INLINE AS (args[0] - 32) * 5/9")
                results = self.run_cbq_query("EXECUTE FUNCTION func1(10)")
                self.assertEqual(results['results'], [-12.222222222222221])
                self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(...) { (args[0] * 9/5) + 32 }")
                results = self.run_cbq_query("EXECUTE FUNCTION func1(10)")
                self.assertEqual(results['results'], [50])
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION func1")
                else:
                    self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_system_functions_create_and_replace(self):
        try:
            self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(...) LANGUAGE INLINE AS (args[0] - 32) * 5/9")
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['expression'], '((((`args`[0]) - 32) * 5) / 9)')
            self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(...) { (args[0] * 9/5) + 32 }")
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['expression'], '((((`args`[0]) * 9) / 5) + 32)')
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_system_functions_drop(self):
        try:
            self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(...) { (args[0] * 9/5) + 32 }")
            self.run_cbq_query("DROP FUNCTION func1")
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'],[])
        except Exception as e:
            self.log.error(str(e))
            self.fail()

    def test_inline_query_function(self):
        try:
            self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(self.scope,self.collections[0]))
            results = self.run_cbq_query("EXECUTE FUNCTION func1('old hotel')")
            self.assertEqual(results['results'],[[{'test1': {'name': 'old hotel', 'type': 'hotel'}}, {'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}}, {'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}]])
            results = self.run_cbq_query("select func1('old hotel')")
            self.assertEqual(results['results'],[{'$1': [{'test1': {'name': 'old hotel', 'type': 'hotel'}}, {'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}}, {'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}]}])
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_inline_query_function_no_index(self):
        try:
            self.run_cbq_query(
                "CREATE OR REPLACE FUNCTION func1(nameval) {{ (select * from default:default.{0}.{1} where fake = nameval) }}".format(
                    self.scope, self.collections[0]))
            results = self.run_cbq_query("EXECUTE FUNCTION func1('old hotel')")
            self.fail()
        except Exception as e:
            self.log.error(str(e))
            self.assertTrue("No index available" in str(e), "Error message is wrong {0}".format(str(e)))
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_inline_query_function_syntax_error(self):
        try:
            self.run_cbq_query(
                "CREATE OR REPLACE FUNCTION func1(nameval) {{ (selet * from default:default.{0}.{1} where fake = nameval) }}".format(
                    self.scope, self.collections[0]))
            self.fail()
        except Exception as e:
            self.log.error(str(e))
            self.assertTrue("syntax error" in str(e), "Error message is wrong {0}".format(str(e)))
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_nested_inline_function(self):
        try:
            self.run_cbq_query("CREATE FUNCTION celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9")
            self.run_cbq_query("CREATE FUNCTION invert(...) { (celsius(args[0]) * 9/5) + 32 }")
            results = self.run_cbq_query("EXECUTE FUNCTION invert(10)")
            self.assertEqual(results['results'], [10])
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION celsius")
            except Exception as e:
                self.log.error(str(e))
            try:
                self.run_cbq_query("DROP FUNCTION invert")
            except Exception as e:
                self.log.error(str(e))

    def test_nested_inline_function_negative(self):
        try:
            self.run_cbq_query("CREATE FUNCTION invert(...) { (celsius(args[0]) * 9/5) + 32 }")
            self.fail("Query did not error and it should have!")
        except Exception as e:
            self.log.error(str(e))
            self.assertTrue("Invalid function celsius" in str(e), "Error message is wrong {0}".format(str(e)))

    def test_inline_from(self):
        try:
            self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(self.scope,self.collections[0]))
            results = self.run_cbq_query('select f.* from func1("old hotel") f')
            self.assertEqual(results['results'], [{'test1': {'name': 'old hotel', 'type': 'hotel'}}, {'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}}, {'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_inline_where(self):
        try:
            try:
                self.run_cbq_query("CREATE FUNCTION func1(degrees) LANGUAGE INLINE AS (degrees - 32) ")
            except Exception as e:
                self.log.error(str(e))
                self.fail("Valid syntax creation error'd {0}".format(str(e)))
            results = self.run_cbq_query("SELECT * FROM default:default.test.test1  WHERE ANY v in test1.numbers SATISFIES v = func1(36) END")
            self.assertEqual(results['results'], [{'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_agg_udf(self):
        try:
            self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(a,b,c) { (SELECT RAW SUM((a+b+c-40))) }")
            results = self.run_cbq_query("EXECUTE FUNCTION func1(10,20,30)")
            self.assertEqual(results['results'], [[20]])

            results = self.run_cbq_query("SELECT func1(10,20,30)")
            self.assertEqual(results['results'], [{'$1': [20]}])

            self.run_cbq_query("CREATE OR REPLACE FUNCTION func2(nameval) {{ (select name from default:default.{0}.{1} where name = nameval) }}".format(self.scope,self.collections[0]))
            results = self.run_cbq_query("select count(func2('old hotel'))")
            self.assertEqual(results['results'], [{'$1': 1}])

        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")

            except Exception as e:
                self.log.error(str(e))

##############################################################################################
#
#   JAVASCRIPT FUNCTIONS
##############################################################################################
    def test_javascript_syntax(self):
        functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
        function_names = ["adder", "multiplier"]
        created = self.create_library("math", functions, function_names)
        try:
            self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")
            self.assertEqual(results['results'], [4])
            self.run_cbq_query(query='CREATE FUNCTION func2(a,b) LANGUAGE JAVASCRIPT AS "multiplier" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func2(2,3)")
            self.assertEqual(results['results'], [6])

            self.run_cbq_query(query='CREATE FUNCTION func3(...) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func3(1,3)")
            self.assertEqual(results['results'], [4])
            self.run_cbq_query(query='CREATE FUNCTION func4(...) LANGUAGE JAVASCRIPT AS "multiplier" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func4(1,3)")
            self.assertEqual(results['results'], [3])

            self.run_cbq_query(query='CREATE FUNCTION func5() LANGUAGE JAVASCRIPT AS "multiplier" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func5()")
            self.assertEqual(results['results'], ['NaN'])
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")
                self.run_cbq_query("DROP FUNCTION func3")
                self.run_cbq_query("DROP FUNCTION func4")
                self.run_cbq_query("DROP FUNCTION func5")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_params(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            self.run_cbq_query(query='CREATE FUNCTION func2(...) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func2(1,3,5)")
            self.assertEqual(results['results'], [4])

            try:
                results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3,5)")
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue("Incorrect number of arguments supplied to function func1" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
            try:
                results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1)")
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue("Incorrect number of arguments supplied to function func1" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_create_or_replace(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'javascript')
            self.assertEqual(results['results'][0]['functions']['definition']['library'], 'math')
            self.assertEqual(results['results'][0]['functions']['definition']['object'], 'adder')

            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(...) LANGUAGE JAVASCRIPT AS "multiplier" AT "math"')
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'javascript')
            self.assertEqual(results['results'][0]['functions']['definition']['library'], 'math')
            self.assertEqual(results['results'][0]['functions']['definition']['object'], 'multiplier')

        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_create_or_replace(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'javascript')
            self.assertEqual(results['results'][0]['functions']['definition']['library'], 'math')
            self.assertEqual(results['results'][0]['functions']['definition']['object'], 'adder')


            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(...) LANGUAGE JAVASCRIPT AS "multiplier" AT "math"')
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'javascript')
            self.assertEqual(results['results'][0]['functions']['definition']['library'], 'math')
            self.assertEqual(results['results'][0]['functions']['definition']['object'], 'multiplier')

        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_if_else(self):
        functions = '[{"name" : "adder","code" : "function adder(a, b, c) {if (a + b > c) { return a + b - c; } else if (a * b > c) { return a*b - c; } else { return a + b + c; }}"}]'
        function_names = ["adder"]
        created = self.create_library("math", functions, function_names)
        try:
            self.run_cbq_query(query='CREATE FUNCTION func1(a,b,c) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3,5)")
            self.assertEqual(results['results'], [9])
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,6,5)")
            self.assertEqual(results['results'], [2])
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(2,3,5)")
            self.assertEqual(results['results'], [1])
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_for_loop(self):
        functions = '[{"name" : "adder","code" : "function adder(a, b, c) { for (i=0; i< b; i++){ a = a + c; } return a; }"}]'
        string_functions = '[{"name" : "concater","code" : "function concater(a) { var text = \\"\\"; var x; for (x in a) {text += a[x] + \\" \\";} return text; }"}]'
        function_names = ["adder"]
        string_function_names = ["concater"]
        created = self.create_library("math", functions, function_names)
        created = self.create_library("strings", string_functions, string_function_names)

        try:
            self.run_cbq_query(query='CREATE FUNCTION func1(a,b,c) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3,5)")
            self.assertEqual(results['results'], [16])
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,2,5)")
            self.assertEqual(results['results'], [11])


            self.run_cbq_query(query='CREATE FUNCTION func2(a) LANGUAGE JAVASCRIPT AS "concater" AT "strings"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func2({'fname':'John', 'lname':'Doe', 'age':25})")
            self.assertEqual(results['results'], ['25 John Doe '])

        finally:
            try:
                self.delete_library("math")
                self.delete_library("strings")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_while_loop(self):
        functions = '[{"name" : "adder","code" : "function adder(a,b) { var i = 0; while (i < 3) { a = a + b; i++; } return a; }"}]'
        string_functions = '[{"name" : "multiplier","code" : "function multiplier(a,b) { do{ a = a + b; } while(a > b) return a; }"}]'
        function_names = ["adder"]
        string_function_names = ["multiplier"]
        created = self.create_library("math", functions, function_names)
        created = self.create_library("strings", string_functions, string_function_names)

        try:
            self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,2)")
            self.assertEqual(results['results'], [7])


            self.run_cbq_query(query='CREATE FUNCTION func2(a,b) LANGUAGE JAVASCRIPT AS "multiplier" AT "strings"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func2(-1,1)")
            self.assertEqual(results['results'], [0])

        finally:
            try:
                self.delete_library("math")
                self.delete_library("strings")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_function_syntax_scope(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION default:default.{0}.func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"'.format(self.scope))
            results = self.run_cbq_query("EXECUTE FUNCTION default:default.{0}.func1(1,4)".format(self.scope))
            self.assertEqual(results['results'], [5])
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION default:default.{0}.func1".format(self.scope))
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_syntax_error(self):
        functions = '[{"name" : "adder","code" : "function adder(a, b) { retur a + b; }"}]'
        function_names = ["adder", "multiplier"]
        created = self.create_library("math", functions, function_names)
        try:
            self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")
        except Exception as e:
            self.log.error(str(e))
            self.assertTrue('evaluator worker returned error' in str(e))
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_replace_lib_func(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'javascript')
            self.assertEqual(results['results'][0]['functions']['definition']['library'], 'math')
            self.assertEqual(results['results'][0]['functions']['definition']['object'], 'adder')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")
            self.assertEqual(results['results'], [4])

            function = '{"name" : "adder", "code" : "function adder(a,b) { return helper(a,b); } function helper(a,b) { return a - b; }"}'
            added = self.add_function("math", "adder", function)

            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'javascript')
            self.assertEqual(results['results'][0]['functions']['definition']['library'], 'math')
            self.assertEqual(results['results'][0]['functions']['definition']['object'], 'adder')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(3,1)")
            self.assertEqual(results['results'], [2])

        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_delete_lib_func(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'javascript')
            self.assertEqual(results['results'][0]['functions']['definition']['library'], 'math')
            self.assertEqual(results['results'][0]['functions']['definition']['object'], 'adder')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")
            self.assertEqual(results['results'], [4])

            deleted = self.delete_function("math", "adder")

            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")

        except Exception as e:
            self.log.error(str(e))
            self.assertTrue('function does not exist function adder' in str(e), "The query failed for the wrong reason {0}".format(str(e)))

        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_negative(self):
        try:
            try:
                self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
                results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue('function does not exist function adder' in str(e),
                                "The query failed for the wrong reason {0}".format(str(e)))

            try:
                functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
                function_names = ["adder", "multiplier"]
                results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")
                self.assertEqual(results['results'], [4])
                self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "sub" AT "math"')
                results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue('function does not exist function adder' in str(e),
                                "The query failed for the wrong reason {0}".format(str(e)))
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

##############################################################################################
#
#   JS-Inline Hybrid Tests
##############################################################################################

    def test_create_or_replace_js_to_inline(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'javascript')
            self.assertEqual(results['results'][0]['functions']['definition']['library'], 'math')
            self.assertEqual(results['results'][0]['functions']['definition']['object'], 'adder')
            self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(...) { (args[0] * 9/5) + 32 }")
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'inline')
            self.assertEqual(results['results'][0]['functions']['definition']['expression'], '((((`args`[0]) * 9) / 5) + 32)')
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_create_or_replace_inline_to_js(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(...) { (args[0] * 9/5) + 32 }")
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'inline')
            self.assertEqual(results['results'][0]['functions']['definition']['expression'], '((((`args`[0]) * 9) / 5) + 32)')

            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'javascript')
            self.assertEqual(results['results'][0]['functions']['definition']['library'], 'math')
            self.assertEqual(results['results'][0]['functions']['definition']['object'], 'adder')
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_js_inline_where(self):
        try:
            self.run_cbq_query("CREATE FUNCTION func1(degrees) LANGUAGE INLINE AS (degrees - 32) ")
            results = self.run_cbq_query("SELECT * FROM default:default.test.test1  WHERE ANY v in test1.numbers SATISFIES v = func1(36) END")
            self.assertEqual(results['results'], [{'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])

            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query(
                    query='CREATE OR REPLACE FUNCTION func2(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query("SELECT * FROM default:default.test.test1  WHERE ANY v in test1.numbers SATISFIES v = func2(1,3) END")
            self.assertEqual(results['results'], [{'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])

        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")
            except Exception as e:
                self.log.error(str(e))

    '''This test case won't work in Chesire cat'''
    def test_udf_index(self):
        try:
            self.run_cbq_query("CREATE FUNCTION func1(degrees) LANGUAGE INLINE AS (degrees - 32) ")
            creation = self.run_cbq_query(query='CREATE INDEX idx on default:default.{0}.{1}(DISTINCT ARRAY v for v in numbers when v < func1(36) END)'.format(self.scope,self.collections[0]))
            results = self.run_cbq_query("SELECT * FROM default:default.test.test1  WHERE ANY v in test1.numbers SATISFIES v < func1(36) END")

        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))


##############################################################################################
#
#   JAVASCRIPT Libraries
##############################################################################################
    def test_create_library(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.assertTrue(created, "The library was not created! Check run logs for more details")
        finally:
            self.delete_library("math")

    def test_update_library(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.assertTrue(created, "The library was not created! Check run logs for more details")
            functions = '[{"name" : "sub","code" : "function sub(a, b) { return a - b; }"}, {"name" : "divider","code" : "function divider(a, b) { return a / b; }"}]'
            if self.replace:
                function_names = ["sub", "divider"]
            else:
                function_names = ["sub", "divider", "adder", "multiplier"]
            created = self.create_library("math", functions, function_names, self.replace)
            self.assertTrue(created, "The library was not updated! Check run logs for more details")
        finally:
            self.delete_library("math")


    def test_delete_library(self):
        functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
        function_names = ["adder", "multiplier"]
        self.create_library("math", functions, function_names)
        deleted = self.delete_library("math")
        self.assertTrue(deleted, "The library was not deleted! Check run logs for more details")

    def test_add_function(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            self.create_library("math", functions, function_names)
            function ='{"name" : "sub", "code" : "function sub(a,b) { return helper(a,b); } function helper(a,b) { return a - b; }"}'
            added = self.add_function("math", "sub", function)
            self.assertTrue(added, "The function was not added to the library!")
        finally:
            self.delete_library("math")

    def test_replace_function(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            self.create_library("math", functions, function_names)
            function = '{"name" : "adder", "code" : "function adder(a,b) { return helper(a,b); } function helper(a,b) { return a - b; }"}'
            added = self.add_function("math", "adder", function)
            url = "http://{0}:{1}/functions/v1/libraries/math/functions/adder".format(self.master.ip, self.n1ql_port)
            function = self.shell.execute_command("{0} {1} -u Administrator:password".format(self.curl_path, url))
            self.assertTrue('a - b' in str(function[0]), "The function was not overridden {0}".format(function))
        finally:
            self.delete_library("math")

    def test_delete_function(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            self.create_library("math", functions, function_names)
            deleted = self.delete_function("math", "adder")
            self.assertTrue(deleted, "The function was not deleted from the library!")
        finally:
            self.delete_library("math")

    def test_read_function_negative(self):
        try:
            functions = '[{"name" : "adder","code" : "function adder(a, b) { return a + b; }"}, {"name" : "multiplier","code" : "function multiplier(a, b) { return a * b; }"}]'
            function_names = ["adder", "multiplier"]
            self.create_library("math", functions, function_names)
            url = "http://{0}:{1}/functions/v1/libraries/math/fakefunction".format(self.master.ip, self.n1ql_port)
            libraries = self.shell.execute_command("{0} {1} -u Administrator:password".format(self.curl_path, url))
            self.assertTrue("fakefunction" not in str(libraries), "this function should not exist! {0}".format(libraries))
        finally:
            self.delete_library("math")

    '''Create a library with functions, check to see that the library was created and the functions were created'''
    def create_library(self, library_name='', functions={},function_names=[], replace= False):
        created = False
        url = "http://{0}:{1}/functions/v1/libraries".format(self.master.ip, self.n1ql_port)
        data = '[{{"name": "{0}"'.format(library_name) + ', "functions": {0}}}]'.format(functions)
        if replace:
            self.shell.execute_command(
                "{0} -X PUT {1} -u Administrator:password -H 'content-type: application/json' -d '{2}'".format(
                    self.curl_path, url, data))
        else:
            self.shell.execute_command("{0} -X POST {1} -u Administrator:password -H 'content-type: application/json' -d '{2}'".format(self.curl_path, url, data))
        libraries = self.shell.execute_command("{0} {1} -u Administrator:password".format(self.curl_path, url))
        if library_name in str(libraries[0]):
            created = True
        else:
            self.log.error("The library {0} was not created: {1}".format(library_name, libraries))

        for function in function_names:
            if function in str(libraries[0]):
                created = True
            else:
                self.log.error("The function {0} was not created! {1}".format(function, libraries))
                created = False
                break
        return created

    '''Delete a library'''
    def delete_library(self, library_name =''):
        deleted = False
        url = "http://{0}:{1}/functions/v1/libraries/{2}".format(self.master.ip, self.n1ql_port, library_name)
        curl_output = self.shell.execute_command("{0} -X DELETE {1} -u Administrator:password ".format(self.curl_path, url))
        libraries = self.shell.execute_command("{0} {1} -u Administrator:password".format(self.curl_path, url))
        if library_name not in str(libraries):
            deleted = True
        return deleted

    '''Add a function to an existing library, it is assumed the library already exists'''
    def add_function(self, library_name ='', function_name ='', function =''):
        added = False
        url = "http://{0}:{1}/functions/v1/libraries/{2}/functions/{3}".format(self.master.ip, self.n1ql_port, library_name, function_name)
        self.shell.execute_command("{0} -X PUT {1} -u Administrator:password -H 'content-type: application/json' -d '{2}'".format(self.curl_path, url, function))
        function = self.shell.execute_command("{0} {1} -u Administrator:password".format(self.curl_path, url))
        if function_name in str(function[0]):
            added = True
        else:
            url = "http://{0}:{1}/functions/v1/libraries".format(self.master.ip, self.n1ql_port)
            library = self.shell.execute_command("{0} {1} -u Administrator:password".format(self.curl_path, url))
            self.log.error("Function url was not found, here is the library it should have been added to! {0}".format(library[0]))
        return added

    '''Delete a specific function'''
    def delete_function(self, library_name ='', function_name =''):
        deleted = False
        url = "http://{0}:{1}/functions/v1/libraries/{2}/functions/{3}".format(self.master.ip, self.n1ql_port, library_name,function_name)
        curl_output = self.shell.execute_command("{0} -X DELETE {1} -u Administrator:password ".format(self.curl_path, url))
        libraries = self.shell.execute_command("{0} {1} -u Administrator:password".format(self.curl_path, url))
        if library_name not in str(libraries):
            deleted = True
        return deleted


