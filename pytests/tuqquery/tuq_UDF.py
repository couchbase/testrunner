
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
        if not self.analytics:
            self.run_cbq_query(query="delete from system:prepareds")
        users = self.input.param("users", None)
        self.inp_users = []
        if users:
            self.inp_users = eval(eval(users))
        self.users = self.get_user_list()
        self.roles = self.get_user_role_list()
        self.all_buckets = self.input.param("all_buckets", False)
        self.scoped = self.input.param("scoped", False)
        self.rebalance_in = self.input.param("rebalance_in", False)
        self.log.info("==============  QuerySanityTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryUDFTests, self).suite_setUp()
        self.log.info("==============  QueryUDFTests suite_setup has started ==============")
        changed = False
        if self.load_collections:
            if not self.analytics:
                self.run_cbq_query(query='CREATE INDEX idx on default(name)')
                self.sleep(5)
                self.wait_for_all_indexes_online()
            self.collections_helper.create_scope(bucket_name="default", scope_name="test2")
            self.collections_helper.create_collection(bucket_name="default", scope_name="test2",
                                                      collection_name=self.collections[0])
            self.collections_helper.create_collection(bucket_name="default", scope_name="test2",
                                                      collection_name=self.collections[1])
            if self.analytics:
                self.run_cbq_query(query="CREATE DATASET collection3 on default.test2.test1")
                self.run_cbq_query(query="CREATE DATASET collection4 on default.test2.test2")
            if not self.analytics:
                self.run_cbq_query(
                    query="CREATE INDEX idx1 on default:default.test2.{0}(name)".format(self.collections[0]))
                self.run_cbq_query(
                    query="CREATE INDEX idx2 on default:default.test2.{0}(name)".format(self.collections[1]))
                self.sleep(5)
                self.wait_for_all_indexes_online()
            if self.analytics:
                self.analytics = False
                changed = True
            self.run_cbq_query(
                query=('INSERT INTO default:default.test2.{0}'.format(self.collections[
                    1]) + '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "old hotel" })'))
            self.run_cbq_query(
                query=('INSERT INTO default:default.test2.{0}'.format(self.collections[1]) + '(KEY, VALUE) VALUES ("key2", { "type" : "hotel", "name" : "new hotel" })'))
            self.run_cbq_query(
                query=('INSERT INTO default:default.test2.{0}'.format(self.collections[1]) + '(KEY, VALUE) VALUES ("key3", { "type" : "hotel", "name" : "new hotel" })'))
            self.sleep(20)
        if self.load_sample:
            self.rest.load_sample("travel-sample")
            init_time = time.time()
            while True:
                next_time = time.time()
                query_response = self.run_cbq_query("SELECT COUNT(*) FROM `travel-sample`")
                if query_response['results'][0]['$1'] == 31591:
                    break
                if next_time - init_time > 600:
                    break
                time.sleep(1)
            if changed:
                self.analytics = True
            if self.analytics:
                self.run_cbq_query(query="CREATE DATASET travel on `travel-sample`")
        if changed:
            self.analytics = True
        self.log.info("==============  QueryUDFTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryUDFTests tearDown has started ==============")
        self.log.info("==============  QueryUDFTests tearDown has completed ==============")
        super(QueryUDFTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueryUDFTests suite_tearDown has started ==============")
        if self.analytics:
            self.run_cbq_query(query="DROP DATASET travel")
            self.run_cbq_query(query="DROP DATASET collection1")
            self.run_cbq_query(query="DROP DATASET collection2")
            self.run_cbq_query(query="DROP DATASET collection3")
            self.run_cbq_query(query="DROP DATASET collection4")

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
                            self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION celsius(deg_) { (`deg_` - 32) * 5/9}")
                        else:
                            self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION celsius(degrees) { (degrees - 32) * 5/9 }")
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
                    if self.analytics:
                        self.assertTrue("Cannot find function with signature" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
                    else:
                        self.assertTrue("Incorrect number of arguments supplied to function 'celsius'" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
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
                    if self.analytics:
                        self.assertTrue("Cannot find function with signature" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
                    else:
                        self.assertTrue("Incorrect number of arguments supplied to function 'celsius'" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
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
                    if self.analytics:
                        self.assertTrue("Cannot find function with signature" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
                    else:
                        self.assertTrue("Incorrect number of arguments supplied to function 'celsius'" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
                else:
                    self.fail()
        finally:
            try:
                if self.analytics:
                    if self.named_params:
                        if self.special_chars:
                            self.run_cbq_query(
                                "DROP ANALYTICS FUNCTION celsius(deg_)")
                        else:
                            self.run_cbq_query(
                                "DROP ANALYTICS FUNCTION celsius(degrees) ")
                    elif self.no_params:
                        self.run_cbq_query("DROP ANALYTICS FUNCTION celsius()")
                    else:
                        self.run_cbq_query("DROP ANALYTICS FUNCTION celsius(...) ")
                else:
                    self.run_cbq_query("DROP FUNCTION celsius")
            except Exception as e:
                self.log.error(str(e))

    def test_inline_drop_function(self):
        try:
            try:
                if self.analytics:
                    if self.special_chars:
                        self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION `c%.-_`(param1) {(param1 - 32) * 5/9}")
                    else:
                        self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION cels(param1) {(param1 - 32) * 5/9}")
                else:
                    if self.special_chars:
                        self.run_cbq_query("CREATE FUNCTION `c%.-_`(...) LANGUAGE INLINE AS (args[0] - 32) * 5/9")
                    else:
                        self.run_cbq_query("CREATE FUNCTION cels(...) LANGUAGE INLINE AS (args[0] - 32) * 5/9")
            except Exception as e:
                self.log.error(str(e))
            try:
                if self.analytics:
                    if self.special_chars:
                        proper = self.run_cbq_query("SELECT RAW `c%.-_`(10)")
                    else:
                        proper = self.run_cbq_query("SELECT RAW cels(10)")
                else:
                    if self.special_chars:
                        proper = self.run_cbq_query("EXECUTE FUNCTION `c%.-_`(10)")
                    else:
                        proper = self.run_cbq_query("EXECUTE FUNCTION cels(10)")
                self.assertEqual(proper['results'], [-12.222222222222221])
            except Exception as e:
                self.log.error(str(e))
            try:
                if self.analytics:
                    if self.special_chars:
                        results = self.run_cbq_query("DROP ANALYTICS FUNCTION `c%.-_`(param1)")
                    else:
                        results = self.run_cbq_query("DROP ANALYTICS FUNCTION cels(param1)")
                else:
                    if self.special_chars:
                        results = self.run_cbq_query("DROP FUNCTION `c%.-_`")
                    else:
                        results = self.run_cbq_query("DROP FUNCTION cels")
                self.assertEqual(results['status'], 'success')
            except Exception as e:
                self.log.error(str(e))
                self.fail()
            try:
                if self.analytics:
                    if self.special_chars:
                        self.run_cbq_query("SELECT RAW `c%.-_`(10)")
                    else:
                        self.run_cbq_query("SELECT RAW cels(10)")
                else:
                    if self.special_chars:
                        self.run_cbq_query("EXECUTE FUNCTION `c%.-_`(10)")
                    else:
                        self.run_cbq_query("EXECUTE FUNCTION cels(10)")
                self.fail("Query should have error'd, but it did not")
            except Exception as e:
                self.log.error(str(e))
                if self.analytics:
                    self.assertTrue('Cannot find function with signature' in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
                else:
                    self.assertTrue('not found' in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
        finally:
            try:
                if self.analytics:
                    if self.special_chars:
                        self.run_cbq_query("DROP ANALYTICS FUNCTION `c%.-_`(param1)")
                    else:
                        self.run_cbq_query("DROP ANALYTICS FUNCTION cels(param1)")
                else:
                    if self.special_chars:
                        self.run_cbq_query("DROP FUNCTION `c%.-_`")
                    else:
                        self.run_cbq_query("DROP FUNCTION cels")
            except Exception as e:
                self.log.info(str(e))

    def test_inline_drop_missing_function(self):
        try:
            if self.analytics:
                self.run_cbq_query(query="DROP ANALYTICS FUNCTION func_does_not_exist")
            else:
                self.run_cbq_query(query="DROP FUNCTION func_does_not_exist")
        except Exception as e:
            self.log.error(str(e))
            if self.analytics:
                self.assertTrue('DROP ANALYTICS FUNCTION func_does_not_exist' in str(e), "Error message is wrong {0}".format(str(e)))
            else:
                self.assertTrue("Function 'func_does_not_exist' not found" in str(e), "Error message is wrong {0}".format(str(e)))

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
            if self.analytics:
                self.run_cbq_query(
                    "CREATE OR REPLACE ANALYTICS FUNCTION Default.celsius(degrees) {(degrees - 32) * 5/9}")
                results = self.run_cbq_query("SELECT RAW Default.celsius(10)".format(self.scope))
            else:
                self.run_cbq_query("CREATE FUNCTION default:default.{0}.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9".format(self.scope))
                results = self.run_cbq_query("EXECUTE FUNCTION default:default.{0}.celsius(10)".format(self.scope))
            self.assertEqual(results['results'], [-12.222222222222221])
            if not self.analytics:
                results = self.run_cbq_query("EXECUTE FUNCTION celsius(10)".format(self.scope))
                self.fail()
        except Exception as e:
            self.log.info(str(e))
            self.assertTrue("Function 'celsius' not found" in str(e))
        finally:
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION Default.celsius(degrees)")
                else:
                    self.run_cbq_query("DROP FUNCTION default:default.{0}.celsius".format(self.scope))
            except Exception as e:
                self.log.error(str(e))

    def test_inline_function_query_context(self):
        try:
            self.run_cbq_query("CREATE FUNCTION celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9", query_context='default:default.test')
            results = self.run_cbq_query("EXECUTE FUNCTION default:default.{0}.celsius(10)".format(self.scope))
            self.assertEqual(results['results'], [-12.222222222222221])
            results = self.run_cbq_query("EXECUTE FUNCTION celsius(10)")
        except Exception as e:
            self.log.info(str(e))
            self.assertTrue("Function 'celsius' not found" in str(e))
        try:
            results = self.run_cbq_query("EXECUTE FUNCTION celsius(10)", query_context='default:default.test')
            self.assertEqual(results['results'], [-12.222222222222221])
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION celsius".format(self.scope), query_context='default:default.test')
            except Exception as e:
                self.log.error(str(e))
    
    def test_MB66219(self):
        try:
            self.run_cbq_query('UPSERT INTO default VALUES("airline_001", {"type": "airline", "c10":10, "c11":11})')
            self.run_cbq_query('CREATE INDEX ix66219 ON default (c10,c11) WHERE type = "airline"')
            self.sleep(5)
            self.wait_for_all_indexes_online()
            self.run_cbq_query('CREATE OR REPLACE FUNCTION func2(param) { (WITH w1 AS ( (SELECT RAW f1 FROM param.f AS f1)[0]) SELECT a.* FROM default AS a WHERE a.type = "airline" AND a.c10 = w1)}')

            results = self.run_cbq_query('EXECUTE FUNCTION func2({"f":10})')
            self.assertEqual(results['results'], [[{'c10': 10, 'c11': 11, 'type': 'airline'}]])

            self.run_cbq_query('DROP INDEX default.ix66219')
            self.run_cbq_query('CREATE INDEX ix66219 ON default (c10,c11) WHERE type = "airline"')
            self.sleep(5)
            self.wait_for_all_indexes_online()

            results = self.run_cbq_query('EXECUTE FUNCTION func2({"f":10})')
            self.assertEqual(results['results'], [[{'c10': 10, 'c11': 11, 'type': 'airline'}]])

        finally:
            try:
                self.run_cbq_query('DROP INDEX default.ix66219')
                self.run_cbq_query('DROP FUNCTION func2')
            except Exception as e:
                self.log.error(str(e))

    def test_inline_join(self):
        try:
            if self.analytics:
                self.run_cbq_query(
                    "CREATE OR REPLACE ANALYTICS FUNCTION func1(nameval) { (select * from collection1 t1 INNER JOIN collection4 t2 ON t1.name = t2.name where t1.name = nameval) }")
            else:
                self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(nameval) {{ (select * from default:default.test.test1 t1 INNER JOIN default:default.test2.test2 t2 ON t1.name = t2.name where t1.name = nameval) }}".format(self.scope,self.collections[0]))
                results = self.run_cbq_query("EXECUTE FUNCTION func1('old hotel')")
                self.assertEqual(results['results'], [[{'t1': {'name': 'old hotel', 'type': 'hotel'}, 't2': {'name': 'old hotel', 'type': 'hotel'}}, {'t1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}, 't2': {'name': 'old hotel', 'type': 'hotel'}}, {'t1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}, 't2': {'name': 'old hotel', 'type': 'hotel'}}]])
            results = self.run_cbq_query("select func1('old hotel')")
            self.assertEqual(results['results'], [{'$1': [{'t1': {'name': 'old hotel', 'type': 'hotel'}, 't2': {'name': 'old hotel', 'type': 'hotel'}}, {'t1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}, 't2': {'name': 'old hotel', 'type': 'hotel'}}, {'t1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}, 't2': {'name': 'old hotel', 'type': 'hotel'}}]}])
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION func1(nameval)")
                else:
                    self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    '''Test a query that uses a function containing a query in the from'''
    def test_inline_subquery_from(self):
        if not self.analytics:
            string_functions = 'function concater(a,b) { var text = ""; var x; for (x in a) {if (x = b) { return x; }} return "n"; } function comparator(a, b) {if (a > b) { return "old hotel"; } else { return "new hotel" }}'
            function_names2 = ["concater","comparator"]
            created2 = self.create_library("strings",string_functions,function_names2)
        try:
            if self.analytics:
                self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION func2(degrees) { (degrees - 32)} ")
                self.run_cbq_query(
                    "CREATE OR REPLACE ANALYTICS FUNCTION func4(nameval) {{ (select * from collection1 where collection1.name = nameval) }}".format(
                        self.scope, self.collections[0]))
                results = self.run_cbq_query(
                    "SELECT f.collection1.name FROM func4('old hotel') as f LET maximum_no = func2(36) WHERE ANY v in f.collection1.numbers SATISFIES v = maximum_no END GROUP BY f.collection1.name LETTING letter = 'o' HAVING f.collection1.name > letter")
                self.assertEqual(results['results'], [{'name': 'old hotel'}])
            else:
                self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "comparator" AT "strings"')
                self.run_cbq_query("CREATE FUNCTION func2(degrees) LANGUAGE INLINE AS (degrees - 32) ")
                self.run_cbq_query(query='CREATE FUNCTION func3(a,b) LANGUAGE JAVASCRIPT AS "concater" AT "strings"')
                self.run_cbq_query(
                    "CREATE OR REPLACE FUNCTION func4(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(
                        self.scope, self.collections[0]))
                results = self.run_cbq_query(
                    "SELECT f.test1.name FROM func4('old hotel') as f LET maximum_no = func2(36) WHERE ANY v in f.test1.numbers SATISFIES v = maximum_no END GROUP BY f.test1.name LETTING letter = func3('old hotel', 'o') HAVING f.test1.name > letter")
                self.assertEqual(results['results'], [{'name': 'old hotel'}])
        finally:
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION func2(degrees)")
                    self.run_cbq_query("DROP ANALYTICS FUNCTION func4(nameval)")
                else:
                    self.delete_library("strings")
                    self.run_cbq_query("DROP FUNCTION func1")
                    self.run_cbq_query("DROP FUNCTION func2")
                    self.run_cbq_query("DROP FUNCTION func3")
                    self.run_cbq_query("DROP FUNCTION func4")
            except Exception as e:
                self.log.error(str(e))

    '''Test a function that contains a subquery and uses other functions'''
    def test_inline_subquery_nested(self):
        string_functions = 'function concater(a,b) { var text = ""; var x; for (x in a) {if (x = b) { return x; }} return "n"; } function comparator(a, b) {if (a > b) { return "old hotel"; } else { return "new hotel" }}'
        function_names2 = ["concater","comparator"]
        created2 = self.create_library("strings",string_functions,function_names2)
        try:
            self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "comparator" AT "strings"')
            self.run_cbq_query("CREATE FUNCTION func2(degrees) LANGUAGE INLINE AS (degrees - 32) ")
            self.run_cbq_query(query='CREATE FUNCTION func3(a,b) LANGUAGE JAVASCRIPT AS "concater" AT "strings"')
            self.run_cbq_query(
                "CREATE OR REPLACE FUNCTION func4(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(
                    self.scope, self.collections[0]))
            results = self.run_cbq_query(
                "CREATE OR REPLACE FUNCTION func5(nameval) {(SELECT f.test1.name FROM func4(nameval) as f LET maximum_no = func2(36) WHERE ANY v in f.test1.numbers SATISFIES v = maximum_no END GROUP BY f.test1.name LETTING letter = func3('old hotel', 'o') HAVING f.test1.name > letter)}")
            results = self.run_cbq_query(query="select func5('old hotel')")
            self.assertEqual(results['results'], [{'$1': [{'name': 'old hotel'}]}])
            results2 = self.run_cbq_query(query="EXECUTE FUNCTION func5('old hotel')")
            self.assertEqual(results2['results'], [[{'name': 'old hotel'}]])
        finally:
            try:
                self.delete_library("strings")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")
                self.run_cbq_query("DROP FUNCTION func3")
                self.run_cbq_query("DROP FUNCTION func4")
                self.run_cbq_query("DROP FUNCTION func5")
            except Exception as e:
                self.log.error(str(e))

    def test_inline_subquery_where(self):
        try:
            if self.analytics:
                self.run_cbq_query(
                    "CREATE OR REPLACE ANALYTICS FUNCTION func4(doctype) { (SELECT RAW city FROM travel WHERE `type` = doctype) }")
                results = self.run_cbq_query(
                    'SELECT t1.city FROM travel t1 WHERE t1.`type` = "landmark" AND t1.city IN func4("airport")')
                self.assertEqual(results['metrics']['resultCount'], 2776)
                results = self.run_cbq_query(
                    'CREATE OR REPLACE ANALYTICS FUNCTION func5(doctype) {(SELECT t1.city FROM travel t1 WHERE t1.`type` = "landmark" AND t1.city IN func4(doctype))}')
                results = self.run_cbq_query('SELECT RAW func5("airport")')
                self.assertEqual(results['metrics']['resultCount'], 1)
            else:
                self.run_cbq_query(
                    "CREATE OR REPLACE FUNCTION func4(doctype) { (SELECT RAW city FROM `travel-sample` WHERE type = doctype) }")
                results = self.run_cbq_query('SELECT t1.city FROM `travel-sample` t1 WHERE t1.type = "landmark" AND t1.city IN func4("airport")')
                self.assertEqual(results['metrics']['resultCount'], 2776)
                results = self.run_cbq_query(
                    'CREATE OR REPLACE FUNCTION func5(doctype) {(SELECT t1.city FROM `travel-sample` t1 WHERE t1.type = "landmark" AND t1.city IN func4(doctype))}')
                results = self.run_cbq_query('EXECUTE FUNCTION func5("airport")')
                self.assertEqual(results['metrics']['resultCount'], 1)
        finally:
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION func4(doctype)")
                    self.run_cbq_query("DROP ANALYTICS FUNCTION func5(doctype)")
                else:
                    self.run_cbq_query("DROP FUNCTION func4")
                    self.run_cbq_query("DROP FUNCTION func5")
            except Exception as e:
                self.log.error(str(e))

    def test_inline_subquery_select(self):
        try:
            self.run_cbq_query(
                "CREATE OR REPLACE FUNCTION func4() { (SELECT RAW t1.geo.alt FROM `travel-sample`) }")
            results = self.run_cbq_query('SELECT array_length(func4()) FROM `travel-sample`')
            self.assertEqual(results['metrics']['resultCount'], 31591)
            self.assertEqual(results['results'][0], {'$1': 31591})
            results = self.run_cbq_query(
                'CREATE OR REPLACE FUNCTION func5() {(SELECT array_length(func4()) FROM `travel-sample`)}')
            results = self.run_cbq_query('EXECUTE FUNCTION func5()')
            self.assertEqual(results['metrics']['resultCount'], 1)
            self.assertEqual(results['results'][0][0], {'$1': 31591})
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func4")
                self.run_cbq_query("DROP FUNCTION func5")
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
                    self.assertTrue('syntax error - line 1, column 17' in str(e))
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
                    self.run_cbq_query("DROP ANALYTICS FUNCTION func1(degree)")
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
            if self.analytics:
                self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION celsius(degrees) {(degrees - 32) * 5/9}")
                self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION invert(param1) { (celsius(param1) * 9/5) + 32 }")
                results = self.run_cbq_query("SELECT RAW invert(10)")
                self.assertEqual(results['results'], [10])
            else:
                self.run_cbq_query("CREATE FUNCTION celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9")
                self.run_cbq_query("CREATE FUNCTION invert(...) { (celsius(args[0]) * 9/5) + 32 }")
                results = self.run_cbq_query("EXECUTE FUNCTION invert(10)")
                self.assertEqual(results['results'], [10])
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION celsius(degrees)")
                else:
                    self.run_cbq_query("DROP FUNCTION celsius")
            except Exception as e:
                self.log.error(str(e))
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION invert(param1)")
                else:
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
                if self.analytics:
                    self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION func1(degrees) {(degrees - 32)} ")
                else:
                    self.run_cbq_query("CREATE FUNCTION func1(degrees) LANGUAGE INLINE AS (degrees - 32) ")
            except Exception as e:
                self.log.error(str(e))
                self.fail("Valid syntax creation error'd {0}".format(str(e)))
            if self.analytics:
                results = self.run_cbq_query(
                    "SELECT * FROM collection1 WHERE ANY v in collection1.numbers SATISFIES v = func1(36) END")
            else:
                results = self.run_cbq_query("SELECT * FROM default:default.test.test1  WHERE ANY v in test1.numbers SATISFIES v = func1(36) END")
            if self.analytics:
                self.assertEqual(results['results'], [{'collection1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
            else:
                self.assertEqual(results['results'], [{'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
        finally:
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION func1(degrees)")
                else:
                    self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    ''' This test makes sure that prepared statements executed one after another do not return UDF nested 
        aggregates not allowed error'''
    def test_MB58582(self):
        self.run_cbq_query('UPSERT INTO default VALUES("k01", {"cid": "c01", "status":"active", "pid": "p01"})')
        self.run_cbq_query('CREATE INDEX ix1 ON default(cid, status, pid)')
        self.run_cbq_query('CREATE OR REPLACE FUNCTION udf1(pObj, pcustId) '
                           '{(WITH pm as ( Select * from OBJECT_INNER_PAIRS(pObj) as m) SELECT pid, count(1) '
                           'AS numRows FROM default WHERE cid = pcustId AND status IN ["active"] GROUP BY pid)}')
        prepare_expected_results = self.run_cbq_query('SELECT a.* FROM udf1({"xx":"xx"}, "c01") AS a')
        prepare_expected_results2 = self.run_cbq_query('SELECT a.* FROM udf1({"xx":"xx"}, "c07") AS a')
        self.run_cbq_query('prepare p1 from SELECT a.* FROM udf1({"xx":"xx"}, "c01") AS a')
        self.run_cbq_query('prepare p2 from SELECT a.* FROM udf1({"xx":"xx"}, "c07") AS a')
        for i in range(0,999):
            prepare_actual_results = self.run_cbq_query('execute p1')
            prepare_actual_results2 = self.run_cbq_query('execute p2')
            diffs = DeepDiff(prepare_actual_results['results'], prepare_expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            diffs = DeepDiff(prepare_actual_results2['results'], prepare_expected_results2['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

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

    def test_abort_udf(self):
        try:
            try:
                self.run_cbq_query('CREATE FUNCTION variadic(...) { CASE WHEN array_length(args) != 1 THEN abort("wrong args: " || to_string(array_length(args)))  WHEN type(args[0]) = "string" THEN args[0] ELSE abort("wrong type " || type(args[0]) || ": " || to_string(args[0])) END }')
            except Exception as e:
                self.log.error(str(e))
                self.fail()
            try:
                results = self.run_cbq_query("EXECUTE FUNCTION variadic(1,2)")
                self.fail("Function should have failed with an error {0}".format(results))
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue("wrong args: 2" in str(e))
            try:
                results = self.run_cbq_query("EXECUTE FUNCTION variadic(1)")
                self.fail("Function should have failed with an error {0}".format(results))
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue("wrong type number" in str(e))
            try:
                results = self.run_cbq_query("EXECUTE FUNCTION variadic('string')")
                self.assertEqual(results['results'], ['string'])
            except Exception as e:
                self.log.error(str(e))
                self.fail()
            try:
                results = self.run_cbq_query("select variadic(1,2)")
                self.fail("Function should have failed with an error {0}".format(results))
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue("wrong args: 2" in str(e))
            try:
                results = self.run_cbq_query("select variadic(1)")
                self.fail("Function should have failed with an error {0}".format(results))
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue("wrong type number" in str(e))
            try:
                results = self.run_cbq_query("select variadic('string')")
                self.assertEqual(results['results'], [{'$1': 'string'}])
            except Exception as e:
                self.log.error(str(e))
                self.fail()
            try:
                self.run_cbq_query('CREATE FUNCTION abort_msg() {  abort("This function simply aborts") }')
                results = self.run_cbq_query("select abort_msg()")
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue("This function simply aborts" in str(e))
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION variadic")
                self.run_cbq_query("DROP FUNCTION abort_msg")
            except Exception as e:
                self.log.error(str(e))

##############################################################################################
#
#   JAVASCRIPT FUNCTIONS
##############################################################################################
    def test_javascript_syntax(self):
        functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
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
            self.assertEqual(results['results'], [None])
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
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
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
                self.assertTrue("Incorrect number of arguments supplied to function 'func1'" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
            try:
                results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1)")
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue("Incorrect number of arguments supplied to function 'func1'" in str(e), "The error message is incorrect, please check it {0}".format(str(e)))
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_create_or_replace(self):
        try:
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
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
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
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
        functions = 'function adder(a, b, c) {if (a + b > c) { return a + b - c; } else if (a * b > c) { return a*b - c; } else { return a + b + c; }}'
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
        functions = 'function adder(a, b, c) { for (i=0; i< b; i++){ a = a + c; } return a; }'
        string_functions = 'function concater(a) { var text = ""; var x; for (x in a) {text += a[x] + " ";} return text; }'
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
        functions = 'function adder(a,b) { var i = 0; while (i < 3) { a = a + b; i++; } return a; }'
        string_functions = 'function multiplier(a,b) { do{ a = a + b; } while(a > b) return a; }'
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

    def test_javascript_infinite_loop(self):
        try:
            string_functions = 'function multiplier(a,b) { do{ a = a; } while(a > b) return a; }'
            string_function_names = ["multiplier"]
            created = self.create_library("strings", string_functions, string_function_names)

            try:
                # Should timeout after 10 seconds
                self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "multiplier" AT "strings"')
                results = self.run_cbq_query(query="EXECUTE FUNCTION func1(2,1)", query_params={'timeout':"10s"})
                self.fail("Query should have timed out")
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue("Timeout 10s exceeded" in str(e), "Query should have stopped due to timeout, check error message!")
            try:
                # Should timeout after 2 minutes
                results = self.run_cbq_query(query="EXECUTE FUNCTION func1(2,1)", query_params={'timeout':"130s"})
                self.fail("Query should have timed out")
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue("stopped after running beyond 120000 ms" in str(e), "Query should have stopped due to timeout, check error message!")

            try:
                # Should timeout after 2 minutes
                results = self.run_cbq_query(query="EXECUTE FUNCTION func1(2,1)")
                self.fail("Query should have timed out")
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue("stopped after running beyond 120000 ms" in str(e), "Query should have stopped due to timeout, check error message!")

        finally:
            try:
                self.delete_library("strings")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_function_syntax_scope(self):
        try:
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
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
        try:
            functions = 'function adder(a, b) { retur a + b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.assertFalse(created, "Library should have failed to create due to a syntax error during compilation, check logs above!")
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_javascript_replace_lib_func(self):
        try:
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'javascript')
            self.assertEqual(results['results'][0]['functions']['definition']['library'], 'math')
            self.assertEqual(results['results'][0]['functions']['definition']['object'], 'adder')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")
            self.assertEqual(results['results'], [4])

            functions = 'function adder(a,b) { return helper(a,b); } function helper(a,b) { return a - b; }'
            function_names = ["adder", "helper"]
            created = self.create_library("math", functions, function_names)

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
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query("select * from system:functions where identity.name = 'func1'")
            self.assertEqual(results['results'][0]['functions']['definition']['#language'], 'javascript')
            self.assertEqual(results['results'][0]['functions']['definition']['library'], 'math')
            self.assertEqual(results['results'][0]['functions']['definition']['object'], 'adder')
            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")
            self.assertEqual(results['results'], [4])

            functions = 'function multiplier(a, b) { return a * b; }'
            function_names = ["multiplier"]
            created = self.create_library("math", functions, function_names)

            results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")

        except Exception as e:
            self.log.error(str(e))
            self.assertTrue('symbol is not a function' in str(e), "The query failed for the wrong reason {0}".format(str(e)))

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
                self.assertTrue('Library or function missing' in str(e),
                                "The query failed for the wrong reason {0}".format(str(e)))

            try:
                functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
                function_names = ["adder", "multiplier"]
                function_names = ["adder", "multiplier"]
                created = self.create_library("math", functions, function_names)
                results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")
                self.assertEqual(results['results'], [4])
                self.run_cbq_query(query='CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "sub" AT "math"')
                results = self.run_cbq_query(query="EXECUTE FUNCTION func1(1,3)")
            except Exception as e:
                self.log.error(str(e))
                self.assertTrue('symbol is not a function' in str(e),
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
    '''The stated udf limit is 500, make sure we can create and use 500 functions'''
    def test_udf_limits(self):
        try:
            function_name = ''
            for i in range(0, 500):
                function_name = "func" + str(i)
                self.run_cbq_query(query='CREATE OR REPLACE FUNCTION {0}(...) {{ (args[0] * 9/5) + 32 }}'.format(function_name))

            result = self.run_cbq_query(query="select * from system:functions")
            self.assertEqual(result['metrics']['resultCount'], 500)

            for i in range(0, 500):
                function_name = "func" + str(i)
                results = self.run_cbq_query(query='EXECUTE FUNCTION {0}(10)'.format(function_name))
                self.assertEqual(results['results'], [50])

            if self.rebalance_in:
                rebalance = self.cluster.async_rebalance(self.servers[:self.nodes_init],
                                                         self.servers[self.nodes_init: (self.nodes_init + 1)], [], services=['n1ql'])
                rebalance.result()

                for i in range(0,500):
                    function_name = "func" + str(i)
                    results = self.run_cbq_query(query='EXECUTE FUNCTION {0}(10)'.format(function_name), server=self.servers[self.nodes_init])
                    self.assertEqual(results['results'], [50])
        finally:
            for i in range(0,500):
                function_name = "func" + str(i)
                try:
                    self.run_cbq_query(query='DROP FUNCTION {0}'.format(function_name))
                except Exception as e:
                    self.log.error(str(e))

    def test_create_or_replace_js_to_inline(self):
        try:
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
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
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
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

            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
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

    def test_udf_insert(self):
        try:
            self.run_cbq_query(
                "CREATE OR REPLACE FUNCTION func1(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(
                    self.scope, self.collections[0]))
            results = self.run_cbq_query(query='INSERT INTO default (KEY, VALUE) VALUES ("key5", {"field1":func1("old hotel")}) RETURNING *')
            self.assertEqual(results['results'], [{'default': {'field1': [{'test1': {'name': 'old hotel', 'type': 'hotel'}}, {'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}}, {'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}]}}])
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_udf_let(self):
        try:
            if self.analytics:
                self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION func1(degrees) {(degrees - 32)} ")
                results = self.run_cbq_query(
                    "SELECT * FROM collection1 LET maximum_no = func1(36) WHERE ANY v in collection1.numbers SATISFIES v = maximum_no END")
                self.assertEqual(results['results'],
                                 [{'maximum_no': 4, 'collection1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
            else:
                self.run_cbq_query("CREATE FUNCTION func1(degrees) LANGUAGE INLINE AS (degrees - 32) ")
                results = self.run_cbq_query(
                    "SELECT * FROM default:default.test.test1 LET maximum_no = func1(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END")
                self.assertEqual(results['results'], [{'maximum_no': 4, 'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])

                functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
                function_names = ["adder", "multiplier"]
                created = self.create_library("math", functions, function_names)
                self.run_cbq_query(
                    query='CREATE OR REPLACE FUNCTION func2(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
                results = self.run_cbq_query(
                    "SELECT * FROM default:default.test.test1 LET maxi=func2(1,3) WHERE ANY v in test1.numbers SATISFIES v = maxi END")
                self.assertEqual(results['results'], [{'maxi': 4, 'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])

        finally:
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION func1(degrees)")
                else:
                    self.delete_library("math")
                    self.run_cbq_query("DROP FUNCTION func1")
                    self.run_cbq_query("DROP FUNCTION func2")
            except Exception as e:
                self.log.error(str(e))

    def test_udf_groupby(self):
        if not self.analytics:
            functions = 'function comparator(a, b) {if (a > b) { return "old hotel"; } else { return "new hotel" }}'
            function_names = ["comparator"]
            created = self.create_library("math", functions, function_names)
        try:
            if self.analytics:
                self.run_cbq_query("CREATE ANALYTICS FUNCTION func2(degrees) {(degrees - 32)}")
                results = self.run_cbq_query(
                    "SELECT COUNT(name), hotel_name FROM collection1 LET maximum_no = func2(36) WHERE ANY v in collection1.numbers SATISFIES v = maximum_no END GROUP BY name AS hotel_name")
                self.assertEqual(results['results'], [{'$1': 1, 'hotel_name': 'old hotel'}])
            else:
                self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "comparator" AT "math"')
                self.run_cbq_query("CREATE FUNCTION func2(degrees) LANGUAGE INLINE AS (degrees - 32) ")
                results = self.run_cbq_query(
                    "SELECT COUNT(name), hotel_name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END GROUP BY func1(1,2) AS hotel_name")
                self.assertEqual(results['results'], [{'$1': 1, 'hotel_name': 'new hotel'}])
                results2 = self.run_cbq_query(
                    "SELECT COUNT(name), hotel_name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END GROUP BY func1(2,1) AS hotel_name")
                self.assertEqual(results2['results'], [{'$1': 1, 'hotel_name': 'old hotel'}])
        finally:
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION func2(degrees)")
                else:
                    self.delete_library("math")
                    self.run_cbq_query("DROP FUNCTION func1")
                    self.run_cbq_query("DROP FUNCTION func2")

            except Exception as e:
                self.log.error(str(e))

    def test_udf_having(self):
        if not self.analytics:
            functions = 'function comparator(a, b) {if (a > b) { return "old hotel"; } else { return "new hotel" }}'
            function_names = ["comparator"]
            created = self.create_library("math", functions, function_names)
        try:
            if self.analytics:
                self.run_cbq_query("CREATE OR REPLACE ANALYTICS FUNCTION func2(degrees) {(degrees - 32)} ")
                results = self.run_cbq_query(
                    "SELECT name FROM collection1 LET maximum_no = func2(36) WHERE ANY v in collection1.numbers SATISFIES v = maximum_no END GROUP BY name HAVING name = 'old hotel'")
                self.assertEqual(results['results'], [{'name': 'old hotel'}])
            else:
                self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "comparator" AT "math"')
                self.run_cbq_query("CREATE FUNCTION func2(degrees) LANGUAGE INLINE AS (degrees - 32) ")
                results = self.run_cbq_query(
                    "SELECT name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END GROUP BY name HAVING name = func1(1,2)")
                self.assertEqual(results['results'], [])
                results2 = self.run_cbq_query(
                    "SELECT name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END GROUP BY name HAVING name = func1(2,1)")
                self.assertEqual(results2['results'], [{'name': 'old hotel'}])
        finally:
            try:
                if self.analytics:
                    self.run_cbq_query("DROP ANALYTICS FUNCTION func2(degrees)")
                else:
                    self.delete_library("math")
                    self.run_cbq_query("DROP FUNCTION func1")
                    self.run_cbq_query("DROP FUNCTION func2")
            except Exception as e:
                self.log.error(str(e))

    def test_udf_letting(self):
        string_functions = 'function concater(a,b) { var text = ""; var x; for (x in a) {if (x = b) { return x; }} return "n"; } function comparator(a, b) {if (a > b) { return "old hotel"; } else { return "new hotel" }}'
        function_names2 = ["concater","comparator"]
        created2 = self.create_library("strings",string_functions,function_names2)
        try:
            self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "comparator" AT "strings"')
            self.run_cbq_query("CREATE FUNCTION func2(degrees) LANGUAGE INLINE AS (degrees - 32) ")
            self.run_cbq_query(query='CREATE FUNCTION func3(a,b) LANGUAGE JAVASCRIPT AS "concater" AT "strings"')

            results = self.run_cbq_query(
                "SELECT name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END GROUP BY name LETTING letter = func3('random string', 'x') HAVING name > letter")
            self.assertEqual(results['results'], [])
            results2 = self.run_cbq_query(
                "SELECT name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END GROUP BY name LETTING letter = func3('old hotel','o') HAVING name > letter")
            self.assertEqual(results2['results'], [{'name': 'old hotel'}])
        finally:
            try:
                self.delete_library("strings")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")
                self.run_cbq_query("DROP FUNCTION func3")
            except Exception as e:
                self.log.error(str(e))

    def test_udf_orderby(self):
        string_functions = 'function concater(a,b) { var text = ""; var x; for (x in a) {if (x = b) { return x; }} return "n"; } function comparator(a, b) {if (a > b) { return "old hotel"; } else { return "new hotel" }} '
        function_names2 = ["concater","comparator"]
        created2 = self.create_library("strings",string_functions,function_names2)
        try:
            self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "comparator" AT "strings"')
            self.run_cbq_query("CREATE FUNCTION func2(degrees) LANGUAGE INLINE AS (degrees - 32) ")
            self.run_cbq_query(query='CREATE FUNCTION func3(a,b) LANGUAGE JAVASCRIPT AS "concater" AT "strings"')

            results = self.run_cbq_query(
                "SELECT name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END ORDER BY func1(2,1)")
            self.assertEqual(results['results'], [{'name': 'old hotel'}])
            results2 = self.run_cbq_query(
                "SELECT name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END ORDER BY func1(1,2) ")
            self.assertEqual(results2['results'], [{'name': 'old hotel'}])
        finally:
            try:
                self.delete_library("strings")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")
                self.run_cbq_query("DROP FUNCTION func3")
            except Exception as e:
                self.log.error(str(e))

    def test_advise_udf(self):
        string_functions = 'function concater(a,b) { var text = ""; var x; for (x in a) {if (x = b) { return x; }} return "n"; } function comparator(a, b) {if (a > b) { return "old hotel"; } else { return "new hotel" }}'
        function_names2 = ["concater","comparator"]
        created2 = self.create_library("strings",string_functions,function_names2)
        try:
            self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "comparator" AT "strings"')
            self.run_cbq_query("CREATE FUNCTION func2(degrees) LANGUAGE INLINE AS (degrees - 32) ")
            self.run_cbq_query(query='CREATE FUNCTION func3(a,b) LANGUAGE JAVASCRIPT AS "concater" AT "strings"')
            self.run_cbq_query("CREATE OR REPLACE FUNCTION func4(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(self.scope,self.collections[0]))

            results2 = self.run_cbq_query(
                "ADVISE SELECT name FROM default:default.test.test1 LET maximum_no = func2(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END GROUP BY name LETTING letter = func3('old hotel','o') HAVING name > letter")
            self.assertTrue('CREATE INDEX adv_DISTINCT_numbers ON `default`:`default`.`test`.`test1`(DISTINCT ARRAY `v` FOR `v` IN `numbers` END)' in str(results2['results']), "Wrong index was advised, check advise output {0}".format(results2))
        finally:
            try:
                self.delete_library("strings")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")
                self.run_cbq_query("DROP FUNCTION func3")
                self.run_cbq_query("DROP FUNCTION func4")
            except Exception as e:
                self.log.error(str(e))

    def test_udf_prepareds(self):
        try:
            self.run_cbq_query(
                "CREATE OR REPLACE FUNCTION func1(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(
                    self.scope, self.collections[0]))
            results = self.run_cbq_query("PREPARE p1 as EXECUTE FUNCTION func1('old hotel')")
            results = self.run_cbq_query("EXECUTE p1")
            self.assertEqual(results['results'], [[{'test1': {'name': 'old hotel', 'type': 'hotel'}},
                                                   {'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}},
                                                   {'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}]])
            results = self.run_cbq_query("PREPARE p2 as select func1('old hotel')")
            results = self.run_cbq_query("EXECUTE p2")
            self.assertEqual(results['results'], [{'$1': [{'test1': {'name': 'old hotel', 'type': 'hotel'}}, {
                'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}}, {'test1': {'name': 'old hotel',
                                                                                          'numbers': [1, 2, 3,
                                                                                                      4]}}]}])
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_udf_prepareds_update(self):
        try:
            self.run_cbq_query(
                "CREATE OR REPLACE FUNCTION func1(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(
                    self.scope, self.collections[0]))
            results = self.run_cbq_query("PREPARE p1 as EXECUTE FUNCTION func1('old hotel')")
            results = self.run_cbq_query("EXECUTE p1")
            self.assertEqual(results['results'], [[{'test1': {'name': 'old hotel', 'type': 'hotel'}},
                                                   {'test1': {'name': 'old hotel', 'nested': {'fields': 'fake'}}},
                                                   {'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}]])
            self.run_cbq_query("CREATE OR REPLACE FUNCTION func1(...) {args[0]}")
            results = self.run_cbq_query("EXECUTE p1")
            self.assertEqual(results['results'], ['old hotel'])
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_udf_prepareds_update_js_inline(self):
        try:
            self.run_cbq_query("CREATE FUNCTION func1(degrees) LANGUAGE INLINE AS (degrees - 32) ")
            results = self.run_cbq_query(
                "PREPARE p1 as SELECT * FROM default:default.test.test1 LET maximum_no = func1(36) WHERE ANY v in test1.numbers SATISFIES v = maximum_no END")
            results = self.run_cbq_query("EXECUTE p1")
            self.assertEqual(results['results'], [{'maximum_no': 4, 'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])

            functions = 'function adder(a) { return (a - 32); } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.run_cbq_query(
                query='CREATE OR REPLACE FUNCTION func1(a) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            results = self.run_cbq_query("EXECUTE p1")
            self.assertEqual(results['results'], [{'maximum_no': 4, 'test1': {'name': 'old hotel', 'numbers': [1, 2, 3, 4]}}])
        except Exception as e:
            self.log.error(str(e))
            self.fail()
        finally:
            try:
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_udf_prepared_drop(self):
        try:
            self.run_cbq_query(
                "CREATE OR REPLACE FUNCTION func1(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(
                    self.scope, self.collections[0]))
            results = self.run_cbq_query("PREPARE p1 as EXECUTE FUNCTION func1('old hotel')")
            self.run_cbq_query("DROP FUNCTION func1")
            results = self.run_cbq_query("EXECUTE p1")
        except Exception as e:
            self.log.error(str(e))
            self.assertTrue("Function 'func1' not found" in str(e))
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
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.assertTrue(created, "The library was not created! Check run logs for more details")
        finally:
            self.delete_library("math")

    def test_update_library(self):
        try:
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            self.assertTrue(created, "The library was not created! Check run logs for more details")
            if self.replace:
                functions = 'function sub(a, b) { return a - b; } function divider(a, b) { return a / b; }'
                function_names = ["sub", "divider"]
            else:
                functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; } function sub(a, b) { return a - b; } function divider(a, b) { return a / b; }'
                function_names = ["sub", "divider", "adder", "multiplier"]
            created = self.create_library("math", functions, function_names, self.replace)
            self.assertTrue(created, "The library was not updated! Check run logs for more details")
        finally:
            self.delete_library("math")


    def test_delete_library(self):
        functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
        function_names = ["adder", "multiplier"]
        self.create_library("math", functions, function_names)
        deleted = self.delete_library("math")
        self.assertTrue(deleted, "The library was not deleted! Check run logs for more details")

    def test_add_function(self):
        try:
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            self.create_library("math", functions, function_names)
            functions ='function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; } function sub(a,b) { return helper(a,b); } function helper(a,b) { return a - b; }'
            function_names = ["adder", "multiplier","sub","helper"]
            created = self.create_library("math", functions, function_names)
            self.assertTrue(created, "The new library was not created! Check run logs for more details")
        finally:
            self.delete_library("math")

    def test_delete_function(self):
        try:
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            self.create_library("math", functions, function_names)
            functions = 'function multiplier(a, b) { return a * b; }'
            function_names = ["multiplier"]
            created = self.create_library("math", functions, function_names)
            self.assertTrue(created, "The new library was not created! Check run logs for more details")
        finally:
            self.delete_library("math")

    '''Create a library with functions, check to see that the library was created and the functions were created'''
    def create_library(self, library_name='', functions={},function_names=[], replace= False):
        created = False
        url = "http://{0}:{1}/evaluator/v1/libraries/{2}".format(self.master.ip, self.n1ql_port, library_name)
        data = '{0}'.format(functions)
        results = self.shell.execute_command("{0} -X POST {1} -u Administrator:password -H 'content-type: application/json' -d '{2}'".format(self.curl_path, url, data))
        self.log.info(results[0])
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
    def delete_library(self, library_name=''):
        deleted = False
        url = "http://{0}:{1}/evaluator/v1/libraries/{2}".format(self.master.ip, self.n1ql_port, library_name)
        curl_output = self.shell.execute_command("{0} -X DELETE {1} -u Administrator:password ".format(self.curl_path, url))
        self.log.info(curl_output[0])
        self.sleep(1)
        libraries = self.shell.execute_command("{0} {1} -u Administrator:password".format(self.curl_path, url))
        if f"Library [{library_name}] not found" in str(libraries):
            deleted = True
        return deleted

    '''Add a function to an existing library, it is assumed the library already exists'''
    def add_function(self, library_name ='', function_name ='', function =''):
        added = False
        url = "http://{0}:{1}/evaluator/v1/libraries/{2}/functions/{3}".format(self.master.ip, self.n1ql_port, library_name, function_name)
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
        url = "http://{0}:{1}/evaluator/v1/libraries/{2}/functions/{3}".format(self.master.ip, self.n1ql_port, library_name,function_name)
        curl_output = self.shell.execute_command("{0} -X DELETE {1} -u Administrator:password ".format(self.curl_path, url))
        libraries = self.shell.execute_command("{0} {1} -u Administrator:password".format(self.curl_path, url))
        if library_name not in str(libraries):
            deleted = True
        return deleted

    ''' Test rbac when you have creation/execution on a function that attemps to access a bucket you dont have perms for'''
    def test_inline_rbac_query(self):
        try:
            self.create_users()
            self.grant_role()
            res = self.curl_with_roles("CREATE OR REPLACE FUNCTION func1(nameval) {{ (select * from default:default.{0}.{1} where name = nameval) }}".format(self.scope, self.collections[0]))
            self.assertEqual(res['status'], 'success')

            res = self.curl_with_roles('EXECUTE FUNCTION func1("old hotel")')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('select raw func1("old hotel")')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_inline_rbac(self):
        try:
            self.create_users()
            self.grant_role()
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            res = self.curl_with_roles('CREATE OR REPLACE FUNCTION default:default.test.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            if self.scoped:
                self.assertEqual(res['status'], 'success')
            else:
                self.assertTrue('User does not have credentials' in res['errors'][0]['msg'], "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE OR REPLACE FUNCTION default:default.test2.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in res['errors'][0]['msg'], "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE OR REPLACE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS \"adder\" AT \"math\"')
            self.assertTrue('User does not have credentials' in res['errors'][0]['msg'], "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE OR REPLACE FUNCTION default:celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            if self.scoped:
                self.assertTrue('User does not have credentials' in res['errors'][0]['msg'], "Error message is wrong: {0}".format(str(res)))
            else:
                self.assertEqual(res['status'], 'success')
            res = self.curl_with_roles('CREATE OR REPLACE FUNCTION celsius1(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            if self.scoped:
                self.assertTrue('User does not have credentials' in res['errors'][0]['msg'], "Error message is wrong: {0}".format(str(res)))
            else:
                self.assertEqual(res['status'], 'success')
            if self.scoped:
                res = self.curl_with_roles('SELECT RAW default:default.test.celsius(10)')
                self.assertEqual(res['results'], [-12.222222222222221])
                res = self.curl_with_roles('EXECUTE FUNCTION default:default.test.celsius(10)')
                self.assertEqual(res['results'], [-12.222222222222221])
            else:
                res = self.curl_with_roles('SELECT RAW celsius(10)')
                self.assertEqual(res['results'], [-12.222222222222221])
                res = self.curl_with_roles('SELECT RAW celsius1(10)')
                self.assertEqual(res['results'], [-12.222222222222221])
                res = self.curl_with_roles('EXECUTE FUNCTION celsius(10)')
                self.assertEqual(res['results'], [-12.222222222222221])
                res = self.curl_with_roles('EXECUTE FUNCTION celsius1(10)')
                self.assertEqual(res['results'], [-12.222222222222221])
        finally:
            try:
                self.delete_library("math")
                if self.scoped:
                    self.run_cbq_query("DROP FUNCTION default:default.test.celsius")
                else:
                    self.run_cbq_query("DROP FUNCTION celsius")
                    self.run_cbq_query("DROP FUNCTION celsius1")
            except Exception as e:
                self.log.error(str(e))

    def test_inline_rbac_creation(self):
        try:
            self.create_users()
            self.grant_role()
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            res = self.curl_with_roles('CREATE FUNCTION default:default.test.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            if self.scoped:
                self.assertEqual(res['status'], 'success')
            else:
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:default.test2.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS \"adder\" AT \"math\"')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            if self.scoped:
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            else:
                self.assertEqual(res['status'], 'success')
            res = self.curl_with_roles('CREATE FUNCTION celsius1(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            if self.scoped:
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            else:
                self.assertEqual(res['status'], 'success')

            if self.scoped:
                res = self.curl_with_roles('SELECT default:default.test.celsius(10)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
                res = self.curl_with_roles('EXECUTE FUNCTION default:default.test.celsius(10)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            else:
                res = self.curl_with_roles('SELECT RAW celsius(10)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
                res = self.curl_with_roles('SELECT RAW celsius1(10)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
                res = self.curl_with_roles('EXECUTE FUNCTION celsius(10)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
                res = self.curl_with_roles('EXECUTE FUNCTION celsius1(10)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
        finally:
            try:
                self.delete_library("math")
                if self.scoped:
                    self.run_cbq_query("DROP FUNCTION default:default.test.celsius")
                else:
                    self.run_cbq_query("DROP FUNCTION celsius")
                    self.run_cbq_query("DROP FUNCTION celsius1")
            except Exception as e:
                self.log.error(str(e))

    def test_inline_rbac_execution(self):
        try:
            self.create_users()
            self.grant_role()
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            res = self.curl_with_roles('CREATE FUNCTION default:default.test.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:default.test2.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS \"adder\" AT \"math\"')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION celsius1(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))

            self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            self.run_cbq_query(query='CREATE FUNCTION default:celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.run_cbq_query(query='CREATE FUNCTION celsius1(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.run_cbq_query(query='CREATE FUNCTION default:default.test.celsius1(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')


            res = self.curl_with_roles('SELECT RAW celsius(10)')
            if self.scoped:
                self.assertTrue('User does not have credentials' in str(res),
                                "Error message is wrong: {0}".format(str(res)))
            else:
                self.assertEqual(res['results'], [-12.222222222222221])
            res = self.curl_with_roles('SELECT RAW celsius1(10)')
            if self.scoped:
                self.assertTrue('User does not have credentials' in str(res),
                                "Error message is wrong: {0}".format(str(res)))
            else:
                self.assertEqual(res['results'], [-12.222222222222221])
            res = self.curl_with_roles('EXECUTE FUNCTION celsius(10)')
            if self.scoped:
                self.assertTrue('User does not have credentials' in str(res),
                                "Error message is wrong: {0}".format(str(res)))
            else:
                self.assertEqual(res['results'], [-12.222222222222221])
            res = self.curl_with_roles('EXECUTE FUNCTION celsius1(10)')
            if self.scoped:
                self.assertTrue('User does not have credentials' in str(res),
                                "Error message is wrong: {0}".format(str(res)))
            else:
                self.assertEqual(res['results'], [-12.222222222222221])
            res = self.curl_with_roles('SELECT RAW func1(10,20)')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('EXECUTE FUNCTION func1(10)')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('SELECT RAW default:default.test.celsius1(10)')
            if self.scoped:
                self.assertEqual(res['results'], [-12.222222222222221])
            else:
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('EXECUTE FUNCTION default:default.test.celsius1(10)')
            if self.scoped:
                self.assertEqual(res['results'], [-12.222222222222221])
            else:
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION celsius")
                self.run_cbq_query("DROP FUNCTION celsius1")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION default:default.test.celsius1")

            except Exception as e:
                self.log.error(str(e))

    def test_js_rbac(self):
        try:
            self.create_users()
            self.grant_role()
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            res = self.curl_with_roles('CREATE FUNCTION default:default.test.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in res['errors'][0]['msg'], "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:default.test2.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in res['errors'][0]['msg'], "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in res['errors'][0]['msg'], "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION celsius1(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in res['errors'][0]['msg'], "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:default.test.func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            if self.scoped:
                self.assertEqual(res['status'], 'success')
            else:
                self.assertTrue('User does not have credentials' in res['errors'][0]['msg'], "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:default.test2.func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            self.assertTrue('User does not have credentials' in res['errors'][0]['msg'], "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            if self.scoped:
                self.assertTrue('User does not have credentials' in res['errors'][0]['msg'],
                                "Error message is wrong: {0}".format(str(res)))
            else:
                self.assertEqual(res['status'], 'success')

            if self.scoped:
                res = self.curl_with_roles('SELECT RAW default:default.test.func1(10,20)')
                self.assertEqual(res['results'], [30])
                res = self.curl_with_roles('EXECUTE FUNCTION default:default.test.func1(10,20)')
                self.assertEqual(res['results'], [30])
            else:
                res = self.curl_with_roles('SELECT RAW func1(10,20)')
                self.assertEqual(res['results'], [30])
                res = self.curl_with_roles('EXECUTE FUNCTION func1(10,20)')
                self.assertEqual(res['results'], [30])
        finally:
            try:
                self.delete_library("math")
                if self.scoped:
                    self.run_cbq_query("DROP FUNCTION default:default.test.func1")
                else:
                    self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_js_rbac_creation(self):
        try:
            self.create_users()
            self.grant_role()
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            res = self.curl_with_roles('CREATE FUNCTION default:default.test.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:default.test2.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION celsius1(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:default.test.func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            if self.scoped:
                self.assertEqual(res['status'], 'success')
            else:
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:default.test2.func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            if self.scoped:
                self.assertTrue('User does not have credentials' in str(res),
                                "Error message is wrong: {0}".format(str(res)))
            else:
                self.assertEqual(res['status'], 'success')

            if self.scoped:
                res = self.curl_with_roles('SELECT RAW default:default.test.func1(10,20)')
                self.assertTrue('User does not have credentials' in str(res),
                                "Error message is wrong: {0}".format(str(res)))
                res = self.curl_with_roles('EXECUTE FUNCTION default:default.test.func1(10,20)')
                self.assertTrue('User does not have credentials' in str(res),
                                "Error message is wrong: {0}".format(str(res)))
            else:
                res = self.curl_with_roles('SELECT RAW func1(10,20)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
                res = self.curl_with_roles('EXECUTE FUNCTION func1(10,20)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
        finally:
            try:
                self.delete_library("math")
                if self.scoped:
                    self.run_cbq_query("DROP FUNCTION default:default.test.func1")
                else:
                    self.run_cbq_query("DROP FUNCTION func1")
            except Exception as e:
                self.log.error(str(e))

    def test_js_rbac_execution(self):
        try:
            self.create_users()
            self.grant_role()
            functions = 'function adder(a, b) { return a + b; } function multiplier(a, b) { return a * b; }'
            function_names = ["adder", "multiplier"]
            created = self.create_library("math", functions, function_names)
            res = self.curl_with_roles('CREATE FUNCTION default:default.test.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:default.test2.celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION default:celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
            res = self.curl_with_roles('CREATE FUNCTION celsius1(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))

            self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')
            self.run_cbq_query(query='CREATE FUNCTION default:celsius(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.run_cbq_query(query='CREATE FUNCTION celsius1(degrees) LANGUAGE INLINE AS (degrees - 32) * 5/9')
            self.run_cbq_query(query='CREATE FUNCTION default:default.test.func1(a,b) LANGUAGE JAVASCRIPT AS "adder" AT "math"')

            if self.scoped:
                res = self.curl_with_roles('SELECT RAW default:default.test.func1(10,20)')
                self.assertEqual(res['results'], [30])
                res = self.curl_with_roles('EXECUTE FUNCTION default:default.test.func1(10,20)')
                self.assertEqual(res['results'], [30])
            else:
                res = self.curl_with_roles('SELECT RAW celsius(10)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
                res = self.curl_with_roles('SELECT RAW celsius1(10)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
                res = self.curl_with_roles('EXECUTE FUNCTION celsius(10)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
                res = self.curl_with_roles('EXECUTE FUNCTION celsius1(10)')
                self.assertTrue('User does not have credentials' in str(res), "Error message is wrong: {0}".format(str(res)))
                res = self.curl_with_roles('SELECT RAW func1(10,20)')
                self.assertEqual(res['results'], [30])
                res = self.curl_with_roles('EXECUTE FUNCTION func1(10,20)')
                self.assertEqual(res['results'], [30])
        finally:
            try:
                self.delete_library("math")
                self.run_cbq_query("DROP FUNCTION celsius")
                self.run_cbq_query("DROP FUNCTION celsius1")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION default:default.test.func1")

            except Exception as e:
                self.log.error(str(e))

    def test_library_name_validation(self):
        invalid_names = [
            "_libname", "lib$name", "lib@name", "lib!name", "lib^name", "lib*name", "lib(name", "lib)name",
            "lib+name", "lib=name", "lib\[name", "lib\]name", "lib:name", "lib,name", "lib;name", "lib<name", "lib>name",
            "_libname", "-libname", "lib\?name", "lib\\name", "lib\:name", "lib\#name"
        ]
        for name in invalid_names:
            with self.subTest(f"Invalid Library name: {name}"):
                url = f"http://{self.master.ip}:{self.n1ql_port}/evaluator/v1/libraries/{name}"
                data = 'function add(a,b) { return a+b;}'
                results = self.shell.execute_command(f"{self.curl_path} -X POST '{url}' -u Administrator:password -H 'content-type: application/json' -d '{data}'")
                self.assertEqual(results[0], ['Library name can start with characters only in range A-Z, a-z, 0-9 and can contain characters only in range A-Z, a-z, 0-9, underscore and hyphen '])

        valid_names = [
            "libname", "LIBNAME", "LibName", "lib0name", "0libname", "libname0",
            "lib_name", "libname_", "lib-name", "libname-", "999"
        ]
        for name in valid_names:
            with self.subTest(f"Valid Library name: {name}"):
                url = f"http://{self.master.ip}:{self.n1ql_port}/evaluator/v1/libraries/{name}"
                data = 'function add(a,b) { return a+b;}'
                results = self.shell.execute_command(f"{self.curl_path} -X POST '{url}' -u Administrator:password -H 'content-type: application/json' -d '{data}'")
                self.assertEqual(results[0], ['{"status": "OK"}'])
                results = self.shell.execute_command(f"{self.curl_path} -X DELETE '{url}' -u Administrator:password")
                self.assertEqual(results[0], ['{"status": "OK"}'])

    def test_MB59183(self):
        upsert = 'UPSERT INTO default (KEY k, VALUE v) SELECT "k00"||TO_STR(d) AS k, {"c1":d, "c2":d, "c3":d} AS v FROM ARRAY_RANGE(1,10) AS d'
        index = 'CREATE INDEX ix1 ON default(c1,c2, c3)'
        self.run_cbq_query(upsert)
        self.run_cbq_query(index)

        udf1 = 'CREATE OR REPLACE FUNCTION f11(data) {(SELECT l, r FROM default AS l JOIN (SELECT default.* FROM default WHERE c1 > 0) AS r USE NL ON l.c3=r.c3 WHERE l.c1 > 0 AND r.c2 = data)}'
        self.run_cbq_query(udf1)

        expected_result = [
            {"$1": [{"l": {"c1": 1,"c2": 1,"c3": 1}, "r": {"c1": 1,"c2": 1,"c3": 1}}]},
            {"$1": [{"l": {"c1": 2,"c2": 2,"c3": 2}, "r": {"c1": 2,"c2": 2,"c3": 2}}]},
            {"$1": [{"l": {"c1": 3,"c2": 3,"c3": 3}, "r": {"c1": 3,"c2": 3,"c3": 3}}]},
            {"$1": [{"l": {"c1": 4,"c2": 4,"c3": 4}, "r": {"c1": 4,"c2": 4,"c3": 4}}]},
            {"$1": [{"l": {"c1": 5,"c2": 5,"c3": 5}, "r": {"c1": 5,"c2": 5,"c3": 5}}]},
            {"$1": [{"l": {"c1": 6,"c2": 6,"c3": 6}, "r": {"c1": 6,"c2": 6,"c3": 6}}]},
            {"$1": [{"l": {"c1": 7,"c2": 7,"c3": 7}, "r": {"c1": 7,"c2": 7,"c3": 7}}]},
            {"$1": [{"l": {"c1": 8,"c2": 8,"c3": 8}, "r": {"c1": 8,"c2": 8,"c3": 8}}]},
            {"$1": [{"l": {"c1": 9,"c2": 9,"c3": 9}, "r": {"c1": 9,"c2": 9,"c3": 9}}]}
        ]
        result = self.run_cbq_query('SELECT f11(t.c1) FROM default AS t WHERE t.c1 > 0')

        udf2 = 'CREATE OR REPLACE FUNCTION f12(data) {(SELECT l, r FROM default AS l JOIN data AS r USE NL ON l.c3=r.c3 WHERE l.c1 > 0)}'
        self.run_cbq_query(udf2)

        result = self.run_cbq_query('SELECT f12(t) FROM default AS t WHERE t.c1 > 0')
        self.assertEqual(expected_result, result['results'])

    def test_MB59078(self):
        udf = 'CREATE OR REPLACE FUNCTION f11() {(SELECT l, r FROM default AS l JOIN default AS r USE HASH (BUILD) ON l.c3=r.c3 WHERE l.c1 > 0 AND r.c1 > 0 AND r.c2 = 1)}'
        self.run_cbq_query(udf)

        expected_result = [{
            'expression': '(select `l`, `r` from `default`:`default` as `l` join `default`:`default` as `r` use hash (build) on ((`l`.`c3`) = (`r`.`c3`)) where (((0 < (`l`.`c1`)) and (0 < (`r`.`c1`))) and ((`r`.`c2`) = 1)))',
            'text': '(SELECT l, r FROM default AS l JOIN default AS r USE HASH (BUILD) ON l.c3=r.c3 WHERE l.c1 > 0 AND r.c1 > 0 AND r.c2 = 1)'    
        }]
        result = self.run_cbq_query('SELECT `definition`.`expression`, `definition`.`text` FROM system:functions WHERE identity.name="f11"')
        self.assertEqual(expected_result, result['results'])

    def test_letting_no_groupby(self):
        udf = 'create or replace function mb() {(select c from [1,2,2,3,3,3,4,4,4,4,5,5,5,5,5] a letting c = count(1))}'
        self.run_cbq_query(udf)
        result = self.run_cbq_query('execute function mb()')
        self.assertEqual(result['results'], [[{"c": 15}]])