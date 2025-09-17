import time
from deepdiff import DeepDiff
import uuid
from .tuq import QueryTests
from collection.collections_stats import CollectionsStats
from couchbase_helper.cluster import Cluster
from lib.couchbase_helper.documentgenerator import SDKDataLoader
from membase.api.rest_client import RestConnection



class QueryMagmaTests(QueryTests):
    def setUp(self):
        super(QueryMagmaTests, self).setUp()
        self.log.info("==============  QueryMagmaTests setup has started ==============")
        self.bucket_name = self.input.param("bucket", self.default_bucket_name)
        self.active_resident_threshold = self.input.param("resident_ratio", 100)
        self.num_items = self.input.param("num_items", 10000)
        self.expiry = self.input.param("expiry", 0)
        self.rollback = self.input.param("rollback", False)
        self.conn = RestConnection(self.master)
        self.stat = CollectionsStats(self.master)
        self.cbqpath = '{0}cbq -quiet -u {1} -p {2} -e=localhost:8093 '.format(self.path, self.username, self.password)
        self.cluster = Cluster()
        self.log.info("==============  QueryMagmaTests setup has completed ==============")

    def suite_setUp(self):
        super(QueryMagmaTests, self).suite_setUp()
        self.log.info("==============  QueryMagmaTests suite_setup has started ==============")
        self.log.info("==============  QueryMagmaTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryMagmaTests tearDown has started ==============")
        self.log.info("==============  QueryMagmaTests tearDown has completed ==============")
        super(QueryMagmaTests, self).tearDown()


    def suite_tearDown(self):
        self.log.info("==============  QueryMagmaTests suite_tearDown has started ==============")
        self.log.info("==============  QueryMagmaTests suite_tearDown has completed ==============")
        super(QueryMagmaTests, self).suite_tearDown()

    ##############################################################################################
    #
    #   DML
    ##############################################################################################

    def test_insert_with_select(self):
        self.load_data()
        num_docs = self.input.param('num_docs', 10)
        keys, values = self._insert_gen_keys(num_docs, prefix="select_i")
        prefix = 'insert%s' % str(uuid.uuid4())[:5]
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            for i in range(num_docs):
                self.query = 'insert into %s (key "%s_%s", value {"name": name}) select name from %s use keys ["%s"]' % (
                query_bucket, prefix, str(i), query_bucket, keys[i])
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        for query_bucket, bucket in zip(self.query_buckets, self.buckets):
            self.query = 'select * from %s %s use keys [%s]' % (
            query_bucket, bucket.name, ','.join(['"%s_%s"' % (prefix, i) for i in range(num_docs)]))
            actual_result = self.run_cbq_query()
            expected_result = [{bucket.name: {'name': doc['name']}} for doc in values[:num_docs]]
            # print("--> expected_result:{}".format(expected_result))
            # print("--> actual_result:{}".format(actual_result))
            expected_result = sorted(expected_result, key=(lambda x: x[bucket.name]['name']))
            actual_result = sorted(actual_result['results'], key=(lambda x: x[bucket.name]['name']))
            self._delete_ids(actual_result)
            self._delete_ids(expected_result)
            self.assertEqual(actual_result, expected_result, 'Item did not appear')
        self.conn.delete_all_buckets()


    def test_insert_returning_elements(self):
        self.load_data()
        keys = ['%s%s' % (k, str(uuid.uuid4())[:5]) for k in range(10)]
        expected_item_values = [{'name': 'return_%s' % v} for v in range(10)]
        for query_bucket in self.query_buckets:
            values = ''
            for k, v in zip(keys, expected_item_values):
                inserted = v
                if isinstance(v, str):
                    inserted = '"%s"' % v
                if v is None:
                    inserted = 'null'
                values += '("%s", %s),' % (k, inserted)
            self.query = 'insert into %s (key , value) VALUES %s RETURNING name' % (query_bucket, values[:-1])
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            diffs = DeepDiff(actual_result['results'],  expected_item_values, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        self.conn.delete_all_buckets()

    def test_update_where(self):
        self.load_data()
        num_docs_update = self.input.param('docs_to_update', 3)
        num_docs = self.input.param('num_docs', 10)
        _, values = self._insert_gen_keys(num_docs, prefix='update_where' + str(uuid.uuid4())[:4])
        updated_value = 'new_name'
        for query_bucket in self.query_buckets:
            self.query = 'update %s set name="%s" where join_day=1 returning name' % (query_bucket, updated_value)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            self.query = 'select name from %s where join_day=1' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertFalse([doc for doc in actual_result['results'] if doc['name'] != updated_value],
                             'Names were not changed')
        self.conn.delete_all_buckets()

    def test_delete_where_clause_json(self):
        self.load_data()
        keys, values = self._insert_gen_keys(self.num_items, prefix='delete_where')
        keys_to_delete = [keys[i] for i in range(len(keys)) if values[i]["job_title"] == 'Engineer']
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'delete from %s where job_title="Engineer"' % query_bucket
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self._keys_are_deleted(keys_to_delete)
        self.conn.delete_all_buckets()

    def test_merge_update_match_set(self):
        self.load_data(num_extra_buckets=1)
        self.run_cbq_query("CREATE INDEX idx1 on default(name)")
        self.run_cbq_query("CREATE INDEX idx2 on bucket0(name)")
        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        key = "test"
        value = '{"name": "new1"}'
        self.query = 'INSERT into bucket0 (key , value) VALUES ("%s", %s)' % (key, value)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        value = '{"name": "new1"}'
        self.query = 'INSERT into default (key , value) VALUES ("%s", %s)' % ("new1", value)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        time.sleep(5)
        new_name = 'edit'
        self.query = 'MERGE INTO default b1 USING bucket0 b2 on b1.name = b2.name when matched then update set b1.name="%s"' % (new_name)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.query = 'SELECT * FROM default where name = "{0}"'.format(new_name)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['results'][0]['default']['name'], new_name, 'Name was not updated')
        self.conn.delete_all_buckets()

    def test_merge_delete_match(self):
        self.load_data(num_extra_buckets=1)
        self.assertTrue(len(self.buckets) >= 2, 'Test needs at least 2 buckets')
        keys, _ = self._insert_gen_keys(self.num_items, prefix='merge_delete')
        self.query = 'MERGE INTO default USING bucket0 sd on key meta().id when matched then delete where meta(sd).id = "%s"' % (keys[0])
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(actual_result['metrics']['mutationCount'], 1, 'Merge deleted more data than intended')
        self.query = 'select * from default where meta().id = "%s"' % (keys[0])
        self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        self.assertEqual(len(actual_result['results']), 0, 'Merge did not delete the item intended')
        self.conn.delete_all_buckets()

    ##############################################################################################
    #
    #   use keys
    ##############################################################################################

    def test_delete_use_keys(self):
        self.load_data()
        keys = "'doc_10000','doc_10100','doc_10200','doc_10300','doc_10400','doc_10500','doc_10600','doc_10700','doc_10800','doc_10900','doc_11000'"
        delete_result = self.run_cbq_query("DELETE FROM default USE KEYS [{0}]".format(keys))
        self.assertEqual(delete_result['status'], 'success', 'Query was not run successfully')
        ensure_deleted = self.run_cbq_query("SELECT * from default USE KEYS [{0}]".format(keys))
        self.assertEqual(ensure_deleted['metrics']['resultCount'], 0)
        self.conn.delete_all_buckets()

    def test_update_use_keys(self):
        self.load_data()
        keys = "'doc_10000','doc_10100','doc_10200','doc_10300','doc_10400','doc_10500','doc_10600','doc_10700','doc_10800','doc_10900','doc_11000'"
        delete_result = self.run_cbq_query("UPDATE default USE KEYS [{0}] SET new_field = 'updated'".format(keys))
        self.assertEqual(delete_result['status'], 'success', 'Query was not run successfully')
        ensure_deleted = self.run_cbq_query("SELECT new_field from default where new_field = 'updated'")
        self.assertEqual(ensure_deleted['metrics']['resultCount'], 11)
        self.conn.delete_all_buckets()

    ##############################################################################################
    #
    #   query types
    ##############################################################################################


    def test_raw_limit(self):
        self.load_data()
        for query_bucket in self.query_buckets:
            self.query = "select raw firstName from %s order by firstName limit 5" % query_bucket
            actual_list = self.run_cbq_query()
            expected_results = ['Aaron', 'Aaron', 'Aaron', 'Aaron', 'Aaron']
            diffs = DeepDiff(actual_list['results'], expected_results, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        self.conn.delete_all_buckets()

    def test_group_by_having(self):
        self.load_data()
        for query_bucket in self.query_buckets:
            self.query = "from %s WHERE age>25 GROUP BY country " % query_bucket + \
                         "HAVING COUNT(country) > 0 SELECT country " + \
                         "ORDER BY country limit 10"
            actual_result = self.run_cbq_query()
            expected_result = [{'country': 'Afghanistan'}, {'country': 'Albania'}, {'country': 'Algeria'}, {'country': 'American Samoa'}, {'country': 'Andorra'}, {'country': 'Angola'}, {'country': 'Anguilla'}, {'country': 'Antarctica (the territory South of 60 deg S)'}, {'country': 'Antigua and Barbuda'}, {'country': 'Argentina'}]
            self.assertEqual(actual_result['results'], expected_result)
        self.conn.delete_all_buckets()

    def test_named_params(self):
        self.load_data()
        for bucket in self.buckets:
            queries = ['\SET -$age 25;', 'select firstName, lastName, age from default where age<=$age limit 100;', '\quit;']
            o = self.shell.execute_commands_inside(self.cbqpath, '', queries, '', '', bucket.name, '' )
            self.assertTrue('"resultCount":100' in o)
        self.conn.delete_all_buckets()

    def test_basic_aliasing(self):
        self.load_data()
        results = self.run_cbq_query(query="SELECT b.lastName FROM default:default._default._default b WHERE b.lastName = 'Kunde' limit 15")
        expected_results = [{'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}, {'lastName': 'Kunde'}]
        self.assertEqual(expected_results,results['results'])
        self.conn.delete_all_buckets()

    def test_union_aggr_fns(self):
        self.load_data()
        self.sleep(600)
        for query_bucket in self.query_buckets:
            self.query = "select count(firstName) as firstnames from default union select count(lastName) as " \
                         "lastnames from default"
            actual_list = self.run_cbq_query()
            docs = self.run_cbq_query(query="select * from default")
            num_docs = docs['metrics']['resultCount']
            # handle case if result is lastnames before firstnames
            expected_results_1 = [{'firstnames': num_docs}, {'lastnames': num_docs}]
            expected_results_2 = [{'lastnames': num_docs}, {'firstnames': num_docs}]
            self.assertTrue(actual_list['results'] == expected_results_1 or actual_list['results'] == expected_results_2,
                            f"Unexpected results: {actual_list['results']}")
        self.conn.delete_all_buckets()

    def test_basic_cte(self):
        self.load_data()

        queries = dict()

        # constant expression
        query_1 = 'with a as (10), b as ("bob"), c as (3.3), d as (0), e as (""), f as (missing), g as (null) '
        query_1 += 'select a, b, c, d, e, f, g, age as h, age+a as i, age+b as j, age+c as k, age+d as l, age+e as m, age+f as n, age+g as o from default '
        query_1 += 'where a > 9 and b == "bob" and c < 4 and d == 0 and e is valued and f is missing and g is null '
        query_1 += 'order by a, b, c, d, e, f, g, h, i, j, k, l, m, n, o limit 100'
        verify_1 = 'select 10 as a, "bob" as b, 3.3 as c, 0 as d, "" as e, missing as f, null as g, age as h, age+10 as i, age+"bob" as j, age+3.3 as k, age+0 as l, age+"" as m, age+missing as n, age+null as o from default '
        verify_1 += 'where 10 > 9 and "bob" == "bob" and 3.3 < 4 and 0 == 0 and "" is valued and missing is missing and null is null '
        verify_1 += 'order by a, b, c, d, e, f, g, h, i, j, k, l, m, n, o limit 100'

        # subquery
        query_2 = 'with a as (select age from default order by age limit 100) select table1.age from a as table1 order by table1.age limit 100'
        verify_2 = 'select age from default order by age limit 100'

        query_3 = 'with a as (select age from default order by age limit 100) select table1.age from a as table1 where table1.age in a[*].age order by table1.age limit 100'
        verify_3 = 'select age from default where not age order by age limit 100'

        query_4 = 'with a as (select age from default order by age limit 100 ) select age from default where age in a[*].age order by age limit 100'
        verify_4 = 'select age from default where not age order by age limit 100'

        query_5 = 'with a as (select raw age from default order by age limit 100) select age from default where age in a order by age limit 100'
        verify_5 = 'select age from default where not age order by age limit 100'

        query_6 = 'with a as (select age from default order by age limit 100) select table1.age from a as table1 where table1.age in a[*].age order by table1.age limit 100'
        verify_6 = 'select age from default where not age order by age limit 100'

        queries["a"] = {"queries": [query_1], "asserts": [self.verifier(verify_1)]}
        queries["b"] = {"queries": [query_2], "asserts": [self.verifier(verify_2)]}
        queries["c"] = {"queries": [query_3], "asserts": [self.verifier(verify_3)]}
        queries["d"] = {"queries": [query_4], "asserts": [self.verifier(verify_4)]}
        queries["e"] = {"queries": [query_5], "asserts": [self.verifier(verify_5)]}
        queries["f"] = {"queries": [query_6], "asserts": [self.verifier(verify_6)]}


        self.query_runner(queries)
        self.conn.delete_all_buckets()

    def test_udf_letting(self):
        self.load_data()
        self.sleep(600)
        string_functions = 'function concater(a,b) { var text = ""; var x; for (x in a) {if (x = b) { return x; }} return "n"; } function comparator(a, b) {if (a > b) { return "old hotel"; } else { return "new hotel" }}'
        function_names2 = ["concater","comparator"]
        created2 = self.create_library("strings",string_functions,function_names2)
        try:
            self.run_cbq_query(query='CREATE FUNCTION func1(a,b) LANGUAGE JAVASCRIPT AS "comparator" AT "strings"')
            self.run_cbq_query("CREATE FUNCTION func2(degrees) LANGUAGE INLINE AS (degrees - 32) ")
            self.run_cbq_query(query='CREATE FUNCTION func3(a,b) LANGUAGE JAVASCRIPT AS "concater" AT "strings"')
            results = self.run_cbq_query(
                "SELECT firstName FROM default:default LET maximum_no = func2(36) WHERE age = maximum_no GROUP BY firstName LETTING letter = func3('Ayako','A') HAVING firstName > letter")
            expected_result = self.run_cbq_query("SELECT firstName FROM default:default LET maximum_no = 4 WHERE age = maximum_no GROUP BY firstName LETTING letter = 'A' HAVING firstName > letter")
            diffs = DeepDiff(results['results'],  expected_result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            try:
                self.delete_library("strings")
                self.run_cbq_query("DROP FUNCTION func1")
                self.run_cbq_query("DROP FUNCTION func2")
                self.run_cbq_query("DROP FUNCTION func3")
                self.conn.delete_all_buckets()
            except Exception as e:
                self.log.error(str(e))

    ##############################################################################################
    #
    #   txns
    ##############################################################################################

    def test_txns(self):
        self.load_data(num_extra_buckets=1)
        self.run_cbq_query("CREATE INDEX idx1 on default(name)")
        self.run_cbq_query("CREATE INDEX idx2 on bucket0(name)")
        results = self.run_cbq_query(query="START TRANSACTION", txtimeout="2m", server=self.master)
        txn_id = results['results'][0]['txid']
        key = "test"
        value = '{"name": "new1"}'
        self.query = 'INSERT into bucket0 (key , value) VALUES ("%s", %s)' % (key, value)
        actual_result = self.run_cbq_query(txnid=txn_id)
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        value = '{"name": "new1"}'
        self.query = 'INSERT into default (key , value) VALUES ("%s", %s)' % ("new1", value)
        actual_result = self.run_cbq_query(txnid=txn_id)
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        time.sleep(5)
        new_name = 'edit'
        self.query = 'MERGE INTO default b1 USING bucket0 b2 on b1.name = b2.name when matched then update set b1.name="%s"' % (new_name)
        actual_result = self.run_cbq_query(txnid=txn_id)
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        if self.rollback:
            self.run_cbq_query(query="ROLLBACK TRANSACTION", txnid=txn_id, server=self.master)
        else:
            self.run_cbq_query(query="COMMIT TRANSACTION", txnid=txn_id, server=self.master)
        self.query = 'SELECT * FROM default where name = "{0}"'.format(new_name)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
        if self.rollback:
            self.assertEqual(actual_result['metrics']['resultCount'], 0, "There should be no results: {0}".format(actual_result))
        else:
            self.assertEqual(actual_result['results'][0]['default']['name'], new_name, 'Name was not updated')
        self.conn.delete_all_buckets()

    def verifier(self, compare_query):
        return lambda x: self.compare_queries(x['q_res'][0], compare_query)

    def compare_queries(self, actual_results, compare_query):
        let_letting_docs = actual_results['results']
        compare_results = self.run_cbq_query(query=compare_query)
        compare_docs = compare_results['results']
        self.assertEqual(len(let_letting_docs), len(compare_docs))
        self.log.info("compared")
        self.assertEqual(let_letting_docs, compare_docs)
        self.log.info("they are equal")

    def load_data(self, num_extra_buckets=0):
        self.conn.delete_all_buckets()
        time.sleep(5)
        self.conn.create_bucket(bucket="default", ramQuotaMB=self.bucket_size, proxyPort=11220, storageBackend="magma", replicaNumber=0)
        time.sleep(5)
        for i in range(0, num_extra_buckets):
            self.conn.create_bucket(bucket="bucket{0}".format(i), ramQuotaMB=self.bucket_size, proxyPort=11220, storageBackend="magma", replicaNumber=0)
            time.sleep(20)
            self.run_cbq_query("CREATE PRIMARY INDEX on `bucket{0}`".format(i))
        self.run_cbq_query("CREATE PRIMARY INDEX on `default`")
        self.buckets = self.conn.get_buckets()
        self.query_buckets = self.buckets
        self.gen_create = SDKDataLoader(num_ops=self.num_items)
        for bucket in self.buckets:
            self.cluster.async_load_gen_docs_till_dgm(server=self.master,
                                                  active_resident_threshold=self.active_resident_threshold,
                                                  bucket=bucket,
                                                  scope=None, collection=None,
                                                  exp=self.expiry,
                                                  value_size=self.value_size, timeout_mins=60,
                                                  java_sdk_client=self.java_sdk_client)
        for bkt in self.buckets:
            print(self.stat.get_collection_stats(bkt))

