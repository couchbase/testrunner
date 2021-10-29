import time
from deepdiff import DeepDiff
import uuid
from .tuq import QueryTests
from collection.collections_stats import CollectionsStats
from couchbase_helper.cluster import Cluster
from lib.couchbase_helper.documentgenerator import SDKDataLoader
from membase.api.rest_client import RestConnection



class QueryArrayFlatteningTests(QueryTests):
    def setUp(self):
        super(QueryArrayFlatteningTests, self).setUp()
        self.log.info("==============  QueryArrayFlatteningTests setup has started ==============")
        self.bucket_name = self.input.param("bucket", self.default_bucket_name)
        self.active_resident_threshold = self.input.param("resident_ratio", 100)
        self.kv_dataset = self.input.param("kv_dataset", "Hotel")
        self.num_items = self.input.param("num_items", 10000)
        self.expiry = self.input.param("expiry", 0)
        self.rollback = self.input.param("rollback", False)
        self.conn = RestConnection(self.master)
        self.stat = CollectionsStats(self.master)
        self.cbqpath = '{0}cbq -quiet -u {1} -p {2} -e=localhost:8093 '.format(self.path, self.username, self.password)
        self.cluster = Cluster()
        self.load_data()
        self.log.info("==============  QueryArrayFlatteningTests setup has completed ==============")

    def suite_setUp(self):
        super(QueryArrayFlatteningTests, self).suite_setUp()
        self.log.info("==============  QueryArrayFlatteningTests suite_setup has started ==============")
        self.log.info("==============  QueryArrayFlatteningTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueryArrayFlatteningTests tearDown has started ==============")
        self.log.info("==============  QueryArrayFlatteningTests tearDown has completed ==============")
        super(QueryArrayFlatteningTests, self).tearDown()


    def suite_tearDown(self):
        self.log.info("==============  QueryArrayFlatteningTests suite_tearDown has started ==============")
        self.log.info("==============  QueryArrayFlatteningTests suite_tearDown has completed ==============")
        super(QueryArrayFlatteningTests, self).suite_tearDown()

    '''Flatten index needs at least one argument, try passing in no arguments'''
    def test_flatten_negative(self):
        try:
            self.run_cbq_query(query="CREATE INDEX idx1 ON default(DISTINCT ARRAY FLATTEN_KEYS() FOR r IN reviews END,country,email)")
            self.fail()
        except Exception as ex:
            self.assertTrue("Number of arguments to function flatten_keys must be between 1 and 32" in str(ex), "Exception is not what was expected, exception should have been a syntax error! Exception is {0}".format(str(ex)))

    '''Multiple array keys are not allowed in one index, test this '''
    def test_flatten_multiple_array_keys(self):
        try:
            self.run_cbq_query(
                query="CREATE INDEX idx2 ON default(DISTINCT ARRAY(ALL ARRAY FLATTEN_KEYS(rting.Cleanliness,rting.Rooms) "
                      "FOR rting IN r.ratings END) FOR r IN reviews END, DISTINCT ARRAY flatten_keys(r.author) FOR r IN "
                      "reviews END, free_parking)")
            self.fail()
        except Exception as ex:
            self.assertTrue("Multiple expressions with ALL are found. Only one array expression is supported per index." in str(ex),
                            "Exception is not what was expected. Exception is {0}".format(
                                str(ex)))

    '''Flatten index takes between 1-32 arguments, try with 33 arguments'''
    def test_flatten_arguments(self):
        try:
            self.run_cbq_query(query="CREATE INDEX idx2 ON default(DISTINCT ARRAY FLATTEN_KEYS(r.field1,r.field1,"
                                     "r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,"
                                     "r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,"
                                     "r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,"
                                     "r.field1,r.field1,r.field1,r.field1) FOR r IN reviews END,country,email);")
            self.fail()
        except Exception as ex:
            self.assertTrue("Number of arguments to function flatten_keys (near line 1, column 59) must be between 1 and 32"
                            in str(ex), "Exception is not what was expected, exception should have been a syntax error! "
                                        "Exception is {0}".format(str(ex)))

    '''Flatten index takes between 1-32 arguments, try with 32 arguments'''
    def test_flatten_max_arguments(self):
        actual_results= self.run_cbq_query(query="CREATE INDEX idx2 ON default(DISTINCT ARRAY FLATTEN_KEYS(r.field1,r.field1,"
                                 "r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,"
                                 "r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,"
                                 "r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,"
                                 "r.field1,r.field1,r.field1) FOR r IN reviews END,country,email);")
        self.assertTrue(actual_results['status'] == 'success', "Index was not successfully created! {0}".format(actual_results))

    ''' We expect it not to pick up this index since its not using any of the fields '''
    def test_flatten_no_fields(self):
        index_results = self.run_cbq_query(
            query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author) FOR r IN reviews END,country,email)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.ratings.Cleanliness > 1 END")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected primary got {0}".format(
                            explain_results))

    '''Verify a basic query that uses flatten against primary index'''
    def test_flatten_basic(self):
        index_results = self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author) FOR r IN reviews END,country,email)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' END")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(explain_results))
        query_results = self.run_cbq_query(query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' END")
        expected_results = self.run_cbq_query(query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' END")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test flatten key with all keyword instead of distinct'''
    def test_flatten_all(self):
        index_results = self.run_cbq_query(
            query="create index idx1 on default(ALL ARRAY FLATTEN_KEYS(r.ratings.Rooms,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.ratings.Rooms = 3 and r.ratings.Cleanliness > 1 END AND free_parking = True")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.ratings.Rooms = 3 and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.ratings.Rooms = 3 and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test flatten key index with an array that contains distinct keyword and another array that contains all keyword'''
    def test_flatten_all_distinct(self):
        index_results = self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((ALL (ARRAY(DISTINCT (ARRAY flatten_keys(n,v) FOR n:v IN (`r`.`ratings`) END)) FOR `r` IN `reviews` END)))")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_all_all(self):
        index_results = self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((ALL (ARRAY(ALL (ARRAY flatten_keys(n,v) FOR n:v IN (`r`.`ratings`) END)) FOR `r` IN `reviews` END)))")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_distinct_all(self):
        index_results = self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((DISTINCT (ARRAY(ALL (ARRAY flatten_keys(n,v) FOR n:v IN (`r`.`ratings`) END)) FOR `r` IN `reviews` END)))")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test flatten with array as non-leading key and some fields not used'''
    def test_flatten_non_leading(self):
        index_results = self.run_cbq_query(query="create index idx1 on default(country, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        expected_results = self.run_cbq_query(query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_field_value(self):
        index_results = self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        expected_results = self.run_cbq_query(query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test flatten with array as leading key and some fields not used'''
    def test_flatten_skip_keys_leading(self):
        index_results = self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        expected_results = self.run_cbq_query(query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test what happens when you have an array with ALL/DISTINCT flatten_keys(v1,v2) but query contains any (v2,1)'''
    def test_flatten_array_ordering(self):
        index_results = self.run_cbq_query(
            query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,p.ratings.Overall) FOR r,p IN reviews END, avg_rating, country)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default AS d WHERE ANY p,r IN d.reviews SATISFIES p.ratings.Overall=5 and r.author='author' END AND avg_rating > 2 AND country is not null")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))

    '''Teset partial index'''
    def test_flatten_partial_index(self):
        index_results = self.run_cbq_query(
            query="create index idx1 on default(ALL ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking) where free_parking = True")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        # Ensure partial index is not selected when it does not apply
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = False")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))

    '''Test out using when clause in array index key, then use queries that fall inside and outside the when clause, equality and comparators'''
    def test_flatten_when(self):
        index_results = self.run_cbq_query(query="create index idx1 on default(country, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews when r.ratings.Cleanliness < 3 END, email, free_parking)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness < 2 END AND free_parking = True AND country is not null")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness < 2 END AND free_parking = True AND country is not null")
        expected_results = self.run_cbq_query(query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness < 2 END AND free_parking = True AND country is not null")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness > 3 END AND free_parking = True AND country is not null")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness > 3 END AND free_parking = True AND country is not null")
        expected_results = self.run_cbq_query(query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness > 3 END AND free_parking = True AND country is not null")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness = 4 END AND free_parking = True AND country is not null")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness = 4 END AND free_parking = True AND country is not null")
        expected_results = self.run_cbq_query(query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness = 4 END AND free_parking = True AND country is not null")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test array_flatten index with array key as leading key, index should be ignored when the when clause is false'''
    def test_flatten_when_leading(self):
        index_results = self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews when r.ratings.Cleanliness < 3 END, country, email, free_parking)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness < 2 END AND free_parking = True AND country is not null")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness < 2 END AND free_parking = True AND country is not null")
        expected_results = self.run_cbq_query(query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness < 2 END AND free_parking = True AND country is not null")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness > 3 END AND free_parking = True AND country is not null")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))

        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness = 4 END AND free_parking = True AND country is not null")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))

    '''Test insert that uses a query that uses an index with flatten_keys in it '''
    def test_flatten_insert(self):
        self.rest.load_sample("travel-sample")
        time.sleep(15)
        index_results = self.run_cbq_query(
            query="CREATE INDEX idx1 ON `default`((ALL (ARRAY(ALL (ARRAY flatten_keys(n,v) FOR n:v IN (`r`.`ratings`) END)) FOR `r` IN `reviews` END)))")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN INSERT INTO `travel-sample`.inventory.landmark (KEY foo, VALUE bar) "
                  "SELECT META(doc).id AS foo, doc AS bar FROM `default` AS doc WHERE ANY r IN doc.reviews SATISFIES "
                  "ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        insert_results = self.run_cbq_query(query="INSERT INTO `travel-sample`.inventory.landmark (KEY foo, VALUE bar) "
                                                  "SELECT META(doc).id AS foo, doc AS bar FROM `default` AS doc WHERE "
                                                  "ANY r IN doc.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' "
                                                  "AND v = 3 END END")
        self.assertTrue(insert_results['status'] == 'success', "Index was not successfully created! {0}".format(insert_results))
        query_results = self.run_cbq_query(
            query="SELECT d.country, d.address, d.free_parking, d.city, d.type, d.url, d.phone, d.price, d.avg_rating, d.name, d.email FROM `travel-sample`.inventory.landmark AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END ")
        old_results = self.run_cbq_query(query="SELECT d.country, d.address, d.free_parking, d.city, d.type, d.url, d.phone, d.price, d.avg_rating, d.name, d.email FROM default AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END ")
        diffs = DeepDiff(query_results['results'], old_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test upsert that uses a query that uses an index with flatten_keys in it'''
    def test_flatten_upsert(self):
        self.rest.load_sample("travel-sample")
        time.sleep(15)
        index_results = self.run_cbq_query(
            query="CREATE INDEX idx1 ON `default`((ALL (ARRAY(ALL (ARRAY flatten_keys(n,v) FOR n:v IN (`r`.`ratings`) END)) FOR `r` IN `reviews` END)))")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN UPSERT INTO `travel-sample`.inventory.landmark (KEY foo, VALUE bar) "
                  "SELECT META(doc).id AS foo, doc AS bar FROM `default` AS doc WHERE ANY r IN doc.reviews SATISFIES "
                  "ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        insert_results = self.run_cbq_query(query="UPSERT INTO `travel-sample`.inventory.landmark (KEY foo, VALUE bar) "
                                                  "SELECT META(doc).id AS foo, doc AS bar FROM `default` AS doc WHERE "
                                                  "ANY r IN doc.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' "
                                                  "AND v = 3 END END")
        self.assertTrue(insert_results['status'] == 'success',
                        "Index was not successfully created! {0}".format(insert_results))
        query_results = self.run_cbq_query(
            query="SELECT d.country, d.address, d.free_parking, d.city, d.type, d.url, d.phone, d.price, d.avg_rating, d.name, d.email FROM `travel-sample`.inventory.landmark AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        old_results = self.run_cbq_query(query="SELECT d.country, d.address, d.free_parking, d.city, d.type, d.url, d.phone, d.price, d.avg_rating, d.name, d.email FROM default AS d WHERE ANY r IN d.reviews "
                                               "SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        diffs = DeepDiff(query_results['results'], old_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        update_results = self.run_cbq_query(query="UPSERT INTO `travel-sample`.inventory.landmark (KEY foo, VALUE bar) "
                                                  "SELECT META(doc).id AS foo, doc AS bar FROM `default` AS doc WHERE "
                                                  "ANY r IN doc.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' "
                                                  "AND v = 3 END END")
        self.assertTrue(insert_results['status'] == 'success',
                        "Index was not successfully created! {0}".format(insert_results))
        query_results = self.run_cbq_query(
            query="SELECT d.country, d.address, d.free_parking, d.city, d.type, d.url, d.phone, d.price, d.avg_rating, d.name, d.email FROM `travel-sample`.inventory.landmark AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        old_results = self.run_cbq_query(query="SELECT d.country, d.address, d.free_parking, d.city, d.type, d.url, d.phone, d.price, d.avg_rating, d.name, d.email FROM default AS d WHERE ANY r IN d.reviews "
                                               "SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END")
        diffs = DeepDiff(query_results['results'], old_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_update(self):
        self.rest.load_sample("travel-sample")
        time.sleep(30)
        self.run_cbq_query(query="create index idx1 on default(country, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, avg_rating)")
        update_results = self.run_cbq_query(
            query="UPDATE `travel-sample`.inventory.airport "
                  "SET foo = 9 WHERE country IN "
                  "(SELECT RAW country FROM default d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END "
                  "AND avg_rating <= 3 AND country IS NOT NULL)")
        self.assertTrue(update_results['status'] == "success")
        mutation_count = update_results['metrics']['mutationCount']
        verify_results = self.run_cbq_query(query="select foo from `travel-sample`.inventory.airport where foo = 9")
        self.assertEqual(verify_results['metrics']['resultCount'], mutation_count, "Results mismatched, here are the verify_results {0}".format(verify_results))


    def test_flatten_delete(self):
        index_results = self.run_cbq_query(
            query="CREATE INDEX idx1 ON default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, free_parking) WHERE ANY r IN default.reviews SATISFIES r.author LIKE 'M%' END")
        explain_results = self.run_cbq_query(query="explain delete from default d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness = 3 END AND free_parking = True")

        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.asserTrue("covers" in str(explain_results), "This query should be covered by the index but it is not: plan {0}",format(explain_results))
        delete_results = self.run_cbq_query(query="delete from default d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness = 3 END AND free_parking = True")
        self.assertTrue(delete_results['status'] == 'success',
                        "Index was not successfully created! {0}".format(delete_results))
        # Ensure no documents remain that fit the criteria
        primary_results = self.run_cbq_query(query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness = 3 END AND free_parking = True")
        self.assertTrue(primary_results['metrics']['resultCount'] == 0 ,"There are results! But there should be no results for this query {0}".format(primary_results))

    def test_flatten_advise(self):
        advise_results = self.run_cbq_query(
            query="ADVISE SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness > 1 END AND free_parking = TRUE AND country IS NOT NULL")
        self.assertTrue("FLATTEN_KEYS" in str(advise_results), "Advisor should've advised an index with flatten_keys but it did not, advisor output {0}".format(advise_results))
        create_idx_statement = advise_results['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement']
        idx_name = advise_results['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement'].split("INDEX")[1].split("ON")[0].strip()
        self.run_cbq_query(query=create_idx_statement)
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness > 1 END AND free_parking = TRUE AND country IS NOT NULL")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == idx_name,
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        results = self.run_cbq_query(
            query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness > 1 END AND free_parking = TRUE AND country IS NOT NULL")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness > 1 END AND free_parking = TRUE AND country IS NOT NULL")
        diffs = DeepDiff(results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_advise_equivalent(self):
        index_results = self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author) FOR r IN reviews END,country,email)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default "
                                                   "WHERE country = 'Norfolk Island' and email = 'Willian.Abshire@hotels.com' "
                                                   "AND ANY r IN reviews SATISFIES r.author = 'Martin Feest' END")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(explain_results))
        advise_results = self.run_cbq_query(
            query="ADVISE SELECT * FROM default WHERE country = 'Norfolk Island' and email = 'Willian.Abshire@hotels.com' "
                  "AND ANY r IN reviews SATISFIES r.author = 'Martin Feest' END")
        self.assertTrue(advise_results['results'][0]['advice']['adviseinfo']['recommended_indexes'] == "No index recommendation at this time.", "There shouldn't be a recommended index! {0}".format(advise_results))

    def test_flatten_orderby_limit(self):
        index_results = self.run_cbq_query(
            query="CREATE INDEX idx1 ON default(DISTINCT ARRAY flatten_keys(r.ratings.Cleanliness,r.ratings.Rooms,r.author) FOR r IN reviews END, country)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT d.country, d.reviews FROM default AS d "
                                                   "WHERE ANY r IN d.reviews SATISFIES r.author LIKE '%m' AND "
                                                   "(r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END "
                                                   "ORDER BY d.country LIMIT 10")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT d.country, d.reviews FROM default AS d "
                                                   "WHERE ANY r IN d.reviews SATISFIES r.author LIKE '%m' AND "
                                                   "(r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END "
                                                   "ORDER BY d.country LIMIT 10")
        expected_results = self.run_cbq_query(
            query="SELECT d.country, d.reviews FROM default AS d USE INDEX (`#primary`)"
                                                   "WHERE ANY r IN d.reviews SATISFIES r.author LIKE '%m' AND "
                                                   "(r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END "
                                                   "ORDER BY d.country LIMIT 10")
        diffs = DeepDiff(query_results['results'], expected_results['results'])
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_unnest_any(self):
        index_results = self.run_cbq_query(query="CREATE INDEX idx1 ON default(public_likes, DISTINCT ARRAY flatten_keys(r.ratings.Cleanliness,r.ratings.Rooms,r.author) FOR r IN reviews END, free_parking)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d UNNEST d.public_likes p WHERE p "
                                                   "LIKE 'R%' AND ANY r IN d.reviews SATISFIES r.author LIKE '%m' AND "
                                                   "(r.ratings.Cleanliness >= 1 OR r.ratings.Rooms <= 3) END AND d.free_parking = TRUE")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(explain_results))
        query_results = self.run_cbq_query(query="SELECT * FROM default AS d UNNEST d.public_likes p WHERE p "
                                                   "LIKE 'R%' AND ANY r IN d.reviews SATISFIES r.author LIKE '%m' AND "
                                                   "(r.ratings.Cleanliness >= 1 OR r.ratings.Rooms <= 3) END AND d.free_parking = TRUE")
        expected_results = self.run_cbq_query(query="SELECT * FROM default AS d USE INDEX (`#primary`) UNNEST d.public_likes p WHERE p "
                                                   "LIKE 'R%' AND ANY r IN d.reviews SATISFIES r.author LIKE '%m' AND "
                                                   "(r.ratings.Cleanliness >= 1 OR r.ratings.Rooms <= 3) END AND d.free_parking = TRUE")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_or(self):
        index_results = self.run_cbq_query(
            query="CREATE INDEX idx1 ON default(public_likes, DISTINCT ARRAY flatten_keys(r.ratings.Cleanliness,r.ratings.Rooms,r.author) FOR r IN reviews END, free_parking)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT * FROM default AS d UNNEST d.public_likes p WHERE p "
                                                   "LIKE 'R%' AND ANY r IN d.reviews SATISFIES r.author LIKE '%m' AND "
                                                   "(r.ratings.Cleanliness >= 1 OR r.ratings.Rooms <= 3) END OR d.free_parking = TRUE")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT * FROM default AS d UNNEST d.public_likes p WHERE p "
                                                 "LIKE 'R%' AND ANY r IN d.reviews SATISFIES r.author LIKE '%m' AND "
                                                 "(r.ratings.Cleanliness >= 1 OR r.ratings.Rooms <= 3) END OR d.free_parking = TRUE")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default AS d USE INDEX (`#primary`) UNNEST d.public_likes p WHERE p "
                  "LIKE 'R%' AND ANY r IN d.reviews SATISFIES r.author LIKE '%m' AND "
                  "(r.ratings.Cleanliness >= 1 OR r.ratings.Rooms <= 3) END OR d.free_parking = TRUE")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''We expect the query to pick up the array with the keys flattened'''
    def test_flatten_index_selection(self):
        self.run_cbq_query(query="create index idx1 on default(country, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)")
        self.run_cbq_query(query="create index idx2 on default(country, DISTINCT ARRAY [r.author,r.ratings.Cleanliness] FOR r IN reviews END, email, free_parking)")
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END AND free_parking = TRUE AND country IS NOT NULL")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == "idx1",
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        results = self.run_cbq_query(
            query="SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3  END AND free_parking = TRUE AND country IS NOT NULL")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END AND free_parking = TRUE AND country IS NOT NULL")
        diffs = DeepDiff(results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def load_data(self, num_extra_buckets=0):
        self.conn.delete_all_buckets()
        time.sleep(5)
        self.conn.create_bucket(bucket="default", ramQuotaMB=256, proxyPort=11220, storageBackend="magma", replicaNumber=0)
        for i in range(0, num_extra_buckets):
            self.conn.create_bucket(bucket="bucket{0}".format(i), ramQuotaMB=256, proxyPort=11220, storageBackend="magma", replicaNumber=0)
            self.run_cbq_query("CREATE PRIMARY INDEX on bucket{0}".format(i))
        self.run_cbq_query("CREATE PRIMARY INDEX on default")
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
                                                  java_sdk_client=self.java_sdk_client, kv_dataset=self.kv_dataset)
        for bkt in self.buckets:
            print(self.stat.get_collection_stats(bkt))

