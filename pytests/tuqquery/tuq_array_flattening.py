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
        self.explicit = self.input.param("explicit", False)
        self.use_all = self.input.param("use_all", False)
        self.use_unnest = self.input.param("use_unnest", False)
        self.any_every = self.input.param("any_every", False)
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

    ##############################################################################################
    #
    #   Negative Tests
    ##############################################################################################

    '''Flatten index needs at least one argument, try passing in no arguments'''
    def test_flatten_negative(self):
        try:
            self.run_cbq_query(query="CREATE INDEX idx1 ON default(DISTINCT ARRAY FLATTEN_KEYS() FOR r IN reviews END,country,email)")
            self.fail()
        except Exception as ex:
            self.assertTrue("Number of arguments to function flatten_keys must be between 1 and 32" in str(ex), "Exception is not what was expected, exception should have been a syntax error! Exception is {0}".format(str(ex)))

    '''Multiple array keys are not allowed in one index, test this'''
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

    '''Every will not pick up the index'''
    def test_flatten_every(self):
        self.run_cbq_query(
            query="create index idx1 on default(ALL ARRAY FLATTEN_KEYS(r.ratings.Rooms,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default AS d WHERE EVERY r IN d.reviews SATISFIES r.ratings.Rooms = 3 and r.ratings.Cleanliness > 1 END AND free_parking = True")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected primary got {0}".format(
                            explain_results))

    '''Unnest should not work unless array index is leading key'''
    def test_flatten_unnest_non_leading(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`(`email`,(distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)))")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected primary got {0}".format(
                            explain_results))

    '''Try to unnest an object'''
    def test_flatten_unnest_object(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`(`email`,(distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)))")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d UNNEST d.reviews AS r UNNEST r.ratings AS s WHERE s.Rooms > 1 AND s.Overall < 5")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected primary got {0}".format(
                            explain_results))

    '''Use an unnest that doesn't use elements in the flatten, should ignore the index'''
    def test_flatten_unnest_negative(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected primary got {0}".format(
                            explain_results))

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
            self.assertTrue("Number of arguments to function flatten_keys must be between 1 and 32"
                            in str(ex), "Exception is not what was expected, exception should have been a syntax error! "
                                        "Exception is {0}".format(str(ex)))

    ''' We expect it not to pick up this index since its not using any of the fields '''
    def test_flatten_no_fields(self):
        self.run_cbq_query(
            query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author) FOR r IN reviews END,country,email)")

        query = "EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.ratings.Cleanliness > 1 END"
        if self.any_every:
            query = query.replace("ANY", "ANY AND EVERY")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query=query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected primary got {0}".format(
                            explain_results))

    ##############################################################################################
    #
    #   General Tests
    ##############################################################################################

    '''Flatten index takes between 1-32 arguments, try with 32 arguments'''
    def test_flatten_max_arguments(self):
        actual_results= self.run_cbq_query(query="CREATE INDEX idx2 ON default(DISTINCT ARRAY FLATTEN_KEYS(r.field1,r.field1,"
                                 "r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,"
                                 "r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,"
                                 "r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,r.field1,"
                                 "r.field1,r.field1,r.field1) FOR r IN reviews END,country,email);")
        self.assertTrue(actual_results['status'] == 'success', "Index was not successfully created! {0}".format(actual_results))


    '''Verify a basic query that uses flatten against primary index'''
    def test_flatten_basic(self):
        self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author) FOR r IN reviews END,country,email)")
        if self.use_unnest:
            query = "SELECT * FROM default AS d unnest reviews as r WHERE r.author LIKE 'M%'"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'M%'"
        else:
            query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' END"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' END"

        if self.any_every:
            query = query.replace("ANY", "ANY AND EVERY")
            primary_query = primary_query.replace("ANY", "ANY AND EVERY")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    '''Try the asc desc keywords in index creation'''
    def test_flatten_asc_desc(self):
        self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author ASC,r.ratings.Cleanliness DESC) FOR r IN reviews END, email, free_parking, country)")
        if self.use_unnest:
            query = "SELECT * FROM default AS d unnest reviews as r WHERE r.author LIKE 'M%' and r.ratings.Cleanliness > 1 AND d.free_parking = True AND d.country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'M%' and r.ratings.Cleanliness > 1 AND d.free_parking = True AND d.country is not null"
        else:
            query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND d.free_parking = True AND d.country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND d.free_parking = True AND d.country is not null"

        if self.any_every:
            query = query.replace("ANY", "ANY AND EVERY")
            primary_query = primary_query.replace("ANY", "ANY AND EVERY")

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.compare_against_primary(query, primary_query)

    '''Test flatten key with all keyword instead of distinct'''
    def test_flatten_all(self):
        self.run_cbq_query(
            query="create index idx1 on default(ALL ARRAY FLATTEN_KEYS(r.ratings.Rooms,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)")
        if self.use_unnest:
            query = "SELECT * FROM default AS d unnest reviews as r WHERE r.ratings.Rooms = 3 and r.ratings.Cleanliness > 1 " \
                    "AND d.free_parking = True AND d.country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.ratings.Rooms = 3 and r.ratings.Cleanliness > 1 " \
                    "AND d.free_parking = True AND d.country is not null"
        else:
            query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.ratings.Rooms = 3 and r.ratings.Cleanliness > 1 END " \
                    "AND free_parking = True AND country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews " \
                            "SATISFIES r.ratings.Rooms = 3 and r.ratings.Cleanliness > 1 END AND free_parking = True " \
                            "AND country is not null"

        if self.any_every:
            query = query.replace("ANY", "ANY AND EVERY")
            primary_query = primary_query.replace("ANY", "ANY AND EVERY")

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.compare_against_primary(query, primary_query)

    '''Test flatten key index with an array that contains distinct keyword and another array that contains all keyword'''
    def test_flatten_all_distinct(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((ALL (ARRAY(DISTINCT (ARRAY flatten_keys(n,v) FOR n:v IN (`r`.`ratings`) END)) FOR `r` IN `reviews` END)))")

        query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END"
        primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.compare_against_primary(query, primary_query)

    def test_flatten_all_all(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((ALL (ARRAY(ALL (ARRAY flatten_keys(n,v) FOR n:v IN (`r`.`ratings`) END)) FOR `r` IN `reviews` END)))")

        query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END"
        primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.compare_against_primary(query, primary_query)

    def test_flatten_distinct_all(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((DISTINCT (ARRAY(ALL (ARRAY flatten_keys(n,v) FOR n:v IN (`r`.`ratings`) END)) FOR `r` IN `reviews` END)))")
        query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END"
        primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES ANY n:v IN r.ratings SATISFIES n = 'Cleanliness' AND v = 3 END END"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.compare_against_primary(query, primary_query)

    '''Test flatten with array as non-leading key and some fields not used'''
    def test_flatten_non_leading(self):
        self.run_cbq_query(query="create index idx1 on default(country, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)")
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
        self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)")
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
        self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)")
        if self.use_unnest:
            query = "SELECT * FROM default AS d unnest reviews as r WHERE r.author LIKE 'M%' and r.ratings.Cleanliness > 1 " \
                    "AND d.free_parking = True AND d.country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'M%' and r.ratings.Cleanliness > 1 " \
                    "AND d.free_parking = True AND d.country is not null"
        else:
            query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END " \
                    "AND free_parking = True AND country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' " \
                            "and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.compare_against_primary(query, primary_query)

    '''Test what happens when you have an array with ALL/DISTINCT flatten_keys(v1,v2) but query contains any (v2,1)'''
    def test_flatten_array_ordering(self):
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)"
        if self.use_unnest:
            query = "SELECT SUM( r.ratings.Cleanliness) FROM default AS d unnest reviews as r WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False AND d.email is not missing GROUP BY r.rating.Cleanliness,r.author"
            primary_query = "SELECT SUM( r.ratings.Cleanliness) FROM default AS d  USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False AND d.email is not missing GROUP BY r.rating.Cleanliness,r.author"
        else:
            query = "SELECT free_parking, email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author  LIKE 'N%' and r.ratings.Cleanliness = 3 END AND free_parking = True AND email is not missing GROUP BY free_parking, email"
            primary_query = "SELECT free_parking, email  FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author  LIKE 'N%'  and r.ratings.Cleanliness = 3 END AND free_parking = True AND email is not missing GROUP BY free_parking,email"
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    '''Teset partial index'''
    def test_flatten_partial_index(self):
        self.run_cbq_query(
            query="create index idx1 on default(ALL ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking) where free_parking = True")
        if self.use_unnest:
            query = "SELECT * FROM default AS d unnest reviews as r WHERE r.author LIKE 'M%' and r.ratings.Cleanliness > 1 " \
                    "AND d.free_parking = True AND d.country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'M%' and r.ratings.Cleanliness > 1 " \
                    "AND d.free_parking = True AND d.country is not null"
            query2 = "SELECT * FROM default AS d unnest reviews as r WHERE r.author LIKE 'M%' and r.ratings.Cleanliness > 1 AND d.free_parking = False"

        else:
            query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 " \
                    "END AND free_parking = True AND country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author " \
                            "LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND country is not null"
            query2 = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = False"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.compare_against_primary(query, primary_query)

        # Ensure partial index is not selected when it does not apply
        explain_results = self.run_cbq_query(query="EXPLAIN " + query2)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))

    '''Test out using when clause in array index key, then use queries that fall inside and outside the when clause, equality and comparators'''
    def test_flatten_when(self):
        self.run_cbq_query(query="create index idx1 on default(country, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews when r.ratings.Cleanliness < 3 END, email, free_parking)")
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
        self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews when r.ratings.Cleanliness < 3 END, country, email, free_parking)")
        if self.use_unnest:
            query = "SELECT * FROM default AS d unnest reviews as r WHERE CONTAINS(r.author,'M') and " \
                    "r.ratings.Cleanliness < 2 AND d.free_parking = True AND d.country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE CONTAINS(r.author,'M') and " \
                            "r.ratings.Cleanliness < 2 AND d.free_parking = True AND d.country is not null"

            query2 = "EXPLAIN SELECT * FROM default AS d unnest reviews as r WHERE CONTAINS(r.author,'M') and r.ratings.Cleanliness > 3 AND d.free_parking = True AND d.country is not null"
            query3 = "EXPLAIN SELECT * FROM default AS d unnest reviews as r WHERE CONTAINS(r.author,'M') and r.ratings.Cleanliness = 4 AND d.free_parking = True AND d.country is not null"

        else:
            query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness < 2 END AND free_parking = True AND country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness < 2 END AND free_parking = True AND country is not null"
            query2 = "EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness > 3 END AND free_parking = True AND country is not null"
            query3 = "EXPLAIN SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES CONTAINS(r.author,'M') and r.ratings.Cleanliness = 4 END AND free_parking = True AND country is not null"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))

        self.compare_against_primary(query, primary_query)

        # These two queries should not pick up the index
        explain_results = self.run_cbq_query(query=query2)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))

        explain_results = self.run_cbq_query(query=query3)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))

    def test_flatten_advise(self):
        if self.use_unnest:
            query = "SELECT * FROM default AS d unnest reviews as r WHERE r.author LIKE 'M%' AND r.ratings.Cleanliness > 1 AND d.free_parking = TRUE AND d.country IS NOT NULL"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'M%' AND r.ratings.Cleanliness > 1 AND d.free_parking = TRUE AND d.country IS NOT NULL"
        else:
            query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness > 1 END AND free_parking = TRUE AND country IS NOT NULL"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness > 1 END AND free_parking = TRUE AND country IS NOT NULL"

        advise_results = self.run_cbq_query(query="ADVISE " + query)
        self.assertTrue("FLATTEN_KEYS" in str(advise_results), "Advisor should've advised an index with flatten_keys but it did not, advisor output {0}".format(advise_results))
        create_idx_statement = advise_results['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement']
        idx_name = advise_results['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement'].split("INDEX")[1].split("ON")[0].strip()
        self.run_cbq_query(query=create_idx_statement)
        explain_results = self.run_cbq_query(
            query="EXPLAIN " + query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == idx_name,
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.compare_against_primary(query, primary_query)

    def test_flatten_advise_equivalent(self):
        self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author) FOR r IN reviews END,email,country)")
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

    def test_flatten_unnest_any(self):
        self.run_cbq_query(query="CREATE INDEX idx1 ON default(public_likes, DISTINCT ARRAY flatten_keys(r.ratings.Cleanliness,r.ratings.Rooms,r.author) FOR r IN reviews END, free_parking)")
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
        self.run_cbq_query(query='DROP INDEX `default`.`#primary` IF EXISTS USING GSI')
        self.run_cbq_query(
            query="CREATE INDEX idx1 ON default(public_likes, DISTINCT ARRAY flatten_keys(r.ratings.Cleanliness,r.ratings.Rooms,r.author) FOR r IN reviews END, free_parking)")
        self.run_cbq_query(query='CREATE PRIMARY INDEX ON default')

        query = "SELECT * FROM default AS d UNNEST d.public_likes p WHERE p " \
                "LIKE 'R%' AND ANY r IN d.reviews SATISFIES r.author LIKE '%m' AND " \
                "(r.ratings.Cleanliness >= 1 OR r.ratings.Rooms <= 3) END OR d.free_parking = TRUE"
        primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) UNNEST d.public_likes p WHERE p " \
                        "LIKE 'R%' AND ANY r IN d.reviews SATISFIES r.author LIKE '%m' AND " \
                        "(r.ratings.Cleanliness >= 1 OR r.ratings.Rooms <= 3) END OR d.free_parking = TRUE"
        self.sleep(30)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue( 'idx1' in str(explain_results),
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.compare_against_primary(query, primary_query)

    def test_flatten_prepared(self):
        self.run_cbq_query(query="delete from system:prepareds")
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.date,r.ratings.Overall) FOR r IN reviews END, email)"
        if self.use_unnest:
            prepare_query = "PREPARE p1 AS SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
            query = "SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
            primary_query = "SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
        else:
            prepare_query = "PREPARE p1 AS SELECT * FROM default d WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
            query = "SELECT * FROM default d WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
            primary_query = "SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
        self.run_cbq_query(query=create_query)
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(explain_results))

        prepare_results = self.run_cbq_query(query=prepare_query)
        self.assertTrue(prepare_results['status'] == "success")
        time.sleep(5)
        node_prepared_name = self.run_cbq_query(query='select * from system:prepareds where name = "p1"')
        prepareds = self.run_cbq_query(query="select * from system:prepareds")
        self.assertTrue(node_prepared_name['metrics']['resultCount'] == 2, "There should be two enteries for p1 check prepareds {0}".format(prepareds))
        execute_results = self.run_cbq_query(query="execute p1")
        expected_results = self.run_cbq_query(query=primary_query)
        diffs = DeepDiff(execute_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_cte(self):
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.date,r.ratings.Overall) FOR r IN reviews END, email)"
        if self.use_unnest:
            query = "with emails as (SELECT raw d.email FROM default d UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing) " \
                    "select d.email from default d UNNEST d.reviews as r where r.ratings.Overall BETWEEN 1 and 3 and r.date is not missing AND d.email in emails order by d.email limit 10"
            primary_query = "with emails as (SELECT raw d.email FROM default d UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing) " \
                    "select d.email from default d USE INDEX (`#primary`) UNNEST d.reviews as r where r.ratings.Overall BETWEEN 1 and 3 and r.date is not missing AND d.email in emails order by d.email limit 10"
        else:
            query = "WITH emails as (SELECT raw email FROM default d WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END) " \
                    "SELECT d.email FROM default d WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END and email in emails order by d.email limit 10"
            primary_query = "WITH emails as (SELECT raw email FROM default d WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END) " \
                            "SELECT d.email FROM default d USE INDEX (`#primary`) WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END and email in emails order by d.email limit 10"
        self.run_cbq_query(query=create_query)
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    def test_flatten_cte_conflict(self):
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.date,r.ratings.Overall) FOR r IN reviews END, email)"
        if self.use_unnest:
            query = "with emails as (SELECT raw d.email FROM default d) " \
                    "select * from default d UNNEST d.reviews as emails where emails.ratings.Overall BETWEEN 1 and 3 and emails.date is not missing AND d.email in emails"

        else:
            query = "WITH emails as (SELECT raw email FROM default d ) " \
                    "SELECT * FROM default d WHERE ANY emails in reviews SATISFIES emails.ratings.Overall BETWEEN 1 and 3 AND emails.date is not missing END and email in emails"

        self.run_cbq_query(query=create_query)
        try:
            self.run_cbq_query(query=query)
            self.fail()
        except Exception as e:
            if self.use_unnest:
                self.assertTrue("Duplicate UNNEST alias 'emails'" in str(e), "The error is incorrect check the error {0}".format(str(e)))
            else:
                self.assertTrue("Duplicate variable: 'emails'" in str(e), "The error is incorrect check the error {0}".format(str(e)))


    def test_flatten_alias_keyspace_collision(self):
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.date,r.ratings.Overall) FOR r IN reviews END, email)"
        if self.use_unnest:
            query = "SELECT * FROM default d UNNEST d.reviews AS d WHERE d.ratings.Overall BETWEEN 1 and 3 AND d.date is not missing"
            primary_query = "SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS d WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
        else:
            query = "SELECT email FROM default d WHERE ANY d in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
            primary_query = "SELECT email FROM default d USE INDEX (`#primary`) WHERE ANY d in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
        self.run_cbq_query(query=create_query)
        try:
            self.run_cbq_query(query=query)
            self.fail()
        except Exception as e:
            if self.use_unnest:
                self.assertTrue("Duplicate UNNEST alias 'd'" in str(e), "The error is incorrect check the error {0}".format(str(e)))
            else:
                self.assertTrue("Duplicate variable" in str(e), "The error is incorrect check the error {0}".format(str(e)))

    '''We expect the query to pick up the array with the keys flattened'''
    def test_flatten_index_selection(self):
        self.run_cbq_query(query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking,country)")
        self.run_cbq_query(query="create index idx2 on default(country, DISTINCT ARRAY [r.author,r.ratings.Cleanliness] FOR r IN reviews END, email, free_parking)")
        if self.use_unnest:
            query = "SELECT * FROM default AS d unnest reviews as r WHERE r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 AND d.free_parking = TRUE AND d.country IS NOT NULL"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 AND d.free_parking = TRUE AND d.country IS NOT NULL"
        else:
            query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END AND d.free_parking = TRUE AND d.country IS NOT NULL"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END AND d.free_parking = TRUE AND d.country IS NOT NULL"
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == "idx1",
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.compare_against_primary(query, primary_query)

    '''Test array index that contains some of the fields indexed and some fields that are in the array and not in the index'''
    def test_flatten_partial_elements(self):
        self.run_cbq_query(query = "CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query = "EXPLAIN SELECT * FROM default d WHERE ANY r in d.reviews satisfies r.ratings.Rooms > 1 AND r.ratings.Cleanliness < 5 AND CONTAINS(r.author,'F') END")
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        query_results = self.run_cbq_query(query = "SELECT * FROM default d WHERE ANY r in d.reviews satisfies r.ratings.Rooms > 1 AND r.ratings.Cleanliness < 5 AND CONTAINS(r.author,'F') END")
        expected_results = self.run_cbq_query(query = "SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY r in d.reviews satisfies r.ratings.Rooms > 1 AND r.ratings.Cleanliness < 5 AND CONTAINS(r.author,'F') END")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    ''''Test array index that contains some of the fields indexed and some fields that are in the array and not in the index'''
    def test_flatten_unnest_partial_elements(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Cleanliness < 5 AND CONTAINS(r.author,'F')")
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Cleanliness < 5 AND CONTAINS(r.author,'F')")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Cleanliness < 5 AND CONTAINS(r.author,'F')")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_named_params(self):
        self.run_cbq_query(
            query="create index idx1 on default(ALL ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking) where free_parking = True")
        if self.use_unnest:
            query = "SELECT * FROM default AS d unnest reviews as r WHERE r.author LIKE $author_name and r.ratings.Cleanliness > $cleaning_score " \
                    "AND d.free_parking = True AND d.country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE $author_name and r.ratings.Cleanliness > $cleaning_score " \
                    "AND d.free_parking = True AND d.country is not null"
            query2 = "SELECT * FROM default AS d unnest reviews as r WHERE r.author LIKE $author_name and r.ratings.Cleanliness > $cleaning_score AND d.free_parking = False"

        else:
            query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE $author_name and r.ratings.Cleanliness > $cleaning_score " \
                    "END AND free_parking = True AND country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author " \
                            "LIKE $author_name and r.ratings.Cleanliness > $cleaning_score END AND free_parking = True AND country is not null"
            query2 = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE $author_name and r.ratings.Cleanliness > $cleaning_score END AND free_parking = False"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query, query_params={'$author_name': "M%", "$cleaning_score": 1})
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query=query, query_params={'$author_name': "M%", "$cleaning_score": 1})
        expected_results = self.run_cbq_query(query=primary_query, query_params={'$author_name': "M%", "$cleaning_score": 1})
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        # Ensure partial index is not selected when it does not apply
        explain_results = self.run_cbq_query(query="EXPLAIN " + query2, query_params={'$author_name': "M%", "$cleaning_score": 1})
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))

    def test_flatten_ansi_joins(self):
        self.load_travel_sample()
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Overall) for r in reviews END,email)"

        query = "select * from `travel-sample`.inventory.hotel t INNER JOIN default d ON (ANY r in d.reviews satisfies " \
                "r.author like 'M%' and r.ratings.Cleanliness > 1 END AND t.country = d.country AND ANY s in t.reviews " \
                "SATISFIES s.author like 'M%' and s.ratings.Cleanliness > 1 END) "

        self.run_cbq_query(query=create_query)

        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("def_inventory_hotel_primary" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(explain_results))

        ansi_results = self.run_cbq_query(query=query)
        self.assertTrue(ansi_results['status'] == 'success',
                        "Merge did not occur successfully! {0}".format(ansi_results))

    def test_flatten_positional_params(self):
        self.run_cbq_query(
            query="create index idx1 on default(ALL ARRAY FLATTEN_KEYS(r.ratings.Overall,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking) where free_parking = True")
        if self.use_unnest:
            query = "SELECT * FROM default AS d unnest reviews as r WHERE r.ratings.Overall < $1 and r.ratings.Cleanliness > $2 " \
                    "AND d.free_parking = True AND d.country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.ratings.Overall < $1  and r.ratings.Cleanliness > $2 " \
                    "AND d.free_parking = True AND d.country is not null"
            query2 = "SELECT * FROM default AS d unnest reviews as r WHERE r.ratings.Overall < $1  and r.ratings.Cleanliness > $2 AND d.free_parking = False"

        else:
            query = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.ratings.Overall < $1  and r.ratings.Cleanliness > $2 " \
                    "END AND free_parking = True AND country is not null"
            primary_query = "SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.ratings.Overall < $1 " \
                            " and r.ratings.Cleanliness > $2 END AND free_parking = True AND country is not null"
            query2 = "SELECT * FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.ratings.Overall < $1  and r.ratings.Cleanliness > $2 END AND free_parking = False"

        args= 'args=[5,1]'

        curl_output = self.shell.execute_command("{0} -u Administrator:password {1}:{2}/query/service -d 'statement=EXPLAIN {3}&{4}'".format(self.curl_path, self.master.ip, self.n1ql_port, query, args))
        explain_results = self.convert_list_to_json(curl_output[0])

        # Ensure the query is actually using the flatten index instead of primary
        #explain_results = self.run_cbq_query(query="EXPLAIN " + query, query_params={'args': ["M%", 1]})
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        curl_output = self.shell.execute_command("{0} -u Administrator:password {1}:{2}/query/service -d 'statement={3}&{4}'".format(self.curl_path, self.master.ip, self.n1ql_port, query, args))
        query_results = self.convert_list_to_json(curl_output[0])

        curl_output = self.shell.execute_command("{0} -u Administrator:password {1}:{2}/query/service -d 'statement={3}&{4}'".format(self.curl_path, self.master.ip, self.n1ql_port, primary_query, args))
        expected_results = self.convert_list_to_json(curl_output[0])


        #query_results = self.run_cbq_query(query=query ,query_params={'args': ["M%", 1]})
        #expected_results = self.run_cbq_query(query=primary_query, query_params={'args': ["M%", 1]})
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

        # Ensure partial index is not selected when it does not apply
        curl_output = self.shell.execute_command("{0} -u Administrator:password {1}:{2}/query/service -d statement='EXPLAIN {3}&{4}'".format(self.curl_path, self.master.ip, self.n1ql_port, query2, args))
        explain_results = self.convert_list_to_json(curl_output[0])
        #explain_results = self.run_cbq_query(query="EXPLAIN " + query2, query_params={'args': ["M%", 1]})
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))

    def test_flatten_multiple_any(self):
        self.run_cbq_query(query='UPSERT INTO default VALUES("doc2", {"contacts":[{"type":"mobile", "phone":"123-45-6789"},'
                                 '{"type":"email","id":"abc@gmail.com"}]});')
        self.run_cbq_query(query='CREATE INDEX flatten1 ON default(DISTINCT ARRAY FLATTEN_KEYS(v1.type,v1.phone) FOR v1 IN contacts END);')
        actual_results = self.run_cbq_query('SELECT META(d).id FROM default AS d WHERE ANY v1 IN contacts '
                                            'SATISFIES v1.type ="mobile" AND v1.phone = "123-45-6789" END '
                                            'AND ANY v2 IN contacts SATISFIES v2.type ="email" AND v2.id = "abc@gmail.com" END;')
        expected_results = [{"id": "doc2"}]
        diffs = DeepDiff(actual_results['results'], expected_results)
        if diffs:
            self.assertTrue(False, diffs)
    ##############################################################################################
    #
    #   Query Context
    ##############################################################################################

    def test_flatten_query_context_namespace_bucket_scope(self):
        self.load_travel_sample()
        create_query = "create index idx1 on hotel(DISTINCT ARRAY FLATTEN_KEYS(r.date,r.ratings.Overall) FOR r IN reviews END, email)"
        if self.use_unnest:
            query = "SELECT * FROM `travel-sample`.inventory.hotel d UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
            primary_query = "SELECT * FROM `travel-sample`.inventory.hotel d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
        else:
            query = "SELECT * FROM `travel-sample`.inventory.hotel d WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
            primary_query = "SELECT * FROM `travel-sample`.inventory.hotel d USE INDEX (`#primary`) WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
        self.run_cbq_query(query=create_query, query_context='default:`travel-sample`.inventory')
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(explain_results))
        self.run_cbq_query(query="CREATE PRIMARY INDEX ON `travel-sample`.inventory.hotel")

        self.compare_against_primary(query, primary_query)

    def test_flatten_query_context_semicolon_bucket_scope(self):
        self.load_travel_sample()
        create_query = "create index idx1 on hotel(DISTINCT ARRAY FLATTEN_KEYS(r.date,r.ratings.Overall) FOR r IN reviews END, email)"
        if self.use_unnest:
            query = "SELECT * FROM `travel-sample`.inventory.hotel d UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
            primary_query = "SELECT * FROM `travel-sample`.inventory.hotel d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
        else:
            query = "SELECT * FROM `travel-sample`.inventory.hotel d WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
            primary_query = "SELECT * FROM `travel-sample`.inventory.hotel d USE INDEX (`#primary`) WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
        self.run_cbq_query(query=create_query, query_context=':`travel-sample`.inventory')
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(explain_results))
        self.run_cbq_query(query="CREATE PRIMARY INDEX ON `travel-sample`.inventory.hotel")

        self.compare_against_primary(query, primary_query)

    def test_flatten_query_context_namespace(self):
        self.load_travel_sample()
        create_query = "create index idx1 on `travel-sample`.inventory.hotel(DISTINCT ARRAY FLATTEN_KEYS(r.date,r.ratings.Overall) FOR r IN reviews END, email) "
        if self.use_unnest:
            query = "SELECT * FROM `travel-sample`.inventory.hotel d UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
            primary_query = "SELECT * FROM `travel-sample`.inventory.hotel d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
        else:
            query = "SELECT * FROM `travel-sample`.inventory.hotel d WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
            primary_query = "SELECT * FROM `travel-sample`.inventory.hotel d USE INDEX (`#primary`) WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
        self.run_cbq_query(query=create_query, query_context='default:')
        explain_results = self.run_cbq_query(query="EXPLAIN " + query, query_context='default:')
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(explain_results))
        self.run_cbq_query(query="CREATE PRIMARY INDEX ON `travel-sample`.inventory.hotel")

        self.compare_against_primary(query, primary_query)

    def test_flatten_query_context(self):
        self.load_travel_sample()
        create_query = "create index idx1 on hotel(DISTINCT ARRAY FLATTEN_KEYS(r.date,r.ratings.Overall) FOR r IN reviews END, email)"
        if self.use_unnest:
            query = "SELECT * FROM hotel d UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
            primary_query = "SELECT * FROM hotel d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing"
        else:
            query = "SELECT * FROM hotel d WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
            primary_query = "SELECT * FROM hotel d USE INDEX (`#primary`) WHERE ANY r in reviews SATISFIES r.ratings.Overall BETWEEN 1 and 3 AND r.date is not missing END"
        self.run_cbq_query(query=create_query, query_context='`travel-sample`.inventory')
        explain_results = self.run_cbq_query(query="EXPLAIN " + query, query_context='`travel-sample`.inventory')
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(explain_results))
        self.run_cbq_query(query="CREATE PRIMARY INDEX ON `travel-sample`.inventory.hotel")

        query_results = self.run_cbq_query(query=query, query_context='`travel-sample`.inventory')
        expected_results = self.run_cbq_query(query=primary_query, query_context='`travel-sample`.inventory')
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)
    ##############################################################################################
    #
    #   Partitioning
    ##############################################################################################

    '''Index has partitions'''
    def test_flatten_partitioning(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end))) PARTITION BY HASH (META().id)")
        if self.use_unnest:
            query = "SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F')"
            primary_query = "SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F')"
        else:
            query = "SELECT * FROM default d WHERE ANY r in reviews SATISFIES r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') END"
            primary_query = "SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY r in reviews SATISFIES r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') END"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query=query)
        expected_results = self.run_cbq_query(query=primary_query)
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Index is partitioned by a non array field in the index'''
    def test_flatten_partitioning_field(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`(email, (distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end))) PARTITION BY HASH (email)")
        query = "SELECT * FROM default d WHERE ANY r in reivews SATISFIES r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') END and d.email LIKE 'I%'"
        primary_query = "SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY r in reivews SATISFIES r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') END and d.email LIKE 'I%'"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query=query)
        expected_results = self.run_cbq_query(query=primary_query)
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Index is partitioned by a non array field in the index'''
    def test_flatten_partitioning_field_array_leading(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)), email) PARTITION BY HASH (email)")
        if self.use_unnest:
            query = "SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') and d.email LIKE 'I%'"
            primary_query = "SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') and d.email LIKE 'I%'"
        else:
            query = "SELECT * FROM default d WHERE ANY r in reviews SATISFIES r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') END and d.email LIKE 'I%'"
            primary_query = "SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY r in reviews SATISFIES r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') END and d.email LIKE 'I%'"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        query_results = self.run_cbq_query(query=query)
        expected_results = self.run_cbq_query(query=primary_query)
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Index is partitioned by an array field in the index'''
    def test_flatten_partitioning_array_field(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end))) PARTITION BY HASH (reviews)")
        if self.use_unnest:
            query = "SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') and d.email LIKE 'I%'"
            primary_query = "SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') and d.email LIKE 'I%'"
        else:
            query = "SELECT * FROM default d WHERE ANY r in reviews SATISFIES r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') END and d.email LIKE 'I%'"
            primary_query = "SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY r in reviews SATISFIES r.ratings.Cleanliness BETWEEN 1 and 3 AND CONTAINS(r.author,'F') END and d.email LIKE 'I%'"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query=query)
        expected_results = self.run_cbq_query(query=primary_query)
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    ##############################################################################################
    #
    #   PUSHDOWN
    ##############################################################################################
    '''Pushdown will work on leading key of index'''
    def test_flatten_groupby_pushdown_leading(self):
        create_query = "create index idx1 on default(email, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END,free_parking)"

        self.run_cbq_query(query=create_query)
        query = "SELECT email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND email is not missing GROUP BY email"
        primary_query = "SELECT email FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND email is not missing GROUP BY email"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        self.assertTrue("covers" in str(explain_results),
                        "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue("index_group_aggs" in str(explain_results), "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    '''Pushdown will work on leading key of index'''
    def test_flatten_groupby_pushdown_array_leading(self):
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)"
        query ="SELECT r.author  FROM default AS d unnest reviews as r WHERE r.author LIKE 'M%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = True AND d.email is not missing GROUP BY r.author"
        primary_query = "SELECT r.author  FROM default AS d  USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'M%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = True AND d.email is not missing GROUP BY r.author;"

        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        self.assertTrue("covers" in str(explain_results),
                        "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue("index_group_aggs" in str(explain_results), "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)


    '''Pushdown will work on groupby clause on query that uses diff order than index key order'''
    def test_flatten_groupby_pushdown_order_unnest(self):
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)"
        query = "SELECT r.author,d.email  FROM default AS d unnest reviews as r WHERE r.author LIKE 'M%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = True AND d.email is not missing GROUP BY d.email,r.author"
        primary_query = "SELECT r.author, d.email FROM default AS d  USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'M%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = True AND d.email is not missing GROUP BY d.email,r.author;"

        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(explain_results))
        self.assertTrue("covers" in str(explain_results),
                        "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue("index_group_aggs" in str(explain_results),
                        "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    def test_flatten_groupby_pushdown_order(self):
        create_query = "create index idx1 on default(email, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END,free_parking)"

        self.run_cbq_query(query=create_query)
        query = "SELECT free_parking,email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND email is not missing GROUP BY free_parking,email"
        primary_query = "SELECT free_parking,email FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND email is not missing GROUP BY free_parking,email"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        self.assertTrue("covers" in str(explain_results),
                        "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue("index_group_aggs" in str(explain_results), "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    '''Pushdown will work on non leading keys '''
    def test_flatten_groupby_pushdown_nonleading(self):
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)"
        self.run_cbq_query(query=create_query)
        if self.use_unnest:
            query = "SELECT d.email FROM default AS d unnest reviews as r WHERE r.author LIKE 'M%' and r.ratings.Cleanliness > 1 AND d.free_parking = True GROUP BY d.email"
            primary_query = "SELECT d.email FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'M%' and r.ratings.Cleanliness > 1 AND d.free_parking = True GROUP BY d.email"
        else:
            query = "SELECT email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END " \
                    "AND free_parking = True GROUP BY email"
            primary_query = "SELECT email FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' " \
                            "and r.ratings.Cleanliness > 1 END AND free_parking = True GROUP BY email"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        self.assertTrue("covers" in str(explain_results),
                        "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue("index_group_aggs" in str(explain_results), "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    '''Pushdown with limit and offset works with group by on leading keys'''
    def test_flatten_groupby_pushdown_limit_offset(self):
        create_query = "create index idx1 on default(email, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END,free_parking)"
        query = "SELECT email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND email is not missing GROUP BY email ORDER BY email LIMIT 10 OFFSET 5"
        primary_query = "SELECT email FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True AND email is not missing GROUP BY email ORDER BY email LIMIT 10 OFFSET 5"

        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))

        self.assertTrue("covers" in str(explain_results),
                        "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue("index_group_aggs" in str(explain_results),
                        "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

    '''Aggregate pushdown can work with no groupby clause'''
    def test_flatten_aggregate_pushdown_no_group(self):
        self.load_travel_sample()
        create_query = "create index idx1 on `travel-sample`.inventory.hotel(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)"
        if self.use_unnest:
            query = "SELECT SUM(r.ratings.Cleanliness) FROM `travel-sample`.inventory.hotel AS d unnest reviews as r WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False AND d.email is not missing"
            primary_query = "SELECT SUM(r.ratings.Cleanliness) FROM `travel-sample`.inventory.hotel AS d  USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False AND d.email is not missing"
        else:
            query = "SELECT COUNT(email), SUM(free_parking)  FROM `travel-sample`.inventory.hotel AS d WHERE ANY r IN d.reviews SATISFIES r.author = 'Nella Ratke' and r.ratings.Cleanliness = 3 END AND free_parking = True AND email is not missing"
            primary_query = "SELECT COUNT(email), SUM(free_parking)  FROM `travel-sample`.inventory.hotel AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author = 'Nella Ratke' and r.ratings.Cleanliness = 3 END AND free_parking = True AND email is not missing"
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.run_cbq_query(query="CREATE PRIMARY INDEX ON `travel-sample`.inventory.hotel")
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        if self.use_unnest:
            self.assertTrue("covers" not in str(explain_results),
                            "The index is not covering, it should be. Check plan {0}".format(explain_results))
            self.assertTrue("index_group_aggs" not in str(explain_results), "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))
        else:
            self.assertTrue("covers" in str(explain_results),
                            "The index is not covering, it should be. Check plan {0}".format(explain_results))
            self.assertTrue("index_group_aggs" in str(explain_results), "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    '''Aggregate pushdown should work with letting and having'''
    def test_flatten_aggregate_pushdown_letting_having(self):
        self.load_travel_sample()
        create_query = "create index idx1 on `travel-sample`.inventory.hotel(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)"
        if self.use_unnest:
            query = "SELECT SUM(r.ratings.Cleanliness) FROM `travel-sample`.inventory.hotel AS d unnest reviews as r " \
                    "WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False " \
                    "AND d.email is not missing GROUP BY r.ratings.Cleanliness LETTING min_cleanliness = 5 HAVING COUNT(r.ratings.Cleanliness) > min_cleanliness"
            primary_query = "SELECT SUM(r.ratings.Cleanliness) FROM `travel-sample`.inventory.hotel AS d  USE INDEX (`#primary`) unnest reviews as r " \
                            "WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND " \
                            "d.free_parking = False AND d.email is not missing GROUP BY r.ratings.Cleanliness LETTING min_cleanliness = 5 HAVING COUNT(r.ratings.Cleanliness) > min_cleanliness"
        else:
            query = "SELECT MAX(email), MIN(email), AVG( free_parking)  FROM `travel-sample`.inventory.hotel AS d WHERE ANY r IN d.reviews SATISFIES r.author = 'Nella Ratke' and r.ratings.Cleanliness = 3 END AND d.free_parking = False AND d.email is not missing GROUP BY d.email, d.free_parking LETTING avg_parking = False HAVING d.free_parking = avg_parking"
            primary_query = "SELECT MAX(email), MIN(email), AVG( free_parking)  FROM `travel-sample`.inventory.hotel AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author = 'Nella Ratke' and r.ratings.Cleanliness = 3 END AND d.free_parking = False AND d.email is not missing GROUP BY d.email, d.free_parking LETTING avg_parking = False HAVING d.free_parking = avg_parking"
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.run_cbq_query(query="CREATE PRIMARY INDEX ON `travel-sample`.inventory.hotel")
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(explain_results))
        if self.use_unnest:
            self.assertTrue("covers" not in str(explain_results),
                            "The index is not covering, it should be. Check plan {0}".format(explain_results))
            self.assertTrue("index_group_aggs" not in str(explain_results),
                            "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))
        else:
            self.assertTrue("covers" in str(explain_results),
                            "The index is not covering, it should be. Check plan {0}".format(explain_results))
            self.assertTrue("index_group_aggs" in str(explain_results),
                            "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    def test_flatten_aggregate_pushdown_distinct(self):
        self.load_travel_sample()
        create_query = "create index idx1 on `travel-sample`.inventory.hotel(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)"
        if self.use_unnest:
            query = "SELECT SUM( DISTINCT r.ratings.Cleanliness) FROM `travel-sample`.inventory.hotel AS d unnest reviews as r WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False AND d.email is not missing GROUP BY r.author"
            primary_query = "SELECT SUM( DISTINCT r.ratings.Cleanliness) FROM `travel-sample`.inventory.hotel AS d  USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False AND d.email is not missing GROUP BY r.author"
        else:
            query = "SELECT COUNT(DISTINCT email), SUM( DISTINCT free_parking)  FROM `travel-sample`.inventory.hotel AS d WHERE ANY r IN d.reviews SATISFIES r.author = 'Nella Ratke' and r.ratings.Cleanliness = 3 END AND free_parking = False AND email is not missing GROUP BY email, free_parking"
            primary_query = "SELECT COUNT(DISTINCT email), SUM(DISTINCT free_parking)  FROM `travel-sample`.inventory.hotel AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author = 'Nella Ratke' and r.ratings.Cleanliness = 3 END AND free_parking = False AND email is not missing GROUP BY email, free_parking"
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.run_cbq_query(query="CREATE PRIMARY INDEX ON `travel-sample`.inventory.hotel")
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        self.assertTrue("covers" in str(explain_results),
                        "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue("index_group_aggs" in str(explain_results), "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    def test_flatten_aggregate_avg_unnest(self):
        self.load_travel_sample()
        create_query = "create index idx1 on `travel-sample`.inventory.hotel(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)"
        query = "SELECT AVG( r.ratings.Cleanliness) FROM `travel-sample`.inventory.hotel AS d unnest reviews as r WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False AND d.email is not missing GROUP BY r.ratings.Cleanliness"
        primary_query = "SELECT AVG( r.ratings.Cleanliness) FROM `travel-sample`.inventory.hotel AS d  USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False AND d.email is not missing GROUP BY r.ratings.Cleanliness"
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.run_cbq_query(query="CREATE PRIMARY INDEX ON `travel-sample`.inventory.hotel")
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        self.assertTrue("covers" in str(explain_results),
                        "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue("index_group_aggs" in str(explain_results), "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    def test_flatten_aggregate_min_max_unnest(self):
        self.load_travel_sample()
        create_query = "create index idx1 on `travel-sample`.inventory.hotel(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)"
        query = "SELECT MIN(r.ratings.Cleanliness), MAX(r.ratings.Cleanliness) FROM `travel-sample`.inventory.hotel AS d unnest reviews as r WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False AND d.email is not missing GROUP BY r.ratings.Cleanliness"
        primary_query = "SELECT MIN(r.ratings.Cleanliness), MAX(r.ratings.Cleanliness) FROM `travel-sample`.inventory.hotel AS d  USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE 'N%' and r.author is not missing and r.ratings.Cleanliness > 1 AND d.free_parking = False AND d.email is not missing GROUP BY r.ratings.Cleanliness"
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.run_cbq_query(query="CREATE PRIMARY INDEX ON `travel-sample`.inventory.hotel")
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        self.assertTrue("covers" in str(explain_results),
                        "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue("index_group_aggs" in str(explain_results), "Index should be pushing down but it isn't. Please check the plan {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    ##############################################################################################
    #
    #   GROUPBY/ORDERBY/LIMIT/OFFSET
    ##############################################################################################

    def test_flatten_groupby_non_array(self):
        self.run_cbq_query(
            query="CREATE INDEX idx1 ON default(DISTINCT ARRAY flatten_keys(r.ratings.Cleanliness,r.ratings.Rooms,r.author) FOR r IN reviews END, email)")
        if self.use_unnest:
            query = "SELECT d.email FROM default AS d unnest reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) GROUP BY d.email ORDER BY d.email"
            primary_query = "SELECT d.email FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) GROUP BY d.email ORDER BY d.email"
        else:
            query = "SELECT d.email FROM default AS d WHERE ANY r IN d.reviews " \
                    "SATISFIES r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END " \
                    "GROUP BY d.email ORDER BY d.email"
            primary_query = "SELECT d.email FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews " \
                            "SATISFIES r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END " \
                            "GROUP BY d.email ORDER BY d.email"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        query_results = self.run_cbq_query(query=query)
        expected_results = self.run_cbq_query(query=primary_query)
        diffs = DeepDiff(query_results['results'], expected_results['results'])
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_groupby_array(self):
        self.run_cbq_query(
            query="CREATE INDEX idx1 ON default(DISTINCT ARRAY flatten_keys(r.ratings.Cleanliness,r.ratings.Rooms,r.author) FOR r IN reviews END, country)")
        if self.use_unnest:
            query = "SELECT r.author FROM default AS d unnest reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) GROUP BY r.author ORDER BY r.author"
            primary_query = "SELECT r.author FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) GROUP BY r.author ORDER BY r.author"
        else:
            query = "SELECT d.reviews FROM default AS d WHERE ANY r IN d.reviews " \
                    "SATISFIES r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END " \
                    "GROUP BY d.reviews ORDER BY d.reviews"
            primary_query = "SELECT d.reviews FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews " \
                            "SATISFIES r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END " \
                            "GROUP BY d.reviews ORDER BY d.reviews"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        query_results = self.run_cbq_query(query=query)
        expected_results = self.run_cbq_query(query=primary_query)
        diffs = DeepDiff(query_results['results'], expected_results['results'])
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_groupby_limit(self):
        self.run_cbq_query(
            query="CREATE INDEX idx1 ON default(DISTINCT ARRAY flatten_keys(r.ratings.Cleanliness,r.ratings.Rooms,r.author) FOR r IN reviews END, country)")
        if self.use_unnest:
            query = "SELECT d.country, count(r.author) FROM default AS d unnest reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) GROUP BY r.author,d.country ORDER BY r.author,d.country LIMIT 10"
            primary_query = "SELECT d.country, count(r.author) FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) GROUP BY r.author,d.country ORDER BY r.author,d.country LIMIT 10"
        else:
            query = "SELECT d.country, COUNT(d.reviews) FROM default AS d WHERE ANY r IN d.reviews " \
                    "SATISFIES r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END " \
                    "GROUP BY d.reviews,d.country ORDER BY d.reviews,d.country LIMIT 10"
            primary_query = "SELECT d.country, COUNT(d.reviews) FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews " \
                            "SATISFIES r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END " \
                            "GROUP BY d.reviews,d.country ORDER BY d.reviews,d.country LIMIT 10"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        query_results = self.run_cbq_query(query=query)
        expected_results = self.run_cbq_query(query=primary_query)
        diffs = DeepDiff(query_results['results'], expected_results['results'])
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_groupby_limit_offset(self):
        self.run_cbq_query(
            query="CREATE INDEX idx1 ON default(DISTINCT ARRAY flatten_keys(r.ratings.Cleanliness,r.ratings.Rooms,r.author) FOR r IN reviews END, country)")
        if self.use_unnest:
            query = "SELECT d.country, count(r.author) FROM default AS d unnest reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) GROUP BY r.author,d.country ORDER BY r.author,d.country LIMIT 10 OFFSET 5"
            primary_query = "SELECT d.country, count(r.author) FROM default AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) GROUP BY r.author,d.country ORDER BY r.author,d.country LIMIT 10 OFFSET 5"
        else:
            query = "SELECT d.country, COUNT(d.reviews) FROM default AS d WHERE ANY r IN d.reviews " \
                    "SATISFIES r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END " \
                    "GROUP BY d.reviews,d.country ORDER BY d.reviews,d.country LIMIT 10 OFFSET 5"
            primary_query = "SELECT d.country, COUNT(d.reviews) FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews " \
                            "SATISFIES r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END " \
                            "GROUP BY d.reviews,d.country ORDER BY d.reviews,d.country LIMIT 10 OFFSET 5"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        query_results = self.run_cbq_query(query=query)
        expected_results = self.run_cbq_query(query=primary_query)
        diffs = DeepDiff(query_results['results'], expected_results['results'])
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_orderby_limit(self):
        self.run_cbq_query(
            query="CREATE INDEX idx1 ON default(DISTINCT ARRAY flatten_keys(r.ratings.Cleanliness,r.ratings.Rooms,r.author) FOR r IN reviews END, country)")
        if self.use_unnest:
            query = "SELECT d.country, d.reviews FROM default AS d UNNEST reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) ORDER BY d.reviews,d.country LIMIT 10"
            primary_query = "SELECT d.country, d.reviews FROM default AS d USE INDEX (`#primary`) UNNEST reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) ORDER BY d.reviews,d.country LIMIT 10"
        else:
            query = "SELECT d.country, d.reviews FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author " \
                    "LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END ORDER BY d.reviews,d.country LIMIT 10"
            primary_query = "SELECT d.country, d.reviews FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author " \
                    "LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END ORDER BY d.reviews,d.country LIMIT 10"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        query_results = self.run_cbq_query(query=query)
        expected_results = self.run_cbq_query(query=primary_query)
        diffs = DeepDiff(query_results['results'], expected_results['results'])
        if diffs:
            self.assertTrue(False, diffs)

    def test_flatten_orderby_limit_offset(self):
        self.run_cbq_query(
            query="CREATE INDEX idx1 ON default(DISTINCT ARRAY flatten_keys(r.ratings.Cleanliness,r.ratings.Rooms,r.author) FOR r IN reviews END, country)")
        if self.use_unnest:
            query = "SELECT d.country, d.reviews FROM default AS d UNNEST reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) ORDER BY d.reviews,d.country LIMIT 10 OFFSET 5"
            primary_query = "SELECT d.country, d.reviews FROM default AS d USE INDEX (`#primary`) UNNEST reviews as r WHERE r.author LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) ORDER BY d.reviews,d.country LIMIT 10 OFFSET 5"
        else:
            query = "SELECT d.country, d.reviews FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author " \
                    "LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END ORDER BY d.reviews,d.country LIMIT 10 OFFSET 5"
            primary_query = "SELECT d.country, d.reviews FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author " \
                    "LIKE '%m' AND (r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3) END ORDER BY d.reviews,d.country LIMIT 10 OFFSET 5"
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),"The query should be using idx1, check explain results {0}".format(explain_results))
        query_results = self.run_cbq_query(query=query)
        expected_results = self.run_cbq_query(query=primary_query)
        diffs = DeepDiff(query_results['results'], expected_results['results'])
        if diffs:
            self.assertTrue(False, diffs)

    ##############################################################################################
    #
    #   DML
    ##############################################################################################

    '''Test insert that uses a query that uses an index with flatten_keys in it '''
    def test_flatten_insert(self):
        self.load_travel_sample()
        self.run_cbq_query(
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
        self.load_travel_sample()
        self.run_cbq_query(
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
        self.load_travel_sample()
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
        self.run_cbq_query(
            query="CREATE INDEX idx1 ON default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, free_parking) WHERE ANY r IN self.reviews SATISFIES r.author LIKE 'M%' END")
        explain_results = self.run_cbq_query(query="explain delete from default d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness = 3 END AND free_parking = True")
        self.assertTrue('idx1' in str(explain_results),
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        self.assertTrue("covers" in str(explain_results), "This query should be covered by the index but it is not: plan {0}".format(explain_results))
        delete_results = self.run_cbq_query(query="delete from default d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness = 3 END AND free_parking = True")
        self.assertTrue(delete_results['status'] == 'success',
                        "Index was not successfully created! {0}".format(delete_results))
        # Ensure no documents remain that fit the criteria
        primary_results = self.run_cbq_query(query="SELECT * FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness = 3 END AND free_parking = True")
        self.assertTrue(primary_results['metrics']['resultCount'] == 0 ,"There are results! But there should be no results for this query {0}".format(primary_results))

    def test_flatten_ansi_merge(self):
        self.load_travel_sample()
        query = "MERGE INTO default d USING `travel-sample`.inventory.hotel t ON t.country = d.country and any r in d.reviews satisfies r.author like 'M%' and r.ratings.Overall > 3 END WHEN MATCHED THEN DELETE WHERE d.free_parking = true"
        self.run_cbq_query(
            query="create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Overall) for r in reviews END,country)")
        explain_results = self.run_cbq_query(query="explain " + query)
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1 check explain results {0}".format(
                            explain_results))
        try:
            merge_results = self.run_cbq_query(query=query)
            self.fail("The ANSI merge operates on multiple documents and should therefore throw an error")
        except Exception as ex:
            self.assertTrue("Multiple UPDATE/DELETE of the same document" in str(ex), f"The error message is not what we expected please check {str(ex)}")

    ##############################################################################################
    #
    #   Covering Tests
    ##############################################################################################

    '''Test a array index covering'''
    def test_flatten_cover(self):
        if self.explicit:
            create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking, reviews)"
        else:
            create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking)"

        if self.use_all:
            create_query = create_query.replace("DISTINCT", "ALL")

        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        self.assertTrue("covers" in str(explain_results), "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        expected_results = self.run_cbq_query(query="SELECT email FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test a array index covering'''
    def test_flatten_cover_no_leading(self):
        create_query = "create index idx1 on default(free_parking, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email)"
        if self.use_all:
            create_query = create_query.replace("DISTINCT", "ALL")
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        self.assertTrue("covers" in str(explain_results), "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        expected_results = self.run_cbq_query(query="SELECT email FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test covering using the flatten array_index'''
    def test_flatten_cover_no_any(self):
        create_query = "create index idx1 on default(free_parking, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, email)"
        if self.use_all:
            create_query = create_query.replace("DISTINCT", "ALL")
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT email FROM default AS d WHERE free_parking = True")
        self.assertTrue("covers" in str(explain_results), "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT email FROM default AS d WHERE free_parking = True")
        expected_results = self.run_cbq_query(query="SELECT email FROM default AS d USE INDEX (`#primary`) WHERE free_parking = True")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''You need to use reviews explicitly stated for covering in any and every'''
    def test_flatten_any_and_every_non_cover(self):
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, phone, free_parking)"
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT phone FROM default AS d WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        self.assertTrue("covers" not in str(explain_results), "The index is covering, it shouldn't be. Check plan {0}".format(explain_results))
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT phone FROM default AS d WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        expected_results = self.run_cbq_query(query="SELECT phone FROM default AS d USE INDEX (`#primary`) WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''You need to use reviews explicitly stated for covering in any and every'''
    def test_flatten_any_and_every_cover(self):
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, address, free_parking, reviews)"
        if self.use_all:
            create_query = create_query.replace("DISTINCT", "ALL")
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT address FROM default AS d WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        self.assertTrue("covers" in str(explain_results), "The index is covering, it shouldn't be. Check plan {0}".format(explain_results))
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT address FROM default AS d WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        expected_results = self.run_cbq_query(query="SELECT address FROM default AS d USE INDEX (`#primary`) WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''You need to use reviews explicitly stated for covering in any and every'''
    def test_flatten_any_and_every_cover_non_leading(self):
        self.run_cbq_query(query="create index idx1 on default(free_parking, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, address, reviews)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT address FROM default AS d WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        self.assertTrue("covers" in str(explain_results), "The index is covering, it shouldn't be. Check plan {0}".format(explain_results))
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT address FROM default AS d WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        expected_results = self.run_cbq_query(query="SELECT address FROM default AS d USE INDEX (`#primary`) WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''You need to use reviews explicitly stated for covering in any and every'''
    def test_flatten_any_and_every_cover_array_leading(self):
        self.run_cbq_query(query="create index idx1 on default(reviews, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END, address, free_parking)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT address FROM default AS d WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        self.assertTrue("covers" not in str(explain_results), "The index is covering, it shouldn't be. Check plan {0}".format(explain_results))
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary',
                        "The correct index is not being used or the plan is different than expected! Expected primary got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT address FROM default AS d WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        expected_results = self.run_cbq_query(query="SELECT address FROM default AS d USE INDEX (`#primary`) WHERE ANY AND EVERY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test array indexing cover with a when clause, query has field not explicitly in index but since the field is in when it should be covering'''
    def test_flatten_when_cover(self):
        create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews WHEN r.ratings.Rooms < 3 END, email, free_parking)"
        if self.use_all:
            create_query = create_query.replace("DISTINCT", "ALL")
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3 END AND free_parking = True")
        self.assertTrue("covers" in str(explain_results), "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3 END AND free_parking = True")
        expected_results = self.run_cbq_query(query="SELECT email FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3 END AND free_parking = True")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test array indexing cover with a when clause, query has field not explicitly in index but since the field is in when it should be covering'''
    def test_flatten_when_cover_non_leading(self):
        create_query = "create index idx1 on default(free_parking, DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews WHEN r.ratings.Rooms < 3 END, email)"
        if self.use_all:
            create_query = create_query.replace("DISTINCT", "ALL")
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3 END AND free_parking = True")
        self.assertTrue("covers" in str(explain_results), "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query="SELECT email FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3 END AND free_parking = True")
        expected_results = self.run_cbq_query(query="SELECT email FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 AND r.ratings.Rooms < 3 END AND free_parking = True")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test covering with a partial array_flattening index'''
    def test_flatten_partial_cover(self):
        create_query = "create index idx1 on default(ALL ARRAY FLATTEN_KEYS(r.author,r.ratings.Cleanliness) FOR r IN reviews END,url) where free_parking = True"
        if self.use_all:
            create_query = create_query.replace("DISTINCT", "ALL")
        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT url FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        self.assertTrue("covers" in str(explain_results), "The index is not covering, it should be. Check plan {0}".format(explain_results))
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT url FROM default AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        expected_results = self.run_cbq_query(
            query="SELECT url FROM default AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' and r.ratings.Cleanliness > 1 END AND free_parking = True")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''With distinct keyword, unnest can't cover, with all keyword, unnest can cover'''
    def test_flatten_unnest_cover(self):
        if self.explicit:
            create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Rooms) FOR r IN reviews END, email, country, reviews)"
        else:
            create_query = "create index idx1 on default(DISTINCT ARRAY FLATTEN_KEYS(r.author,r.ratings.Rooms) FOR r IN reviews END, email, country)"
        if self.use_all:
            create_query = create_query.replace("DISTINCT", "ALL")

        self.run_cbq_query(query=create_query)
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN SELECT d.email FROM default AS d UNNEST reviews AS rev "
                                                   "WHERE rev.author LIKE 'M%' AND rev.ratings.Rooms > 1 AND d.country IS NOT NULL")
        if self.use_all:
            self.assertTrue("covers" in str(explain_results), "The index is not covering, it should be. Check plan {0}".format(explain_results))
            self.assertTrue("filter_covers" in str(explain_results), "The index is not covering, it should be. Check plan {0}".format(explain_results))
            self.assertTrue('idx1' in str(explain_results),
                            "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                                explain_results))
        else:
            self.assertTrue("covers" not in str(explain_results), "The index is not covering, it should be. Check plan {0}".format(explain_results))
            self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                            "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                                explain_results))
        query_results = self.run_cbq_query(query="SELECT d.email FROM default AS d UNNEST reviews AS rev "
                                                   "WHERE rev.author LIKE 'M%' AND rev.ratings.Rooms > 1 AND d.country IS NOT NULL")
        expected_results = self.run_cbq_query(query="SELECT d.email FROM default AS d USE INDEX (`#primary`) UNNEST reviews AS rev "
                                                   "WHERE rev.author LIKE 'M%' AND rev.ratings.Rooms > 1 AND d.country IS NOT NULL")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    ##############################################################################################
    #
    #   Intersect/Union/Except
    ##############################################################################################

    '''Test intersect scan between two any/any and every arrays'''
    def test_flatten_intersect_any(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        self.run_cbq_query(query="CREATE INDEX `idx2` ON `default`((distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)),`email`)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d WHERE ANY r IN d.reviews SATISFIES r.ratings.Cleanliness > 1 "
                  "AND r.author LIKE 'M%' END AND ANY s IN d.reviews SATISFIES s.ratings.Rooms > 3 AND s.ratings.Overall > 1 END")
        self.assertTrue("IntersectScan" in str(explain_results), "The query should be using an instersect scan, check explain results {0}".format(explain_results))
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        self.assertTrue("idx2" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default d WHERE ANY r IN d.reviews SATISFIES r.ratings.Cleanliness > 1 "
                  "AND r.author LIKE 'M%' END AND ANY s IN d.reviews SATISFIES s.ratings.Rooms > 3 AND s.ratings.Overall > 1 END")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.ratings.Cleanliness > 1 "
                  "AND r.author LIKE 'M%' END AND ANY s IN d.reviews SATISFIES s.ratings.Rooms > 3 AND s.ratings.Overall > 1 END")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test intersect scan between an any clause and an unnest clause'''
    def test_flatten_intersect_any_unnest(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        self.run_cbq_query(query="CREATE INDEX `idx2` ON `default`((distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)),`email`)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " AND ANY s IN d.reviews SATISFIES r.ratings.Rooms > 1 AND r.ratings.Overall < 5 END")
        self.assertTrue("IntersectScan" in str(explain_results), "The query should be using an instersect scan, check explain results {0}".format(explain_results))
        self.assertTrue("Unnest" in str(explain_results), "The query should be using an unnest, check explain results {0}".format(explain_results))
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        self.assertTrue("idx2" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " AND ANY s IN d.reviews SATISFIES r.ratings.Rooms > 1 AND r.ratings.Overall < 5 END")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews  AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " AND ANY s IN d.reviews SATISFIES r.ratings.Rooms > 1 AND r.ratings.Overall < 5 END")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test intersect scan between two unnest clauses'''
    def test_flatten_intersect_unnest(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        self.run_cbq_query(query="CREATE INDEX `idx2` ON `default`((distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)),`email`)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " AND r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        self.assertTrue("IntersectScan" in str(explain_results), "The query should be using an instersect scan, check explain results {0}".format(explain_results))
        self.assertTrue("Unnest" in str(explain_results), "The query should be using an unnest, check explain results {0}".format(explain_results))
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        self.assertTrue("idx2" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " AND r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " AND r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test unionall scan between two any/any and every arrays'''
    def test_flatten_union_any(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        self.run_cbq_query(query="CREATE INDEX `idx2` ON `default`((distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)),`email`)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d WHERE ANY r IN d.reviews SATISFIES r.ratings.Cleanliness > 1 "
                  "AND r.author LIKE 'M%' END UNION SELECT * FROM default d WHERE ANY s IN d.reviews SATISFIES s.ratings.Rooms > 3 AND s.ratings.Overall > 1 END")
        self.assertTrue("UnionAll" in str(explain_results), "The query should be using an instersect scan, check explain results {0}".format(explain_results))
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        self.assertTrue("idx2" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default d WHERE ANY r IN d.reviews SATISFIES r.ratings.Cleanliness > 1 "
                  "AND r.author LIKE 'M%' END UNION SELECT * FROM default d WHERE ANY s IN d.reviews SATISFIES s.ratings.Rooms > 3 AND s.ratings.Overall > 1 END")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.ratings.Cleanliness > 1 "
                  "AND r.author LIKE 'M%' END UNION SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY s IN d.reviews SATISFIES s.ratings.Rooms > 3 AND s.ratings.Overall > 1 END")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test union all scan between an any clause and an unnest clause'''
    def test_flatten_union_any_unnest(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        self.run_cbq_query(query="CREATE INDEX `idx2` ON `default`((distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)),`email`)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " UNION SELECT * FROM default d WHERE ANY s IN d.reviews SATISFIES s.ratings.Rooms > 1 AND s.ratings.Overall < 5 END")
        self.assertTrue("UnionAll" in str(explain_results), "The query should be using an unionall scan, check explain results {0}".format(explain_results))
        self.assertTrue("Unnest" in str(explain_results), "The query should be using an unnest, check explain results {0}".format(explain_results))
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using an union all scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        self.assertTrue("idx2" in str(explain_results),
                        "The query should be using an union all scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " UNION SELECT * FROM default d WHERE ANY s IN d.reviews SATISFIES s.ratings.Rooms > 1 AND s.ratings.Overall < 5 END")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " UNION SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY s IN d.reviews SATISFIES s.ratings.Rooms > 1 AND s.ratings.Overall < 5 END")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test union all scan between two any/any and every arrays'''
    def test_flatten_union_unnest(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        self.run_cbq_query(query="CREATE INDEX `idx2` ON `default`((distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)),`email`)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%' "
                  "UNION SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        self.assertTrue("UnionAll" in str(explain_results), "The query should be using an instersect scan, check explain results {0}".format(explain_results))
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        self.assertTrue("idx2" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%' "
                  "UNION SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%' "
                  "UNION SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test exceptall scan between two unnest'''
    def test_flatten_except_unnest(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        self.run_cbq_query(query="CREATE INDEX `idx2` ON `default`((distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)),`email`)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%' "
                  "EXCEPT SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        self.assertTrue("ExceptAll" in str(explain_results), "The query should be using an instersect scan, check explain results {0}".format(explain_results))
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        self.assertTrue("idx2" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%' "
                  "EXCEPT SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%' "
                  "EXCEPT SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test unionall scan between two any/any and every arrays'''
    def test_flatten_intersectall_any(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        self.run_cbq_query(query="CREATE INDEX `idx2` ON `default`((distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)),`email`)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d WHERE ANY r IN d.reviews SATISFIES r.ratings.Cleanliness > 1 "
                  "AND r.author LIKE 'M%' END INTERSECT SELECT * FROM default d WHERE ANY s IN d.reviews SATISFIES s.ratings.Rooms > 3 AND s.ratings.Overall > 1 END")
        self.assertTrue("IntersectAll" in str(explain_results), "The query should be using an instersect scan, check explain results {0}".format(explain_results))
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        self.assertTrue("idx2" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default d WHERE ANY r IN d.reviews SATISFIES r.ratings.Cleanliness > 1 "
                  "AND r.author LIKE 'M%' END INTERSECT SELECT * FROM default d WHERE ANY s IN d.reviews SATISFIES s.ratings.Rooms > 3 AND s.ratings.Overall > 1 END")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.ratings.Cleanliness > 1 "
                  "AND r.author LIKE 'M%' END INTERSECT SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY s IN d.reviews SATISFIES s.ratings.Rooms > 3 AND s.ratings.Overall > 1 END")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test union all scan between an any clause and an unnest clause'''
    def test_flatten_intersectall_any_unnest(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        self.run_cbq_query(query="CREATE INDEX `idx2` ON `default`((distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)),`email`)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " INTERSECT SELECT * FROM default d WHERE ANY s IN d.reviews SATISFIES r.ratings.Rooms > 1 AND r.ratings.Overall < 5 END")
        self.assertTrue("IntersectAll" in str(explain_results), "The query should be using an instersect scan, check explain results {0}".format(explain_results))
        self.assertTrue("Unnest" in str(explain_results), "The query should be using an unnest, check explain results {0}".format(explain_results))
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        self.assertTrue("idx2" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " INTERSECT SELECT * FROM default d WHERE ANY s IN d.reviews SATISFIES r.ratings.Rooms > 1 AND r.ratings.Overall < 5 END")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%'"
                  " INTERSECT SELECT * FROM default d USE INDEX (`#primary`) WHERE ANY s IN d.reviews SATISFIES r.ratings.Rooms > 1 AND r.ratings.Overall < 5 END")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    '''Test union all scan between two any/any and every arrays'''
    def test_flatten_intersectall_unnest(self):
        self.run_cbq_query(
            query="CREATE INDEX `idx1` ON `default`((distinct (array flatten_keys((`r`.`author`), ((`r`.`ratings`).`Cleanliness`)) for `r` in `reviews` end)))")
        self.run_cbq_query(query="CREATE INDEX `idx2` ON `default`((distinct (array flatten_keys(((`r`.`ratings`).`Rooms`), ((`r`.`ratings`).`Overall`)) for `r` in `reviews` end)),`email`)")
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(
            query="EXPLAIN SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%' "
                  "INTERSECT SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        self.assertTrue("IntersectAll" in str(explain_results), "The query should be using an instersect scan, check explain results {0}".format(explain_results))
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        self.assertTrue("idx2" in str(explain_results),
                        "The query should be using an instersect scan between idx1 and idx2, check explain results {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(
            query="SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%' "
                  "INTERSECT SELECT * FROM default d UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        expected_results = self.run_cbq_query(
            query="SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Cleanliness = 1 AND r.author LIKE 'M%' "
                  "INTERSECT SELECT * FROM default d USE INDEX (`#primary`) UNNEST d.reviews AS r WHERE r.ratings.Rooms > 1 AND r.ratings.Overall < 5")
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    ##############################################################################################
    #
    #   nested tests
    ##############################################################################################

    def test_flatten_triple_nested_covered(self):
        self.load_nested()
        self.run_cbq_query(query="CREATE INDEX idx1 ON default( ALL ARRAY(ALL ARRAY( ALL ARRAY FLATTEN_KEYS(sec.num,pg.description,ch.name) FOR sec IN pg.sections END) FOR pg IN ch.pages END) FOR ch IN chapters END, year, chapters, `type`)")
        if self.use_unnest:
            query = "SELECT d.year, d.`type` FROM default d UNNEST chapters AS ch UNNEST ch.pages AS pg " \
                    "UNNEST pg.sections AS sec WHERE d.`type` = 'book' AND ch.name = 'chapter 1' AND pg.description LIKE 'page%' AND sec.num = 1"
            primary_query = "SELECT d.year, d.`type` FROM default d USE INDEX (`#primary`) UNNEST chapters AS ch UNNEST ch.pages AS pg " \
                    "UNNEST pg.sections AS sec WHERE d.`type` = 'book' AND ch.name = 'chapter 1' AND pg.description LIKE 'page%' AND sec.num = 1"
        else:
            query = "SELECT year, `type` FROM default d WHERE `type` = 'book' AND ANY ch IN chapters SATISFIES ANY pg IN ch.pages " \
                    "SATISFIES ANY sec IN pg.sections SATISFIES ch.name = 'chapter 1' AND pg.description LIKE 'page%' AND sec.num = 1 END END END"
            primary_query = "SELECT year, `type` FROM default d USE INDEX (`#primary`) WHERE `type` = 'book' AND ANY ch IN chapters SATISFIES ANY pg IN ch.pages " \
                    "SATISFIES ANY sec IN pg.sections SATISFIES ch.name = 'chapter 1' AND pg.description LIKE 'page%' AND sec.num = 1 END END END"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(
                            explain_results))
        self.assertTrue("covers" in str(explain_results),
                        "The index is not covering, it should be. Check plan {0}".format(explain_results))

        self.compare_against_primary(query, primary_query)

    def test_flatten_triple_nested_or(self):
        self.load_nested()
        self.run_cbq_query(query="CREATE INDEX idx1 ON default( ALL ARRAY(ALL ARRAY( ALL ARRAY FLATTEN_KEYS(sec.num,pg.description,ch.name) FOR sec IN pg.sections END) FOR pg IN ch.pages END) FOR ch IN chapters END, year, chapters, `type`)")
        if self.use_unnest:
            query = "SELECT d.year, d.`type` FROM default d UNNEST chapters AS ch UNNEST ch.pages AS pg " \
                    "UNNEST pg.sections AS sec WHERE d.`type` = 'book' OR (ch.name = 'chapter 1' AND pg.description LIKE 'page%' AND sec.num = 1)"
            primary_query = "SELECT d.year, d.`type` FROM default d USE INDEX (`#primary`) UNNEST chapters AS ch UNNEST ch.pages AS pg " \
                    "UNNEST pg.sections AS sec WHERE d.`type` = 'book' OR (ch.name = 'chapter 1' AND pg.description LIKE 'page%' AND sec.num = 1)"
        else:
            query = "SELECT year, `type` FROM default d WHERE `type` = 'book' OR (ANY ch IN chapters SATISFIES ANY pg IN ch.pages " \
                    "SATISFIES ANY sec IN pg.sections SATISFIES ch.name = 'chapter 1' AND pg.description LIKE 'page%' AND sec.num = 1 END END END)"
            primary_query = "SELECT year, `type` FROM default d USE INDEX (`#primary`) WHERE `type` = 'book' OR (ANY ch IN chapters SATISFIES ANY pg IN ch.pages " \
                    "SATISFIES ANY sec IN pg.sections SATISFIES ch.name = 'chapter 1' AND pg.description LIKE 'page%' AND sec.num = 1 END END END)"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("#primary" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(
                            explain_results))

        self.compare_against_primary(query, primary_query)

    def test_flatten_triple_nested_multiple_levels(self):
        self.load_nested()
        self.run_cbq_query(
            query="CREATE INDEX idx1 ON default( ALL ARRAY(ALL ARRAY( ALL ARRAY FLATTEN_KEYS(sec.num,pg.description,ch.name) FOR sec IN pg.sections END) FOR pg IN ch.pages END) FOR ch IN chapters END, year, chapters, `type`)")

        query = "SELECT year, `type` FROM default d WHERE `type` = 'book' AND ANY ch IN chapters SATISFIES ANY pg IN ch.pages " \
                "SATISFIES ANY sec IN pg.sections SATISFIES ch.name = 'chapter 1' AND sec.num = 1 END AND pg.description LIKE 'page%' END END"
        primary_query = "SELECT year, `type` FROM default d USE INDEX (`#primary`) WHERE `type` = 'book' AND ANY ch IN chapters SATISFIES ANY pg IN ch.pages " \
                        "SATISFIES ANY sec IN pg.sections SATISFIES ch.name = 'chapter 1' AND sec.num = 1 END  AND pg.description LIKE 'page%' END END"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("idx1" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(
                            explain_results))

        self.compare_against_primary(query, primary_query)

    def test_flatten_triple_nested_unnest_any_mix(self):
        self.load_nested()
        self.run_cbq_query(
            query="CREATE INDEX idx1 ON default( ALL ARRAY(ALL ARRAY( ALL ARRAY FLATTEN_KEYS(sec.num,pg.description,ch.name) FOR sec IN pg.sections END) FOR pg IN ch.pages END) FOR ch IN chapters END, year, chapters, `type`)")

        query = "SELECT d.year, d.`type` FROM default d UNNEST chapters AS ch UNNEST ch.pages AS pg WHERE d.`type` = 'book' " \
                "AND ch.name = 'chapter 1' AND pg.description LIKE 'page%' AND ANY sec IN pg.sections SATISFIES sec.num = 1 END"
        primary_query = "SELECT d.year, d.`type` FROM default d USE INDEX (`#primary`) UNNEST chapters AS ch UNNEST ch.pages AS pg WHERE d.`type` = 'book' " \
                "AND ch.name = 'chapter 1' AND pg.description LIKE 'page%' AND ANY sec IN pg.sections SATISFIES sec.num = 1 END"

        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query)
        self.assertTrue("#primary" in str(explain_results),
                        "The query should be using idx1, check explain results {0}".format(
                            explain_results))

        self.compare_against_primary(query, primary_query)

    ##############################################################################################
    #
    #   UNKNOWN TESTING
    ##############################################################################################

    def test_unknowns_null(self):
        self.load_unknowns()

        if self.use_all:
            index_name = "ix13"
        else:
            index_name = "ix10"

        if self.use_unnest:
            query = f"Select meta(d).id from default d  USE INDEX ({index_name}) unnest reviews rev where rev.x is NULL"
            primary_query = "Select meta(d).id from default d USE INDEX (`#primary`) unnest reviews rev where rev.x is NULL"
        else:
            query = f"SELECT META().id FROM default d USE INDEX ({index_name}) WHERE d.a = 1 AND ANY v IN d.reviews SATISFIES v.x IS NULL END"
            primary_query = "SELECT META().id FROM default d USE INDEX (`#primary`) WHERE d.a = 1 AND ANY v IN d.reviews SATISFIES v.x IS NULL END"
        explain_results = self.run_cbq_query(query='EXPLAIN ' + query)
        self.assertTrue(index_name in str(explain_results), f"The correct index is not being used, please check {explain_results}")
        self.assertTrue("covers" not in str(explain_results), f"The correct index is not being used, please check {explain_results}")
        self.compare_against_primary(query, primary_query)

    def test_unknowns_missing(self):
        self.load_unknowns()

        if self.use_all:
            index_name = "ix13"
        else:
            index_name = "ix10"

        if self.use_unnest:
            query = f"Select meta(d).id from default d USE INDEX ({index_name}) unnest reviews rev where rev.x is MISSING"
            primary_query = "Select meta(d).id from default d USE INDEX (`#primary`) unnest reviews rev where rev.x is MISSING"
        else:
            query = f"SELECT META().id FROM default d USE INDEX ({index_name}) WHERE d.a = 1 AND ANY v IN d.reviews SATISFIES v.x IS MISSING END"
            primary_query = "SELECT META().id FROM default d USE INDEX (`#primary`) WHERE d.a = 1 AND ANY v IN d.reviews SATISFIES v.x IS MISSING END"
        explain_results = self.run_cbq_query(query='EXPLAIN ' + query)
        self.assertTrue(index_name in str(explain_results),
                        f"The correct index is not being used, please check {explain_results}")
        self.assertTrue("covers" not in str(explain_results),
                        f"The correct index is not being used, please check {explain_results}")
        self.compare_against_primary(query, primary_query)

    def test_unknowns_partial(self):
        self.load_unknowns()

        if self.use_all:
            index_name = "ix15"
        else:
            index_name = "ix12"

        if self.use_unnest:
            query = f"Select meta(d).id from default d USE INDEX ({index_name}) unnest reviews rev where rev.x is MISSING and rev.z = 3"
            primary_query = "Select meta(d).id from default d USE INDEX (`#primary`) unnest reviews rev where rev.x is MISSING and rev.z = 3"
        else:
            query = f"SELECT META().id FROM default d USE INDEX ({index_name}) WHERE d.a = 1 AND ANY v IN d.reviews SATISFIES v.x IS MISSING and v.z = 3 END"
            primary_query = "SELECT META().id FROM default d USE INDEX (`#primary`) WHERE d.a = 1 AND ANY v IN d.reviews SATISFIES v.x IS MISSING and v.z = 3 END"
        explain_results = self.run_cbq_query(query='EXPLAIN ' + query)
        self.assertTrue(index_name in str(explain_results),
                        f"The correct index is not being used, please check {explain_results}")
        self.assertTrue("covers" not in str(explain_results),
                        f"The correct index is not being used, please check {explain_results}")
        self.compare_against_primary(query, primary_query)

    def test_unknown_array_index_key_unnest(self):
        self.load_unknowns_array_key()

        unnest_query = "SELECT META(d).id FROM default AS d USE INDEX (ix2) UNNEST d.arr1 AS v WHERE v.id IS NULL ORDER by META(d).id"
        unnest_explain_results = self.run_cbq_query(query='EXPLAIN ' + unnest_query)
        self.assertTrue("ix2" in str(unnest_explain_results), f"The correct index is not being used, please check {unnest_explain_results}")
        self.assertTrue("covers" not in str(unnest_explain_results), f"The correct index is not being used, please check {unnest_explain_results}")

        unnest_results = self.run_cbq_query(query=unnest_query)
        self.assertEqual(unnest_results['results'], [{"id": "k01"}, {"id": "k05"}, {"id": "k05"}])

        unnest_query2 = 'SELECT META(d).id FROM default AS d USE INDEX (ix3) UNNEST d.arr1 AS v WHERE v.id IS NULL ORDER BY META(d).id'
        unnest_explain_results2 = self.run_cbq_query(query='EXPLAIN ' + unnest_query2)
        self.assertTrue("ix3" in str(unnest_explain_results2), f"The correct index is not being used, please check {unnest_explain_results2}")
        unnest_results2 = self.run_cbq_query(query=unnest_query2)
        self.assertEqual(unnest_results2['results'], [{"id": "k01"}, {"id": "k05"}, {"id": "k05"}])

    def test_unkown_array_index_key_any(self):
        self.load_unknowns_array_key()

        any_query = "SELECT META(d).id FROM default AS d USE INDEX (ix2) WHERE any v in arr1 SATISFIES v.id IS NULL END"
        explain_any_results = self.run_cbq_query(query='EXPLAIN ' + any_query)
        self.assertTrue("ix2" in str(explain_any_results), f"The correct index is not being used, please check {explain_any_results}")
        self.assertTrue("covers" not in str(explain_any_results), f"The correct index is not being used, please check {explain_any_results}")


        any_results = self.run_cbq_query(query=any_query)
        self.assertEqual(any_results['results'], [{"id": "k01"}, {"id": "k05"}])


    def test_unkown_array_index_key_any_every(self):
        self.load_unknowns_array_key()

        any_every_query = "SELECT META(d).id FROM default AS d USE INDEX (ix2) WHERE any and every v in arr1 SATISFIES v.id IS NULL END"
        explain_any_every_results = self.run_cbq_query(query='EXPLAIN ' + any_every_query)
        self.assertTrue("ix2" in str(explain_any_every_results), f"The correct index is not being used, please check {explain_any_every_results}")
        self.assertTrue("covers" not in str(explain_any_every_results), f"The correct index is not being used, please check {explain_any_every_results}")

        any_every_results = self.run_cbq_query(query=any_every_query)
        self.assertEqual(any_every_results['results'], [{"id": "k05"}])

    ##############################################################################################
    #
    #   Helper
    ##############################################################################################

    def load_data(self, num_extra_buckets=0):
        self.conn.delete_all_buckets()
        time.sleep(5)
        self.conn.create_bucket(bucket="default", ramQuotaMB=self.bucket_size, proxyPort=11220, storageBackend=self.bucket_storage, replicaNumber=0)
        for i in range(0, num_extra_buckets):
            self.conn.create_bucket(bucket="bucket{0}".format(i), ramQuotaMB=self.bucket_size, proxyPort=11220, storageBackend=self.bucket_storage, replicaNumber=0)
            time.sleep(5)
            self.run_cbq_query("CREATE PRIMARY INDEX on bucket{0}".format(i))
        time.sleep(5)
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

    def load_nested(self):
        upsert1 = "UPSERT INTO default VALUES ('book1', { 'type':'book', 'author': 'James', 'rev': 2, 'year': 2020, 'name': 'book 1', 'description': 'book 1 description', 'isbn': 1, 'chapters': [ {'num': 1, 'name': 'chapter 1', 'description': 'chapter 1 description', 'pages' : [ { 'num':1, 'name':'page 1', 'description': 'page 1 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':2, 'name':'page 2', 'description': 'page 2 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':3, 'name':'page 3', 'description': 'page 3 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] } ] }, {'num': 2, 'name': 'chapter 2', 'description': 'chapter 2 description', 'pages' : [ { 'num':1, 'name':'page 1', 'description': 'page 1 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':2, 'name':'page 2', 'description': 'page 2 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':3, 'name':'page 3', 'description': 'page 3 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] } ] }, {'num': 3, 'name': 'chapter 3', 'description': 'chapter 3 description', 'pages' : [ { 'num':1, 'name':'page 1', 'description': 'page 1 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':2, 'name':'page 2', 'description': 'page 2 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':3, 'name':'page 3', 'description': 'page 3 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] } ] } ] })"
        upsert2 = "UPSERT INTO default VALUES ('book2', { 'type':'book', 'author': 'Mark', 'rev': 3, 'year': 2021, 'name': 'book 2', 'description': 'book 2 description', 'isbn': 2, 'chapters': [ {'num': 1, 'name': 'chapter 1', 'description': 'chapter 1 description', 'pages' : [ { 'num':1, 'name':'page 1', 'description': 'page 1 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':2, 'name':'page 2', 'description': 'page 2 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':3, 'name':'page 3', 'description': 'page 3 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] } ] }, {'num': 2, 'name': 'chapter 2', 'description': 'chapter 2 description', 'pages' : [ { 'num':1, 'name':'page 1', 'description': 'page 1 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':2, 'name':'page 2', 'description': 'page 2 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':3, 'name':'page 3', 'description': 'page 3 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] } ] }, {'num': 3, 'name': 'chapter 3', 'description': 'chapter 3 description', 'pages' : [ { 'num':1, 'name':'page 1', 'description': 'page 1 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':2, 'name':'page 2', 'description': 'page 2 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':3, 'name':'page 3', 'description': 'page 3 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] } ] } ] })"
        upsert3 = "UPSERT INTO default VALUES ('book3', { 'type':'book', 'author': 'Chris', 'rev': 1, 'year': 2019, 'name': 'book 3', 'description': 'book 3 description', 'isbn': 3, 'chapters': [ {'num': 1, 'name': 'chapter 1', 'description': 'chapter 1 description', 'pages' : [ { 'num':1, 'name':'page 1', 'description': 'page 1 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':2, 'name':'page 2', 'description': 'page 2 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':3, 'name':'page 3', 'description': 'page 3 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] } ] }, {'num': 2, 'name': 'chapter 2', 'description': 'chapter 2 description', 'pages' : [ { 'num':1, 'name':'page 1', 'description': 'page 1 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':2, 'name':'page 2', 'description': 'page 2 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':3, 'name':'page 3', 'description': 'page 3 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] } ] }, {'num': 3, 'name': 'chapter 3', 'description': 'chapter 3 description', 'pages' : [ { 'num':1, 'name':'page 1', 'description': 'page 1 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':2, 'name':'page 2', 'description': 'page 2 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] }, { 'num':3, 'name':'page 3', 'description': 'page 3 description', 'sections':[ { 'num':1, 'name':'section 1', 'description': 'section 1 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':2, 'name':'section 2', 'description': 'section 2 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] }, { 'num':3, 'name':'section 3', 'description': 'section 3 description', 'paragraphs': [ { 'num': 1, 'name': 'paragraph 1', 'description': 'paragraph 1 description' }, { 'num': 2, 'name': 'paragraph 2', 'description': 'paragraph 2 description' }, { 'num': 3, 'name': 'paragraph 3', 'description': 'paragraph 3 description' } ] } ] } ] } ] })"

        self.run_cbq_query(query = upsert1)
        self.run_cbq_query(query = upsert2)
        self.run_cbq_query(query = upsert3)

    def load_unknowns(self):
        self.run_cbq_query(query='UPSERT INTO default VALUES("k001", {"a":1})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("k002", {"a":1, "reviews":NULL})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("k003", {"a":1, "reviews":1})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("k004", {"a":1, "reviews":"abc"})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ko01", {"a":1, "reviews":{"x":1, "y":2, "z":3}})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ko02", {"a":1, "reviews":{"xx":1, "y":3, "z":3}})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ko03", {"a":1, "reviews":{"x":1, "yy":3, "z":3}})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ka04", {"a":1, "reviews":{"xx":1, "y":3, "z":3}})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ka01", {"a":1, "reviews":[]})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ka02", {"a":1, "reviews":[{"xx":1, "yy":2, "z":3}]})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ka03", {"a":1, "reviews":[{"xx":1, "y":2, "z":3}]})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ka04", {"a":1, "reviews":[{"x":1, "yy":2, "z":3}]})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ka05", {"a":1, "reviews":[{"x":1, "y":2, "z":3}]})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ka06", {"a":1, "reviews":[{"xx":1, "yy":2, "z":3}, {"x":1, "y":2, "z":3}]})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ka07", {"a":1, "reviews":[{"xx":1, "y":2, "z":3}, {"x":1, "y":2, "z":3}]})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ka08", {"a":1, "reviews":[{"x":1, "yy":2, "z":3}, {"x":1, "y":2, "z":3}]})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ka09", {"a":1, "reviews":[{"x":1, "y":2, "z":3}, {"x":1, "y":2, "z":3}]})')
        self.run_cbq_query(query='UPSERT INTO default VALUES("ka09", {"a":1, "reviews":[{"x":NULL, "y":2, "z":3}, {"x":1, "y":2, "z":3}]})')
        try:
            self.run_cbq_query(query='CREATE INDEX ix10 ON default (a, DISTINCT ARRAY FLATTEN_KEYS(v.x, v.y) FOR v IN reviews END)')
            self.run_cbq_query(query='CREATE INDEX ix11 ON default (a, DISTINCT ARRAY FLATTEN_KEYS(v.x, v.y) FOR v IN reviews  WHEN v.z = 5 END)')
            self.run_cbq_query(query='CREATE INDEX ix12 ON default (a, DISTINCT ARRAY FLATTEN_KEYS(v.x, v.y) FOR v IN reviews  WHEN v.z = 3 END)')
            self.run_cbq_query(query='CREATE INDEX ix13 ON default ( ALL ARRAY FLATTEN_KEYS(v.x, v.y) FOR v IN reviews END)')
            self.run_cbq_query(query='CREATE INDEX ix14 ON default ( ALL ARRAY FLATTEN_KEYS(v.x, v.y) FOR v IN reviews  WHEN v.z = 5 END)')
            self.run_cbq_query(query='CREATE INDEX ix15 ON default ( ALL ARRAY FLATTEN_KEYS(v.x, v.y) FOR v IN reviews  WHEN v.z = 3 END)')
            self.sleep(10)
        except Exception as ex:
            pass
    def load_unknowns_array_key(self):
        self.run_cbq_query(query='UPSERT into default (key, value) values("k01", { "c1": "a", "arr1": [ {"id":NULL}, {"id":7}] })')
        self.run_cbq_query(query='UPSERT into default (key, value) values("k02", { "c1": "a", "arr1": NULL})')
        self.run_cbq_query(query='UPSERT into default (key, value) values("k03", { "c1": "a", "arr1": 5})')
        self.run_cbq_query(query='UPSERT into default (key, value) values("k05", { "c1": "a", "arr1": [{"id":NULL},{"id":NULL}] })')
        try:
            self.run_cbq_query(query='create index ix2 ON default(ALL ARRAY v.id FOR v IN arr1 END)')
            self.run_cbq_query(query='create index ix3 ON default(ALL ARRAY v.id FOR v IN arr1 END, arr1)')
        except Exception as ex:
            pass

    def compare_against_primary(self,query="", primary_query=""):
        query_results = self.run_cbq_query(query=query)
        expected_results = self.run_cbq_query(query=primary_query)
        if query_results['metrics']['resultCount'] != 0 and expected_results['metrics']['resultCount'] != 0:
            diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        else:
            self.fail(f"We expect non zero results! {query_results}")

    def load_travel_sample(self):
        self.rest.load_sample("travel-sample")
        time.sleep(60)
        self.wait_for_all_indexes_online()