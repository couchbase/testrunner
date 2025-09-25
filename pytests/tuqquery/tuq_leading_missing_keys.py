from .tuq import QueryTests
from deepdiff import DeepDiff
import time

class QueryLMKTests(QueryTests):
    def setUp(self):
        super(QueryLMKTests, self).setUp()
        self.log.info("==============  QueryLMKTests setup has started ==============")
        self.log_config_info()
        self.query_buckets = self.get_query_buckets(sample_buckets=['travel-sample'], check_all_buckets=True)
        self.query_bucket = self.query_buckets[0]
        self.sample_bucket = self.query_buckets[1]
        self.sample_bucket = self.sample_bucket.replace('`travel-sample`', '\`travel-sample\`')
        self.use_unnest = self.input.param("use_unnest", False)
        self.log.info("==============  QueryLMKTests setup has completed ==============")


    def suite_setUp(self):
        super(QueryLMKTests, self).suite_setUp()
        self.log.info("==============  QueryLMKTests suite_setup has started ==============")
        self.rest.load_sample("travel-sample")
        self.log.info("==============  QueryCurlTests suite_setup has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryLMKTests tearDown has started ==============")
        self.log.info("==============  QueryLMKTests tearDown has completed ==============")
        super(QueryLMKTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryLMKTests suite_tearDown has started ==============")
        self.log.info("==============  QueryLMKTests suite_tearDown has completed ==============")
        super(QueryLMKTests, self).suite_tearDown()

    def test_lmk_basic(self):
        # idx1 cannot be used for the query, need idx2
        invalid_index = "CREATE INDEX idx1 ON `travel-sample`(name)"
        correct_index = "CREATE INDEX idx2 ON `travel-sample`(name INCLUDE MISSING)"
        self.run_cbq_query(invalid_index)
        try:
            query = "select name from `travel-sample`"
            primary_query = "select name from `travel-sample` use index (def_primary)"

            # check explain before correct_index is in the system
            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("idx1" not in str(explain_plan) and ("def_primary" in str(explain_plan) or "#primary" in str(explain_plan)),
                            f"Idx1 should not be usable, primary index should be used, please check explain {explain_plan}")

            # create the correct new index and see if it is being used
            self.run_cbq_query(correct_index)
            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("idx2" in str(explain_plan) and "def_primary" not in str(explain_plan) and "#primary" not in str(explain_plan),
                            f"Idx2 should be used, please check explain {explain_plan}")

            actual_results = self.run_cbq_query(query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")
            self.run_cbq_query("drop index `travel-sample`.idx2")

    def test_lmk_primary(self):
        # use lmk like a primary index, does not matter what field you create include missing on
        lmk_index = "CREATE INDEX idx1 ON `travel-sample`(name INCLUDE MISSING)"
        self.run_cbq_query(lmk_index)
        self.run_cbq_query("drop index `travel-sample`.def_primary")
        self.run_cbq_query("drop primary index on `travel-sample`")
        try:
            query = "select * from `travel-sample` where address is missing"
            primary_query = "select * from `travel-sample` use index(def_primary) where address is missing"

            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue(
                "idx1" in str(explain_plan),
                f"Idx1 should be used, please check explain {explain_plan}")

            actual_results = self.run_cbq_query(query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

            # handle a special edge case
            special_query = "select 1 from `travel-sample`"
            special_results = self.run_cbq_query(special_query)
            self.assertEqual(special_results['metrics']['resultCount'], 31591)
            self.assertTrue("{'$1': 1}" in str(special_results))
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")

    def test_lmk_partial(self):
        # idx1 is better than the existing index def_type because of the where clause
        index = "CREATE INDEX idx1 ON `travel-sample`(`type` INCLUDE MISSING) where type = 'hotel'"
        self.run_cbq_query(index)
        try:
            query = "select * from `travel-sample` where type = 'hotel'"
            primary_query = "select * from `travel-sample` use index (def_primary) where type = 'hotel'"

            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("idx1" in str(explain_plan),
                            f"Idx1 is not being picked up, it should be, please check explain {explain_plan}")

            actual_results = self.run_cbq_query(query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

            query = "select * from `travel-sample` where type = 'airport'"
            primary_query = "select * from `travel-sample` use index (def_primary) where type = 'airport'"

            # now that the partial index doesnt match we expect def_type to be a superior index always
            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("def_type" in str(explain_plan),
                            f"def_type is not being picked up, it should be, please check explain {explain_plan}")

            actual_results = self.run_cbq_query(query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")

    def test_lmk_missing_clause_non_covering(self):
        #index1 is going to be ideal and comepeting index is going to be suboptimal, we expect idx1 to be picked
        index = "CREATE INDEX idx1 ON `travel-sample`(`faa` INCLUDE MISSING,`type`)"
        competeing_index = "CREATE INDEX idx2 ON `travel-sample`(`faa` INCLUDE MISSING)"
        self.run_cbq_query(index)
        self.run_cbq_query(competeing_index)
        try:
            query = "select name from `travel-sample` where faa is missing"
            primary_query = "select name from `travel-sample` use index (def_primary) where faa is missing"

            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("idx1" in str(explain_plan) or "idx2" in str(explain_plan), f"Idx1 is not being picked up, it should be, please check explain {explain_plan}")
            self.assertTrue("{'high': 'null', 'inclusion': 0, 'index_key': '`faa`'}" in str(explain_plan), f"Only the missing docs should be picked up, please check explain {explain_plan}")


            actual_results = self.run_cbq_query(query, query_params={"profile": "timings"})
            self.assertTrue(actual_results['profile']['phaseCounts']['indexScan'] == 29623, "Index should only be scanning the required number of docs instead of all docs")
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")
            self.run_cbq_query("drop index `travel-sample`.idx2")

    def test_lmk_missing_clause_covering(self):
        #index1 is going to be ideal and comepeting index is going to be suboptimal, we expect idx1 to be picked
        index = "CREATE INDEX idx1 ON `travel-sample`(`faa` INCLUDE MISSING,`type`)"
        competeing_index = "CREATE INDEX idx2 ON `travel-sample`(`faa` INCLUDE MISSING)"
        self.run_cbq_query(index)
        self.run_cbq_query(competeing_index)
        try:
            query = "select type from `travel-sample` where faa is missing"
            primary_query = "select type from `travel-sample` use index (def_primary) where faa is missing"

            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("idx1" in str(explain_plan), f"Idx1 is not being picked up, it should be, please check explain {explain_plan}")
            self.assertTrue("cover" in str(explain_plan))
            self.assertTrue("{'high': 'null', 'inclusion': 0, 'index_key': '`faa`'}" in str(explain_plan), f"Only the missing docs should be picked up, please check explain {explain_plan}")


            actual_results = self.run_cbq_query(query, query_params={"profile": "timings"})
            #we know the number of keys where faa is missing ahead of time, index should not be scanning every single doc as it would have before improvement
            self.assertTrue(actual_results['profile']['phaseCounts']['indexScan'] == 29623, "Index should only be scanning the required number of docs instead of all docs")
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")
            self.run_cbq_query("drop index `travel-sample`.idx2")

    def test_lmk_missing_non_leading(self):
        index = "CREATE INDEX idx1 ON `travel-sample`(name INCLUDE MISSING, airline)"
        competeing_index = "CREATE INDEX idx2 ON `travel-sample`(name INCLUDE MISSING)"
        self.run_cbq_query(index)
        self.run_cbq_query(competeing_index)
        try:
            query = "select name from `travel-sample` where airline is missing"
            primary_query = "select name from `travel-sample` use index(def_primary) where airline is missing"

            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("idx1" in str(explain_plan), f"Idx1 is not being picked up, it should be, please check explain {explain_plan}")
            self.assertTrue("cover" in str(explain_plan), f"Index should be covering, please check explain {explain_plan}")
            self.assertTrue("{'high': 'null', 'inclusion': 0, 'index_key': '`airline`'}" in str(explain_plan), f"Only the missing docs should be picked up, please check explain {explain_plan}")


            actual_results = self.run_cbq_query(query,query_params={"profile": "timings"})
            self.assertTrue(actual_results['profile']['phaseCounts']['indexScan'] == 7567, "Index should only be scanning the required number of docs instead of all docs")
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")
            self.run_cbq_query("drop index `travel-sample`.idx2")

    def test_lmk_skip_key(self):
        index = "CREATE INDEX idx1 ON `travel-sample`(description INCLUDE MISSING, country, type)"
        self.run_cbq_query(index)
        try:
            query = "select * from `travel-sample` where type = 'hotel' and description like 'a%'"
            primary_query = "select * from `travel-sample` use index (def_primary) where type = 'hotel' and description like 'a%'"

            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("idx1" in str(explain_plan),
                            f"Idx1 is not being picked up, it should be, please check explain {explain_plan}")

            actual_results = self.run_cbq_query(query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")

    def test_lmk_skip_key_non_leading(self):
        index = "CREATE INDEX idx1 ON `travel-sample`(description INCLUDE MISSING, country, type)"
        self.run_cbq_query(index)
        try:
            query = "select * from `travel-sample` where lower(country) = 'united kingdom' and name like '%a'"
            primary_query = "select * from `travel-sample` use index (def_primary) where lower(country) = 'united kingdom' and name like '%a'"

            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("idx1" in str(explain_plan),
                            f"Idx1 is not being picked up, it should be, please check explain {explain_plan}")

            actual_results = self.run_cbq_query(query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")

    def test_lmk_early_order_desc(self):
        index = "CREATE INDEX idx1 ON landmark(city INCLUDE MISSING DESC, name)"
        self.run_cbq_query(index, query_context="default:`travel-sample`.inventory")
        try:
            query = "SELECT city, name,address FROM landmark where lower(city) = 'paris' ORDER BY name, city DESC LIMIT 5"
            primary_query = "SELECT city, name,address FROM landmark use index (def_inventory_landmark_primary) where lower(city) = 'paris' ORDER BY name, city DESC LIMIT 5"

            self.run_cbq_query("drop index `default`:`travel-sample`.inventory.landmark.def_inventory_landmark_city")
            explain_plan = self.run_cbq_query("EXPLAIN " + query, query_context="default:`travel-sample`.inventory")
            self.assertTrue("idx1" in str(explain_plan), f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("index_keys" in str(explain_plan), f"We expect early filter to take place here but it does not, please check plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`city`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            self.assertTrue("'_index_key ((`landmark`.`name`))'" in str(explain_plan), f"The wrong key is being early filtered! please check explain plan {explain_plan}")
            # we need to make sure the order is being applied in the correct sequence idx -> order -> offset -> fetch
            self.assertTrue(explain_plan['results'][0]['plan']['~children'][0]['~children'][1]['#operator'] == 'Order', f"Order is not being applied first, it should be. please check explain plan {explain_plan}")
            actual_results = self.run_cbq_query(query, query_context="default:`travel-sample`.inventory")
            expected_results = self.run_cbq_query(primary_query, query_context="default:`travel-sample`.inventory")
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index landmark.idx1", query_context="default:`travel-sample`.inventory")
            try:
                # recreate index that comes w/travel-sample so there are no run order dependencies
                self.run_cbq_query("CREATE INDEX `def_inventory_landmark_city` ON `travel-sample`.`inventory`.`landmark`(`city`)")
            except:
                pass

    def test_lmk_cte(self):
        index = "Create index idx1 on `travel-sample`(city INCLUDE MISSING,name,country)"
        self.run_cbq_query(index)

        try:
            select_query = "WITH cities as(select distinct raw city from `travel-sample` where city like '%s') select country from `travel-sample` where city in cities and country like 'United%' and id  > 28000"
            primary_query = "WITH cities as(select distinct raw city from `travel-sample` use index (def_primary) where city like '%s') select country from `travel-sample` use index (def_primary) where city in cities and country like 'United%' and id  > 28000"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")


            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")

    def test_lmk_subquery(self):
        index = "Create index idx1 on `travel-sample`(city INCLUDE MISSING,name,country)"
        self.run_cbq_query(index)

        try:
            select_query = "select t.name,t.id from (select name,id from `travel-sample` where city is missing and id between 28000 and 40000) t"
            primary_query = "select t.name,t.id from (select name,id from `travel-sample` use index (def_primary) where city is missing and id between 28000 and 40000) t"
            explain_query = "EXPLAIN " + select_query
            explain_plan = self.run_cbq_query(explain_query)
            self.assertTrue("idx1" in str(explain_plan),
                            f"Query is not using the correct index! check explain plan {explain_plan}")
            self.assertTrue("'range': [{'high': 'null', 'inclusion': 0, 'index_key': '`city`'}" in str(explain_plan), f"The index should only be fetching docs w/missing values, check explain plan {explain_plan}")

            actual_results = self.run_cbq_query(select_query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")

    def test_lmk_left_join(self):
        index = "CREATE INDEX idx1 ON `travel-sample`.`inventory`.`landmark`(`city` INCLUDE MISSING) "
        self.run_cbq_query(index)
        try:
            #cannot use primary index on right side of a join, so we will compare missing index to non missing secondary index
            query = "SELECT DISTINCT  MIN(aport.airportname) AS Airport__Name, MIN(aport.tz) AS Airport__Time, \
                     MIN(lmark.name) AS Landmark_Name \
                     FROM airport aport \
                     LEFT JOIN landmark lmark \
                     ON aport.city = lmark.city \
                     AND lmark.country = 'United States' \
                     GROUP BY aport.airportname \
                     ORDER BY aport.airportname LIMIT 10;"
            non_missing_query_results = self.run_cbq_query(query, query_context="default:`travel-sample`.inventory")

            # technically non missing index is a superior index and will be picked up, we need to drop it
            self.run_cbq_query("drop index `default`:`travel-sample`.inventory.landmark.def_inventory_landmark_city")
            explain_plan = self.run_cbq_query("EXPLAIN " + query, query_context= "default:`travel-sample`.inventory")
            self.assertTrue("idx1" in str(explain_plan),
                            f"idx1 is not being picked up, it should be, please check explain {explain_plan}")

            actual_results = self.run_cbq_query(query, query_context= "default:`travel-sample`.inventory")
            diffs = DeepDiff(actual_results['results'], non_missing_query_results['results'])
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.`inventory`.`landmark`.idx1")
            try:
                # recreate index that comes w/travel-sample so there are no run order dependencies
                self.run_cbq_query("CREATE INDEX `def_inventory_landmark_city` ON `travel-sample`.`inventory`.`landmark`(`city`)")
            except:
                pass

    def test_lmk_right_join(self):
        index = "CREATE INDEX idx1 ON `travel-sample`.`inventory`.`airport`(`city` INCLUDE MISSING) "
        self.run_cbq_query(index)
        try:
            #cannot use primary index on right side of a join, so we will compare missing index to non missing secondary index
            query = "SELECT DISTINCT  MIN(aport.airportname) AS Airport__Name, MIN(aport.tz) AS Airport__Time, \
                     MIN(lmark.name) AS Landmark_Name \
                     FROM airport aport \
                     RIGHT JOIN landmark lmark \
                     ON aport.city = lmark.city \
                     AND lmark.country = 'United States' \
                     GROUP BY aport.airportname \
                     ORDER BY aport.airportname LIMIT 10;"
            non_missing_query_results = self.run_cbq_query(query, query_context= "default:`travel-sample`.inventory")

            # technically non missing index is a superior index and will be picked up, we need to drop it
            self.run_cbq_query("drop index default:`travel-sample`.inventory.airport.def_inventory_airport_city")
            explain_plan = self.run_cbq_query("EXPLAIN " + query, query_context= "default:`travel-sample`.inventory")
            self.assertTrue("def_inventory_landmark_primary" in str(explain_plan),
                            f"def_inventory_landmark_primary is not being picked up, it should be, please check explain {explain_plan}")

            actual_results = self.run_cbq_query(query, query_context= "default:`travel-sample`.inventory")
            diffs = DeepDiff(actual_results['results'], non_missing_query_results['results'])
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.`inventory`.airport.idx1")
            try:
                # recreate index that comes w/travel-sample so there are no run order dependencies
                self.run_cbq_query("CREATE INDEX `def_inventory_airport_city` ON `travel-sample`.`inventory`.`airport`(`city`)")
            except:
                pass

    def test_lmk_join_neg(self):
        #joins cannot work w/primary indexes on the join terms, let us see if it recognizes when lmk is being used as a primary index
        index = "CREATE INDEX idx1 ON `travel-sample`.`inventory`.`landmark`(`title` INCLUDE MISSING) "
        self.run_cbq_query(index)
        try:
            #cannot use primary index on right side of a join, so we will compare missing index to non missing secondary index
            query = "SELECT DISTINCT  MIN(aport.airportname) AS Airport__Name, MIN(aport.tz) AS Airport__Time, \
                     MIN(lmark.name) AS Landmark_Name \
                     FROM airport aport \
                     LEFT JOIN landmark lmark \
                     ON aport.city = lmark.city \
                     AND lmark.country = 'United States' \
                     GROUP BY aport.airportname \
                     ORDER BY aport.airportname LIMIT 10;"

            # technically non missing index is a superior index and will be picked up, we need to drop it
            self.run_cbq_query("drop index default:`travel-sample`.inventory.landmark.def_inventory_landmark_city")
            explain_plan = self.run_cbq_query("EXPLAIN " + query, query_context= "default:`travel-sample`.inventory")
            self.assertTrue("idx1" not in str(explain_plan), f"This index should not be in the explain plan! Plan is {explain_plan}")
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.`inventory`.`landmark`.idx1")
            try:
                # recreate index that comes w/travel-sample so there are no run order dependencies
                self.run_cbq_query("CREATE INDEX `def_inventory_landmark_city` ON `travel-sample`.`inventory`.`landmark`(`city`)")
            except:
                pass

    def test_lmk_update(self):
        index = "CREATE INDEX idx1 ON `travel-sample`.`inventory`.`airport`(`faa` INCLUDE MISSING) "
        self.run_cbq_query(index)
        try:
            # cannot use primary index on right side of a join, so we will compare missing index to non missing secondary index
            query = "UPDATE `travel-sample`.inventory.airport AS a SET hotels = (SELECT  h.name, h.id FROM  `travel-sample`.inventory.hotel AS h WHERE h.city = 'Nice') WHERE a.faa = 'NCE' RETURNING a;"

            # technically non missing index is a superior index and will be picked up, we need to drop it
            self.run_cbq_query("drop index default:`travel-sample`.inventory.airport.def_inventory_airport_faa")
            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("idx1" in str(explain_plan),
                            f"idx1 is not being picked up, it should be, please check explain {explain_plan}")

            expected_result = [{'a': {'airportname': "Cote D\\'Azur", 'city': 'Nice', 'country': 'France', 'faa': 'NCE', 'geo': {'alt': 12, 'lat': 43.658411, 'lon': 7.215872}, 'hotels': [{'id': 20419, 'name': 'Best Western Hotel Riviera Nice'}, {'id': 20420, 'name': 'Hotel Anis'}, {'id': 20421, 'name': 'NH Nice'}, {'id': 20422, 'name': 'Hotel Suisse'}, {'id': 20423, 'name': 'Gounod'}, {'id': 20424, 'name': 'Grimaldi Hotel Nice'}, {'id': 20425, 'name': 'Negresco'}], 'icao': 'LFMN', 'id': 1354, 'type': 'airport', 'tz': 'Europe/Paris'}}]
            actual_results = self.run_cbq_query(query)
            diffs = DeepDiff(actual_results['results'], expected_result, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index default:`travel-sample`.`inventory`.airport.idx1")
            try:
                # recreate index that comes w/travel-sample so there are no run order dependencies
                self.run_cbq_query(
                    "CREATE INDEX `def_inventory_airport_faa` ON `travel-sample`.`inventory`.`airport`(`faa`)")
            except:
                pass

    def test_lmk_insert(self):
        # get the number of docs that have missing faa
        query_count = "SELECT COUNT(*) as count FROM `travel-sample` WHERE faa IS MISSING"
        result = self.run_cbq_query(query_count)
        count = result['results'][0]['count']
        self.log.info(f"count is: {count}")

        index = "CREATE INDEX idx1 ON `travel-sample`(`faa` INCLUDE MISSING) "
        self.run_cbq_query(index)
        self.sleep(10, "sleep for index to be online")
        
        try:
            query = "INSERT INTO `travel-sample`.inventory.hotel (KEY foo, VALUE bar) SELECT 'copy_' || meta(doc).id AS foo, doc AS bar FROM `travel-sample` AS doc where faa is missing"

            # technically non missing index is a superior index and will be picked up, we need to drop it
            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("idx1" in str(explain_plan),
                            f"idx1 is not being picked up, it should be, please check explain {explain_plan}")

            actual_results = self.run_cbq_query(query)
            self.assertEqual(actual_results['metrics']['mutationCount'], count, f"Mutation count is not correct, expected {count} got {actual_results['metrics']['mutationCount']}")
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")

    def test_lmk_delete(self):
        index = "CREATE INDEX idx1 ON `travel-sample`(`type` INCLUDE MISSING) "
        self.run_cbq_query(index)
        try:
            # cannot use primary index on right side of a join, so we will compare missing index to non missing secondary index
            query = "delete from `travel-sample` where type = 'hotel'"

            # technically non missing index is a superior index and will be picked up, we need to drop it
            self.run_cbq_query("drop index `travel-sample`.def_type")
            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue("idx1" in str(explain_plan),
                            f"idx1 is not being picked up, it should be, please check explain {explain_plan}")
            self.run_cbq_query(query)
            time.sleep(5)
            verify_delete_query = "select * from `travel-sample` where type = 'hotel'"
            verify_delete = self.run_cbq_query(verify_delete_query)
            self.assertEqual(verify_delete['metrics']['resultCount'], 0 , f"There shouldnt be any results after the delete, please check {verify_delete}")
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")
            try:
                # recreate index that comes w/travel-sample so there are no run order dependencies
                self.run_cbq_query(
                    "CREATE INDEX `def_type` ON `travel-sample`(`type`)")
            except:
                pass

    def test_lmk_array_index_flattening_named_params(self):
        self.run_cbq_query(
            query="create index idx1 on `travel-sample`(ALL ARRAY FLATTEN_KEYS(r.author INCLUDE MISSING,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking) where free_parking = True")
        if self.use_unnest:
            query = "SELECT * FROM `travel-sample` AS d unnest reviews as r WHERE r.author LIKE $author_name and r.ratings.Cleanliness > $cleaning_score " \
                    "AND d.free_parking = True AND d.country is not null"
            primary_query = "SELECT * FROM `travel-sample` AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author LIKE $author_name and r.ratings.Cleanliness > $cleaning_score " \
                            "AND d.free_parking = True AND d.country is not null"
            query2 = "SELECT * FROM `travel-sample` AS d unnest reviews as r WHERE r.author LIKE $author_name and r.ratings.Cleanliness > $cleaning_score AND d.free_parking = False"

        else:
            query = "SELECT * FROM `travel-sample` AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE $author_name and r.ratings.Cleanliness > $cleaning_score " \
                    "END AND free_parking = True AND country is not null"
            primary_query = "SELECT * FROM `travel-sample` AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author " \
                            "LIKE $author_name and r.ratings.Cleanliness > $cleaning_score END AND free_parking = True AND country is not null"
            query2 = "SELECT * FROM `travel-sample` AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE $author_name and r.ratings.Cleanliness > $cleaning_score END AND free_parking = False"
        try:
            # Ensure the query is actually using the flatten index instead of primary
            explain_results = self.run_cbq_query(query="EXPLAIN " + query,
                                                 query_params={'$author_name': "M%", "$cleaning_score": 1})
            self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                            "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                                explain_results))
            query_results = self.run_cbq_query(query=query, query_params={'$author_name': "M%", "$cleaning_score": 1})
            expected_results = self.run_cbq_query(query=primary_query,
                                                  query_params={'$author_name': "M%", "$cleaning_score": 1})
            diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

            # Ensure partial index is not selected when it does not apply
            explain_results = self.run_cbq_query(query="EXPLAIN " + query2,
                                                 query_params={'$author_name': "M%", "$cleaning_score": 1})
            self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary' or explain_results['results'][0]['plan']['~children'][0]['index'] == 'def_primary',
                            "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                                explain_results))
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")


    def test_lmk_array_index_flattening_missing(self):
        self.run_cbq_query(
            query="create index idx1 on `travel-sample`(ALL ARRAY FLATTEN_KEYS(r.author INCLUDE MISSING,r.ratings.Cleanliness) FOR r IN reviews END, email, free_parking) where free_parking = True")
        if self.use_unnest:
            query = "SELECT * FROM `travel-sample` AS d unnest reviews as r WHERE r.author IS MISSING " \
                    "AND d.free_parking = True AND d.country is not null"
            primary_query = "SELECT * FROM `travel-sample` AS d USE INDEX (`#primary`) unnest reviews as r WHERE r.author IS MISSING " \
                            "AND d.free_parking = True AND d.country is not null"
            query2 = "SELECT * FROM `travel-sample` AS d unnest reviews as r WHERE r.author IS MISSING AND d.free_parking = False"

        else:
            query = "SELECT * FROM `travel-sample` AS d WHERE ANY r IN d.reviews SATISFIES r.author IS MISSING " \
                    "END AND free_parking = True AND country is not null"
            primary_query = "SELECT * FROM `travel-sample` AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author " \
                            "IS MISSING END AND free_parking = True AND country is not null"
            query2 = "SELECT * FROM `travel-sample` AS d WHERE ANY r IN d.reviews SATISFIES r.author IS MISSING END AND free_parking = False"

        try:
            # Ensure the query is actually using the flatten index instead of primary
            explain_results = self.run_cbq_query(query="EXPLAIN " + query)
            self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'idx1',
                            "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                                explain_results))
            self.assertTrue("{'inclusion': 0, 'index_key': '(`r`.`author`)'}" in str(explain_results), f"all documents need to be fetched due to it being an array, check explain {explain_results}")
            query_results = self.run_cbq_query(query=query)
            expected_results = self.run_cbq_query(query=primary_query)
            diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)

            # Ensure partial index is not selected when it does not apply
            explain_results = self.run_cbq_query(query="EXPLAIN " + query2)
            self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['index'] == '#primary' or explain_results['results'][0]['plan']['~children'][0]['index'] == 'def_primary',
                            "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                                explain_results))
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx1")

    def test_lmk_advise(self):
        query = "select type from `travel-sample`"
        primary_query = "select type from `travel-sample` use index (def_primary)"

        # get advise for index needed for query
        advise_output = self.run_cbq_query("ADVISE " + query)
        self.assertTrue(advise_output['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'] == "CREATE INDEX adv_type ON `travel-sample`(`type` INCLUDE MISSING)")
        try:
            self.run_cbq_query(advise_output['results'][0]['advice']['adviseinfo']['recommended_indexes']['covering_indexes'][0]['index_statement'])

            actual_results = self.run_cbq_query(query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index `travel-sample`.adv_type")

    def test_lmk_array_index_flattening_advise(self):
        query = "SELECT * FROM `travel-sample` AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE $author_name and r.ratings.Cleanliness > $cleaning_score END AND faa is missing"
        primary_query = "SELECT * FROM `travel-sample` AS d USE INDEX (`#primary`) WHERE ANY r IN d.reviews SATISFIES r.author " \
                        "LIKE $author_name and r.ratings.Cleanliness > $cleaning_score END AND faa is missing"

        advise_output = self.run_cbq_query("ADVISE " + query)
        self.run_cbq_query(advise_output['results'][0]['advice']['adviseinfo']['recommended_indexes']['indexes'][0]['index_statement'])
        # Ensure the query is actually using the flatten index instead of primary
        explain_results = self.run_cbq_query(query="EXPLAIN " + query,
                                             query_params={'$author_name': "M%", "$cleaning_score": 1})
        self.assertTrue(explain_results['results'][0]['plan']['~children'][0]['scan']['index'] == 'adv_faaISMISSING_DISTINCT_reviews_author_ratings_Cleanliness',
                        "The correct index is not being used or the plan is different than expected! Expected idx1 got {0}".format(
                            explain_results))
        query_results = self.run_cbq_query(query=query, query_params={'$author_name': "M%", "$cleaning_score": 1})
        expected_results = self.run_cbq_query(query=primary_query,
                                              query_params={'$author_name': "M%", "$cleaning_score": 1})
        diffs = DeepDiff(query_results['results'], expected_results['results'], ignore_order=True)
        if diffs:
            self.assertTrue(False, diffs)

    def test_lmk_cbo(self):
        index = "CREATE INDEX idx_city_country ON `travel-sample`(faa INCLUDE MISSING, country)"
        self.run_cbq_query(index)
        self.run_cbq_query("UPDATE STATISTICS FOR INDEX `travel-sample`.idx_city_country")
        try:
            query = "select country, name from `travel-sample` where faa is missing"
            primary_query = "select country,name from `travel-sample` use index (def_primary) where faa is missing"

            explain_plan = self.run_cbq_query("EXPLAIN " + query)
            self.assertTrue('optimizer_estimates' in str(explain_plan), f"We updated stats on this field, it should now have cost and cardinality estimates, please check {explain_plan}")
            self.assertTrue(
                "idx_city_country" in str(explain_plan),
                f"idx_city_country should be used, please check explain {explain_plan}")

            actual_results = self.run_cbq_query(query)
            expected_results = self.run_cbq_query(primary_query)
            diffs = DeepDiff(actual_results['results'], expected_results['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            self.run_cbq_query("drop index `travel-sample`.idx_city_country")

    def test_lmk_limit_pushdown(self):
        # MB-54859
        collection_name = "shellTest"

        # Create the collection
        self.run_cbq_query(f"CREATE COLLECTION `default`.`_default`.`{collection_name}`")

        # Insert some test data
        test_data = [
            {"test_id": "advise", "c11": "value1"},
            {"test_id": "advise", "c11": "value2"},
            {"test_id": "advise", "c11": "value3"},
            {"test_id": "advise", "c11": "value4"},
            {"test_id": "advise", "c11": "value5"},
            {"test_id": "other", "c11": "other_value1"},
            {"test_id": "other", "c11": "other_value2"}
        ]

        for doc in test_data:
            # Insert with explicit doc key to include the doc key
            doc_key = f"{doc['test_id']}_{doc['c11']}"
            self.run_cbq_query(f"INSERT INTO `default`.`_default`.`{collection_name}` (KEY, VALUE) VALUES ('{doc_key}', {doc})")

        try:
            # Create index with INCLUDE MISSING and WHERE clause
            index_query = f"CREATE INDEX adv_c11 ON `default`.`_default`.`{collection_name}`(`c11` INCLUDE MISSING) WHERE `test_id` = 'advise'"
            self.run_cbq_query(index_query)

            # Test query with LIMIT
            query = f"SELECT c11 FROM `default`.`_default`.`{collection_name}` WHERE test_id = 'advise' LIMIT 2"
            explain_query = f"EXPLAIN {query}"

            explain_plan = self.run_cbq_query(explain_query)

            # Check that the index is being used in an IndexScan3 operator and that limit is pushed down
            plan = explain_plan['results'][0]['plan']
            found_indexscan3 = False
            found_limit_in_indexscan3 = False

            nodes_to_check = [plan]
            while nodes_to_check:
                node = nodes_to_check.pop()
                if isinstance(node, dict):
                    if node.get('#operator') == 'IndexScan3':
                        found_indexscan3 = True
                        if node.get('index') == 'adv_c11':
                            if 'limit' in node and str(node['limit']) == "2":
                                found_limit_in_indexscan3 = True
                    for v in node.values():
                        if isinstance(v, (dict, list)):
                            nodes_to_check.append(v)
                elif isinstance(node, list):
                    for item in node:
                        if isinstance(item, (dict, list)):
                            nodes_to_check.append(item)

            self.assertTrue(found_indexscan3, f"IndexScan3 operator not found in plan: {plan}")
            self.assertTrue(found_limit_in_indexscan3, f"Limit not pushed down to IndexScan3 operator in plan: {plan}")

            # Verify the actual results
            actual_results = self.run_cbq_query(query)
            self.assertEqual(len(actual_results['results']), 2, 
                           f"Should return exactly 2 results, got {len(actual_results['results'])}")
        finally:
            # Clean up: just drop collection if exists
            self.run_cbq_query(f"DROP COLLECTION `default`.`_default`.`{collection_name}` IF EXISTS")