from tuqquery.tuq import QueryTests
import time
from membase.api.exception import CBQError
from lib.couchbase_helper.analytics_helper import AnalyticsHelper
import threading

class CbasAnalyzeTests(QueryTests):

    def setUp(self):
        super(CbasAnalyzeTests, self).setUp()
        self.log.info("==============  CbasAnalyzeTests setup has started ==============")
        self.sample_size = self.input.param("sample_size", "low")
        self.sample_seed = self.input.param("sample_seed", 123456)
        self.reuse_seed = self.input.param("reuse_seed", False)
        self.collection_name = self.input.param("collection_name", "route")
        self.cardinality = {
            'airline': 187,
            'route': 24024,
            'airport': 1968
        }
        self.repeat_mutation = 20
        self.count_mutation = 100
        self.analytics_helper = AnalyticsHelper()
        self.log.info("==============  CbasAnalyzeTests setup has completed ==============")

    def suite_setUp(self):
        super(CbasAnalyzeTests, self).suite_setUp()
        self.log.info("==============  CbasAnalyzeTests suite_setup has started ==============")
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
        self.analytics = True
        self.run_cbq_query("CREATE DATASET if not exists travel on `travel-sample`")
        self.run_cbq_query("CREATE DATASET if not exists airline on `travel-sample`.inventory.airline")
        self.run_cbq_query("CREATE DATASET if not exists airport on `travel-sample`.inventory.airport")
        self.run_cbq_query("CREATE DATASET if not exists route on `travel-sample`.inventory.route")
        self.analytics = False
        self.log.info("==============  CbasAnalyzeTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  CbasAnalyzeTests tearDown has started ==============")
        self.log.info("==============  CbasAnalyzeTests tearDown has completed ==============")
        super(CbasAnalyzeTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  CbasAnalyzeTests suite_tearDown has started ==============")
        travel_sample = self.get_bucket_from_name("travel-sample")
        if travel_sample:
            self.analytics = True
            self.run_cbq_query("DROP DATASET travel if exists")
            self.run_cbq_query("DROP DATASET airline if exists")
            self.run_cbq_query("DROP DATASET airport if exists")
            self.run_cbq_query("DROP DATASET route if exists")
            self.analytics = False
            self.delete_bucket(travel_sample)
        self.log.info("==============  CbasAnalyzeTests suite_tearDown has completed ==============")
        super(CbasAnalyzeTests, self).suite_tearDown()

    def _get_index_stats(self, dataset):
        self.analytics = True
        result = self.run_cbq_query(f"SELECT * FROM Metadata.`Index` WHERE IndexStructure = 'SAMPLE' AND DatasetName = '{dataset}'")
        self.analytics = False
        return result['results'][0]['Index']

    def test_analyze_collection(self):
        self.analytics = True
        self.run_cbq_query(f"ANALYZE ANALYTICS COLLECTION {self.collection_name}")
        stats = self._get_index_stats(self.collection_name)
        self.assertEqual(stats['SourceCardinality'], self.cardinality[self.collection_name])
        self.assertEqual(stats['SampleCardinalityTarget'], 1063)
        self.analytics = False

    def test_analyze_with_size(self):
        samples = {
            'low': 1063,
            'medium': 1063*4,
            'high': 1063*4*4
        }
        if type(self.sample_size) in [int, float]:
            if self.sample_size >= 1063 and self.sample_size <= 17008:
                samples[self.sample_size] = int(self.sample_size)
                self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {self.collection_name} WITH {{"sample":{self.sample_size}}}', is_analytics=True)
            else:
                try:
                    self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {self.collection_name} WITH {{"sample":{self.sample_size}}}', is_analytics=True)
                    self.assertFalse('ANALYZE should have failed since value should be between 1063 and 17008')
                except CBQError as ex:
                    error = self.process_CBQE(ex)
                    self.assertEqual(error['code'], 24174)
                    self.assertEqual(error['msg'], 'Sample size has to be between 1063 and 17008')
                    return
        else:
            self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {self.collection_name} WITH {{"sample":"{self.sample_size}"}}', is_analytics=True)

        stats = self._get_index_stats(self.collection_name)
        self.assertEqual(stats['SourceCardinality'], self.cardinality[self.collection_name])
        self.assertEqual(stats['SampleCardinalityTarget'], samples[self.sample_size])
        if self.reuse_seed:
            self.analytics = True
            dump_index = self.run_cbq_query(f'SET `import-private-functions` `true`; FROM DUMP_INDEX("{stats["DataverseName"]}", "{stats["DatasetName"]}", "{stats["IndexName"]}") AS v SELECT v.values[0] as k ORDER BY k asc;', is_analytics=True)
            sample_docs_before = dump_index['results']
            seed = stats['SampleSeed']
            self.run_cbq_query(f"ANALYZE ANALYTICS COLLECTION {self.collection_name} DROP STATISTICS")
            if type(self.sample_size) in [int, float]:
                self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {self.collection_name} WITH {{"sample":{self.sample_size}, "sample-seed":{seed}}}')
            else:
                self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {self.collection_name} WITH {{"sample":"{self.sample_size}", "sample-seed":{seed}}}')
            stats = self._get_index_stats(self.collection_name)
            self.assertEqual(stats['SourceCardinality'], self.cardinality[self.collection_name])
            self.assertEqual(stats['SampleCardinalityTarget'], samples[self.sample_size])
            dump_index = self.run_cbq_query(f'SET `import-private-functions` `true`; FROM DUMP_INDEX("{stats["DataverseName"]}", "{stats["DatasetName"]}", "{stats["IndexName"]}") AS v SELECT v.values[0] as k ORDER BY k asc;', is_analytics=True)
            sample_docs_after = dump_index['results']
            self.assertEqual(sample_docs_before, sample_docs_after)
            self.analytics = False

    def test_analyze_with_seed(self):
        self.analytics = True
        self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {self.collection_name} WITH {{"sample-seed":{self.sample_seed}}}')
        stats = self._get_index_stats(self.collection_name)
        self.assertEqual(stats['SourceCardinality'], self.cardinality[self.collection_name])
        expected_seed = self.sample_seed
        if type(self.sample_seed) is float:
            expected_seed = int(self.sample_seed)
        self.assertEqual(stats['SampleSeed'], expected_seed)
        self.analytics = False

    def test_analyze_with_size_invalid(self):
        self.analytics = True
        try:
            self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {self.collection_name} WITH {{"sample":"{self.sample_size}"}}')
            self.assertFalse('ANALYZE should have failed but did not')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.analytics = False
            self.assertEqual(error['code'], 24173)
            self.assertEqual(error['msg'], 'Sample size options are "low", "medium", "high", or a number')

    def test_analyze_with_seed_invalid(self):
        self.analytics = True
        try:
            self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {self.collection_name} WITH {{"sample-seed":{self.sample_seed}}}')
            self.assertFalse('ANALYZE should have failed but did not')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.analytics = False
            self.assertEqual(error['code'], 24000)
            self.assertTrue(error['msg'] == "Syntax error: ASX1177: Sample seed has to be a number or a string convertible to a number (in line 1, at column 1)" or 'Could not parse numeric literal' in error['msg'])
    
    def test_analyze_with_size_seed(self):
        samples = {
            'low': 1063,
            'medium': 1063*4,
            'high': 1063*4*4
        }
        self.analytics = True
        self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {self.collection_name} WITH {{"sample":"{self.sample_size}", "sample-seed":{self.sample_seed}}}')
        stats = self._get_index_stats(self.collection_name)
        self.assertEqual(stats['SourceCardinality'], self.cardinality[self.collection_name])
        self.assertEqual(stats['SampleCardinalityTarget'], samples[self.sample_size])
        expected_seed = self.sample_seed
        if type(self.sample_seed) is float:
            expected_seed = int(self.sample_seed)
        self.assertEqual(stats['SampleSeed'], expected_seed)
        self.analytics = False

    def test_analyze_with_invalid(self):
        self.analytics = True
        try:
            self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {self.collection_name} WITH {{"sample-size":"{self.sample_size}"}}')
        except CBQError as ex:
            error = self.process_CBQE(ex)
            self.assertEqual(error['code'], 24000)
            self.assertEqual(error['msg'], "Syntax error: ASX0050: Invalid parameter 'sample-size' (in line 1, at column 1)")
        self.analytics = False

    def test_analyze_drop(self):
        self.run_cbq_query(f"ANALYZE ANALYTICS COLLECTION {self.collection_name}", is_analytics=True)
        stats = self._get_index_stats(self.collection_name)
        self.assertEqual(stats['SourceCardinality'], self.cardinality[self.collection_name])
        self.assertEqual(stats['SampleCardinalityTarget'], 1063)
        self.run_cbq_query(f"ANALYZE ANALYTICS COLLECTION {self.collection_name} DROP STATISTICS", is_analytics=True)
        result = self.run_cbq_query(f"SELECT * FROM Metadata.`Index` WHERE IndexStructure = 'SAMPLE' AND DatasetName = '{self.collection_name}'", is_analytics=True)
        self.assertEqual(result['results'], [])

    def test_analyze_mutation(self):
        self.run_cbq_query("DELETE FROM `travel-sample`.inventory.airline WHERE country = 'Patriam'")
        source_card = self.cardinality['airline']
        for i in range(self.repeat_mutation):
            self.analytics = True
            self.run_cbq_query(f"ANALYZE ANALYTICS COLLECTION airline")
            stats = self._get_index_stats("airline")
            self.assertEqual(stats['SourceCardinality'], source_card)
            self.analytics = False
            self.run_cbq_query(f'INSERT INTO `travel-sample`.inventory.airline (KEY k, VALUE v) SELECT "new"||TO_STR(d) AS k , {{"id":d, "type":"airline", "name":"name"||TO_STR(d), "country": "Patriam"}} AS v FROM ARRAY_RANGE({i*self.count_mutation}, {(i+1)*self.count_mutation}) AS d')
            source_card += self.count_mutation
            self.sleep(3)

    def run_analyze(self):
        self.run_cbq_query(f"ANALYZE ANALYTICS COLLECTION airline", is_analytics=True)

    def run_insert(self):
        self.run_cbq_query("DELETE FROM `travel-sample`.inventory.airline WHERE country = 'Patriam'")
        self.run_cbq_query(f'INSERT INTO `travel-sample`.inventory.airline (KEY k, VALUE v) SELECT "new"||TO_STR(d) AS k , {{"id":d, "type":"airline", "name":"name"||TO_STR(d), "country": "Patriam"}} AS v FROM ARRAY_RANGE(0, 1000) AS d')
        self.run_cbq_query(f'INSERT INTO `travel-sample`.inventory.airline (KEY k, VALUE v) SELECT "new"||TO_STR(d) AS k , {{"id":d, "type":"airline", "name":"name"||TO_STR(d), "country": "Patriam"}} AS v FROM ARRAY_RANGE(1000, 2000) AS d')

    def test_analyze_parallel(self):
        threads = []

        # Setup threads with mutation (insert) and multiple analyze
        th = threading.Thread(target=self.run_insert,args=())
        threads.append(th)

        for i in range(5):
            th = threading.Thread(target=self.run_analyze,args=())
            threads.append(th)

        # Start threads
        for i in range(len(threads)):
            threads[i].start()

        # Wait for threads to finish
        for i in range(len(threads)):
            threads[i].join()
