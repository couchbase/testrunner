from tuqquery.tuq import QueryTests
import time
from membase.api.exception import CBQError

class CbasHintsTests(QueryTests):

    def setUp(self):
        super(CbasHintsTests, self).setUp()
        self.log.info("==============  CbasHintsTests setup has started ==============")
        self.hash_hint = self.input.param("hash_hint", "hashbcast")
        self.productivity = self.input.param("productivity", 1000.0)
        self.cardinality = {
            'airline': 187,
            'route': 24024,
            'airport': 1968
        }
        self.log.info("==============  CbasHintsTests setup has completed ==============")

    def suite_setUp(self):
        super(CbasHintsTests, self).suite_setUp()
        self.log.info("==============  CbasHintsTests suite_setup has started ==============")
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
        self.log.info("==============  CbasHintsTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  CbasHintsTests tearDown has started ==============")
        self.log.info("==============  CbasHintsTests tearDown has completed ==============")
        super(CbasHintsTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  CbasHintsTests suite_tearDown has started ==============")
        travel_sample = self.get_bucket_from_name("travel-sample")
        if travel_sample:
            self.analytics = True
            self.run_cbq_query("DROP DATASET travel if exists")
            self.run_cbq_query("DROP DATASET airline if exists")
            self.run_cbq_query("DROP DATASET airport if exists")
            self.run_cbq_query("DROP DATASET route if exists")
            self.analytics = False
            self.delete_bucket(travel_sample)
        self.log.info("==============  CbasHintsTests suite_tearDown has completed ==============")
        super(CbasHintsTests, self).suite_tearDown()

    def _get_index_stats(self, dataset):
        result = self.run_cbq_query(f"SELECT * FROM Metadata.`Index` WHERE IndexStructure = 'SAMPLE' AND DatasetName = '{dataset}'", is_analytics=True)
        return result['results'][0]['Index']

    def run_analyze(self, dataset='airline'):
        self.run_cbq_query(f"ANALYZE ANALYTICS COLLECTION {dataset}", is_analytics=True)

    def test_hints_warnings(self):
        hints_warning = {
            'hashbcast': 'Unexpected hint: hashbcast. "hash-bcast", "indexnl", "hashjoin", "skip-index", "use-index", "selectivity", "productivity" expected at this location (in line 1, at column 102)',
            'hash-bcast (z)': 'Could not apply broadcast hash join hint: broadcast z (in line 1, at column 122)',
            'hashjoin': 'Invalid specification for hint hashjoin. Expected hash join build/probe collection name (in line 1, at column 102)',
            'hashjoin probe': 'Invalid specification for hint hashjoin. ASX1001: Syntax error: In line 1 >>probe<< Encountered <EOF> at column 5.  (in line 1, at column 102)',
            'hashjoin build': 'Invalid specification for hint hashjoin. ASX1001: Syntax error: In line 1 >>build<< Encountered <EOF> at column 5.  (in line 1, at column 102)',
            'hashjoin probe(z)': 'Could not apply hash join hint: probe with z (in line 1, at column 125)',
            'hashjoin build(z)': 'Could not apply hash join hint: build with z (in line 1, at column 125)',
            'hashjoin probing(r)': 'Invalid specification for hint hashjoin. ASX1001: Syntax error: The string after hashjoin has to be "build" or "probe". (in line 1, at column 10) (in line 1, at column 102)',
            'hashjoin build()': 'Invalid specification for hint hashjoin. ASX1001: Syntax error: In line 1 >>build()<< Encountered ")" at column 7.  (in line 1, at column 102)',
            'hashjoin probe()': 'Invalid specification for hint hashjoin. ASX1001: Syntax error: In line 1 >>probe()<< Encountered ")" at column 7.  (in line 1, at column 102)',
            'productivity': 'Invalid specification for hint productivity. Expected productivity collection name and value (in line 1, at column 102)',
            'productivity 100.0': 'Invalid specification for hint productivity. Invalid format for productivity values (in line 1, at column 102)',
            'productivity z 600.0': 'Could not apply productivity hint: Invalid collection name/alias: z (in line 1, at column 128)',
            'productivity a': 'Invalid specification for hint productivity. Invalid format for productivity values (in line 1, at column 102)',
            'productivity a r': 'Invalid specification for hint productivity. Invalid format for productivity values (in line 1, at column 102)',
            'productivity r 100': 'Invalid specification for hint productivity. Invalid format for productivity values (in line 1, at column 102)',
            'productivity a -10.0': 'Invalid specification for hint productivity. Invalid format for productivity values (in line 1, at column 102)',
            'productivity a 0.0': 'Could not apply productivity hint: Productivity specified: 0.0, has to be a decimal value greater than 0 (in line 1, at column 126)',
        }
        for collection in ["route", "airport"]:
            analyze = self.run_cbq_query(f"ANALYZE ANALYTICS COLLECTION {collection}", is_analytics=True)
        hash_join = f"SELECT COUNT(DISTINCT r.destinationairport) FROM airport a JOIN route r ON r.sourceairport /*+ {self.hash_hint} */ = a.faa WHERE a.city = 'San Francisco' AND a.country = 'United States'"
        explain = self.run_cbq_query(f"EXPLAIN {hash_join};", is_analytics=True)
        self.assertEqual(explain["warnings"][0]["msg"], hints_warning[self.hash_hint])
        result = self.run_cbq_query(hash_join, is_analytics=True)
        self.assertEqual(result['results'], [{'$1': 104}])

    @staticmethod
    def _find_join_operator(plan):
        while True:
            if "operator" not in plan.keys():
                return []
            if plan['operator'] == 'join':
                return plan
            plan = plan['inputs'][0]

    def test_hints_hashjoin(self):
        expected = {
            'build(r)': {'cardinality': 24024.0, 'op-cost': 24024.0, 'total-cost': 72457.26999999999},
            'probe(a)': {'cardinality': 24024.0, 'op-cost': 24024.0, 'total-cost': 72457.26999999999},
            'build(a)': {'cardinality': 1.85, 'op-cost': 1.85, 'total-cost': 1977.6299999999999},
            'probe(r)': {'cardinality': 1.85, 'op-cost': 1.85, 'total-cost': 1977.6299999999999},
        }
        for collection in ["route", "airport"]:
            analyze = self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {collection} WITH {{"sample-seed":1000}}', is_analytics=True)
        hash_join = f"SELECT COUNT(DISTINCT r.destinationairport) FROM airport a JOIN route r ON r.sourceairport /*+ hashjoin {self.hash_hint} */ = a.faa WHERE a.city = 'San Francisco' AND a.country = 'United States'"
        explain = self.run_cbq_query(f"EXPLAIN {hash_join};", is_analytics=True)
        join = self._find_join_operator(explain['results'][0])
        self.assertEqual(join['build-side'], 0)
        self.assertEqual(join['inputs'][0]['optimizer-estimates'], expected[self.hash_hint])
        result = self.run_cbq_query(hash_join, is_analytics=True)
        self.assertEqual(result['results'], [{'$1': 104}])

    def test_hints_productivity(self):
        for collection in ["route", "airport"]:
            analyze = self.run_cbq_query(f'ANALYZE ANALYTICS COLLECTION {collection} WITH {{"sample-seed":2000}}', is_analytics=True)
        hash_join = f"SELECT COUNT(DISTINCT r.destinationairport) FROM airport a JOIN route r ON r.sourceairport /*+ productivity a {self.productivity} */ = a.faa WHERE a.city = 'San Francisco' AND a.country = 'United States'"
        explain = self.run_cbq_query(f"EXPLAIN {hash_join};", is_analytics=True)
        join = self._find_join_operator(explain['results'][0])
        if self.productivity == 0.0:
            self.assertEqual(join['optimizer-estimates']['cardinality'], 0)
        else:
            expected_cardinality = round(join['optimizer-estimates']['cardinality']/self.productivity, 2)
            actual_cardinality = join['inputs'][0]['optimizer-estimates']['cardinality']
            self.log.info(f"expected_cardinality: {expected_cardinality}, actual_cardinality: {actual_cardinality}")
            try:
                self.assertEqual(expected_cardinality, actual_cardinality)
            except AssertionError:
                if expected_cardinality == 2.1: # https://jira.issues.couchbase.com/browse/MB-68278
                    self.assertEqual(expected_cardinality, 2.1)
                else:
                    raise AssertionError(f"Cardinality mismatch: expected {expected_cardinality}, actual {actual_cardinality}")
        result = self.run_cbq_query(hash_join, is_analytics=True)
        self.assertEqual(result['results'], [{'$1': 104}])

    def test_hints_hashbcast(self):
        for collection in ["route", "airport"]:
            analyze = self.run_cbq_query(f"ANALYZE ANALYTICS COLLECTION {collection}", is_analytics=True)
        hash_join = f"SELECT COUNT(DISTINCT r.destinationairport) FROM airport a JOIN route r ON r.sourceairport /*+ {self.hash_hint} */ = a.faa WHERE a.city = 'San Francisco' AND a.country = 'United States'"
        explain = self.run_cbq_query(f"EXPLAIN {hash_join};", is_analytics=True)
        join = self._find_join_operator(explain['results'][0])
        self.assertEqual(join['inputs'][0]['physical-operator'], 'BROADCAST_EXCHANGE')
        result = self.run_cbq_query(hash_join, is_analytics=True)
        self.assertEqual(result['results'], [{'$1': 104}])