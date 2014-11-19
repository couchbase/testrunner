from tuqquery.tuq import QueryTests
from couchbase.documentgenerator import DocumentGenerator

class NULLTests(QueryTests):
    def setUp(self):
        super(NULLTests, self).setUp()
        self.gens_load = self.generate_docs()
        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket=bucket,
                                  timeout=self.wait_timeout * 5)
        self.load(self.gens_load)
        self.create_primary_index_for_3_0_and_greater()

    def suite_setUp(self):
        super(NULLTests, self).suite_setUp()

    def tearDown(self):
        super(NULLTests, self).tearDown()

    def suite_tearDown(self):
        super(NULLTests, self).suite_tearDown()

    def test_null_query(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE coverage_tests.P0 IS NULL ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P0" in doc['coverage_tests'] and\
                                doc['coverage_tests']['P0'] is None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_not_null_query(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE coverage_tests.P0 IS NOT NULL ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P0" in doc['coverage_tests'] and\
                                doc['coverage_tests']['P0'] is not None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_missing_query(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE coverage_tests.P0 IS MISSING ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if not "P0" in doc['coverage_tests']]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_not_missing_query(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE coverage_tests.P1 IS NOT MISSING ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P1" in doc['coverage_tests']]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_valued(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE coverage_tests.P0 IS VALUED ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P0" in doc['coverage_tests'] and\
                                doc['coverage_tests']['P0'] is not None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_not_valued(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE coverage_tests.P0 IS NOT VALUED ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P0" not in doc['coverage_tests'] or\
                                doc['coverage_tests']['P0'] is None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_precedense(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE 2+2=4 AND coverage_tests.P0 IS VALUED "
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P0" in doc['coverage_tests'] and\
                                doc['coverage_tests']['P0'] is not None]
            expected_result = sorted(expected_result)
            actual_result = actual_result['results']
            self._verify_results(actual_result, expected_result)

    def test_nulls_over(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE ANY story_point_n IN story_point SATISFIES story_point_n IS NULL END ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if len([point for point in doc['story_point'] if point is None]) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE EVERY story_point_n IN story_point SATISFIES story_point_n IS NULL END ORDER BY feature_name"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if len([point for point in doc['story_point']
                                       if point is None]) == len(doc['story_point'])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_ifnan(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name, IFNAN(story_point[2],story_point[1]) as point" +\
                        " FROM %s ORDER BY feature_name"  % bucket.name
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = []
            for doc in full_list:
                if len(doc['story_point']) < 3:
                    expected_result.append({'feature_name' : doc['feature_name'],
                                            'point': None})
                else:
                    expected_result.append({'feature_name' : doc['feature_name'],
                                            'point' : doc['story_point'][2]})
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_ifnull(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name, IFNAN(story_point[2],story_point[1]) as point" +\
                        " FROM %s ORDER BY feature_name"  % bucket.name
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = []
            for doc in full_list:
                if len(doc['story_point']) < 3:
                    expected_result.append({'feature_name' : doc['feature_name'],
                                            'points' : None})
                else:
                    expected_result.append({'feature_name' : doc['feature_name'],
                                            'point' : doc['story_point'][2]})
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_firstnum(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name, FIRSTNUM(story_point[2],story_point[1]) as point" +\
                        " FROM %s ORDER BY feature_name"  % bucket.name
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = []
            for doc in full_list:
                if len(doc['story_point']) < 3:
                    expected_result.append({'feature_name' : doc['feature_name'],
                                            'point' : None})
                elif doc['story_point'][2] is None:
                    expected_result.append({'feature_name' : doc['feature_name'],
                                            'point' : doc['story_point'][1]})
                else:
                    expected_result.append({'feature_name' : doc['feature_name'],
                                            'point' : doc['story_point'][2]})
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_nanif(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name, NANIF(story_points[0],story_point[0])" +\
                        " FROM %s ORDER BY feature_name"  % bucket.name
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_ifposinf(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name, IFPOSINF(story_point[2],story_point[1]) as point" +\
                        " FROM %s ORDER BY feature_name"  % bucket.name
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = []
            for doc in full_list:
                if len(doc['story_point']) < 3:
                    expected_result.append({'feature_name' : doc['feature_name'],
                                            'point' : None})
                else:
                    expected_result.append({'feature_name' : doc['feature_name'],
                                            'point' : doc['story_point'][2]})
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)
            self.query = "SELECT feature_name, IFNEGINF(story_point[2],story_point[1]) as point" +\
                        " FROM %s ORDER BY feature_name"  % bucket.name
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], expected_result)

    def test_ifinf(self):
        queries = ["SELECT feature_name, IFINF(story_point[2],story_point[1]) as point" +\
                   " FROM %s ORDER BY feature_name",
                   "SELECT feature_name, IFNANORINF(story_point[2],story_point[1]) as point" +\
                   " FROM %s ORDER BY feature_name",]
        full_list = self._generate_full_docs_list(self.gens_load)
        expected_result = []
        for doc in full_list:
            if len(doc['story_point']) < 3:
                expected_result.append({'feature_name' : doc['feature_name'],
                                        'point': None})
            else:
                expected_result.append({'feature_name' : doc['feature_name'],
                                            'point' : doc['story_point'][2]})
        expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
        for bucket in self.buckets:
            for query in queries:
                self.run_cbq_query(query % bucket)
                self.sleep(3)
                actual_result = self.run_cbq_query(query % bucket)
                self._verify_results(actual_result['results'], expected_result)

    def test_ifmissing(self):
        queries = ["SELECT feature_name, IFMISSINGORNULL(coverage_tests.P0," +\
                        "coverage_tests.P4) as C FROM %s ORDER BY feature_name",
                    "SELECT feature_name, IFMISSING(coverage_tests.P0," +\
                        "coverage_tests.P4) as C FROM %s ORDER BY feature_name"]
        full_list = self._generate_full_docs_list(self.gens_load)
        expected_result = []
        for doc in full_list:
            if "P0" in doc['coverage_tests']:
                expected_result.append({'feature_name' : doc['feature_name'],
                                        'C' : doc['coverage_tests']['P0']})
            else:
                expected_result.append({'feature_name' : doc['feature_name'],
                                        'C' : doc['coverage_tests']['P4']})
        expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
        for bucket in self.buckets:
            for query in queries:
                self.run_cbq_query(query % bucket)
                self.sleep(3)
                actual_result = self.run_cbq_query(query % bucket)
                self._verify_results(actual_result['results'], expected_result)

    def test_missingif(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name, MISSINGIF(coverage_tests.P0,0) as point" +\
                        " FROM %s ORDER BY feature_name"  % bucket.name
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = []
            for doc in full_list:
                if (not 'P0' in doc['coverage_tests']) or doc['coverage_tests']['P0'] == 0:
                    expected_result.append({'feature_name' : doc['feature_name']})
                else:
                    expected_result.append({'feature_name' : doc['feature_name'],
                                            'point' : doc['coverage_tests']['P0']})
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def generate_docs(self, name="tuq", start=0, end=0):
        if not end:
            end = self.num_items
        generators = []
        index = end/3
        template = '{{ "feature_name":"{0}", "coverage_tests" : {{"P0":{1}, "P1":{2}, "P2":{3}}},'
        template += '"story_point" : {4}}}'
        names = [str(i) for i in xrange(0, index)]
        rates = xrange(0, index)
        points = [[1,2,3],]
        generators.append(DocumentGenerator(name, template,
                                            names, rates, rates, rates, points,
                                            start=start, end=index))
        template = '{{ "feature_name":"{0}", "coverage_tests" : {{"P0": null, "P1":null, "P2":null}},'
        template += '"story_point" : [1,2,null]}}'
        names = [str(i) for i in xrange(index, index + index)]
        generators.append(DocumentGenerator(name, template,
                                            names,
                                            start=index, end=index + index))
        template = '{{ "feature_name":"{0}", "coverage_tests" : {{"P4": 2}},'
        template += '"story_point" : [null,null]}}'
        names = [str(i) for i in xrange(index + index, end)]
        generators.append(DocumentGenerator(name, template,
                                            names, start=index + index, end=end))
        return generators