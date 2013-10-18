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
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P0" in doc['coverage_tests'] and\
                                doc['coverage_tests']['P0'] is None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_not_null_query(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE coverage_tests.P0 IS NOT NULL ORDER BY feature_name"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P0" in doc['coverage_tests'] and\
                                doc['coverage_tests']['P0'] is not None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_missing_query(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE coverage_tests.P0 IS MISSING ORDER BY feature_name"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if not "P0" in doc['coverage_tests']]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_not_missing_query(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE coverage_tests.P1 IS NOT MISSING ORDER BY feature_name"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P1" in doc['coverage_tests']]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_valued(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE coverage_tests.P0 IS VALUED ORDER BY feature_name"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P0" in doc['coverage_tests'] and\
                                doc['coverage_tests']['P0'] is not None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_not_valued(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE coverage_tests.P0 IS NOT VALUED ORDER BY feature_name"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P0" in doc['coverage_tests'] and\
                                doc['coverage_tests']['P0'] is None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_precedense(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE NOT coverage_tests.P0 IS NULL"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if (not "P0" in doc['coverage_tests']) or\
                               doc['coverage_tests']['P0'] is not None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['resultset'], expected_result)
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE 2+2=4 AND coverage_tests.P0 IS VALUED "
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if "P0" in doc['coverage_tests'] and\
                                doc['coverage_tests']['P0'] is not None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['resultset'], expected_result)

    def test_nulls_over(self):
        for bucket in self.buckets:
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE ANY story_point IS NULL OVER story_point END ORDER BY feature_name"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if len([point for point in doc['story_point'] if point is None]) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['resultset'], expected_result)
            self.query = "SELECT feature_name FROM %s"  % bucket.name +\
                        " WHERE ALL story_point IS NULL OVER story_point END ORDER BY feature_name"
            actual_result = self.run_cbq_query()
            full_list = self._generate_full_docs_list(self.gens_load)
            expected_result = [{'feature_name' : doc['feature_name']}
                               for doc in full_list
                               if len([point for point in doc['story_point']
                                       if point is None]) == len(doc['story_point'])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['resultset'], expected_result)

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