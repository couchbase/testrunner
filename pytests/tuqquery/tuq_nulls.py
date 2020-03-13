from tuqquery.tuq import QueryTests


class NULLTests(QueryTests):
    def setUp(self):
        self.skip_generation = True
        self.analytics = False
        super(NULLTests, self).setUp()
        self.gens_load = self.gen_docs(type='nulls')
        self.full_list = self.generate_full_docs_list(self.gens_load)
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)

    def suite_setUp(self):
        super(NULLTests, self).suite_setUp()

    def tearDown(self):
        super(NULLTests, self).tearDown()

    def suite_tearDown(self):
        super(NULLTests, self).suite_tearDown()

    def test_null_query(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P0 IS NULL ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name']}
                               for doc in self.full_list
                               if "P0" in doc['coverage_tests'] and
                               doc['coverage_tests']['P0'] is None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_not_null_query(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P0 IS NOT NULL ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name']}
                               for doc in self.full_list
                               if "P0" in doc['coverage_tests'] and
                               doc['coverage_tests']['P0'] is not None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_not_null_query(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P0 IS NOT NULL ORDER BY feature_name"
            self.prepared_common_body()

    def test_null_query_any(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, jira_tickets FROM %s d WHERE " % query_bucket + \
                         "(ANY ticket IN d.jira_tickets SATISFIES ticket.description is null END)" + \
                         " ORDER BY feature_name"

            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name'],
                                'jira_tickets': doc['jira_tickets']}
                               for doc in self.full_list
                               if len([t for t in doc["jira_tickets"]
                                       if 'description' in t and t['description'] is None]) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_not_null_query_any(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, jira_tickets FROM %s d WHERE " % query_bucket + \
                         "(ANY ticket IN d.jira_tickets SATISFIES ticket.description is not null END)" + \
                         " ORDER BY feature_name"

            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name'],
                                'jira_tickets': doc['jira_tickets']}
                               for doc in self.full_list
                               if len([t for t in doc["jira_tickets"]
                                       if 'description' in t and t['description'] is not None]) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_null_query(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P0 IS NULL ORDER BY feature_name"
            self.prepared_common_body()

    def test_let_null(self):
        for query_bucket in self.query_buckets:
            self.query = "select compare from %s let compare = (coverage_tests.P0 is null)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [
                {"compare": None if 'P0' not in doc["coverage_tests"] else doc["coverage_tests"]['P0'] is None}
                for doc in self.full_list]
            expected_result = [{} if doc['compare'] is None else doc for doc in expected_result]
            self._verify_results(actual_result, expected_result)

    def test_let_not_null(self):
        for query_bucket in self.query_buckets:
            self.query = "select compare from %s let compare = (coverage_tests.P0 is not null)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [
                {"compare": None if 'P0' not in doc["coverage_tests"] else doc["coverage_tests"]['P0'] is not None}
                for doc in self.full_list]
            expected_result = [{} if doc['compare'] is None else doc for doc in expected_result]
            self._verify_results(actual_result, expected_result)

    def test_missing_query(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P0 IS MISSING ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name']}
                               for doc in self.full_list
                               if "P0" not in doc['coverage_tests']]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_missing_query(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P0 IS MISSING ORDER BY feature_name"
            self.prepared_common_body()

    def test_missing_query_any(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, jira_tickets FROM %s d WHERE " % query_bucket + \
                         "(ANY ticket IN d.jira_tickets SATISFIES ticket.description is missing END)"  + \
                         " ORDER BY feature_name"

            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name'],
                                'jira_tickets': doc['jira_tickets']}
                               for doc in self.full_list
                               if len([t for t in doc["jira_tickets"]
                                       if 'description' not in t]) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_not_missing_query_any(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, jira_tickets FROM %s d WHERE " % query_bucket + \
                         "(ANY ticket IN d.jira_tickets SATISFIES ticket.description is not missing END)" + \
                         " ORDER BY feature_name"
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name'],
                                'jira_tickets': doc['jira_tickets']}
                               for doc in self.full_list
                               if len([t for t in doc["jira_tickets"]
                                       if 'description' in t]) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_not_missing_query(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P0 IS NOT missing ORDER BY feature_name"
            self.prepared_common_body()

    def test_let_missing(self):
        for query_bucket in self.query_buckets:
            self.query = "select compare from %s let compare = (coverage_tests.P0 is missing)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"compare": 'P0' not in doc["coverage_tests"]}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_let_not_missing(self):
        for query_bucket in self.query_buckets:
            self.query = "select compare from %s let compare = (coverage_tests.P0 is not missing)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"compare": 'P0' in doc["coverage_tests"]}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_not_missing_query(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P1 IS NOT MISSING ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name']}
                               for doc in self.full_list
                               if "P1" in doc['coverage_tests']]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_valued(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P0 IS VALUED ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name']}
                               for doc in self.full_list
                               if "P0" in doc['coverage_tests'] and
                               doc['coverage_tests']['P0'] is not None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_not_valued(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P0 IS NOT VALUED ORDER BY feature_name"
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name']}
                               for doc in self.full_list
                               if "P0" not in doc['coverage_tests'] or
                               doc['coverage_tests']['P0'] is None]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_valued_query_any(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, jira_tickets FROM %s d WHERE " % query_bucket + \
                         "(ANY ticket IN d.jira_tickets SATISFIES ticket.description is valued END)" + \
                         " ORDER BY feature_name"

            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name'],
                                'jira_tickets': doc['jira_tickets']}
                               for doc in self.full_list
                               if len([t for t in doc["jira_tickets"]
                                       if 'description' in t and t['description'] is not None]) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_not_valued_query_any(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, jira_tickets FROM %s d WHERE " % query_bucket + \
                         "(ANY ticket IN d.jira_tickets SATISFIES ticket.description is not valued END)" + \
                         " ORDER BY feature_name"
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name'],
                                'jira_tickets': doc['jira_tickets']}
                               for doc in self.full_list
                               if len([t for t in doc["jira_tickets"]
                                       if 'description' not in t or t['description'] is None]) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_valued_query(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P0 IS valued ORDER BY feature_name"
            self.prepared_common_body()

    def test_prepared_not_valued_query(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE coverage_tests.P0 IS NOT valued ORDER BY feature_name"
            self.prepared_common_body()

    def test_let_valued(self):
        for query_bucket in self.query_buckets:
            self.query = "select compare from %s let compare = (coverage_tests.P0 is valued)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"compare": 'P0' in doc["coverage_tests"] and doc['coverage_tests']['P0'] is not None}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_let_not_valued(self):
        for query_bucket in self.query_buckets:
            self.query = "select compare from %s let compare = (coverage_tests.P0 is not valued)" % query_bucket

            actual_list = self.run_cbq_query()
            actual_result = actual_list['results']
            expected_result = [{"compare": 'P0' not in doc["coverage_tests"] or doc['coverage_tests']['P0'] is None}
                               for doc in self.full_list]
            self._verify_results(actual_result, expected_result)

    def test_precedense(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s" % query_bucket + \
                         " WHERE 2+2=4 AND coverage_tests.P0 IS VALUED "
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name']}
                               for doc in self.full_list
                               if "P0" in doc['coverage_tests'] and
                               doc['coverage_tests']['P0'] is not None]
            actual_result = actual_result['results']
            self._verify_results(actual_result, expected_result)

    def test_nulls_over(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s " % query_bucket + \
                         "WHERE ANY story_point_n IN story_point SATISFIES story_point_n IS NULL END ORDER BY " \
                         "feature_name "

            if self.analytics:
                self.query = "SELECT feature_name FROM %s" % query_bucket + \
                             " WHERE ANY story_point_n IN story_point SATISFIES story_point_n IS NULL ORDER BY " \
                             "feature_name "

            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name']}
                               for doc in self.full_list
                               if len([point for point in doc['story_point'] if point is None]) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)
            self.query = "SELECT feature_name FROM %s " % query_bucket + \
                         "WHERE EVERY story_point_n IN story_point SATISFIES story_point_n IS NULL END ORDER BY " \
                         "feature_name "
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name']}
                               for doc in self.full_list
                               if len([point for point in doc['story_point']
                                       if point is None]) == len(doc['story_point'])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_nulls_over(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name FROM %s " % query_bucket + \
                         "WHERE ANY story_point_n IN story_point SATISFIES story_point_n IS NULL END ORDER BY " \
                         "feature_name "
            self.prepared_common_body()

    def test_ifnan(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, IFNAN(story_point[2],story_point[1]) as point" + \
                         " FROM %s ORDER BY feature_name" % query_bucket
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = []
            for doc in self.full_list:
                if len(doc['story_point']) < 3:
                    expected_result.append({'feature_name': doc['feature_name'],
                                            'point': None})
                else:
                    expected_result.append({'feature_name': doc['feature_name'],
                                            'point': doc['story_point'][2]})
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_ifnull(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, IFNAN(story_point[2],story_point[1]) as point" + \
                         " FROM %s ORDER BY feature_name" % query_bucket
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = []
            for doc in self.full_list:
                if len(doc['story_point']) < 3:
                    expected_result.append({'feature_name': doc['feature_name'],
                                            'point': None})
                else:
                    expected_result.append({'feature_name': doc['feature_name'],
                                            'point': doc['story_point'][2]})
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_firstnum(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, FIRSTNUM(story_point[2],story_point[1]) as point" + \
                         " FROM %s ORDER BY feature_name" % query_bucket
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = []
            for doc in self.full_list:
                if len(doc['story_point']) < 3:
                    expected_result.append({'feature_name': doc['feature_name'],
                                            'point': None})
                elif doc['story_point'][2] is None:
                    expected_result.append({'feature_name': doc['feature_name'],
                                            'point': doc['story_point'][1]})
                else:
                    expected_result.append({'feature_name': doc['feature_name'],
                                            'point': doc['story_point'][2]})
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_nanif(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, NANIF(story_points[0],story_point[0])" + \
                         " FROM %s ORDER BY feature_name" % query_bucket
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = [{'feature_name': doc['feature_name']}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_ifposinf(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, POSINFIF(story_point[2],story_point[1]) as point" + \
                         " FROM %s ORDER BY feature_name" % query_bucket
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = []
            for doc in self.full_list:
                if len(doc['story_point']) < 3:
                    expected_result.append({'feature_name': doc['feature_name']})
                else:
                    expected_result.append({'feature_name': doc['feature_name'],
                                            'point': doc['story_point'][2]})
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)
            self.query = "SELECT feature_name, NEGINFIF(story_point[2],story_point[1]) as point" + \
                         " FROM %s ORDER BY feature_name" % query_bucket
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], expected_result)

    def test_ifinf(self):
        queries = ["SELECT feature_name, IFINF(story_point[2],story_point[1]) as point" +
                   " FROM %s ORDER BY feature_name", ]
        expected_result = []
        for doc in self.full_list:
            if len(doc['story_point']) < 3:
                expected_result.append({'feature_name': doc['feature_name']})
            else:
                expected_result.append({'feature_name': doc['feature_name'],
                                        'point': doc['story_point'][2]})
        expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
        for query_bucket in self.query_buckets:
            for query in queries:
                self.run_cbq_query(query % query_bucket)
                self.sleep(3)
                actual_result = self.run_cbq_query(query % query_bucket)
                self._verify_results(actual_result['results'], expected_result)

        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, IFNANORINF(story_point[2],story_point[1]) as point" + \
                         " FROM %s ORDER BY feature_name" % query_bucket
            expected_result = []
            for doc in self.full_list:
                if len(doc['story_point']) < 3:
                    expected_result.append({'feature_name': doc['feature_name']})
                else:
                    expected_result.append({'feature_name': doc['feature_name'],
                                            'point': doc['story_point'][2]})
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            actual_result = self.run_cbq_query(self.query)
            self._verify_results(actual_result['results'], expected_result)

    def test_ifmissing(self):
        queries = ["SELECT feature_name, IFMISSINGORNULL(coverage_tests.P0," +
                   "coverage_tests.P4) as C FROM %s ORDER BY feature_name",
                   "SELECT feature_name, IFMISSING(coverage_tests.P0," +
                   "coverage_tests.P4) as C FROM %s ORDER BY feature_name"]
        expected_result = []
        for doc in self.full_list:
            if "P0" in doc['coverage_tests']:
                expected_result.append({'feature_name': doc['feature_name'],
                                        'C': doc['coverage_tests']['P0']})
            else:
                expected_result.append({'feature_name': doc['feature_name'],
                                        'C': doc['coverage_tests']['P4']})
        expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
        for query_bucket in self.query_buckets:
            for query in queries:
                self.run_cbq_query(query % query_bucket)
                self.sleep(3)
                actual_result = self.run_cbq_query(query % query_bucket)
                self._verify_results(actual_result['results'], expected_result)

    def test_missingif(self):
        for query_bucket in self.query_buckets:
            self.query = "SELECT feature_name, MISSINGIF(coverage_tests.P0,0) as point" + \
                         " FROM %s ORDER BY feature_name" % query_bucket
            self.run_cbq_query()
            self.sleep(3)
            actual_result = self.run_cbq_query()
            expected_result = []
            for doc in self.full_list:
                if ('P0' not in doc['coverage_tests']) or doc['coverage_tests']['P0'] == 0:
                    expected_result.append({'feature_name': doc['feature_name']})
                else:
                    expected_result.append({'feature_name': doc['feature_name'],
                                            'point': doc['coverage_tests']['P0']})
            expected_result = sorted(expected_result, key=lambda doc: (doc['feature_name']))
            self._verify_results(actual_result['results'], expected_result)
