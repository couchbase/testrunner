import math
from tuqquery.tuq import QueryTests


class PrecedenceTests(QueryTests):
    def setUp(self):
        super(PrecedenceTests, self).setUp()
        self.log.info("==============  PrecedenceTests setup has started ==============")
        self.log.info("==============  PrecedenceTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(PrecedenceTests, self).suite_setUp()
        self.log.info("==============  PrecedenceTests suite_setup has started ==============")
        self.log.info("==============  PrecedenceTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  PrecedenceTests teardown has started ==============")
        self.log.info("==============  PrecedenceTests teardown has completed ==============")
        super(PrecedenceTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  PrecedenceTests suite_teardown has started ==============")
        self.log.info("==============  PrecedenceTests suite_teardown has completed ==============")
        super(PrecedenceTests, self).suite_tearDown()

    def test_case_and_like(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            self.query = "SELECT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE CASE WHEN" % (bucket.name) +\
            " join_mo < 3 OR join_mo > 11 THEN 'winter' ELSE 'other' END LIKE 'win%'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                doc['name'], doc['period']))
            expected_result = [{"name" : doc['name'],
                                "period" : ('other', 'winter')
                                            [doc['join_mo'] in [12, 1, 2]]}
                               for doc in self.full_list
                               if ('other', 'winter')[doc['join_mo'] in [12, 1, 2]].startswith(
                                                                                'win')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_case_and_logic_exp(self):
        for bucket in self.buckets:
            self.query = "SELECT DISTINCT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE CASE WHEN join_mo < 3" %(bucket.name) +\
            " OR join_mo > 11 THEN 1 ELSE 0 END > 0 AND job_title='Sales'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name'], doc['period']))
            expected_result = [{"name" : doc['name'],
                                "period" : ('other', 'winter')
                                            [doc['join_mo'] in [12, 1, 2]]}
                               for doc in self.full_list
                               if (0, 1)[doc['join_mo'] in [12, 1, 2]] > 0 and\
                                  doc['job_title'] == 'Sales']
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_prepared_case_and_logic_exp(self):
        for bucket in self.buckets:
            self.query = "SELECT DISTINCT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE CASE WHEN join_mo < 3" %(bucket.name) +\
            " OR join_mo > 11 THEN 1 ELSE 0 END > 0 AND job_title='Sales'"
            self.prepared_common_body()

    def test_case_and_comparision_exp(self):
        for bucket in self.buckets:
            self.query = "SELECT DISTINCT name, CASE WHEN join_mo < 3 OR join_mo > 11 THEN" +\
            " 'winter' ELSE 'other' END AS period FROM %s WHERE CASE WHEN join_mo < 3" %(bucket.name) +\
            " OR join_mo > 11 THEN 1 END = 1 AND job_title='Sales'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name'], doc['period']))
            expected_result = [{"name" : doc['name'],
                                "period" : ('other', 'winter')
                                            [doc['join_mo'] in [12, 1, 2]]}
                               for doc in self.full_list
                               if (doc['join_mo'], 1)[doc['join_mo'] in [12, 1, 2]] == 1 and\
                                  doc['job_title'] == 'Sales']
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_arithm_and_comparision_exp(self):
        for bucket in self.buckets:
            self.query = "SELECT name from %s WHERE join_mo > 3 + 1" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name']))
            expected_result = [{"name" : doc['name']}
                               for doc in self.full_list
                               if doc['join_mo'] > (3 + 1)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT name from %s WHERE join_mo = 3 + 1" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name']))
            expected_result = [{"name" : doc['name']}
                               for doc in self.full_list
                               if doc['join_mo'] == (3 + 1)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_arithm_and_like_exp(self):
        for bucket in self.buckets:
            self.query = "SELECT name from {0} WHERE job_title LIKE 'S%' = TRUE".format(bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name']))
            expected_result = [{"name" : doc['name']}
                               for doc in self.full_list
                               if doc['job_title'].startswith('S')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_logic_exp(self):
        for bucket in self.buckets:
            self.query = "SELECT name, join_mo from %s WHERE NOT join_mo>10 AND" % (bucket.name) +\
            " job_title='Sales' OR join_mo<2"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name'], doc['join_mo']))
            expected_result = [{"name" : doc['name'], "join_mo" : doc['join_mo']}
                               for doc in self.full_list
                               if ((not (doc['join_mo'] > 10)) and doc['job_title'] == 'Sales') or\
                                   (doc['join_mo'] < 2)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'], doc['join_mo']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT DISTINCT email from %s WHERE NOT join_mo<10 OR join_mo<2" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['email']))
            expected_result = [{"email" : doc['email']}
                               for doc in self.full_list
                               if (not (doc['join_mo'] < 10)) or (doc['join_mo'] < 2)]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['email']))
            self._verify_results(actual_result, expected_result)

    def test_prepared_logic_exp(self):
        for bucket in self.buckets:
            self.query = "SELECT name, join_mo from %s WHERE NOT join_mo>10 AND" % (bucket.name) +\
            " job_title='Sales' OR join_mo<2"
            self.prepared_common_body()

    def test_logic_exp_nulls(self):
        for bucket in self.buckets:
            self.query = "SELECT name, join_mo from %s WHERE NOT join_mo IS NULL" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name'], doc['join_mo']))
            expected_result = [{"name" : doc['name'], "join_mo" : doc['join_mo']}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'], doc['join_mo']))
            self._verify_results(actual_result, expected_result)