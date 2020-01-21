import logging
import threading
from .tuq import QueryTests

class QueryNewTuqTests(QueryTests):

    def setUp(self):
        super(QueryNewTuqTests, self).setUp()
        self.log.info("==============  QueryNewTuqTests setup has started ==============")
        self.log.info("==============  QueryNewTuqTests setup has started ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueryNewTuqTests, self).suite_setUp()
        self.log.info("==============  QueryNewTuqTests suite_setup has started ==============")
        self.log.info("==============  QueryNewTuqTests suite_setup has started ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryNewTuqTests tearDown has started ==============")
        self.log.info("==============  QueryNewTuqTests tearDown has started ==============")
        super(QueryNewTuqTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  QueryNewTuqTests suite_tearDown has started ==============")
        self.log.info("==============  QueryNewTuqTests suite_tearDown has started ==============")
        super(QueryNewTuqTests, self).suite_tearDown()

##############################################################################################
#
#   SIMPLE CHECKS
##############################################################################################
    def test_simple_check(self):
        self.fail_if_no_buckets()
        self.ensure_primary_indexes_exist()
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t1 = threading.Thread(name='run_simple', target=self.run_active_requests, args=(e, 2))
                t1.start()

            query = 'select * from %s' %(bucket.name)
            self.run_cbq_query(query)
            logging.debug("event is set")
            if self.monitoring:
                e.set()
                t1.join(100)
            query_template = 'FROM %s select $str0, $str1 ORDER BY $str0,$str1 ASC' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    #No usages anywhere
    def test_joins_monitoring(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            e = threading.Event()
            if self.monitoring:
                e = threading.Event()
                t2 = threading.Thread(name='run_joins', target=self.run_active_requests, args=(e, 2))
                t2.start()
            query = 'select * from %s b1 inner join %s b2 on keys b1.CurrencyCode inner join %s b3 on keys b1.CurrencyCode left outer join %s b4 on keys b1.CurrencyCode' % (bucket.name, bucket.name, bucket.name, bucket.name)
            actual_result = self.run_cbq_query(query)
            logging.debug("event is set")
            if self.monitoring:
                e.set()
                t2.join(100)

    def test_simple_negative_check(self):
        queries_errors = {'SELECT $str0 FROM {0} WHERE COUNT({0}.$str0)>3':
                          'Aggregates not allowed in WHERE',
                          'SELECT *.$str0 FROM {0}': 'syntax error',
                          'SELECT *.* FROM {0} ... ERROR': 'syntax error',
                          'FROM %s SELECT $str0 WHERE id=null': 'syntax error',}
        self.negative_common_body(queries_errors)

    def test_unnest(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT emp.$int0, task FROM %s emp UNNEST emp.$nested_list_3l0 task' % bucket.name
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_subquery_select(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            self.query = 'SELECT $str0, $subquery(SELECT COUNT($str0) cn FROM %s d USE KEYS $5) as names FROM %s' % (bucket.name,
                                                                                                                     bucket.name)
            actual_result, expected_result = self.run_query_with_subquery_select_from_template(self.query)
            self._verify_results(actual_result['results'], expected_result)

    def test_subquery_from(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            self.query = 'SELECT tasks.$str0 FROM $subquery(SELECT $str0, $int0 FROM %s) as tasks' % (bucket.name)
            actual_result, expected_result = self.run_query_with_subquery_from_template(self.query)
            self._verify_results(actual_result['results'], expected_result)

    def test_consistent_simple_check(self):
        self.fail_if_no_buckets()
        queries = [self.gen_results.generate_query('SELECT $str0, $int0, $int1 FROM %s ' +\
                    'WHERE $str0 IS NOT NULL AND $int0<10 ' +\
                    'OR $int1 = 6 ORDER BY $int0, $int1'),
                   self.gen_results.generate_query('SELECT $str0, $int0, $int1 FROM %s ' +\
                    'WHERE $int1 = 6 OR $str0 IS NOT NULL AND ' +\
                    '$int0<10 ORDER BY $int0, $int1')]
        for bucket in self.buckets:
            actual_result1 = self.run_cbq_query(queries[0] % bucket.name)
            actual_result2 = self.run_cbq_query(queries[1] % bucket.name)
            self.assertTrue(actual_result1['results'] == actual_result2['results'],
                              "Results are inconsistent.Difference: %s %s %s %s" %(
                                    len(actual_result1['results']), len(actual_result2['results']),
                                    actual_result1['results'][:100], actual_result2['results'][:100]))

    def test_simple_nulls(self):
        self.fail_if_no_buckets()
        queries = ['SELECT id FROM %s WHERE id=NULL or id="null"']
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t3 = threading.Thread(name='run_simple_nulls', target=self.run_active_requests, args=(e, 2))
                t3.start()
            for query in queries:
                actual_result = self.run_cbq_query(query % (bucket.name))
                logging.debug("event is set")
                if self.monitoring:
                     e.set()
                     t3.join(100)
                self._verify_results(actual_result['results'], [])

    '''MB-22550: Simple check that ensures
       OBJECT_RENAME and OBJECT_REPLACE functions work as intended'''
    def test_object_rename_replace(self):
        rename_result = self.run_cbq_query(
            'select OBJECT_RENAME( { "a": 1, "b": 2 }, "b", "c" ) ')
        self.assertTrue("b" not in rename_result['results'][0]['$1']
                        and "c" in rename_result['results'][0]['$1'])

        replace_result = self.run_cbq_query(
            'select OBJECT_REPLACE( { "a": 1, "b": 2 }, 2, 3 )')
        self.assertTrue(replace_result['results'][0]['$1']['b'] == 3)

        str_replace = self.run_cbq_query(
            'select OBJECT_REPLACE( { "a": 1, "b": 2 }, 2, "ajay" ) ')
        self.assertTrue(str_replace['results'][0]['$1']['b'] == "ajay")

##############################################################################################
#
#   LIMIT OFFSET CHECKS
##############################################################################################
    #Not used anywhere
    def test_limit_negative(self):
        #queries_errors = {'SELECT * FROM default LIMIT {0}' : ('Invalid LIMIT value 2.5', 5030)}
        queries_errors = {'SELECT ALL * FROM %s' : ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_limit_offset(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t4 = threading.Thread(name='run_limit_offset', target=self.run_active_requests, args=(e, 2))
                t4.start()
            query_template = 'SELECT DISTINCT $str0 FROM %s ORDER BY $str0 LIMIT 10' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            if self.monitoring:
                e.set()
                t4.join(100)
            self._verify_results(actual_result['results'], expected_result)
            query_template = 'SELECT DISTINCT $str0 FROM %s ORDER BY $str0 LIMIT 10 OFFSET 10' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)

    def test_limit_offset_zero(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT DISTINCT $str0 FROM %s ORDER BY $str0 LIMIT 0' % (bucket.name)
            self.query = self.gen_results.generate_query(query_template)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['results'], [],
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['results'], []))
            query_template = 'SELECT DISTINCT $str0 FROM %s ORDER BY $str0 LIMIT 10 OFFSET 0' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self.assertEqual(actual_result['results'], expected_result,
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['results'], expected_result))

    def test_limit_offset_negative_check(self):
        queries_errors = {'SELECT DISTINCT $str0 FROM {0} LIMIT 1.1' :
                          'Invalid LIMIT value 1.1',
                          'SELECT DISTINCT $str0 FROM {0} OFFSET 1.1' :
                          'Invalid OFFSET value 1.1'}
        self.negative_common_body(queries_errors)

    def test_limit_offset_sp_char_check(self):
        queries_errors = {'SELECT DISTINCT $str0 FROM {0} LIMIT ~' :
                          'syntax erro',
                          'SELECT DISTINCT $str0 FROM {0} OFFSET ~' :
                          'syntax erro'}
        self.negative_common_body(queries_errors)
##############################################################################################
#
#   ALIAS CHECKS
##############################################################################################

    def test_simple_alias(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t5 = threading.Thread(name='run_limit_offset', target=self.run_active_requests, args=(e, 2))
                t5.start()
            query_template = 'SELECT COUNT($str0) AS COUNT_EMPLOYEE FROM %s' % (bucket.name)
            if self.analytics:
                query_template = 'SELECT COUNT(`$str0`) AS COUNT_EMPLOYEE FROM %s' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self.assertEqual(actual_result['results'], expected_result,
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['results'], expected_result))

            query_template = 'SELECT COUNT(*) + 1 AS COUNT_EMPLOYEE FROM %s' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            if self.monitoring:
                e.set()
                t5.join(100)
            expected_result = [ { "COUNT_EMPLOYEE": expected_result[0]['COUNT_EMPLOYEE'] + 1 } ]
            self.assertEqual(actual_result['results'], expected_result,
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['results'], expected_result))


    def test_simple_negative_alias(self):
        queries_errors = {'SELECT $str0._last_name as *' : 'syntax error',
                          'SELECT $str0._last_name as DATABASE ?' : 'syntax error',
                          'SELECT $str0 AS NULL FROM {0}' : 'syntax error',
                          'SELECT $str1 as $str0, $str0 FROM {0}' :
                                'Duplicate result alias name'}

        if self.does_test_meet_server_version(6, 5, 0):
            queries_errors['SELECT test.$obj0 as points FROM {0} AS TEST ' +
                           'GROUP BY $obj0 AS GROUP_POINT'] = 'must depend only on group keys or aggregates'

        self.negative_common_body(queries_errors)

    def test_alias_from_clause(self):
        self.fail_if_no_buckets()
        queries_templates = ['SELECT $obj0.$_obj0_int0 AS points FROM %s AS test ORDER BY points',
                   'SELECT $obj0.$_obj0_int0 AS points FROM %s AS test WHERE test.$int0 >0'  +\
                   ' ORDER BY points',
                   'SELECT $obj0.$_obj0_int0 AS points FROM %s AS test ' +\
                   'GROUP BY test.$obj0.$_obj0_int0 ORDER BY points']
        # if self.analytics:
        #      queries_templates = ['SELECT test.$obj0.$_obj0_int0 AS points FROM %s AS test ORDER BY test.points',
        #            'SELECT test.$obj0.$_obj0_int0 AS points FROM %s AS test WHERE test.$int0 >0'  +\
        #            ' ORDER BY test.points',
        #            'SELECT test.$obj0.$_obj0_int0 AS points FROM %s AS test ' +\
        #            'GROUP BY test.$obj0.$_obj0_int0 ORDER BY test.points']
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t6 = threading.Thread(name='run_limit_offset', target=self.run_active_requests, args=(e, 2))
                t6.start()
            for query_template in queries_templates:
                actual_result, expected_result = self.run_query_from_template(query_template  % (bucket.name))
                if self.monitoring:
                    e.set()
                    t6.join(100)
                self._verify_results(actual_result['results'], expected_result)

    def test_alias_from_clause_group(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $obj0.$_obj0_int0 AS points FROM %s AS test ' %(bucket.name) +\
                         'GROUP BY $obj0.$_obj0_int0 ORDER BY points'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_alias_order_desc(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t7 = threading.Thread(name='run_limit_offset', target=self.run_active_requests, args=(e, 2))
                t7.start()
            query_template = 'SELECT $str0 AS name_new FROM %s AS test ORDER BY name_new DESC' %(
                                                                                bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            if self.monitoring:
                    e.set()
                    t7.join(100)
            self._verify_results(actual_result['results'], expected_result)

    def test_alias_order_asc(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $str0 AS name_new FROM %s AS test ORDER BY name_new ASC' %(
                                                                                bucket.name)
            if self.analytics:
                query_template = 'SELECT `$str0` AS name_new FROM %s AS test ORDER BY name_new ASC' %(
                                                                                bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_alias_aggr_fn(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t8 = threading.Thread(name='run_limit_offset', target=self.run_active_requests, args=(e, 2))
                t8.start()
            query_template = 'SELECT COUNT(TEST.$str0) from %s AS TEST' %(bucket.name)
            if self.analytics:
                 query_template = 'SELECT COUNT(TEST.`$str0`) from %s AS TEST' %(bucket.name)

            actual_result, expected_result = self.run_query_from_template(query_template)
            if self.monitoring:
                    e.set()
                    t8.join(100)
            self._verify_results(actual_result['results'], expected_result)

    def test_alias_unnest(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT count(skill) FROM %s AS emp UNNEST emp.$list_str0 AS skill' %(
                                                                            bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

            query_template = 'SELECT count(skill) FROM %s AS emp UNNEST emp.$list_str0 skill' %(
                                                                            bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

##############################################################################################
#
#   ORDER BY CHECKS
##############################################################################################

    def test_order_by_check(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $str0, $str1, $obj0.$_obj0_int0 points FROM %s'  % (bucket.name) +\
            ' ORDER BY $str1, $str0, $obj0.$_obj0_int0'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)
            query_template = 'SELECT $str0, $str1 FROM %s'  % (bucket.name) +\
            ' ORDER BY $obj0.$_obj0_int0, $str0, $str1'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_order_by_alias(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $str1, $obj0 AS points FROM %s'  % (bucket.name) +\
            ' AS test ORDER BY $str1 DESC, points DESC'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_order_by_alias_arrays(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $str1, $obj0, $list_str0[0] AS SKILL FROM %s'  % (
                                                                            bucket.name) +\
            ' AS TEST ORDER BY SKILL, $str1, TEST.$obj0'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_order_by_alias_aggr_fn(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $int0, $int1, count(*) AS emp_per_month from %s'% (
                                                                            bucket.name) +\
            ' WHERE $int1 >7 GROUP BY $int0, $int1 ORDER BY emp_per_month, $int1, $int0'
            actual_result, expected_result = self.run_query_from_template(query_template)
            #self.assertTrue(len(actual_result['results'])== 0)

    def test_order_by_aggr_fn(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $str1 AS TITLE, min($int1) day FROM %s GROUP'  % (bucket.name) +\
            ' BY $str1 ORDER BY MIN($int1), $str1'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

            if self.analytics:
                self.query = 'SELECT d.email AS TITLE, min(d.join_day) day FROM %s d GROUP'  % (bucket.name) +\
            ' BY d.$str1 ORDER BY MIN(d.join_day), d.$str1'
                actual_result1 = self.run_cbq_query()
                self._verify_results(actual_result1['results'], actual_result['results'])

    def test_order_by_precedence(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $str0, $str1 FROM %s'  % (bucket.name) +\
            ' ORDER BY $str0, $str1'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

            query_template = 'SELECT $str0, $str1 FROM %s'  % (bucket.name) +\
            ' ORDER BY $str1, $str0'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_order_by_satisfy(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $str0, $list_obj0 FROM %s AS employee ' % (bucket.name) +\
                        'WHERE ANY vm IN employee.$list_obj0 SATISFIES vm.$_list_obj0_int0 > 5 AND' +\
                        ' vm.$_list_obj0_str0 = "ubuntu" END ORDER BY $str0, $list_obj0[0].$_list_obj0_int0'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

##############################################################################################
#
#   DISTINCT
##############################################################################################

    def test_distinct(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT DISTINCT $str1 FROM %s ORDER BY $str1'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_distinct_nested(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT DISTINCT $obj0.$_obj0_int0 as VAR FROM %s '  % (bucket.name) +\
                         'ORDER BY $obj0.$_obj0_int0'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

            query_template = 'SELECT DISTINCT $list_str0[0] as skill' +\
                         ' FROM %s ORDER BY $list_str0[0]'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

            self.query = 'SELECT DISTINCT $obj0.* FROM %s'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

##############################################################################################
#
#   COMPLEX PATHS
##############################################################################################

    #Not used anywhere
    def test_simple_complex_paths(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $_obj0_int0 FROM %s.$obj0'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    #Not used anywhere
    def test_alias_complex_paths(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $_obj0_int0 as new_attribute FROM %s.$obj0'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    #Not used anywhere
    def test_where_complex_paths(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_template = 'SELECT $_obj0_int0 FROM %s.$obj0 WHERE $_obj0_int0 = 1'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_new_contains_functions(self):
        self.query = 'SELECT * FROM default WHERE ANY v IN tokens(default, {"specials":true}) SATISFIES REGEXP_LIKE(TOSTRING(v),"(\d{3}-\d{2}-\d{4})|\d{9)|(\d{3}[ ]\d{2}[ ]\d{4})")END order by meta().id'
        expected_result = self.run_cbq_query()
        self.query = 'SELECT * FROM default WHERE CONTAINS_TOKEN_REGEXP(TOSTRING(v),"(\d{3}-\\d{2}-\\d{4})|(\\b\\d{9}\\b)|(\\d{3}[ ]\\d{2}[ ]\\d{4})") order by meta().id'
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], expected_result['results'])
        self.query = 'SELECT * FROM default WHERE CONTAINS_TOKEN_REGEXP(TOSTRING(v),"(\d{3}-\d{2}-\d{4})|\d{9)|(\d{3}[ ]\d{2}[ ]\d{4})") order by meta().id'
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], expected_result['results'])
