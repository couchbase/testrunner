import json
import random
import string
from random import randint

from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests


class WindowFunctionsTest(QueryTests):

    def setUp(self):
        super(WindowFunctionsTest, self).setUp()
        self.log_config_info()
        self.log.info("==============  WindowFunctionsTest setup has started ==============")

        self.window_functions = {'RANK':
                                     {'name': 'RANK',
                                      'params': '()',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''},
                                 'NTILE':
                                     {'name': 'NTILE',
                                      'params': '(10)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''},
                                 'FIRST_VALUE':
                                     {'name': 'FIRST_VALUE',
                                      'params': '(decimal_field)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'DENSE_RANK':
                                     {'name': 'DENSE_RANK',
                                      'params': '()',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'RATIO_TO_REPORT':
                                     {'name': 'RATIO_TO_REPORT',
                                      'params': '(decimal_field)',
                                      'partition': 'partition by char_field',
                                      'order': '',
                                      'range': ''
                                      },
                                 'LAST_VALUE':
                                     {'name': 'LAST_VALUE',
                                      'params': '(decimal_field)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': 'RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING'
                                      },
                                 'PERCENT_RANK':
                                     {'name': 'PERCENT_RANK',
                                      'params': '()',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'ROW_NUMBER':
                                     {'name': 'ROW_NUMBER',
                                      'params': '()',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'NTH_VALUE':
                                     {'name': 'NTH_VALUE',
                                      'params': '(decimal_field, 3)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'CUME_DIST':
                                     {'name': 'CUME_DIST',
                                      'params': '()',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'LAG':
                                     {'name': 'LAG',
                                      'params': '(decimal_field)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      },
                                 'LEAD':
                                     {'name': 'LEAD',
                                      'params': '(decimal_field)',
                                      'partition': 'partition by char_field',
                                      'order': 'order by decimal_field',
                                      'range': ''
                                      }
                                 }

        self.aggregate_functions = {'ARRAY_AGG':
                                        {'name': 'ARRAY_AGG',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''},
                                    'AVG':
                                        {'name': 'AVG',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''},
                                    'COUNT':
                                        {'name': 'COUNT',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'COUNTN':
                                        {'name': 'COUNTN',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'MAX':
                                        {'name': 'MAX',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'MEAN':
                                        {'name': 'MEAN',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'MEDIAN':
                                        {'name': 'MEDIAN',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'MIN':
                                        {'name': 'MIN',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'SUM':
                                        {'name': 'SUM',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'STDDEV':
                                        {'name': 'STDDEV',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'STDDEV_SAMP':
                                        {'name': 'STDDEV_SAMP',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'STDDEV_POP':
                                        {'name': 'STDDEV_POP',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'VARIANCE':
                                        {'name': 'VARIANCE',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'VAR_SAMP':
                                        {'name': 'VAR_SAMP',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         },
                                    'VAR_POP':
                                        {'name': 'VAR_POP',
                                         'params': '(decimal_field)',
                                         'partition': 'partition by char_field',
                                         'order': '',
                                         'range': ''
                                         }
                                    }

        self.union_intersect_except = ['UNION', 'INTERSECT', 'EXCEPT']
        # https://issues.couchbase.com/browse/MB-32269
        # SHOULD BE self.all_distinct = [' ', 'ALL', 'DISTINCT']
        self.all_distinct = [' ', 'ALL', 'DISTINCT']
        self.raw_element_value = ['RAW', 'ELEMENT', 'VALUE']

        self.primary_idx = {'name': '#primary', 'bucket': 'test_bucket', 'fields': (), 'state': 'online',
                            'using': self.index_type.lower(), 'is_primary': True}
        self.idx_1 = {'name': 'ix_char', 'bucket': 'test_bucket', 'fields': [('char_field', 0)], 'state': 'online',
                      'using': self.index_type.lower(), 'is_primary': False}
        self.idx_2 = {'name': 'ix_decimal', 'bucket': 'test_bucket', 'fields': [('decimal_field', 0)],
                      'state': 'online', 'using': self.index_type.lower(), 'is_primary': False}
        self.idx_3 = {'name': 'ix_int', 'bucket': 'test_bucket', 'fields': [('int_field', 0)], 'state': 'online',
                      'using': self.index_type.lower(), 'is_primary': False}

        self.indexes = [self.primary_idx, self.idx_1, self.idx_2, self.idx_3]

        self.alphabet = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
                         'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

        if self.test_buckets != 'test_bucket':
            self.test_buckets = 'test_bucket'
        self.query_buckets = self.get_query_buckets(deferred_bucket=self.test_buckets)
        self.query_bucket = self.query_buckets[-1]
        self.log.info("==============  WindowFunctionsTest setup has completed ==============")

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  WindowFunctionsTest tearDown has started ==============")
        super(WindowFunctionsTest, self).tearDown()
        self.log.info("==============  WindowFunctionsTest tearDown has completed ==============")

    def suite_setUp(self):
        super(WindowFunctionsTest, self).suite_setUp()
        if self.test_buckets != 'test_bucket':
            self.test_buckets = 'test_bucket'
        self.init_nodes()
        self.query_buckets = self.get_query_buckets(deferred_bucket=self.test_buckets)
        self.query_bucket = self.query_buckets[-1]
        self.load_test_data(self.query_bucket)
        self.create_primary_index(self.test_buckets)
        self.create_secondary_indexes(self.test_buckets)
        self.adopt_test_data(self.test_buckets)
        self.log_config_info()
        self.log.info("==============  WindowFunctionsTest suite_setup has started ==============")
        self.log.info("==============  WindowFunctionsTest suite_setup has completed ==============")

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  WindowFunctionsTest suite_tearDown has started ==============")
        super(WindowFunctionsTest, self).suite_tearDown()
        self.log.info("==============  WindowFunctionsTest suite_tearDown has completed ==============")

    def run_all(self):
        self.test_select_term()
        self.test_order_by()
        self.test_order_by_limit()
        self.test_order_by_limit_offset()
        self.test_subselect()
        self.test_where()
        self.test_let_where()
        self.test_group_by()
        self.test_from_select()
        self.test_from_select_where()
        self.test_from_select_let_where()
        self.test_from_select_group_by()

        self.test_window_union_intersect_except_simple()
        self.test_simple_union_intersect_except_window()
        self.test_window_union_intersect_except_window()

        self.test_all_distinct_raw_element_value()

        self.test_let_alias()
        self.test_where_window_param_condition()
        self.test_group_by_letting_having()
        self.test_group_by_having()
        self.test_group_by_letting()

        self.test_order_by_window_result()

        self.test_tcpds_1()
        self.test_tcpds_2()
        self.test_tcpds_3()
        self.test_tcpds_4()
        self.test_tcpds_5()
        self.test_tcpds_6()
        self.test_tcpds_7()
        self.test_tcpds_8()
        self.test_tcpds_9()

        self.test_first_value()
        self.test_ratio_to_report()
        self.test_last_value()
        self.test_row_number()

        self.test_array_agg()
        self.test_avg()
        self.test_count()
        self.test_countn()
        self.test_max()
        self.test_mean()
        self.test_median()
        self.test_min()
        self.test_sum()
        self.test_stddev()
        self.test_stddev_samp()
        self.test_stddev_pop()
        self.test_variance()
        self.test_var_samp()
        self.test_var_pop()

        self.test_prepared_statements()
        self.test_parameterized_positional()
        self.test_parameterized_named()
        self.test_meta_properties()

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions.
        Returned data set is not analyzed.
    '''

    def test_select_term(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + "  as wf_result from {0} order by decimal_field".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + "  as wf_result from {0} order by decimal_field".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with combination of ORDER BY by window function result.
        Returned data set is not analyzed.
    '''

    def test_order_by(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + " as summa, char_field from {0} order by summa".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + " as summa, char_field from {0} order by summa".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with combination of ORDER BY by window function result plus LIMIT.
        Returned data set is not analyzed.
    '''

    def test_order_by_limit(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + " as summa, char_field from {0} order by summa limit 5".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + " as summa, char_field from {0} order by summa limit 5".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with combination of ORDER BY by window function result plus LIMIT plus OFFSET.
        Returned data set is not analyzed.
    '''

    def test_order_by_limit_offset(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + " as summa, char_field from {0} order by summa limit 5 offset 100".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + " as summa, char_field from {0} order by summa limit 5 offset 100".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        in subselect.
        Returned data set is not analyzed.
    '''

    def test_subselect(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, (select " + self._construct_window_function_call(
                v) + " from {0} t1) as subselect, ".format(self.query_bucket) \
                    + self._construct_window_function_call(v) + \
                    " as summa, char_field from {0} order by summa".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, (select " + self._construct_window_function_call(
                v) + " from {0} t1) as subselect, ".format(self.query_bucket) \
                    + self._construct_window_function_call(v) + \
                    " as summa, char_field from {0} order by summa".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of window function parameter in WHERE clause.
        Returned data set is not analyzed.
    '''

    def test_where(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(v) + \
                    " as summa, char_field from {0} where decimal_field > 1000 order by summa".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(v) + \
                    " as summa, char_field from {0} where decimal_field > 1000 order by summa".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of window function parameter in WHERE clause with the use of LET clause.
        Returned data set is not analyzed.
    '''

    def test_let_where(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(v) + \
                    " as summa, char_field from {0} let cval=1000 where decimal_field > " \
                    "cval order by summa".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + " as summa, char_field from {0} let cval=1000 where decimal_field > cval " \
                     "order by summa".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of window function parameter in GROUP BY clause.
        Returned data set is not analyzed.
    '''

    def test_group_by(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + ", char_field from {0} group by char_field, decimal_field".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + ", char_field from {0} group by char_field, decimal_field".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of FROM (SELECT ...) clause.
        Returned data set is not analyzed.
    '''

    def test_from_select(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            if v['name'] in ['NTH_VALUE', 'CUME_DIST', 'RANK', 'ROW_NUMBER', 'DENSE_RANK', 'LEAD', 'LAG',
                             'PERCENT_RANK', 'NTILE']:
                continue
            query = "from (select decimal_field as f1, " + self._construct_window_function_call(v) + \
                    " as f2, char_field as f3 from {0} group by char_field, decimal_field) a " \
                    "select a.f1 , a.f2, ".format(self.query_bucket) + v['name'] + \
                    "(a.f2) over (partition by a.f2), a.f3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        exceptional_functions = ['NTH_VALUE', 'CUME_DIST', 'RANK', 'ROW_NUMBER', 'DENSE_RANK', 'LEAD', 'LAG',
                                 'PERCENT_RANK', 'NTILE']
        for f_name in exceptional_functions:
            func = self.window_functions[f_name]
            if f_name == 'NTH_VALUE':
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(func) + \
                        " as f2, char_field as f3 from {0} group by char_field, decimal_field) a " \
                        "select a.f1 , a.f2, ".format(self.query_bucket) + func['name'] + \
                        "(a.f2, 3) over (partition by a.f2), a.f3"
            elif f_name == 'LAG' or f_name == 'LEAD' or f_name == 'NTILE':
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(
                    func) + " as f2, char_field as f3 from {0} group by char_field, decimal_field) a " \
                            "select a.f1 , a.f2, ".format(self.query_bucket) + func[
                            'name'] + "(a.f2) over (partition by a.f2 order by decimal_field), a.f3"
            else:
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(func) + \
                        " as f2, char_field as f3 from {0} group by char_field, decimal_field) a " \
                        "select a.f1 , a.f2, ".format(self.query_bucket) + func['name'] + \
                        "() over (partition by a.f2 order by a.f2), a.f3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "from (select decimal_field as f1, " + self._construct_window_function_call(v) + \
                    " as f2, char_field as f3 from {0} group by char_field, decimal_field) a " \
                    "select a.f1 , a.f2, ".format(self.query_bucket) + v['name'] + \
                    "(a.f2) over (partition by a.f2), a.f3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of FROM (SELECT ...) clause plus window function call result usage as condition in WHERE clause.
        Returned data set is not analyzed.
    '''

    def test_from_select_where(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            if v['name'] in ['NTH_VALUE', 'CUME_DIST', 'RANK', 'ROW_NUMBER', 'DENSE_RANK', 'LEAD', 'LAG',
                             'PERCENT_RANK', 'NTILE']:
                continue
            query = "from (select decimal_field as f1, " + self._construct_window_function_call(
                v) + " as f2, char_field as f3 from {0} group by char_field, decimal_field) a " \
                     "where a.f2 in [157331,165546] select a.f1 , a.f2, ".format(self.query_bucket) + v[
                        'name'] + "(a.f2) over (partition by a.f2), a.f3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        exceptional_functions = ['NTH_VALUE', 'CUME_DIST', 'RANK', 'ROW_NUMBER', 'DENSE_RANK', 'LEAD', 'LAG',
                                 'PERCENT_RANK', 'NTILE']
        for f_name in exceptional_functions:
            func = self.window_functions[f_name]
            if f_name == 'NTH_VALUE':
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(
                    func) + " as f2, char_field as f3 from {0} group by char_field, decimal_field) a " \
                            "where a.f2 in [157331,165546] select a.f1 , a.f2, ".format(self.query_bucket) + func[
                            'name'] + "(a.f2, 3) over (partition by a.f2), a.f3"
            elif f_name == 'LAG' or f_name == 'LEAD' or f_name == 'NTILE':
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(
                    func) + " as f2, char_field as f3 from {0} group by char_field, decimal_field) a " \
                            "where a.f2 in [157331,165546] select a.f1 , a.f2, ".format(self.query_bucket) + func[
                            'name'] + "(a.f2) over (partition by a.f2 order by decimal_field), a.f3"
            else:
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(
                    func) + " as f2, char_field as f3 from {0} group by char_field, decimal_field) a " \
                            "where a.f2 in [157331,165546] select a.f1 , a.f2, ".format(self.query_bucket) + func[
                            'name'] + "() over (partition by a.f2 order by a.f2), a.f3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "from (select decimal_field as f1, " + self._construct_window_function_call(
                v) + " as f2, char_field as f3 from {0} group by char_field, decimal_field) a " \
                     "where a.f2 in [157331,165546] select a.f1 , a.f2, ".format(self.query_bucket) + v[
                        'name'] + "(a.f2) over (partition by a.f2), a.f3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of FROM (SELECT..) clause plus window function parameter in WHERE clause with the use of LET clause.
        Returned data set is not analyzed.
    '''

    def test_from_select_let_where(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            if v['name'] in ['NTH_VALUE', 'CUME_DIST', 'RANK', 'ROW_NUMBER', 'DENSE_RANK', 'LEAD', 'LAG',
                             'PERCENT_RANK', 'NTILE']:
                continue
            query = "from (select decimal_field as f1, " + self._construct_window_function_call(v) + \
                    " as f2, char_field as f3 from {0} group by char_field, decimal_field) a let cond = " \
                    "[157331,165546] where a.f2 in cond select a.f1 , a.f2, ".format(self.query_bucket) + \
                    v['name'] + "(a.f2) over (partition by a.f2), a.f3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        exceptional_functions = ['NTH_VALUE', 'CUME_DIST', 'RANK', 'ROW_NUMBER', 'DENSE_RANK', 'LEAD', 'LAG',
                                 'PERCENT_RANK', 'NTILE']
        for f_name in exceptional_functions:
            func = self.window_functions[f_name]
            if f_name == 'NTH_VALUE':
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(func) + \
                        " as f2, char_field as f3 from {0} group by char_field, decimal_field) a let cond = " \
                        "[157331,165546] where a.f2 in cond select a.f1 , a.f2, ".format(self.query_bucket) + func[
                            'name'] + "(a.f2, 3) over (partition by a.f2), a.f3"
            elif f_name == 'LAG' or f_name == 'LEAD' or f_name == 'NTILE':
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(func) + \
                        " as f2, char_field as f3 from {0} group by char_field, decimal_field) a let cond = " \
                        "[157331,165546] where a.f2 in cond select a.f1 , a.f2, ".format(self.query_bucket) + func[
                            'name'] + "(a.f2) over (partition by a.f2 order by decimal_field), a.f3"
            else:
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(func) + \
                        " as f2, char_field as f3 from {0} group by char_field, decimal_field) a let cond = " \
                        "[157331,165546] where a.f2 in cond select a.f1 , a.f2, ".format(self.query_bucket) + func[
                            'name'] + "() over (partition by a.f2 order by a.f2), a.f3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "from (select decimal_field as f1, " + self._construct_window_function_call(v) + \
                    " as f2, char_field as f3 from {0} group by char_field, decimal_field) a let cond = " \
                    "[157331,165546] where a.f2 in cond select a.f1 , a.f2, ".format(self.query_bucket) + v['name'] + \
                    "(a.f2) over (partition by a.f2), a.f3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of FROM (SELECT ...) clause plus window function parameter in GROUP BY clause.
        Returned data set is not analyzed.
    '''

    def test_from_select_group_by(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            if v['name'] in ['NTH_VALUE', 'CUME_DIST', 'RANK', 'ROW_NUMBER', 'DENSE_RANK', 'LEAD', 'LAG',
                             'PERCENT_RANK', 'NTILE']:
                continue
            query = "from (select decimal_field as f1, " + self._construct_window_function_call(v) + \
                    " as f2, char_field as f3 from {0} group by char_field, decimal_field) a let cond = " \
                    "[157331,165546] where a.f2 in cond group by a.f2, a.f1, a.f3 select a.f1 as res1, a.f2" \
                    " as res2, ".format(self.query_bucket) + v['name'] + \
                    "(a.f2) over (partition by a.f2) as summa, a.f3 as res3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        exceptional_functions = ['NTH_VALUE', 'CUME_DIST', 'RANK', 'ROW_NUMBER', 'DENSE_RANK', 'LEAD', 'LAG',
                                 'PERCENT_RANK', 'NTILE']
        for f_name in exceptional_functions:
            func = self.window_functions[f_name]
            if f_name == 'NTH_VALUE':
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(func) + \
                        " as f2, char_field as f3 from {0} group by char_field, decimal_field) a let cond = " \
                        "[157331,165546] where a.f2 in cond group by a.f2, a.f1, a.f3 select a.f1 as res1, " \
                        "a.f2 as res2, ".format(self.query_bucket) + func['name'] + \
                        "(a.f2, 3) over (partition by a.f2) as summa, a.f3 as res3"
            elif f_name == 'LAG' or f_name == 'LEAD' or f_name == 'NTILE':
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(func) + \
                        " as f2, char_field as f3 from {0} group by char_field, decimal_field) a let cond = " \
                        "[157331,165546] where a.f2 in cond group by a.f2, a.f1, a.f3 select a.f1 as res1, " \
                        "a.f2 as res2, ".format(self.query_bucket) + func['name'] + \
                        "(a.f2) over (partition by a.f2 order by a.f2) as summa, a.f3 as res3"
            else:
                query = "from (select decimal_field as f1, " + self._construct_window_function_call(func) + \
                        " as f2, char_field as f3 from {0} group by char_field, decimal_field) a let cond = " \
                        "[157331,165546] where a.f2 in cond group by a.f2, a.f1, a.f3 select a.f1 as res1, " \
                        "a.f2 as res2, ".format(self.query_bucket) + func['name'] + \
                        "() over (partition by a.f2 order by a.f2) as summa, a.f3 as res3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "from (select decimal_field as f1, " + self._construct_window_function_call(v) + \
                    " as f2, char_field as f3 from {0} group by char_field, decimal_field) a let cond = " \
                    "[157331,165546] where a.f2 in cond group by a.f2, a.f1, a.f3 select a.f1 as res1," \
                    " a.f2 as res2, ".format(self.query_bucket) \
                    + v['name'] + "(a.f2) over (partition by a.f2) as summa, a.f3 as res3"
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of all types of UNION clause (union, intersect, except). Window function call is in the left side of UNION
        Returned data set is not analyzed.
    '''

    def test_window_union_intersect_except_simple(self):
        options = ['', 'all']
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            for operand in self.union_intersect_except:
                for option in options:
                    query = "select decimal_field, " + self._construct_window_function_call(
                        v) + "  as summa from {0} ".format(self.query_bucket) + operand + " " + option + \
                            " select int_field, int_field as summa from {0}".format(self.query_bucket)
                    lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                    test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                       "pre_queries": [],
                                                       "queries": [query],
                                                       "post_queries": [],
                                                       "asserts": [lambda1],
                                                       "cleanups": []
                                                       }
                    count += 1

        for k, v in self.aggregate_functions.items():
            for operand in self.union_intersect_except:
                for option in options:
                    query = "select decimal_field, " + self._construct_window_function_call(
                        v) + "  as summa from {0} ".format(self.query_bucket) + operand + " " + option + \
                            " select int_field, int_field as summa from {0}".format(self.query_bucket)
                    lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                    test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                       "pre_queries": [],
                                                       "queries": [query],
                                                       "post_queries": [],
                                                       "asserts": [lambda1],
                                                       "cleanups": []
                                                       }
                    count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of all types of UNION clause (union, intersect, except). Window function call is in the right side of UNION
        Returned data set is not analyzed.
    '''

    def test_simple_union_intersect_except_window(self):
        options = ['', 'all']
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            for operand in self.union_intersect_except:
                for option in options:
                    query = "select decimal_field, decimal_field as summa from {0} ".format(self.query_bucket) + \
                            operand + " " + option + " select int_field, " + self._construct_window_function_call(
                        v) + " as summa from {0}".format(self.query_bucket)
                    lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                    test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                       "pre_queries": [],
                                                       "queries": [query],
                                                       "post_queries": [],
                                                       "asserts": [lambda1],
                                                       "cleanups": []
                                                       }
                    count += 1

        for k, v in self.aggregate_functions.items():
            for operand in self.union_intersect_except:
                for option in options:
                    query = "select decimal_field, decimal_field as summa from {0} ".format(self.query_bucket) + \
                            operand + " " + option + " select int_field, " + self._construct_window_function_call(
                        v) + " as summa from {0}".format(self.query_bucket)
                    lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                    test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                       "pre_queries": [],
                                                       "queries": [query],
                                                       "post_queries": [],
                                                       "asserts": [lambda1],
                                                       "cleanups": []
                                                       }
                    count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of all types of UNION clause (union, intersect, except). Window function call is in both sides of UNION
        Returned data set is not analyzed.
    '''

    def test_window_union_intersect_except_window(self):
        options = ['', 'all']
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            for operand in self.union_intersect_except:
                for option in options:
                    query = "select decimal_field, " + self._construct_window_function_call(
                        v) + " as summa from {0} ".format(self.query_bucket) + operand + " " + option + \
                            " select int_field, " + self._construct_window_function_call(
                        v) + " as summa from {0}".format(self.query_bucket)
                    lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                    test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                       "pre_queries": [],
                                                       "queries": [query],
                                                       "post_queries": [],
                                                       "asserts": [lambda1],
                                                       "cleanups": []
                                                       }
                    count += 1

        for k, v in self.aggregate_functions.items():
            for operand in self.union_intersect_except:
                for option in options:
                    query = "select decimal_field, " + self._construct_window_function_call(
                        v) + " as summa from {0} ".format(self.query_bucket) + operand + " " + option + \
                            " select int_field, " + self._construct_window_function_call(
                        v) + " as summa from {0}".format(self.query_bucket)
                    lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                    test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                       "pre_queries": [],
                                                       "queries": [query],
                                                       "post_queries": [],
                                                       "asserts": [lambda1],
                                                       "cleanups": []
                                                       }
                    count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of all the following: ("", ALL, DISTINCT), (RAW, ELEMENT, VALUE) . 
        Returned data set is not analyzed.
    '''

    def test_all_distinct_raw_element_value(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            for typ in self.raw_element_value:
                for howmany in self.all_distinct:
                    query = "select " + howmany + " " + typ + " " + self._construct_window_function_call(
                        v) + " as summa from {0}".format(self.query_bucket)
                    lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                    test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                       "pre_queries": [],
                                                       "queries": [query],
                                                       "post_queries": [],
                                                       "asserts": [lambda1],
                                                       "cleanups": []
                                                       }
                    count += 1

        for k, v in self.aggregate_functions.items():
            for typ in self.raw_element_value:
                for howmany in self.all_distinct:
                    query = "select " + howmany + " " + typ + " " + self._construct_window_function_call(
                        v) + " as summa from {0}".format(self.query_bucket)
                    lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                    test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                       "pre_queries": [],
                                                       "queries": [query],
                                                       "post_queries": [],
                                                       "asserts": [lambda1],
                                                       "cleanups": []
                                                       }
                    count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of LET condition in WHERE clause . 
        Returned data set is not analyzed.
    '''

    def test_let_alias(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(v) + \
                    " as summa, char_field from {0} let char_val='A' where " \
                    "char_field=char_val".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + " as summa, char_field from {0} let char_val='A' where " \
                     "char_field=char_val".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of PARTITION BY parameter as a condition in WHERE clause . 
        Returned data set is not analyzed.
    '''

    def test_where_window_param_condition(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + " as summa, char_field from {0} where char_field='A'".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + " as summa, char_field from {0} where char_field='A'".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of PARTITION BY parameter as a condition in LETTING HAVING clause . 
        Returned data set is not analyzed.
    '''

    def test_group_by_letting_having(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + ", char_field from {0} group by char_field, decimal_field letting char_val='A' " \
                     "having char_field=char_val".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + ", char_field from {0} group by char_field, decimal_field letting char_val='A' " \
                     "having char_field=char_val".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of PARTITION BY parameter as a condition in LETTING HAVING clause and window function parameter 
        as a part of GROUP BY. 
        Returned data set is not analyzed.
    '''

    def test_group_by_having(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + ", char_field from {0} group by char_field, decimal_field having " \
                     "char_field='A'".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + ", char_field from {0} group by char_field, decimal_field " \
                     "having char_field='A'".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of PARTITION BY parameter as a condition in LETTING HAVING clause and window function 
        parameter as a part of GROUP BY. 
        Returned data set is not analyzed.
    '''

    def test_group_by_letting(self):
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + ", char_field from {0} group by char_field, decimal_field letting " \
                     "char_val='A'".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        for k, v in self.aggregate_functions.items():
            query = "select decimal_field, " + self._construct_window_function_call(
                v) + ", char_field from {0} group by char_field, decimal_field letting " \
                     "char_val='A'".format(self.query_bucket)
            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            test_dict["%d-default" % count] = {"indexes": self.indexes,
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1],
                                               "cleanups": []
                                               }
            count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of very basic usage of all sorts of window functions 
        with the use of window function call result as a condition in ORDER BY clause plus all possible ORDER BY
         options (direction, limit, offset). 
        Returned data set is not analyzed.
    '''

    def test_order_by_window_result(self):
        asc_desc = [' ', ' ASC ', ' DESC ']
        limit = [' ', ' LIMIT 10 ']
        offset = [' ', ' OFFSET 5 ']
        test_dict = dict()
        count = 0

        for k, v in self.window_functions.items():
            for op in asc_desc:
                for lim in limit:
                    for off in offset:
                        query = "select " + self._construct_window_function_call(
                            v) + " as summa from {0} order by summa ".format(self.query_bucket) + op + lim + off
                        lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                        test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                           "pre_queries": [],
                                                           "queries": [query],
                                                           "post_queries": [],
                                                           "asserts": [lambda1],
                                                           "cleanups": []
                                                           }
                        count += 1

        for k, v in self.aggregate_functions.items():
            for op in asc_desc:
                for lim in limit:
                    for off in offset:
                        query = "select " + self._construct_window_function_call(
                            v) + " as summa from {0} order by summa ".format(self.query_bucket) + op + lim + off
                        lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                        test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                           "pre_queries": [],
                                                           "queries": [query],
                                                           "post_queries": [],
                                                           "asserts": [lambda1],
                                                           "cleanups": []
                                                           }
                        count += 1

        self.query_runner(test_dict)

    '''
        Test idea - check syntax correctness of tpcds test suite query 
        see http://www.tpc.org/tpcds/ for details
        Returned data set is not analyzed.
    '''

    def test_tcpds_1(self):
        query = "SELECT char_field ,sum(decimal_field) AS itemrevenue , sum(decimal_field)*100/Sum(Sum(" \
                "decimal_field)) OVER (partition BY char_field) AS revenueratio " \
                "from {0} GROUP BY char_field ORDER BY char_field,revenueratio LIMIT 100;".format(self.query_bucket)
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')

    '''
        Test idea - check syntax correctness of tpcds test suite query 
        see http://www.tpc.org/tpcds/ for details
        Returned data set is not analyzed.
    '''

    def test_tcpds_2(self):
        query = "SELECT char_field, Sum(decimal_field) AS itemrevenue , Sum(decimal_field)*100/Sum(Sum(" \
                "decimal_field)) OVER (partition BY char_field) AS revenueratio from {0} GROUP BY char_field " \
                "ORDER BY char_field , revenueratio LIMIT 100;".format(self.query_bucket)
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')

    '''
        Test idea - check syntax correctness of tpcds test suite query 
        see http://www.tpc.org/tpcds/ for details
        Returned data set is not analyzed.
    '''

    def test_tcpds_3(self):
        query = "SELECT 'web' AS channel, web.item, web.return_ratio, web.return_rank,web.currency_rank " \
                "FROM (SELECT item, return_ratio, currency_ratio,Rank() OVER (ORDER BY return_ratio) AS return_rank," \
                "Rank() OVER (ORDER BY currency_ratio) AS currency_rank FROM " \
                "(SELECT ws.char_field AS item, (Sum(COALESCE(ws.int_field, 0)) / " \
                "(Sum(COALESCE(ws.decimal_field, 0)) )) AS return_ratio," \
                "(Sum(COALESCE(ws.int_field, 0))) / (Sum(COALESCE(ws.decimal_field, 0))) AS currency_ratio " \
                "from {0} ws GROUP BY ws.char_field) in_web) web WHERE ( web.return_rank <= 10 OR " \
                "web.currency_rank <= 10 ) UNION SELECT 'catalog' AS " \
                "channel,catalog.item,catalog.return_ratio,catalog.return_rank,catalog.currency_rank " \
                "FROM (SELECT item,return_ratio,currency_ratio,Rank() OVER (ORDER BY return_ratio) AS return_rank," \
                "Rank() OVER (ORDER BY currency_ratio) AS currency_rank FROM (SELECT cs.char_field AS item," \
                "(Sum(COALESCE(cs.int_field, 0))) / (Sum(COALESCE(cs.decimal_field, 0))) AS return_ratio," \
                "(Sum(COALESCE(cs.int_field, 0))) / (Sum(COALESCE(cs.decimal_field, 0))) AS currency_ratio " \
                "from {0} cs GROUP BY cs.char_field) in_cat) catalog WHERE " \
                "( catalog.return_rank <= 10 OR catalog.currency_rank <= 10 ) UNION " \
                "SELECT 'store' AS channel, store.item,store.return_ratio,store.return_rank,store.currency_rank " \
                "FROM (SELECT item,return_ratio,currency_ratio,Rank() OVER (ORDER BY return_ratio) AS return_rank," \
                "Rank() OVER (ORDER BY currency_ratio) AS currency_rank FROM (SELECT sts.char_field AS item," \
                "(Sum(COALESCE(sts.int_field, 0))) / (Sum(COALESCE(sts.decimal_field, 0))) AS return_ratio," \
                "(Sum(COALESCE(sts.int_field, 0))) / (Sum(COALESCE(sts.decimal_field, 0))) AS currency_ratio " \
                "from {0} sts GROUP BY sts.char_field) in_store) store " \
                "WHERE ( store.return_rank <= 10 OR store.currency_rank <= 10 ) ORDER BY 1,4,5 " \
                "LIMIT 100".format(self.query_bucket)
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')

    '''
        Test idea - check syntax correctness of tpcds test suite query 
        see http://www.tpc.org/tpcds/ for details
        Returned data set is not analyzed.
    '''

    def test_tcpds_4(self):
        query = "SELECT * FROM (SELECT char_field,SUM(decimal_field) sum_sales,AVG(SUM(decimal_field)) OVER " \
                "( PARTITION BY char_field) avg_quarterly_sales " \
                "from {0} WHERE char_field IN " \
                "['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'] " \
                "GROUP BY char_field) tmp1 WHERE CASE WHEN avg_quarterly_sales > 0 THEN " \
                "ABS (sum_sales - avg_quarterly_sales) / avg_quarterly_sales ELSE NULL END > 0.1 " \
                "ORDER BY avg_quarterly_sales,sum_sales,char_field LIMIT 100".format(self.query_bucket)
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')

    '''
        Test idea - check syntax correctness of tpcds test suite query 
        see http://www.tpc.org/tpcds/ for details
        Returned data set is not analyzed.
    '''

    def test_tcpds_5(self):
        query = "SELECT * FROM (SELECT char_field,SUM(decimal_field) sum_sales,AVG(SUM(int_field)) OVER " \
                "( PARTITION BY char_field) avg_monthly_sales " \
                "from {0} GROUP BY char_field) tmp1 WHERE CASE WHEN avg_monthly_sales > 0 THEN ABS " \
                "(sum_sales - avg_monthly_sales) / avg_monthly_sales ELSE NULL END > 0.1 " \
                "ORDER BY char_field,avg_monthly_sales,sum_sales LIMIT 100".format(self.query_bucket)
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')

    '''
        Test idea - check syntax correctness of tpcds test suite query 
        see http://www.tpc.org/tpcds/ for details
        Returned data set is not analyzed.
    '''

    def test_tcpds_6(self):
        query = "SELECT SUM(decimal_field) AS total_sum,char_field,Rank() OVER ( PARTITION BY char_field ORDER BY " \
                "SUM(decimal_field) DESC) AS rank_within_parent " \
                "from {0} a WHERE char_field IN (SELECT char_field FROM (SELECT char_field,Rank() OVER " \
                "( PARTITION BY char_field ORDER BY SUM(decimal_field) DESC) AS ranking " \
                "from {0} b GROUP BY char_field) tmp1 WHERE ranking <= 5) GROUP BY char_field ORDER BY " \
                "lochierarchy DESC, CASE WHEN lochierarchy = 0 THEN s_state END, rank_within_parent " \
                "LIMIT 100".format(self.query_bucket)
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')

    '''
        Test idea - check syntax correctness of tpcds test suite query 
        see http://www.tpc.org/tpcds/ for details
        Returned data set is not analyzed.
    '''

    def test_tcpds_7(self):
        query = "SELECT SUM(decimal_field) AS total_sum,char_field,Rank() OVER ( PARTITION BY char_field ORDER BY " \
                "SUM(decimal_field) DESC) AS rank_within_parent " \
                "from {0} GROUP BY char_field ORDER BY total_sum DESC,CASE WHEN total_sum = 0 THEN " \
                "char_field END,rank_within_parent LIMIT 100".format(self.query_bucket)
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')

    '''
        Test idea - check syntax correctness of tpcds test suite query 
        see http://www.tpc.org/tpcds/ for details
        Returned data set is not analyzed.
    '''

    def test_tcpds_8(self):
        query = "SELECT * FROM (SELECT char_field, SUM(decimal_field) sum_sales,AVG(SUM(decimal_field)) OVER " \
                "( PARTITION BY char_field, int_field ) avg_monthly_sales " \
                "from {0} GROUP BY char_field, sum_sales, int_field) tmp1 LIMIT 100".format(self.query_bucket)
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')

    '''
        Test idea - check syntax correctness of tpcds test suite query 
        see http://www.tpc.org/tpcds/ for details
        Returned data set is not analyzed.
    '''

    def test_tcpds_9(self):
        query = "SELECT char_field,SUM(decimal_field) AS itemrevenue,SUM(decimal_field) * 100 / SUM(" \
                "SUM(decimal_field)) OVER ( PARTITION BY char_field) AS revenueratio " \
                "from {0} WHERE char_field IN ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O'," \
                "'P','Q','R','S','T','U','V','W','Z','Y','Z'] " \
                "GROUP BY char_field ORDER BY char_field, revenueratio;".format(self.query_bucket)
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')

    '''
        Test idea - check FIRST_VALUE function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_first_value(self):
        query = "select first_value(decimal_field) over (partition by char_field order by decimal_field) as fv, " \
                "decimal_field as df, char_field as cf from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)

        test_dict = dict()
        for alpha in self.alphabet:
            query_min = "select decimal_field from {0} where char_field='{1}' order " \
                        "by decimal_field".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(query_min)
            val = 0
            if 'decimal_field' not in test_result['results'][0]:
                val = None
            else:
                val = test_result['results'][0]['decimal_field']
            test_dict[alpha] = val

        for res in result['results']:
            ch = res['cf']
            db_val = None
            if 'fv' in res:
                db_val = res['fv']

            if db_val != test_dict[ch]:
                self.assertEqual('True', 'False', "Values are: db - " + str(db_val) + ", test - " + str(
                    test_dict[ch]) + ", letter - " + str(ch))

    '''
        Test idea - check RATIO_TO_REPORT function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_ratio_to_report(self):
        query = "select ratio_to_report(decimal_field) over (partition by char_field) as ratio_to_report, " \
                "decimal_field, char_field from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select sum(decimal_field) from {0} where char_field='{1}'".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]['$1']

        for res in result['results']:
            if 'decimal_field' in res:
                if res['decimal_field']:
                    alpha = res['char_field']
                    checksum = float(res['decimal_field']) / float(test_dict[alpha])
                    if res['ratio_to_report'] != checksum:
                        self.assertEqual('True', 'False',
                                         'Test is failed: char_field - ' + str(alpha) + ", decimal_field - " + str(
                                             res['decimal_field']) +
                                         ", ratio_to_report - " + str(res['ratio_to_report']) + ", checksum - " + str(
                                             checksum) + ", test_dict - " + str(test_dict[alpha]))

    '''
        Test idea - check LAST_VALUE function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_last_value(self):
        query = "select last_value(decimal_field) over (partition by char_field order by decimal_field RANGE " \
                "BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_value, " \
                "decimal_field, char_field from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)

        test_dict = {}
        for alpha in self.alphabet:
            query_max = "select decimal_field from {0} where char_field='{1}' order by " \
                        "decimal_field desc".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(query_max)
            if 'decimal_field' in test_result['results'][0]:
                test_dict[alpha] = test_result['results'][0]['decimal_field']
            else:
                test_dict[alpha] = None

        for res in result['results']:
            test_val = test_dict[res['char_field']]
            if 'last_value' in res:
                db_val = res['last_value']
            else:
                db_val = None
            if test_val != db_val:
                self.assertEqual('True', 'False',
                                 'Test is failed: char - ' + str(res['char_field']) + ", decimal - " + str(
                                     res['decimal_field']) +
                                 ", test_val - " + str(test_val) + ", last_value - " + str(db_val))

    '''
        Test idea - check ROW_NUMBER function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_row_number(self):
        check_number = 8
        query = "select sq.df as decimal_field, sq.cf as char_field, sq.rn from (select row_number() over " \
                "(partition by char_field order by decimal_field) as rn, " \
                "decimal_field as df, char_field as cf from {0}) sq where sq.rn={1}".format(self.query_bucket,
                                                                                            check_number)
        result = self.run_cbq_query(query)

        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select decimal_field from {0} where char_field='{1}' order by " \
                         "decimal_field".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(test_query)
            if len(test_result['results']) > 0:
                if 'decimal_field' in test_result['results'][check_number - 1]:
                    test_dict[alpha] = test_result['results'][check_number - 1]['decimal_field']
            else:
                test_dict[alpha] = None

        for res in result['results']:
            char_val = str(res['char_field'])
            if 'decimal_field' in res:
                db_val = res['decimal_field']
            else:
                db_val = None
            if char_val in test_dict:
                if db_val != test_dict[char_val]:
                    self.assertEqual('True', 'False', 'Test is failed: char - ' + str(char_val) + ", db_val - " + str(
                        db_val) + ", test_val - " + str(test_dict[char_val]))

    '''
        Test idea - check ARRAY_AGG window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_array_agg(self):
        query = "select distinct(char_field), array_agg(decimal_field) over (partition by char_field) as " \
                "agg from {0} where decimal_field is not null".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select array_agg(decimal_field) from {0} where decimal_field is not null and char_field='{1}'".format(self.query_bucket,
                                                                                                  alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]
        for alpha in self.alphabet:
            if alpha in db_dict.keys():
                db_value = db_dict[alpha]
            else:
                db_value = None
            test_value = test_dict[alpha]['$1']
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check AVG window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_avg(self):
        query = "select distinct(char_field), avg(decimal_field) over (partition by char_field) as " \
                "agg from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select avg(decimal_field) from {0} where char_field='{1}'".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = db_dict[alpha]
            test_value = test_dict[alpha]['$1']
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check COUNT window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_count(self):
        query = "select distinct(char_field), count(decimal_field) over (partition by char_field) as agg " \
                "from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select count(decimal_field) from {0} where char_field='{1}'".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = db_dict[alpha]
            test_value = test_dict[alpha]['$1']
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check COUNTN window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_countn(self):
        query = "select distinct(char_field), countn(decimal_field) over (partition by char_field) as agg" \
                " from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select countn(decimal_field) from {0} where char_field='{1}'".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = db_dict[alpha]
            test_value = test_dict[alpha]['$1']
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check MAX window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_max(self):
        query = "select distinct(char_field), max(decimal_field) over (partition by char_field) as agg " \
                "from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select max(decimal_field) from {0} where char_field='{1}'".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = db_dict[alpha]
            test_value = test_dict[alpha]['$1']
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check MEAN window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_mean(self):
        query = "select distinct(char_field), mean(decimal_field) over (partition by char_field) as agg " \
                "from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select mean(decimal_field) from {0} where char_field='{1}'".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = db_dict[alpha]
            test_value = test_dict[alpha]['$1']
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check MEDIAN window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_median(self):
        query = "select distinct(char_field), median(decimal_field) over (partition by char_field) as agg " \
                "from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select median(decimal_field) from {0} where char_field='{1}'".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = db_dict[alpha]
            test_value = test_dict[alpha]['$1']
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check MIN window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_min(self):
        query = "select distinct(char_field), min(decimal_field) over (partition by char_field) as agg " \
                "from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select min(decimal_field) from {0} where char_field='{1}'".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = db_dict[alpha]
            test_value = test_dict[alpha]['$1']
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check SUM window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_sum(self):
        query = "select distinct(char_field), sum(decimal_field) over (partition by char_field) as " \
                "agg from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select sum(decimal_field) from {0} where char_field='{1}'".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = db_dict[alpha]
            test_value = test_dict[alpha]['$1']
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check STDDEV window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_stddev(self):
        query = "select distinct(char_field), round(stddev(decimal_field) over (partition by char_field),10) as agg " \
                "from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select round(stddev(decimal_field),10) from {0} where char_field='{1}'".format(self.query_bucket, alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = str(db_dict[alpha])
            test_value = str(test_dict[alpha]['$1'])
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check STDDEV_SAMP window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_stddev_samp(self):
        query = "select distinct(char_field), round(stddev_samp(decimal_field) over (partition by char_field), 10) as " \
                "agg from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select round(stddev_samp(decimal_field), 10) from {0} where char_field='{1}'".format(self.query_bucket,
                                                                                                    alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = str(db_dict[alpha])
            test_value = str(test_dict[alpha]['$1'])
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check STDDEV_POP window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_stddev_pop(self):
        query = "select distinct(char_field), round(stddev_pop(decimal_field) over (partition by char_field), 10) as " \
                "agg from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select round(stddev_pop(decimal_field),10) from {0} where char_field='{1}'".format(self.query_bucket,
                                                                                                   alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = str(db_dict[alpha])
            test_value = str(test_dict[alpha]['$1'])
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check VARIANCE window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_variance(self):
        query = "select distinct(char_field), round(variance(decimal_field) over (partition by char_field), 7) as agg" \
                " from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select round(variance(decimal_field),7) from {0} where char_field='{1}'".format(self.query_bucket,
                                                                                                 alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = str(db_dict[alpha])
            test_value = str(test_dict[alpha]['$1'])
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check VAR_SAMP window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_var_samp(self):
        query = "select distinct(char_field), round(var_samp(decimal_field) over (partition by char_field), 7) as" \
                " agg from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select round(var_samp(decimal_field), 7) from {0} where char_field='{1}'".format(self.query_bucket,
                                                                                                 alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = str(db_dict[alpha])
            test_value = str(test_dict[alpha]['$1'])
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check VAR_POP window function call against appropriate non window function queries 
        Returned data set is checked.
    '''

    def test_var_pop(self):
        query = "select distinct(char_field), round(var_pop(decimal_field) over (partition by char_field), 7) as agg " \
                "from {0}".format(self.query_bucket)
        result = self.run_cbq_query(query)
        db_dict = {}
        for res in result['results']:
            char_key = str(res['char_field'])
            db_dict[char_key] = res['agg']
        test_dict = {}
        for alpha in self.alphabet:
            test_query = "select round(var_pop(decimal_field),7) from {0} where char_field='{1}'".format(self.query_bucket,
                                                                                                alpha)
            test_result = self.run_cbq_query(test_query)
            test_dict[alpha] = test_result['results'][0]

        for alpha in self.alphabet:
            db_value = str(db_dict[alpha])
            test_value = str(test_dict[alpha]['$1'])
            self.assertEqual(db_value, test_value,
                             'Test failed: window value ::' + str(db_value) + ':: test_value ::' + str(
                                 test_value) + ':: char - ' + str(alpha))

    '''
        Test idea - check prepareds creation and execution with the use of window function call. 
        Returned data set is not checked.
    '''

    def test_prepared_statements(self):
        shell = RemoteMachineShellConnection(self.master)

        # ------------------ create prepareds -----------------------
        for k, v in self.window_functions.items():
            cmd = ("{3} -u {0}:{1} http://{2}:8093/query/service -d "
                   "statement='prepare prepared_" + v['name'] + " from select decimal_field, " +
                   self._construct_window_function_call(v) +
                   "  as wf_result from {4} where char_field in $1 order by "
                   "char_field'").format('Administrator', 'password', self.master.ip, self.curl_path, self.query_bucket)
            shell.execute_command(cmd)

        for k, v in self.aggregate_functions.items():
            cmd = ("{3} -u {0}:{1} http://{2}:8093/query/service -d "
                   "statement='prepare prepared_" + v['name'] +
                   " from select decimal_field, " + self._construct_window_function_call(v) +
                   "  as wf_result from {4} where char_field in $1 order by "
                   "char_field'").format('Administrator', 'password', self.master.ip, self.curl_path, self.query_bucket)
            shell.execute_command(cmd)

        # -------------- execute prepareds -----------------------
        for k, v in self.window_functions.items():
            cmd = ("{3} -u {0}:{1} http://{2}:8093/query/service -d "
                   "statement='execute prepared_" + v['name'] + "&args=[[\"A\",\"B\",\"C\",\"D\",\"E\"]]'"
                   ). \
                format('Administrator', 'password', self.master.ip, self.curl_path)

            output, error = shell.execute_command(cmd)
            json_output_str = ''
            for s in output:
                json_output_str += s
            json_output = json.loads(json_output_str)
            results_count = int(json_output['metrics']['resultCount'])
            status = str(json_output['status'])
            self.assertEqual(results_count > 0 and status == 'success', True,
                             "Test is failed. Function name is " + v['name'])

        for k, v in self.aggregate_functions.items():
            cmd = ("{3} -u {0}:{1} http://{2}:8093/query/service -d "
                   "statement='execute prepared_" + v['name'] + "&args=[[\"A\",\"B\",\"C\",\"D\",\"E\"]]'"
                   ). \
                format('Administrator', 'password', self.master.ip, self.curl_path)

            output, error = shell.execute_command(cmd)
            json_output_str = ''
            for s in output:
                json_output_str += s
            json_output = json.loads(json_output_str)
            results_count = int(json_output['metrics']['resultCount'])
            status = str(json_output['status'])
            self.assertEqual(results_count > 0 and status == 'success', True,
                             "Test is failed. Function name is " + v['name'])

    '''
        Test idea - check parameterized queries calls with the use of positional parameters 
        Returned data set is checked.
    '''

    def test_parameterized_positional(self):
        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d ""statement='select 2*$1&args" \
              "=[3]'".format('Administrator', 'password', self.master.ip, self.curl_path)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is simple test")

        cmd = ("{3} -u {0}:{1} http://{2}:8093/query/service -d "
               "statement='select decimal_field, ntile($1) over(partition by char_field order by char_field) as"
               " wf_result from {4}&args=[2]'").format('Administrator', 'password', self.master.ip,
                                                       self.curl_path, self.query_bucket)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is NTILE")

        cmd = ("{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select decimal_field, "
               "first_value(decimal_field) over(partition by char_field  order by char_field groups between "
               "unbounded preceding and $1 following) as wf_result from"
               " {4}&args=[2]'").format('Administrator', 'password', self.master.ip, self.curl_path, self.query_bucket)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is FIRST_VALUE")

        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select decimal_field, " \
              "ratio_to_report(decimal_field) over(partition by char_field  " \
              "order by char_field groups between unbounded preceding and $1 following) as wf_result from " \
              "{4}&args=[2]'".format('Administrator', 'password', self.master.ip, self.curl_path, self.query_bucket)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is RATIO_TO_REPORT")

        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select decimal_field, " \
              "last_value(decimal_field) over(partition by char_field  order by char_field groups " \
              "between unbounded preceding and $1 following) as wf_result from " \
              "{4}&args=[2]'".format('Administrator', 'password', self.master.ip, self.curl_path, self.query_bucket)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is LAST_VALUE")

        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select decimal_field, " \
              "nth_value(decimal_field, $1) over(partition by char_field  order by char_field groups between " \
              "unbounded preceding and $2 following) as wf_result from " \
              "{4}&args=[2,3]'".format('Administrator', 'password', self.master.ip, self.curl_path, self.query_bucket)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is NTH_VALUE")

        for k, v in self.aggregate_functions.items():
            cmd = ("{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select decimal_field, " + v['name'] +
                   "(decimal_field) over(partition by char_field  order by char_field groups between unbounded "
                   "preceding and $1 following) as wf_result from {4}&args=[2]'").format('Administrator', 'password',
                                                                                         self.master.ip, self.curl_path,
                                                                                         self.query_bucket)
            res = self._execute_and_check_shell_query(cmd)
            self.assertEqual(res, True, "Test is failed. Function name is " + v['name'])

    '''
        Test idea - check parameterized queries calls with the use of named parameters 
        Returned data set is checked.
    '''

    def test_parameterized_named(self):
        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select " \
              "2*$param&$param=3'".format('Administrator', 'password', self.master.ip, self.curl_path)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is simple test")

        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select decimal_field, ntile($param) " \
              "over(partition by char_field order by char_field) as wf_result from " \
              "{4}&$param=2'".format('Administrator', 'password', self.master.ip, self.curl_path, self.query_bucket)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is NTILE")

        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select decimal_field, " \
              "first_value(decimal_field) over(partition by char_field  order by char_field groups between " \
              "unbounded preceding and $param following) as wf_result from " \
              "{4}&$param=2'".format('Administrator', 'password', self.master.ip, self.curl_path, self.query_bucket)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is FIRST_VALUE")

        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select decimal_field, " \
              "ratio_to_report(decimal_field) over(partition by char_field  order by char_field groups between " \
              "unbounded preceding and $param following) as wf_result from " \
              "{4}&$param=2'".format('Administrator', 'password', self.master.ip, self.curl_path, self.query_bucket)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is RATIO_TO_REPORT")

        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select decimal_field, " \
              "last_value(decimal_field) over(partition by char_field  order by char_field groups between unbounded " \
              "preceding and $param following) as wf_result from " \
              "{4}&$param=2'".format('Administrator', 'password', self.master.ip, self.curl_path, self.query_bucket)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is LAST_VALUE")

        cmd = "{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select decimal_field, " \
              "nth_value(decimal_field, $param1) over(partition by char_field  order by char_field groups between " \
              "unbounded preceding and $param2 following) as wf_result from " \
              "{4}&$param1=2&$param2=3'".format('Administrator', 'password', self.master.ip, self.curl_path,
                                                self.query_bucket)
        res = self._execute_and_check_shell_query(cmd)
        self.assertEqual(res, True, "Test is failed. Function name is NTH_VALUE")

        for k, v in self.aggregate_functions.items():
            cmd = ("{3} -u {0}:{1} http://{2}:8093/query/service -d statement='select decimal_field, " + v['name'] +
                   "(decimal_field) over(partition by char_field  order by char_field groups between unbounded "
                   "preceding and $param following) as wf_result from "
                   "{4}&$param=2'").format('Administrator', 'password', self.master.ip,
                                           self.curl_path, self.query_bucket)
            res = self._execute_and_check_shell_query(cmd)
            self.assertEqual(res, True, "Test is failed. Function name is " + v['name'])

    '''
        Test idea - check all kinds of meta() fields as PARTITION BY argument. 
        Returned data set is checked.
    '''

    def test_meta_properties(self):
        test_dict = dict()
        count = 0

        meta_fields = [' meta().cas ', ' meta().expiration ', ' meta().flags ', ' meta().id ', ' meta().type ']
        for k, v in self.window_functions.items():
            for meta in meta_fields:
                query = "select decimal_field, " + v['name'] + v['params'] + " over(partition by " + meta + " " + v[
                    'order'] + " " + v['range'] + ") as wf_result from {0} order by " \
                                                  "decimal_field".format(self.query_bucket)
                lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                   "pre_queries": [],
                                                   "queries": [query],
                                                   "post_queries": [],
                                                   "asserts": [lambda1],
                                                   "cleanups": []
                                                   }
                count += 1

        for k, v in self.aggregate_functions.items():
            for meta in meta_fields:
                query = "select decimal_field, " + v['name'] + v['params'] + " over(partition by " + meta + " " + v[
                    'order'] + " " + v['range'] + ") as wf_result from {0} order by " \
                                                  "decimal_field".format(self.query_bucket)
                lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                test_dict["%d-default" % count] = {"indexes": self.indexes,
                                                   "pre_queries": [],
                                                   "queries": [query],
                                                   "post_queries": [],
                                                   "asserts": [lambda1],
                                                   "cleanups": []
                                                   }
                count += 1

        self.query_runner(test_dict)

    def _execute_and_check_shell_query(self, cmd):
        shell = RemoteMachineShellConnection(self.master)
        output, error = shell.execute_command(cmd)
        json_output_str = ''
        for s in output:
            json_output_str += s
        json_output = json.loads(json_output_str)
        results_count = int(json_output['metrics']['resultCount'])
        status = str(json_output['status'])
        return results_count > 0 and status == 'success'

    def _construct_window_function_call(self, v):
        return v['name'] + v['params'] + " over(" + v['partition'] + " " + v['order'] + " " + v['range'] + ")"

    def init_nodes(self):
        test_bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket(self.test_buckets, 11222, test_bucket_params)

    def load_test_data(self, bucket_name='test_bucket'):
        for i in range(0, 999, 1):
            initial_statement = (" INSERT INTO {0} (KEY, VALUE) VALUES ('primary_key_" + str(i) + "',").format(
                bucket_name)
            initial_statement += "{"
            initial_statement += "'primary_key':'primary_key_" + str(i) + "','char_field':'" + \
                                 random.choice(string.ascii_uppercase) + "','decimal_field':" + \
                                 str(round(10000 * random.random(), 0)) + ",'int_field':" + \
                                 str(randint(0, 100000000)) + "})"
            self.run_cbq_query(initial_statement)

    def adopt_test_data(self, bucket_name='test_bucket'):
        bucket_name = self.get_collection_name(bucket_name)
        self.run_cbq_query("update {0} set decimal_field=null where char_field='A'".format(bucket_name))
        self.run_cbq_query("update {0} set decimal_field=missing where char_field='B'".format(bucket_name))

        self.run_cbq_query(
            "update {0} set decimal_field=null where char_field='C' and decimal_field%2=0".format(bucket_name))
        self.run_cbq_query(
            "update {0} set decimal_field=missing where char_field='C' and decimal_field%3=0".format(bucket_name))

        self.run_cbq_query(
            "update {0} set decimal_field=2 where char_field='D' and decimal_field%2=0".format(bucket_name))
        self.run_cbq_query("update {0} set decimal_field=1 where char_field='E'".format(bucket_name))

    def create_primary_index(self, bucket_name='test_bucket'):
        bucket_name = self.get_collection_name(bucket_name)
        self.run_cbq_query("CREATE PRIMARY INDEX `#primary` ON {0}".format(bucket_name))

    def create_secondary_indexes(self, bucket_name='test_bucket'):
        query_bucket = self.get_collection_name(bucket_name)
        self.run_cbq_query('CREATE INDEX ix_char ON {0}(char_field);'.format(query_bucket))
        self._wait_for_index_online(bucket=bucket_name, index_name='ix_char')

        self.run_cbq_query('CREATE INDEX ix_decimal ON {0}(decimal_field);'.format(query_bucket))
        self._wait_for_index_online(bucket=bucket_name, index_name='ix_decimal')

        self.run_cbq_query('CREATE INDEX ix_int ON {0}(int_field);'.format(query_bucket))
        self._wait_for_index_online(bucket=bucket_name, index_name='ix_int')

        self.run_cbq_query('CREATE INDEX ix_primary ON {0}(primary_key);'.format(query_bucket))
        self._wait_for_index_online(bucket=bucket_name, index_name='ix_primary')

    def test_MB53353(self):
        query = '''
        SELECT  x.a AS a,
        SUM(x.a) OVER (PARTITION BY x.c ORDER BY x.a ROWS BETWEEN CURRENT ROW AND unbounded FOLLOWING) AS aSum
        FROM [
        {"a":0,"c":'one'},
        {"a":1,"c":'one'},
        {"a":4,"c":'one'},
        {"a":7,"c":'one'}
        ] AS x
        ORDER BY a ASC
        '''
        result = self.run_cbq_query(query)
        expected = [
            {'a': 0, 'aSum': 12},
            {'a': 1, 'aSum': 12},
            {'a': 4, 'aSum': 11},
            {'a': 7, 'aSum': 7}
        ]
        self.assertEqual(result['results'], expected, f"We got unexpected result {result['results']}")