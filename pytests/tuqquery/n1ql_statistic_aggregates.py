from .tuq import QueryTests
import math
import sys
from membase.api.exception import CBQError


class StatisticAggregatesTest(QueryTests):

    def setUp(self):
        super(StatisticAggregatesTest, self).setUp()
        self.log_config_info()
        self.log.info("==============  StatisticAggregatesTest setup has started ==============")

        self.numbers = [1, 3, 5, 7, 9, 10, 0, 2, 4, 6, 8, 13]
        self.func_names = ['median', 'stddev', 'variance', 'stddev_pop', 'variance_pop', 'stddev_samp', 'variance_samp', 'mean']
        self.datatypes = ['int', 'float']

        self.primary_idx = {'name': '#primary', 'bucket': "temp_bucket", 'fields': [], 'state': 'online', 'using': self.index_type.lower(), 'is_primary': True}
        self.idx_1 = {'name': "ix1", 'bucket': "temp_bucket", 'fields': [("int_field", 0)], 'state': "online", 'using': self.index_type.lower(), 'is_primary': False}
        self.idx_3 = {'name': "ix3", 'bucket': "temp_bucket", 'fields': [("float_field", 0)], 'state': "online", 'using': self.index_type.lower(), 'is_primary': False}

        self.indexes = [self.primary_idx, self.idx_1, self.idx_3]
        self.log.info("==============  StatisticAggregatesTest setup has completed ==============")

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  StatisticAggregatesTest tearDown has started ==============")
        super(StatisticAggregatesTest, self).tearDown()
        self.log.info("==============  StatisticAggregatesTest tearDown has completed ==============")

    def suite_setUp(self):
        super(StatisticAggregatesTest, self).suite_setUp()
        self.log_config_info()
        self.log.info("==============  StatisticAggregatesTest suite_setup has started ==============")
        self.log.info("==============  StatisticAggregatesTest suite_setup has completed ==============")

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  StatisticAggregatesTest suite_tearDown has started ==============")
        super(StatisticAggregatesTest, self).suite_tearDown()
        self.log.info("==============  StatisticAggregatesTest suite_tearDown has completed ==============")

    ''' executes all tests from this suite '''
    def run_all(self):
        self.test_simple_call_all_datatypes()
        self.test_simple_call_all_datatypes_nulls_included()
        self.test_simple_call_all_nulls_missings()
        self.test_func_names_different_cases()
        self.test_secondary_index_usage()
        self.test_mixed_parameters()
        self.test_index_pushdown()
        self.test_nested_calls_negative()
        self.test_empty_args_list_calls_negative()

    '''test for all simple functions calls with the use of 2 datatypes: int and float.
    select {median|stddev|variance|stddev_pop|vbariance_pop|stddev_sample|variance_sample|mean}(int|long|float) from bucket where field is not nullormissing '''
    def test_simple_call_all_datatypes(self):
        self._load_test_data()

        test_dict = dict()
        count = 0

        for fname in self.func_names:
            for datatype in self.datatypes:
                query = "select "+fname+"("+datatype+"_field) from temp_bucket where "+datatype+"_field is not null or missing"

                args = [float(x) for x in self.numbers]
                if datatype == 'float':
                    args = [x*math.pi for x in self.numbers]

                arithmetic_result = eval('self._calculate_'+fname+'_value(args)')
                my_result = "%.9f" % float(arithmetic_result)

                lambda1 = lambda x, y = my_result: self.assertEqual("%.9f" % float(str(x['q_res'][0]['results'][0]["$1"])), y)
                test_dict["%d-default" % (count)] = {"indexes": self.indexes,
                                                     "pre_queries": [],
                                                     "queries": [query],
                                                     "post_queries": [],
                                                     "asserts": [lambda1],
                                                     "cleanups": []}
                count += 1

        try:
            self.query_runner(test_dict)
        finally:
            self._unload_test_data()

    '''test for all simple functions calls with the use of 2 datatypes: int and float. Null values are included
    select {median|stddev|variance|stddev_pop|vbariance_pop|stddev_sample|variance_sample|mean}(int|long|float) from bucket'''
    def test_simple_call_all_datatypes_nulls_included(self):
        self._load_test_data_including_nulls()

        test_dict = dict()
        count = 0

        for fname in self.func_names:
            for datatype in self.datatypes:
                query = "select "+fname+"("+datatype+"_field) from temp_bucket"

                args = [float(x) for x in self.numbers]
                if datatype == 'float':
                    args = [x*math.pi for x in self.numbers]

                arithmetic_result = eval('self._calculate_'+fname+'_value(args)')
                my_result = "%.9f" % float(arithmetic_result)

                lambda1 = lambda x, y=my_result: self.assertEqual("%.9f" % float(str(x['q_res'][0]['results'][0]["$1"])), y)
                test_dict["%d-default" % (count)] = {"indexes": self.indexes,
                                                     "pre_queries": [],
                                                     "queries": [query],
                                                     "post_queries": [],
                                                     "asserts": [lambda1],
                                                     "cleanups": []}
                count += 1

        try:
            self.query_runner(test_dict)
        finally:
            self._unload_test_data()

    '''test for simple function calls with the use of only NULL parameters'''
    '''select {median|stddev|variance|stddev_pop|vbariance_pop|stddev_sample|variance_sample|mean}(int|long|float) from bucket where field is nullormissing'''
    def test_simple_call_all_nulls_missings(self):
        self._load_test_data_nulls_missings()

        test_dict = dict()
        count = 0

        for fname in self.func_names:
            query = "select "+fname+"(int_field) from temp_bucket"

            my_result = None
            lambda1 = lambda x, y=my_result: self.assertEqual(x['q_res'][0]['results'][0]["$1"], y)
            test_dict["%d-default" % (count)] = {"indexes": self.indexes,
                                                 "pre_queries": [],
                                                 "queries": [query],
                                                 "post_queries": [],
                                                 "asserts": [lambda1],
                                                 "cleanups": []}
            count += 1

        try:
            self.query_runner(test_dict)
        finally:
            self._unload_test_data()

    '''test for different letter cases in function names'''
    def test_func_names_different_cases(self):
        self._load_test_data()

        test_dict = dict()
        count = 0

        func_names = ['median', 'stddev', 'variance', 'stddev_pop', 'variance_pop', 'stddev_samp', 'variance_samp', 'mean',
                      'MediaN', 'stDdev', 'vaRiance', 'stDdev_pop', 'variAnce_pop', 'stdDev_samp', 'variaNce_samp', 'meAn',
                      'MEDIAN', 'STDDEV', 'VARIANCE', 'STDDEV_POP', 'VARIANCE_POP', 'STDDEV_SAMP', 'VARIANCE_SAMP', 'MEAN']

        for fname in func_names:
            query = "select "+fname+"(int_field) from temp_bucket"

            my_result = "success"
            lambda1 = lambda x, y=my_result: self.assertEqual(x['q_res'][0]['status'], y)
            test_dict["%d-default" % (count)] = {"indexes": self.indexes,
                                                 "pre_queries": [],
                                                 "queries": [query],
                                                 "post_queries": [],
                                                 "asserts": [lambda1],
                                                 "cleanups": []}
            count += 1

        try:
            self.query_runner(test_dict)
        finally:
            self._unload_test_data()

    '''test for secondary index usage'''
    def test_secondary_index_usage(self):
        self._load_test_data()

        test_dict = dict()
        count = 0

        for fname in self.func_names:
            for datatype in self.datatypes:
                query = "explain select "+fname+"("+datatype+"_field) from temp_bucket"
                result = ''
                if datatype == 'int':
                    result = 'ix1'
                else:
                    result = 'ix3'

                lambda1 = lambda x, y=result: self.assertEqual(x['q_res'][0]['results'][0]['plan']['~children'][0]['index'], y)
                test_dict["%d-default" % (count)] = {"indexes": self.indexes,
                                                     "pre_queries": [],
                                                     "queries": [query],
                                                     "post_queries": [],
                                                     "asserts": [lambda1],
                                                     "cleanups": []}
                count += 1

        try:
            self.query_runner(test_dict)
        finally:
            self._unload_test_data()

    '''mixed datatypes in function parameters list'''
    def test_mixed_parameters(self):
        self._load_mixed_datatypes()

        test_dict = dict()
        count = 0

        for fname in self.func_names:
            for datatype in self.datatypes:
                query = "select "+fname+"("+datatype+"_field) from temp_bucket"

                args = []
                if datatype == 'int':
                    args = [1.0, 3.0, 6.0, 8.0, 9.0, 10.0]
                else:
                    args = [3.14, 9.14, 18.14, 21.14, 24.14, 30.14]

                arithmetic_result = eval('self._calculate_'+fname+'_value(args)')
                my_result = "%.9f" % float(arithmetic_result)

                lambda1 = lambda x, y=my_result: self.assertEqual("%.9f" % float(str(x['q_res'][0]['results'][0]["$1"])), y)
                test_dict["%d-default" % (count)] = {"indexes": self.indexes,
                                                     "pre_queries": [],
                                                     "queries": [query],
                                                     "post_queries": [],
                                                     "asserts": [lambda1],
                                                     "cleanups": []}
                count += 1

        try:
            self.query_runner(test_dict)
        finally:
            self._unload_test_data()

    '''test for index pushdown. Available only for mean() function'''
    def test_index_pushdown(self):
        self._load_test_data()
        query = "explain select mean(int_field) from temp_bucket where int_field is not missing"
        n1ql_result = self.run_cbq_query(query)
        result = 'index_group_aggs'

        try:
            self.assertEqual(str(n1ql_result).find(result) > -1, True)
        finally:
            self._unload_test_data()

    '''Negative test for incorrect functions calls, like median(mean(...))'''
    def test_nested_calls_negative(self):
        self._load_test_data()

        for i in range(len(self.func_names)):
            for j in range(len(self.func_names)):
                fname1 = self.func_names[i]
                fname2 = self.func_names[j]
                query = "select "+fname1+"("+fname2+"(int_field)) from temp_bucket"
                error_detected = False
                try:
                    n1ql_result = self.run_cbq_query(query)
                except CBQError as e:
                    error_detected = True

                self.assertEqual(error_detected, True)

        self._unload_test_data()

    '''Negative test for empty function parameters list'''
    def test_empty_args_list_calls_negative(self):
        self._load_test_data()

        for i in range(len(self.func_names)):
            fname = self.func_names[i]
            query = "select "+fname+"() from temp_bucket"
            error_detected = False
            try:
                n1ql_result = self.run_cbq_query(query)
            except CBQError as e:
                error_detected = True

            self.assertEqual(error_detected, True)

        self._unload_test_data()


    '''########################################################################'''
    '''    Statistic finctions implementation                                  '''
    '''########################################################################'''


    def _calculate_median_value(self, params):
        filtered_params = self._filter_digit_params(params)
        sortedParams = sorted(filtered_params)
        retVal = 0

        if len(sortedParams) == 0:
            return retVal

        if len(sortedParams) == 1:
            return sortedParams[0]

        if len(sortedParams) == 2:
            return (sortedParams[0] + sortedParams[1])//2

        if len(sortedParams)%2 != 0:
            retVal = sortedParams[len(sortedParams)//2]
        else:
            valLeft = sortedParams[len(sortedParams)//2 -1]
            valRight = sortedParams[len(sortedParams)//2]
            retVal = (valLeft+valRight)//2

        return retVal

    def _calculate_stddev_value(self, params):
        filtered_params = self._filter_digit_params(params)
        avg = self._calculate_avg(filtered_params)
        sum = 0
        for i in range(len(filtered_params)):
            sum+=math.pow(filtered_params[i] - avg, 2)

        return math.sqrt(sum//(len(filtered_params)-1))

    def _calculate_stddev_pop_value(self, params):
        filtered_params = self._filter_digit_params(params)
        avg = self._calculate_avg(filtered_params)
        sum = 0
        for i in range(len(filtered_params)):
            sum+=math.pow(filtered_params[i] - avg, 2)

        return math.sqrt(sum//len(filtered_params))

    def _calculate_stddev_samp_value(self, params):
        filtered_params = self._filter_digit_params(params)
        if len(filtered_params) <= 1:
            return None

        return self._calculate_stddev_value(filtered_params)

    def _calculate_mean_value(self, params):
        return self._calculate_avg(params)

    def _calculate_avg(self, params):
        sum = 0
        for i in range(len(params)):
            sum += params[i]
        return sum//len(params)


    def _calculate_variance_value(self, params):
        filtered_params = self._filter_digit_params(params)
        avg = self._calculate_avg(filtered_params)
        sum = 0;
        for i in range(len(filtered_params)):
            sum+=math.pow(filtered_params[i] - avg, 2)

        return sum//(len(filtered_params)-1)

    def _calculate_variance_pop_value(self, params):
        filtered_params = self._filter_digit_params(params)
        avg = self._calculate_avg(filtered_params)
        sum = 0;
        for i in range(len(filtered_params)):
            sum += math.pow(filtered_params[i] - avg, 2)

        return sum//len(params)


    def _calculate_variance_samp_value(self, params):
        filtered_params = self._filter_digit_params(params)
        avg = self._calculate_avg(filtered_params)
        sum = 0;
        for i in range(len(filtered_params)):
            sum+=math.pow(filtered_params[i] - avg, 2)

        return sum//(len(filtered_params)-1)

    def _filter_digit_params(self, params):
        ret_val = list([x for x in params if isinstance(x, int) or isinstance(x, int) or isinstance(x, float)])
        for i in range(len(ret_val)):
            ret_val[i] = float(ret_val[i])

        return ret_val


    '''########################################################################'''
    '''    Test data load finctions implementation                                  '''
    '''########################################################################'''


    def _load_test_data(self):
        temp_bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket("temp_bucket", 11222, temp_bucket_params)

        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket=bucket, timeout=self.wait_timeout * 5)

        for i1 in range(len(self.numbers)):
            query = "insert into temp_bucket values ('key_" + str(i1) + "', {"
            int_field_insert = "'int_field': " + str(self.numbers[i1]) + ","
            float_field_insert = "'float_field': " + str((self.numbers[i1]*math.pi))

            query += " " + int_field_insert + float_field_insert + "})"
            self.run_cbq_query(query)

        self.run_cbq_query('CREATE PRIMARY INDEX `#primary` ON `temp_bucket`')
        self.run_cbq_query('CREATE INDEX ix1 ON temp_bucket(int_field);')
        self.run_cbq_query('CREATE INDEX ix3 ON temp_bucket(float_field);')

    def _load_test_data_including_nulls(self):
        self._load_test_data()
        for i in [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]:
            int_val = "null"
            float_val = "null"
            if i % 2 == 0:
                int_val = "missing"
            if i % 3 == 0:
                float_val = "missing"

            query = "insert into temp_bucket values ('key_" + str(i) + "', {"
            if int_val == "null":
                int_field_insert = "'int_field': null"
            else:
                int_field_insert = ""
            if float_val == "null":
                float_field_insert = "'float_field': null"
            else:
                float_field_insert = ""

            comma = ','
            if len(int_field_insert) == 0 or len (float_field_insert) == 0:
                comma = ''

            query += " " + int_field_insert + comma + float_field_insert + "})"
            self.run_cbq_query(query)

    def _load_test_data_nulls_missings(self):
        temp_bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket("temp_bucket", 11222, temp_bucket_params)

        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket=bucket, timeout=self.wait_timeout * 5)

        for i in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
            int_val = "null"
            float_val = "null"
            if i%2 == 0:
                int_val = "missing"
            if i%3 == 0:
                float_val = "missing"

            query = "insert into temp_bucket values ('key_" + str(i) + "', {"
            if int_val == "null":
                int_field_insert = "'int_field': null"
            else:
                int_field_insert = ""
            if float_val == "null":
                float_field_insert = "'float_field': null"
            else:
                float_field_insert = ""

            comma = ','
            if len(int_field_insert) == 0 or len(float_field_insert) == 0:
                comma = ''
            query += " " + int_field_insert + comma + float_field_insert + "})"
            self.run_cbq_query(query)

        self.run_cbq_query('CREATE PRIMARY INDEX `#primary` ON `temp_bucket`')
        self.run_cbq_query('CREATE INDEX ix1 ON temp_bucket(int_field);')
        self.run_cbq_query('CREATE INDEX ix3 ON temp_bucket(float_field);')

    def _load_mixed_datatypes(self):
        temp_bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket("temp_bucket", 11222, temp_bucket_params)

        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket=bucket, timeout=self.wait_timeout * 5)

        query = "insert into temp_bucket values ('key_1', {'int_field': 1, 'float_field': 3.14})"
        self.run_cbq_query(query)
        query = "insert into temp_bucket values ('key_2', {'int_field': 'a', 'float_field': 'a'})"
        self.run_cbq_query(query)
        query = "insert into temp_bucket values ('key_3', {'int_field': 3, 'float_field': 9.14})"
        self.run_cbq_query(query)
        query = "insert into temp_bucket values ('key_4', {'int_field': null, 'float_field': null})"
        self.run_cbq_query(query)
        query = "insert into temp_bucket values ('key_5', {'int_field': 'b', 'float_field': 'b'})"
        self.run_cbq_query(query)
        query = "insert into temp_bucket values ('key_6', {'int_field': 6, 'float_field': 18.14})"
        self.run_cbq_query(query)
        query = "insert into temp_bucket values ('key_7', {'float_field': 21.14})"
        self.run_cbq_query(query)
        query = "insert into temp_bucket values ('key_8', {'int_field': 8, 'float_field': 24.14})"
        self.run_cbq_query(query)
        query = "insert into temp_bucket values ('key_9', {'int_field': 9})"
        self.run_cbq_query(query)
        query = "insert into temp_bucket values ('key_10',{'int_field': 10, 'float_field': 30.14})"
        self.run_cbq_query(query)

        self.run_cbq_query('CREATE PRIMARY INDEX `#primary` ON `temp_bucket`')
        self.run_cbq_query('CREATE INDEX ix1 ON temp_bucket(int_field);')
        self.run_cbq_query('CREATE INDEX ix3 ON temp_bucket(float_field);')

    def _unload_test_data(self):
        self.cluster.bucket_delete(self.master, "temp_bucket")


