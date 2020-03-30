from .tuq import QueryTests
from membase.api.exception import CBQError

class ScalarFunctionsTests(QueryTests):

    def suite_setUp(self):
        super(ScalarFunctionsTests, self).suite_setUp()
        self.load_data_for_scalar_tests()

    def test_nvl2_leading_non_null(self):
        test_dict = dict()
        count = 0

        param2 = [self.scalars_test_data['null'], self.scalars_test_data['missing'], self.scalars_test_data['not_null_1']]
        param3 = [self.scalars_test_data['null'], self.scalars_test_data['missing'], self.scalars_test_data['not_null_2']]

        for i in range(len(param2)):
            for j in range(len(param3)):
                # expected_result is 'y' parameter in lambda - cannot be used directly since lambdas lazy calculation
                expected_result = self.null_to_none(param2[i]['value'])

                query = ("select nvl2(" + self.scalars_test_data['not_null_1']['key'] +
                         ",%s,%s) from default USE KEYS ['plain'] where key1='" +
                         self.scalars_test_data['not_null_1']['value'] + "'") % (param2[i]['key'], param3[j]['key'])

                lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                lambda2 = lambda x, y = self.null_to_none(param2[i]['value']): self.assertEqual(self.normalize_result(x['q_res'][0]), y)

                test_dict["%d-default" % (count)] = {"indexes": [],
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1, lambda2],
                                               "cleanups": []
                                                     }
                count += 1
        self.query_runner(test_dict)

    def test_nvl_all_nulls(self):
        query = "select nvl(" + self.scalars_test_data['null']['key'] + ", null) from default USE KEYS ['plain'] where key1='" + \
                self.scalars_test_data['not_null_1']['value'] + "'"

        expected_result = None
        result = self.run_cbq_query(query)

        self.assertTrue(result['status'] == 'success')
        self.assertTrue(self.normalize_result(result) == expected_result)


    def test_nvl_all_non_nulls(self):
        query = "select nvl(" + self.scalars_test_data['not_null_1']['key'] + ", " + self.scalars_test_data['not_null_2']['key'] + \
                ") from default USE KEYS ['plain'] where key1='" + self.scalars_test_data['not_null_1']['value'] + "'"

        expected_result = self.scalars_test_data['not_null_1']['value']
        result = self.run_cbq_query(query)

        self.assertTrue(result['status'] == 'success')
        self.assertTrue(self.normalize_result(result) == expected_result)

    def test_nvl_leading_null(self):
        query = "select nvl(" + self.scalars_test_data['null']['key'] + ", " + self.scalars_test_data['not_null_1']['key'] + \
                ") from default USE KEYS ['plain'] where key1='" + self.scalars_test_data['not_null_1']['value'] + "'"

        expected_result = self.scalars_test_data['not_null_1']['value']
        result = self.run_cbq_query(query)

        self.assertTrue(result['status'] == 'success')
        self.assertTrue(self.normalize_result(result) == expected_result)

    def test_nvl_trailing_null(self):
        query = "select nvl(" + self.scalars_test_data['not_null_1']['key'] + ", " + self.scalars_test_data['null']['key'] + \
                ") from default USE KEYS ['plain'] where key1='" + self.scalars_test_data['not_null_1']['value'] + "'"

        expected_result = self.scalars_test_data['not_null_1']['value']
        result = self.run_cbq_query(query)

        self.assertTrue(result['status'] == 'success')
        self.assertTrue(self.normalize_result(result) == expected_result)

    def test_coalesce_different_nulls(self):
        test_dict = dict()
        count = 0

        params = ['NULL', 'null', 'NuLl']

        for param in params:
            # expected_result can be used directly since it is constant
            expected_result = self.scalars_test_data['not_null_1']['value']

            query = ("select coalesce(%s, " + self.scalars_test_data['not_null_1']['key'] +
                     ") from default USE KEYS ['plain'] where key1='" + self.scalars_test_data['not_null_1']['value'] +
                     "'") % (param)

            test_dict["%d-default" % (count)] = {"indexes": [],
                                                 "pre_queries": [],
                                                 "queries": [query],
                                                 "post_queries": [],
                                                 "asserts": [
                                                     lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success'),
                                                     lambda x: self.assertTrue(self.normalize_result(x['q_res'][0]) == expected_result)],
                                                 "cleanups": []}
            count += 1
        self.query_runner(test_dict)

    coalesce_function_names = ['coalesce', 'COALESCE', 'CoaLesCe']
    nvl2_function_names = ['nvl2', 'NVL2', 'NvL2']
    nvl_function_names = ['nvl', 'NVL', 'NvL']

    def test_scalars(self):
        self.fail_if_no_buckets()
        #self.load_data_for_scalar_tests()
        self.test_coalesce_common()
        self.test_nvl2_common()
        self.test_nvl_common()

    def test_coalesce_common(self):
        self.fail_if_no_buckets()

        self.test_coalesce_function_name()
        self.test_coalesce_correct_params()
        self.test_coalesce_different_nulls()
        self.test_coalesce_all_nulls()
        self.test_coalesce_null_string()
        self.test_coalesce_empty_string()
        self.test_coalesce_sequential_nulls()
        self.test_coalesce_multiple_nulls()
        self.test_coalesce_negative_zero_params()
        self.test_coalesce_negative_one_param()

    def test_nvl2_common(self):
        self.fail_if_no_buckets()

        self.test_nvl2_function_name()
        self.test_nvl2_leading_null()
        self.test_nvl2_leading_non_null()
        self.test_nvl2_different_nulls()
        self.test_nvl2_null_string()
        self.test_nvl2_empty_string()
        self.test_nvl2_negative_four_params()
        self.test_nvl2_negative_one_param()
        self.test_nvl2_negative_two_params()
        self.test_nvl2_negative_zero_params()

    def test_nvl_common(self):
        self.fail_if_no_buckets()

        self.test_nvl_function_name()
        self.test_nvl_all_nulls()
        self.test_nvl_all_non_nulls()
        self.test_nvl_leading_null()
        self.test_nvl_trailing_null()
        self.test_nvl_different_nulls()
        self.test_nvl_null_string()
        self.test_nvl_empty_string()
        self.test_nvl_negative_one_param()
        self.test_nvl_negative_three_params()
        self.test_nvl_negative_zero_params()


    def test_coalesce_correct_params(self):
        params = [self.scalars_test_data['not_null_1'], self.scalars_test_data['null'], self.scalars_test_data['missing']]
        test_dict = dict()
        count = 0
        ##### 2 params cases
        for i in range(len(params)):
            for j in range(len(params)):
                if params[i]['value'] != 'val1' and params[j]['value'] != 'val1':
                    continue

                query = ("select coalesce(%s,%s) from default USE KEYS ['plain'] where key1='" +
                         self.scalars_test_data['not_null_1']['value'] + "'") % (params[i]['key'], params[j]['key'])

                # expected_result is 'y' parameter in lambda - cannot be used directly since lambdas lazy calculation
                expected_result = self.null_to_none(params[i]['value'])
                if params[i]['value'] != 'val1':
                    expected_result = self.null_to_none(params[j]['value'])

                lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                lambda2 = lambda x, y = self.null_to_none(params[i]['value']): self.assertEqual(self.normalize_result(x['q_res'][0]), y)
                if params[i]['value'] != 'val1':
                    lambda2 = lambda x, y=self.null_to_none(params[j]['value']): self.assertEqual(self.normalize_result(x['q_res'][0]), y)

                test_dict["%d-default" % (count)] = {"indexes": [],
                                               "pre_queries": [],
                                               "queries": [query],
                                               "post_queries": [],
                                               "asserts": [lambda1, lambda2],
                                               "cleanups": []}
                count += 1

        ##### 3 params cases
        for i in range(len(params)):
            for j in range(len(params)):
                for k in range(len(params)):
                    if params[i]['key'] != 'key1' and params[j]['key'] != 'key1' and params[k]['key'] != 'key1':
                        continue
                    query = ("select coalesce(%s,%s,%s) from default USE KEYS ['plain'] where key1='" +
                             self.scalars_test_data['not_null_1']['value'] + "'") % (params[i]['key'],
                                                                                     params[j]['key'],
                                                                                     params[k]['key'])

                    # expected_result is 'y' parameter in lambda - cannot be used directly since lambdas lazy calculation
                    expected_result = self.null_to_none(params[i]['value'])
                    if params[i]['value'] != 'val1':
                        expected_result = self.null_to_none(params[j]['value'])
                    if params[i]['key'] != 'key1' and params[j]['key'] != 'key1':
                        expected_result = self.null_to_none(params[k]['value'])

                    lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                    lambda2 = lambda x, y=self.null_to_none(params[i]['value']): self.assertEqual(self.normalize_result(x['q_res'][0]), y)
                    if params[i]['value'] != 'val1':
                        lambda2 = lambda x, y=self.null_to_none(params[j]['value']): self.assertEqual(
                            self.normalize_result(x['q_res'][0]), y)
                    if params[i]['key'] != 'key1' and params[j]['key'] != 'key1':
                        lambda2 = lambda x, y=self.null_to_none(params[k]['value']): self.assertEqual(
                            self.normalize_result(x['q_res'][0]), y)


                    test_dict["%d-default" % (count)] = {"indexes": [],
                                                         "pre_queries": [],
                                                         "queries": [query],
                                                         "post_queries": [],
                                                         "asserts": [lambda1, lambda2],
                                                         "cleanups": []}
                    count+=1
        self.query_runner(test_dict)

    def test_nvl_function_name(self):
        test_dict = dict()
        count = 0
        for func_name in self.nvl_function_names:
            query = ("select %s(key1, key2) from default USE KEYS ['plain'] where key1='" +
                     self.scalars_test_data['not_null_1']['value'] + "'") % (func_name)

            test_dict["%d-default" % (count)] = {"indexes": [],
                                                 "pre_queries": [],
                                                 "queries": [query],
                                                 "post_queries": [],
                                                 "asserts": [
                                                     lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')],
                                                 "cleanups": []}
            count += 1
        self.query_runner(test_dict)

    def test_nvl2_function_name(self):
        test_dict = dict()
        count = 0
        for func_name in self.nvl2_function_names:
            query = ("select %s(key1,key1,key1) from default USE KEYS ['plain'] where key1='" +
                     self.scalars_test_data['not_null_1']['value'] + "'") % (func_name)

            test_dict["%d-default" % (count)] = {"indexes": [],
                                                 "pre_queries": [],
                                                 "queries": [query],
                                                 "post_queries": [],
                                                 "asserts": [
                                                     lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')],
                                                 "cleanups": []}
            count += 1
        self.query_runner(test_dict)

    def test_coalesce_function_name(self):
        test_dict = dict()
        count = 0
        for func_name in self.coalesce_function_names:
            query = ("select %s(key1, key2) from default USE KEYS ['plain'] where key1='" +
                     self.scalars_test_data['not_null_1']['value'] + "'") % (func_name)

            test_dict["%d-default" % (count)] = {"indexes": [],
                                                 "pre_queries": [],
                                                 "queries": [query],
                                                 "post_queries": [],
                                                 "asserts": [
                                                     lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')],
                                                 "cleanups": []}
            count += 1
        self.query_runner(test_dict)

    def test_nvl2_leading_null(self):
        test_dict = dict()
        count = 0

        param1 = [self.scalars_test_data['null'], self.scalars_test_data['missing']]
        param2 = [self.scalars_test_data['null'], self.scalars_test_data['missing'], self.scalars_test_data['not_null_1']]
        param3 = [self.scalars_test_data['null'], self.scalars_test_data['missing'], self.scalars_test_data['not_null_2']]

        for i in range(len(param1)):
            for j in range(len(param2)):
                for k in range(len(param3)):
                    # expected_result is 'y' parameter in lambda - cannot be used directly since lambdas lazy calculation
                    expected_result = self.null_to_none(param3[k]['value'])

                    query = ("select nvl2(%s,%s,%s) from default USE KEYS ['plain'] where key1='" +
                            self.scalars_test_data['not_null_1']['value'] + "'") % \
                            (param1[i]['key'], param2[j]['key'], param3[k]['key'])

                    lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
                    lambda2 = lambda x, y=self.null_to_none(param3[k]['value']): self.assertEqual(self.normalize_result(x['q_res'][0]), y)

                    test_dict["%d-default" % (count)] = {"indexes": [],
                                                         "pre_queries": [],
                                                         "queries": [query],
                                                         "post_queries": [],
                                                         "asserts": [lambda1, lambda2],
                                                         "cleanups": []}

                    count += 1
        self.query_runner(test_dict)

    def test_nvl2_different_nulls(self):
        test_dict = dict()
        count = 0

        params = ['NULL', 'null', 'NuLl']

        for param in params:
            expected_result = self.scalars_test_data['not_null_2']['value']

            query = ("select nvl2(%s, " + self.scalars_test_data['not_null_1']['key'] + ", " +
                     self.scalars_test_data['not_null_2']['key'] + ") from default USE KEYS ['plain'] where key1='" +
                     self.scalars_test_data['not_null_1']['value'] + "'") % (param)

            test_dict["%d-default" % (count)] = {"indexes": [],
                                                 "pre_queries": [],
                                                 "queries": [query],
                                                 "post_queries": [],
                                                 "asserts": [
                                                     lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success'),
                                                     lambda x: self.assertTrue(self.normalize_result(x['q_res'][0]) == expected_result)],
                                                 "cleanups": []}
            count += 1
        self.query_runner(test_dict)

    def test_nvl_different_nulls(self):
        test_dict = dict()
        count = 0

        params = ['NULL', 'null', 'NuLl']

        for param in params:
            expected_result = self.scalars_test_data['not_null_1']['value']

            query = ("select nvl(%s, " + self.scalars_test_data['not_null_1']['key'] +
                     ") from default USE KEYS ['plain'] where key1='" + self.scalars_test_data['not_null_1']['value'] +
                     "'") % (param)

            test_dict["%d-default" % (count)] = {"indexes": [],
                                                 "pre_queries": [],
                                                 "queries": [query],
                                                 "post_queries": [],
                                                 "asserts": [
                                                     lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success'),
                                                     lambda x: self.assertTrue(self.normalize_result(x['q_res'][0]) == expected_result)],
                                                 "cleanups": []}
            count += 1
        self.query_runner(test_dict)

    def test_coalesce_all_nulls(self):
        query = "select coalesce(" + self.scalars_test_data['null']['key'] + ", " + self.scalars_test_data['null']['key'] + ", " + \
                self.scalars_test_data['null']['key'] + ") from default USE KEYS ['plain'] where key1='" + \
                self.scalars_test_data['not_null_1']['value'] + "'"

        expected_result = None
        result = self.run_cbq_query(query)

        self.assertTrue(result['status'] == 'success')
        self.assertTrue(self.normalize_result(result) == expected_result)

    def test_coalesce_null_string(self):
        test_dict = dict()
        count = 0

        params = [self.scalars_test_data['string_NULL'], self.scalars_test_data['string_NuLl'], self.scalars_test_data['string_null']]

        for param in params:
            # expected_result is 'y' parameter in lambda - cannot be used directly since lambdas lazy calculation
            expected_result = param['value']

            query = ("select coalesce(%s, " + self.scalars_test_data['not_null_1']['key'] +
                     ") from default USE KEYS ['plain'] where key1='" + self.scalars_test_data['not_null_1']['value'] +
                     "'") % (param['key'])

            lambda1 = lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success')
            lambda2 = lambda x, y=param['value']: self.assertTrue(self.normalize_result(x['q_res'][0]) == y)

            test_dict["%d-default" % (count)] = {"indexes": [],
                                                 "pre_queries": [],
                                                 "queries": [query],
                                                 "post_queries": [],
                                                 "asserts": [lambda1, lambda2],
                                                 "cleanups": []}
            count += 1
        self.query_runner(test_dict)

    def test_nvl2_null_string(self):
        test_dict = dict()
        count = 0

        params = [self.scalars_test_data['string_NULL'], self.scalars_test_data['string_NuLl'], self.scalars_test_data['string_null']]

        for param in params:
            expected_result = self.scalars_test_data['not_null_1']['value']

            query = ("select nvl2(%s, " + self.scalars_test_data['not_null_1']['key'] + ", " +
                     self.scalars_test_data['not_null_2']['key'] + ") from default USE KEYS ['plain'] where key1='" +
                     self.scalars_test_data['not_null_1']['value'] + "'") % (param['key'])

            test_dict["%d-default" % (count)] = {"indexes": [],
                                                 "pre_queries": [],
                                                 "queries": [query],
                                                 "post_queries": [],
                                                 "asserts": [
                                                     lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success'),
                                                     lambda x: self.assertTrue(self.normalize_result(x['q_res'][0]) == expected_result)],
                                                 "cleanups": []}
            count += 1
        self.query_runner(test_dict)


    def test_nvl_null_string(self):
        test_dict = dict()
        count = 0

        params = [self.scalars_test_data['string_NULL'], self.scalars_test_data['string_NuLl'], self.scalars_test_data['string_null']]

        for param in params:
            # expected_result is 'y' parameter in lambda - cannot be used directly since lambdas lazy calculation
            expected_result = param['value']

            query = ("select nvl(%s, " + self.scalars_test_data['not_null_1']['key'] +
                     ") from default USE KEYS ['plain'] where key1='" + self.scalars_test_data['not_null_1']['value'] +
                     "'") % (param['key'])

            lambda1 = lambda x:self.assertTrue(x['q_res'][0]['status'] == 'success')
            lambda2 = lambda x, y=param['value']: self.assertTrue(self.normalize_result(x['q_res'][0]) == y)

            test_dict["%d-default" % (count)] = {"indexes": [],
                                                 "pre_queries": [],
                                                 "queries": [query],
                                                 "post_queries": [],
                                                 "asserts": [lambda1, lambda2],
                                                 "cleanups": []}
            count += 1
        self.query_runner(test_dict)

    def test_coalesce_empty_string(self):
        query = "select coalesce(" + self.scalars_test_data['null']['key'] + ", " + self.scalars_test_data['empty_string']['key'] + ", " + \
                self.scalars_test_data['not_null_1']['key'] + ") from default USE KEYS ['plain'] where key1='" + \
                self.scalars_test_data['not_null_1']['value'] + "'"

        expected_result = self.scalars_test_data['empty_string']['value']
        result = self.run_cbq_query(query)

        self.assertTrue(result['status'] == 'success')
        self.assertTrue(self.normalize_result(result) == expected_result)

    def test_nvl2_empty_string(self):
        query = "select nvl2(" + self.scalars_test_data['empty_string']['key'] + ", " + self.scalars_test_data['not_null_1']['key'] + \
                ", " + self.scalars_test_data['not_null_2']['key'] + ") from default USE KEYS ['plain'] where key1='" + \
                self.scalars_test_data['not_null_1']['value'] + "'"

        expected_result = self.scalars_test_data['not_null_1']['value']
        result = self.run_cbq_query(query)

        self.assertTrue(result['status'] == 'success')
        self.assertTrue(self.normalize_result(result) == expected_result)

    def test_nvl_empty_string(self):
        query = "select nvl(" + self.scalars_test_data['empty_string']['key'] + ", " + self.scalars_test_data['not_null_1']['key'] + \
                ") from default USE KEYS ['plain'] where key1='" + self.scalars_test_data['not_null_1']['value'] + "'"

        expected_result = self.scalars_test_data['empty_string']['value']
        result = self.run_cbq_query(query)

        self.assertTrue(result['status'] == 'success')
        self.assertTrue(self.normalize_result(result) == expected_result)

    def test_coalesce_sequential_nulls(self):
        query = "select coalesce(" + self.scalars_test_data['null']['key'] + ", " + self.scalars_test_data['null']['key'] + ", " + \
                self.scalars_test_data['not_null_1']['key'] + ") from default USE KEYS ['plain'] where key1='" + \
                self.scalars_test_data['not_null_1']['value'] + "'"

        expected_result = self.scalars_test_data['not_null_1']['value']
        result = self.run_cbq_query(query)

        self.assertTrue(result['status'] == 'success')
        self.assertTrue(self.normalize_result(result) == expected_result)

    def test_coalesce_multiple_nulls(self):
        ### leading null
        test_dict = dict()
        expected_result = self.scalars_test_data['not_null_1']['value']

        query = "select coalesce(" + self.scalars_test_data['null']['key'] + ", " + self.scalars_test_data['not_null_1']['key'] + ", " + \
                self.scalars_test_data['null']['key'] + ", " + self.scalars_test_data['not_null_2']['key'] + \
                ") from default USE KEYS ['plain'] where key1='" + self.scalars_test_data['not_null_1']['value'] + "'"

        test_dict["%d-default" % (0)] = {"indexes": [],
                                             "pre_queries": [],
                                             "queries": [query],
                                             "post_queries": [],
                                             "asserts": [
                                                 lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success'),
                                                 lambda x: self.assertTrue(self.normalize_result(x['q_res'][0]) == expected_result)],
                                             "cleanups": []}

        ### trailing null
        expected_result = self.scalars_test_data['not_null_1']['value']

        query = "select coalesce(" + self.scalars_test_data['not_null_1']['key'] + ", " + self.scalars_test_data['null']['key'] + ", " + \
                self.scalars_test_data['not_null_2']['key'] + ") from default USE KEYS ['plain'] where key1='" + \
                self.scalars_test_data['not_null_1']['value'] + "'"


        test_dict["%d-default" % (1)] = {"indexes": [],
                                             "pre_queries": [],
                                             "queries": [query],
                                             "post_queries": [],
                                             "asserts": [lambda x: self.assertTrue(x['q_res'][0]['status'] == 'success'),
                                                         lambda x: self.assertTrue(self.normalize_result(x['q_res'][0]) == expected_result)],
                                             "cleanups": []}
        self.query_runner(test_dict)

    def test_coalesce_negative_zero_params(self):
        is_err_detected = False
        is_err_message_correct = False

        try:
            self.run_cbq_query('select coalesce() from system:dual')
        except CBQError as e:
            is_err_detected = True
            is_err_message_correct = str(e).lower().find('Number of arguments to function coalesce must be between 2 and 32767. - at'.lower()) > -1

        self.assertTrue(is_err_detected)
        self.assertTrue(is_err_message_correct)

    def test_coalesce_negative_one_param(self):
        is_err_detected = False
        is_err_message_correct = False

        try:
            self.run_cbq_query("select coalesce('param1') from system:dual")
        except CBQError as e:
            is_err_detected = True
            is_err_message_correct = str(e).lower().find('Number of arguments to function coalesce must be between 2 and 32767. - at'.lower()) > -1

        self.assertTrue(is_err_detected)
        self.assertTrue(is_err_message_correct)


    def test_nvl2_negative_zero_params(self):
        is_err_detected = False
        is_err_message_correct = False

        try:
            self.run_cbq_query("select nvl2() from system:dual")
        except CBQError as e:
            is_err_detected = True;
            is_err_message_correct = str(e).lower().find('Number of arguments to function nvl2 must be 3. - at'.lower()) > -1

        self.assertTrue(is_err_detected)
        self.assertTrue(is_err_message_correct)


    def test_nvl_negative_zero_params(self):
        is_err_detected = False
        is_err_message_correct = False

        try:
            self.run_cbq_query("select nvl() from system:dual")
        except CBQError as e:
            is_err_detected = True
            is_err_message_correct = str(e).lower().find('Number of arguments to function nvl must be 2. - at'.lower()) > -1

        self.assertTrue(is_err_detected)
        self.assertTrue(is_err_message_correct)


    def test_nvl2_negative_one_param(self):
        is_err_detected = False
        is_err_message_correct = False

        try:
            self.run_cbq_query("select nvl2('param1') from system:dual")
        except CBQError as e:
            is_err_detected = True
            is_err_message_correct = str(e).lower().find('Number of arguments to function nvl2 must be 3. - at'.lower()) > -1

        self.assertTrue(is_err_detected)
        self.assertTrue(is_err_message_correct)

    def test_nvl_negative_one_param(self):
        is_err_detected = False
        is_err_message_correct = False

        try:
            self.run_cbq_query("select nvl('param1') from system:dual")
        except CBQError as e:
            is_err_detected = True
            is_err_message_correct = str(e).lower().find('Number of arguments to function nvl must be 2. - at'.lower()) > -1

        self.assertTrue(is_err_detected)
        self.assertTrue(is_err_message_correct)

    def test_nvl2_negative_two_params(self):
        is_err_detected = False
        is_err_message_correct = False

        try:
            self.run_cbq_query("select nvl2('param1', 'param2') from system:dual")
        except CBQError as e:
            is_err_detected = True
            is_err_message_correct = str(e).lower().find('Number of arguments to function nvl2 must be 3. - at'.lower()) > -1

        self.assertTrue(is_err_detected)
        self.assertTrue(is_err_message_correct)

    def test_nvl_negative_three_params(self):
        is_err_detected = False
        is_err_message_correct = False

        try:
            self.run_cbq_query("select nvl('param1','param2','param3') from system:dual")
        except CBQError as e:
            is_err_detected = True
            is_err_message_correct = str(e).lower().find('Number of arguments to function nvl must be 2. - at'.lower()) > -1

        self.assertTrue(is_err_detected)
        self.assertTrue(is_err_message_correct)

    def test_nvl2_negative_four_params(self):
        is_err_detected = False
        is_err_message_correct = False

        try:
            self.run_cbq_query("select nvl2('param1','param2','param3','param4') from system:dual")
        except CBQError as e:
            is_err_detected = True
            is_err_message_correct = str(e).lower().find('Number of arguments to function nvl2 must be 3. - at'.lower()) > -1

        self.assertTrue(is_err_detected)
        self.assertTrue(is_err_message_correct)


    scalars_test_data = {'not_null_1': {'key': 'key1', 'value': 'val1'},
                'not_null_2': {'key':'key2', 'value':'val2'},
                'null': {'key':'key3', 'value':'null'},
                'missing': {'key':'key4', 'value':'missing'},
                'empty_string': {'key':'key5', 'value':''},
                'string_null':{'key':'key6', 'value':'null'},
                'string_NULL':{'key':'key7', 'value':'NULL'},
                'string_NuLl':{'key':'key8', 'value':'NuLl'}
                        }

    def load_data_for_scalar_tests(self):
        #query = "insert into default values ('plain',{'key1':'val1','key2':'val2','key3':null,...})"
        counter = 0
        query = "insert into default values('plain',{"
        for key, value in self.scalars_test_data.items():
            if key == 'missing':
                continue
            query+="'"+value['key']+"':"
            if key != 'null':
                query+="'"
            query+=value['value']
            if key != 'null':
                query+="'"
            if counter<len(self.scalars_test_data)-2:
                query+=","
            counter+=1

        query+="})"
        self.run_cbq_query(query)


