from .tuq import QueryTests
from membase.api.exception import CBQError
from remote.remote_util import RemoteMachineShellConnection
import pdb
import json

class InListOperatorTests(QueryTests):

    contains_list_str = " [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]"
    not_contains_list_str = " [100,200,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,2000]"
    contains_select_str = "  (select raw int_field from temp_bucket a where int_field < 21)"
    not_contains_select_str = " (select raw int_field from temp_bucket a where int_field > 100)"

    contains_or_condition = " (int_field=1 or int_field=2 or int_field=3 or int_field=4 or int_field=5 or "\
                            " int_field=6 or int_field=7 or int_field=8 or int_field=9 or int_field=10 or "\
                            " int_field=11 or int_field=12 or int_field=13 or int_field=14 or int_field=15 or "\
                            " int_field=16 or int_field=17 or int_field=18 or int_field=19 or int_field=20) "
    not_contains_or_condition = " (int_field=100 or int_field=200 or int_field=300 or int_field=400 or int_field=500 or "\
                            " int_field=600 or int_field=700 or int_field=800 or int_field=900 or int_field=1000 or "\
                            " int_field=1100 or int_field=1200 or int_field=1300 or int_field=1400 or int_field=1500 or "\
                            " int_field=1600 or int_field=1700 or int_field=1800 or int_field=1900 or int_field=2000) "
    contains_and_condition = " (int_field!=1 and int_field!=2 and int_field!=3 and int_field!=4 and int_field!=5 and "\
                            " int_field!=6 and int_field!=7 and int_field!=8 and int_field!=9 and int_field!=10 and "\
                            " int_field!=11 and int_field!=12 and int_field!=13 and int_field!=14 and int_field!=15 and "\
                            " int_field!=16 and int_field!=17 and int_field!=18 and int_field!=19 and int_field!=20) "
    not_contains_and_condition = " (int_field!=100 and int_field!=200 and int_field!=300 and int_field!=400 and int_field!=500 and "\
                            " int_field!=600 and int_field!=700 and int_field!=800 and int_field!=900 and int_field!=1000 and "\
                            " int_field!=1100 and int_field!=1200 and int_field!=1300 and int_field!=1400 and int_field!=1500 and "\
                            " int_field!=1600 and int_field!=1700 and int_field!=1800 and int_field!=1900 and int_field!=2000) "
    in_str = " in "
    not_in_str = " not in "
    in_not_in = [in_str, not_in_str]
    constant_list = [contains_list_str, not_contains_list_str]
    subquery = [contains_select_str, not_contains_select_str]

    def suite_setUp(self):
        super(InListOperatorTests, self).suite_setUp()
        self.log.info("==============  InListOperatorTests suite_setup has started ==============")
        self.log.info("==============  InListOperatorTests suite_setup has completed ==============")
        self.log_config_info()

    def setUp(self):
        super(InListOperatorTests, self).setUp()
        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket=bucket,
                                      timeout=self.wait_timeout * 5)
        self.log.info("==============  InListOperatorTests setup has started ==============")
        self.log.info("==============  InListOperatorTests setup has completed ==============")
        self.log_config_info()


    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  InListOperatorTests tearDown has started ==============")
        self.log.info("==============  InListOperatorTests tearDown has completed ==============")
        super(InListOperatorTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  InListOperatorTests suite_tearDown has started ==============")
        self.log.info("==============  InListOperatorTests suite_tearDown has completed ==============")
        super(InListOperatorTests, self).suite_tearDown()


    def run_all(self):
        self.test_in_single_constant()
        self.test_in_single_subquery()
        self.test_not_in_single_constant()
        self.test_not_in_single_subquery()
        self.test_in_1001_constant()
        self.test_not_in_1001_constant()
        self.test_simple_query()
        self.test_or_connection()
        self.test_and_connection()
        self.test_in_mixed_datatypes_constant()
        self.test_not_in_mixed_datatypes_constant()
        self.test_hosted_vars()
        self.test_hosted_list()
        self.test_index_usage_list()
        self.test_index_usage_subquery()

    ############## single constant [in|not in][constant][subquery] ###########
    # select f1 from t where f2 {in|not in} {[]|(select ...)}
    def test_in_single_constant(self):
        self._load_test_data()
        self._create_indexes()
        query_in = "select varchar_field from temp_bucket  where int_field in [1] order by varchar_field"
        query_or = "select varchar_field from temp_bucket  where int_field=1 order by varchar_field"
        try:
            self._validate_results(query_in, query_or)
        finally:
            self._unload_test_data()

    def test_in_single_subquery(self):
        self._load_test_data()
        self._create_indexes()
        query_in = "select varchar_field from temp_bucket where int_field in (select raw int_field from temp_bucket a where a.int_field=1) order by varchar_field"
        query_or = "select varchar_field from temp_bucket where int_field = 1 order by varchar_field"
        try:
            self._validate_results(query_in, query_or)
        finally:
            self._unload_test_data()

    def test_not_in_single_constant(self):
        self._load_test_data()
        self._create_indexes()
        query_in = "select varchar_field from temp_bucket  where int_field not in [1] order by varchar_field"
        query_or = "select varchar_field from temp_bucket  where int_field!=1 order by varchar_field"
        try:
            self._validate_results(query_in, query_or)
        finally:
            self._unload_test_data()

    def test_not_in_single_subquery(self):
        self._load_test_data()
        self._create_indexes()
        query_in = "select varchar_field from temp_bucket  where int_field not in (select raw int_field from temp_bucket a where a.int_field=1) order by varchar_field"
        query_or = "select varchar_field from temp_bucket  where int_field!=1 order by varchar_field"
        try:
            self._validate_results(query_in, query_or)
        finally:
            self._unload_test_data()

    ############## [in|not in][constant] #####################################
    # select f1 from t where f2 {in|not in} [1001 values]
    def test_in_1001_constant(self):
        self._load_test_data()
        self._create_indexes()
        query = "select varchar_field from temp_bucket where int_field in ["
        for i in range(1001):
            query+=str(i)
            if i<1000:
                query+=","
        query+="]"
        results_in = self.run_cbq_query(query)
        try:
            self.assertEqual(results_in['status'], 'success')
        finally:
            self._unload_test_data()

    def test_not_in_1001_constant(self):
        self._load_test_data()
        self._create_indexes()
        query = "select varchar_field from temp_bucket where int_field not in ["
        for i in range(1001):
            query+=str(i)
            if i<1000:
                query+=","
        query+="]"
        results_in = self.run_cbq_query(query)
        try:
            self.assertEqual(results_in['status'], 'success')
        finally:
            self._unload_test_data()


    # select f1 from t where f2 {in|not in} {[contains]|[not contains]|(select ... contains)|(select ... not contains)}
    def test_simple_query(self):
        self._load_test_data()
        self._create_indexes()
        count = 0
        test_dict = dict()
        primary_idx = {'name': '#primary', 'bucket': "temp_bucket", 'fields': (), 'state': 'online', 'using': self.index_type.lower(), 'is_primary': True}
        idx_1 = {'name': "ix1", 'bucket': "temp_bucket", 'fields': [("int_field", 0)], 'state': "online", 'using': self.index_type.lower(), 'is_primary':False}

        for innotin in self.in_not_in:
            #for wherecond in (self.constant_list+self.subquery):
            for wherecond in self.constant_list:
                query_in = "select varchar_field from temp_bucket where int_field "
                query_or = "select varchar_field from temp_bucket where "
                query_in += innotin + wherecond
                if innotin == self.in_str:
                    if wherecond == self.contains_list_str or wherecond == self.contains_select_str:
                        query_or += self.contains_or_condition
                    else:
                        query_or += self.not_contains_or_condition
                else:
                    if wherecond == self.contains_list_str or wherecond == self.contains_select_str:
                        query_or+=self.contains_and_condition
                    else:
                        query_or += self.not_contains_and_condition
                query_in += " order by varchar_field"
                query_or += " order by varchar_field"

                expected_result = self.run_cbq_query(query_or)

                lambda1 = lambda x, y = expected_result['results']: self.assertEqual(x['q_res'][0]['results'], y)
                test_dict["%d-default" % (count)] = {"indexes": [primary_idx, idx_1],
                                                     "pre_queries": [],
                                                     "queries": [query_in],
                                                     "post_queries": [],
                                                     "asserts": [lambda1],
                                                     "cleanups": []}
                count += 1

        try:
            self.query_runner(test_dict)
        finally:
            self._unload_test_data()

    # select f1 from t where f2 {in|not in} {[contains]|[not contains]|(select...contains)|(select...not contains)}
    #                           or f2 {in|not in} {[contains]|[not contains]|(select...contains)|(select...not contains)}
    def test_or_connection(self):
        self._load_test_data()
        self._create_indexes()
        count = 0
        test_dict = dict()
        primary_idx = {'name': '#primary', 'bucket': "temp_bucket", 'fields': [], 'state': 'online', 'using': self.index_type.lower(), 'is_primary': True}
        idx_1 = {'name': "ix1", 'bucket': "temp_bucket", 'fields': [("int_field", 0)], 'state': "online", 'using': self.index_type.lower(), 'is_primary':False}

        for innotin1 in self.in_not_in:
            for wherecond1 in self.constant_list+self.subquery:
                for innotin2 in self.in_not_in:
                    for wherecond2 in self.constant_list+self.subquery:
                        query_in = "select varchar_field from temp_bucket where int_field "
                        query_or = "select varchar_field from temp_bucket where "
                        query_in += innotin1 + wherecond1 + " or int_field " + innotin2 + wherecond2
                        if innotin1 == self.in_str:
                            if wherecond1 == self.contains_list_str or wherecond1 == self.contains_select_str:
                                query_or += self.contains_or_condition
                            else:
                                query_or += self.not_contains_or_condition
                        else:
                            if wherecond1 == self.contains_list_str or wherecond1 == self.contains_select_str:
                                query_or += self.contains_and_condition
                            else:
                                query_or += self.not_contains_and_condition

                        query_or += ' or '
                        if innotin2 == self.in_str:
                            if wherecond2 == self.contains_list_str or wherecond2 == self.contains_select_str:
                                query_or += self.contains_or_condition
                            else:
                                query_or += self.not_contains_or_condition
                        else:
                            if wherecond2 == self.contains_list_str or wherecond2 == self.contains_select_str:
                                query_or += self.contains_and_condition
                            else:
                                query_or += self.not_contains_and_condition
                        query_in+=" order by varchar_field"
                        query_or+=" order by varchar_field"
                        expected_result = self.run_cbq_query(query_or)

                        lambda1 = lambda x, y=expected_result['results']: self.assertEqual(x['q_res'][0]['results'], y)
                        test_dict["%d-default" % (count)] = {"indexes": [primary_idx, idx_1],
                                             "pre_queries": [],
                                             "queries": [query_in],
                                             "post_queries": [],
                                             "asserts": [lambda1],
                                             "cleanups": []}
                        count += 1

        try:
            self.query_runner(test_dict)
        finally:
            self._unload_test_data()

    # select f1 from t where f2 {in|not in} {[contains]|[not contains]|(select...contains)|(select...not contains)}
    #                           and f2 {in|not in} {[contains]|[not contains]|(select...contains)|(select...not contains)}
    def test_and_connection(self):
        self._load_test_data()
        self._create_indexes()
        count = 0
        test_dict = dict()
        primary_idx = {'name': '#primary', 'bucket': "temp_bucket", 'fields': [], 'state': 'online', 'using': self.index_type.lower(), 'is_primary': True}
        idx_1 = {'name': "ix1", 'bucket': "temp_bucket", 'fields': [("int_field", 0)], 'state': "online", 'using': self.index_type.lower(), 'is_primary':False}

        for innotin1 in self.in_not_in:
            for wherecond1 in self.constant_list + self.subquery:
                for innotin2 in self.in_not_in:
                    for wherecond2 in self.constant_list + self.subquery:
                        query_in = "select varchar_field from temp_bucket where int_field "
                        query_or = "select varchar_field from temp_bucket where "
                        query_in += innotin1 + wherecond1 + " and int_field " + innotin2 + wherecond2
                        if innotin1 == self.in_str:
                            if wherecond1 == self.contains_list_str or wherecond1 == self.contains_select_str:
                                query_or += self.contains_or_condition
                            else:
                                query_or += self.not_contains_or_condition
                        else:
                            if wherecond1 == self.contains_list_str or wherecond1 == self.contains_select_str:
                                query_or += self.contains_and_condition
                            else:
                                query_or += self.not_contains_and_condition

                        query_or += ' and '
                        if innotin2 == self.in_str:
                            if wherecond2 == self.contains_list_str or wherecond2 == self.contains_select_str:
                                query_or += self.contains_or_condition
                            else:
                                query_or += self.not_contains_or_condition
                        else:
                            if wherecond2 == self.contains_list_str or wherecond2 == self.contains_select_str:
                                query_or += self.contains_and_condition
                            else:
                                query_or += self.not_contains_and_condition

                        query_in += " order by varchar_field"
                        query_or += " order by varchar_field"
                        expected_result = self.run_cbq_query(query_or)

                        lambda1 = lambda x, y=expected_result['results']: self.assertEqual(x['q_res'][0]['results'], y)
                        test_dict["%d-default" % (count)] = {"indexes": [primary_idx, idx_1],
                                             "pre_queries": [],
                                             "queries": [query_in],
                                             "post_queries": [],
                                             "asserts": [lambda1],
                                             "cleanups": []}
                        count += 1

        try:
            self.query_runner(test_dict)
        finally:
            self._unload_test_data()

    def test_in_mixed_datatypes_constant(self):
        self._load_test_data()
        self._create_indexes()
        query_in = "select varchar_field from temp_bucket where int_field in [1, True, 'string1',2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]"
        query_or = "select varchar_field from temp_bucket where int_field=1 or int_field=True or int_field='string1' or int_field=2 or int_field=3 or int_field=4"\
                   " or int_field=5 or int_field=6 or int_field=7 or int_field=8 or int_field=9 or int_field=10 or int_field=11 or int_field=12 or int_field=13"\
                   " or int_field=14 or int_field=15 or int_field=16"
        query_in += " order by varchar_field"
        query_or += " order by varchar_field"
        try:
            self._validate_results(query_in, query_or)
        finally:
            self._unload_test_data()

    def test_not_in_mixed_datatypes_constant(self):
        self._load_test_data()
        self._create_indexes()
        query_in = "select varchar_field from temp_bucket where int_field not in [1, True, 'string1',2,3,4,5,6,7,8,9,10,11,12,13,14,15]"
        query_or = "select varchar_field from temp_bucket where int_field!=1 and int_field!=True and int_field!='string1' and int_field!=2 and int_field!=3 and int_field!=4"\
                   " and int_field!=5 and int_field!=6 and int_field!=7 and int_field!=8 and int_field!=9 and int_field!=10 and int_field!=11 and int_field!=12"\
                   " and int_field!=13 and int_field!=14 and int_field!=15"
        query_in += " order by varchar_field"
        query_or += " order by varchar_field"
        try:
            self._validate_results(query_in, query_or)
        finally:
            self._unload_test_data()


    def test_hosted_vars(self):
        self._load_test_data()
        self._create_indexes()
        args = "args=[2,4,5,7,9,11,14]"
        cmd = ("{3} -u {0}:{1} http://{2}:8093/query/service -d " \
              "statement='select varchar_field from temp_bucket where int_field in [1,$1,3,$2,$3,6,$4,8,$5,10,$6,12,13,$7,15,16,17,18,19] order by varchar_field&" \
               +args+"'").\
              format('Administrator', 'password', self.master.ip, self.curl_path)

        shell = RemoteMachineShellConnection(self.master)

        output, error = shell.execute_command(cmd)
        query_or = "select varchar_field from temp_bucket where int_field=1 or int_field=%d or int_field=3 or int_field=%d or int_field=%d or int_field=6"\
                   " or int_field=%d or int_field=8 or int_field=%d or int_field=10 or int_field=%d or int_field=12 or int_field=13 or int_field=%d or int_field=15"\
                   " or int_field=16 or int_field=17 or int_field=18 or int_field=19 order by varchar_field" % (2, 4, 5, 7, 9, 11, 14)
        result_or = self.run_cbq_query(query_or)
        json_output_str = ''
        for s in output:
            json_output_str+=s
        json_output = json.loads(json_output_str)
        normalized_output = json_output['results']
        try:
            self.assertEqual(normalized_output, result_or['results'])
        finally:
            self._unload_test_data()

    def test_hosted_list(self):
        self._load_test_data()
        self._create_indexes()
        args = "args=[[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]]"
        cmd = ("{3} -u {0}:{1} http://{2}:8093/query/service -d " \
              "statement='select varchar_field from temp_bucket where int_field in $1 order by varchar_field&" \
               +args+"'").\
              format('Administrator', 'password', self.master.ip, self.curl_path)

        shell = RemoteMachineShellConnection(self.master)
        output, error = shell.execute_command(cmd)

        query_or = "select varchar_field from temp_bucket where int_field=1 or int_field=2 or int_field=3 or int_field=4 or int_field=5 or int_field=6 or int_field=7 or int_field=8 or "\
                   "int_field=9 or int_field=10 or int_field=11 or int_field=12 or int_field=13 or int_field=14 or int_field=15 or int_field=16 or int_field=17 or int_field=18 or int_field=19 "\
                   " order by varchar_field"
        result_or = self.run_cbq_query(query_or)
        json_output_str = ''
        for s in output:
            json_output_str+=s
        json_output = json.loads(json_output_str)
        normalized_output = json_output['results']
        try:
            self.assertEqual(normalized_output, result_or['results'])
        finally:
            self._unload_test_data()

    def test_index_usage_list(self):
        self._load_test_data()
        self._create_indexes()
        query = "explain select varchar_field from temp_bucket where int_field in [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]"
        result = self.run_cbq_query(query)
        index_in_use = result['results'][0]['plan']['~children'][0]['index']
        try:
            self.assertEqual(index_in_use, "ix1")
        finally:
            self._unload_test_data()

    def test_index_usage_subquery(self):
        self._load_test_data()
        self._create_indexes()
        query = "explain select varchar_field from temp_bucket where int_field in (select int_field from temp_bucket t1 where varchar_field='string1')"
        result = self.run_cbq_query(query)
        index_in_use = result['results'][0]['plan']['~children'][0]['index']
        try:
            self.assertEqual(index_in_use, "ix1")
        finally:
            self._unload_test_data()

    def _validate_results(self, query_in, query_or):
        results_in = self.run_cbq_query(query_in)
        results_or = self.run_cbq_query(query_or)
        self.assertEqual(results_in['results'], results_or['results'])


    def _unload_test_data(self):
        self.cluster.bucket_delete(self.master, "temp_bucket")


    def _load_test_data(self):
        test_data = {"int_field": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"],
                             "bool_field": ["TRUE", "FALSE"],
                             "varchar_field": ["string1", "string2", "string3", "string4", "string5",
                                               "string6", "string7", "string8", "string9", "string10",
                                               "string11", "string12", "string13", "string14", "string15",
                                               "string16", "string17", "string18", "string19", "string20"]
                             }

        temp_bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket("temp_bucket", 11222, temp_bucket_params)

        for bucket in self.buckets:
            self.cluster.bucket_flush(self.master, bucket=bucket, timeout=self.wait_timeout * 5)

        for i1 in range(len(test_data["int_field"])):
            for i2 in range(len(test_data["bool_field"])):
                for i3 in range(len(test_data["varchar_field"])):
                    query = "insert into temp_bucket values ('key_"+str(i1)+"_"+str(i2)+"_"+str(i3)+"', {"
                    int_field_insert = "'int_field': "+test_data["int_field"][i1]+","
                    bool_field_insert = "'bool_field': "+test_data["bool_field"][i2]+","
                    varchar_field_insert = "'varchar_field': '"+test_data["varchar_field"][i3]+"'"

                    query += " "+int_field_insert+bool_field_insert+varchar_field_insert+"})"
                    self.run_cbq_query(query)

    def _create_indexes(self):
        self.run_cbq_query('CREATE PRIMARY INDEX `#primary` ON `temp_bucket`')
        self.run_cbq_query('CREATE INDEX ix1 ON temp_bucket(int_field);')

    def _drop_indexes(self):
        self.run_cbq_query("DROP INDEX temp_bucket.`#primary` USING GSI")
        self.run_cbq_query("DROP INDEX temp_bucket.`ix1` USING GSI")
