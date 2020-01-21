from .tuq import QueryTests

class AscDescTests(QueryTests):
    def setUp(self):
        self.skip_buckets_handle = True if not self._testMethodName == 'suite_setUp' else None
        super(AscDescTests, self).setUp()

    def tearDown(self):
        super(AscDescTests, self).tearDown()

    # This test is for composite index on different fields where it makes sure the query uses the particular asc/desc index
    # and results are compared against query run against primary index and static results generated and sorted manually.
    def test_asc_desc_composite_index(self):

        test_dict = dict()
        index_type = self.index_type.lower()

        # extra defs
        static_res_2 = ['query-testemployee96373.2660745-3', 'query-testemployee96373.2660745-2', 'query-testemployee96373.2660745-1',
                        'query-testemployee96373.2660745-0', 'query-testemployee92486.5251626-5']
        static_res_4 = ['query-testemployee10153.1877827-0', 'query-testemployee10153.1877827-1', 'query-testemployee10153.1877827-2',
                        'query-testemployee10153.1877827-3', 'query-testemployee10153.1877827-4']
        static_res_6 = ['query-testemployee10153.1877827-2', 'query-testemployee10153.1877827-3', 'query-testemployee10153.1877827-4',
                        'query-testemployee10153.1877827-5', 'query-testemployee10194.855617-0']

        # index defs
        #primary_index = ("#primary", "default", [], "online", index_type)
        primary_index = {'name': '#primary',
                         'bucket': 'default',
                         'fields': [],
                         'state': 'online',
                         'using': index_type,
                         'is_primary': True}
        #index_1 = ("idx", "default", ["join_yr ASC", " _id DESC"], "online", index_type)
        index_1 = {'name': 'idx',
                   'bucket': 'default',
                   'fields': [("join_yr", 0), ("_id DESC", 1)],
                   'state': 'online',
                   'using': index_type,
                   'is_primary': False}

        # pre query defs

        # query defs
        query_1 = 'explain SELECT * FROM default WHERE join_yr > 10 ORDER BY join_yr, _id DESC LIMIT 100 OFFSET 200'
        query_2 = 'SELECT * FROM default WHERE join_yr > 10 ORDER BY join_yr, _id DESC,_id LIMIT 10 OFFSET 2'
        query_3 = 'explain SELECT * FROM default WHERE join_yr > 10 ORDER BY join_yr,meta().id ASC LIMIT 10 OFFSET 2'
        query_4 = 'SELECT * FROM default WHERE join_yr > 10 ORDER BY meta().id,join_yr ASC LIMIT 10'
        query_5 = 'explain SELECT * FROM default WHERE join_yr > 10 and _id like "query-test%" ORDER BY join_yr desc,_id asc LIMIT 10 OFFSET 2'
        query_6 = 'SELECT * FROM default WHERE join_yr > 10 ORDER BY _id,join_yr asc LIMIT 10 OFFSET 2'
        query_7 = 'explain SELECT * FROM default WHERE join_yr > 10 and meta().id like "query-test%" ORDER BY join_yr asc,meta().id ASC LIMIT 10 OFFSET 2'
        query_8 = 'SELECT * FROM default WHERE join_yr > 10 and meta().id like "query-test%" ORDER BY meta().id,join_yr asc LIMIT 10 OFFSET 2'
        query_9 = 'SELECT * FROM default WHERE join_yr > 10 ORDER BY meta().id,join_yr DESC LIMIT 10'

        # post query defs
        explain_1 = lambda x: self.ExplainPlanHelper(x['q_res'][0])

        # assert defs
        assert_1 = lambda x: self.assertEqual(x['post_q_res'][0]['~children'][0]['~children'][0]['index'], 'idx')
        assert_2 = lambda x: self.compare("test_asc_desc_composite_index", query_2, static_res_2)
        assert_4 = lambda x: self.compare("test_asc_desc_composite_index", query_4, static_res_4)
        assert_6 = lambda x: self.compare("test_asc_desc_composite_index", query_6, static_res_6)
        assert_7 = lambda x: self.assertEqual(x['post_q_res'][0]['~children'][0]['~children'][0]['scans'][0]['index'], 'idx')
        assert_8 = lambda x: self.compare("test_asc_desc_composite_index", query_8, static_res_6)
        assert_9 = lambda x: self.compare("test_asc_desc_composite_index", query_9, static_res_4)

        # cleanup defs

        for bucket in self.buckets:
            bname = bucket.name
            test_dict["1-%s" % (bname)] = {"indexes": [primary_index, index_1],
                                           "pre_queries": [],
                                           "queries": [query_1],
                                           "post_queries": [explain_1],
                                           "asserts": [assert_1],
                                           "cleanups": []}

            test_dict["2-%s" % (bname)] = {"indexes": [primary_index, index_1], "pre_queries": [], "queries": [query_2],
                                           "post_queries": [], "asserts": [assert_2], "cleanups": []}

            test_dict["3-%s" % (bname)] = {"indexes": [primary_index, index_1], "pre_queries": [], "queries": [query_3],
                                           "post_queries": [explain_1], "asserts": [assert_1],"cleanups": []}

            test_dict["4-%s" % (bname)] = {"indexes": [primary_index, index_1], "pre_queries": [], "queries": [query_4],
                                           "post_queries": [], "asserts": [assert_4], "cleanups": []}

            test_dict["5-%s" % (bname)] = {"indexes": [primary_index, index_1], "pre_queries": [], "queries": [query_5],
                                           "post_queries": [explain_1], "asserts": [assert_1], "cleanups": []}

            test_dict["6-%s" % (bname)] = {"indexes": [primary_index, index_1], "pre_queries": [], "queries": [query_6],
                                           "post_queries": [], "asserts": [assert_6], "cleanups": []}

            test_dict["7-%s" % (bname)] = {"indexes": [primary_index, index_1], "pre_queries": [], "queries": [query_7],
                                           "post_queries": [explain_1], "asserts": [assert_7], "cleanups": []}

            test_dict["8-%s" % (bname)] = {"indexes": [primary_index, index_1], "pre_queries": [], "queries": [query_8],
                                            "post_queries": [], "asserts": [assert_8], "cleanups": []}

            test_dict["9-%s" % (bname)] = {"indexes": [primary_index, index_1], "pre_queries": [], "queries": [query_9],
                                            "post_queries": [], "asserts": [assert_9], "cleanups": []}

        self.query_runner(test_dict)

    # This test test various combination of fields in an array index.
    def test_asc_desc_array_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT array i FOR i in %s END asc,_id desc) WHERE (department[0] = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.query = "EXPLAIN select * from %s WHERE department[0] = 'Support' and ( ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby asc" % (
                bucket.name, bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['scan']['index'], idx)

                self.query = "EXPLAIN select * from %s WHERE department[0] = 'Support' and ( ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby desc" % (
                bucket.name, bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['scan']['index'], idx)

                self.query = "EXPLAIN select * from %s WHERE department[0] = 'Support' and ( ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby desc,_id asc" % (
                bucket.name, bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['scan']['index'], idx)

                self.query = "EXPLAIN select * from %s WHERE department[0] = 'Support' and ( ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby asc,_id desc" % (
                bucket.name, bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['scan']['index'], idx)

                self.query = "select * from %s WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby asc,_id desc limit 10" % (
                bucket.name, bucket.name)
                static_expected_results_list = ['query-testemployee28748.5695367-5', 'query-testemployee28748.5695367-4',
                                                'query-testemployee28748.5695367-3', 'query-testemployee28748.5695367-2',
                                                'query-testemployee28748.5695367-1']

                self.compare("test_asc_desc_array_index", self.query, static_expected_results_list)

                self.query = "select * from %s WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby desc,_id asc" % (
                bucket.name, bucket.name) + \
                             " limit 10"
                static_expected_results_list = ['query-testemployee78599.6934121-0', 'query-testemployee78599.6934121-1',
                                                'query-testemployee78599.6934121-2', 'query-testemployee78599.6934121-3',
                                                'query-testemployee78599.6934121-4']
                self.compare("test_asc_desc_array_index", self.query, static_expected_results_list)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()

    #This test checks if the results with descending index and order by are in reverse of ascending one.
    def test_desc_isReverse_ascOrder(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "create index %s on %s(VMs[0].memory desc)"% (idx, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.query = "Explain select meta().id from %s where VMs[0].memory > 0 order by VMs[0].memory" %(bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx)
                self.assertTrue("sort_terms" in str(actual_result['results']))
                self.assertTrue("covers" in str(plan))
                self.query = "Explain select meta().id from %s where VMs[0].memory > 0 order by  VMs[0].memory desc" %(bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx)
                self.assertTrue("sort_terms" not in actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select meta().id from %s where VMs[0].memory > 0 order by meta().id,VMs[0].memory limit 10" %(bucket.name)
                static_expected_results_list =['query-testemployee10153.1877827-0', 'query-testemployee10153.1877827-1',
                                               'query-testemployee10153.1877827-2', 'query-testemployee10153.1877827-3',
                                               'query-testemployee10153.1877827-4']
                self.compare("test_desc_isReverse_ascOrder", self.query, static_expected_results_list)

                self.query = "select meta().id from %s where VMs[0].memory > 0 order by meta().id, VMs[0].memory desc limit 10" %(bucket.name)
                static_expected_results_list =['query-testemployee10153.1877827-0', 'query-testemployee10153.1877827-1',
                                               'query-testemployee10153.1877827-2', 'query-testemployee10153.1877827-3',
                                               'query-testemployee10153.1877827-4']

                self.compare("test_desc_isReverse_ascOrder", self.query, static_expected_results_list)
                idx2 = "idx2"
                self.query = "create index %s on %s(email,_id)"% (idx2, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                idx3 = "idx3"
                self.query = "create index %s on %s(email,_id desc)"% (idx3, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx3)
                self.query = 'select _id from %s where email like "%s" order by _id limit 2' %(bucket.name, '24-mail%')
                actual_result_asc = self.run_cbq_query()
                static_expected_results_list = ["query-testemployee12264.589461-0", "query-testemployee12264.589461-1"]
                actual_result_asc = [actual_result_asc['results'][0]['_id'], actual_result_asc['results'][1]['_id']]
                self.assertEqual(static_expected_results_list, actual_result_asc)

                self.query = 'select _id from %s where email like "%s" order by _id desc limit 2' %(bucket.name, '24-mail%')
                actual_result_desc = self.run_cbq_query()
                static_expected_results_list =["query-testemployee9987.55838821-5", "query-testemployee9987.55838821-4"]
                actual_result_desc = [actual_result_desc['results'][0]['_id'], actual_result_desc['results'][1]['_id']]
                self.assertEqual(static_expected_results_list, actual_result_desc)
            finally:
              for idx in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                self.run_cbq_query()

    def test_prepared_statements(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                self.query = "create index idx_cover_asc on %s(_id asc, VMs[0].RAM asc)" % bucket.name
                created_indexes.append('idx_cover_asc')
                self.run_cbq_query()
                self.query = "PREPARE ascdescquery1 FROM SELECT * from %s WHERE _id is not null order by _id asc limit 2" % bucket.name
                res = self.run_cbq_query()
                self.assertTrue("idx_cover_asc" in str(res['results'][0]))
                actual_result = self.run_cbq_query(query='ascdescquery1', is_prepared=True)
                self.assertEqual(actual_result['results'][0][bucket.name]['name'], [{'FirstName': 'employeefirstname-9'}, {'MiddleName': 'employeemiddlename-9'}, {'LastName': 'employeelastname-9'}])
                self.query = "drop index %s.idx_cover_asc" % bucket.name
                self.run_cbq_query()
                created_indexes.remove('idx_cover_asc')
                if self._is_index_in_list(bucket, "#primary"):
                    self.query = "drop primary index on %s" % bucket.name
                    self.run_cbq_query()
                self.query = "create index idx_cover_desc on %s(_id desc, VMs[0].RAM desc) " % bucket.name
                self.run_cbq_query()
                created_indexes.append('idx_cover_desc')
                self.query = "PREPARE ascdescquery2 FROM SELECT * from %s WHERE _id is not null order by _id desc limit 2"  % bucket.name
                res = self.run_cbq_query()
                self.assertTrue("idx_cover_desc" in str(res['results'][0]))
                actual_result = self.run_cbq_query(query='ascdescquery2', is_prepared=True)
                self.assertEqual(actual_result['results'][0][bucket.name]['name'], [{'FirstName': 'employeefirstname-24'}, {'MiddleName': 'employeemiddlename-24'}, {'LastName': 'employeelastname-24'}])
                self.query = "drop index %s.idx_cover_desc" % bucket.name
                self.run_cbq_query()
                created_indexes.remove('idx_cover_desc')
                if not self._is_index_in_list(bucket, "#primary"):
                    self.query = "create primary index on %s" % bucket.name
                    self.run_cbq_query()
                try:
                    self.run_cbq_query(query='ascdescquery1', is_prepared=True)
                except Exception as ex:
                    msg = "Error message is %s." % str(ex)
                    self.fail("query should not fail")
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    #This test creates asc,desc index on meta and use it in predicate and order by
    def test_meta(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(meta().id asc,_id,tasks,age,hobbies.hobby)" % (
                  idx, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                idx2 = "idx2"
                self.query = "CREATE INDEX %s ON %s(meta().id desc,_id,tasks,age,hobbies.hobby)" % (
                  idx2, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = 'explain select * from %s where meta().id ="query-testemployee10317.9004497-0" and _id is not null and hobbies.hobby is not missing' \
                             ' order by meta().id asc' %(bucket.name)
                res =self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx2 or plan['~children'][0]['~children'][0]['index']==idx)
                self.query = 'explain select * from %s where meta().id ="query-testemployee10317.9004497-0" and _id is not missing and tasks is not null and hobbies.hobby is not missing' \
                             ' order by meta().id desc' %(bucket.name)
                res =self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx2 or plan['~children'][0]['~children'][0]['index']==idx)
                self.query = 'select * from %s where meta().id ="query-testemployee10317.9004497-0" and _id is not null and hobbies.hobby is not missing' \
                             ' order by meta().id asc'%(bucket.name)
                actual_result =self.run_cbq_query()
                self.assertEqual(actual_result['results'][0]['default']['_id'], "query-testemployee10317.9004497-0")

                self.query = 'select * from %s where meta().id ="query-testemployee10317.9004497-0" and tasks is not null and hobbies.hobby is not missing' \
                             ' order by meta().id desc'%(bucket.name)
                actual_result =self.run_cbq_query()
                self.assertEqual(actual_result['results'][0]['default']['_id'], "query-testemployee10317.9004497-0")

                self.query = "drop index default.idx"
                self.run_cbq_query()
                self.query = 'explain select * from %s where meta().id ="query-testemployee10317.9004497-0" and _id is not null and hobbies.hobby is not missing' \
                             ' order by meta().id'%(bucket.name)
                res =self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx2)
                self.query = "CREATE INDEX %s ON %s(meta().id asc,_id,tasks,age,hobbies.hobby)" % (
                  idx, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                self.query = "drop index default.idx2"
                self.run_cbq_query()
                created_indexes.remove(idx2)
                self.query = 'explain select * from default where meta().id ="query-testemployee10317.9004497-0" and _id is not missing and tasks is not null and age is not missing' \
                             ' order by meta().id desc'
                res =self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx)
            finally:
                  for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()



    def test_max_min(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx2 = "idx2"
                self.query = "CREATE INDEX %s ON %s(_id desc,join_yr[0] desc)" % (
                  idx2, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query ='select max(join_yr[0]) from default where _id is not missing and join_yr[0] is not null'
                res = self.run_cbq_query()
                self.assertEqual(res['results'], [{'$1': 2016}])
                self.query = "drop index default.idx2"
                self.run_cbq_query()
                created_indexes.remove(idx2)
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(_id asc,join_yr[0] asc)" % (
                  idx, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.query = 'select min(_id) from default where _id is not missing'
                res = self.run_cbq_query()
                self.assertEqual(res['results'], [{'$1': 'query-testemployee10153.1877827-0'}])
            finally:
                  for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()

    def test_datetime_boolean_long_mapvalues(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                #long values
                #datetime value
                #boolean value
                #map value
                self.query = 'insert into %s (KEY, VALUE) VALUES ("test1",{"id":9223372036854775806,' \
                             '"indexMap":{"key1":"val1", "key2":"val2"},"data":{"foo":"bar"},' \
                             '"datetime":"2017-04-28T11:57:26.852-07:00","isPresent":true})'%(bucket.name)
                self.run_cbq_query()
                self.query = 'insert into %s (KEY, VALUE) VALUES ("test2",{"id":9223372036854775807,' \
                             '"indexMap":{"key1":"val1", "key2":"val2"},"data":{"foo":"bar"},' \
                             '"datetime":"2017-04-29T11:57:26.852-07:00","isPresent":false})'%(bucket.name)
                self.run_cbq_query()
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(datetime asc)" % (
                  idx, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.query = "explain select meta().id from %s where datetime is not missing order by datetime"%(bucket.name)
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx)
                self.query = "select meta().id from %s where datetime is not null order by datetime asc"%(bucket.name)
                res = self.run_cbq_query()
                self.assertEqual(res['results'][0]['id'], "test1")
                self.assertEqual(res['results'][1]['id'], "test2")
                idx2 = "idx2"
                self.query = "CREATE INDEX %s ON %s(isPresent desc)" % (
                  idx2, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = 'explain select meta().id from %s where isPresent is not missing order by isPresent'%(bucket.name)
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx2)
                self.query = 'select meta().id from %s where isPresent=true or isPresent=false order by isPresent'%(bucket.name)
                res = self.run_cbq_query()
                self.assertEqual(res['results'][0]['id'], "test2")
                self.assertEqual(res['results'][1]['id'], "test1")
                self.query = 'select meta().id from %s where isPresent=true or isPresent=false order by isPresent desc'%(bucket.name)
                res = self.run_cbq_query()
                self.assertEqual(res['results'][0]['id'], "test1")
                self.assertEqual(res['results'][1]['id'], "test2")
                idx3 = "idx3"
                self.query = "CREATE INDEX %s ON %s(id desc)" % (
                  idx3, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx3)
                self.query = 'explain select meta().id from %s where id > 1 order by id'%(bucket.name)
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx3)
                self.query = 'select meta().id from %s where id > 1 order by id'%(bucket.name)
                res = self.run_cbq_query()
                self.assertEqual(res['results'][0]['id'], "test1")
                self.assertEqual(res['results'][1]['id'], "test2")
                idx4 = "idx4"
                self.query = "CREATE INDEX %s ON %s(indexMap asc)" % (
                  idx4, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx4)
                idx5 = "idx5"
                self.query = "CREATE INDEX %s ON %s(id asc)" % (
                  idx5, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx5)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx5)
                idx6 = "idx6"
                self.query = "CREATE INDEX %s ON %s(indexMap desc)" % (
                  idx6, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx6)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx6)
                idx7 = "idx7"
                self.query = "CREATE INDEX %s ON %s(isPresent asc)" % (
                  idx7, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx7)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx7)
                created_indexes.remove(idx2)
                self.query = 'drop index default.idx2'
                self.run_cbq_query()
                self.query = 'explain select meta().id from %s where isPresent = true order by isPresent'%(bucket.name)
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx7)
                self.query = 'select meta().id from %s where isPresent=true or isPresent=false order by isPresent asc'%(bucket.name)
                res = self.run_cbq_query()
                self.assertEqual(res['results'][0]['id'], "test2")
                self.assertEqual(res['results'][1]['id'], "test1")
                idx8 = "idx8"
                self.query = "CREATE INDEX %s ON %s(datetime desc)" % (
                  idx8, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx8)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx8)
                created_indexes.remove(idx)
                self.query = 'drop index default.idx'
                self.run_cbq_query()
                self.query = "explain select meta().id from %s where datetime > '2006-01-02T15:04:05' order by datetime"%(bucket.name)
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx8)
                self.query = "select meta().id from %s where datetime > '2006-01-02T15:04:05' order by datetime"%(bucket.name)
                res = self.run_cbq_query()
                self.assertEqual(res['results'][0]['id'], "test1")
                self.assertEqual(res['results'][1]['id'], "test2")
            finally:
                  for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()

    #Test for bug MB-23941
    def test_asc_desc_unnest(self):
         self.fail_if_no_buckets()
         for bucket in self.buckets:
            created_indexes = []
            try:
                self.query = 'INSERT INTO %s VALUES ("k001", {"arr":[{"y":1},{"y":1}, {"y":2},{"y":2}, {"y":2}, {"y":12},{"y":21}]})'%(bucket.name)
                self.run_cbq_query()
                self.query = 'INSERT INTO %s VALUES ("k002", {"arr":[{"y":11},{"y1":-1}, {"z":42},{"p":42}, {"q":-2}, {"y":102},{"y":201}]})'%(bucket.name)
                self.run_cbq_query()

                self.query = 'CREATE INDEX ix1 ON %s(ALL ARRAY a.y FOR a IN arr END)'%(bucket.name)
                created_indexes.append("ix1")
                self.run_cbq_query()
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 limit 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], 'ix1')
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y limit 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], 'ix1')
                self.query = 'EXPLAIN SELECT MIN(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['index'], 'ix1')
                self.assertTrue("covers" in str(plan))
                self.query = 'EXPLAIN SELECT COUNT(1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['index'], 'ix1')
                self.query = 'EXPLAIN SELECT COUNT(a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['index'], 'ix1')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['index'], 'ix1')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['index'], 'ix1')
                self.query = 'SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 order by meta(d).id,a.y'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT a.y FROM default d USE INDEX (`#primary`) UNNEST d.arr As a WHERE a.y > 10 order by meta(d).id,a.y'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT a.y FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y desc'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT a.y FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y desc'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT MIN(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT MIN(a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT COUNT(1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(1) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'drop index default.ix1'
                self.run_cbq_query()
                created_indexes.remove("ix1")
                self.query = 'create index ix2 on default(ALL ARRAY a.y for a in arr END desc)'
                self.run_cbq_query()
                created_indexes.append("ix2")
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 limit 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], 'ix2')
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y DESC limit 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], 'ix2')
                self.query = 'EXPLAIN SELECT MAX(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['index'], 'ix2')
                self.query = 'EXPLAIN SELECT COUNT(1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['index'], 'ix2')
                self.query = 'EXPLAIN SELECT COUNT(a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix2')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['index'], 'ix2')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['index'], 'ix2')
                self.query = 'SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 order by meta(d).id,a.y'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT a.y FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10 order by meta(d).id,a.y'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y DESC limit 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT a.y FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y DESC limit 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query ='SELECT MAX(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                actual_result = self.run_cbq_query()
                self.query ='SELECT MAX(a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT COUNT(1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(1) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT COUNT(a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d USE INDEX (`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'drop index default.ix2'
                self.run_cbq_query()
                created_indexes.remove("ix2")
                self.query = 'create index ix3 on default(ALL DISTINCT ARRAY a.y FOR a IN arr END DESC )'
                self.run_cbq_query()
                created_indexes.append("ix3")
                self.query = 'EXPLAIN SELECT MIN(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['index'], 'ix3')
                self.query = 'EXPLAIN SELECT MAX(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['index'], 'ix3')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['index'], 'ix3')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertEqual(plan['~children'][0]['index'], 'ix3')
                self.query = 'SELECT MIN(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT MIN(a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT MAX(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT MAX(a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d  UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d use index(`#primary`)  UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'SELECT MIN(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT MIN(a.y) FROM default d USE INDEX (`#primary`) UNNEST d.arr As a WHERE a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
            finally:
                  for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                  self.query = 'delete from default use keys["k001","k002"]'
                  self.run_cbq_query()

    #Test for bug MB-23941
    def test_pushdown_ascdesc_unnest(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                self.query = 'create index ix1 on default(ALL ARRAY a.y FOR a IN arr END)'
                self.run_cbq_query()
                created_indexes.append("ix1")
                self.query = 'INSERT INTO %s VALUES ("k001", {"arr":[{"y":1},{"y":1}, {"y":2},{"y":2}, {"y":2}, {"y":12},{"y":21}]})'%(bucket.name)
                self.run_cbq_query()
                self.query = 'INSERT INTO %s VALUES ("k002", {"arr":[{"y":11},{"y1":-1}, {"z":42},{"p":42}, {"q":-2}, {"y":102},{"y":201}]})'%(bucket.name)
                self.run_cbq_query()
                self.query = 'explain select a.y from default d UNNEST d.arr As a where a.y > 10 limit 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '10')
                self.query = 'explain select a.y from default d UNNEST d.arr As a where a.y > 10 ORDER BY a.y limit 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '10')
                self.query = 'explain select min(a.y) from default d UNNEST d.arr As a where a.y > 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue('index_group_aggs' in str(plan))
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue('index_group_aggs' in str(plan))
                self.query = 'explain select count(a.y) from default d UNNEST d.arr As a where a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertEqual( plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')
                self.query ='select min(a.y) from default d UNNEST d.arr As a where a.y > 10'
                actual_result = self.run_cbq_query()
                self.query ='select min(a.y) from default d use index(`#primary`) UNNEST d.arr As a where a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = 'drop index default.ix1'
                self.run_cbq_query()
                created_indexes.remove("ix1")
                self.query = 'create index ix2 on default(ALL ARRAY a.y FOR a IN arr END desc)'
                self.run_cbq_query()
                created_indexes.append("ix2")
                self.query = 'explain select max(a.y) from default d UNNEST d.arr As a where a.y > 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue('index_group_aggs' in str(plan))
                self.query = 'create index ix3 on default(ALL DISTINCT ARRAY a.y FOR a IN arr END DESC )'
                self.run_cbq_query()
                created_indexes.append("ix3")
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 limit 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '10')
                self.query = 'EXPLAIN SELECT a.y FROM default d  UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y DESC limit 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '10')
                self.query = 'EXPLAIN SELECT COUNT(1) FROM default d  UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("limit" not in str(plan['~children'][0]))
                self.query = 'EXPLAIN SELECT COUNT(a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("limit" not in str(plan['~children'][0]))
                self.query = 'drop index default.ix2'
                self.run_cbq_query()
                created_indexes.remove("ix2")
                self.query = 'create index ix4 on default(ALL DISTINCT ARRAY a.y FOR a IN arr END DESC )'
                self.run_cbq_query()
                created_indexes.append("ix4")
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 limit 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("limit" not in str(plan['~children'][0]))
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y limit 10'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue("limit" not in str(plan['~children'][0]))
            finally:
                  for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                  self.query = 'delete from default use keys["k001","k002"]'
                  self.run_cbq_query()


