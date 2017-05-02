from lib import testconstants
from lib.membase.api.exception import CBQError
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.basetestcase import BaseTestCase
from tuqquery.tuq import ExplainPlanHelper
from pytests.tuqquery.tuq import QueryTests


class AscDescTests(QueryTests):
    def setUp(self):
        if not self._testMethodName == 'suite_setUp':
            self.skip_buckets_handle = True
        super(AscDescTests, self).setUp()
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.scan_consistency = self.input.param("scan_consistency", 'REQUEST_PLUS')

    def tearDown(self):
        server = self.master
        shell = RemoteMachineShellConnection(server)
        self.sleep(20)
        super(AscDescTests, self).tearDown()

    def test_asc_desc_composite_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON default(join_yr ASC, join_day DESC)"%(idx)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.query = 'explain SELECT * FROM default WHERE join_yr > 10 ' \
                             'ORDER BY join_yr, join_day DESC LIMIT 100 OFFSET 200'
                actual_result = self.run_cbq_query()
                plan=ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx)
                self.query = 'SELECT * FROM default WHERE join_yr > 10 ' \
                             'ORDER BY join_yr, join_day DESC,_id LIMIT 10 OFFSET 2'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT * FROM default use index(`#primary`) WHERE join_yr > 10 ' \
                             'ORDER BY join_yr, join_day DESC,_id LIMIT 10 OFFSET 2'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])

                self.query = 'explain SELECT * FROM default WHERE join_yr > 10 ' \
                             'ORDER BY join_yr,meta().id ASC LIMIT 10 OFFSET 2'
                actual_result = self.run_cbq_query()
                plan=ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx)
                self.query = 'SELECT * FROM default WHERE join_yr > 10 ' \
                             'ORDER BY join_yr,meta().id ASC LIMIT 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT * FROM default use index(`#primary`) WHERE join_yr > 10 ' \
                             'ORDER BY join_yr,meta().id ASC LIMIT 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])

                self.query = 'explain SELECT * FROM default WHERE join_yr > 10 and join_day <10 ' \
                             'ORDER BY join_yr asc,join_day desc LIMIT 10 OFFSET 2'
                actual_result = self.run_cbq_query()
                plan=ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx)
                self.query = 'SELECT * FROM default WHERE join_yr > 10 and join_day <10 ' \
                             'ORDER BY join_yr asc,join_day desc,_id LIMIT 10 OFFSET 2'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT * FROM default use index(`#primary`) WHERE join_yr > 10 ' \
                             'and join_day <10 ORDER BY join_yr asc,join_day desc,_id LIMIT 10 offset 2'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])

                self.query = 'explain SELECT * FROM default WHERE join_yr > 10 and join_day <10 and meta().id like "query-test%"' \
                             'ORDER BY join_yr asc,join_day desc,meta().id ASC LIMIT 10 OFFSET 2'
                actual_result = self.run_cbq_query()
                plan=ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']==idx)
                self.query = 'SELECT * FROM default WHERE join_yr > 10 and join_day <10 and meta().id like "query-test%"' \
                             'ORDER BY join_yr asc,join_day desc,meta().id ASC LIMIT 10 OFFSET 2'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT * FROM default use index(`#primary`) WHERE join_yr > 10 and join_day <10 and meta().id like "query-test%"' \
                             'ORDER BY join_yr asc,join_day desc,meta().id ASC LIMIT 10 OFFSET 2'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'explain SELECT * FROM default WHERE join_yr > 10 and join_day <10 and meta().id like "query-test%"' \
                             'ORDER BY join_yr asc,join_day desc,meta().id DESC LIMIT 10'
                actual_result = self.run_cbq_query()
                plan=ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx)
                self.query = 'SELECT * FROM default WHERE join_yr > 10 and join_day <10 ' \
                             'ORDER BY join_yr asc,join_day desc,meta().id DESC LIMIT 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT * FROM default use index(`#primary`) WHERE join_yr > 10 and join_day <10 ' \
                             'ORDER BY join_yr asc,join_day desc,meta().id DESC LIMIT 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()

    def test_asc_desc_array_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT array i FOR i in %s END asc,_id desc) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and ( ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby asc" % (
                bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue( plan['~children'][0]['~children'][0]['scan']['index']==idx)
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and ( ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby desc" % (
                bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue( plan['~children'][0]['~children'][0]['scan']['index']==idx)
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and ( ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby desc,_id asc" % (
                bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue( plan['~children'][0]['~children'][0]['scan']['index']==idx)
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and ( ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby asc,_id desc" % (
                bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue( plan['~children'][0]['~children'][0]['scan']['index']==idx)
                self.query = "select * from %s WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby asc,_id desc limit 10" % (
                bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select * from %s use index(`#primary`) WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby asc,_id desc" % (
                bucket.name,bucket.name) + \
                             " limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = "select * from %s use index(`#primary`) WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby desc,_id asc" % (
                bucket.name,bucket.name) + \
                             " limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select * from %s use index(`#primary`) WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) order by hobbies.hobby desc,_id asc" % (
                bucket.name,bucket.name) + \
                             " limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()

    def test_desc_isReverse_ascOrder(self):
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
		    plan = ExplainPlanHelper(actual_result)
                    self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx)
                    self.assertTrue("sort_terms" in str(actual_result['results']))
                    self.assertTrue("covers" in str(plan))
                    self.query = "Explain select meta().id from %s where VMs[0].memory > 0 order by  VMs[0].memory desc" %(bucket.name)
                    actual_result = self.run_cbq_query()
		    plan = ExplainPlanHelper(actual_result)
                    self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx)
                    self.assertTrue("sort_terms" not in actual_result)
                    self.assertTrue("covers" in str(plan))
                    self.query = "select meta().id from %s where VMs[0].memory > 0 order by VMs[0].memory,meta().id limit 10" %(bucket.name)
                    actual_result = self.run_cbq_query()
                    self.query = "select meta().id from %s use index(`#primary`) where VMs[0].memory > 0 order by VMs[0].memory,meta().id limit 10" %(bucket.name)
                    expected_result = self.run_cbq_query()
                    self.assertTrue(actual_result['results']==expected_result['results'])
                    self.query = "select meta().id from %s where VMs[0].memory > 0 order by VMs[0].memory desc,meta().id limit 10" %(bucket.name)
                    actual_result = self.run_cbq_query()
                    self.query = "select meta().id from %s use index(`#primary`) where VMs[0].memory > 0 order by VMs[0].memory desc,meta().id limit 10" %(bucket.name)
                    expected_result = self.run_cbq_query()
                    self.assertTrue(actual_result['results']==expected_result['results'])
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
                    self.query = 'select _id from %s where email like "%s" order by _id limit 2' %(bucket.name,'24-mail%')
                    actual_result_asc = self.run_cbq_query()
                    print actual_result_asc['results']
                    self.assertTrue(actual_result_asc['results'][0]['_id']=="query-testemployee12264.589461-0")
                    self.assertTrue(actual_result_asc['results'][1]['_id']=="query-testemployee13052.0229901-0")
                    self.query = 'select _id from %s where email like "%s" order by _id desc limit 2' %(bucket.name,'24-mail%')
                    actual_result_desc = self.run_cbq_query()
                    print actual_result_desc['results']
                    self.assertTrue(actual_result_desc['results'][0]['_id']=="query-testemployee9987.55838821-0")
                    self.assertTrue(actual_result_desc['results'][1]['_id']=="query-testemployee99749.8338694-0")
                finally:
                  for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()

    def test_prepared_statements(self):
        for bucket in self.buckets:
            self.query = "create index idx_cover_asc on %s(_id asc, VMs[0].RAM asc)" % (bucket.name)
            self.run_cbq_query()
            self.query = "PREPARE ascdescquery1 FROM SELECT * from default WHERE _id is not null order by _id asc limit 2"
            res = self.run_cbq_query()
            self.assertTrue("idx_cover_asc" in str(res['results'][0]))
            actual_result = self.run_cbq_query(query='ascdescquery1', is_prepared=True)
            print actual_result
            self.assertTrue(actual_result['results'][0]['default']['name']==[{u'FirstName': u'employeefirstname-9'}, {u'MiddleName': u'employeemiddlename-9'}, {u'LastName': u'employeelastname-9'}])
            self.query = "drop index default.idx_cover_asc"
            self.run_cbq_query()
            self.query = "drop primary index on default"
            self.run_cbq_query()
            self.query = "create index idx_cover_desc on %s(_id desc, VMs[0].RAM desc) " % (bucket.name)
            self.run_cbq_query()
            self.query = "PREPARE ascdescquery2 FROM SELECT * from default WHERE _id is not null order by _id desc limit 2"
            res = self.run_cbq_query()
            self.assertTrue("idx_cover_desc" in str(res['results'][0]))
            actual_result = self.run_cbq_query(query='ascdescquery2', is_prepared=True)
            print actual_result
            self.assertTrue(actual_result['results'][0]['default']['name']==[{u'FirstName': u'employeefirstname-24'}, {u'MiddleName': u'employeemiddlename-24'}, {u'LastName': u'employeelastname-24'}])
            try:
                self.run_cbq_query(query='ascdescquery1', is_prepared=True)
            except Exception, ex:
               self.assertTrue(str(ex).find("Index Not Found - cause: queryport.indexNotFound") != -1,
                              "Error message is %s." % str(ex))
            else:
                self.fail("Error message expected")

    def test_meta(self):
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
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx)
                self.query = 'explain select * from %s where meta().id ="query-testemployee10317.9004497-0" and _id is not missing and tasks is not null and age is not missing' \
                             ' order by meta().id desc' %(bucket.name)
                res =self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx2 or plan['~children'][0]['~children'][0]['index']==idx)
                self.query = 'select * from %s where meta().id ="query-testemployee10317.9004497-0" and _id is not null and hobbies.hobby is not missing' \
                             ' order by meta().id asc'%(bucket.name)
                actual_result =self.run_cbq_query()
                self.query = 'select * from %s use index(`#primary`) where meta().id ="query-testemployee10317.9004497-0" and _id is not null and hobbies.hobby is not missing' \
                             ' order by meta().id asc'%(bucket.name)
                expected_result =self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'select * from %s where meta().id ="query-testemployee10317.9004497-0" and tasks is not null and age is not missing' \
                             ' order by meta().id desc'%(bucket.name)
                actual_result =self.run_cbq_query()
                self.query = 'select * from %s use index(`#primary`) where meta().id ="query-testemployee10317.9004497-0" and tasks is not null and age is not missing' \
                             ' order by meta().id desc'%(bucket.name)
                expected_result =self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = "drop index default.idx"
                self.run_cbq_query()
                self.query = 'explain select * from %s where meta().id ="query-testemployee10317.9004497-0" and _id is not null and hobbies.hobby is not missing' \
                             ' order by meta().id'%(bucket.name)
                res =self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx2)
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
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx)
            finally:
                  for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()



    def test_max_min(self):
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
                self.query = 'explain select max(_id) from default where _id is not missing'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['limit']=='1')
                self.query ='select max(join_yr[0]) from default where _id is not missing and join_yr[0] is not null'
                res = self.run_cbq_query()
                self.assertTrue(res['results']==[{u'$1': 2016}])
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
                self.query = 'explain select min(_id) from default where _id is not missing'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['limit']=='1')
                self.query = 'select min(_id) from default where _id is not missing'
                res = self.run_cbq_query()
                self.assertTrue(res['results']==[{u'$1': u'query-testemployee10153.1877827-0'}])
            finally:
                  for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()

    def test_datetime_boolean_long_mapvalues(self):
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
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx)
                self.query = "select meta().id from %s where datetime is not null order by datetime asc"%(bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res['results'][0]['id']=="test1")
                self.assertTrue(res['results'][1]['id']=="test2")
                idx2 = "idx2"
                self.query = "CREATE INDEX %s ON %s(isPresent desc)" % (
                  idx2, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = 'explain select meta().id from %s where isPresent is not missing order by isPresent'%(bucket.name)
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx2)
                self.query = 'select meta().id from %s where isPresent=true or isPresent=false order by isPresent'%(bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res['results'][0]['id']=="test2")
                self.assertTrue(res['results'][1]['id']=="test1")
                self.query = 'select meta().id from %s where isPresent=true or isPresent=false order by isPresent desc'%(bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res['results'][0]['id']=="test1")
                self.assertTrue(res['results'][1]['id']=="test2")
                idx3 = "idx3"
                self.query = "CREATE INDEX %s ON %s(id desc)" % (
                  idx3, bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx3)
                self.query = 'explain select meta().id from %s where id > 1 order by id'%(bucket.name)
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx3)
                self.query = 'select meta().id from %s where id > 1 order by id'%(bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res['results'][0]['id']=="test1")
                self.assertTrue(res['results'][1]['id']=="test2")
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
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx7)
                self.query = 'select meta().id from %s where isPresent=true or isPresent=false order by isPresent asc'%(bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res['results'][0]['id']=="test2")
                self.assertTrue(res['results'][1]['id']=="test1")
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
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx8)
                self.query = "select meta().id from %s where datetime > '2006-01-02T15:04:05' order by datetime"%(bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res['results'][0]['id']=="test1")
                self.assertTrue(res['results'][1]['id']=="test2")
            finally:
                  for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()


    def test_asc_desc_unnest(self):
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
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['~children'][0]['index']=='ix1')
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y limit 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['~children'][0]['index']=='ix1')
                self.query = 'EXPLAIN SELECT MIN(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['index']=='ix1')
                self.assertTrue("covers" in str(plan))
                self.query = 'EXPLAIN SELECT COUNT(1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix1')
                self.query = 'EXPLAIN SELECT COUNT(a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix1')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix1')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix1')
                self.query = 'SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 order by meta(d).id,a.y'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT a.y FROM default d USE INDEX (`#primary`) UNNEST d.arr As a WHERE a.y > 10 order by meta(d).id,a.y'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT a.y FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y desc'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT a.y FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y desc'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT MIN(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT MIN(a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT COUNT(1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(1) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'drop index default.ix1'
                self.run_cbq_query()
                created_indexes.remove("ix1")
                self.query = 'create index ix2 on default(ALL ARRAY a.y for a in arr END desc)'
                self.run_cbq_query()
                created_indexes.append("ix2")
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 limit 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['~children'][0]['index']=='ix2')
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y DESC limit 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['~children'][0]['index']=='ix2')
                self.query = 'EXPLAIN SELECT MAX(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix2')
                self.query = 'EXPLAIN SELECT COUNT(1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix2')
                self.query = 'EXPLAIN SELECT COUNT(a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix2')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix2')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix2')
                self.query = 'SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 order by meta(d).id,a.y'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT a.y FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10 order by meta(d).id,a.y'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y DESC limit 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT a.y FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y DESC limit 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query ='SELECT MAX(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                actual_result = self.run_cbq_query()
                self.query ='SELECT MAX(a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT COUNT(1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(1) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT COUNT(a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d USE INDEX (`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'drop index default.ix2'
                self.run_cbq_query()
                created_indexes.remove("ix2")
                self.query = 'create index ix3 on default(ALL DISTINCT ARRAY a.y FOR a IN arr END DESC )'
                self.run_cbq_query()
                created_indexes.append("ix3")
                self.query = 'EXPLAIN SELECT MIN(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['scan']['index']=='ix3')
                self.query = 'EXPLAIN SELECT MAX(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix3')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("IndexCountDistinctScan2" in str(plan))
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix3')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index']=='ix3')
                self.query = 'SELECT MIN(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT MIN(a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT MAX(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT MAX(a.y) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d  UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT a.y) FROM default d use index(`#primary`)  UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT COUNT(DISTINCT 1) FROM default d use index(`#primary`) UNNEST d.arr As a WHERE a.y = 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'SELECT MIN(a.y) FROM default d UNNEST d.arr As a WHERE a.y > 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT MIN(a.y) FROM default d USE INDEX (`#primary`) UNNEST d.arr As a WHERE a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
            finally:
                  for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()

    #Test for bug MB-23941
    def test_pushdown_ascdesc_unnest(self):
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
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['limit']=='10')
                self.query = 'explain select a.y from default d UNNEST d.arr As a where a.y > 10 ORDER BY a.y limit 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['limit']=='10')
                self.query = 'explain select min(a.y) from default d UNNEST d.arr As a where a.y > 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['limit']=='1')
                self.query = 'EXPLAIN SELECT COUNT(DISTINCT 1) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['limit']=='1')
                self.query = 'explain select count(a.y) from default d UNNEST d.arr As a where a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue('IndexCountScan2' in str(plan))
                self.query ='select min(a.y) from default d UNNEST d.arr As a where a.y > 10'
                actual_result = self.run_cbq_query()
                self.query ='select min(a.y) from default d use index(`#primary`) UNNEST d.arr As a where a.y > 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'drop index default.ix1'
                self.run_cbq_query()
                created_indexes.remove("ix1")
                self.query = 'create index ix2 on default(ALL ARRAY a.y FOR a IN arr END desc)'
                self.run_cbq_query()
                created_indexes.append("ix2")
                self.query = 'explain select max(a.y) from default d UNNEST d.arr As a where a.y > 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['limit']=='1')
                self.query = 'create index ix3 on default(ALL DISTINCT ARRAY a.y FOR a IN arr END DESC )'
                self.run_cbq_query()
                created_indexes.append("ix3")
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 limit 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['limit']=='10')
                self.query = 'EXPLAIN SELECT a.y FROM default d  UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y DESC limit 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]['~children'][0]['limit']=='10')
                self.query = 'EXPLAIN SELECT COUNT(1) FROM default d  UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("limit" not in str(plan['~children'][0]))
                self.query = 'EXPLAIN SELECT COUNT(a.y) FROM default d UNNEST d.arr As a WHERE a.y = 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("limit" not in str(plan['~children'][0]))
                self.query = 'drop index default.ix2'
                self.run_cbq_query()
                created_indexes.remove("ix2")
                self.query = 'create index ix4 on default(ALL DISTINCT ARRAY a.y FOR a IN arr END DESC )'
                self.run_cbq_query()
                created_indexes.append("ix4")
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 limit 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("limit" not in str(plan['~children'][0]))
                self.query = 'EXPLAIN SELECT a.y FROM default d UNNEST d.arr As a WHERE a.y > 10 ORDER BY a.y limit 10'
                res = self.run_cbq_query()
                plan = ExplainPlanHelper(res)
                self.assertTrue("limit" not in str(plan['~children'][0]))
            finally:
                  for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()



