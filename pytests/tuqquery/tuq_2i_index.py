import json
import math
import re
import uuid
import time

from tuq import QueryTests
from tuq import ExplainPlanHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError

class QueriesIndexTests(QueryTests):

    FIELDS_TO_INDEX = [('name', 'job_title'), ('name', 'join_yr'), ('VMs', 'name')]
    COMPLEX_FIELDS_TO_INDEX = ['VMs', 'tasks_points', 'skills']
    def setUp(self):
        super(QueriesIndexTests, self).setUp()
        self.num_indexes = self.input.param('num_indexes', 1)
        if self.num_indexes > len(self.FIELDS_TO_INDEX):
            self.input.test_params["stop-on-failure"] = True
            self.log.error("MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED")
            self.fail('MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED')
        self.rest = RestConnection(self.master)

    def suite_setUp(self):
        super(QueriesIndexTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesIndexTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesIndexTests, self).suite_tearDown()

    def test_meta_indexcountscan(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
               idx = "idx"
               self.query = "CREATE INDEX %s ON %s ( name )" %(idx,bucket.name)
               self.run_cbq_query()
               idx2 = "idx2"
               self.query = "CREATE INDEX %s ON %s ( department )"%(idx2,bucket.name)
               self.run_cbq_query()
               created_indexes.append(idx)
               created_indexes.append(idx2)
               self.query = 'explain select count(1) from {0} where name = [ '.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}'+\
               ' ] and meta().id = "query-testemployee10317.9004497-0"'
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue("IndexCountScan" not in plan)
               self.query = 'select count(1) from {0} where name = ['.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}'+\
               '] and meta().id = "query-testemployee10317.9004497-0"'
               actual_result = self.run_cbq_query()
               self.assertTrue(actual_result['results'] == ([{u'$1': 1}]))
               self.query = 'explain select name from {0} where name = ['.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}'+\
               '] and meta().id like "query-testemployee%" limit 3'
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue("limit" not in plan['~children'][0])
               self.query = 'select name from {0} where name = ['.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}'+\
               '] and meta().id like "query-testemployee%" order by meta().id limit 3'
               actual_result = self.run_cbq_query()
               self.assertTrue(actual_result['results'] == ([{u'name': [{u'FirstName': u'employeefirstname-23'}, {u'MiddleName': u'employeemiddlename-23'}, {u'LastName': u'employeelastname-23'}]}, {u'name': [{u'FirstName': u'employeefirstname-23'}, {u'MiddleName': u'employeemiddlename-23'}, {u'LastName': u'employeelastname-23'}]}, {u'name': [{u'FirstName': u'employeefirstname-23'}, {u'MiddleName': u'employeemiddlename-23'}, {u'LastName': u'employeelastname-23'}]}]))

               self.query = 'explain select min(department) from {0} where department = "Support" and meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)

               self.assertTrue("limit" not in plan['~children'][0])


               self.query = 'select min(department) from {0} where department = "Support" and meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
               actual_result = self.run_cbq_query()
               self.assertTrue(actual_result['results'] ==  [{u'$1': None}])

               self.query = 'explain select count(1) from {0} where name = ['.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}]'
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue(plan['~children'][0]['#operator']=="IndexCountScan")

               self.query = 'select count(1) from {0} where name = ['.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}]'

               actual_result = self.run_cbq_query()
               self.assertTrue(actual_result['results']==[{u'$1': 360}])

               self.query = 'explain select count(1) from {0} where name = ['.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}] and join_yr=2010'
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue(plan['~children'][0]['#operator']!="IndexCountScan")

               self.query = 'select count(1) from {0} where name = ['.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}] and department="Support"'
               actual_result = self.run_cbq_query()
               self.assertTrue(actual_result['results'] == [{u'$1': 36}])

               self.query = 'drop index {0}.idx'.format(bucket.name)
               self.run_cbq_query()
               self.query = "CREATE INDEX %s ON %s ( name,meta().id )" %(idx,bucket.name)
               self.run_cbq_query()
               self.query = 'explain select count(1) from {0} where name = [ '.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}'+\
               ' ] and meta().id = "query-testemployee10317.9004497-0"'
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue(plan['~children'][0]['#operator']=="IndexCountScan")

               self.query = 'select count(1) from {0} where name = ['.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}'+\
               '] and meta().id = "query-testemployee10317.9004497-0"'
               actual_result = self.run_cbq_query()
               self.assertTrue(actual_result['results'] == ([{u'$1': 1}]))


               self.query = 'drop index {0}.idx'.format(bucket.name)
               self.run_cbq_query()
               self.query = "CREATE INDEX %s ON %s ( meta().id )" %(idx,bucket.name)
               self.run_cbq_query()
               self.query = 'explain select count(1) from {0} where meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue(plan['~children'][0]['#operator']=="IndexCountScan")
               self.query = 'select count(1) from {0} where meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
               actual_result = self.run_cbq_query()
               self.assertTrue(actual_result['results']==[{u'$1': 1}])

               self.query = 'drop index {0}.idx'.format(bucket.name)
               self.run_cbq_query()

               self.query = 'explain select count(1) from {0} where meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue(plan['~children'][0]['#operator']=="IndexCountScan")
               self.query = 'select count(1) from {0} where meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
               actual_result = self.run_cbq_query()
               self.assertTrue(actual_result['results']==[{u'$1': 1}])

               self.query = 'CREATE INDEX idx ON {0}( join_yr[0] ) WHERE department = "Support"'.format(bucket.name)
               self.run_cbq_query()
               self.query = 'explain select count(*) from {0} where join_yr[0]>2012 and department = "Support"'.format(bucket.name)
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue(plan['~children'][0]['#operator']=="IndexCountScan")

               self.query = 'select count(*) from {0} where join_yr[0]>2012 and department = "Support"'.format(bucket.name)
               actual_result = self.run_cbq_query()
               self.assertTrue(actual_result['results']==[{u'$1': 181}])


               self.query = 'drop index {0}.idx'.format(bucket.name)
               self.run_cbq_query()
               self.query = 'CREATE INDEX idx ON {0}( department ) WHERE join_yr[0]<2012'.format(bucket.name)
               self.run_cbq_query()
               self.query = 'explain select count(*) from {0} where department = "Support" and join_yr[0]<2010'.format(bucket.name)
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue(plan['~children'][0]['#operator']!="IndexCountScan")
               self.query = 'select count(*) from {0} where department = "Support" and join_yr[0]<2011'.format(bucket.name)
               actual_result = self.run_cbq_query()
               print "count is {0}".format(actual_result['results'])
               #self.assertTrue(actual_result['results']==[{u'$1': 53}])


               self.query = 'explain select count(*) from {0} where department = "Support" and join_yr[0]<2012 limit 10'.format(bucket.name)
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)

               self.assertTrue("limit" not in plan['~children'][0])

               self.query = 'drop index {0}.idx'.format(bucket.name)
               self.run_cbq_query()
               self.query = 'CREATE INDEX idx ON {0}( name,department,join_yr[0] )'.format(bucket.name)
               self.run_cbq_query()
               self.query = 'explain select count(1) from {0} where name = [ '.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}'+\
               ' ] and department = "Support"'
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue(plan['~children'][0]['#operator']=="IndexCountScan")

               self.query = 'explain select count(1) from {0} where name = [ '.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}'+\
               ' ] and join_yr[0] = 2010'
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue(plan['~children'][0]['#operator']!="IndexCountScan")

               self.query = 'explain select tasks from {0} where name = [ '.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}'+\
               ' ] and department = "Support" limit 5'
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue("limit" in plan['~children'][0]['~children'][0])

               self.query = 'explain select department from {0} where name = [ '.format(bucket.name)+\
               '{"FirstName": "employeefirstname-23"},{"MiddleName": "employeemiddlename-23"},{ "LastName": "employeelastname-23"}'+\
               ' ] and join_yr[0] = 2010 limit 5'
               actual_result = self.run_cbq_query()
               plan = ExplainPlanHelper(actual_result)
               self.assertTrue("limit"  not in plan['~children'][0])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")



    def test_index_missing_null(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s ( VMs )" %(idx,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "Explain select * from %s where 5 in VMs"  %(bucket.name)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index']==idx)
                idx2= "idx2"
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                self.run_cbq_query()
                created_indexes.remove(idx)
                self.query = "CREATE INDEX %s ON %s ( VMs[0].RAM = 11 and join_yr = 2010  )" %(idx2,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = "Explain select * from %s where VMs[0].RAM = 11 and join_yr = 2010 "  %(bucket.name)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index']==idx2)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_limit_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s ( department,_id )" %(idx,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "Explain select * from %s where "  %(bucket.name)+\
                             "department = 'Manager' and _id LIKE 'query-test%' limit 10"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan["~children"][0]["~children"][0]["limit"] == "10")

                idx2 = "idx2"
                self.query = "CREATE INDEX %s ON %s ( _id  )" %(idx2,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = "Explain  select * from {0} b1 nest {0} b2 on keys b1._id where b1._id like 'query-testemployee%' limit 5 ".format(bucket.name)
                actual_result = self.run_cbq_query()
                plan1 = ExplainPlanHelper(actual_result)
                self.assertFalse("limit" in plan1["~children"][0]["~children"][0])
                self.query = "Explain  select * from {0} b1 nest {0} b2 on key b2._id FOR b1 where b1._id like 'query-testemployee%'  limit 5 ".format(bucket.name)
                actual_result = self.run_cbq_query()
                plan2 = ExplainPlanHelper(actual_result)
                self.assertFalse("limit" in plan2["~children"][0]["~children"][0])
                self.query = "Explain  select * from {0} b1 left nest {0} b2 on keys b1._id where b1._id like 'query-testemployee%' limit 10 ".format(bucket.name)
                actual_result = self.run_cbq_query()
                plan3 = ExplainPlanHelper(actual_result)
                self.assertTrue(plan3["~children"][0]["~children"][0]["limit"] == "10")
                self.query = "Explain  select * from {0} b1 left nest {0} b2 on key b2._id FOR b1 where b1._id like 'query-testemployee%'  limit 10 ".format(bucket.name)
                actual_result = self.run_cbq_query()
                plan4 = ExplainPlanHelper(actual_result)
                self.assertTrue(plan4["~children"][0]["~children"][0]["limit"] == "10")

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_ifmissing_ifnull(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                idx2 = "idx2"
                idx3 = "idx3"
                idx4 = "idx4"
                idx5 = "idx5"

                self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()
                self.sleep(15,'wait for index')
                self.query = 'CREATE INDEX {0} ON {1}( IFMISSING( IsSpecial,b, name ), join_day,name ) using {2}'.format(idx,bucket.name,self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (bucket.name, "k01", {"x": 10, "b":True, "c":"pq"})
                self.run_cbq_query()
                self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (bucket.name, "k02", {"IsSpecial": True, "b":True, "c":"pq"})
                self.run_cbq_query()
                self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (bucket.name, "k03", {"IsSpecial": False, "b":True, "c":"pq"})
                self.run_cbq_query()
                self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)' % (bucket.name, "k04", {"IsSpecial":json.JSONEncoder().encode(None), "b":2, "c":"pq"})
                self.run_cbq_query()
                self.query = 'SELECT meta().id, IFMISSING(IsSpecial,b,name) from %s limit 5'%(bucket.name)
                actual_result = self.run_cbq_query()
                expected_result = [{u'id': u'k01', u'$1': True}, {u'id': u'k02', u'$1': True}, {u'id': u'k03', u'$1': False}, {u'id': u'k04', u'$1': u'null'}, {u'id': u'query-testemployee10153.1877827-0', u'$1': u'employee-9'}]
                self.assertTrue(actual_result['results']==expected_result)
                self.query = 'EXPLAIN SELECT name from {0} where IFMISSING(IsSpecial,b,name) = TRUE and join_day> 1'.format(bucket.name)
                actual_result = self.run_cbq_query()
                plan1 = ExplainPlanHelper(actual_result)
                self.assertTrue(plan1['~children'][0]['index']==idx)

                self.query = 'CREATE INDEX {0} ON {1}( IFNULL( IsSpecial,b, name ), join_day,name ) using {2}'.format(idx2,bucket.name,self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)

                self.query = 'SELECT meta().id, IFNULL(IsSpecial,b,2,3) from {0} limit 5'.format(bucket.name)
                actual_result = self.run_cbq_query()
                expected_result = [{u'id': u'k01'}, {u'id': u'k02', u'$1': True}, {u'id': u'k03', u'$1': False}, {u'id': u'k04', u'$1': u'null'}, {u'id': u'query-testemployee10153.1877827-0'}]
                self.assertTrue(actual_result['results']==expected_result)

                self.query = 'EXPLAIN SELECT name from {0} where IFNULL(IsSpecial,b,name) = null and join_day> 1'.format(bucket.name)
                actual_result = self.run_cbq_query()
                plan2 = ExplainPlanHelper(actual_result)
                self.assertTrue(plan2['~children'][0]['index']==idx2)

                self.query = 'CREATE INDEX {0} ON {1}( MISSINGIF( IsSpecial,b ), join_day,name ) using {2}'.format(idx3,bucket.name,self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx3)

                self.query = 'SELECT meta().id, MISSINGIF(IsSpecial,b) from {0} limit 5'.format(bucket.name)
                actual_result = self.run_cbq_query()
                expected_result = [{u'id': u'k01'}, {u'id': u'k02'}, {u'id': u'k03', u'$1': False}, {u'id': u'k04', u'$1': u'null'}, {u'id': u'query-testemployee10153.1877827-0'}]
                self.assertTrue(actual_result['results']==expected_result)

                self.query = 'EXPLAIN SELECT name from {0} where MISSINGIF(IsSpecial,b) = TRUE and join_day> 1'.format(bucket.name)
                actual_result = self.run_cbq_query()
                plan2 = ExplainPlanHelper(actual_result)
                self.assertTrue(plan2['~children'][0]['index']==idx3)


                self.query = 'CREATE INDEX {0} ON {1}( NULLIF( IsSpecial,b ), join_day,name ) using {2}'.format(idx4,bucket.name,self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx4)

                self.query = 'SELECT meta().id, NULLIF(IsSpecial,b) from {0} limit 5'.format(bucket.name)
                actual_result = self.run_cbq_query()
                expected_result = [{u'id': u'k01'}, {u'id': u'k02', u'$1': None}, {u'id': u'k03', u'$1': False}, {u'id': u'k04', u'$1': u'null'}, {u'id': u'query-testemployee10153.1877827-0'}]
                self.assertTrue(actual_result['results']==expected_result)

                self.query = 'EXPLAIN SELECT name from {0} where NULLIF(IsSpecial,b) IS null and join_day> 1'.format(bucket.name)
                actual_result = self.run_cbq_query()
                plan2 = ExplainPlanHelper(actual_result)
                self.assertTrue(plan2['~children'][0]['index']==idx4)

                self.query = 'CREATE INDEX {0} ON {1}( IFMISSINGORNULL( IsSpecial,b,name,2,3 ), join_day,name )'.format(idx5,bucket.name)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx5)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx5)

                self.query = 'SELECT meta().id, IFMISSINGORNULL(IsSpecial,b,name,2,3) from {0} limit 5'.format(bucket.name)
                actual_result = self.run_cbq_query()
                expected_result = [{u'id': u'k01', u'$1': True}, {u'id': u'k02', u'$1': True}, {u'id': u'k03', u'$1': False}, {u'id': u'k04', u'$1': u'null'}, {u'id': u'query-testemployee10153.1877827-0', u'$1': u'employee-9'}]
                self.assertTrue(actual_result['results']==expected_result)

                self.query = 'EXPLAIN SELECT name from {0} where IFMISSINGORNULL(IsSpecial,b,name,2,3) IS NULL and join_day> 1'.format(bucket.name)
                actual_result = self.run_cbq_query()
                plan2 = ExplainPlanHelper(actual_result)
                self.assertTrue(plan2['~children'][0]['index']==idx5)

            finally:
                self.query = 'delete from {0} where meta().id = {1}'.format(bucket.name, "k01")
                self.run_cbq_query()
                self.query = 'delete from {0} where meta().id = {1}'.format(bucket.name, "k02")
                self.run_cbq_query()
                self.query = 'delete from {0} where meta().id = {1}'.format(bucket.name, "k03")
                self.run_cbq_query()
                self.query = 'delete from {0} where meta().id = {1}'.format(bucket.name, "k04")
                self.run_cbq_query()
                self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_subdoc_field(self):
            for bucket in self.buckets:
                created_indexes = []
                try:
                    idx = "idx"
                    self.query = 'CREATE INDEX idx on {0}(tasks_points.task1) using {1}'.format(bucket.name,self.index_type)
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, idx)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(idx)
                    self.query = 'select tasks_points.task1 from {0} where tasks_points.task1 = 1'.format(bucket.name)
                    actual_result = self.run_cbq_query()
                    self.assertTrue(actual_result['metrics']['resultCount']==2016)
                finally:
                    for idx in created_indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                        actual_result = self.run_cbq_query()
                        self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_intersect(self):
        for bucket in self.buckets:
            self.query = 'select ARRAY_INTERSECT(join_yr,[2011,2012,2016,"test"], [2011,2016], [2012,2016]) as test from {0}'.format(bucket.name)
            actual_result = self.run_cbq_query()
            os = self.shell.extract_remote_info().type.lower()
            if os == "windows":
                number_of_doc = 1680
            else:
                number_of_doc = 10079
            self.assertTrue(actual_result['metrics']['resultCount'] == number_of_doc)

    def test_in_spans(self):
         for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s ( join_day )" %(idx,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.query = 'explain select join_day from %s where join_day in ["5", $1] '  %(bucket.name)
                actual_result = self.run_cbq_query()
                plan1 = ExplainPlanHelper(actual_result)
                self.assertTrue(len(plan1['~children'][0]['scan']['spans'])==2)
                self.query = 'explain select join_day from %s where join_day in [$1] '  %(bucket.name)
                actual_result = self.run_cbq_query()
                plan2 = ExplainPlanHelper(actual_result)

                self.assertTrue(len(plan2['~children'][0]['spans'])==1)

                self.query = 'explain select join_day from %s where join_day in  $1 '  %(bucket.name)
                actual_result = self.run_cbq_query()

                plan3 = ExplainPlanHelper(actual_result)
                self.assertTrue(len(plan3['~children'][0]['spans'])==1)
                # There is different expected behavior in 4.7 vs earlier versions
                if self.index_type.lower() == 'gsi':
                    self.assertTrue(plan3['~children'][0]['spans'][0]['range'][0]['low'] == 'array_min($1)')
                else:
                    self.assertTrue(plan3['~children'][0]['spans'][0]['Range']['Low'][0] == 'array_min($1)')

                self.query = 'explain select join_day from %s where join_day in  [] '  %(bucket.name)
                actual_result = self.run_cbq_query()
                plan4 = ExplainPlanHelper(actual_result)
                if self.index_type.lower() == 'gsi':
                    self.assertTrue(plan4['~children'][0]['spans'][0]['range'][0]['high']=="null")
                else:
                    self.assertTrue(plan4['~children'][0]['spans'][0]['Range']['High'][0] == "null")

                self.query = 'explain select join_day from %s where join_day in  [] and join_day is not null '  %(bucket.name)
                actual_result = self.run_cbq_query()
                plan7 = ExplainPlanHelper(actual_result)
                if self.index_type.lower() == 'gsi':
                    self.assertTrue(plan4['~children'][0]['spans'][0]['range'][0]['high']=="null")
                else:
                    self.assertTrue(plan4['~children'][0]['spans'][0]['Range']['High'][0] == "null")
                
                self.query = 'explain select join_day from %s where join_day in  [$1,$2,$3] '  %(bucket.name)
                actual_result = self.run_cbq_query()
                plan5 = ExplainPlanHelper(actual_result)
                self.assertTrue(len(plan5['~children'][0]['scan']['spans'])==3)

                self.query = 'explain select join_day from %s where join_day in  [$1,$2,$3] limit 5 '  %(bucket.name)
                actual_result = self.run_cbq_query()
                plan6 = ExplainPlanHelper(actual_result)
                self.assertTrue(len(plan6['~children'][0]['~children'][0]['scan']['spans'])==3)
                self.assertTrue( plan6["~children"][1]['expr'] == "5" and plan6["~children"][1]['#operator'] == 'Limit')

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_stable_scan(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s ( join_day )" %(idx,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                idx = "idx2"
                self.query = "CREATE INDEX %s ON %s ( meta().id )" %(idx,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.query = 'create primary index on default'
                self.run_cbq_query()
                self.query = 'select join_day from %s where join_day in [4,7,5,10] order by meta().id' %(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = 'select join_day from %s use index(`#primary`) where join_day in [4,7,5,10] order by meta().id' %(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'select join_day from %s where join_day = 4 OR join_day > 5 OR join_day < 10 order by meta().id'%(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = 'select join_day from %s use index(`#primary`) where join_day = 4 OR join_day > 5 OR join_day < 10 order by meta().id' %(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query ='select join_day from %s where join_day != 10 order by meta().id'%(bucket.name)
                actual_result = self.run_cbq_query()
                self.query ='select join_day from %s use index(`#primary`) where join_day != 10 order by meta().id'%(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'select join_day from %s where meta().id in ["query-testemployee10153.1877827-0","","query-testemployee10194.855617-0"] order by meta().id' %(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = 'select join_day from %s use index(`#primary`) where meta().id in ["query-testemployee10153.1877827-0","","query-testemployee10194.855617-0"] order by meta().id' %(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.assertTrue(actual_result['results']==[{u'join_day': 9}, {u'join_day': 4}])
                self.query = 'select join_day from %s where meta().id = "query-testemployee10231.2819054-0" OR meta().id > "" OR meta().id < "query-testemployee10317.9004497-0" order by meta().id'%(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = 'select join_day from %s use index(`#primary`) where meta().id = "query-testemployee10231.2819054-0" OR meta().id > "" OR meta().id < "query-testemployee10317.9004497-0" order by meta().id' %(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'select join_day from %s where meta().id = "query-testemployee10231.2819054-0" OR meta().id > "" OR meta().id < "query-testemployee10317.9004497-0" order by meta().id limit 10'%(bucket.name)
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==[{u'join_day': 9}, {u'join_day': 9}, {u'join_day': 9}, {u'join_day': 9}, {u'join_day': 9}, {u'join_day': 9}, {u'join_day': 4}, {u'join_day': 4}, {u'join_day': 4}, {u'join_day': 4}])
                self.query ='select join_day from %s where meta().id != "query-testemployee10231.2819054-0" order by meta().id'%(bucket.name)
                actual_result = self.run_cbq_query()
                self.query ='select join_day from %s use index(`#primary`) where meta().id != "query-testemployee10231.2819054-0" order by meta().id'%(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query ='select join_day from %s where meta().id != "query-testemployee10231.2819054-0" order by meta().id limit 10'%(bucket.name)
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==[{u'join_day': 9}, {u'join_day': 9}, {u'join_day': 9}, {u'join_day': 9}, {u'join_day': 9}, {u'join_day': 9}, {u'join_day': 4}, {u'join_day': 4}, {u'join_day': 4}, {u'join_day': 4}])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                    self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()

    def test_meta_cas(self):
         for bucket in self.buckets:
            created_indexes = []
            try:
                self.query = 'create primary index on default'
                self.run_cbq_query()
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s ( meta().cas )" %(idx,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                idx = "idx2"
                self.query = "CREATE INDEX %s ON %s ( meta().expiration )" %(idx,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                idx = "idx3"
                self.query = "CREATE INDEX %s ON %s ( meta().id, meta().cas, meta().expiration )" %(idx,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                idx = "idx4"
                self.query = "CREATE INDEX %s ON %s ( meta().id )" %(idx,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                idx = "idx5"
                self.query = "CREATE INDEX %s ON %s ( meta().cas, meta().expiration )" %(idx,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                idx = "idx6"
                self.query = "CREATE INDEX %s ON %s ( meta().cas ) where meta().cas > 1487875768758304768" %(idx,bucket.name)+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.query = 'explain SELECT  meta().id , meta().cas, meta().expiration FROM default where meta().id > ""'
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] == 'idx3')
                self.query = 'explain SELECT  meta().id , meta().cas, meta().expiration FROM default where meta().id !="query-testemployee10231.2819054-0" or meta().cas > 0 or meta().expiration = 0'
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index'] =='#primary')
                self.query = 'explain SELECT meta().cas, meta().expiration,meta().id FROM default where meta().cas = 1487875768758304768 and meta().expiration > 0'
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index'] =='idx5')
                self.query = 'explain SELECT meta().cas, meta().expiration FROM default where meta().cas !=1487875768758304768 or meta().expiration != 0'
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator']=='UnionScan')
                self.query = 'explain SELECT  meta().id  FROM default where meta().id in ["",null,"query-testemployee10231.2819054-0","query-testemployee9987.55838821-0"]'
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] == "#primary")
                self.query = 'explain SELECT  meta().cas,meta().id  FROM default where meta().cas > 1487875768758304768'
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] == "idx6")
                self.query = 'explain SELECT  meta().expiration,meta().id  FROM default where meta().expiration > 0'
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] == "idx2")
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                    self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()



    def test_suffix(self):
        for bucket in self.buckets:
            created_indexes = []
            idx = "idx"
            self.query = "CREATE INDEX %s ON %s (( DISTINCT ARRAY s FOR s IN SUFFIXES ( LOWER( _id )  )" % (
            idx, bucket.name) + \
                         " END),LOWER(_id),department ,name)  " \
                         " USING %s" % (self.index_type)
            # if self.gsi_type:
            #     self.query += " WITH {'index_type': 'memdb'}"
            actual_result = self.run_cbq_query()
            self._wait_for_index_online(bucket, idx)
            self._verify_results(actual_result['results'], [])
            created_indexes.append(idx)

            self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

            self.query = "EXPLAIN select name from %s WHERE ANY s IN SUFFIXES( LOWER( _id ) ) " % (bucket.name) + \
                         " SATISFIES s LIKE 'query-test%' END" \
                         " AND  department = 'Manager' ORDER BY name "

            self.test_explain_covering_index(idx)
            self.query = "select name from %s WHERE ANY s IN SUFFIXES( LOWER( _id ) ) " % (bucket.name) + \
                         " SATISFIES s LIKE  'query-test%'  END" \
                         " AND  department = 'Manager' ORDER BY name "

            actual_result = self.run_cbq_query()
            for idx in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                actual_result1 = self.run_cbq_query()
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
            self.query = "select name from %s  WHERE ANY s IN SUFFIXES( LOWER( _id ) ) " % (bucket.name) + \
                         " SATISFIES s LIKE  'query-test%'  END" \
                         " AND  department = 'Manager' ORDER BY name "

            expected_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'] == expected_result['results'])



    def test_simple_array_index_distinct(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY v FOR v in %s END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY x.RAM FOR x in %s END) USING %s" % (
                    idx2, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)

                self.query = "EXPLAIN select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan',
                    "Intersect Scan is not being used in and query for 2 array indexes")

                result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                self.query = "select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from %s use index (`#primary`)  where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']),sorted(expected_result['results']))
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                created_indexes.remove(idx)
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx2, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx2), "Index is in list")
                created_indexes.remove(idx2)

                idx3 = "idxjoin_yr2"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY v FOR v within %s END) USING %s" % (
                    idx3, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY x.RAM FOR x within %s END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "EXPLAIN select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result_within = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result_within)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan',
                    "Intersect Scan is not being used in and query for 2 array indexes")

                result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx3 or result1 == idx4)
                self.assertTrue(result2 == idx4 or result2 == idx3)

                self.query = "EXPLAIN select name from %s where any v within %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x within %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result_within = self.run_cbq_query()

		plan = ExplainPlanHelper(actual_result_within)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan',
                    "Intersect Scan is not being used in and query for 2 array indexes")

                result3 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                result4 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']

                self.assertTrue(result3 == idx4 or result3 == idx3)
                self.assertTrue(result4 == idx3 or result4 == idx4)
                self.query = "select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x within %s.VMs SATISFIES x.RAM between 1 and 5  END ) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                actual_result_within = self.run_cbq_query()
                self.query = "select name from %s use index (`#primary`) where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x within %s.VMs SATISFIES x.RAM between 1 and 5  END ) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result_within['results']) == sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_array_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s( ARRAY v FOR v in %s END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s( ARRAY x.RAM FOR x in %s END) USING %s" % (
                    idx2, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = "select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                created_indexes.remove(idx)
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx2, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx2), "Index is in list")
                created_indexes.remove(idx2)

                idx3 = "idxjoin_yr2"
                self.query = "CREATE INDEX %s ON %s(  ARRAY v FOR v within %s END) USING %s" % (
                    idx3, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s(  ARRAY x.RAM FOR x within %s END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")
                self.query = "select name from %s USE INDEX(%s) where " % (
                bucket.name,idx4) + \
                             "(ANY x within %s.VMs SATISFIES x.RAM between 1 and 5  END ) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                actual_result_within = self.run_cbq_query()
                self.query = "select name from %s USE INDEX(`#primary`) where " % (
                bucket.name) + \
                             "(ANY x within %s.VMs SATISFIES x.RAM between 1 and 5  END ) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result_within['results']) == sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_update_arrays(self):
        created_indexes = []
        for bucket in self.buckets:
            try:
                self.query = "UPDATE {0} SET s.newField = 'newValue' FOR s IN ARRAY_FLATTEN(tasks[*].Marketing, 1) END".format(bucket.name)
                self.run_cbq_query()
                idx = "nested_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j for j within i end) FOR i in %s END,tasks,name) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "select name from %s  WHERE ANY i IN tasks SATISFIES  (ANY j within i SATISFIES j='newValue' END) END ; " % (
                bucket.name)
                actual_result = self.run_cbq_query()
                os = self.shell.extract_remote_info().type.lower()
                if os == "windows":
                    number_of_doc = 1680
                else:
                    number_of_doc = 10079
                self.assertTrue(actual_result['metrics']['resultCount']==number_of_doc)
                self.query = "UPDATE {0} SET s.newField = 'newValue' FOR s IN ARRAY_FLATTEN (ARRAY i.Marketing FOR i IN tasks END, 1) END;".format(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select name from %s  WHERE ANY i IN tasks SATISFIES  (ANY j within i SATISFIES j='newValue' END) END ; " % (
                bucket.name)
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['metrics']['resultCount']==number_of_doc)
                self.query = "UPDATE {0} SET i.Marketing = ( ARRAY OBJECT_ADD(s, 'newField', 'test' ) FOR s IN i.Marketing END ) FOR i IN tasks END;".format(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select name from %s  WHERE ANY i IN tasks SATISFIES  (ANY j within i SATISFIES j='newValue' END) END ; " % (
                bucket.name)
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['metrics']['resultCount']==number_of_doc)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_covering_index_collections(self):
        created_indexes = []
        idx = "ix"
        idx2 = "ix2"
        try:
           for bucket in self.buckets:
            self.query = "create index {1} on {0}(x)".format(bucket.name,idx)
            actual_result =self.run_cbq_query()
            self._wait_for_index_online(bucket, idx)
            self._verify_results(actual_result['results'], [])
            created_indexes.append(idx)
            self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

            self.query = 'INSERT into %s (key , value) VALUES ("%s", %s)'%(bucket.name,"k01",'{"x":10}')
            self.run_cbq_query()
            self.query = 'explain select x1 from {0} let x1 = FIRST c for c IN [2,1,10] WHEN c = x END where x = 10 '.format(bucket.name)
            self.test_explain_covering_index(idx)
            self.query = 'select x1 from {0} let x1 = FIRST c for c IN [2,1,10] WHEN c = x END where x = 10 '.format(bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(actual_result['results']==[{u'x1': 10}])
            self.query = 'CREATE INDEX {1} ON {0}(SUBSTR(transDate,0,10),code) WHERE code != "" AND meta().id LIKE "account-customerXYZ%" '.format(bucket.name,idx2)
            actual_result = self.run_cbq_query()
            self._wait_for_index_online(bucket, idx2)
            self._verify_results(actual_result['results'], [])
            created_indexes.append(idx2)
            self.query = 'INSERT into %s (key,value) values("%s",{ "accountNumber": 123456789, "docId": "account-customerXYZ-123456789", "code": "001" , "transDate":"2016-07-02"})'%(bucket.name,"account-customerXYZ-123456789")
            self.run_cbq_query()
            self.query = 'INSERT into %s (key,value) values("%s", ' %(bucket.name,"codes-version-9") +\
                         '{ "version": 9, "docId": "codes-version-9", "codes": [{ "code": "001", "type": "P", "title": "SYSTEM W MCC", "weight": 26.2466 },' \
                         '{ "code": "166", "type": "P", "title": "SYSTEM W/O MCC", "weight": 14.6448 }] })'
            self.run_cbq_query()
            self.query = 'explain SELECT SUBSTR(account.transDate,0,10) As transDate, AVG(codes.weight) As avgWeight ' \
                         'FROM {0} account JOIN {0} codesDoc ON KEYS "codes-version-9" ' \
                         'LET codes = FIRST c for c IN codesDoc.codes WHEN c.code = account.code END ' \
                         'WHERE account.code != "" AND meta(account).id LIKE "account-customerXYZ-%" ' \
                         'AND SUBSTR(account.transDate,0,10) >= "2016-07-01" AND SUBSTR(account.transDate,0,10) < "2016-07-03"' \
                         'GROUP BY SUBSTR(account.transDate,0,10)'.format(bucket.name)
            self.run_cbq_query()
            self.test_explain_covering_index(idx2)
            self.query = 'SELECT SUBSTR(account.transDate,0,10) As transDate, AVG(codes.weight) As avgWeight ' \
                         'FROM {0} account JOIN {0} codesDoc ON KEYS "codes-version-9" ' \
                         'LET codes = FIRST c for c IN codesDoc.codes WHEN c.code = account.code END ' \
                         'WHERE account.code != "" AND meta(account).id LIKE "account-customerXYZ-%" ' \
                         'AND SUBSTR(account.transDate,0,10) >= "2016-07-01" AND SUBSTR(account.transDate,0,10) < "2016-07-03"' \
                         'GROUP BY SUBSTR(account.transDate,0,10)'.format(bucket.name)
            actual_result = self.run_cbq_query()
            print actual_result
            self.assertTrue(actual_result['results'][0]['avgWeight']== 26.2466)
            self.assertTrue(actual_result['results'][0]['transDate']=='2016-07-02')
        finally:
            for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_pushdown_complex_filter(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_simple_array_index_all(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY v FOR v in %s END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s( All ARRAY x.RAM FOR x in %s END) USING %s" % (
                    idx2, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)

                self.query = "EXPLAIN select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan',
                    "Intersect Scan is not being used in and query for 2 array indexes")

                result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                self.query = "select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                created_indexes.remove(idx)
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx2, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx2), "Index is in list")
                created_indexes.remove(idx2)

                idx3 = "idxjoin_yr2"
                self.query = "CREATE INDEX %s ON %s( all ARRAY v FOR v within %s END) USING %s" % (
                    idx3, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within %s END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "EXPLAIN select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result_within = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result_within)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan',
                    "Intersect Scan is not being used in and query for 2 array indexes")

                result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx3 or result1 == idx4)
                self.assertTrue(result2 == idx4 or result2 == idx3)


                self.query = "EXPLAIN select name from %s where any v within %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x within %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result_within = self.run_cbq_query()

		plan = ExplainPlanHelper(actual_result_within)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan',
                    "Intersect Scan is not being used in and query for 2 array indexes")

                result3 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                result4 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']

                self.assertTrue(result3 == idx4 or result3 == idx3)
                self.assertTrue(result4 == idx3 or result4 == idx4)
                self.query = "select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x within %s.VMs SATISFIES x.RAM between 1 and 5  END ) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                actual_result_within = self.run_cbq_query()
                self.query = "select name from %s use index (`#primary`) where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x within %s.VMs SATISFIES x.RAM between 1 and 5  END ) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result_within['results']) == sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_array_index_all_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s(department, DISTINCT ARRAY v FOR v in %s END,join_yr,name) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s(name,All ARRAY x.RAM FOR x in %s END,VMs) USING %s" % (
                    idx2, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)

                self.query = "EXPLAIN select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

		plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ("cover ((`%s`.`department`))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx)

                self.query = "EXPLAIN select name from %s where any join_yr in %s.join_yr satisfies join_yr = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

		plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ("cover ((`%s`.`department`))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx)

                self.query = "select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from %s use index(`#primary`) where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))

                self.query = "EXPLAIN select name from %s where (ANY x within %s.VMs SATISFIES x.RAM between 1 and 5  END ) " % (
                bucket.name, bucket.name) + \
                             "and name is not null ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

		plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ("cover ((`%s`.`name`))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == ("cover ((all (array (`x`.`RAM`) for `x` in (`%s`.`VMs`) end)))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][2]) == ("cover ((`%s`.`VMs`))"  % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][3]) == ("cover ((meta(`%s`).`id`))" % "default"))

                self.query = "EXPLAIN select name from %s where (ANY foo within %s.VMs SATISFIES foo.RAM between 1 and 5  END ) " % (
                bucket.name, bucket.name) + \
                             "and name is not null ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

		plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ("cover ((`%s`.`name`))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == ("cover ((all (array (`x`.`RAM`) for `x` in (`%s`.`VMs`) end)))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][2]) == ("cover ((`%s`.`VMs`))"  % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][3]) == ("cover ((meta(`%s`).`id`))" % "default"))

                self.query = "select name from `%s` where (ANY x within `%s`.VMs SATISFIES x.RAM between 1 and 5  END ) " % (
                bucket.name, bucket.name) + \
                             "and name is not null ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from `%s` use index(`#primary`) where (ANY x within %s.VMs SATISFIES x.RAM between 1 and 5  END ) " % (
                bucket.name, bucket.name) + \
                             "and name is not null ORDER BY name limit 10"
                expected_result = self.run_cbq_query()

                self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_simple_nested_index_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j for j in i end) FOR i in %s END,tasks,department,name) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx2"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j for j in i end) FOR i in %s END,tasks,name) USING %s" % (
                    idx2, bucket.name, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                idx3 = "nested_idx3"
                self.query = "CREATE INDEX %s ON %s(tasks,name,department) USING %s" % (
                    idx3, bucket.name, self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")

                idx4 = "nested_idx4"
                self.query = "CREATE INDEX %s ON %s(tasks,name) USING %s" % (
                    idx4, bucket.name, self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']=='nested_idx2')
                idx5 = "nested_idx5"
                self.query = ' CREATE INDEX %s ON %s((distinct (array (i.RAM) for i in VMs end))) ' %(idx5,bucket.name)+\
                             'WHERE (_id = "query-testemployee10153.1877827-0")'
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx5)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx5)

                self.query = 'explain SELECT VMs FROM %s  WHERE ANY i IN VMs SATISFIES i.RAM = 10 END AND _id="query-testemployee10153.1877827-0"' %(bucket.name)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['scan']['index']==idx5)
         #        self.query = "EXPLAIN select name from %s WHERE ANY tasks IN %s.tasks SATISFIES  (ANY j IN task SATISFIES j='Search' end) END " % (
         #        bucket.name,bucket.name) + \
         #                     "order BY name limit 10"
         #        actual_result = self.run_cbq_query()
		# plan = ExplainPlanHelper(actual_result)
         #        self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']=='nested_idx2')

         #        self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY task IN i SATISFIES task='Search' end) END " % (
         #        bucket.name,bucket.name) + \
         #                     "order BY name limit 10"
         #        actual_result = self.run_cbq_query()
		# plan = ExplainPlanHelper(actual_result)
                #self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']=='nested_idx2')

                self.query = "EXPLAIN select name from %s WHERE tasks is not missing and name is not null " % (
                bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']=='nested_idx4')

                self.query = "EXPLAIN select name from %s WHERE ANY task IN %s.tasks SATISFIES  (ANY innertask IN task SATISFIES innertask='Search' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']=='nested_idx2')

                self.query = "EXPLAIN select name from %s WHERE tasks is not missing and name is not null " % (
                bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']=='nested_idx4')



                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name,bucket.name) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ("cover ((distinct (array (distinct (array `j` for `j` in `i` end)) for `i` in (`%s`.`tasks`) end)))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == ("cover ((`%s`.`tasks`))" % bucket.name))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['scan'][0]['covers'][2]) == ("cover ((`default`.`department`)"))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['scan'][0]['covers'][3]) == ("cover ((meta(`default`).`id`))"))
                self.query = "select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name,bucket.name) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from %s use index(`#primary`) WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name,bucket.name) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                expected_result =  self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")



    def test_simple_nested_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxtasks"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY x FOR x in %s END) USING %s" % (
                    idx2, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)

                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan',
                    "Intersect Scan is not being used in and query for 2 array indexes")
                result1 =plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                result2 =plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                actual_result = self.run_cbq_query()

                self.query = "select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from %s use index(`#primary`) WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"

                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_shortest_covering_index(self):
         for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithwhere%s" % ind
                    self.query = "CREATE INDEX %s ON %s(email, VMs) where join_day > 10 USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)

                    index_name2 = "shortcoveringindex%s" % ind
                    self.query = "CREATE INDEX %s ON %s(email) where join_day > 10 USING %s" % (index_name2, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name2)
                    created_indexes.append(index_name2)
                    self.query = "explain select email,VMs[0].RAM from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "select email,join_day from %s "  % (bucket.name) +\
                                 "where email LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    actual_result1 = self.run_cbq_query()

                    self.query = "explain select email from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and join_day > 10"
                    if self.covering_index:
                        self.test_explain_covering_index(index_name2)
                    self.query = " select email from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and join_day > 10"
                    actual_result2 = self.run_cbq_query()
                    expected_result = [{"email" : doc["email"]}
                               for doc in self.full_list
                               if re.match(r'.*@.*\..*', doc['email']) and \
                                  doc['join_day'] > 10]
                    #self._verify_results(actual_result['results'], expected_result)

            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()
                    self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()
                    self.sleep(15,'wait for index')
                    self.query = "select email,join_day from %s use index(`#primary`) where email "  % (bucket.name) +\
                                  "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    result = self.run_cbq_query()
                    self.assertEqual(sorted(actual_result1['results']),sorted(result['results']))
                    self.query = " select email from %s use index(`#primary`) where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and join_day > 10"
                    result = self.run_cbq_query()
                    self.assertEqual(sorted(actual_result2['results']),sorted(result['results']))
                    self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()


    def test_covering_nonarray_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxtasks"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY x FOR x in %s END) USING %s" % (
                    idx2, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)

                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                idx3 = "idxtasks0"
                self.query = "CREATE INDEX %s ON %s(tasks[1],department) USING %s" % (
                    idx3, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx3)

                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")

                idx4 = "idxMarketing"
                self.query = "CREATE INDEX %s ON %s(tasks[0].Marketing,department) USING %s" % (
                    idx4, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")


                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan',
                    "Intersect Scan is not being used in and query for 2 array indexes")
                result1 =plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                result2 =plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                actual_result = self.run_cbq_query()

                self.query = "explain select department from %s WHERE  tasks[1]='Sales'" % (
                bucket.name) + \
                             " AND  NOT (department = 'Manager') limit 10"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']==idx3)
                self.assertTrue("cover" in str(plan))

                self.query = "select meta().id from %s WHERE  tasks[1]='Sales'" % (
                bucket.name) + \
                             " AND  NOT (department = 'Manager') order by department limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from %s use index(`#primary`) WHERE  tasks[1]='Sales'" % (
                bucket.name) + \
                             " AND  NOT (department = 'Manager') order by department limit 10"

                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))

                str1 = [{"region2": "International","region1": "South"},{"region2": "South"}]
                self.query = "explain select meta().id from {0} WHERE  tasks[0].Marketing={1}".format(bucket.name,str1) + \
                             "AND  NOT (department = 'Manager') order BY meta().id limit 10"
                actual_result = self.run_cbq_query()

                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']==idx4)
                self.assertTrue("cover" in str(plan))

                #str = [{"region2": "International","region1": "South"},{"region2": "South"}]
                self.query = "select meta().id from {0} WHERE  tasks[0].Marketing={1}".format(bucket.name,str1) + \
                             " AND  NOT (department = 'Manager') order BY  meta().id  limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select meta().id from {0} use index(`#primary`) WHERE  tasks[0].Marketing={1}".format(bucket.name,str1) + \
                             " AND  NOT (department = 'Manager') order BY meta().id limit 10"

                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")



    def test_simple_unnest_index_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "unnest_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( ALL array j for j in i end) FOR i in %s END,department,tasks,name) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select %s.name from %s UNNEST tasks as i UNNEST i as j WHERE j = 'Search' " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)

                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)

                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ("cover ((distinct (array (all (array `j` for `j` in `i` end)) for `i` in (`default`.`tasks`) end)))"))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['scan'][0]['covers'][1]) == ("cover ((`default`.`department`))"))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][2]) == ("cover ((`default`.`tasks`))" ))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][3]) == ("cover ((`default`.`name`))" ))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['scan'][0]['covers'][4]) == ("cover ((meta(`default`).`id`))" ))


                self.query = "select %s.name from %s  UNNEST tasks as i UNNEST i as j WHERE j = 'Search'  " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select %s.name from %s use index (`#primary`)  UNNEST tasks as i UNNEST i as j WHERE j = 'Search'  " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)
                expected_result = self.run_cbq_query()

                self.assertEqual(sorted(actual_result['results']),sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")





    def test_simple_unnest_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "unnest_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( ALL array j for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select %s.name from %s UNNEST tasks as i UNNEST i as j WHERE j = 'Search' " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)
                # self.query = "EXPLAIN select %s.name from %s UNNEST tasks as i UNNEST i as j WHERE j = 'Search' " % (
                #  bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                # self.assertTrue(
                #     plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan',
                #     "Intersect Scan is not being used in and query for 2 array indexes")
                result1 =plan['~children'][0]['~children'][0]['scan']['index']

                #result2 =plan['~children'][0]['~children'][0]['scans'][1]['scans'][0]['index']
                self.assertTrue(result1 == idx )
                #self.assertTrue(result2 == idx or result2 == idx2)
                self.query = "select %s.name from %s  UNNEST tasks as i UNNEST i as j WHERE j = 'Search'  " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select %s.name from %s use index (`#primary`)  UNNEST tasks as i UNNEST i as j WHERE j = 'Search'  " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)
                expected_result = self.run_cbq_query()

                self.assertEqual(sorted(actual_result['results']),sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_join_unnest_alias(self):
        for bucket in self.buckets:
            created_indexes=[]
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within %s END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                # idx5 = "idxVM3"
                # self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.os FOR x within %s END) USING %s" % (
                #     idx5, bucket.name, "VMs", self.index_type)
                # # if self.gsi_type:
                # #     self.query += " WITH {'index_type': 'memdb'}"
                # create_result = self.run_cbq_query()
                # self._wait_for_index_online(bucket, idx5)
                # self._verify_results(create_result['results'], [])
                # created_indexes.append(idx5)

                #self.assertTrue(self._is_index_in_list(bucket, idx5), "Index is not in list")

                self.query = "explain SELECT x FROM default emp1 USE INDEX(%s)  UNNEST emp1.VMs as x  JOIN default task ON KEYS meta(`emp1`).id where x.RAM > 1 and x.RAM < 5  ;" %(idx4)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                result1 =plan['~children'][0]['scan']['index']
                self.assertTrue(result1==idx4)
                self.query = "SELECT x FROM default emp1 USE INDEX(%s) UNNEST emp1.VMs as x JOIN default task ON KEYS meta(`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;"%(idx4)
                actual_result = self.run_cbq_query()
                self.query = "SELECT x FROM default emp1 USE INDEX(`#primary`)  UNNEST emp1.VMs as x  JOIN default task ON KEYS meta(`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) ==sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_join_unnest_alias_covering(self):
        for bucket in self.buckets:
            created_indexes=[]
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within %s END,VMs) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "explain SELECT x FROM default emp1 USE INDEX(%s)  UNNEST emp1.VMs as x  JOIN default task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5   ;" %(idx4)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))


                self.query = "SELECT x FROM default emp1 USE INDEX(%s) UNNEST emp1.VMs as x JOIN default task ON KEYS meta(`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;"%(idx4)
                actual_result = self.run_cbq_query()
                self.query = "SELECT x FROM default emp1 USE INDEX(`#primary`)  UNNEST emp1.VMs as x  JOIN default task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5 ;"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) ==sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")





    def test_unnest_multilevel_attribute(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr_nest"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")


                self.query = "explain SELECT emp.name FROM %s emp  UNNEST emp.tasks as i UNNEST i.Marketing as j where j.region1 = 'South'" % (bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from %s USE INDEX(`#primary`) WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results'])==sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_unnest_multilevel_attribute_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr_nest"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in %s END,tasks,name) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")


                self.query = "explain SELECT emp.name FROM %s emp  UNNEST emp.tasks as i UNNEST i.Marketing as j where j.region1 = 'South'" % (bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                print plan
                self.assertTrue("covers" in str(plan))
                self.query = "select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from %s USE INDEX(`#primary`) WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results'])==sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( All array j.region1 for j in i.Marketing end) FOR i in %s END) USING %s" % (
                    idx2, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                idx3 = "nested_idx_attr3"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in %s END) USING %s" % (
                    idx3, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "nested_idx_attr4"
                self.query = "CREATE INDEX %s ON %s(DISTINCT ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in %s END,name,tasks) USING %s" % (
                    idx4, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END and name is not null " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)
                self.query = "select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result1 = self.run_cbq_query()
                self.query = "select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result2 = self.run_cbq_query()

                self.query = "select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result3 = self.run_cbq_query()

                self.query = "select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result4 = self.run_cbq_query()


                self.query = "select name from %s use index (`#primary`) WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result5 = self.run_cbq_query()

                self.assertTrue(sorted(actual_result1['results'])==sorted(actual_result2['results']))
                self.assertTrue(sorted(actual_result2['results'])==sorted(actual_result3['results']))
                self.assertTrue(sorted(actual_result4['results'])==sorted(actual_result3['results']))
                self.assertTrue(sorted(actual_result4['results'])==sorted(actual_result5['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_index_within(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY i FOR i within %s END) USING %s" % (
                    idx, bucket.name, "hobbies", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY i FOR i within %s END) USING %s" % (
                    idx2, bucket.name, "hobbies", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                self.query = "EXPLAIN select name from %s use index(%s) WHERE ANY i within %s.hobbies SATISFIES i = 'bhangra' END " % (
                bucket.name,idx2,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)

                self.query = "EXPLAIN select name from %s use index(%s) WHERE ANY i within %s.hobbies SATISFIES i = 'bhangra' END " % (
                bucket.name,idx,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "select name from %s use index(%s) WHERE ANY i within %s.hobbies SATISFIES i = 'bhangra' END " % (
                bucket.name,idx,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result1 = sorted(actual_result['results'])

                self.query = "select name from %s use index(%s) WHERE ANY i within %s.hobbies SATISFIES i = 'bhangra' END " % (
                bucket.name,idx2,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result2 = sorted(actual_result['results'])

                self.query = "select name from %s use index(`#primary`) WHERE ANY i within %s.hobbies SATISFIES i = 'bhangra' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result3 = sorted(actual_result['results'])


                self.assertTrue(actual_result1 ==  actual_result2 == actual_result3)

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_array_index_in_distinct(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j for j in i.dance end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                actual_result = self.run_cbq_query()
                self.query = "select name from %s WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s USE INDEX(`#primary`) WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_nested_attr_array_index_in_all(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( all array j for j in i.dance end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "select name from %s WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s USE INDEX(`#primary`) WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_nested_attr_array_index_in_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( all array j for j in i.dance end) FOR i in %s END,hobbies.hobby,department,name) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)

                print plan
                self.assertTrue("covers" in str(plan))
                self.query = "select name from %s WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s USE INDEX(`#primary`) WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx),"Index is in list")

    def test_array_partial_index_distinct(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( distinct array i FOR i in %s END) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from %s WHERE department = 'Support' and ANY i IN %s.hobbies.hobby SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s use index (`#primary`) WHERE department = 'Support' and ANY i IN %s.hobbies.hobby SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_partial_index_distinct_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( distinct array i FOR i in %s END,hobbies.hobby,name) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                print plan
                self.assertTrue("covers" in str(plan))
                self.query = "select name from %s WHERE department = 'Support' and ANY i IN %s.hobbies.hobby SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s use index (`#primary`) WHERE department = 'Support' and ANY i IN %s.hobbies.hobby SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_indexcountscan(self):
        for bucket in self.buckets:
                created_indexes = []
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(%s) " %(idx,bucket.name,'meta().id')+\
                             " USING %s" % (self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select count(1) from %s WHERE meta().id like '%s' " %(bucket.name,'query-test%')

                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                result1 = plan['~children'][0]['index']
                self.assertTrue(result1 == idx)
                self.query = "select count(1) from %s WHERE meta().id like '%s' " %(bucket.name,'query-test%')
                actual_result = self.run_cbq_query()
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'IndexCountScan',
                    "IndexCountScan is not being used")
                self.query = "explain select a.cnt from (select count(1) from default where meta().id is not null) as a"
                actual_result2 = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result2)
                self.assertTrue(
                    plan['~children'][0]['#operator'] != 'IndexCountScan',
                    "IndexCountScan should not be used in subquery")
                self.query = "select a.cnt from (select count(1) from default where meta().id is not null) as a"
                actual_result2 = self.run_cbq_query()
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                    self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()
                    self.sleep(15,'wait for index')
                    self.query = "select count(1) from %s use index(`#primary`) WHERE meta().id like '%s'  " %(bucket.name,'query-test%')
                    result = self.run_cbq_query()
                    self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
                    self.query = "select a.cnt from (select count(1) from %s where _id is not null) as a " %(bucket.name)
                    result = self.run_cbq_query()
                    self.assertEqual(sorted(actual_result2['results']),sorted(result['results']))
                    self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()


    def test_partial_like_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(%s) " %(idx,bucket.name,'_id')+\
                             " where _id like '%s' " %('query-test%') + \
                             " USING %s" % (self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select meta().id from %s WHERE _id like '%s' " %(bucket.name,'query-test%')

                self.test_explain_covering_index(idx)

                self.query = "EXPLAIN select meta().id from %s WHERE _id like '%s' " %(bucket.name,'query-testemployee10%')

                self.test_explain_covering_index(idx)
                self.query = "select meta().id from %s WHERE _id like '%s' " %(bucket.name,'query-testemployee10%')

                actual_result = self.run_cbq_query()
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                    self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()
                    self.sleep(15,'wait for index')
                    self.query = "select meta().id from %s use index(`#primary`) WHERE _id like '%s' " %(bucket.name,'query-testemployee10%')
                    result = self.run_cbq_query()
                    self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
                    self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()

    def test_dynamic_names(self):
            self.query = "CREATE PRIMARY INDEX ON %s" % "default"
            self.run_cbq_query()
            self.sleep(15,'wait for index')
            self.query = 'select { UPPER("foo"):1,"foo"||"bar":2 }'
            actual_result = self.run_cbq_query()
            expected_result = [{u'$1': {u'foobar': 2, u'FOO': 1}}]
            self.assertTrue(actual_result['results']==expected_result)
            self.query = 'insert into {0} (key k,value doc)  select to_string(name)|| UUID() as k , doc as doc from {0} where name is not null'.format("default")
            self.run_cbq_query()
            self.query = 'select * from {0}'.format("default")
            actual_result = self.run_cbq_query()
            print "result count should be {0}".format(actual_result['metrics']['resultCount'])
            self.assertTrue(actual_result['metrics']['resultCount']==24196)
            self.query = 'delete from {0} where meta().id in (select RAW to_string(name)|| UUID()  from {0} d)'.format("default")
            self.run_cbq_query()
            self.query = "DROP PRIMARY INDEX ON %s" % "default"
            self.run_cbq_query()

    def test_between_spans(self):
        for bucket in self.buckets:
            self.query = 'create index ix5 on %s(x)' % bucket.name
            self.run_cbq_query()
            self.query = 'insert into %s (KEY, VALUE) VALUES ("kk02",{"x":100,"y":101,"z":102,"id":"kk02"})'%(bucket.name)
            self.run_cbq_query()
            self.query = 'explain select count(1) from %s where x BETWEEN $1 AND $2' %(bucket.name)
            actual_result = self.run_cbq_query()
            plan =  ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['#operator']=='IndexCountScan')
            self.query = 'explain select count(1) from %s where x >= $1 AND x <= $2'%(bucket.name)
            actual_result = self.run_cbq_query()
            plan =  ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['#operator']=='IndexCountScan')
            self.query = 'drop index %s.ix5' %bucket.name
            self.run_cbq_query()
            self.query = 'delete from %s use keys ["kk02"]'%(bucket.name)
            self.run_cbq_query()

    def test_and_every_array_index(self):
        for bucket in self.buckets:
            self.query = 'CREATE INDEX `rec1-1_record_by_index_map` ON %s((distinct (array `i` for `i` in object_pairs(`indexMap`) end)))'%bucket.name
            self.run_cbq_query()
            self.query = 'CREATE INDEX `rec1-2_record_by_index_map` ON %s((distinct (array `i` for `i` in object_pairs(`data`) end)))'%bucket.name
            self.run_cbq_query()
            self.query = 'CREATE INDEX `rec1-3_record_by_index_map` ON %s((distinct (array `j` for `j` in object_pairs(`data`) end)))'%bucket.name
            self.run_cbq_query()
            self.query = 'insert into %s (KEY, VALUE) VALUES ("test",{"type":"testType","indexMap":{"key1":"val1", "key2":"val2"},"data":{"foo":"bar"}})'%(bucket.name)
            self.run_cbq_query()
            self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "value":"val1"} end AND any i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
            actual_result = self.run_cbq_query()
            plan = ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan')
            self.assertTrue(plan['~children'][0]['~children'][0]['scans'][0]['index']=='rec1-1_record_by_index_map')
            self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND any i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(sorted(actual_result['results'][0]['doc']) == ([u'data', u'indexMap', u'type']))
            self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE every i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND any i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
            actual_result = self.run_cbq_query()
            plan = ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan')
            self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']=='rec1-1_record_by_index_map')
            self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE every i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end or any i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
            actual_result=self.run_cbq_query()
            self.assertTrue(sorted(actual_result['results'][0]['doc']) == ([u'data', u'indexMap', u'type']))
            self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND every i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
            actual_result = self.run_cbq_query()
            plan = ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan')
            self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']=='rec1-1_record_by_index_map')
            self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end or every i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(sorted(actual_result['results'][0]['doc']) == ([u'data', u'indexMap', u'type']))
            self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE some and every i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND some and every i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
            actual_result = self.run_cbq_query()
            plan = ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan')
            self.assertTrue(plan['~children'][0]['~children'][0]['scans'][0]['index']=='rec1-1_record_by_index_map')
            self.query = 'insert into %s (KEY, VALUE) VALUES ("test1",{"type":"testType","indexMap":{"key1":"val1"},"data":{"foo":"bar"}})'%(bucket.name)
            self.run_cbq_query()
            self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE SOME AND EVERY i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND SOME AND EVERY i in object_pairs(data) satisfies i = { "name":"foo", "val":"bar"} end LIMIT 100'%(bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(sorted(actual_result['results'][0]['doc']) == ([u'data', u'indexMap', u'type']))
            self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND any j in object_pairs(indexMap) satisfies j = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
            actual_result = self.run_cbq_query()
            plan = ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan')
            self.assertTrue(plan['~children'][0]['~children'][0]['scans'][0]['index'] == 'rec1-1_record_by_index_map')
            self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND any j in object_pairs(indexMap) satisfies j = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
            actual_result = self.run_cbq_query()
            self.assertTrue(sorted(actual_result['results'][0]['doc']) == ([u'data', u'indexMap', u'type']))
            self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE some and every i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND some and every j in object_pairs(indexMap) satisfies j = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
            actual_result = self.run_cbq_query()
            plan = ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan')
            self.assertTrue(plan['~children'][0]['~children'][0]['scans'][0]['index'] == 'rec1-1_record_by_index_map')
            self.query = 'delete from %s use keys["test","test1"]'%bucket.name
            self.run_cbq_query()


    def test_distinct_raw_orderby(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(%s,%s) " %(idx,bucket.name,'name', 'join_day')+\
                             " USING %s" % (self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select distinct raw join_day from %s WHERE name like '%s' order by join_day limit 10" %(bucket.name,'query-test%')

                self.test_explain_covering_index(idx)
                self.query = "select distinct raw join_day from %s WHERE name like '%s' order by join_day limit 10 " %(bucket.name,'employee%')

                actual_result = self.run_cbq_query()
                self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()
                self.sleep(15,'wait for index')
                self.query = "select distinct raw join_day from %s use index(`#primary`) WHERE name like '%s' order by join_day limit 10 " %(bucket.name,'employee%')

                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'],expected_result['results'])
                self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_array_partial_index_all(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( all array i FOR i in %s END) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from %s WHERE department = 'Support' and ANY i IN %s.hobbies.hobby SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s WHERE department = 'Support' and ANY i IN %s.hobbies.hobby SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_with_inner_joins(self):
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "nested_inner_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
             finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_with_inner_joins_covering(self):
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "nested_inner_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in %s END,address,department) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT employee.department new_project " +\
                "FROM %s as employee  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                print plan
                self.assertTrue("covers" in str(plan))
             finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_array_index_left_outer_join(self):
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "outer_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "outer_join_all"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( All array j.city for j in i end) FOR i in %s END) USING %s" % (
                    idx2, bucket.name, "address", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee  left JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx or result1 == idx2)
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee use index (%s) left JOIN default as new_project_full " % (bucket.name,idx2) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.query = "SELECT new_project_full.department new_project " +\
                "FROM %s as employee left JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result1 = (actual_result['results'])
                self.query = "SELECT new_project_full.department new_project " +\
                "FROM %s as employee left JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result2 = (actual_result['results'])
                self.query = "SELECT new_project_full.department new_project " +\
                "FROM %s as employee use index (`#primary`) left JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result3 = (actual_result['results'])
                self.assertTrue(actual_result1==actual_result3)
                self.assertTrue(actual_result2==actual_result3)
             finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_with_inner_joins_covering(self):
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "nested_inner_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in %s END,address,department) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT employee.department new_project " +\
                "FROM %s as employee  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "SELECT employee.department new_project " +\
                "FROM %s as employee  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                self.query = "SELECT employee.department new_project " +\
                "FROM %s as employee use index (`#primary`)  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                expected_result = self.run_cbq_query()
                expected_result = expected_result['results']
                self.assertTrue(sorted(expected_result)==sorted(actual_result['results']))

             finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_regexp(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "iregex"
                self.query = " CREATE INDEX %s ON %s( DISTINCT ARRAY REGEXP_LIKE(v.os,%s)  FOR v IN VMs END )  USING %s" % (
                  idx, bucket.name,"'ub%'", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select * from %s use index(`#primary`)  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1  END  " % (
                bucket.name,"'ub%'") + \
                             "order BY name limit 10"
                self.query = "select * from %s  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])

                self.assertTrue(expected_result==sorted(actual_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_regexp_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "iregex"
                self.query = " CREATE INDEX %s ON %s( DISTINCT ARRAY REGEXP_LIKE(v.os,%s)  FOR v IN VMs END,VMs )  USING %s" % (
                  idx, bucket.name,"'ub%'", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select VMs from %s  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + \
                             "limit 10"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                print plan

                self.assertTrue("covers" in str(plan))

                self.query = "select VMs from %s  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + \
                             "limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select VMs from %s use index(`#primary`)  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1  END  " % (
                bucket.name,"'ub%'") + \
                             "limit 10"

                expected_result = self.run_cbq_query()
                expected_result = expected_result['results']

                self.assertTrue(sorted(expected_result)==sorted(actual_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_greatest(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX %s ON %s(department, DISTINCT ARRAY GREATEST(v.RAM,100)  FOR v IN VMs END )  USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE department = 'Support' and ANY v IN VMs SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from %s WHERE department = 'Support' and ANY v IN VMs SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                actual_result = actual_result['results']
                self.query = "select name from %s USE index(`#primary`) WHERE department = 'Support' and ANY v IN VMs SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                expected_result = self.run_cbq_query()
                expected_result = expected_result['results']
                self.assertTrue(sorted(expected_result)==sorted(actual_result))

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_greatest_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX %s ON %s(department, DISTINCT ARRAY GREATEST(v.RAM,100)  FOR v IN VMs END ,VMs,name)  USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE department = 'Support' and ANY v IN VMs SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                print plan
                self.assertTrue("covers" in str(plan))
                self.query = "select name from %s WHERE department = 'Support' and ANY v IN VMs SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                actual_result = actual_result['results']
                self.query = "select name from %s USE index(`#primary`) WHERE department = 'Support' and ANY v IN VMs SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                expected_result = self.run_cbq_query()
                expected_result = expected_result['results']
                self.assertTrue(sorted(expected_result)==sorted(actual_result))

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_array_index_nest_keys_where(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in department end) USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.department SATISFIES  j = 'Support' end;"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.department SATISFIES  j = 'Support' end;"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select emp.name " + \
                         "FROM %s emp USE INDEX(`#primary`) NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.department SATISFIES  j = 'Support' end;"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result == expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_nest_keys_where_not_equal(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in department end) USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + \
                         "FROM %s emp NEST %s VMs " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.department SATISFIES  j != 'Engineer' end;"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + \
                         "FROM %s emp NEST %s VMs " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.department SATISFIES  j = 'Support' end;"
                actual_result = self.run_cbq_query()
                actual_result = actual_result['results']
                self.query = "select emp.name " + \
                         "FROM %s emp USE INDEX(`#primary`)   NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id where  ANY j IN emp.department SATISFIES  j = 'Support' end;"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(sorted(actual_result) == expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_nest_keys_where_between(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in join_yr end,name,join_yr ) USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxbetween"
                self.query = "CREATE INDEX %s ON %s(name) where join_yr between 2010 and 2012 USING %s" % (
                    idx2, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                self.query = "explain select name from %s where join_yr between 2010 and 2012 and name is not missing"%(bucket.name)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['filter_covers']=={u'cover (((`default`.`join_yr`) between 2010 and 2012))': True, u'cover (((`default`.`join_yr`) <= 2012))': True, u'cover ((2010 <= (`default`.`join_yr`)))': True})
                self.query = "select name from %s where join_yr between 2010 and 2012 and name is not missing"%(bucket.name)
                actual_result1 = self.run_cbq_query()

                self.query = "explain select name from %s where join_yr >= 2010 and join_yr <=2012 and name is not null"%(bucket.name)
                actual_result = self.run_cbq_query()

                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['filter_covers']=={u'cover (((`default`.`join_yr`) between 2010 and 2012))': True, u'cover (((`default`.`join_yr`) <= 2012))': True, u'cover ((2010 <= (`default`.`join_yr`)))': True})
                self.query = "select name from %s where join_yr >= 2010 and join_yr <=2012 and name is not null"%(bucket.name)
                actual_result2 = self.run_cbq_query()

                self.assertTrue(sorted(actual_result1['results'])==sorted(actual_result2['results']))



                self.query = "EXPLAIN select emp.name " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.join_yr SATISFIES  j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Distinct Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where ANY j IN emp.join_yr SATISFIES  j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select emp.name " + \
                         "FROM %s emp USE INDEX(`#primary`)  NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where ANY j IN emp.join_yr SATISFIES  j between 2010 and 2012 end;"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result == expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")



    def test_nest_keys_where_between_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in join_yr end,name,join_yr) USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.join_yr SATISFIES  j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.department SATISFIES  j = 'Support' end;"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select emp.name " + \
                         "FROM %s emp USE INDEX(`#primary`)  NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.department SATISFIES  j = 'Support' end;"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result == expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nest_keys_where_less_more_equal(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in join_yr end) USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name, ARRAY item.department FOR item in items end departments " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.join_yr SATISFIES  j <= 2014 and j >= 2012 end;"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name, ARRAY item.department FOR item in items end departments " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where ANY j IN emp.join_yr SATISFIES  j <= 2014 and j >= 2012 end;"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select emp.name, ARRAY item.department FOR item in items end departments " + \
                         "FROM %s emp USE INDEX(`#primary`)  NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where ANY j IN emp.join_yr SATISFIES  j <= 2014 and j >= 2012 end;"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result == expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")



    def test_array_index_sum(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "indexwitharraysum%s" % ind
                    self.query = "CREATE INDEX %s ON %s(department, DISTINCT ARRAY round(v.memory + v.RAM) FOR v in VMs END ) where join_yr=2012 USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "EXPLAIN SELECT count(name),department" + \
                                 " FROM %s where join_yr=2012 AND ANY v IN VMs SATISFIES round(v.memory+v.RAM)<100 END AND department = 'Engineer'  GROUP BY department" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    plan = ExplainPlanHelper(actual_result)
                    self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                    result1 = plan['~children'][0]['scan']['index']
                    self.assertTrue(result1 == index_name)
                    self.query = "SELECT count(name),department" + \
                                 " FROM %s where join_yr=2012 AND ANY v IN VMs SATISFIES round(v.memory+v.RAM)<100 END AND department = 'Engineer'  GROUP BY department" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    actual_result = sorted(actual_result['results'])
                    self.query = "SELECT count(name),department" + \
                                 " FROM %s use index(`#primary`) where join_yr=2012 AND ANY v IN VMs SATISFIES round(v.memory+v.RAM)<100 END AND department = 'Engineer'  GROUP BY department" % (bucket.name)
                    expected_result = self.run_cbq_query()
                    expected_result = sorted(expected_result['results'])
                    self.assertTrue(actual_result == expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_array_index_sum_non_leading_key(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "indexwitharraysum%s" % ind
                    self.query = "CREATE INDEX %s ON %s(department,join_yr, DISTINCT ARRAY round(v.memory + v.RAM) FOR v in VMs END ) where join_yr=2012 USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "EXPLAIN SELECT count(department)" + \
                                 " FROM %s where join_yr=2012 AND department = 'Engineer'  GROUP BY department" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    plan = ExplainPlanHelper(actual_result)
                    print plan
                    self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "IndexScan is not being used")
                    result1 = plan['~children'][0]['scan']['index']
                    self.assertTrue(result1 == index_name)
                    self.query = "SELECT count(department)" + \
                                 " FROM %s where join_yr=2012 AND department = 'Engineer'  GROUP BY department" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    self.query = "SELECT count(department)" + \
                                 " FROM %s use index(`#primary`) where join_yr=2012 AND department = 'Engineer'  GROUP BY department" % (bucket.name)
                    expected_result = self.run_cbq_query()
                    self.assertTrue(sorted(actual_result['results']),sorted(expected_result['results']))
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_array_index_substring(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxsubstr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY SUBSTR(j.FirstName,8) for j in name end) USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name, SUBSTR(name.FirstName, 8) as firstname from %s  where ANY j IN name SATISFIES substr(j.FirstName,8) != 'employee' end" % (bucket.name)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_update(self):
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "arrayidx_update"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                updated_value = 'new_dept' * 30
                self.query = "EXPLAIN update %s set name=%s where ANY i IN address SATISFIES  (ANY j IN i SATISFIES j.city='Mumbai' end) END  returning element department"  % (bucket.name, updated_value)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "update %s set department=%s where ANY i IN address SATISFIES  (ANY j IN i SATISFIES j.city='Mumbai' end) END  returning element department"  % (bucket.name, updated_value)
                self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
             finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_delete(self):
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "arrayidx_delete"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY v FOR v in %s END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = 'select count(*) as actual from %s where join_yr=2012'  % (bucket.name)
                self.run_cbq_query()
                self.sleep(5, 'wait for index')
                actual_result = self.run_cbq_query()
                current_docs = actual_result['results'][0]['actual']
                self.query = 'EXPLAIN delete from %s where any v in join_yr SATISFIES v=2012 end LIMIT 1'  % (bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                result = plan['~children'][0]['scan']['index']
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used in and query for 2 array indexes")
                self.assertTrue(result == idx )
                self.query = 'delete from %s where any v in join_yr satisfies v=2012 end LIMIT 1'  % (bucket.name)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

             finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_create_delete_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "my_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING GSI" % (
                                            view_name, bucket.name, ','.join(self.FIELDS_TO_INDEX[ind - 1]))
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self.assertTrue(self._is_index_in_list(bucket, view_name), "Index is not in list")
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, view_name)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, view_name), "Index is in list")

    def test_create_delete_index_with_query(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "tuq_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s)  USING GSI" % (view_name, bucket.name, ','.join(self.FIELDS_TO_INDEX[ind - 1]))
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self.query = "select %s from %s where %s is not null and %s is not null" % (','.join(self.FIELDS_TO_INDEX[ind - 1]), bucket.name,
                                                                                                self.FIELDS_TO_INDEX[ind - 1][0], self.FIELDS_TO_INDEX[ind - 1][1])
                    actual_result = self.run_cbq_query()
                    self.assertTrue(len(actual_result['results']), self.num_items)
            except Exception, ex:
                content = self.cluster.query_view(self.master, "ddl_%s" % view_name, view_name, {"stale" : "ok"},
                                                  bucket="default", retry_time=1)
                self.log.info("Generated view has %s items" % len(content['rows']))
                raise ex
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, view_name)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                self.query = "select %s from %s where %s is not null and %s is not null" % (','.join(self.FIELDS_TO_INDEX[ind - 1]), bucket.name,
                                                                                                self.FIELDS_TO_INDEX[ind - 1][0], self.FIELDS_TO_INDEX[ind - 1][1])
                actual_result = self.run_cbq_query(query_params={'scan_consistency' : 'statement_plus'})
                self.assertTrue(len(actual_result['results']), self.num_items)

    def test_create_delete_index_with_query_loop(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    idx_name = "tuq_index1%s" % ind
                    self.query = "CREATE INDEX %s ON %s(a)  USING GSI" % (idx_name, bucket.name)
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, idx_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(idx_name)
                    idx_name = "tuq_index2%s" % ind
                    self.query = "CREATE INDEX %s ON %s(b)  USING GSI" % (idx_name, bucket.name)
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, idx_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(idx_name)
                    self.query = "CREATE INDEX `a1s_idx_search_legacy` ON `default`(`_class`,`typeRef`,(`fields`.`accountNumber`),`createdDate`) USING GSI"
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, "a1s_idx_search_legacy")
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append('a1s_idx_search_legacy')
                    self.query = "create index `idx_class_name` on `default`(`_class`,`name`) USING GSI "
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, "idx_class_name")
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append("idx_class_name")

                    self.query = "create index `idx_class` on `default`(`_class`) USING GSI"
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, "idx_class")
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append("idx_class")


                    for i in range(0,100):
                        self.query = "select * from %s where b is not missing and a is not missing " %(bucket.name)
                        actual_result = self.run_cbq_query()
                        print actual_result['results']
                        self.query = "delete from %s where a is not missing and b is not missing" %(bucket.name)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("%s", {"_class":"com.comcast.esp.unifiednotes.entity.TypeDefinition", "name":"MemoBillingNote", "c":"pq"})' % (bucket.name,i+1)

                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("%s", {"_class":"com.comcast.esp.unifiednotes.entity.UNote", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f", "fields.accountNumber":"8771300052225673"},"createdDate":1456790401005})' % (bucket.name,i+2)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("3", {"_class":"com.comcast.esp.unifiednotes.entity.UNote", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f", "fields.accountNumber":"8771300052225673"})' % (bucket.name,i+3)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("4", {"_class":"com.comcast.esp.unifiednotes.entity.UNote", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f", "fields.accountNumber":"8771300052225673"})' % (bucket.name,i+4)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("5", {"_class":"com.comcast.esp.unifiednotes.entity.UNote", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f", "fields.accountNumber":"8771300052225673","createdDate":1456790401004})' % (bucket.name,i+5)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("6", {"_class":"com.comcast.esp.unifiednotes.entity.UNote", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f", "fields.accountNumber":"8771300052225673","createdDate":1456790401003})' % (bucket.name,i+6)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("7", {"_class":"com.comcast.esp.unifiednotes.entity.TypeDefinition", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f", "fields.accountNumber":"8771300052225673","createdDate":1456790401002})' % (bucket.name,i+7)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("8", {"_class":"com.comcast.esp.unifiednotes.entity.TypeDefinition", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f", "fields.accountNumber":"8771300052225673","createdDate":1456790401001})' % (bucket.name,i+8)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("9", {"_class":"com.comcast.esp.unifiednotes.entity.TypeDefinition", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f", "fields.accountNumber":"8771300052225673","createdDate":1456790401001})' % (bucket.name,i+9)
                        self.run_cbq_query()
                        self.query = 'explain select * from `default` where _class="com.comcast.esp.unifiednotes.entity.TypeDefinition" ' \
                                     'and name="MemoBillingNote" limit 1'

                        actual_result = self.run_cbq_query()
                        self.query = 'EXPLAIN SELECT * FROM `default` WHERE _class = "com.comcast.esp.unifiednotes.entity.UNote" AND typeRef = "dd833af2-405e-11e5-a151-feff819cdc9f" AND createdDate BETWEEN 1456790401000 AND 1490897416000 AND fields.accountNumber = "8771300052225673" '
                        self.run_cbq_query()
                        self.query = 'delete from %s where b = 1' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'select * from %s where a = "m"' % (bucket.name)
                        self.run_cbq_query()

                        self.query = 'explain select * from `default` where _class="com.comcast.esp.unifiednotes.entity.TypeDefinition" order by acs'
                        self.run_cbq_query()
                        self.query = 'upsert into %s(KEY, VALUE) VALUES ("1", {"a":"m", "b":1, "c":"pq"})' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'select * from %s where a = "m"' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'upsert into %s(KEY, VALUE) VALUES ("1", {"a":"k", "b":2, "c":"pq"})' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'upsert into %s(KEY, VALUE) VALUES ("1", {"a":"k", "b":1, "c":"pq"})' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'select * from %s where a = "k"' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("3", {"a":"k", "b":2, "c":"pq"})' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("4", {"a":"k", "b":1, "c":"pq"})' % (bucket.name)
                        self.run_cbq_query()

                        self.query = 'insert into %s(KEY, VALUE) VALUES ("5", {"a":"k", "b":2, "c":"pq"})' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'select * from %s where b = 2' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'delete from %s where b = 2' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("5", {"a":"k", "b":1, "c":"pq"})' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'insert into %s(KEY, VALUE) VALUES ("6", {"a":"k", "b":2, "c":"pq"})' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'select * from %s where a = "k"' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'delete from %s where b = 1' % (bucket.name)
                        self.run_cbq_query()
                        self.query = 'select * from %s where a = "k" and b = 1' % (bucket.name)
                        self.run_cbq_query()
                        self.sleep(5,"sleep for kv operations")
            except Exception, ex:
                content = self.cluster.query_view(self.master, "ddl_%s" % view_name, view_name, {"stale" : "ok"},
                                                  bucket="default", retry_time=1)
                self.log.info("Generated view has %s items" % len(content['rows']))
                raise ex
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, view_name)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                self.query = "select %s from %s where %s is not null and %s is not null" % (','.join(self.FIELDS_TO_INDEX[ind - 1]), bucket.name,
                                                                                                self.FIELDS_TO_INDEX[ind - 1][0], self.FIELDS_TO_INDEX[ind - 1][1])
                actual_result = self.run_cbq_query(query_params={'scan_consistency' : 'statement_plus'})
                self.assertTrue(len(actual_result['results']), self.num_items)

    def test_explain_query_count(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs, name)  USING %s" % (index_name, bucket.name,self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s where VMs is not null union SELECT count(name) FROM %s where name is not null' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]["~children"][0]['~children'][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_query_group_by(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs, join_yr)  USING %s" % (index_name, bucket.name,self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(*) FROM %s where VMs is not null GROUP BY VMs, join_yr' % (bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_query_meta(self):
        for bucket in self.buckets:
            index_name = "my_index_meta"
            try:
                self.query = "CREATE INDEX %s ON %s(meta(%s).id, name)  USING %s" % (index_name, bucket.name, bucket.name,self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT name FROM %s WHERE meta(%s).id is not null and name is not null' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                self.run_cbq_query()

    def test_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindex%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name, join_day)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT name, join_day FROM %s where name = 'employee-9'"% (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                        self.query = "EXPLAIN SELECT * FROM %s where name = 'employee-9'"% (bucket.name)
                        res = self.run_cbq_query()
			plan = ExplainPlanHelper(res)
                        self.assertTrue(plan["~children"][0]['index'] == index_name,"correct index is not used")
                    self.query = "SELECT name, join_day FROM %s where name = 'employee-9'"  % (bucket.name)
                    actual_result = self.run_cbq_query()
                    expected_result = [{"name" : doc["name"],"join_day" : doc["join_day"]}
                               for doc in self.full_list
                               if doc['name'] == 'employee-9']
                    expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
                    self._verify_results(actual_result['results'], expected_result)
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()
                #self.covering_index = False
                self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()
                self.sleep(15,'wait for index')
                self.query = "SELECT name, join_day FROM %s where name = 'employee-9'"  % (bucket.name)
                result = self.run_cbq_query()
                self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
                self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()

    def test_index_like(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT * FROM %s where name " % (bucket.name) +\
                                  "like 'xyz%'"
                    res = self.run_cbq_query()
                    plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]['index'] == index_name,"correct index is not used")
                self.assertTrue(plan["~children"][0]['spans'][0]["range"][0]["high"]=="\"xy{\"")
                self.assertTrue(plan["~children"][0]['spans'][0]["range"][0]["low"]=="\"xyz\"")
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_covering_partial_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithwhere%s" % ind
                    self.query = "CREATE INDEX %s ON %s(email, VMs, join_day) where join_day > 10 USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain select email,VMs[0].RAM from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "select email,join_day from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    actual_result = self.run_cbq_query()
                    expected_result = [{"join_day":doc["join_day"],"email" : doc["email"]}
                               for doc in self.full_list
                               if re.match(r'.*@.*\..*', doc['email']) and \
                                  doc['join_day'] > 10 and \
                                  len([vm for vm in doc["VMs"]
                                        if vm["RAM"] > 5]) > 0]
                    expected_result = sorted(expected_result, key=lambda doc: (doc["join_day"]))
                    self._verify_results(actual_result['results'], expected_result)
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()
                    self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()
                    self.sleep(15,'wait for index')
                    self.query = "select email,join_day from %s where email "  % (bucket.name) +\
                                  "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    result = self.run_cbq_query()
                    self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
                    self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()


    def test_covering_orderby_limit(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithlimit%s" % ind
                    self.query = "CREATE INDEX %s ON %s(skills[0], join_yr, VMs[0].os,name) where join_yr =2010 USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain select  name,skills[0] as skills  from %s where skills[0]='skill2010' and join_yr=2010 and ( VMs[0].os IN ['ubuntu','windows','linux'] OR VMs[0].os IN ['ubuntu','windows','linux'] ) order by _id asc LIMIT 10 OFFSET 0;" % (bucket.name)
                    actual_result=self.run_cbq_query()
                    plan = ExplainPlanHelper(actual_result)
                    if self.index_type.lower() == 'gsi':
                        self.assertTrue(plan["~children"][0]["~children"][0]["scan"]["#operator"] == "IndexScan2")
                    else:
                        self.assertTrue(plan["~children"][0]["~children"][0]["scan"]["#operator"] == "IndexScan")
                    self.assertTrue(plan["~children"][0]["~children"][0]["scan"]["index"] == index_name)
                    #self.test_explain_union(index_name)
                    self.query = "select name,skills[0] as skills from %s where skills[0]='skill2010' and join_yr=2010 and VMs[0].os IN ['ubuntu','windows','linux','linux'] order by name LIMIT 15 OFFSET 0;" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    expected_result = [{"name" : doc["name"],"skills" : doc["skills"][0]}
                                       for doc in self.full_list
                                       if doc['join_yr']==2010 and \
                                          doc['skills'][0]=='skill2010' and \
                                          len([vm for vm in doc["VMs"]
                                                if vm["os"] == 'ubuntu' or vm["os"] == 'windows' or vm["os"] == "linux"])>0]
                    expected_result= sorted(expected_result,key=lambda doc: (doc["name"]))[:15]
                    self.max_verify = 15
                    self._verify_results(actual_result['results'], expected_result)

            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_covering_groupby(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgroupby%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_yr,name) where join_yr >2009 USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT count(*),join_yr,name FROM %s where join_yr > 2009 GROUP BY join_yr,name ORDER BY name';" % (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "SELECT count(*),join_yr FROM %s  where join_yr > 2009 GROUP BY join_yr,name ORDER BY name;" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    print actual_result
            finally:
                    for index_name in created_indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                        self.run_cbq_query()
                    self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()
                    self.sleep(15,'wait for index')
                    self.query = "SELECT count(*),join_yr FROM %s  where join_yr > 2009 GROUP BY join_yr,name ORDER BY name;"  % (bucket.name)
                    result = self.run_cbq_query()
                    self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
                    self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()



    def test_covering_groupby_having(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgroupbyhaving%s" % ind
                    self.query = "CREATE INDEX %s ON %s(job_title,join_mo,test_rate) USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain SELECT join_mo, SUM(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Engineer' GROUP BY join_mo " +\
                         "HAVING SUM(employees.test_rate) > 0 and " +\
                         "SUM(test_rate) < 100000"
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "SELECT join_mo, SUM(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Engineer' GROUP BY join_mo " +\
                         "HAVING SUM(employees.test_rate) > 0 and " +\
                         "SUM(test_rate) < 100000"
                    actual_result = self.run_cbq_query()
                    actual_result = [{"join_mo" : doc["join_mo"], "rate" : round(doc["rate"])} for doc in actual_result['results']]
                    actual_result = sorted(actual_result, key=lambda doc: (doc['join_mo']))
                    tmp_groups = set([doc['join_mo'] for doc in self.full_list])
                    expected_result = [{"join_mo" : group,
                                "rate" : round(math.fsum([doc['test_rate']
                                                          for doc in self.full_list
                                                          if doc['join_mo'] == group and\
                                                             doc['job_title'] == 'Engineer']))}
                               for group in tmp_groups
                               if math.fsum([doc['test_rate']
                                            for doc in self.full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Engineer'] ) > 0 and\
                                  math.fsum([doc['test_rate']
                                            for doc in self.full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Engineer'] ) < 100000]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
                    self._verify_results(actual_result, expected_result)
            finally:
                    for index_name in created_indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                        self.run_cbq_query()

    def test_covering_groupby_letting(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgroupbyletting%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,test_rate) USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain SELECT join_mo, sum_test from %s WHERE join_mo>7 group by join_mo letting sum_test = sum(test_rate) " % (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "SELECT join_mo, sum_test from %s WHERE join_mo>7 group by join_mo letting sum_test = sum(test_rate)" % (bucket.name)
                    actual_list = self.run_cbq_query()
                    actual_result = sorted(actual_list['results'])
                    tmp_groups = set([doc['join_mo'] for doc in self.full_list if doc['join_mo']>7])
                    expected_result = [{"join_mo" : group,
                              "sum_test" : sum([x["test_rate"] for x in self.full_list
                                               if x["join_mo"] == group])}
                               for group in tmp_groups]
                    expected_result = sorted(expected_result)
                    self._verify_results(actual_result, expected_result)
            finally:
                    for index_name in created_indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                        self.run_cbq_query()

    def test_covering_index_hints_select_explain(self):
        index_name_prefix_list = ['hint' + str(uuid.uuid4())[:4],'covering_hint' + str(uuid.uuid4())[:4]]
        created_indexes = []
        for bucket in self.buckets:
            for ind in index_name_prefix_list:
                    for attr in ['join_day', 'join_mo']:
                        ind_name = '%s_%s' % (ind, attr)
                        self.query = "CREATE INDEX %s ON %s(%s)  USING %s" % (ind_name,
                                                                    bucket.name, attr, self.index_type)
                        # if self.gsi_type:
                        #     self.query += " WITH {'index_type': 'memdb'}"
                        self.run_cbq_query()
                        self._wait_for_index_online(bucket, ind_name)
                        created_indexes.append('%s' % (ind_name))
        for bucket in self.buckets:
                try:
                    for ind in created_indexes:
                        if "covering" in ind:
                                if "join_day" in ind:
                                    self.query = 'EXPLAIN SELECT join_day FROM %s  USE INDEX(%s using %s) WHERE join_day>2' % (bucket.name, ind, self.index_type)
                                    self.test_explain_covering_index(ind)
                                elif "join_mo" in ind:
                                    self.query = 'EXPLAIN SELECT join_mo FROM %s  USE INDEX(%s using %s) WHERE join_mo>3' % (bucket.name, ind, self.index_type)
                                    self.test_explain_covering_index(ind)
                        else:
                            self.query = 'EXPLAIN SELECT name,join_day, join_mo FROM %s  USE INDEX(%s using %s) WHERE join_day>2 AND join_mo>3' % (bucket.name, ind, self.index_type)
                            res = self.run_cbq_query()
			    plan = ExplainPlanHelper(res)
                            self.assertTrue(plan["~children"][0]["index"] == ind,
                                    "Index should be %s, but is: %s" % (ind, plan["~children"][0]["index"]))
                            self.query = 'SELECT name,join_day, join_mo FROM %s  USE INDEX(%s using %s) WHERE join_day>2 AND join_mo>3' % (bucket.name, ind, self.index_type)
                            res = self.run_cbq_query()
                finally:
                     for index_name in set(created_indexes):
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                        self.run_cbq_query()
                self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()
                self.sleep(15,'wait for index')
                self.query = "SELECT name,join_day, join_mo FROM %s WHERE join_day>2 AND join_mo>3"  % (bucket.name)
                result = self.run_cbq_query()
                self.assertEqual(sorted(result['results']),sorted(res['results']))
                self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()

    def async_monitor_index(self, bucket, index_name = None):
        monitor_index_task = self.cluster.async_monitor_index(
                 server = self.n1ql_server, bucket = bucket,
                 n1ql_helper = self.n1ql_helper,
                 index_name = index_name)
        return monitor_index_task

##############################################################################################
#
#   SCALAR FN
##############################################################################################

    def test_ceil_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithceil%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)
        for bucket in self.buckets:
                tasks=[]
                for index_name in created_indexes:
                    try:
                        tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                        for task in tasks:
                            task.result()
                    except Exception, ex:
                        self.log.info(ex)


                self.query = "explain select name,ceil(test_rate) as rate from %s where name LIKE '%s' and ceil(test_rate) > 5"  % (bucket.name,"employee")
                if self.covering_index:
                    self.test_explain_covering_index(index_name)
                self.query = "select test_rate from %s where name = 'employee-9' and ceil(test_rate) > 5"  % (bucket.name)
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'], key=lambda doc: (doc['test_rate']))
                expected_result = [{"test_rate" : doc['test_rate']} for doc in self.full_list
                                   if doc['name'] == 'employee-9' and
                                   math.ceil(doc['test_rate']) > 5]
                expected_result = sorted(expected_result, key=lambda doc: (doc['test_rate']))
                self._verify_results(actual_result, expected_result)
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_floor_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithfloor%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)
        for bucket in self.buckets:
            try:
                tasks=[]
                for index_name in created_indexes:
                    try:
                        tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                        for task in tasks:
                            task.result()
                    except Exception, ex:
                        self.log.info(ex)

                self.query = "explain select name,floor(test_rate) from %s where name LIKE '%s' and floor(test_rate) > 5"  % (bucket.name,"employee")
                if self.covering_index:
                    self.test_explain_covering_index(index_name)

                self.query = "select test_rate from %s where name = 'employee-9' and floor(test_rate) > 5"  % (bucket.name)
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'], key=lambda doc: (doc['test_rate']))
                expected_result = [{"test_rate" : doc['test_rate']} for doc in self.full_list
                                   if doc['name'] == 'employee-9' and
                                   math.floor(doc['test_rate']) > 5]
                expected_result = sorted(expected_result, key=lambda doc: (doc['test_rate']))
                self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_greatest_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgreatest%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,skills[0], skills[1])  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)
        for bucket in self.buckets:
                tasks=[]
                for index_name in created_indexes:
                    try:
                        tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                        for task in tasks:
                            task.result()
                    except Exception, ex:
                        self.log.info(ex)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain select GREATEST(skills[0], skills[1]) as SKILL from %s where name LIKE '%s' and skills[0]='skill2010'"  % (bucket.name,"employee")
                if self.covering_index:
                    self.test_explain_covering_index(index_name)

                self.query = "select GREATEST(skills[0], skills[1]) as SKILL from %s where name = 'employee-9' and skills[0]='skill2010'"  % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'],
                                       key=lambda doc: (doc['SKILL']))

                expected_result = [{"SKILL" :
                                        (doc['skills'][0], doc['skills'][1])[doc['skills'][0]<doc['skills'][1]]}
                                   for doc in self.full_list
                                   if doc['skills'][0]=='skill2010' and doc['name']=='employee-9']
                expected_result = sorted(expected_result, key=lambda doc: (doc['SKILL']))
                self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_least_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithleast%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,skills[0], skills[1])  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
                tasks=[]
                for index_name in created_indexes:
                    try:
                        tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                        for task in tasks:
                            task.result()
                    except Exception, ex:
                        self.log.info(ex)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain select LEAST(skills[0], skills[1]) as SKILL from %s where name LIKE '%s' and skills[0]='skill2010'"  % (bucket.name,"employee")
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "select LEAST(skills[0], skills[1]) as SKILL from %s where name = 'employee-9' and skills[0]='skill2010'"  % (
                        bucket.name)
                    actual_result = self.run_cbq_query()
                    actual_result = sorted(actual_result['results'],
                                           key=lambda doc: (doc['SKILL']))

                    expected_result = [{"SKILL" :
                                            (doc['skills'][0], doc['skills'][1])[doc['skills'][0]>doc['skills'][1]]}
                                       for doc in self.full_list
                                       if doc['skills'][0]=='skill2010' and doc['name']=='employee-9']
                    expected_result = sorted(expected_result, key=lambda doc: (doc['SKILL']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

##############################################################################################
#
#   AGGR FN
##############################################################################################

    def test_min_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithmin%s" % ind
                    self.query = "CREATE INDEX %s ON %s(job_title,join_mo,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    created_indexes.append(index_name)
                    index_name2 = "coveringindexwithcontains%s" % ind
                    self.query = "CREATE INDEX %s ON %s(test_rate)  USING %s" % (index_name2, bucket.name,self.index_type)
                    self.run_cbq_query()
                    created_indexes.append(index_name2)

        for bucket in self.buckets:
                tasks=[]
                for index_name in created_indexes:
                    try:
                        tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                        for task in tasks:
                            task.result()
                    except Exception, ex:
                        self.log.info(ex)

        for bucket in self.buckets:
            try:
                    self.query = "explain SELECT join_mo, MIN(test_rate) as rate FROM %s " % (bucket.name) + \
                                 "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                                 "HAVING MIN(employees.test_rate) > 0 and " + \
                                 "SUM(test_rate) < 100000"
                    if self.covering_index:
                        self.test_explain_covering_index(created_indexes[0])

                    self.query = "explain select MIN(test_rate) from %s where test_rate is not missing and CONTAINS(test_rate,'z')" % (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(created_indexes[1])

                    self.query = "select MIN(test_rate) from %s where test_rate is not missing and CONTAINS(test_rate,'z')" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    self.assertTrue(str(actual_result['results'][0]) == "{u'$1': None}")

                    self.query = "explain select count(test_rate) from %s where test_rate is not missing and CONTAINS(test_rate,'z')" % (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(created_indexes[1])

                    self.query = "select count(test_rate) as count from %s where test_rate is not missing and CONTAINS(test_rate,'z')" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    self.assertTrue(actual_result['results'][0]['count'] == 0)

                    self.query = "SELECT join_mo, MIN(test_rate) as rate FROM %s " % (bucket.name) + \
                                 "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                                 "HAVING MIN(employees.test_rate) > 0 and " + \
                                 "SUM(test_rate) < 100000"

                    actual_result = self.run_cbq_query()
                    actual_result = sorted(actual_result['results'], key=lambda doc: (doc['join_mo']))
                    tmp_groups = set([doc['join_mo'] for doc in self.full_list])
                    expected_result = [{"join_mo" : group,
                                        "rate" : min([doc['test_rate']
                                                      for doc in self.full_list
                                                      if doc['join_mo'] == group and \
                                                      doc['job_title'] == 'Sales'])}
                                       for group in tmp_groups
                                       if min([doc['test_rate']
                                               for doc in self.full_list
                                               if doc['join_mo'] == group and \
                                               doc['job_title'] == 'Sales']) > 0 and \
                                       math.fsum([doc['test_rate']
                                                  for doc in self.full_list
                                                  if doc['join_mo'] == group and \
                                                  doc['job_title'] == 'Sales']) < 100000]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_max_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithmax%s" % ind
                    self.query = "CREATE INDEX %s ON %s(job_title,join_mo,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
                tasks=[]
                for index_name in created_indexes:
                    try:
                        tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                        for task in tasks:
                            task.result()
                    except Exception, ex:
                        self.log.info(ex)
        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT join_mo, MAX(test_rate) as rate FROM %s " % (bucket.name) + \
                                 "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                                 "HAVING MAX(employees.test_rate) > 0 and " + \
                                 "SUM(test_rate) < 100000"

                    if self.covering_index:
                        self.test_explain_covering_index(index_name)


                    self.query = "SELECT join_mo, MAX(test_rate) as rate FROM %s " % (bucket.name) + \
                                 "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                                 "HAVING MAX(employees.test_rate) > 0 and " + \
                                 "SUM(test_rate) < 100000"

                    actual_result = self.run_cbq_query()
                    actual_result = sorted(actual_result['results'], key=lambda doc: (doc['join_mo']))
                    tmp_groups = set([doc['join_mo'] for doc in self.full_list])
                    expected_result = [{"join_mo" : group,
                                        "rate" : max([doc['test_rate']
                                                      for doc in self.full_list
                                                      if doc['join_mo'] == group and \
                                                      doc['job_title'] == 'Sales'])}
                                       for group in tmp_groups
                                       if max([doc['test_rate']
                                               for doc in self.full_list
                                               if doc['join_mo'] == group and \
                                               doc['job_title'] == 'Sales'] ) > 0 and \
                                       math.fsum([doc['test_rate']
                                                  for doc in self.full_list
                                                  if doc['join_mo'] == group and \
                                                  doc['job_title'] == 'Sales'] ) < 100000]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_array_agg_distinct_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraydistinct%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate,name)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
                tasks=[]
                for index_name in created_indexes:
                    try:
                        tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                        for task in tasks:
                            task.result()
                    except Exception, ex:
                        self.log.info(ex)
        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_agg(DISTINCT name) as names" + \
                                 " FROM %s  WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                    if self.covering_index:
                        self.test_explain_covering_index(index_name)

                    self.query = "SELECT job_title, array_agg(DISTINCT name) as names" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                    actual_list = self.run_cbq_query()
                    actual_result = self.sort_nested_list(actual_list['results'])
                    actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

                    tmp_groups = set([doc['job_title'] for doc in self.full_list if doc['join_mo']==7])
                    expected_list = [{"job_title" : group,
                                      "names" : set([x["name"] for x in self.full_list
                                                     if x["job_title"] == group])}
                                     for group in tmp_groups]
                    expected_result = self.sort_nested_list(expected_list)
                    expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_array_length_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraylength%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
                tasks=[]
                for index_name in created_indexes:
                    try:
                        tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                        for task in tasks:
                            task.result()
                    except Exception, ex:
                        self.log.info(ex)
        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_length(array_agg(DISTINCT name)) as num_names" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                    if self.covering_index:
                        self.test_explain_covering_index(index_name)

                    self.query = "SELECT job_title, array_length(array_agg(DISTINCT name)) as num_names" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                    actual_result = self.run_cbq_query()
                    actual_result = sorted(actual_result['results'], key=lambda doc: (doc['job_title']))

                    tmp_groups = set([doc['job_title'] for doc in self.full_list if doc['join_mo']==7])
                    expected_result = [{"job_title" : group,
                                        "num_names" : len(set([x["name"] for x in self.full_list
                                                               if x["job_title"] == group]))}
                                       for group in tmp_groups]

                    expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_array_append_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayappend%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name) USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title," + \
                                 " array_append(array_agg(DISTINCT name), 'new_name') as names" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)

                    self.query = "SELECT job_title," + \
                                 " array_append(array_agg(DISTINCT name), 'new_name') as names" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                    actual_list = self.run_cbq_query()
                    actual_result = self.sort_nested_list(actual_list['results'])
                    actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

                    tmp_groups = set([doc['job_title'] for doc in self.full_list if doc['join_mo']==7])
                    expected_result = [{"job_title" : group,
                                        "names" : sorted(set([x["name"] for x in self.full_list
                                                              if x["job_title"] == group] + ['new_name']))}
                                       for group in tmp_groups]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()




    def test_array_concat_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayconcat%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name,email)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title," + \
                                 " array_concat(array_agg(name), array_agg(email)) as names" + \
                                 " FROM %s WHERE join_mo=7  GROUP BY job_title" % (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()


    def test_array_prepend_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayprepend%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)


        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title," + \
                                 " array_prepend(1.2, array_agg(test_rate)) as rates" + \
                                 " FROM %s WHERE join_mo=7  GROUP BY job_title" % (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()


    def test_array_remove_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayremove%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)


        value = 'employee-1'
        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title," + \
                                 " array_remove(array_agg(DISTINCT name), '%s') as names" % (value) + \
                                 " FROM %s WHERE join_mo=7  GROUP BY job_title" % (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)

                    self.query = "SELECT job_title," + \
                                 " array_remove(array_agg(DISTINCT name), '%s') as names" % (value) + \
                                 " FROM %s WHERE join_mo=7  GROUP BY job_title" % (bucket.name)

                    actual_list = self.run_cbq_query()
                    actual_result = self.sort_nested_list(actual_list['results'])
                    actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

                    tmp_groups = set([doc['job_title'] for doc in self.full_list if doc['join_mo']==7])
                    expected_result = [{"job_title" : group,
                                        "names" : sorted(set([x["name"] for x in self.full_list
                                                              if x["job_title"] == group and x["name"]!= value]))}
                                       for group in tmp_groups]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()


    def test_array_avg_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayavg%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_avg(array_agg(test_rate))" + \
                                 " as rates FROM %s  WHERE join_mo=7  GROUP BY job_title" % (bucket.name)

                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "SELECT job_title, array_avg(array_agg(test_rate))" + \
                                 " as rates FROM %s  WHERE join_mo=7  GROUP BY job_title" % (bucket.name)
                    actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()
            self.sleep(15,'wait for index')
            self.query = "SELECT job_title, array_avg(array_agg(test_rate))" + \
                                 " as rates FROM %s  WHERE join_mo=7  GROUP BY job_title" % (bucket.name)
            result = self.run_cbq_query()
            self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
            self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()


    def test_array_contains_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraycontains%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_contains(array_agg(name), 'employee-1')" + \
                                 " as emp_job FROM %s  WHERE join_mo=7  GROUP BY job_title" % (bucket.name)

                    if self.covering_index:
                        self.test_explain_covering_index(index_name)

                    self.query = "SELECT job_title, array_contains(array_agg(name), 'employee-1')" + \
                                 " as emp_job FROM %s  WHERE join_mo=7  GROUP BY job_title" % (bucket.name)

                    actual_result = self.run_cbq_query()
                    actual_result = sorted(actual_result['results'])

                    tmp_groups = set([doc['job_title'] for doc in self.full_list if doc['join_mo']==7])
                    expected_result = [{"job_title" : group,
                                        "emp_job" : 'employee-1' in [x["name"] for x in self.full_list
                                                                     if x["job_title"] == group] }
                                       for group in tmp_groups]
                    expected_result = sorted(expected_result)
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_array_count_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraycount%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_count(array_agg(name)) as names" + \
                                 " FROM %s WHERE join_mo=7  GROUP BY job_title" % (bucket.name)

                if self.covering_index:
                    self.test_explain_covering_index(index_name)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()


    def test_array_distinct_covering_indexes(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraydistinct%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_distinct(array_agg(name)) as names" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                    if self.covering_index:
                        self.test_explain_covering_index(index_name)


                    self.query = "SELECT job_title, array_distinct(array_agg(name)) as names" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                    actual_list = self.run_cbq_query()
                    actual_result = self.sort_nested_list(actual_list['results'])
                    actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

                    tmp_groups = set([doc['job_title'] for doc in self.full_list if doc['join_mo']==7])
                    expected_result = [{"job_title" : group,
                                        "names" : sorted(set([x["name"] for x in self.full_list
                                                              if x["job_title"] == group]))}
                                       for group in tmp_groups]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_array_max_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraymax%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_max(array_agg(test_rate)) as rates" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "SELECT job_title, array_max(array_agg(test_rate)) as rates" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)
                    actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()
            self.sleep(15,'wait for index')
            self.query = "SELECT job_title, array_max(array_agg(test_rate)) as rates" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)
            result = self.run_cbq_query()
            self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
            self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()

    def test_array_sum_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraysum%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, round(array_sum(array_agg(test_rate))) as rates" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "SELECT job_title, round(array_sum(array_agg(test_rate))) as rates" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)
                    actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()
            self.sleep(15,'wait for index')
            self.query = "SELECT job_title, round(array_sum(array_agg(test_rate))) as rates" + \
                                 " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)
            result = self.run_cbq_query()
            self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
            self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()


    def test_subquery_union_intersect_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexforsubquery%s" % ind
                    self.query = "CREATE INDEX %s ON %s(job_title)  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            try:
             for index_name in created_indexes:
                self.query = " explain SELECT count(*) cnt, 0 randomValue, job_title from %s" % (bucket.name)+ \
                             " where job_title is not missing and DATE_DIFF_STR(NOW_STR(),datetime_field1,'day') < 30" + \
                             " group by job_title union all select count(*) cnt, 0 randomValue, job_title from %s" % (bucket.name)+ \
                             " where job_title is not missing group by job_title;"

                if self.covering_index:
                    self.test_explain_covering_index(index_name)
                self.query = " SELECT count(*) cnt, 0 randomValue, job_title from %s"  % (bucket.name) + \
                             " where job_title is not missing and DATE_DIFF_STR(NOW_STR(),datetime_field1,'day') < 30" + \
                             " group by job_title union all select count(*) cnt, 0 randomValue, job_title from %s"  % (bucket.name) + \
                             " where job_title is not missing group by job_title;"
                actual_result =  self.run_cbq_query()
                self.query = "select * from (SELECT count(*) cnt, 0 randomValue, job_title from %s"  % (bucket.name) + \
                             " where job_title is not missing and DATE_DIFF_STR(NOW_STR(),datetime_field1,'day') < 30" + \
                             " group by job_title union all select count(*) cnt, 0 randomValue, job_title from %s"  % (bucket.name) + \
                             " where job_title is not missing group by job_title) c;"
                expected_result = self.run_cbq_query()
                self.assertEqual(len(actual_result['results']),len(expected_result['results']))
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

            self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()
            self.sleep(15,'wait for index')
            self.query = "select * from (SELECT count(*) cnt, 0 randomValue, job_title from %s"  % (bucket.name) + \
                             " where job_title is not missing and DATE_DIFF_STR(NOW_STR(),datetime_field1,'day') < 30" + \
                             " group by job_title union all select count(*) cnt, 0 randomValue, job_title from %s"  % (bucket.name) + \
                             " where job_title is not missing group by job_title) c;"
            result = self.run_cbq_query()
            self.assertEqual(sorted(expected_result['results']),sorted(result['results']))
            self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()

    def test_array_min_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraymin%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            try:
             for index_name in created_indexes:
                self.query = "explain SELECT job_title, array_min(array_agg(test_rate)) as rates" + \
                             " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                if self.covering_index:
                    self.test_explain_covering_index(index_name)
                self.query = "SELECT job_title, array_min(array_agg(test_rate)) as rates" + \
                             " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)
                actual_result = self.run_cbq_query()

            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()
            self.sleep(15,'wait for index')
            self.query =  self.query = "SELECT job_title, array_min(array_agg(test_rate)) as rates" + \
                             " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)
            result = self.run_cbq_query()
            self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
            self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()


    def test_array_put_covering_index(self):
         for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayput%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

         for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

         for bucket in self.buckets:
             try:
                 self.query = "explain SELECT job_title, array_put(array_agg(distinct name), 'employee-1') as emp_job" + \
                              " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                 if self.covering_index:
                     self.test_explain_covering_index(index_name)

                 self.query = "SELECT job_title, array_put(array_agg(distinct name), 'employee-1') as emp_job" + \
                              " FROM %s WHERE join_mo=7  GROUP BY job_title" % (bucket.name)

                 actual_list = self.run_cbq_query()
                 actual_result = self.sort_nested_list(actual_list['results'])
                 actual_result = sorted(actual_result,
                                        key=lambda doc: (doc['job_title']))

                 tmp_groups = set([doc['job_title'] for doc in self.full_list if doc['join_mo']==7])
                 expected_result = [{"job_title" : group,
                                     "emp_job" : sorted(set([x["name"] for x in self.full_list
                                                             if x["job_title"] == group]))}
                                    for group in tmp_groups]
                 expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                 self._verify_results(actual_result, expected_result)

                 self.query = "SELECT job_title, array_put(array_agg(distinct name), 'employee-47') as emp_job" + \
                              " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                 actual_list = self.run_cbq_query()
                 actual_result = self.sort_nested_list(actual_list['results'])
                 actual_result = sorted(actual_result,
                                        key=lambda doc: (doc['job_title']))

                 expected_result = [{"job_title" : group,
                                     "emp_job" : sorted(set([x["name"] for x in self.full_list
                                                             if x["job_title"] == group] + ['employee-47']))}
                                    for group in tmp_groups]
                 expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                 self._verify_results(actual_result, expected_result)
             finally:
                 for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()


    def test_array_replace_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayreplace%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            try:
                self.query = "explain SELECT job_title, array_replace(array_agg(name), 'employee-1', 'employee-47') as emp_job" + \
                             " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)

                if self.covering_index:
                    self.test_explain_covering_index(index_name)
                self.query = " SELECT job_title, array_replace(array_agg(name), 'employee-1', 'employee-47') as emp_job" + \
                             " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)
                actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()
            self.sleep(15,'wait for index')
            self.query = " SELECT job_title, array_replace(array_agg(name), 'employee-1', 'employee-47') as emp_job" + \
                             " FROM %s WHERE join_mo=7 GROUP BY job_title" % (bucket.name)
            result = self.run_cbq_query()
            self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
            self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()

    def test_array_sort_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraysort%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            try:
                self.query = "explain SELECT job_title, array_sort(array_agg(distinct test_rate)) as emp_job" +\
                " FROM %s WHERE join_mo=7  GROUP BY job_title" % (bucket.name)

                if self.covering_index:
                    self.test_explain_covering_index(index_name)
                self.query = " SELECT job_title, array_sort(array_agg(distinct test_rate)) as emp_job" +\
                " FROM %s WHERE join_mo=7  GROUP BY job_title" % (bucket.name)

                actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()
            self.sleep(15,'wait for index')
            self.query = " SELECT job_title, array_sort(array_agg(distinct test_rate)) as emp_job" +\
                " FROM %s WHERE join_mo=7  GROUP BY job_title" % (bucket.name)
            result = self.run_cbq_query()
            self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
            self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()



    def test_poly_length_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithpolylength%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,tasks_points,VMs,skills)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            try:
                query_fields = ['tasks_points', 'VMs', 'skills']
                for query_field in query_fields:
                    self.query = "explain select length(%s) as custom_num from %s WHERE join_mo=7" % (query_field, bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "select length(%s) as custom_num from %s WHERE join_mo=7" % (query_field, bucket.name)
                    actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()
            self.sleep(15,'wait for index')
            self.query = "select length(%s) as custom_num from %s WHERE join_mo=7" % (query_field, bucket.name)
            result = self.run_cbq_query()
            self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
            self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()

    def test_maximumlimit_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithmaximumlimit%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,name,mutated,skills,join_day,email,test_rate,join_yr,_id,VMs[0].RAM,VMs[1].os,job_title)  USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            try:
                query_fields = ['name', 'mutated', 'skills','join_day','join_mo','email','test_rate','join_yr','_id','VMs[0].RAM','VMs[1].os','job_title']
                for query_field in query_fields:
                    self.query = "explain Select length(%s) as custom_num from %s WHERE join_mo=7" % (query_field, bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)


                    self.query = "select length(%s) as custom_num from %s WHERE join_mo=7" % (query_field, bucket.name)
                    actual_result = self.run_cbq_query()
                    expected_result = [{"custom_num" : None} for doc in self.full_list if doc['join_mo']==7]
                    self._verify_results(actual_result['results'], expected_result)

                    self.query = "select poly_length(%s) as custom_num from %s WHERE join_mo=7 " % (query_field, bucket.name)
                    actual_result = self.run_cbq_query()
                    expected_result = [{"custom_num" : len(doc[query_field])}
                                       for doc in self.full_list
                                       if doc['join_mo']==7]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['custom_num']))
                    self._verify_results(actual_result['results'], expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_count_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithsubstr%s" % ind
                    self.query = "CREATE INDEX %s ON %s(_id,join_mo)  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
                tasks=[]
                for index_name in created_indexes:
                    try:
                        tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                        for task in tasks:
                            task.result()
                    except Exception, ex:
                        self.log.info(ex)

        for bucket in self.buckets:
            try:
                    self.query = "explain select count(_id) from %s where join_mo is not missing and _id is not null" % (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)

                    self.query = "select count(_id) as count from %s where join_mo is not missing and _id is not null" % (bucket.name)
                    actual_result1 = self.run_cbq_query()
                    print actual_result1
                    self.assertTrue(actual_result1['results'][0]['count'] == 2016)

                    self.query = "select count(_id)  as count from %s where join_mo is not missing and _id is null" % (bucket.name)

                    actual_result2 = self.run_cbq_query()
                    self.assertTrue(actual_result2['results'][0]['count'] == 0)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()
            self.sleep(15,'wait for index')
            self.query = "select count(_id) as count from %s where join_mo is not missing and _id is not null" % (bucket.name)
            result = self.run_cbq_query()
            self.assertEqual(sorted(actual_result1['results']),sorted(result['results']))
            self.query = "select count(_id)  as count from %s where join_mo is not missing and _id is null" % (bucket.name)
            result = self.run_cbq_query()
            self.assertEqual(sorted(actual_result2['results']),sorted(result['results']))
            self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
            self.run_cbq_query()


    def test_explain_index_join(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name, project)  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT employee.name, new_task.project FROM %s as employee JOIN %s as new_task ON KEYS ['key1']" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == "#primary",
                                    "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_explain_index_subquery(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_day, name)  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN select task_name, (select sum(test_rate) cn from %s as d use keys ['query-1'] where join_day>2 and name =='abc') as names from %s" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
		    plan = ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == "#primary",
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_explain_childs_list_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_child2"
            try:
                self.query = "CREATE INDEX %s ON %s(distinct array vm.RAM for vm in VMs END)  USING %s" % (index_name, bucket.name,self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT VMs[0].RAM FROM %s ' % (bucket.name) + \
                        'WHERE ANY vm IN VMs SATISFIES vm.RAM > 5 end'
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["scan"]['index']  == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_childs_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_obj"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points.task1, join_mo)  USING %s" % (index_name, bucket.name,self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE task_points.task1 > 0 and join_mo>7 '
                res = self.run_cbq_query()
		plan = ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    # def test_explain_childs_objects_element(self):
    #     for bucket in self.buckets:
    #         index_name = "my_index_obj_el"
    #         try:
    #             self.query = "CREATE INDEX %s ON %s(tasks_points.task1, join_mo)  USING %s" % (index_name, bucket.name,self.index_type)
    #             if self.gsi_type:
    #                 self.query += " WITH {'index_type': 'memdb'}"
    #             self.run_cbq_query()
    #             self._wait_for_index_online(bucket, index_name)
    #             self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
    #                          'WHERE task_points.task1 > 0 and join_mo>7'
    #             res = self.run_cbq_query()
    #		  plan = ExplainPlanHelper(res)
    #             self.assertTrue(plan["~children"][0]["index"] == index_name,
    #                             "Index should be %s, but is: %s" % (index_name, plan))
    #         finally:
    #             self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
    #             try:
    #                 self.run_cbq_query()
    #             except:
    #                 pass

    def _is_index_in_list(self, bucket, index_name):
        query = "SELECT * FROM system:indexes"
        res = self.run_cbq_query(query)
        for item in res['results']:
            if 'keyspace_id' not in item['indexes']:
                self.log.error(item)
                continue
            if item['indexes']['keyspace_id'] == bucket.name and item['indexes']['name'] == index_name:
                return True
        return False


    def test_tokencountscan(self):
        for bucket in self.buckets:
                created_indexes = []
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(tokens(%s)) " %(idx,bucket.name,'_id')+\
                             " USING %s" % (self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select count(1) from %s WHERE tokens(_id) like '%s' " %(bucket.name,'query-test%')

                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                result1 = plan['~children'][0]['index']
                self.assertTrue(result1 == idx)
                self.query = "select count(1) from %s WHERE tokens(_id) like '%s' " %(bucket.name,'query-test%')
                actual_result = self.run_cbq_query()
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'IndexCountScan',
                    "IndexCountScan is not being used")
                self.query = "explain select a.cnt from (select count(1) from default where tokens(_id) is not null) as a"
                actual_result2 = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result2)
                self.assertTrue(
                    plan['~children'][0]['#operator'] != 'IndexCountScan',
                    "IndexCountScan should not be used in subquery")
                self.query = "select a.cnt from (select count(1) from default where tokens(_id) is not null) as a"
                actual_result2 = self.run_cbq_query()

                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                    self.query = "select count(1) from %s use index(`#primary`) WHERE tokens(_id) like '%s'  " %(bucket.name,'query-test%')
                    result = self.run_cbq_query()
                    self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
                    self.query = "select a.cnt from (select count(1) from %s where _id is not null) as a " %(bucket.name)
                    result = self.run_cbq_query()
                    self.assertEqual(sorted(actual_result2['results']),sorted(result['results']))


    def test_join_unnest_tokens_covering(self):
        for bucket in self.buckets:
            created_indexes=[]
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s) END,VMs) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "explain SELECT x FROM default emp1 USE INDEX(%s)  UNNEST tokens(emp1.VMs) as x  JOIN default task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5   ;" %(idx4)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))


                self.query = "SELECT x FROM default emp1 USE INDEX(%s) UNNEST tokens(emp1.VMs) as x JOIN default task ON KEYS meta(`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;"%(idx4)
                actual_result = self.run_cbq_query()
                self.query = "SELECT x FROM default emp1 USE INDEX(`#primary`)  UNNEST tokens(emp1.VMs) as x  JOIN default task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5 ;"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) ==sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_substring(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxsubstr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY SUBSTR(j.FirstName,8) for j in tokens(name) end) USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name, SUBSTR(name.FirstName, 8) as firstname from %s  where ANY j IN tokens(name) SATISFIES substr(j.FirstName,8) != 'employee' end" % (bucket.name)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_update(self):
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "arrayidx_update"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in tokens(i) end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                updated_value = 'new_dept' * 30
                self.query = "EXPLAIN update %s set name=%s where ANY i IN tokens(address) SATISFIES  (ANY j IN tokens(i) SATISFIES j.city='Mumbai' end) END  returning element department"  % (bucket.name, updated_value)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "update %s set department=%s where ANY i IN tokens(address) SATISFIES  (ANY j IN i SATISFIES j.city='Mumbai' end) END  returning element department"  % (bucket.name, updated_value)
                self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
             finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_delete(self):
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "arrayidx_delete"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY v FOR v in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = 'select count(*) as actual from %s where tokens(join_yr)=2012'  % (bucket.name)
                self.run_cbq_query()
                self.sleep(5, 'wait for index')
                actual_result = self.run_cbq_query()
                current_docs = actual_result['results'][0]['actual']
                self.query = 'EXPLAIN delete from %s where any v in tokens(join_yr) SATISFIES v=2012 end LIMIT 1'  % (bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                result = plan['~children'][0]['scan']['index']
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used in and query for 2 array indexes")
                self.assertTrue(result == idx )
                self.query = 'delete from %s where any v in tokens(join_yr) satisfies v=2012 end LIMIT 1'  % (bucket.name)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

             finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nesttokens_keys_where_between(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in tokens(join_yr) end) USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN tokens(emp.join_yr) SATISFIES  j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.join_yr) SATISFIES  j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select emp.name " + \
                         "FROM %s emp USE INDEX(`#primary`)  NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id where  ANY j IN tokens(emp.join_yr) SATISFIES  j between 2010 and 2012 end;"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result == expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nesttokens_keys_where_less_more_equal(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in tokens(join_yr) end) USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name, ARRAY item.department FOR item in items end departments " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN tokens(emp.join_yr) SATISFIES  j <= 2014 and j >= 2012 end;"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name, ARRAY item.department FOR item in items end departments " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.join_yr) SATISFIES  j <= 2014 and j >= 2012 end;"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select emp.name, ARRAY item.department FOR item in items end departments " + \
                         "FROM %s emp USE INDEX(`#primary`)  NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.join_yr) SATISFIES  j <= 2014 and j >= 2012 end;"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result == expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_sum(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "indexwitharraysum%s" % ind
                    self.query = "CREATE INDEX %s ON %s(department, DISTINCT ARRAY round(v.memory + v.RAM) FOR v in tokens(VMs) END ) where join_yr=2012 USING %s" % (index_name, bucket.name,self.index_type)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "EXPLAIN SELECT count(name),department" + \
                                 " FROM %s where join_yr=2012 AND ANY v IN tokens(VMs) SATISFIES round(v.memory+v.RAM)<100 END AND department = 'Engineer'  GROUP BY department" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    plan = ExplainPlanHelper(actual_result)
                    self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                    result1 = plan['~children'][0]['scan']['index']
                    self.assertTrue(result1 == index_name)
                    self.query = "SELECT count(name),department" + \
                                 " FROM %s where join_yr=2012 AND ANY v IN tokens(VMs) SATISFIES round(v.memory+v.RAM)<100 END AND department = 'Engineer'  GROUP BY department" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    actual_result = sorted(actual_result['results'])
                    self.query = "SELECT count(name),department" + \
                                 " FROM %s use index(`#primary`) where join_yr=2012 AND ANY v IN tokens(VMs) SATISFIES round(v.memory+v.RAM)<100 END AND department = 'Engineer'  GROUP BY department" % (bucket.name)
                    expected_result = self.run_cbq_query()
                    expected_result = sorted(expected_result['results'])
                    self.assertTrue(actual_result == expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_nesttokens_keys_where_not_equal(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in tokens(department) end) USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + \
                         "FROM %s emp NEST %s VMs " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN tokens(emp.department) SATISFIES  j != 'Engineer' end;"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + \
                         "FROM %s emp NEST %s VMs " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN tokens(emp.department) SATISFIES  j = 'Support' end;"
                actual_result = self.run_cbq_query()
                actual_result = actual_result['results']
                self.query = "select emp.name " + \
                         "FROM %s emp USE INDEX(`#primary`)   NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id where  ANY j IN tokens(emp.department) SATISFIES  j = 'Support' end;"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(sorted(actual_result) == expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_nest_keys_where(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in tokens(department) end) USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN tokens(emp.department) SATISFIES  j = 'Support' end;"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + \
                         "FROM %s emp NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN tokens(emp.department) SATISFIES  j = 'Support' end;"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select emp.name " + \
                         "FROM %s emp USE INDEX(`#primary`) NEST %s items " % (
                             bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN tokens(emp.department) SATISFIES  j = 'Support' end;"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result == expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_regexp(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "iregex"
                self.query = " CREATE INDEX %s ON %s( DISTINCT ARRAY REGEXP_LIKE(v.os,%s)  FOR v IN tokens(VMs) END )  USING %s" % (
                  idx, bucket.name,"'ub%'", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s  WHERE ANY v IN tokens(VMs) SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select * from %s use index(`#primary`)  WHERE ANY v IN tokens(VMs) SATISFIES REGEXP_LIKE(v.os,%s) = 1  END  " % (
                bucket.name,"'ub%'") + \
                             "order BY tokens(name) limit 10"
                self.query = "select * from %s  WHERE ANY v IN tokens(VMs) SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + \
                             "order BY tokens(name) limit 10"
                actual_result = self.run_cbq_query()
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])

                self.assertTrue(expected_result==sorted(actual_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_left_outer_join(self):
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "outer_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "outer_join_all"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( All array j.city for j in i end) FOR i in tokens(%s) END) USING %s" % (
                    idx2, bucket.name, "address", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee use index (%s) left JOIN default as new_project_full " % (bucket.name,idx) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee use index (%s) left JOIN default as new_project_full " % (bucket.name,idx2) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)
                self.query = "SELECT new_project_full.department new_project " +\
                "FROM %s as employee use index (%s) left JOIN default as new_project_full " % (bucket.name,idx) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result1 = (actual_result['results'])
                self.query = "SELECT new_project_full.department new_project " +\
                "FROM %s as employee use index (%s) left JOIN default as new_project_full " % (bucket.name,idx2) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result2 = (actual_result['results'])
                self.query = "SELECT new_project_full.department new_project " +\
                "FROM %s as employee use index (`#primary`) left JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result3 = (actual_result['results'])
                self.assertTrue(actual_result1==actual_result3)
                self.assertTrue(actual_result2==actual_result3)
             finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_with_inner_joins(self):
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "nested_inner_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
             finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_partial_tokens_all(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( all array i FOR i in tokens(%s) END) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and (ANY i IN tokens(%s.hobbies.hobby) SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from %s WHERE department = 'Support' and ANY i IN tokens(%s.hobbies.hobby) SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s WHERE department = 'Support' and ANY i IN tokens(%s.hobbies.hobby) SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_greatest(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX %s ON %s(department, DISTINCT ARRAY GREATEST(v.RAM,100)  FOR v IN tokens(VMs) END )  USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE department = 'Support' and ANY v IN tokens(VMs) SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from %s WHERE department = 'Support' and ANY v IN tokens(VMs) SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                actual_result = actual_result['results']
                self.query = "select name from %s USE index(`#primary`) WHERE department = 'Support' and ANY v IN tokens(VMs) SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                expected_result = self.run_cbq_query()
                expected_result = expected_result['results']
                self.assertTrue(sorted(expected_result)==sorted(actual_result))

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_partial_tokens_distinct(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( distinct array i FOR i in tokens(%s) END) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and (ANY i IN tokens(%s.hobbies.hobby) SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from %s WHERE department = 'Support' and ANY i IN tokens(%s.hobbies.hobby) SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s use index (`#primary`) WHERE department = 'Support' and ANY i IN tokens(%s.hobbies.hobby) SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_array_tokens_in_all(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( all array j for j in i.dance end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "select name from %s WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s USE INDEX(`#primary`) WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_token_within(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY i FOR i within tokens(%s) END) USING %s" % (
                    idx, bucket.name, "hobbies", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY i FOR i within tokens(%s) END) USING %s" % (
                    idx2, bucket.name, "hobbies", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                self.query = "EXPLAIN select name from %s use index(%s) WHERE ANY i within tokens(%s.hobbies) SATISFIES i = 'bhangra' END " % (
                bucket.name,idx2,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)

                self.query = "EXPLAIN select name from %s use index(%s) WHERE ANY i within tokens(%s.hobbies) SATISFIES i = 'bhangra' END " % (
                bucket.name,idx,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "select name from %s use index(%s) WHERE ANY i within tokens(%s.hobbies) SATISFIES i = 'bhangra' END " % (
                bucket.name,idx,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result1 = sorted(actual_result['results'])

                self.query = "select name from %s use index(%s) WHERE ANY i within tokens(%s.hobbies) SATISFIES i = 'bhangra' END " % (
                bucket.name,idx2,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result2 = sorted(actual_result['results'])

                self.query = "select name from %s use index(`#primary`) WHERE ANY i within tokens(%s.hobbies) SATISFIES i = 'bhangra' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result3 = sorted(actual_result['results'])


                self.assertTrue(actual_result1 ==  actual_result2 == actual_result3)

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_array_tokens_in_distinct(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j for j in i.dance end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                actual_result = self.run_cbq_query()
                self.query = "select name from %s WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s USE INDEX(`#primary`) WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_token(self):
         for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( All array j.region1 for j in i.Marketing end) FOR i in tokens(%s) END) USING %s" % (
                    idx2, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                idx3 = "nested_idx_attr3"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in tokens(%s) END) USING %s" % (
                    idx3, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")

                idx4 = "nested_idx_attr4"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( ALL array j.region1 for j in i.Marketing end) FOR i in tokens(%s) END) USING %s" % (
                    idx4, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "EXPLAIN select name from %s USE INDEX(%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "EXPLAIN select name from %s USE INDEX(%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx2,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)

                self.query = "EXPLAIN select name from %s USE INDEX(%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx3,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx3)

                self.query = "EXPLAIN select name from %s USE INDEX(%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx4,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)

                self.query = "select name from %s use index (%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx,bucket.name) + \
                             "order BY name limit 10"
                actual_result1 = self.run_cbq_query()
                self.query = "select name from %s use index (%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx2,bucket.name) + \
                             "order BY name limit 10"
                actual_result2 = self.run_cbq_query()

                self.query = "select name from %s use index (%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx3,bucket.name) + \
                             "order BY name limit 10"
                actual_result3 = self.run_cbq_query()

                self.query = "select name from %s use index (%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx4,bucket.name) + \
                             "order BY name limit 10"
                actual_result4 = self.run_cbq_query()


                self.query = "select name from %s use index (`#primary`) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result5 = self.run_cbq_query()

                self.assertTrue(sorted(actual_result1['results'])==sorted(actual_result2['results']))
                self.assertTrue(sorted(actual_result2['results'])==sorted(actual_result3['results']))
                self.assertTrue(sorted(actual_result4['results'])==sorted(actual_result3['results']))
                self.assertTrue(sorted(actual_result4['results'])==sorted(actual_result5['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_unnest_multilevel_attribute_tokens(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr_nest"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( DISTINCT array j.region1 for j in tokens(i.Marketing) end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")


                self.query = "explain SELECT emp.name FROM %s emp  UNNEST emp.tasks as i UNNEST tokens(i.Marketing) as j where j.region1 = 'South'" % (bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN tokens(i.Marketing) SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from %s USE INDEX(`#primary`) WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN tokens(i.Marketing) SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results'])==sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_tokens_join_unnest_alias_covering(self):
        for bucket in self.buckets:
            created_indexes=[]
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s) END,VMs) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                # idx5 = "idxVM3"
                # self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.os FOR x within %s END) USING %s" % (
                #     idx5, bucket.name, "VMs", self.index_type)
                # # if self.gsi_type:
                # #     self.query += " WITH {'index_type': 'memdb'}"
                # create_result = self.run_cbq_query()
                # self._wait_for_index_online(bucket, idx5)
                # self._verify_results(create_result['results'], [])
                # created_indexes.append(idx5)

                #self.assertTrue(self._is_index_in_list(bucket, idx5), "Index is not in list")

                self.query = "explain SELECT x FROM default emp1  UNNEST tokens(emp1.VMs) as x  JOIN default task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5   ;"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "SELECT x FROM default emp1 UNNEST tokens(emp1.VMs) as x JOIN default task ON KEYS meta(`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;"
                actual_result = self.run_cbq_query()
                self.query = "SELECT x FROM default emp1 USE INDEX(`#primary`)  UNNEST tokens(emp1.VMs) as x  JOIN default task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5 ;"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) ==sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_tokens_all(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = 'CREATE INDEX %s ON %s( ALL ARRAY v FOR v in tokens(%s,{"case":"lower"}) END) USING %s' % (
                    idx, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = 'CREATE INDEX %s ON %s( All ARRAY x.RAM FOR x in tokens(%s,{"names":true}) END) USING %s' % (
                    idx2, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)

                self.query = 'EXPLAIN select name from %s where any v in tokens(%s.join_yr,{"case":"lower"}) satisfies v = 2016 END ' % (
                bucket.name, bucket.name) + \
                             'AND (ANY x IN tokens(%s.VMs,{"names":true}) SATISFIES x.RAM between 1 and 5 END) ' % (bucket.name) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'
                actual_result = self.run_cbq_query()

		plan = ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan')

                result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                self.query = 'select name from %s where any v in tokens(%s.join_yr,{"case":"lower"}) satisfies v = 2016 END ' % (
                bucket.name, bucket.name) + \
                             'AND (ANY x IN tokens(%s.VMs,{"names":true}) SATISFIES x.RAM between 1 and 5 END) ' % (bucket.name) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'
                actual_result = self.run_cbq_query()

                self.query = 'select name from %s use index (`#primary`) where any v in tokens(%s.join_yr,{"case":"lower"}) satisfies v = 2016 END ' % (
                bucket.name, bucket.name) + \
                             'AND (ANY x IN tokens(%s.VMs,{\'"names"\':true}) SATISFIES x.RAM between 1 and 5 END) ' % (bucket.name) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results'])==sorted(expected_result['results']))
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                created_indexes.remove(idx)
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx2, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx2), "Index is in list")
                created_indexes.remove(idx2)

                idx3 = "idxjoin_yr2"
                self.query = 'CREATE INDEX %s ON %s( all ARRAY v FOR v within tokens(%s,{"names":true,"case":"lower"}) END) USING %s' % (

                    idx3, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = 'CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s,{"names":true,"case":"lower"}) END) USING %s' % (
                    idx4, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = 'EXPLAIN select name from %s where any v in tokens(%s.join_yr,{"case":"lower","names":true}) satisfies v = 2016 END ' % (
                bucket.name, bucket.name) + \
                             'AND (ANY x IN tokens(%s.VMs,{"names":true,"case":"lower"}) SATISFIES x.RAM between 1 and 5 END) ' % (bucket.name) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'
                actual_result_within = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result_within)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan',
                    "Intersect Scan is not being used in and query for 2 array indexes")

                result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx3 or result1 == idx4)
                self.assertTrue(result2 == idx4 or result2 == idx3)

                idx5 = "idxtokens1"
                self.query = 'CREATE INDEX %s ON %s( all ARRAY v FOR v within tokens(%s)  END) USING %s' % (

                    idx5, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx5)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx5)
                self.assertTrue(self._is_index_in_list(bucket, idx5), "Index is not in list")

                idx8 = "idxtokens2"
                self.query = 'CREATE INDEX %s ON %s( all ARRAY v FOR v within tokens(%s,{"names":true,"case":"lower","specials":false})  END) USING %s' % (

                    idx8, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx8)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx8)
                self.assertTrue(self._is_index_in_list(bucket, idx8), "Index is not in list")

                idx6 = "idxVM4"
                self.query = 'CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s,{"specials":false}) END) USING %s' % (
                    idx6, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx6)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx6)

                self.assertTrue(self._is_index_in_list(bucket, idx6), "Index is not in list")

                idx7 = "idxVM3"
                self.query = 'CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s,{"names":true,"case":"lower"}) END) USING %s' % (
                    idx7, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx7)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx7)

                self.assertTrue(self._is_index_in_list(bucket, idx7), "Index is not in list")

                self.query = 'EXPLAIN select name from %s where any v within tokens(%s.join_yr,{"case"}) satisfies v = 2016 END ' % (
                bucket.name, bucket.name) + \
                             'AND (ANY x within tokens(%s.VMs,{"specials"}) SATISFIES x.RAM between 1 and 5 END) ' % (bucket.name) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'
                # actual_result_within = self.run_cbq_query()
                # self.assertTrue("Object member has no value" in actual_result_within)

                self.query = 'select name from %s where any v in tokens(%s.join_yr,{"case":"lower","names":true,"specials":false}) satisfies v = 2016 END ' % (
                bucket.name, bucket.name) + \
                             'AND (ANY x within tokens(%s.VMs,{"specials":false}) SATISFIES x.RAM between 1 and 5  END ) ' % (bucket.name) + \
                             'AND  NOT (department = "Manager") order by name limit 10'
                actual_result_within = self.run_cbq_query()
                self.query = "select name from %s use index (`#primary`) where any v in tokens(%s.join_yr) satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x within tokens(%s.VMs) SATISFIES x.RAM between 1 and 5  END ) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result_within['results']) == sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_tokens(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( all array j for j in i.dance end) FOR i in tokens(%s) END,hobbies.hobby,department,name) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)

                self.assertTrue("covers" in str(plan))
                self.query = "select name from %s WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s USE INDEX(`#primary`) WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx),"Index is in list")

    def test_tokens_partial_index_distinct_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = 'CREATE INDEX %s ON %s( distinct array i FOR i in tokens(%s,{"names":true}) END,hobbies.hobby,name) WHERE (department = "Support")  USING %s' % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = 'EXPLAIN select name from %s WHERE department = "Support" and (ANY i IN tokens(%s.hobbies.hobby,{"names":true}) SATISFIES  i = "art" END) ' % (
                bucket.name,bucket.name) + \
                             'order BY name limit 10'
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = 'select name from %s WHERE department = "Support" and ( ANY i IN tokens(%s.hobbies.hobby,{"names":true} ) SATISFIES i = "art" END) ' % (bucket.name,bucket.name) + \
                             "order BY name limit 10"

                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = 'select name from %s use index (`#primary`) WHERE department = "Support" and (ANY i IN tokens(%s.hobbies.hobby,{"names":true}) SATISFIES i = "art" END) ' % (
                bucket.name,bucket.name) + \
                             'order BY name limit 10'
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_tokens_distinct_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( distinct array i FOR i in tokens(%s) END,hobbies.hobby,name) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE department = 'Support' and (ANY i IN tokens(%s.hobbies.hobby) SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                print plan
                self.assertTrue("covers" in str(plan))
                self.query = "select name from %s WHERE department = 'Support' and ANY i IN tokens(%s.hobbies.hobby) SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select name from %s use index (`#primary`) WHERE department = 'Support' and ANY i IN tokens(%s.hobbies.hobby) SATISFIES i = 'art' END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertTrue(actual_result==expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_tokens_with_inner_joins_covering(self):
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "nested_inner_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in tokens(%s) END,address,department) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT employee.department new_project " +\
                "FROM %s as employee  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "SELECT employee.department new_project " +\
                "FROM %s as employee  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                self.query = "SELECT employee.department new_project " +\
                "FROM %s as employee use index (`#primary`)  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id  WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                expected_result = self.run_cbq_query()
                expected_result = expected_result['results']
                self.assertTrue(sorted(expected_result)==sorted(actual_result['results']))

             finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_tokens_regexp_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "iregex"
                self.query = " CREATE INDEX %s ON %s( DISTINCT ARRAY REGEXP_LIKE(v.os,%s)  FOR v IN tokens(VMs) END,VMs )  USING %s" % (
                  idx, bucket.name,"'ub%'", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select VMs from %s  WHERE ANY v IN tokens(VMs) SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + \
                             "limit 10"
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)

                self.assertTrue("covers" in str(plan))

                self.query = "select VMs from %s  WHERE ANY v IN tokens(VMs) SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + \
                             "limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select VMs from %s use index(`#primary`)  WHERE ANY v IN tokens(VMs) SATISFIES REGEXP_LIKE(v.os,%s) = 1  END  " % (
                bucket.name,"'ub%'") + \
                             "limit 10"

                expected_result = self.run_cbq_query()
                expected_result = expected_result['results']

                self.assertTrue(sorted(expected_result)==sorted(actual_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")


    def test_tokens_greatest_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX %s ON %s( department, DISTINCT ARRAY GREATEST(v.RAM,100)  FOR v IN tokens(VMs) END ,VMs,name )  USING %s" % (
                    idx, bucket.name, self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = " EXPLAIN select name from %s WHERE department = 'Support' and ANY v IN tokens(VMs) SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                print plan
                self.assertTrue("covers" in str(plan))
                self.query = "select name from %s WHERE department = 'Support' and ANY v IN tokens(VMs) SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                actual_result = actual_result['results']
                self.query = "select name from %s USE index(`#primary`) WHERE department = 'Support' and ANY v IN tokens(VMs) SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                expected_result = self.run_cbq_query()
                expected_result = expected_result['results']
                self.assertTrue(sorted(expected_result)==sorted(actual_result))

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_unnest_tokens_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "unnest_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( ALL array j for j in i end) FOR i in tokens(%s) END,department,tasks,name) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select %s.name from %s  UNNEST TOKENS(tasks) AS i UNNEST i AS j WHERE j = 'Search' " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN TOKENS(default.tasks) SATISFIES x = 'Sales' END) " + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)

                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))

                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][2]) == ("cover ((`default`.`tasks`))" ))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][3]) == ("cover ((`default`.`name`))" ))

                self.query = "select %s.name from %s UNNEST TOKENS(tasks) AS i UNNEST i AS j WHERE j = 'Search'  " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN tokens(%s.tasks) SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select %s.name from %s use index (`#primary`)  UNNEST TOKENS(tasks) AS i UNNEST i as j WHERE j = 'Search'  " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN tokens(%s.tasks) SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)
                expected_result = self.run_cbq_query()

                self.assertEqual(sorted(actual_result['results']),sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_token_covering(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT ARRAY j for j in i end) FOR i in tokens(%s) END,tasks,department,name)" % (
                    idx, bucket.name, "tasks")

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")


                self.query = "EXPLAIN select name from %s WHERE ANY i IN tokens(tasks) SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx)
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == ("cover ((`%s`.`tasks`))" % bucket.name))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['scan'][0]['covers'][2]) == ("cover ((`default`.`department`)"))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['scan'][0]['covers'][3]) == ("cover ((meta(`default`).`id`))"))
                self.query = "select name from %s WHERE ANY i IN tokens(tasks) SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from %s use index(`#primary`) WHERE ANY i IN tokens(tasks) SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                expected_result =  self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_token_nonarrayindex(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s( ARRAY v FOR v in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s( ARRAY x.RAM FOR x in tokens(%s) END) USING %s" % (
                    idx2, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = "select name from %s where any v in tokens(%s.join_yr) satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND ( ANY x IN tokens(%s.VMs) SATISFIES x.RAM between 1 and 5 END ) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                created_indexes.remove(idx)
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx2, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx2), "Index is in list")
                created_indexes.remove(idx2)

                idx3 = "idxjoin_yr2"
                self.query = "CREATE INDEX %s ON %s(  ARRAY v FOR v within tokens(%s) END) USING %s" % (
                    idx3, bucket.name, "join_yr", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s(  ARRAY x.RAM FOR x within tokens(%s) END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")
                self.query = "select name from %s USE INDEX(%s) where " % (bucket.name,idx4) + \
                             "(ANY x within tokens(%s.VMs) SATISFIES x.RAM between 1 and 5  END ) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                actual_result_within = self.run_cbq_query()
                self.query = "select name from %s USE INDEX(`#primary`) where " % (
                bucket.name) + \
                             "(ANY x within tokens(%s.VMs) SATISFIES x.RAM between 1 and 5  END ) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result_within['results']) == sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_unnest_token(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "unnest_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( ALL array j for j in i end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select %s.name from %s UNNEST tokens(tasks) as i UNNEST i as j WHERE j = 'Search' " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)
                # self.query = "EXPLAIN select %s.name from %s UNNEST tasks as i UNNEST i as j WHERE j = 'Search' " % (
                #  bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
		plan = ExplainPlanHelper(actual_result)
                # self.assertTrue(
                #     plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan',
                #     "Intersect Scan is not being used in and query for 2 array indexes")
                result1 =plan['~children'][0]['~children'][0]['scan']['index']

                #result2 =plan['~children'][0]['~children'][0]['scans'][1]['scans'][0]['index']
                self.assertTrue(result1 == idx )
                #self.assertTrue(result2 == idx or result2 == idx2)
                self.query = "select %s.name from %s  UNNEST tokens(tasks) as i UNNEST i as j WHERE j = 'Search'  " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select %s.name from %s use index (`#primary`)  UNNEST tokens(tasks) as i UNNEST i as j WHERE j = 'Search'  " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)
                expected_result = self.run_cbq_query()

                self.assertEqual(sorted(actual_result['results']),sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_join_unnest_alias_tokens(self):
        for bucket in self.buckets:
            created_indexes=[]
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s) END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                # idx5 = "idxVM3"
                # self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.os FOR x within %s END) USING %s" % (
                #     idx5, bucket.name, "VMs", self.index_type)
                # # if self.gsi_type:
                # #     self.query += " WITH {'index_type': 'memdb'}"
                # create_result = self.run_cbq_query()
                # self._wait_for_index_online(bucket, idx5)
                # self._verify_results(create_result['results'], [])
                # created_indexes.append(idx5)

                #self.assertTrue(self._is_index_in_list(bucket, idx5), "Index is not in list")

                self.query = "explain SELECT x FROM default emp1 USE INDEX(%s)  UNNEST tokens(emp1.VMs) as x  JOIN default task ON KEYS meta(`emp1`).id where x.RAM > 1 and x.RAM < 5  ;" %(idx4)
                actual_result = self.run_cbq_query()
                plan = ExplainPlanHelper(actual_result)
                result1 =plan['~children'][0]['scan']['index']
                self.assertTrue(result1==idx4)
                self.query = "SELECT x FROM default emp1 USE INDEX(%s) UNNEST tokens(emp1.VMs) as x JOIN default task ON KEYS meta(`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;"%(idx4)
                actual_result = self.run_cbq_query()
                self.query = "SELECT x FROM default emp1 USE INDEX(`#primary`)  UNNEST tokens(emp1.VMs) as x  JOIN default task ON KEYS meta(`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) ==sorted(expected_result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_token(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s (department, DISTINCT (ARRAY lower(to_string(d)) for d in tokens(%s) END),join_yr,name)  " % (idx, bucket.name,"join_yr")
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s(`name`,(DISTINCT (array (`x`.`RAM`) for `x` in tokens(%s) end)),VMs)" % (
                    idx2, bucket.name, "VMs")
                # if self.gsi_type:
                #     self.query += " WITH {'index_type': 'memdb'}"
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                created_indexes.append(idx2)

                self.query = "EXPLAIN select name from %s WHERE ANY d in tokens(join_yr) satisfies lower(to_string(d)) = '2016' END " % (
                bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

		plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ("cover ((`%s`.`department`))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx)
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == ("cover ((distinct (array `v` for `v` in (`%s`.`join_yr`) end)))" % bucket.name))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['scan'][0]['covers'][2]) == ("cover ((`default`.`join_yr`)" ))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['scan'][0]['covers'][3]) == ("cover ((`default`.`name`)" ))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['scan'][0]['covers'][4]) == ("cover ((meta(`default`).`id`))" ))

                self.query = "select name from %s WHERE ANY d in tokens(join_yr) satisfies lower(to_string(d)) = '2016' END " % (
                bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from %s use index(`#primary`) WHERE ANY d in tokens(join_yr) satisfies lower(to_string(d)) = '2016' END " % (
                bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                expected_result = self.run_cbq_query()

                self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))

                self.query = "EXPLAIN select name from %s where (ANY x within tokens(VMs) SATISFIES x.RAM between 1 and 5  END ) " % (
                bucket.name) + \
                             "and name is not null ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

		plan = ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx2)
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['covers'][0]) == ("cover ((`%s`.`name`))" % bucket.name))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['covers'][1]) == ("cover (all (array (`x`.`RAM`) for `x` in (`%s`.`VMs`) end))" % bucket.name))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['covers'][2]) == ("cover ((`%s`.`VMs`)" % bucket.name))
                #self.assertTrue(str(plan['~children'][0]['~children'][0]['covers'][3]) == ("cover ((meta(`default`).`id`))" % bucket.name))

                self.query = "select name from `%s` where (ANY x within tokens(VMs) SATISFIES x.RAM between 1 and 5  END ) " % (
                bucket.name) + \
                             "and name is not null ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from `%s` use index(`#primary`) where (ANY x within tokens(VMs) SATISFIES x.RAM between 1 and 5  END ) " % (
                bucket.name) + \
                             "and name is not null ORDER BY name limit 10"
                expected_result = self.run_cbq_query()

                self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
