import json
import math
import re
import uuid
import time
from tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError


class QueriesIndexTests(QueryTests):

    FIELDS_TO_INDEX = [('name', 'job_title'), ('name', 'join_yr'), ('VMs', 'name')]
    COMPLEX_FIELDS_TO_INDEX = ['VMs', 'tasks_points', 'skills']
    def setUp(self):
        super(QueriesIndexTests, self).setUp()
        self.log.info("==============  QueriesIndexTests setup has started ==============")
        self.num_indexes = self.input.param('num_indexes', 1)
        if self.num_indexes > len(self.FIELDS_TO_INDEX):
            self.input.test_params["stop-on-failure"] = True
            self.log.error("MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED")
            self.fail('MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED')
        self.rest = RestConnection(self.master)
        self.shell = RemoteMachineShellConnection(self.master)
        self.delete_sample = self.input.param("delete_sample", False)
        self.log.info("==============  QueriesIndexTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(QueriesIndexTests, self).suite_setUp()
        self.log.info("==============  QueriesIndexTests suite_setup has started ==============")
        self.log.info("==============  QueriesIndexTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueriesIndexTests tearDown has started ==============")
        self.log.info("==============  QueriesIndexTests tearDown has completed ==============")
        super(QueriesIndexTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueriesIndexTests suite_tearDown has started ==============")
        self.log.info("==============  QueriesIndexTests suite_tearDown has completed ==============")
        super(QueriesIndexTests, self).suite_tearDown()

    '''MB-22321: test that ordered intersectscan is used for pagination use cases'''
    def test_orderedintersectscan(self):
        for bucket in self.buckets:
            self.cluster.bucket_delete(self.master, bucket=bucket, timeout=180000)
        rest = RestConnection(self.master)
        rest.load_sample("beer-sample")
        created_indexes = []
        try:
            idx = "idx_abv"
            self.query = "CREATE INDEX %s ON `beer-sample`(abv)" % (idx)
            self.run_cbq_query()
            time.sleep(15)
            idx2 = "idx_name"
            self.query = "CREATE INDEX %s ON `beer-sample`(name)" % (idx2)
            self.run_cbq_query()
            time.sleep(15)
            created_indexes.append(idx)
            created_indexes.append(idx2)
            self.query = "explain select * from `beer-sample` where name like 'A%' and abv > 0 order by abv limit 10"
            result = self.run_cbq_query()
            self.assertEqual(result['results'][0]['plan']['~children'][0]['~children'][0]['#operator'], 'OrderedIntersectScan')
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX `beer-sample`.%s USING %s" % (idx, self.index_type)
                self.run_cbq_query()
            if self.delete_sample:
                rest.delete_bucket("beer-sample")

    '''MB-22412: equality predicates and constant keys should be removed from order by clause'''
    def test_remove_equality_orderby(self):
        for bucket in self.buckets:
            self.cluster.bucket_delete(self.master, bucket=bucket, timeout=180000)
        rest = RestConnection(self.master)
        rest.load_sample("beer-sample")
        created_indexes = []
        try:
            idx = "idx_abv"
            self.query = "CREATE INDEX %s ON `beer-sample`(abv)" % (idx)
            self.run_cbq_query()
            time.sleep(15)
            created_indexes.append(idx)
            self.query = "EXPLAIN SELECT * FROM `beer-sample` WHERE abv = 0 ORDER BY abv"
            result = self.run_cbq_query()
            self.assertTrue('Order' not in result['results'][0]['plan'])
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX `beer-sample`.%s USING %s" % (idx, self.index_type)
                self.run_cbq_query()
            if self.delete_sample:
                rest.delete_bucket("beer-sample")

    '''MB-22470: Like matching should use suffixes index if it is available, also need to test token
       indexes'''
    def test_use_suffixes_and_tokens(self):
        for bucket in self.buckets:
            self.cluster.bucket_delete(self.master, bucket=bucket, timeout=180000)
        rest = RestConnection(self.master)
        rest.load_sample("beer-sample")
        created_indexes = []
        try:
            idx = "idx_name_suffixes"
            self.query = "CREATE INDEX %s ON `beer-sample`( DISTINCT ARRAY s FOR s IN SUFFIXES(name) " \
                         "END )" % (idx)
            self.run_cbq_query()
            time.sleep(15)
            created_indexes.append(idx)
            # Test that LIKE uses suffixes index
            self.query = "EXPLAIN SELECT name FROM `beer-sample` WHERE name LIKE '%21%'"
            result = self.run_cbq_query()
            self.assertTrue(result['results'][0]['plan']['~children'][0]['scan']['index'] == idx)

            # Test that contains uses suffixes index
            self.query = "EXPLAIN SELECT * FROM `beer-sample` WHERE CONTAINS( name, 'Cafe' )"
            result = self.run_cbq_query()
            self.assertTrue(result['results'][0]['plan']['~children'][0]['scan']['index'] == idx)

            idx2 = "idx_descr_tokens"
            self.query = "CREATE INDEX %s ON `beer-sample`( DISTINCT ARRAY t FOR t IN TOKENS" \
                         "(description) END )" % (idx2)
            self.run_cbq_query()
            time.sleep(15)
            created_indexes.append(idx2)
            # Test that has_token uses tokens index
            self.query = "EXPLAIN SELECT description FROM `beer-sample` WHERE HAS_TOKEN(description, 'Great' )"
            result = self.run_cbq_query()
            self.assertTrue(result['results'][0]['plan']['~children'][0]['scan']['index'] == idx2)
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX `beer-sample`.%s USING %s" % (idx, self.index_type)
                self.run_cbq_query()
            if self.delete_sample:
                rest.delete_bucket("beer-sample")

    def test_offset_orderby_limit(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s ( join_day,join_yr,_id ) USING %s" % (idx, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx = "idx2"
                self.query = "CREATE INDEX %s ON %s ( _id )  USING %s" % (idx, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.query = "Explain SELECT meta().id FROM %s WHERE join_day = 5 LIMIT 10" % bucket.name
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '10')
                self.query="explain SELECT meta().id FROM %s WHERE join_day > 5 LIMIT 5 OFFSET 10" % bucket.name
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '5')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '10')

                self.query="SELECT meta().id FROM %s WHERE join_day > 5 order by meta().id LIMIT 5 OFFSET 10" % bucket.name
                actual_result = self.run_cbq_query()
                self.query="SELECT meta().id FROM %s use index(`#primary`) WHERE join_day > 5 order by meta().id LIMIT 5 OFFSET 10" % bucket.name
                expected_result= self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query="SELECT meta().id FROM %s WHERE join_day > 5 order by meta().id LIMIT 5 OFFSET 3" % bucket.name
                actual_result = self.run_cbq_query()
                self.query="SELECT meta().id FROM %s use index(`#primary`) WHERE join_day > 5 order by meta().id LIMIT 5 OFFSET 3" % bucket.name
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query="explain SELECT meta().id FROM %s WHERE join_day > 5 order by _id LIMIT 5 OFFSET 0" % bucket.name
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], 'idx')
                self.assertTrue("limit" not in plan['~children'][0]['~children'][0])
                self.assertEqual(plan['~children'][0]['~children'][0]['#operator'], 'IndexScan3')
                self.query="explain SELECT meta().id FROM %s WHERE join_day > 5 LIMIT 5 OFFSET 0" % bucket.name
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '5')
                self.assertTrue("offset" not in plan['~children'][0]['~children'][0])

                self.query="SELECT meta().id FROM %s WHERE join_day > 5 order by _id LIMIT 5 OFFSET 0" % bucket.name
                actual_result = self.run_cbq_query()
                self.query="SELECT meta().id FROM %s use index(`#primary`) WHERE join_day > 5 order by _id LIMIT 5 OFFSET 0" % bucket.name
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query="explain SELECT meta().id FROM %s WHERE join_day > 5 LIMIT 5 OFFSET -1" % bucket.name
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '5')
                self.assertTrue("offset" not in plan['~children'][0]['~children'][0])
                self.query="SELECT meta().id FROM %s WHERE join_day > 5 order by meta().id LIMIT 5 OFFSET -1" % bucket.name
                actual_result = self.run_cbq_query()
                self.query="SELECT meta().id FROM %s use index(`#primary`) WHERE join_day > 5 order by meta().id LIMIT 5 OFFSET -1" % bucket.name
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query="explain SELECT meta().id FROM default WHERE join_day > 5  ORDER BY join_day LIMIT 5 OFFSET 20"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '5')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '20')
                self.query="SELECT meta().id FROM default WHERE join_day > 5  ORDER BY join_day,meta().id LIMIT 5 OFFSET 20"
                actual_result = self.run_cbq_query()
                self.query="SELECT meta().id FROM default use index(`#primary`) WHERE join_day > 5  ORDER BY join_day,meta().id LIMIT 5 OFFSET 20"
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])

                self.query="explain SELECT * FROM default WHERE join_day BETWEEN 0 AND 10 OR join_day >= 5	LIMIT 4 OFFSET 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['#operator'], 'IndexScan3')
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '4')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '10')

                self.query="explain SELECT * FROM default WHERE join_day BETWEEN 0 AND 10 OR _id like 'query-test%'	LIMIT 4 OFFSET 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['#operator'], 'UnionScan')
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '(4 + 10)')
                self.assertEqual(plan['~children'][0]['~children'][0]['scans'][0]['index'], 'idx')
                self.assertEqual(plan['~children'][0]['~children'][0]['scans'][1]['index'], 'idx2')

                idx2= "idx3"
                self.query = "CREATE INDEX %s ON %s ( VMs[0].RAM,_id  ) where join_yr > 2010  USING %s" %(idx2, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = "explain select * from %s where VMs[0].RAM > 5 and join_yr > 2010 order by VMs[0].RAM limit 2 offset 1"  % bucket.name
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '2')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '1')
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx2)
                self.query = 'explain SELECT * FROM default WHERE (VMs[0].RAM BETWEEN 2 AND 10 OR VMs[0].RAM >= 5) and join_yr >2010 limit 10 offset 100'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)

                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '10')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '100')
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx2)

                self.query = 'SELECT * FROM default WHERE (VMs[0].RAM BETWEEN 2 AND 10 OR VMs[0].RAM >= 5) and join_yr >2010 order by _id LIMIT 10 OFFSET 100'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT * FROM default use index(`#primary`) WHERE (VMs[0].RAM BETWEEN 2 AND 10 OR VMs[0].RAM >= 5) and join_yr >2010 order by _id LIMIT 10 OFFSET 100'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'explain SELECT * FROM default WHERE VMs[0].RAM BETWEEN 2 AND 10 and join_yr > 2010 LIMIT 5 OFFSET 5'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '5')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '5')
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx2)
                self.query = 'SELECT * FROM default WHERE VMs[0].RAM BETWEEN 2 AND 10 and join_yr > 2010 order by _id LIMIT 5 OFFSET 5'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT * FROM default use index(`#primary`) WHERE VMs[0].RAM BETWEEN 2 AND 10 and join_yr > 2010 order by _id LIMIT 5 OFFSET 5'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query="explain SELECT * FROM default WHERE join_day in [1,2,3,4,5,6,7,8,9,10] and join_yr != 2010 LIMIT 4 OFFSET 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '4')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '10')
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], 'idx')
                self.query="SELECT * FROM default WHERE join_day in [1,2,3,4,5,6,7,8,9,10] and join_yr != 2010 order by _id LIMIT 4 OFFSET 10"
                actual_result = self.run_cbq_query()
                self.query="SELECT * FROM default use index(`#primary`) WHERE join_day in [1,2,3,4,5,6,7,8,9,10] and join_yr != 2010 order by _id LIMIT 4 OFFSET 10"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query="explain SELECT * FROM default WHERE join_day not in [1,2,3,4,5,6,7,8,9,10] and join_yr != 2010 LIMIT 4 OFFSET 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']=='idx')
                self.query="SELECT * FROM default WHERE join_day not in [1,2,3,4,5,6,7,8,9,10] and join_yr != 2010 order by _id LIMIT 4 OFFSET 10"
                actual_result = self.run_cbq_query()
                self.query="SELECT * FROM default use index(`#primary`) WHERE join_day not in [1,2,3,4,5,6,7,8,9,10] and join_yr != 2010 order by _id LIMIT 4 OFFSET 10"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_pairs(self):
        self.query = "select pairs(self) from default order by meta().id limit 1"
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['metrics']['resultSize'] == 3608)

    def test_index_missing_null(self):
        self.fail_if_no_buckets()
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
                plan = self.ExplainPlanHelper(actual_result)
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
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index']==idx2)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_limit_index(self):
        self.fail_if_no_buckets()
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
                plan = self.ExplainPlanHelper(actual_result)
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
                plan1 = self.ExplainPlanHelper(actual_result)
                self.assertFalse("limit" in plan1["~children"][0]["~children"][0])
                self.query = "Explain  select * from {0} b1 nest {0} b2 on key b2._id FOR b1 where b1._id like 'query-testemployee%'  limit 5 ".format(bucket.name)
                actual_result = self.run_cbq_query()
                plan2 = self.ExplainPlanHelper(actual_result)
                self.assertFalse("limit" in plan2["~children"][0]["~children"][0])
                self.query = "Explain  select * from {0} b1 left nest {0} b2 on keys b1._id where b1._id like 'query-testemployee%' limit 10 ".format(bucket.name)
                actual_result = self.run_cbq_query()
                plan3 = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan3["~children"][1]['expr'] == "10")
                self.query = "Explain  select * from {0} b1 left nest {0} b2 on key b2._id FOR b1 where b1._id like 'query-testemployee%'  limit 10 ".format(bucket.name)
                actual_result = self.run_cbq_query()
                plan4 = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan4["~children"][1]['expr'] == "10")

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_ifmissing_ifnull(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                idx2 = "idx2"
                idx3 = "idx3"
                idx4 = "idx4"
                idx5 = "idx5"
                self.ensure_primary_indexes_exist()
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
                plan1 = self.ExplainPlanHelper(actual_result)
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
                plan2 = self.ExplainPlanHelper(actual_result)
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
                plan2 = self.ExplainPlanHelper(actual_result)
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
                plan2 = self.ExplainPlanHelper(actual_result)
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
                plan2 = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan2['~children'][0]['index']==idx5)
            finally:
                self.query = 'delete from {0} where meta().id = {1}'.format(bucket.name, "'k01'")
                self.run_cbq_query()
                self.query = 'delete from {0} where meta().id = {1}'.format(bucket.name, "'k02'")
                self.run_cbq_query()
                self.query = 'delete from {0} where meta().id = {1}'.format(bucket.name, "'k03'")
                self.run_cbq_query()
                self.query = 'delete from {0} where meta().id = {1}'.format(bucket.name, "'k04'")
                self.run_cbq_query()
                self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_subdoc_field(self):
        self.fail_if_no_buckets()
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
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_intersect(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            self.query = 'select ARRAY_INTERSECT(join_yr,[2011,2012,2016,"test"], [2011,2016], [2012,2016]) as test from {0}'.format(bucket.name)
            actual_result = self.run_cbq_query()
            os = self.shell.extract_remote_info().type.lower()
            if os == "windows":
                number_of_doc = 1680
            else:
                number_of_doc = 10080
            self.assertEqual(actual_result['metrics']['resultCount'], number_of_doc)

    def test_in_spans(self):
        self.fail_if_no_buckets()
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
                self.query = 'explain select join_day from %s where join_day in ["5", $1] ' % bucket.name
                actual_result = self.run_cbq_query()
                plan1 = self.ExplainPlanHelper(actual_result)
                if self.index_type.lower() == 'gsi':
                    self.assertEqual(len(plan1['~children'][0]['spans']), 2)
                else:
                    self.assertEqual(len(plan1['~children'][0]['scan']['spans']), 2)
                self.query = 'explain select join_day from %s where join_day in [$1] ' % bucket.name
                actual_result = self.run_cbq_query()
                plan2 = self.ExplainPlanHelper(actual_result)
                self.assertEqual(len(plan2['~children'][0]['spans']), 1)
                self.query = 'explain select join_day from %s where join_day in  $1 ' % bucket.name
                actual_result = self.run_cbq_query()
                plan3 = self.ExplainPlanHelper(actual_result)
                self.assertEqual(len(plan3['~children'][0]['spans']), 1)
                if self.index_type.lower() == 'gsi':
                    self.assertEqual(plan3['~children'][0]['spans'][0]['range'][0]['low'], 'array_min($1)')
                else:
                    self.assertEqual(plan3['~children'][0]['spans'][0]['Range']['Low'][0], 'array_min($1)')
                self.query = 'explain select join_day from %s where join_day in  [] ' % (bucket.name)
                actual_result = self.run_cbq_query()
                plan4 = self.ExplainPlanHelper(actual_result)
                if self.index_type.lower() == 'gsi':
                    self.assertEqual(plan4['~children'][0]['spans'][0]['range'][0]['high'], "null")
                else:
                    self.assertEqual(plan4['~children'][0]['spans'][0]['Range']['High'][0], "null")

                if self.index_type.lower() == 'gsi':
                    self.assertEqual(plan4['~children'][0]['spans'][0]['range'][0]['high'], "null")
                else:
                    self.assertEqual(plan4['~children'][0]['spans'][0]['Range']['High'][0], "null")

                self.query = 'explain select join_day from %s where join_day in  [$1,$2,$3] ' % bucket.name
                actual_result = self.run_cbq_query()
                plan5 = self.ExplainPlanHelper(actual_result)
                if self.index_type.lower() == 'gsi':
                    self.assertEqual(len(plan5['~children'][0]['spans']), 3)
                else:
                    self.assertEqual(len(plan5['~children'][0]['scan']['spans']), 3)

                self.query = 'explain select join_day from %s where join_day in  [$1,$2,$3] limit 5 ' % bucket.name
                actual_result = self.run_cbq_query()
                plan6 = self.ExplainPlanHelper(actual_result)
                if self.index_type.lower() == 'gsi':
                    self.assertEqual(len(plan6['~children'][0]['~children'][0]['spans']), 3)
                else:
                    self.assertEqual(len(plan6['~children'][0]['~children'][0]['scan']['spans']), 3)
                self.assertEqual(plan6["~children"][1]['expr'] == "5" and plan6["~children"][1]['#operator'], 'Limit')

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_stable_scan(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s ( join_day )" % (idx,bucket.name) + "USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                idx = "idx2"
                self.query = "CREATE INDEX %s ON %s ( meta().id )" % (idx,bucket.name) + "USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
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
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_meta_cas_expiration(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
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
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] == 'idx3')
                self.query = 'explain SELECT  meta().id , meta().cas, meta().expiration FROM default where meta().id !="query-testemployee10231.2819054-0" or meta().cas > 0 or meta().expiration = 0'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator']=='UnionScan')
                self.assertTrue(plan['~children'][0]['scans'][0]['index'] in ['idx3', '#primary'] or plan['~children'][0]['scans'][1]['index'] in ['idx3', '#primary'])
                self.query = 'explain SELECT meta().cas, meta().expiration,meta().id FROM default where meta().cas = 1487875768758304768 and meta().expiration > 0'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index'] =='idx5')
                self.query = 'explain SELECT meta().cas, meta().expiration FROM default where meta().cas !=1487875768758304768 or meta().expiration != 0'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator']=='UnionScan')
                self.query = 'explain SELECT  meta().id  FROM default where meta().id in ["",null,"query-testemployee10231.2819054-0","query-testemployee9987.55838821-0"]'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] == "#primary")
                self.query = 'SELECT  meta().id  FROM default where meta().id in ["",null,"query-testemployee10231.2819054-0","query-testemployee9987.55838821-0"]'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT  meta().id  FROM default use index(`#primary`) where meta().id in ["",null,"query-testemployee10231.2819054-0","query-testemployee9987.55838821-0"]'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'explain SELECT  meta().cas,meta().id  FROM default where meta().cas > 1487875768758304768'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] == "idx6")
                self.query = 'explain SELECT  meta().expiration,meta().id  FROM default where meta().expiration > 0'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] == "idx2")
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_suffix(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            idx = "idx"
            self.query = "CREATE INDEX %s ON %s (( DISTINCT ARRAY s FOR s IN SUFFIXES ( LOWER( _id )  )" % (
            idx, bucket.name) + \
                         " END),LOWER(_id),department ,name)  " \
                         " USING %s" % (self.index_type)
            actual_result = self.run_cbq_query()
            self._wait_for_index_online(bucket, idx)
            self._verify_results(actual_result['results'], [])
            created_indexes.append(idx)
            self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
            self.query = "EXPLAIN select name from %s WHERE ANY s IN SUFFIXES( LOWER( _id ) ) " % (bucket.name) + \
                         " SATISFIES s LIKE 'query-test%' END" \
                         " AND  department = 'Manager' ORDER BY name "
            self.check_explain_covering_index(idx)
            self.query = "select name from %s WHERE ANY s IN SUFFIXES( LOWER( _id ) ) " % (bucket.name) + \
                         " SATISFIES s LIKE  'query-test%'  END" \
                         " AND  department = 'Manager' ORDER BY name "
            actual_result = self.run_cbq_query()
            for idx in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                self.run_cbq_query()
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
            self.query = "select name from %s  WHERE ANY s IN SUFFIXES( LOWER( _id ) ) " % (bucket.name) + \
                         " SATISFIES s LIKE  'query-test%'  END" \
                         " AND  department = 'Manager' ORDER BY name "

            expected_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'] == expected_result['results'])

    def test_simple_array_index_distinct(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY v FOR v in %s END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY x.RAM FOR x in %s END) USING %s" % (
                    idx2, bucket.name, "VMs", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = "EXPLAIN select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.log.info(plan)
                self.log.info(plan['~children'][0]['~children'][0]['#operator'])
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan'
                                or plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan')
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                self.query = "EXPLAIN select meta().id from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY v IN %s.join_yr SATISFIES v = 2014 END) " % (bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" not in str(plan))
                self.query = "EXPLAIN select meta().id from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "EXPLAIN select meta().id from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "OR (ANY v IN %s.join_yr SATISFIES v = 2014 END) " % (bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" not in str(plan))
                self.query = "select meta().id from default where ANY v IN join_yr SATISFIES v IN [ 2016,2014] END order by meta().id limit 3"
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from default use index (`#primary`) where ANY v IN join_yr SATISFIES v IN [ 2016,2014] END order by meta().id limit 3"
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = "select meta().id from default where ANY v IN join_yr SATISFIES v = 2016 OR v= 2014 END order by meta().id limit 3"
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])

                self.query = "select meta().id from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY v IN %s.join_yr SATISFIES v = 2014 END) order by meta().id limit 3" % (bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from %s use index(`#primary`) where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY v IN %s.join_yr SATISFIES v = 2014 END) order by meta().id limit 3" % (bucket.name)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = "select meta().id from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "OR (ANY v IN %s.join_yr SATISFIES v = 2014 END) order by meta().id limit 3" % (bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from %s use index(`#primary`) where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "OR (ANY v IN %s.join_yr SATISFIES v = 2014 END) order by meta().id limit 3" % (bucket.name)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                idx8 = "idxjoin_yr4"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY v FOR v in %s END,join_yr) USING %s" % (
                    idx8, bucket.name, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx8)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx8)
                self.assertTrue(self._is_index_in_list(bucket, idx8), "Index is not in list")
                self.query = "EXPLAIN select meta().id from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY v IN %s.join_yr SATISFIES v = 2014 END) " % (bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "EXPLAIN select meta().id from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "OR (ANY v IN %s.join_yr SATISFIES v = 2014 END) " % (bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select meta().id from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY v IN %s.join_yr SATISFIES v = 2014 END) order by meta().id limit 3" % (bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from %s use index(`#primary`) where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY v IN %s.join_yr SATISFIES v = 2014 END) order by meta().id limit 3" % (bucket.name)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])

                self.query = "select meta().id from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "OR (ANY v IN %s.join_yr SATISFIES v = 2014 END) order by meta().id limit 3" % (bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from %s use index(`#primary`) where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "OR (ANY v IN %s.join_yr SATISFIES v = 2014 END) order by meta().id limit 3" % (bucket.name)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])

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
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx8, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx8), "Index is in list")
                created_indexes.remove(idx8)

                idx3 = "idxjoin_yr2"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY v FOR v within %s END) USING %s" % (
                    idx3, bucket.name, "join_yr", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY x.RAM FOR x within %s END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result_within)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan'
                                or plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan')
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx3 or result1 == idx4)
                self.assertTrue(result2 == idx4 or result2 == idx3)

                self.query = "EXPLAIN select name from %s where any v within %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x within %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result_within = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result_within)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan'
                                or plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan',
                                "Intersect Scan is not being used in and query for 2 array indexes")
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result3 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result4 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
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
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_array_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s( ARRAY v FOR v in %s END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s( ARRAY x.RAM FOR x in %s END) USING %s" % (
                    idx2, bucket.name, "VMs", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = "select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                self.run_cbq_query()
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
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s(  ARRAY x.RAM FOR x within %s END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
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
        self.fail_if_no_buckets()
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
                    number_of_doc = 10080
                self.assertEqual(actual_result['metrics']['resultCount'], number_of_doc)
                self.query = "UPDATE {0} SET s.newField = 'newValue' FOR s IN ARRAY_FLATTEN (ARRAY i.Marketing FOR i IN tasks END, 1) END;".format(bucket.name)
                self.run_cbq_query()
                self.query = "select name from %s  WHERE ANY i IN tasks SATISFIES  (ANY j within i SATISFIES j='newValue' END) END ; " % (
                bucket.name)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['metrics']['resultCount'], number_of_doc)
                self.query = "UPDATE {0} SET i.Marketing = ( ARRAY OBJECT_ADD(s, 'newField', 'test' ) FOR s IN i.Marketing END ) FOR i IN tasks END;".format(bucket.name)
                self.run_cbq_query()
                self.query = "select name from %s  WHERE ANY i IN tasks SATISFIES  (ANY j within i SATISFIES j='newValue' END) END ; " % (
                bucket.name)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['metrics']['resultCount'], number_of_doc)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_covering_like_array_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        idx = "ix"
        for bucket in self.buckets:
            try:
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in %s.%s END) where VMs[0].os = 'ubuntu' USING %s" % (
                        idx, bucket.name,bucket.name, "tasks", self.index_type)
                actual_result =self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "explain select meta().id from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END and VMs[0].os = 'ubuntu'"  % (bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['scan']['index'] == idx)
                self.query = 'explain select meta().id from {0} WHERE ANY i IN tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1 like "{1}" end) END and VMs[0].os = "ubuntu"' .format(bucket.name,'Sou%')
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['scan']['index'] == idx)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_panic_in_null(self):
        queries = dict()
        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_1 = "explain select join_yr from default where join_yr IN [NULL]"
        verifier = lambda x: self.assertEqual(x['q_res'][0]['results'][0]['plan']['~children'][0]['index'], 'idx1')
        queries["a"] = {"indexes": [index_1], "queries": [query_1],
                        "asserts": [verifier]}
        self.query_runner(queries)

    def test_avoid_intersect_scan(self):
        created_indexes = []
        idx = "idx_country"
        idx2 = "gix_USMales"
        try:
            self.query = "CREATE INDEX {0} ON `default`(`address`[1][0].`country`) WHERE (`_id` = 'query-testemployee10194.855617-0')".format(idx)
            self.run_cbq_query()
            created_indexes.append(idx)
            self.query = "CREATE INDEX gix_USMales ON `default`(distinct (pairs(self))) WHERE (`_id` = 'query-testemployee10194.855617-0') and (`gender` = 'M') and (`address`[1][0].`country` = 'United States of America')"
            self.run_cbq_query()
            created_indexes.append(idx2)
            self.query = "explain select count(*) from default where _id = 'query-testemployee10194.855617-0' and address[1][0].country = 'United States of America' and gender = 'M'"
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue("cover" in str(plan))
            self.assertTrue("IntersectScan" in str(plan))
            self.assertTrue(plan['~children'][0]['scans'][0]['scan']['index']==idx2)
            self.query = "select count(*) from default where _id = 'query-testemployee10194.855617-0' and address[1][0].country = 'United States of America' and gender = 'M'"
            actual_result = self.run_cbq_query()
            self.query = "select count(*) from default use index(`#primary`) where _id = 'query-testemployee10194.855617-0' and address[1][0].country = 'United States of America' and gender = 'M'"
            expected_result = self.run_cbq_query()
            self.assertTrue(actual_result['results']==expected_result['results'])
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % ("default", idx, self.index_type)
                actual_result = self.run_cbq_query()
                self._verify_results(actual_result['results'], [])

    def test_OR_UnionScan(self):
        created_indexes = []
        try:
            self.query = "CREATE INDEX `k1` ON `default`(`k01`)"
            self.run_cbq_query()
            created_indexes.append('k1')
            self.query = "CREATE INDEX `k2` ON `default`(`k02`)"
            self.run_cbq_query()
            created_indexes.append('k2')
            self.query = "CREATE INDEX `k3` ON `default`(`k03`)"
            self.run_cbq_query()
            created_indexes.append('k3')
            self.query = 'explain SELECT  k01  FROM default where k01 > "abc" OR k02 > 123 or k03>10'
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['#operator']=='UnionScan')
            self.query = 'explain select * from default where k01 < 10 OR k02 < 20'
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['#operator']=='UnionScan')
            self.query = 'explain select * from default where k01 < 10 OR k02 < 20 OR k03 < 30'
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['#operator']=='UnionScan')
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % ("default", idx, self.index_type)
                actual_result = self.run_cbq_query()
                self._verify_results(actual_result['results'], [])

    def test_index_join(self):
        created_indexes = []
        idx = "idx_parent_chkey"
        idx2 = "idx_parent_chkeyw"
        try:
            self.query = 'insert into standard_bucket0 values("child1",{"x":1,"y":9,"z":999})'
            self.run_cbq_query()
            self.query = 'insert into default values("parent1",{"a":1,"b":9,"c":999,"chkey":"1"})'
            self.run_cbq_query()
            self.query = 'create index {0} on default(chkey)'.format(idx)
            self.run_cbq_query()
            created_indexes.append(idx)
            self.query = 'select default,standard_bucket0 from standard_bucket0 left outer join default on key ("child1" || default.chkey) for standard_bucket0'
            try:
                self.run_cbq_query()
            except CBQError as ex:
                    self.log.error(ex)
                    self.assertTrue(str(ex).find("No index available for join term default") != -1,
                                    "Error is incorrect.")
            else:
                    self.fail("There was no errors. Error expected: %s" % "No index available for join term default")

            self.query = 'select default,standard_bucket0 from standard_bucket0 left outer join default on key ("standard_bucket0" || default.chkey) for standard_bucket0'
            try:
                self.run_cbq_query()
            except CBQError as ex:
                    self.log.error(ex)
                    self.assertTrue(str(ex).find("No index available for join term default") != -1,
                                    "Error is incorrect.")
            else:
                    self.fail("There was no errors. Error expected: %s" % "No index available for join term default")

            self.query = 'create index {0} on default("standard_bucket0" || default.chkey)'.format(idx2)
            self.run_cbq_query()
            created_indexes.append(idx2)
            self.query = 'select default,standard_bucket0 from standard_bucket0 left outer join default on key ("standard_bucket0" || default.chkey) for standard_bucket0 order by meta(default).id limit 2'
            actual_result =self.run_cbq_query()
            self.assertTrue(actual_result['results']==[{u'standard_bucket0': {u'y': 9, u'x': 1, u'z': 999}}, {u'standard_bucket0': {u'tasks_points': {u'task1': 1, u'task2': 1}, u'name': u'employee-9', u'mutated': 0, u'skills': [u'skill2010', u'skill2011'], u'join_day': 9, u'email': u'9-mail@couchbase.com', u'test_rate': 10.1, u'join_mo': 10, u'join_yr': 2011, u'_id': u'query-testemployee10153.1877827-0', u'VMs': [{u'RAM': 10, u'os': u'ubuntu', u'name': u'vm_10', u'memory': 10}, {u'RAM': 10, u'os': u'windows', u'name': u'vm_11', u'memory': 10}], u'job_title': u'Engineer'}}])
            self.query = 'delete from default use keys("parent1")'
            self.run_cbq_query()
            self.query = 'delete from standard_bucket0 use keys("child1")'
            self.run_cbq_query()
        finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % ("default", idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])

    def test_covering_index_collections(self):
        created_indexes = []
        idx = "ix"
        idx2 = "ix2"
        self.fail_if_no_buckets()
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
                self.check_explain_covering_index(idx)
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
                self.check_explain_covering_index(idx2)
                self.query = 'SELECT SUBSTR(account.transDate,0,10) As transDate, AVG(codes.weight) As avgWeight ' \
                             'FROM {0} account JOIN {0} codesDoc ON KEYS "codes-version-9" ' \
                             'LET codes = FIRST c for c IN codesDoc.codes WHEN c.code = account.code END ' \
                             'WHERE account.code != "" AND meta(account).id LIKE "account-customerXYZ-%" ' \
                             'AND SUBSTR(account.transDate,0,10) >= "2016-07-01" AND SUBSTR(account.transDate,0,10) < "2016-07-03"' \
                             'GROUP BY SUBSTR(account.transDate,0,10)'.format(bucket.name)
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'][0]['avgWeight']== 26.2466)
                self.assertTrue(actual_result['results'][0]['transDate']=='2016-07-02')
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                actual_result = self.run_cbq_query()
                self._verify_results(actual_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_pushdown_complex_filter(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                for bucket in self.buckets:
                    idx = "idxjoining"
                    self.query = "create index {1} on {0}(join_yr,join_day)".format(bucket.name,idx)
                    actual_result =self.run_cbq_query()
                    self._wait_for_index_online(bucket, idx)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(idx)
                    self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                    self.query = 'explain select meta().id from default where join_day between 0 and 5 and ' \
                                 'join_yr = 2011'
                    actual_result= self.run_cbq_query()
                    plan=self.ExplainPlanHelper(actual_result)
                    self.assertTrue(plan['~children'][0]['index']==idx)
                    self.assertTrue("covers" in str(plan))
                    self.query = 'select meta().id from default where join_day between 0 and 5 and ' \
                                 'join_yr = 2011'
                    actual_result= self.run_cbq_query(query_params={'profile' : 'timings'})
                    self.assertTrue("covers" in str( actual_result['profile']['executionTimings']['~children'][0]['~child']['~children']))
                    self.assertTrue(actual_result['profile']['executionTimings']['~children'][0]['~child']['~children'][0]['index'] == idx)
                    all_docs_list = self.generate_full_docs_list(self.gens_load)
                    expected_result = [doc for doc in all_docs_list if  doc['join_day'] >= 0 and doc['join_day'] <= 5 and doc['join_yr'] == 2011 ]
                    num_of_items = len(expected_result)
                    for i in [0,1,2]:
                        self.assertTrue(actual_result['profile']['executionTimings']['~children'][0]['~child']['~children'][1]['~children'][i]['#stats']['#itemsIn'] == num_of_items)
                        self.assertTrue(actual_result['profile']['executionTimings']['~children'][0]['~child']['~children'][1]['~children'][i]['#stats']['#itemsOut'] == num_of_items)

                    self.query = 'explain select meta().id from default where join_day between 0 and 5 and ' \
                                 'join_yr != 2011'
                    actual_result= self.run_cbq_query()
                    plan=self.ExplainPlanHelper(actual_result)
                    self.assertTrue(plan['~children'][0]['index'] == idx)
                    self.assertTrue("covers" in str(plan))
                    self.query = 'select meta().id from default where join_day between 0 and 5 and ' \
                                 'join_yr != 2011'
                    actual_result= self.run_cbq_query(query_params={'profile' : 'timings'})
                    self.assertTrue("covers" in str( actual_result['profile']['executionTimings']['~children'][0]['~child']['~children']))
                    self.assertTrue(actual_result['profile']['executionTimings']['~children'][0]['~child']['~children'][0]['index'] == idx)
                    expected_result = [doc for doc in all_docs_list if  doc['join_day'] >= 0 and doc['join_day'] <= 5 and doc['join_yr'] != 2011 ]
                    num_of_items = len(expected_result)
                    for i in [0,1,2]:
                        self.assertTrue(actual_result['profile']['executionTimings']['~children'][0]['~child']['~children'][1]['~children'][i]['#stats']['#itemsIn'] == num_of_items)
                        self.assertTrue(actual_result['profile']['executionTimings']['~children'][0]['~child']['~children'][1]['~children'][i]['#stats']['#itemsOut'] == num_of_items)

                    self.query = 'select meta().id from default where join_day between 0 and 5 and ' \
                                 'join_yr != 2015 order by meta().id'
                    actual_result= self.run_cbq_query()
                    self.query = 'select meta().id from default use index(`#primary`) where join_day between 0 and 5 and ' \
                                 'join_yr != 2015 order by meta().id'
                    expected_result= self.run_cbq_query()
                    self.assertTrue(actual_result['results']==expected_result['results'])

                    self.query = 'select meta().id from default where join_day between 0 and 5 and ' \
                                 'join_yr != 2011 order by meta().id'
                    actual_result= self.run_cbq_query()
                    self.query = 'select meta().id from default use index(`#primary`) where join_day between 0 and 5 and ' \
                                 'join_yr != 2011 order by meta().id'
                    expected_result= self.run_cbq_query()
                    self.assertTrue(actual_result['results']==expected_result['results'])
                    self.query = 'select meta().id from default where join_day between 0 and 5 and ' \
                                 'join_yr = 2015 order by meta().id'
                    actual_result= self.run_cbq_query()
                    self.query = 'select meta().id from default use index(`#primary`) where join_day between 0 and 5 and ' \
                                 'join_yr = 2015 order by meta().id'
                    expected_result= self.run_cbq_query()
                    self.assertTrue(actual_result['results']==expected_result['results'])
                    self.query = 'select meta().id from default where join_day between 0 and 5 and ' \
                                 'join_yr = 2011 order by meta().id'
                    actual_result= self.run_cbq_query()
                    self.query = 'select meta().id from default use index(`#primary`) where join_day between 0 and 5 and ' \
                                 'join_yr = 2011 order by meta().id'
                    expected_result= self.run_cbq_query()
                    self.assertTrue(actual_result['results']==expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_array_index_all(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY v FOR v in %s END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s( All ARRAY x.RAM FOR x in %s END) USING %s" % (
                    idx2, bucket.name, "VMs", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = "EXPLAIN select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan'
                                or plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan')
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                self.query = "select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x IN %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                self.run_cbq_query()
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
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within %s END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result_within)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan'
                                or plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan')
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx3 or result1 == idx4)
                self.assertTrue(result2 == idx4 or result2 == idx3)
                self.query = "EXPLAIN select name from %s where any v within %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND (ANY x within %s.VMs SATISFIES x.RAM between 1 and 5 END) " % (bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result_within = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result_within)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan'
                                or plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan',
                                "Intersect Scan is not being used in and query for 2 array indexes")
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result3 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result4 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s(department, DISTINCT ARRAY v FOR v in %s END,join_yr,name) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s(name,All ARRAY x.RAM FOR x in %s END,VMs) USING %s" % (
                    idx2, bucket.name, "VMs", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.query = "EXPLAIN select name from %s where any v in %s.join_yr satisfies v = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ("cover ((`%s`.`department`))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx)

                self.query = "EXPLAIN select name from %s where any join_yr in %s.join_yr satisfies join_yr = 2016 END " % (
                bucket.name, bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ("cover ((`%s`.`name`))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == ("cover ((all (array (`x`.`RAM`) for `x` in (`%s`.`VMs`) end)))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][2]) == ("cover ((`%s`.`VMs`))"  % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][3]) == ("cover ((meta(`%s`).`id`))" % "default"))

                self.query = "EXPLAIN select name from %s where (ANY foo within %s.VMs SATISFIES foo.RAM between 1 and 5  END ) " % (
                bucket.name, bucket.name) + \
                             "and name is not null ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
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
                plan = self.ExplainPlanHelper(actual_result)
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
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['scan']['index']==idx5)
                self.query = "EXPLAIN select name from %s WHERE tasks is not missing and name is not null " % (
                bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']=='nested_idx4')

                self.query = "EXPLAIN select name from %s WHERE ANY task IN %s.tasks SATISFIES  (ANY innertask IN task SATISFIES innertask='Search' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']=='nested_idx2')

                self.query = "EXPLAIN select name from %s WHERE tasks is not missing and name is not null " % (
                bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']=='nested_idx4')
                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i SATISFIES j='Search' end) END " % (
                bucket.name,bucket.name) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ("cover ((distinct (array (distinct (array `j` for `j` in `i` end)) for `i` in (`%s`.`tasks`) end)))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == ("cover ((`%s`.`tasks`))" % bucket.name))
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "nested_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxtasks"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY x FOR x in %s END) USING %s" % (
                    idx2, bucket.name, "tasks", self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result)
                if plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan':
                    self.assertEqual(plan['~children'][0]['~children'][0]['#operator'], 'IntersectScan')
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                    self.assertTrue(result1 == idx2 or result1 == idx)
                    self.assertTrue(result2 == idx or result2 == idx2)
                elif plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    self.assertEqual(plan['~children'][0]['~children'][0]['#operator'], 'UnionScan')
                    self.assertEqual(plan['~children'][0]['~children'][0]['scans'][0]['#operator'], 'IntersectScan')
                    self.assertEqual(plan['~children'][0]['~children'][0]['scans'][1]['#operator'], 'IntersectScan')
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                    self.assertTrue(result1 == idx2 or result1 == idx)
                    self.assertTrue(result2 == idx or result2 == idx2)
                self.run_cbq_query()
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithwhere%s" % ind
                    self.query = "CREATE INDEX %s ON %s(email, VMs) where join_day > 10 USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    index_name2 = "shortcoveringindex%s" % ind
                    self.query = "CREATE INDEX %s ON %s(email) where join_day > 10 USING %s" % (index_name2, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name2)
                    created_indexes.append(index_name2)
                    self.query = "explain select email,VMs[0].RAM from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "select email,join_day from %s "  % (bucket.name) +\
                                 "where email LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    actual_result1 = self.run_cbq_query()

                    self.query = "explain select email from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and join_day > 10"
                    if self.covering_index:
                        self.check_explain_covering_index(index_name2)
                    self.query = " select email from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and join_day > 10"
                    actual_result2 = self.run_cbq_query()
                    expected_result = [{"email" : doc["email"]}
                               for doc in self.full_list
                               if re.match(r'.*@.*\..*', doc['email']) and \
                                  doc['join_day'] > 10]
                    self._verify_results(sorted(actual_result2['results']), sorted(expected_result))
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
                    self.query = " select email from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and join_day > 10 order by meta().id limit 10"
                    actual_result2 = self.run_cbq_query()
                    self.query = " select email from %s use index(`#primary`) where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and join_day > 10 order by meta().id limit 10"
                    result = self.run_cbq_query()
                    self.assertEqual((actual_result2['results']),(result['results']))
                    self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()

    def test_covering_nonarray_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxtasks"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY x FOR x in %s END) USING %s" % (
                    idx2, bucket.name, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")
                idx3 = "idxtasks0"
                self.query = "CREATE INDEX %s ON %s(tasks[1],department) USING %s" % (
                    idx3, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxMarketing"
                self.query = "CREATE INDEX %s ON %s(tasks[0].Marketing,department) USING %s" % (
                    idx4, bucket.name, self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result)
                scan1 = plan['~children'][0]['~children'][0]
                self.assertTrue((scan1['#operator'] == 'IntersectScan') or
                                (scan1['#operator'] == 'UnionScan' and scan1['scans'][0]['#operator'] == 'IntersectScan' and scan1['scans'][1]['#operator'] == 'IntersectScan'),
                                "Single InteresectScan or UnionScan of two IntersectScans not being used")

                if 'scan' in plan['~children'][0]['~children'][0]['scans'][0]:
                    result1 =plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                else:
                    result1 =plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']

                if 'scan' in plan['~children'][0]['~children'][0]['scans'][1]:
                    result2 =plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                else:
                    result2 =plan['~children'][0]['~children'][0]['scans'][1]['scans'][0]['scan']['index']

                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                self.run_cbq_query()
                self.query = "explain select department from %s WHERE  tasks[1]='Sales'" % (bucket.name) + \
                             " AND  NOT (department = 'Manager') limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx3)
                self.assertTrue("cover" in str(plan))
                self.query = "select meta().id from %s WHERE  tasks[1]='Sales'" % (bucket.name) + \
                             " AND  NOT (department = 'Manager') order by department limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from %s use index(`#primary`) WHERE  tasks[1]='Sales'" % (bucket.name) + \
                             " AND  NOT (department = 'Manager') order by department limit 10"

                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))

                str1 = [{"region2": "International","region1": "South"},{"region2": "South"}]
                self.query = "explain select meta().id from {0} WHERE  tasks[0].Marketing={1}".format(bucket.name,str1) + \
                             "AND  NOT (department = 'Manager') order BY meta().id limit 10"
                actual_result = self.run_cbq_query()

                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index']==idx4)
                self.assertTrue("cover" in str(plan))

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

    #Test for MB-25590-Unnest Scan considers wrong array index
    def test_unnest_singlefield_array_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        try:
            self.query = "create index ix300 on default (VMs)"
            self.run_cbq_query()
            created_indexes.append("ix300")
            self.query = 'explain SELECT v.os FROM default USE index (ix300) UNNEST default.VMs AS v WHERE  v.os = "centos"'
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue("ix300" in str(plan))
            self.assertTrue("covers" in str(plan))
            self.query = 'SELECT v.os FROM default USE index (ix300) UNNEST default.VMs AS v WHERE  v.os = "centos"'
            actual_result = self.run_cbq_query()
            self.query = 'SELECT v.os FROM default USE index (`#primary`) UNNEST default.VMs AS v WHERE  v.os = "centos"'
            expected_result = self.run_cbq_query()
            self.assertEqual(sorted(actual_result['results']),sorted(expected_result['results']))
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX %s.%s USING %s" % ("default", idx, self.index_type)
                actual_result = self.run_cbq_query()
                self._verify_results(actual_result['results'], [])

    def test_simple_unnest_index_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "unnest_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( ALL array j for j in i end) FOR i in %s END,department,tasks,name) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(len(plan['~children'][0]['~children'][0]['scan']['covers']) == 5)

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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "unnest_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( ALL array j for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result)
                result1 =plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx )
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes=[]
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within %s END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")
                self.query = "explain SELECT x FROM default emp1 USE INDEX(%s)  UNNEST emp1.VMs as x  JOIN default task ON KEYS meta(`emp1`).id where x.RAM > 1 and x.RAM < 5  ;" %(idx4)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes=[]
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within %s END,VMs) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "explain SELECT x FROM default emp1 USE INDEX(%s)  UNNEST emp1.VMs as x  JOIN default task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5   ;" %(idx4)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "nested_idx_attr_nest"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")


                self.query = "explain SELECT emp.name FROM %s emp  UNNEST emp.tasks as i UNNEST i.Marketing as j where j.region1 = 'South'" % (bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "explain select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END"  % (
                bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr_nest"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in %s END,tasks,name) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")


                self.query = "explain SELECT emp.name FROM %s emp  UNNEST emp.tasks as i UNNEST i.Marketing as j where j.region1 = 'South'" % (bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( All array j.region1 for j in i.Marketing end) FOR i in %s END) USING %s" % (
                    idx2, bucket.name, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                idx3 = "nested_idx_attr3"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in %s END) USING %s" % (
                    idx3, bucket.name, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "nested_idx_attr4"
                self.query = "CREATE INDEX %s ON %s(DISTINCT ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in %s END,name,tasks) USING %s" % (
                    idx4, bucket.name, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END and name is not null " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.tasks SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY i FOR i within %s END) USING %s" % (
                    idx, bucket.name, "hobbies", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY i FOR i within %s END) USING %s" % (
                    idx2, bucket.name, "hobbies", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                self.query = "EXPLAIN select name from %s use index(%s) WHERE ANY i within %s.hobbies SATISFIES i = 'bhangra' END " % (
                bucket.name,idx2,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan', "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)

                self.query = "EXPLAIN select name from %s use index(%s) WHERE ANY i within %s.hobbies SATISFIES i = 'bhangra' END " % (
                bucket.name,idx,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan', "DistinctScan is not being used")
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j for j in i.dance end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan', "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.run_cbq_query()
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( all array j for j in i.dance end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan', "DistinctScan is not being used")
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( all array j for j in i.dance end) FOR i in %s END,hobbies.hobby,department,name) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN %s.hobbies.hobby SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( distinct array i FOR i in %s END) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( distinct array i FOR i in %s END,hobbies.hobby,name) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            try:
                created_indexes = []
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(%s) " %(idx,bucket.name,'meta().id')+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select count(1) from %s WHERE meta().id like '%s' " %(bucket.name,'query-test%')

                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                result1 = plan['~children'][0]['index']
                self.assertTrue(result1 == idx)
                self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()
                self.sleep(15,'wait for index')
                self.query = "select count(1) from %s use index(idx) WHERE meta().id like '%s' " %(bucket.name,'query-test%')
                actual_result = self.run_cbq_query()
                self.query = "select count(1) from %s use index(`#primary`) WHERE meta().id like '%s' " %(bucket.name,'query-test%')
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.assertTrue(actual_result['results']==[{u'$1': self.docs_per_day*2016}])
                self.assertTrue("index_group_aggs" in str(plan))
                self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], "COUNT")
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'IndexScan3',
                    "IndexScan3 is not being used")
                self.query = "explain select a.cnt from (select count(1) as cnt from default where meta().id is not null) as a"
                actual_result2 = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result2)
                self.assertTrue(
                    plan['~children'][0]['#operator'] != 'IndexScan3',
                    "IndexScan3 should not be used in subquery")
                self.query = "select a.cnt from (select count(1) as cnt from default where meta().id is not null) as a"
                actual_result2 = self.run_cbq_query()
                self.query = "select a.cnt from (select count(1) as cnt from %s where _id is not null) as a " %(bucket.name)
                result = self.run_cbq_query()
                self.assertEqual(sorted(actual_result2['results']),sorted(result['results']))
                self.assertTrue(actual_result2['results']==[{u'cnt': self.docs_per_day*2016}])
                self.query = "select count(DISTINCT 1) from %s WHERE meta().id like '%s' " %(bucket.name,'query-test%')
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==[{u'$1': 1}])
                self.query = "select count(DISTINCT meta().id) from %s use index(idx) WHERE meta().id like '%s' " %(bucket.name,'query-test%')
                actual_result = self.run_cbq_query()
                self.query = "select count(DISTINCT meta().id) from %s use index(`#primary`) WHERE meta().id like '%s' " %(bucket.name,'query-test%')
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.assertTrue(actual_result['results']==[{u'$1': self.docs_per_day*2016}])
                self.query = "select count(1) from %s use index(`#primary`) WHERE meta().id like '%s'  " %(bucket.name,'query-test%')
                result = self.run_cbq_query()
                self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                    self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                    self.run_cbq_query()

    def test_index_projection(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(%s,%s) " %(idx,bucket.name,'join_yr','_id')+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select join_yr from %s WHERE _id like '%s' and join_yr=2010 " %(bucket.name,'query-test%')
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [0, 1])

                self.query = "EXPLAIN select count(join_yr) from %s WHERE _id like '%s' and join_yr=2010 " %(bucket.name,'query-test%')
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')

                self.query = "select join_yr from %s WHERE _id like '%s' and join_yr=2010 order by meta().id limit 10" %(bucket.name,'query-test%')
                actual_result = self.run_cbq_query()
                self.query = "select join_yr from %s use index(`#primary`) WHERE _id like '%s' and join_yr=2010 order by meta().id limit 10" %(bucket.name,'query-test%')
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "EXPLAIN select count(DISTINCT join_yr) from %s WHERE _id like '%s' and join_yr=2010 " %(bucket.name,'query-test%')
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = 'EXPLAIN select _id from %s WHERE _id= "query-testemployee10317.9004497-0"' %(bucket.name)
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index'], '#primary')

                self.query = "EXPLAIN select count( DISTINCT join_yr) from %s WHERE join_yr is not null " %(bucket.name)
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "EXPLAIN select count(DISTINCT _id) from %s WHERE _id is not null " %(bucket.name)
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index'], '#primary')

                self.query = "select join_yr from %s WHERE join_yr=2010 order by meta().id limit 10" %(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select join_yr from %s use index(`#primary`) WHERE join_yr=2010 order by meta().id limit 10" %(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "select count(join_yr) from %s WHERE join_yr=2010 " %(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select count(join_yr) from %s use index(`#primary`) WHERE join_yr=2010 " %(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertEqual(sorted(actual_result['results']), sorted(expected_result['results']))

                self.query = "EXPLAIN select meta().id,join_yr from %s WHERE join_yr=2010 " %(bucket.name)
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [0])

                self.query = "EXPLAIN select count(meta().id),count(join_yr) from %s WHERE join_yr is not missing" %(bucket.name)
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [3, 4])

                self.query = "EXPLAIN select meta().id,join_yr from %s WHERE join_yr between 2010 and 2012 " %(bucket.name)
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [0])

                self.query = "EXPLAIN select count(meta().id),count(join_yr) from %s WHERE join_yr between 2010 and 2012 " %(bucket.name)
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [3, 4])

                self.query = "select meta().id,join_yr from %s WHERE join_yr is not missing order by meta().id limit 10" %(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id,join_yr from %s use index(`#primary`)  WHERE join_yr is not missing order by meta().id limit 10" %(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "select meta().id,join_yr from %s WHERE join_yr between 2010 and 2012 order by meta().id limit 10" %(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id,join_yr from %s use index(`#primary`)  WHERE join_yr between 2010 and 2012 order by meta().id limit 10" %(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "select count(meta().id),count(join_yr) from %s WHERE join_yr between 2010 and 2012 " %(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = "select count(meta().id),count(join_yr) from %s use index(`#primary`) WHERE join_yr between 2010 and 2012 " %(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = 'explain select _id,meta().id from %s WHERE join_yr > 2010' %(bucket.name)
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertTrue("index_projection" not in str(plan))

                self.query = 'select _id,meta().id from %s WHERE join_yr > 2010 order by meta().id limit 10' %(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = 'select _id,meta().id from %s use index(`#primary`)  WHERE join_yr > 2010 order by meta().id limit 10' %(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query= 'explain select join_yr from default where join_yr = 2010 or join_yr = 2012'
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [0])

                self.query= 'select join_yr from default where join_yr = 2010 or join_yr = 2012'
                actual_result = self.run_cbq_query()
                self.query= 'select join_yr from default use index(`#primary`) where join_yr = 2010 or join_yr = 2012'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = 'explain select join_yr,join_day from %s where join_yr in [2010,2011,2012]'  %(bucket.name)
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection'], {u'primary_key': True})

                self.query = 'explain select count(join_yr),count(join_day) from %s where join_yr in [2010,2011,2012]'  %(bucket.name)
                actual_result = self.run_cbq_query()
                plan=self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection'], {u'primary_key': True})

                self.query = 'select join_yr,join_day from %s where join_yr in [2010,2011,2012] order by meta().id limit 10' %(bucket.name)
                actual_result = self.run_cbq_query()
                self.query = 'select join_yr,join_day from %s use index(`#primary`) where join_yr in [2010,2011,2012] order by meta().id limit 10' %(bucket.name)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_count_distinct(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(%s) " %(idx,bucket.name,'job_title')+\
                             "where email  like '%@%.%' " + \
                             "USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "explain select count(1) from default where job_title IN ['Support','',null,'Engineer'] AND email like '%@%.%'"
                actual_result=self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')

                self.query = "select count(1) from default where job_title IN ['Support','',null,'Engineer'] AND email like '%@%.%'"
                actual_result=self.run_cbq_query()
                self.query = "select count(1) from default use index(`#primary`) where job_title IN ['Support','',null,'Engineer'] AND email like '%@%.%'"
                expected_result=self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(1) from default where job_title ='Support' AND  email like '%@%.%'"
                actual_result=self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')

                self.query = "select count(1) from default where job_title ='Support' AND  email like '%@%.%'"
                actual_result=self.run_cbq_query()
                self.query = "select count(1) from default use index(`#primary`) where job_title ='Support' AND  email like '%@%.%'"
                expected_result=self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(1) from default where (job_title ='Support' or job_title is not null) AND email like '%@%.%'"
                actual_result=self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(1) from default where (job_title ='Support' or job_title is not null) AND email like '%@%.%'"
                actual_result=self.run_cbq_query()
                self.query = "select count(1) from default  use index(`#primary`) where (job_title ='Support' or job_title is not null) AND email like '%@%.%'"
                expected_result=self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = "explain select count(1) from default where (job_title ='Support' or job_title is not missing) AND email like '%@%.%'"
                actual_result=self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')

                self.query = "select count(1) from default where (job_title ='Support' or job_title is not missing) AND email like '%@%.%'"
                actual_result=self.run_cbq_query()
                self.query = "select count(1) from default use index(`#primary`) where (job_title ='Support' or job_title is not missing) AND email like '%@%.%'"
                expected_result=self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query ="explain select count(1) from default where (job_title ='Support' or job_title = 'Engineer') AND email like '%@%.%'"
                actual_result=self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query ="select count(1) from default where (job_title ='Support' or job_title = 'Engineer') AND email like '%@%.%'"
                actual_result=self.run_cbq_query()
                self.query ="select count(1) from default use index(`#primary`)  where (job_title ='Support' or job_title = 'Engineer') AND email like '%@%.%'"
                expected_result=self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                idx2 = "idx2"
                self.query = "CREATE INDEX %s ON %s(%s,%s) " %(idx2,bucket.name,'VMs[1].os','tasks_points.task1')+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)

                idx = "idx3"
                self.query = "CREATE INDEX %s ON %s(%s,%s) where tasks_points.task1=1 " %(idx,bucket.name,'VMs[1].os','name')+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.query = "explain select count(distinct VMs[1].os) from default where VMs[1].os='windows' and tasks_points.task1>1"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "select count(distinct VMs[1].os) from default where VMs[1].os='windows' and tasks_points.task1>1"
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from default use index(`#primary`) where VMs[1].os='windows' and tasks_points.task1>1"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from default where VMs[1].os='windows' and tasks_points.task1 in [1,2,3]"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "select count(distinct VMs[1].os) from default where VMs[1].os='windows' and tasks_points.task1 in [1,2,3]"
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from default use index(`#primary`) where VMs[1].os='windows' and tasks_points.task1 in [1,2,3]"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from default where VMs[1].os='windows' or tasks_points.task1>1"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(distinct VMs[1].os) from default where VMs[1].os='windows' or tasks_points.task1>1"
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from default use index(`#primary`)  where VMs[1].os='windows' or tasks_points.task1>1"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from default where VMs[1].os='windows' and tasks_points.task1!=1"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertEqual( plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(distinct VMs[1].os) from default where VMs[1].os='windows' and tasks_points.task1!=1"
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from default  use index(`#primary`) where VMs[1].os='windows' and tasks_points.task1!=1"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from default where VMs[1].os='windows' and tasks_points.task1=1"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "select count(distinct VMs[1].os) from default where VMs[1].os='windows' and tasks_points.task1=1"
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from default use index(`#primary`) where VMs[1].os='windows' and tasks_points.task1=1"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from default where VMs[1].os in ['centos','windows','ubuntu'] and tasks_points.task1=1"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "explain select count(distinct VMs[1].os) from default where VMs[1].os in ['centos','windows','windows','centos','ubuntu','ubuntu',null] and tasks_points.task1=1"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "select count(distinct VMs[1].os) from default where VMs[1].os in ['centos','windows','ubuntu'] and tasks_points.task1=1"
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from default use index(`#primary`) where VMs[1].os in ['centos','windows','ubuntu'] and tasks_points.task1=1"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "select count(distinct VMs[1].os) from default where VMs[1].os in ['centos','windows','windows','centos','ubuntu','ubuntu',null] and tasks_points.task1=1"
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from default use index(`#primary`) where VMs[1].os in ['centos','windows','windows','centos','ubuntu','ubuntu',null] and tasks_points.task1=1"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from default where VMs[1].os in ['$1','centos','windows','ubuntu'] and tasks_points.task1=1"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "explain select count(distinct VMs[1].os) from default where VMs[1].os in ['centos','windows','ubuntu'] and tasks_points.task1 in [1,2,3]"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "select count(distinct VMs[1].os) from default where VMs[1].os in ['centos','windows','ubuntu'] and tasks_points.task1 in [1,2,3]"
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from default use index(`#primary`) where VMs[1].os in ['centos','windows','ubuntu'] and tasks_points.task1 in [1,2,3]"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct name) from default where VMs[1].os='windows' and name = 'employee-1' and tasks_points.task1>1"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(distinct name) from default where VMs[1].os='windows' and name = 'employee-1' and tasks_points.task1>1"
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct name) from default use index(`#primary`) where VMs[1].os='windows' and name = 'employee-1' and tasks_points.task1>1"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct name) from default  where VMs[1].os='windows' and name is not null and tasks_points.task1=1"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertEqual( plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(distinct name) from default  where VMs[1].os='windows' and name is not null and tasks_points.task1=1"
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct name) from default use index(`#primary`) where VMs[1].os='windows' and name is not null and tasks_points.task1=1"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct name) from default where VMs[1].os='windows' and name in ['employee-1','employee-2','employee-3']" \
                             " and tasks_points.task1=1"
                actual_result = self.run_cbq_query()
                plan =self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(distinct name) from default where VMs[1].os='windows' and name in ['employee-1','employee-2','employee-3']" \
                             " and tasks_points.task1=1"
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct name) from default use index(`#primary`) where VMs[1].os='windows' and name in ['employee-1','employee-2','employee-3']" \
                             " and tasks_points.task1=1"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_avoid_full_span(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = 'create index %s on default( join_yr,join_day )'%(idx)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "explain select * from default where join_yr in [2011,2012]"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['spans'][0]['range'], [{u'high': u'2011', u'low': u'2011', u'inclusion': 3}])
                self.assertEqual(plan['~children'][0]['spans'][1]['range'], [{u'high': u'2012', u'low': u'2012', u'inclusion': 3}])
                self.query = "select * from default where join_yr in [2011,2012] order by meta().id"
                actual_result = self.run_cbq_query()
                self.query = 'select * from default use index(`#primary`) where join_yr in [2011,2012] order by meta().id'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
                self.query = 'explain select * from default where join_yr =2011 and join_day in [1,2,3,$1,$2,null,""]'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['spans'][0]['range'], [{u'high': u'2011', u'low': u'2011', u'inclusion': 3}, {u'high': u'1', u'low': u'1', u'inclusion': 3}])
                self.assertEqual(plan['~children'][0]['spans'][1]['range'], [{u'high': u'2011', u'low': u'2011', u'inclusion': 3}, {u'high': u'2', u'low': u'2', u'inclusion': 3}])
                self.assertEqual(plan['~children'][0]['spans'][2]['range'], [{u'high': u'2011', u'low': u'2011', u'inclusion': 3}, {u'high': u'3', u'low': u'3', u'inclusion': 3}])
                self.assertEqual(plan['~children'][0]['spans'][3]['range'], [{u'high': u'2011', u'low': u'2011', u'inclusion': 3}, {u'high': u'$1', u'low': u'$1', u'inclusion': 3}])
                self.assertEqual(plan['~children'][0]['spans'][4]['range'], [{u'high': u'2011', u'low': u'2011', u'inclusion': 3}, {u'high': u'$2', u'low': u'$2', u'inclusion': 3}])
                self.assertEqual(plan['~children'][0]['spans'][5]['range'], [{u'high': u'2011', u'low': u'2011', u'inclusion': 3}, {u'high': u'""', u'low': u'""', u'inclusion': 3}])
                self.query = 'select * from default where join_yr =2011 and join_day in [1,2,3,null,""] order by meta().id'
                actual_result = self.run_cbq_query()
                self.query = 'select * from default use index(`#primary`) where join_yr =2011 and join_day in [1,2,3,null,""] order by meta().id'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_distinct_meta(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(%s,meta().id) " %(idx,bucket.name,'_id')+\
                             " where _id like '%s' " %('query-test%') + \
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "explain SELECT DISTINCT _id, meta().id FROM default WHERE _id like 'query-test%'"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][1]['~child']['~children'][2]['#operator'], "Distinct")
                self.query = "SELECT DISTINCT _id, meta().id FROM default WHERE _id like 'query-test%'"
                actual_result = self.run_cbq_query()
                self.query = "SELECT DISTINCT _id, meta().id FROM default use index(`#primary`) WHERE _id like 'query-test%'"
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                idx = "idx2"
                self.query = "CREATE INDEX %s ON %s(%s,meta().id) " %(idx,bucket.name,'_id')+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = 'explain SELECT DISTINCT _id, meta().id FROM default WHERE _id > "query-testemployee10317.9004497-0"'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][1]['~child']['~children'][2]['#operator'], "Distinct")
                self.query = 'SELECT DISTINCT _id, meta().id FROM default WHERE _id > "query-testemployee10317.9004497-0"'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT DISTINCT _id, meta().id FROM default use index(`#primary`) WHERE _id > "query-testemployee10317.9004497-0"'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = 'explain SELECT DISTINCT _id, meta().id FROM default WHERE _id in ["query-testemployee10317.9004497-0","query-testemployee10317.9004497-1","query-testemployee10317.9004497-2"]'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][1]['~child']['~children'][2]['#operator'], "Distinct")
                self.query = 'SELECT DISTINCT _id, meta().id FROM default WHERE _id in ["query-testemployee10317.9004497-0","query-testemployee10317.9004497-1","query-testemployee10317.9004497-2"]'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT DISTINCT _id, meta().id FROM default use index(`#primary`) WHERE _id in ["query-testemployee10317.9004497-0","query-testemployee10317.9004497-1","query-testemployee10317.9004497-2"]'
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_create_index_desc(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(%s DESC) " %(idx,bucket.name,'_id')
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_partial_like_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(%s) " %(idx,bucket.name,'_id')+\
                             " where _id like '%s' " %('query-test%') + \
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select meta().id from %s WHERE _id like '%s' " %(bucket.name,'query-test%')

                self.check_explain_covering_index(idx)

                self.query = "EXPLAIN select meta().id from %s WHERE _id like '%s' " %(bucket.name,'query-testemployee10%')

                self.check_explain_covering_index(idx)
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
        self.fail_if_no_buckets()
        num_docs = self.docs_per_day * 2016
        bucket_doc_map = {"default": num_docs}
        bucket_status_map = {"default": "healthy"}
        self.wait_for_buckets_status(bucket_status_map, 5, 120)
        self.wait_for_bucket_docs(bucket_doc_map, 5, 120)
        self.run_cbq_query('create primary index on {0} using GSI'.format("default"))
        self._wait_for_index_online("default", "#primary")
        try:
            self.query = 'select { UPPER("foo"):1,"foo"||"bar":2 }'
            actual_result = self.run_cbq_query()
            expected_result = [{u'$1': {u'foobar': 2, u'FOO': 1}}]
            self.assertEqual(actual_result['results'], expected_result)
            self.query = 'insert into {0} (key k,value doc)  select to_string(name)|| UUID() as k , doc as doc from {0} where name is not null'.format("default")
            self.run_cbq_query()
            self.query = 'select * from {0}'.format("default")
            new_num_docs = num_docs*2
            bucket_doc_map = {"default": new_num_docs}
            self.wait_for_bucket_docs(bucket_doc_map, 5, 120)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['metrics']['resultCount'], new_num_docs)
        finally:
            self.query = 'delete from {0} where meta().id in (select RAW to_string(name)|| UUID()  from {0} d)'.format("default")
            self.run_cbq_query()
            self.run_cbq_query("DROP PRIMARY INDEX ON {0}".format("default"))

    def test_between_spans(self):
        self.fail_if_no_buckets()
        if self.DGM == True:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
        for bucket in self.buckets:
            self.query = 'create index ix5 on %s(x)' % bucket.name
            self.run_cbq_query()
            self.query = 'insert into %s (KEY, VALUE) VALUES ("kk02",{"x":100,"y":101,"z":102,"id":"kk02"})'%(bucket.name)
            self.run_cbq_query()
            self.query = 'explain select count(1) from %s where x BETWEEN $1 AND $2' %(bucket.name)
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue('index_group_aggs' in str(plan))
            self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')
            self.query = 'explain select count(1) from %s where x >= $1 AND x <= $2'%(bucket.name)
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue('index_group_aggs' in str(plan))
            self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')
            self.query = 'drop index %s.ix5' %bucket.name
            self.run_cbq_query()
            self.query = 'delete from %s use keys ["kk02"]'%(bucket.name)
            self.run_cbq_query()

    def test_and_every_array_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            created_indexes = []
            try:
                self.query = 'CREATE INDEX `rec1-1_record_by_index_map` ON %s((distinct (array `i` for `i` in object_pairs(`indexMap`) end)))'%bucket.name
                self.run_cbq_query()
                created_indexes.append('rec1-1_record_by_index_map')
                self.query = 'CREATE INDEX `rec1-2_record_by_index_map` ON %s((distinct (array `i` for `i` in object_pairs(`data`) end)))'%bucket.name
                self.run_cbq_query()
                created_indexes.append('rec1-2_record_by_index_map')
                self.query = 'CREATE INDEX `rec1-3_record_by_index_map` ON %s((distinct (array `j` for `j` in object_pairs(`data`) end)))'%bucket.name
                self.run_cbq_query()
                created_indexes.append('rec1-3_record_by_index_map')
                self.query = 'insert into %s (KEY, VALUE) VALUES ("test",{"type":"testType","indexMap":{"key1":"val1", "key2":"val2"},"data":{"foo":"bar"}})'%(bucket.name)
                self.run_cbq_query()
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "value":"val1"} end AND any i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan')
                self.assertTrue(plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']=='rec1-1_record_by_index_map')
                self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND any i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
                actual_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results'][0]['doc']) == ([u'data', u'indexMap', u'type']))
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE every i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND any i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan')
                self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']=='rec1-1_record_by_index_map')
                self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE every i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end or any i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
                actual_result=self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results'][0]['doc']) == ([u'data', u'indexMap', u'type']))
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND every i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan')
                self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index']=='rec1-1_record_by_index_map')
                self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end or every i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
                actual_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results'][0]['doc']) == ([u'data', u'indexMap', u'type']))
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE some and every i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND some and every i in object_pairs(indexMap) satisfies i = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan')
                self.assertTrue(plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']=='rec1-1_record_by_index_map')
                self.query = 'insert into %s (KEY, VALUE) VALUES ("test1",{"type":"testType","indexMap":{"key1":"val1"},"data":{"foo":"bar"}})'%(bucket.name)
                self.run_cbq_query()
                self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE SOME AND EVERY i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND SOME AND EVERY i in object_pairs(data) satisfies i = { "name":"foo", "val":"bar"} end LIMIT 100'%(bucket.name)
                actual_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results'][0]['doc']) == ([u'data', u'indexMap', u'type']))
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND any j in object_pairs(indexMap) satisfies j = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan')
                self.assertTrue(plan['~children'][0]['~children'][0]['scans'][0]['scan']['index'] == 'rec1-1_record_by_index_map')
                self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE any i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND any j in object_pairs(indexMap) satisfies j = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
                actual_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results'][0]['doc']) == ([u'data', u'indexMap', u'type']))
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM %s AS r WHERE some and every i in object_pairs(indexMap) satisfies i = { "name":"key1", "val":"val1"} end AND some and every j in object_pairs(indexMap) satisfies j = { "name":"key2", "val":"val2"} end LIMIT 100'%(bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan')
                self.assertTrue(plan['~children'][0]['~children'][0]['scans'][0]['scan']['index'] == 'rec1-1_record_by_index_map')
                self.query = 'delete from %s use keys["test","test1"]'%bucket.name
                self.run_cbq_query()
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.`%s` USING %s" % (bucket.name, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_distinct_raw_orderby(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s(%s,%s) " %(idx,bucket.name,'name', 'join_day')+\
                             " USING %s" % (self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select distinct raw join_day from %s WHERE name like '%s' order by join_day limit 10" %(bucket.name,'query-test%')

                self.check_explain_covering_index(idx)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( all array i FOR i in %s END) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and (ANY i IN %s.hobbies.hobby SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "nested_inner_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan', "DistinctScan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_left_outer_join(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "outer_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "outer_join_all"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( All array j.city for j in i end) FOR i in %s END) USING %s" % (
                    idx2, bucket.name, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee  left JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx or result1 == idx2)
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee use index (%s) left JOIN default as new_project_full " % (bucket.name,idx2) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_inner_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in %s END,address,department) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT employee.department new_project " +\
                "FROM %s as employee  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "iregex"
                self.query = " CREATE INDEX %s ON %s( DISTINCT ARRAY REGEXP_LIKE(v.os,%s)  FOR v IN VMs END )  USING %s" % (
                  idx, bucket.name,"'ub%'", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select * from %s use index(`#primary`)  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1  END  " % (
                bucket.name,"'ub%'") + "order BY name limit 10"
                self.query = "select * from %s  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + "order BY name limit 10"
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "iregex"
                self.query = " CREATE INDEX %s ON %s( DISTINCT ARRAY REGEXP_LIKE(v.os,%s)  FOR v IN VMs END,VMs )  USING %s" % (
                  idx, bucket.name,"'ub%'", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select VMs from %s  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + "limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select VMs from %s  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + "limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select VMs from %s use index(`#primary`)  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,%s) = 1  END  " % (
                bucket.name,"'ub%'") + "limit 10"
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX %s ON %s(department, DISTINCT ARRAY GREATEST(v.RAM,100)  FOR v IN VMs END )  USING %s" % (
                    idx, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE department = 'Support' and ANY v IN VMs SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX %s ON %s(department, DISTINCT ARRAY GREATEST(v.RAM,100)  FOR v IN VMs END ,VMs,name)  USING %s" % (
                    idx, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE department = 'Support' and ANY v IN VMs SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in department end) USING %s" % (
                    idx, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + "FROM %s emp NEST %s items " % (bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.department SATISFIES  j = 'Support' end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + "FROM %s emp NEST %s items " % (bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.department SATISFIES  j = 'Support' end;"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select emp.name " + "FROM %s emp USE INDEX(`#primary`) NEST %s items " % (bucket.name, bucket.name) + \
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in department end) USING %s" % (idx, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + "FROM %s emp NEST %s VMs " % (bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.department SATISFIES  j != 'Engineer' end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + "FROM %s emp NEST %s VMs " % (bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.department SATISFIES  j = 'Support' end;"
                actual_result = self.run_cbq_query()
                actual_result = actual_result['results']
                self.query = "select emp.name " + "FROM %s emp USE INDEX(`#primary`)   NEST %s items " % (bucket.name, bucket.name) + \
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in join_yr end,name,join_yr ) USING %s" % (
                    idx, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxbetween"
                self.query = "CREATE INDEX %s ON %s(name) where join_yr between 2010 and 2012 USING %s" % (
                    idx2, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")
                self.query = "explain select name from %s where join_yr between 2010 and 2012 and name is not missing"%(bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['filter_covers']=={u'cover (((`default`.`join_yr`) between 2010 and 2012))': True, u'cover (((`default`.`join_yr`) <= 2012))': True, u'cover ((2010 <= (`default`.`join_yr`)))': True})
                self.query = "select name from %s where join_yr between 2010 and 2012 and name is not missing"%(bucket.name)
                actual_result1 = self.run_cbq_query()
                self.query = "explain select name from %s where join_yr >= 2010 and join_yr <=2012 and name is not null"%(bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['filter_covers']=={u'cover (((`default`.`join_yr`) between 2010 and 2012))': True, u'cover (((`default`.`join_yr`) <= 2012))': True, u'cover ((2010 <= (`default`.`join_yr`)))': True})
                self.query = "select name from %s where join_yr >= 2010 and join_yr <=2012 and name is not null"%(bucket.name)
                actual_result2 = self.run_cbq_query()
                self.assertTrue(sorted(actual_result1['results'])==sorted(actual_result2['results']))
                self.query = "EXPLAIN select emp.name " + "FROM %s emp NEST %s items " % (bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.join_yr SATISFIES  j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Distinct Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + "FROM %s emp NEST %s items " % (bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where ANY j IN emp.join_yr SATISFIES  j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select emp.name " + "FROM %s emp USE INDEX(`#primary`)  NEST %s items " % (bucket.name, bucket.name) + \
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in join_yr end,name,join_yr) USING %s" % (
                    idx, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + "FROM %s emp NEST %s items " % (bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN emp.join_yr SATISFIES  j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in join_yr end) USING %s" % (
                    idx, bucket.name, self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "indexwitharraysum%s" % ind
                    self.query = "CREATE INDEX %s ON %s(department, DISTINCT ARRAY round(v.memory + v.RAM) FOR v in VMs END ) where join_yr=2012 USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    created_indexes.append(index_name)
        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "EXPLAIN SELECT count(name),department" + \
                                 " FROM %s where join_yr=2012 AND ANY v IN VMs SATISFIES round(v.memory+v.RAM)<100 END AND department = 'Engineer'  GROUP BY department" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "indexwitharraysum%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,join_yr, DISTINCT ARRAY round(v.memory + v.RAM) FOR v in VMs END ) where join_yr=2012 USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    created_indexes.append(index_name)
        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "EXPLAIN SELECT count(name)" + \
                                 " FROM %s where join_yr=2012 AND name = 'query-testemployee10317.9004497-0'  GROUP BY name" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "IndexScan is not being used")
                    result1 = plan['~children'][0]['scan']['index']
                    self.assertTrue(result1 == index_name)
                    self.query = "SELECT count(name)" + \
                                 " FROM %s where join_yr=2012 AND name = 'query-testemployee10317.9004497-0'  GROUP BY name" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    self.query = "SELECT count(name)" + \
                                 " FROM %s use index(`#primary`) where join_yr=2012 AND name = 'query-testemployee10317.9004497-0'  GROUP BY name" % (bucket.name)
                    expected_result = self.run_cbq_query()
                    self.assertEquals(actual_result['results'],expected_result['results'])
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_array_index_substring(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxsubstr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY SUBSTR(j.FirstName,8) for j in name end) USING %s" % (
                    idx, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name, SUBSTR(name.FirstName, 8) as firstname from %s  where ANY j IN name SATISFIES substr(j.FirstName,8) != 'employee' end" % (bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "arrayidx_update"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                updated_value = 'new_dept' * 30
                self.query = "EXPLAIN update %s set name=%s where ANY i IN address SATISFIES  (ANY j IN i SATISFIES j.city='Mumbai' end) END  returning element department"  % (bucket.name, updated_value)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "arrayidx_delete"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY v FOR v in %s END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result)
                result = plan['~children'][0]['scan']['index']
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan', "DistinctScan is not being used in and query for 2 array indexes")
                self.assertTrue(result == idx )
                self.query = 'delete from %s where any v in join_yr satisfies v=2012 end LIMIT 1' % (bucket.name)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            finally:
                self.sleep(5, 'sleep for 5 seconds')
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_create_delete_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in xrange(self.num_indexes):
                    view_name = "my_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING GSI" % (
                                            view_name, bucket.name, ','.join(self.FIELDS_TO_INDEX[ind - 1]))
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in xrange(self.num_indexes):
                    view_name = "tuq_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s)  USING GSI" % (view_name, bucket.name, ','.join(self.FIELDS_TO_INDEX[ind - 1]))
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
        self.fail_if_no_buckets()
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                self.query = "CREATE INDEX %s ON %s(VMs, name)  USING %s" % (index_name, bucket.name,self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s where VMs is not null union SELECT count(name) FROM %s where name is not null' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]["~children"][0]['~children'][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_query_group_by(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                self.query = "CREATE INDEX %s ON %s(VMs, join_yr)  USING %s" % (index_name, bucket.name,self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(*) FROM %s where VMs is not null GROUP BY VMs, join_yr' % (bucket.name)
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_query_meta(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            index_name = "my_index_meta"
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                self.query = "CREATE INDEX %s ON %s(meta(%s).id, name)  USING %s" % (index_name, bucket.name, bucket.name,self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT name FROM %s WHERE meta(%s).id is not null and name is not null' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                self.run_cbq_query()

    def test_covering_meta(self):
        self.fail_if_no_buckets()
        self.query = 'CREATE INDEX `iy` ON `default`((distinct (array (`v`.`os`) for `v` in `VMs` end)))'
        self.run_cbq_query()
        self.query = 'EXPLAIN SELECT meta().id FROM default WHERE ANY v IN VMs SATISFIES v.os = "ubuntu" END'
        res = self.run_cbq_query()
        plan = self.ExplainPlanHelper(res)
        self.assertTrue('cover ((meta(`default`).`id`))' in str(plan['~children']))

    def test_covering_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if self.DGM == True:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindex%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name, join_day)  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT name, join_day FROM %s where name = 'employee-9'"% (bucket.name)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                        self.query = "EXPLAIN SELECT * FROM %s where name = 'employee-9'"% (bucket.name)
                        res = self.run_cbq_query()
                        plan = self.ExplainPlanHelper(res)
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
                self.query = "CREATE PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()
                self.sleep(15,'wait for index')
                self.query = "SELECT name, join_day FROM %s where name = 'employee-9'"  % (bucket.name)
                result = self.run_cbq_query()
                self.assertEqual(sorted(actual_result['results']),sorted(result['results']))
                self.query = "DROP PRIMARY INDEX ON %s" % bucket.name
                self.run_cbq_query()

    def test_index_like(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in xrange(self.num_indexes):
                    index_name = "index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name)  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT * FROM %s where name " % (bucket.name) +\
                                  "like 'xyz%'"
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]['index'] == index_name,"correct index is not used")
                self.assertTrue(plan["~children"][0]['spans'][0]["range"][0]["high"]=="\"xy{\"")
                self.assertTrue(plan["~children"][0]['spans'][0]["range"][0]["low"]=="\"xyz\"")
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_covering_partial_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if self.DGM == True:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithwhere%s" % ind
                    self.query = "CREATE INDEX %s ON %s(email, VMs, join_day) where join_day > 10 USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain select email,VMs[0].RAM from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithlimit%s" % ind
                    self.query = "CREATE INDEX %s ON %s(skills[0], join_yr, VMs[0].os,name) where join_yr =2010 USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain select  name,skills[0] as skills  from %s where skills[0]='skill2010' and join_yr=2010 and ( VMs[0].os IN ['ubuntu','windows','linux'] OR VMs[0].os IN ['ubuntu','windows','linux'] ) order by _id asc LIMIT 10 OFFSET 0;" % (bucket.name)
                    actual_result=self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue(plan["~children"][0]["~children"][0]["#operator"] == "IndexScan3")
                    self.assertTrue(plan["~children"][0]["~children"][0]["index"] == index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgroupby%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_yr,name) where join_yr >2009 USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT count(*),join_yr,name FROM %s where join_yr > 2009 GROUP BY join_yr,name ORDER BY name;" % (bucket.name)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT count(*),join_yr FROM %s  where join_yr > 2009 GROUP BY join_yr,name ORDER BY name;" % (bucket.name)
                    actual_result = self.run_cbq_query()
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgroupbyhaving%s" % ind
                    self.query = "CREATE INDEX %s ON %s(job_title,join_mo,test_rate) USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain SELECT join_mo, SUM(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Engineer' GROUP BY join_mo " +\
                         "HAVING SUM(employees.test_rate) > 0 and " +\
                         "SUM(test_rate) < 100000"
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgroupbyletting%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,test_rate) USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain SELECT join_mo, sum_test from %s WHERE join_mo>7 group by join_mo letting sum_test = sum(test_rate) " % (bucket.name)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        index_name_prefix_list = ['hint' + str(uuid.uuid4())[:4],'covering_hint' + str(uuid.uuid4())[:4]]
        created_indexes = []
        for bucket in self.buckets:
            for ind in index_name_prefix_list:
                    for attr in ['join_day', 'join_mo']:
                        ind_name = '%s_%s' % (ind, attr)
                        self.query = "CREATE INDEX %s ON %s(%s)  USING %s" % (ind_name,
                                                                    bucket.name, attr, self.index_type)
                        self.run_cbq_query()
                        self._wait_for_index_online(bucket, ind_name)
                        created_indexes.append('%s' % (ind_name))
        for bucket in self.buckets:
                try:
                    for ind in created_indexes:
                        if "covering" in ind:
                                if "join_day" in ind:
                                    self.query = 'EXPLAIN SELECT join_day FROM %s  USE INDEX(%s using %s) WHERE join_day>2' % (bucket.name, ind, self.index_type)
                                    self.check_explain_covering_index(ind)
                                elif "join_mo" in ind:
                                    self.query = 'EXPLAIN SELECT join_mo FROM %s  USE INDEX(%s using %s) WHERE join_mo>3' % (bucket.name, ind, self.index_type)
                                    self.check_explain_covering_index(ind)
                        else:
                            self.query = 'EXPLAIN SELECT name,join_day, join_mo FROM %s  USE INDEX(%s using %s) WHERE join_day>2 AND join_mo>3' % (bucket.name, ind, self.index_type)
                            res = self.run_cbq_query()
                            plan = self.ExplainPlanHelper(res)
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
#   SCALAR FN
##############################################################################################
    def test_ceil_covering_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithceil%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
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
                    self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithfloor%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
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
                    self.check_explain_covering_index(index_name)

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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgreatest%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,skills[0], skills[1])  USING %s" % (index_name, bucket.name,self.index_type)
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
                    self.check_explain_covering_index(index_name)

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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithleast%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,skills[0], skills[1])  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)
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
#   AGGR FN
##############################################################################################

    def test_min_covering_index(self):
        self.fail_if_no_buckets()
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
                    self.check_explain_covering_index(created_indexes[0])

                self.query = "explain select MIN(test_rate) from %s where test_rate is not missing and CONTAINS(test_rate,'z')" % (bucket.name)
                if self.covering_index:
                    self.check_explain_covering_index(created_indexes[1])

                self.query = "select MIN(test_rate) from %s where test_rate is not missing and CONTAINS(test_rate,'z')" % (bucket.name)
                actual_result = self.run_cbq_query()
                self.assertTrue(str(actual_result['results'][0]) == "{u'$1': None}")

                self.query = "explain select count(test_rate) from %s where test_rate is not missing and CONTAINS(test_rate,'z')" % (bucket.name)
                if self.covering_index:
                    self.check_explain_covering_index(created_indexes[1])

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

    def test_max_pushdown(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            try:
                created_indexes = []
                self.query = 'CREATE INDEX idx ON default(test_rate DESC,join_mO DESC)'
                self.run_cbq_query()
                created_indexes.append("idx")
                self.query = 'explain SELECT MAX(test_rate) FROM default WHERE test_rate > 10'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index']=="idx")
                self.query = 'SELECT MAX(test_rate) FROM default WHERE test_rate > 10'
                actual_result = self.run_cbq_query()
                self.query = 'SELECT MAX(test_rate) FROM default use index(`#primary`) WHERE test_rate > 10'
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results']==expected_result['results'])
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_max_covering_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithmax%s" % ind
                    self.query = "CREATE INDEX %s ON %s(job_title,join_mo,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraydistinct%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate,name)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)

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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraylength%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)

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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayappend%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name) USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)

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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayconcat%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name,email)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_array_prepend_covering_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayprepend%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_array_remove_covering_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayremove%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayavg%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraycontains%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraycount%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
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
                    self.check_explain_covering_index(index_name)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_array_distinct_covering_indexes(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraydistinct%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraymax%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraysum%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
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
                    self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraymin%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
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
                    self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                index_name = "coveringindexwitharrayput%s" % ind
                self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
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
                     self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharrayreplace%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,name)  USING %s" % (index_name, bucket.name,self.index_type)
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
                    self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwitharraysort%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,job_title,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
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
                    self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithpolylength%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,tasks_points,VMs,skills)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithmaximumlimit%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,name,mutated,skills,join_day,email,test_rate,join_yr,_id,VMs[0].RAM,VMs[1].os,job_title)  USING %s" % (index_name, bucket.name,self.index_type)
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
                        self.check_explain_covering_index(index_name)


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
        self.fail_if_no_buckets()
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
                        self.check_explain_covering_index(index_name)

                    self.query = "select count(_id) as count from %s where join_mo is not missing and _id is not null" % (bucket.name)
                    actual_result1 = self.run_cbq_query()
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                if (self.DGM == True):
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name, project)  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT employee.name, new_task.project FROM %s as employee JOIN %s as new_task ON KEYS ['key1']" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == "#primary",
                                    "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_explain_index_subquery(self):
        self.fail_if_no_buckets()
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
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == "#primary",
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_explain_childs_list_objects(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            index_name = "my_index_child2"
            try:
                self.query = "CREATE INDEX %s ON %s(distinct array vm.RAM for vm in VMs END)  USING %s" % (index_name, bucket.name,self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT VMs[0].RAM FROM %s ' % (bucket.name) + \
                        'WHERE ANY vm IN VMs SATISFIES vm.RAM > 5 end'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["scan"]['index']  == index_name,
                                "Index should be %s, but is: %s" % (index_name, plan))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_childs_objects(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            index_name = "my_index_obj"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points.task1, join_mo)  USING %s" % (index_name, bucket.name,self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE task_points.task1 > 0 and join_mo>7 '
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
                created_indexes = []
                try:
                    idx = "idx"
                    self.query = "CREATE INDEX %s ON %s(tokens(%s)) " %(idx,bucket.name,'_id')+\
                                 " USING %s" % (self.index_type)

                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, idx)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(idx)

                    self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                    self.query = "EXPLAIN select count(1) from %s WHERE tokens(_id) like '%s' " %(bucket.name,'query-test%')

                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue("covers" in str(plan))
                    result1 = plan['~children'][0]['index']
                    self.assertTrue(result1 == idx)
                    self.query = "select count(1) from %s WHERE tokens(_id) like '%s' " %(bucket.name,'query-test%')
                    actual_result = self.run_cbq_query()
                    self.assertTrue(
                        plan['~children'][0]['#operator'] == 'IndexScan3',
                        "IndexCountScan is not being used")
                    self.query = "explain select a.cnt from (select count(1) from default where tokens(_id) is not null) as a"
                    actual_result2 = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result2)
                    self.assertTrue(
                        plan['~children'][0]['#operator'] != 'IndexScan3',
                        "IndexCountScan should not be used in subquery")
                    self.query = "select a.cnt from (select count(1) from default where tokens(_id) is not null) as a"
                    actual_result2 = self.run_cbq_query()

                    for idx in created_indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                        self.run_cbq_query()
                        self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                        created_indexes.remove(idx)
                        self.query = "select count(1) from %s use index(`#primary`) WHERE tokens(_id) like '%s'  " %(bucket.name,'query-test%')
                        result = self.run_cbq_query()

                        self.assertEqual(sorted(actual_result['results']), sorted(result['results']))
                        self.query = "select a.cnt from (select count(1) from %s where _id is not null) as a " %(bucket.name)
                        result = self.run_cbq_query()
                        self.assertEqual(sorted(actual_result2['results']),sorted(result['results']))
                finally:
                    for idx in created_indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                        actual_result = self.run_cbq_query()
                        self._verify_results(actual_result['results'], [])
                        self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_join_unnest_tokens_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes=[]
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s) END,VMs) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)

                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "explain SELECT x FROM default emp1 USE INDEX(%s)  UNNEST tokens(emp1.VMs) as x  JOIN default task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5   ;" %(idx4)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxsubstr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY SUBSTR(j.FirstName,8) for j in tokens(name) end) USING %s" % (
                    idx, bucket.name, self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name, SUBSTR(name.FirstName, 8) as firstname from %s  where ANY j IN tokens(name) SATISFIES substr(j.FirstName,8) != 'employee' end" % (bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "arrayidx_update"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in tokens(i) end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                updated_value = 'new_dept' * 30
                self.query = "EXPLAIN update %s set name=%s where ANY i IN tokens(address) SATISFIES  (ANY j IN tokens(i) SATISFIES j.city='Mumbai' end) END  returning element department"  % (bucket.name, updated_value)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "arrayidx_delete"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY v FOR v in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)

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
                plan = self.ExplainPlanHelper(actual_result)
                result = plan['~children'][0]['scan']['index']
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan',
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in tokens(join_yr) end) USING %s" % (idx, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name FROM %s emp NEST %s items " % (bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where  ANY j IN tokens(emp.join_yr) SATISFIES  j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'DistinctScan', "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1, idx)
                self.query = "select emp.name FROM %s emp NEST %s items " % (bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.join_yr) SATISFIES  j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'])
                self.query = "select emp.name FROM %s emp USE INDEX(`#primary`)  NEST %s items " % (bucket.name, bucket.name) + \
                         "ON KEYS meta(`emp`).id where  ANY j IN tokens(emp.join_yr) SATISFIES  j between 2010 and 2012 end;"
                expected_result = self.run_cbq_query()
                expected_result = sorted(expected_result['results'])
                self.assertEqual(actual_result, expected_result)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nesttokens_keys_where_less_more_equal(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in tokens(join_yr) end) USING %s" % (
                    idx, bucket.name, self.index_type)

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
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "indexwitharraysum%s" % ind
                    self.query = "CREATE INDEX %s ON %s(department, DISTINCT ARRAY round(v.memory + v.RAM) FOR v in tokens(VMs) END ) where join_yr=2012 USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            try:
                for index_name in created_indexes:
                    self.query = "EXPLAIN SELECT count(name),department" + \
                                 " FROM %s where join_yr=2012 AND ANY v IN tokens(VMs) SATISFIES round(v.memory+v.RAM)<100 END AND department = 'Engineer'  GROUP BY department" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in tokens(department) end) USING %s" % (
                    idx, bucket.name, self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY j for j in tokens(department) end) USING %s" % (
                    idx, bucket.name, self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "iregex"
                self.query = " CREATE INDEX %s ON %s( DISTINCT ARRAY REGEXP_LIKE(v.os,%s)  FOR v IN tokens(VMs) END )  USING %s" % (
                  idx, bucket.name,"'ub%'", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s  WHERE ANY v IN tokens(VMs) SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "outer_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "outer_join_all"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( All array j.city for j in i end) FOR i in tokens(%s) END) USING %s" % (
                    idx2, bucket.name, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee use index (%s) left JOIN default as new_project_full " % (bucket.name,idx) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee use index (%s) left JOIN default as new_project_full " % (bucket.name,idx2) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "nested_inner_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "address", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " +\
                "FROM %s as employee  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( all array i FOR i in tokens(%s) END) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and (ANY i IN tokens(%s.hobbies.hobby) SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX %s ON %s(department, DISTINCT ARRAY GREATEST(v.RAM,100)  FOR v IN tokens(VMs) END )  USING %s" % (
                    idx, bucket.name, self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE department = 'Support' and ANY v IN tokens(VMs) SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( distinct array i FOR i in tokens(%s) END) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from %s WHERE department = 'Support' and (ANY i IN tokens(%s.hobbies.hobby) SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( all array j for j in i.dance end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY i FOR i within tokens(%s) END) USING %s" % (
                    idx, bucket.name, "hobbies", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY i FOR i within tokens(%s) END) USING %s" % (
                    idx2, bucket.name, "hobbies", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                self.query = "EXPLAIN select name from %s use index(%s) WHERE ANY i within tokens(%s.hobbies) SATISFIES i = 'bhangra' END " % (
                bucket.name,idx2,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)

                self.query = "EXPLAIN select name from %s use index(%s) WHERE ANY i within tokens(%s.hobbies) SATISFIES i = 'bhangra' END " % (
                bucket.name,idx,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j for j in i.dance end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( All array j.region1 for j in i.Marketing end) FOR i in tokens(%s) END) USING %s" % (
                    idx2, bucket.name, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                idx3 = "nested_idx_attr3"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end) FOR i in tokens(%s) END) USING %s" % (
                    idx3, bucket.name, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")

                idx4 = "nested_idx_attr4"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( ALL array j.region1 for j in i.Marketing end) FOR i in tokens(%s) END) USING %s" % (
                    idx4, bucket.name, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "EXPLAIN select name from %s USE INDEX(%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "EXPLAIN select name from %s USE INDEX(%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx2,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)

                self.query = "EXPLAIN select name from %s USE INDEX(%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx3,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx3)

                self.query = "EXPLAIN select name from %s USE INDEX(%s) WHERE ANY i IN tokens(%s.tasks) SATISFIES  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END " % (
                bucket.name,idx4,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr_nest"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( DISTINCT array j.region1 for j in tokens(i.Marketing) end) FOR i in %s END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")


                self.query = "explain SELECT emp.name FROM %s emp  UNNEST emp.tasks as i UNNEST tokens(i.Marketing) as j where j.region1 = 'South'" % (bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan',
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes=[]
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s) END,VMs) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "explain SELECT x FROM default emp1  UNNEST tokens(emp1.VMs) as x  JOIN default task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5   ;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = 'CREATE INDEX %s ON %s( ALL ARRAY v FOR v in tokens(%s,{"case":"lower"}) END) USING %s' % (
                    idx, bucket.name, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = 'CREATE INDEX %s ON %s( All ARRAY x.RAM FOR x in tokens(%s,{"names":true}) END) USING %s' % (
                    idx2, bucket.name, "VMs", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx2)

                self.query = 'EXPLAIN select name from %s where any v in tokens(%s.join_yr,{"case":"lower"}) satisfies v = 2016 END ' % (
                bucket.name, bucket.name) + \
                             'AND (ANY x IN tokens(%s.VMs,{"names":true}) SATISFIES x.RAM between 1 and 5 END) ' % (bucket.name) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan' or plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan')
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
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
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = 'CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s,{"names":true,"case":"lower"}) END) USING %s' % (
                    idx4, bucket.name, "VMs", self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result_within)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan' or plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan')
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx3 or result1 == idx4)
                self.assertTrue(result2 == idx4 or result2 == idx3)

                idx5 = "idxtokens1"
                self.query = 'CREATE INDEX %s ON %s( all ARRAY v FOR v within tokens(%s)  END) USING %s' % (

                    idx5, bucket.name, "join_yr", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx5)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx5)
                self.assertTrue(self._is_index_in_list(bucket, idx5), "Index is not in list")

                idx8 = "idxtokens2"
                self.query = 'CREATE INDEX %s ON %s( all ARRAY v FOR v within tokens(%s,{"names":true,"case":"lower","specials":false})  END) USING %s' % (

                    idx8, bucket.name, "join_yr", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx8)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx8)
                self.assertTrue(self._is_index_in_list(bucket, idx8), "Index is not in list")

                idx6 = "idxVM4"
                self.query = 'CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s,{"specials":false}) END) USING %s' % (
                    idx6, bucket.name, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx6)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx6)

                self.assertTrue(self._is_index_in_list(bucket, idx6), "Index is not in list")

                idx7 = "idxVM3"
                self.query = 'CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s,{"names":true,"case":"lower"}) END) USING %s' % (
                    idx7, bucket.name, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx7)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx7)

                self.assertTrue(self._is_index_in_list(bucket, idx7), "Index is not in list")

                self.query = 'EXPLAIN select name from %s where any v within tokens(%s.join_yr,{"case"}) satisfies v = 2016 END ' % (
                bucket.name, bucket.name) + \
                             'AND (ANY x within tokens(%s.VMs,{"specials"}) SATISFIES x.RAM between 1 and 5 END) ' % (bucket.name) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'

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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX %s ON %s( ALL ARRAY ( all array j for j in i.dance end) FOR i in tokens(%s) END,hobbies.hobby,department,name) USING %s" % (
                    idx, bucket.name, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from %s WHERE ANY i IN tokens(%s.hobbies.hobby) SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'" % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = 'CREATE INDEX %s ON %s( distinct array i FOR i in tokens(%s,{"names":true}) END,hobbies.hobby,name) WHERE (department = "Support")  USING %s' % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = 'EXPLAIN select name from %s WHERE department = "Support" and (ANY i IN tokens(%s.hobbies.hobby,{"names":true}) SATISFIES  i = "art" END) ' % (
                bucket.name,bucket.name) + \
                             'order BY name limit 10'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX %s ON %s( distinct array i FOR i in tokens(%s) END,hobbies.hobby,name) WHERE (department = 'Support')  USING %s" % (
                  idx, bucket.name, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from %s WHERE department = 'Support' and (ANY i IN tokens(%s.hobbies.hobby) SATISFIES  i = 'art' END) " % (
                bucket.name,bucket.name) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
             created_indexes = []
             try:
                idx = "nested_inner_join"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) FOR i in tokens(%s) END,address,department) USING %s" % (
                    idx, bucket.name, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT employee.department new_project " +\
                "FROM %s as employee  JOIN default as new_project_full " % (bucket.name) +\
                "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "iregex"
                self.query = " CREATE INDEX %s ON %s( DISTINCT ARRAY REGEXP_LIKE(v.os,%s)  FOR v IN tokens(VMs) END,VMs )  USING %s" % (
                  idx, bucket.name,"'ub%'", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select VMs from %s  WHERE ANY v IN tokens(VMs) SATISFIES REGEXP_LIKE(v.os,%s) = 1 END  " % (
                bucket.name,"'ub%'") + \
                             "limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)

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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX %s ON %s( department, DISTINCT ARRAY GREATEST(v.RAM,100)  FOR v IN tokens(VMs) END ,VMs,name )  USING %s" % (
                    idx, bucket.name, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = " EXPLAIN select name from %s WHERE department = 'Support' and ANY v IN tokens(VMs) SATISFIES GREATEST(v.RAM,100) END " % (
                    bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "unnest_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( ALL array j for j in i end) FOR i in tokens(%s) END,department,tasks,name) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
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
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
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
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx)
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == ("cover ((`%s`.`tasks`))" % bucket.name))
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s( ARRAY v FOR v in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s( ARRAY x.RAM FOR x in tokens(%s) END) USING %s" % (
                    idx2, bucket.name, "VMs", self.index_type)
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
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s(  ARRAY x.RAM FOR x within tokens(%s) END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "unnest_idx"
                self.query = "CREATE INDEX %s ON %s( DISTINCT ARRAY ( ALL array j for j in i end) FOR i in tokens(%s) END) USING %s" % (
                    idx, bucket.name, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self._verify_results(actual_result['results'], [])
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select %s.name from %s UNNEST tokens(tasks) as i UNNEST i as j WHERE j = 'Search' " % (
                bucket.name,bucket.name) + \
                             "AND (ANY x IN %s.tasks SATISFIES x = 'Sales' END) " % (bucket.name) + \
                             "AND  NOT (%s.department = 'Manager') order BY %s.name limit 10" % (bucket.name,bucket.name)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                result1 =plan['~children'][0]['~children'][0]['scan']['index']

                self.assertTrue(result1 == idx )
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes=[]
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX %s ON %s( aLL ARRAY x.RAM FOR x within tokens(%s) END) USING %s" % (
                    idx4, bucket.name, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self._verify_results(create_result['results'], [])
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "explain SELECT x FROM default emp1 USE INDEX(%s)  UNNEST tokens(emp1.VMs) as x  JOIN default task ON KEYS meta(`emp1`).id where x.RAM > 1 and x.RAM < 5  ;" %(idx4)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
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
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX %s ON %s (department, DISTINCT (ARRAY lower(to_string(d)) for d in tokens(%s) END),join_yr,name)  " % (idx, bucket.name,"join_yr")
                self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = "CREATE INDEX %s ON %s(`name`,(DISTINCT (array (`x`.`RAM`) for `x` in tokens(%s) end)),VMs)" % (
                    idx2, bucket.name, "VMs")
                self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                created_indexes.append(idx2)

                self.query = "EXPLAIN select name from %s WHERE ANY d in tokens(join_yr) satisfies lower(to_string(d)) = '2016' END " % (
                bucket.name) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == ("cover ((`%s`.`department`))" % bucket.name))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx)
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
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx2)
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
