import json
import math
import re
import time
import uuid

from membase.api.exception import CBQError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from .tuq import QueryTests
from deepdiff import DeepDiff


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
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)

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
        rest.load_sample(self.sample_bucket)
        created_indexes = []
        query_bucket = self.get_collection_name('`{0}`'.format(self.sample_bucket))
        try:
            time.sleep(5)
            self.wait_for_all_indexes_online()
            idx = "idx_abv"
            self.query = "CREATE INDEX {0} ON {1}(abv)".format(idx, query_bucket)
            self.run_cbq_query()
            self.wait_for_all_indexes_online()
            idx2 = "idx_name"
            self.query = "CREATE INDEX {0} ON {1}(name)".format(idx2, query_bucket)
            self.run_cbq_query()
            self.wait_for_all_indexes_online()
            created_indexes.append(idx)
            created_indexes.append(idx2)
            self.query = "explain select * from {0} where name like 'A%' and" \
                         " abv > 0 order by abv limit 10".format(query_bucket)
            result = self.run_cbq_query()
            self.assertEqual(result['results'][0]['plan']['~children'][0]['~children'][0]['#operator'],
                             'OrderedIntersectScan')
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                self.run_cbq_query()
            if self.delete_sample:
                rest.delete_bucket(self.sample_bucket)

    '''MB-22412: equality predicates and constant keys should be removed from order by clause'''

    def test_remove_equality_orderby(self):
        for bucket in self.buckets:
            self.cluster.bucket_delete(self.master, bucket=bucket, timeout=180000)
        rest = RestConnection(self.master)
        rest.load_sample(self.sample_bucket)
        query_bucket = self.get_collection_name("`{0}`".format(self.sample_bucket))
        created_indexes = []
        try:
            time.sleep(5)
            self.wait_for_all_indexes_online()
            idx = "idx_abv"
            self.query = "CREATE INDEX {0} ON {1}(abv)".format(idx, query_bucket)
            self.run_cbq_query()
            time.sleep(15)
            created_indexes.append(idx)
            self.query = "EXPLAIN SELECT * FROM {0} WHERE abv = 0 ORDER BY abv".format(query_bucket)
            result = self.run_cbq_query()
            self.assertTrue('Order' not in result['results'][0]['plan'])
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                self.run_cbq_query()
            if self.delete_sample:
                rest.delete_bucket(self.sample_bucket)

    '''MB-22470: Like matching should use suffixes index if it is available, also need to test token
       indexes'''

    def test_use_suffixes_and_tokens(self):
        for bucket in self.buckets:
            self.cluster.bucket_delete(self.master, bucket=bucket, timeout=180000)
        rest = RestConnection(self.master)
        rest.load_sample(self.sample_bucket)
        query_bucket = self.get_collection_name('`{0}`'.format(self.sample_bucket))
        created_indexes = []
        try:
            time.sleep(5)
            self.wait_for_all_indexes_online()
            idx = "idx_name_suffixes"
            self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY s FOR s IN SUFFIXES(name) " \
                         "END )".format(idx, query_bucket)
            self.run_cbq_query()
            time.sleep(15)
            created_indexes.append(idx)
            # Test that LIKE uses suffixes index
            self.query = "EXPLAIN SELECT name FROM {0} WHERE name LIKE '%21%'".format(query_bucket)
            result = self.run_cbq_query()
            self.assertTrue(result['results'][0]['plan']['~children'][0]['scan']['index'] == idx)

            # Test that contains uses suffixes index
            self.query = "EXPLAIN SELECT * FROM {0} WHERE CONTAINS( name, 'Cafe' )".format(query_bucket)
            result = self.run_cbq_query()
            self.assertTrue(result['results'][0]['plan']['~children'][0]['scan']['index'] == idx)

            idx2 = "idx_descr_tokens"
            self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY t FOR t IN TOKENS" \
                         "(description) END )".format(idx2, query_bucket)
            self.run_cbq_query()
            time.sleep(15)
            created_indexes.append(idx2)
            # Test that has_token uses tokens index
            self.query = "EXPLAIN SELECT description FROM {0} WHERE HAS_TOKEN(description, 'Great' )".format(
                query_bucket)
            result = self.run_cbq_query()
            self.assertTrue(result['results'][0]['plan']['~children'][0]['scan']['index'] == idx2)
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                self.run_cbq_query()
            if self.delete_sample:
                rest.delete_bucket(self.sample_bucket)

    def test_offset_orderby_limit(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1} ( join_day,join_yr,_id ) USING {2}".format(idx, query_bucket,
                                                                                                 self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx = "idx2"
                self.query = "CREATE INDEX {0} ON {1} ( _id )  USING {2}".format(idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.query = "Explain SELECT meta().id FROM {0} WHERE join_day = 5 LIMIT 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '10')
                self.query = "explain SELECT meta().id FROM {0} WHERE join_day > 5 LIMIT 5 OFFSET 10".format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '5')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '10')

                self.query = "SELECT meta().id FROM {0} WHERE join_day > 5 order by meta().id" \
                             " LIMIT 5 OFFSET 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "SELECT meta().id FROM {0} use index(`#primary`) WHERE join_day > 5" \
                             " order by meta().id LIMIT 5 OFFSET 10".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "SELECT meta().id FROM {0} WHERE join_day > 5 order by meta().id" \
                             " LIMIT 5 OFFSET 3".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "SELECT meta().id FROM {0} use index(`#primary`) WHERE join_day > 5" \
                             " order by meta().id LIMIT 5 OFFSET 3".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain SELECT meta().id FROM {0} WHERE join_day > 5 order by _id" \
                             " LIMIT 5 OFFSET 0".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], 'idx')
                self.assertTrue("limit" not in plan['~children'][0]['~children'][0])
                self.assertEqual(plan['~children'][0]['~children'][0]['#operator'], 'IndexScan3')
                self.query = "explain SELECT meta().id FROM {0} WHERE join_day > 5 LIMIT 5 OFFSET 0".format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '5')
                self.assertTrue("offset" not in plan['~children'][0]['~children'][0])

                self.query = "SELECT meta().id FROM {0} WHERE join_day > 5 order by _id" \
                             " LIMIT 5 OFFSET 0".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "SELECT meta().id FROM {0} use index(`#primary`) WHERE join_day > 5 order by _id" \
                             " LIMIT 5 OFFSET 0".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain SELECT meta().id FROM {0} WHERE join_day > 5 LIMIT 5 OFFSET -1".format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '5')
                self.assertTrue("offset" not in plan['~children'][0]['~children'][0])
                self.query = "SELECT meta().id FROM {0} WHERE join_day > 5 order by meta().id" \
                             " LIMIT 5 OFFSET -1".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "SELECT meta().id FROM {0} use index(`#primary`) WHERE join_day > 5 order by meta().id" \
                             " LIMIT 5 OFFSET -1".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain SELECT meta().id FROM {0} WHERE join_day > 5  ORDER BY join_day" \
                             " LIMIT 5 OFFSET 20".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '5')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '20')
                self.query = "SELECT meta().id FROM {0} WHERE join_day > 5  ORDER BY join_day,meta().id" \
                             " LIMIT 5 OFFSET 20".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "SELECT meta().id FROM {0} use index(`#primary`) WHERE join_day > 5  " \
                             "ORDER BY join_day,meta().id LIMIT 5 OFFSET 20".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])

                self.query = "explain SELECT * FROM {0} WHERE join_day BETWEEN 0 AND 10 OR join_day >= 5	" \
                             "LIMIT 4 OFFSET 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['#operator'], 'IndexScan3')
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '4')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '10')

                self.query = "explain SELECT * FROM {0} WHERE join_day BETWEEN 0 AND 10 OR _id like 'query-test%'" \
                             "	LIMIT 4 OFFSET 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['#operator'], 'UnionScan')
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '(4 + 10)')
                self.assertEqual(plan['~children'][0]['~children'][0]['scans'][0]['index'], 'idx')
                self.assertEqual(plan['~children'][0]['~children'][0]['scans'][1]['index'], 'idx2')

                idx2 = "idx3"
                self.query = "CREATE INDEX {0} ON {1} ( VMs[0].RAM,_id  ) where" \
                             " join_yr > 2010  USING {2}".format(idx2, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.query = "explain select * from {0} where VMs[0].RAM > 5 and join_yr > 2010 order by VMs[0].RAM " \
                             "limit 2 offset 1".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '2')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '1')
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx2)
                self.query = 'explain SELECT * FROM {0} WHERE (VMs[0].RAM BETWEEN 2 AND 10 OR VMs[0].RAM >= 5) and' \
                             ' join_yr >2010 limit 10 offset 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)

                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '10')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '100')
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx2)

                self.query = 'SELECT * FROM {0} WHERE (VMs[0].RAM BETWEEN 2 AND 10 OR VMs[0].RAM >= 5) and' \
                             ' join_yr >2010 order by _id LIMIT 10 OFFSET 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'SELECT * FROM {0} use index(`#primary`) WHERE (VMs[0].RAM BETWEEN 2 AND' \
                             ' 10 OR VMs[0].RAM >= 5) and join_yr >2010 order by _id' \
                             ' LIMIT 10 OFFSET 100'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = 'explain SELECT * FROM {0} WHERE VMs[0].RAM BETWEEN 2 AND 10 and' \
                             ' join_yr > 2010 LIMIT 5 OFFSET 5'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '5')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '5')
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], idx2)
                self.query = 'SELECT * FROM {0} WHERE VMs[0].RAM BETWEEN 2 AND 10 and' \
                             ' join_yr > 2010 order by _id LIMIT 5 OFFSET 5'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'SELECT * FROM {0} use index(`#primary`) WHERE VMs[0].RAM BETWEEN 2 AND' \
                             ' 10 and join_yr > 2010 order by _id LIMIT 5 OFFSET 5'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain SELECT * FROM {0} WHERE join_day in [1,2,3,4,5,6,7,8,9,10] and" \
                             " join_yr != 2010 LIMIT 4 OFFSET 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['~children'][0]['limit'], '4')
                self.assertEqual(plan['~children'][0]['~children'][0]['offset'], '10')
                self.assertEqual(plan['~children'][0]['~children'][0]['index'], 'idx')
                self.query = "SELECT * FROM {0} WHERE join_day in [1,2,3,4,5,6,7,8,9,10] and join_yr != 2010 " \
                             "order by _id LIMIT 4 OFFSET 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "SELECT * FROM {0} use index(`#primary`) WHERE join_day in [1,2,3,4,5,6,7,8,9,10] and" \
                             " join_yr != 2010 order by _id LIMIT 4 OFFSET 10".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain SELECT * FROM {0} WHERE join_day not in [1,2,3,4,5,6,7,8,9,10] and" \
                             " join_yr != 2010 LIMIT 4 OFFSET 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index'] == 'idx')
                self.query = "SELECT * FROM {0} WHERE join_day not in [1,2,3,4,5,6,7,8,9,10] and" \
                             " join_yr != 2010 order by _id LIMIT 4 OFFSET 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "SELECT * FROM {0} use index(`#primary`) WHERE join_day not in [1,2,3,4,5,6,7,8,9,10]" \
                             " and join_yr != 2010 order by _id LIMIT 4 OFFSET 10".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_pairs(self):
        query_bucket = self.query_buckets[0]
        self.query = "select pairs(self) from {0} order by meta().id limit 1".format(query_bucket)
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['metrics']['resultSize'], 3597)

    def test_index_missing_null(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1} ( VMs ) USING {2}".format(idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "Explain select * from {0} where 5 in VMs".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index'] == idx)
                idx2 = "idx2"
                self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.remove(idx)
                self.query = "CREATE INDEX {0} ON {1} ( VMs[0].RAM = 11 and join_yr = 2010  )" \
                             " USING {2}".format(idx2, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.query = "Explain select * from {0} where VMs[0].RAM = 11 and join_yr = 2010 ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index'] == idx2)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_limit_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1} ( department,_id )".format(idx, query_bucket) + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "Explain select * from {0} where ".format(query_bucket) + \
                             "department = 'Manager' and _id LIKE 'query-test%' limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan["~children"][0]["~children"][0]["limit"] == "10")

                idx2 = "idx2"
                self.query = "CREATE INDEX {0} ON {1} ( _id  )".format(idx2, query_bucket) + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.query = "Explain  select * from {0} b1 nest {0} b2 on keys b1._id where b1._id like" \
                             " 'query-testemployee%' limit 5 ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan1 = self.ExplainPlanHelper(actual_result)
                self.assertFalse("limit" in plan1["~children"][0]["~children"][0])
                self.query = "Explain  select * from {0} b1 nest {0} b2 on key b2._id FOR b1 where b1._id like" \
                             " 'query-testemployee%'  limit 5 ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan2 = self.ExplainPlanHelper(actual_result)
                self.assertFalse("limit" in plan2["~children"][0]["~children"][0])
                self.query = "Explain  select * from {0} b1 left nest {0} b2 on keys b1._id where b1._id like" \
                             " 'query-testemployee%' limit 10 ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan3 = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan3["~children"][1]['expr'] == "10")
                self.query = "Explain  select * from {0} b1 left nest {0} b2 on key b2._id FOR b1 where b1._id" \
                             " like 'query-testemployee%'  limit 10 ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan4 = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan4["~children"][1]['expr'] == "10")

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_ifmissing_ifnull(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                idx2 = "idx2"
                idx3 = "idx3"
                idx4 = "idx4"
                idx5 = "idx5"
                self.ensure_primary_indexes_exist()
                self.query = 'CREATE INDEX {0} ON {1}( IFMISSING( IsSpecial,b, name ),' \
                             ' join_day,name ) using {2}'.format(idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.query = 'INSERT into {0} (key , value) VALUES ("{1}", {2})'.format(query_bucket, "k01",
                                                                                        {"x": 10, "b": True,
                                                                                         "c": "pq"})
                self.run_cbq_query()
                self.query = 'INSERT into {0} (key , value) VALUES ("{1}", {2})'.format(query_bucket, "k02",
                                                                                        {"IsSpecial": True, "b": True,
                                                                                         "c": "pq"})
                self.run_cbq_query()
                self.query = 'INSERT into {0} (key , value) VALUES ("{1}", {2})'.format(query_bucket, "k03",
                                                                                        {"IsSpecial": False, "b": True,
                                                                                         "c": "pq"})
                self.run_cbq_query()
                self.query = 'INSERT into {0} (key , value) VALUES ("{1}",' \
                             ' {2})'.format(query_bucket, "k04",
                                            {"IsSpecial": json.JSONEncoder().encode(None),
                                             "b": 2,
                                             "c": "pq"})
                self.run_cbq_query()
                self.query = 'SELECT meta().id, IFMISSING(IsSpecial,b,name) from {0} limit 5'.format(query_bucket)
                actual_result = self.run_cbq_query()
                expected_result = [{'id': 'k01', '$1': True}, {'id': 'k02', '$1': True},
                                   {'id': 'k03', '$1': False}, {'id': 'k04', '$1': 'null'},
                                   {'id': 'query-testemployee10153.187782713003-0', '$1': 'employee-9'}]
                # self.assertTrue(actual_result['results'] == expected_result)
                diffs = DeepDiff(actual_result['results'], expected_result, ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                self.query = 'EXPLAIN SELECT name from {0} where IFMISSING(IsSpecial,b,name) = TRUE and' \
                             ' join_day> 1'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan1 = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan1['~children'][0]['index'] == idx)
                self.query = 'CREATE INDEX {0} ON {1}( IFNULL( IsSpecial,b, name ), join_day,name )' \
                             ' using {2}'.format(idx2, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.query = 'SELECT meta().id, IFNULL(IsSpecial,b,2,3) from {0} limit 5'.format(query_bucket)
                actual_result = self.run_cbq_query()
                expected_result = [{'id': 'k01'}, {'id': 'k02', '$1': True}, {'id': 'k03', '$1': False},
                                   {'id': 'k04', '$1': 'null'}, {'id': 'query-testemployee10153.187782713003-0'}]
                self.assertTrue(actual_result['results'] == expected_result, )
                self.query = 'EXPLAIN SELECT name from {0} where IFNULL(IsSpecial,b,name) = null and' \
                             ' join_day> 1'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan2 = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan2['~children'][0]['index'] == idx2)
                self.query = 'CREATE INDEX {0} ON {1}( MISSINGIF( IsSpecial,b ), join_day,name )' \
                             ' using {2}'.format(idx3, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self.assertTrue(idx3 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx3)
                self.query = 'SELECT meta().id, MISSINGIF(IsSpecial,b) from {0} limit 5'.format(query_bucket)
                actual_result = self.run_cbq_query()
                expected_result = [{'id': 'k01'}, {'id': 'k02'}, {'id': 'k03', '$1': False},
                                   {'id': 'k04', '$1': 'null'}, {'id': 'query-testemployee10153.187782713003-0'}]
                self.assertTrue(actual_result['results'] == expected_result)
                self.query = 'EXPLAIN SELECT name from {0} where MISSINGIF(IsSpecial,b) = TRUE' \
                             ' and join_day> 1'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan2 = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan2['~children'][0]['index'] == idx3)
                self.query = 'CREATE INDEX {0} ON {1}( NULLIF( IsSpecial,b ), join_day,name )' \
                             ' using {2}'.format(idx4, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx4)
                self.query = 'SELECT meta().id, NULLIF(IsSpecial,b) from {0} limit 5'.format(query_bucket)
                actual_result = self.run_cbq_query()
                expected_result = [{'id': 'k01'}, {'id': 'k02', '$1': None}, {'id': 'k03', '$1': False},
                                   {'id': 'k04', '$1': 'null'}, {'id': 'query-testemployee10153.187782713003-0'}]
                self.assertTrue(actual_result['results'] == expected_result)
                self.query = 'EXPLAIN SELECT name from {0} where NULLIF(IsSpecial,b)' \
                             ' IS null and join_day> 1'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan2 = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan2['~children'][0]['index'] == idx4)
                self.query = 'CREATE INDEX {0} ON {1}( IFMISSINGORNULL( IsSpecial,b,name,2,3 ),' \
                             ' join_day,name )'.format(idx5, query_bucket)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx5)
                self.assertTrue(idx5 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx5)
                self.query = 'SELECT meta().id, IFMISSINGORNULL(IsSpecial,b,name,2,3) from {0}' \
                             ' limit 5'.format(query_bucket)
                actual_result = self.run_cbq_query()
                expected_result = [{'id': 'k01', '$1': True}, {'id': 'k02', '$1': True},
                                   {'id': 'k03', '$1': False}, {'id': 'k04', '$1': 'null'},
                                   {'id': 'query-testemployee10153.187782713003-0', '$1': 'employee-9'}]
                self.assertTrue(actual_result['results'] == expected_result)
                self.query = 'EXPLAIN SELECT name from {0} where IFMISSINGORNULL(IsSpecial,b,name,2,3)' \
                             ' IS NULL and join_day> 1'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan2 = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan2['~children'][0]['index'] == idx5)
            finally:
                self.query = 'delete from {0} where meta().id = {1}'.format(query_bucket, "'k01'")
                self.run_cbq_query()
                self.query = 'delete from {0} where meta().id = {1}'.format(query_bucket, "'k02'")
                self.run_cbq_query()
                self.query = 'delete from {0} where meta().id = {1}'.format(query_bucket, "'k03'")
                self.run_cbq_query()
                self.query = 'delete from {0} where meta().id = {1}'.format(query_bucket, "'k04'")
                self.run_cbq_query()
                self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
                self.run_cbq_query()
                for idx in created_indexes:
                    self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_subdoc_field(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = 'CREATE INDEX idx on {0}(tasks_points.task1) using {1}'.format(query_bucket,
                                                                                            self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.query = 'select tasks_points.task1 from {0} where tasks_points.task1 = 1'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['metrics']['resultCount'] == 2016)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_intersect(self):
        self.fail_if_no_buckets()
        for query_bucket in self.query_buckets:
            self.query = 'select ARRAY_INTERSECT(join_yr,[2011,2012,2016,"test"], [2011,2016], [2012,2016])' \
                         ' as test from {0}'.format(query_bucket)
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
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1} ( join_day )".format(idx, query_bucket) + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.query = 'explain select join_day from {0} where join_day in ["5", $1] '.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan1 = self.ExplainPlanHelper(actual_result)
                if self.index_type.lower() == 'gsi':
                    self.assertEqual(len(plan1['~children'][0]['spans']), 2)
                else:
                    self.assertEqual(len(plan1['~children'][0]['scan']['spans']), 2)
                self.query = 'explain select join_day from {0} where join_day in [$1] '.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan2 = self.ExplainPlanHelper(actual_result)
                self.assertEqual(len(plan2['~children'][0]['spans']), 1)
                self.query = 'explain select join_day from {0} where join_day in  $1 '.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan3 = self.ExplainPlanHelper(actual_result)
                self.assertEqual(len(plan3['~children'][0]['spans']), 1)
                if self.index_type.lower() == 'gsi':
                    self.assertEqual(plan3['~children'][0]['spans'][0]['range'][0]['low'], 'array_min($1)')
                else:
                    self.assertEqual(plan3['~children'][0]['spans'][0]['Range']['Low'][0], 'array_min($1)')
                self.query = 'explain select join_day from {0} where join_day in  [] '.format(query_bucket)
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

                self.query = 'explain select join_day from {0} where join_day in  [$1,$2,$3] '.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan5 = self.ExplainPlanHelper(actual_result)
                if self.index_type.lower() == 'gsi':
                    self.assertEqual(len(plan5['~children'][0]['spans']), 3)
                else:
                    self.assertEqual(len(plan5['~children'][0]['scan']['spans']), 3)

                self.query = 'explain select join_day from {0} ' \
                             'where join_day in [$1,$2,$3] limit 5 '.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan6 = self.ExplainPlanHelper(actual_result)
                if self.index_type.lower() == 'gsi':
                    self.assertEqual(len(plan6['~children'][0]['~children'][0]['spans']), 3)
                else:
                    self.assertEqual(len(plan6['~children'][0]['~children'][0]['scan']['spans']), 3)
                self.assertEqual(plan6["~children"][1]['expr'] == "5" and plan6["~children"][1]['#operator'], 'Limit')

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_stable_scan(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1} ( join_day )".format(idx, query_bucket) + \
                             "USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                idx = "idx2"
                self.query = "CREATE INDEX {0} ON {1} ( meta().id )".format(idx, query_bucket) + \
                             "USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.query = 'select join_day from {0} where join_day in [4,7,5,10] ' \
                             'order by meta().id'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select join_day from {0} use index(`#primary`) ' \
                             'where join_day in [4,7,5,10] order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = 'select join_day from {0} where join_day = 4 OR join_day > 5 OR join_day < 10 ' \
                             'order by meta().id'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select join_day from {0} use index(`#primary`) where join_day = 4 OR join_day > 5 ' \
                             'OR join_day < 10 order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = 'select join_day from {0} where join_day != 10 order by meta().id'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select join_day from {0} use index(`#primary`) where join_day != 10 ' \
                             'order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = 'select join_day from {0} where meta().id ' \
                             'in ["query-testemployee10153.187782713003-0","",' \
                             '"query-testemployee10194.855617030253-0"] order by meta().id'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select join_day from {0} use index(`#primary`) where meta().id in ' \
                             '["query-testemployee10153.187782713003-0","","query-testemployee10194.855617030253-0"]' \
                             ' order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.assertTrue(actual_result['results'] == [{'join_day': 9}, {'join_day': 4}])
                self.query = 'select join_day from {0} where meta().id = "query-testemployee10231.2819054-0" OR' \
                             ' meta().id > "" OR meta().id < "query-testemployee10317.9004497-0"' \
                             ' order by meta().id'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select join_day from {0} use index(`#primary`) where' \
                             ' meta().id = "query-testemployee10231.2819054-0" OR' \
                             ' meta().id > "" OR meta().id < "query-testemployee10317.9004497-0"' \
                             ' order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = 'select join_day from {0} where meta().id = "query-testemployee10231.2819054-0" OR' \
                             ' meta().id > "" OR meta().id < "query-testemployee10317.9004497-0"' \
                             ' order by meta().id limit 10'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.assertTrue(
                    actual_result['results'] == [{'join_day': 9}, {'join_day': 9}, {'join_day': 9}, {'join_day': 9},
                                                 {'join_day': 9}, {'join_day': 9}, {'join_day': 4}, {'join_day': 4},
                                                 {'join_day': 4}, {'join_day': 4}])
                self.query = 'select join_day from {0} where meta().id != "query-testemployee10231.2819054-0"' \
                             ' order by meta().id'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select join_day from {0} use index(`#primary`) where' \
                             ' meta().id != "query-testemployee10231.2819054-0" ' \
                             'order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = 'select join_day from {0} where meta().id != "query-testemployee10231.2819054-0"' \
                             ' order by meta().id limit 10'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.assertTrue(
                    actual_result['results'] == [{'join_day': 9}, {'join_day': 9}, {'join_day': 9}, {'join_day': 9},
                                                 {'join_day': 9}, {'join_day': 9}, {'join_day': 4}, {'join_day': 4},
                                                 {'join_day': 4}, {'join_day': 4}])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_meta_cas_expiration(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1} ( meta().cas )".format(idx, query_bucket) + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                idx = "idx2"
                self.query = "CREATE INDEX {0} ON {1} ( meta().expiration )".format(idx, query_bucket) + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                idx = "idx3"
                self.query = "CREATE INDEX {0} ON {1} ( meta().id, meta().cas," \
                             " meta().expiration )".format(idx, query_bucket) + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                idx = "idx4"
                self.query = "CREATE INDEX {0} ON {1} ( meta().id )".format(idx, query_bucket) + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                idx = "idx5"
                self.query = "CREATE INDEX {0} ON {1} ( meta().cas, meta().expiration )".format(idx, query_bucket) + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                idx = "idx6"
                self.query = "CREATE INDEX {0} ON {1} ( meta().cas ) where" \
                             " meta().cas > 1487875768758304768".format(idx, query_bucket) + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.query = 'explain SELECT  meta().id , meta().cas, meta().expiration FROM {0}' \
                             ' where meta().id > ""'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] == 'idx3')
                self.query = 'explain SELECT  meta().id , meta().cas, meta().expiration FROM {0} ' \
                             'where meta().id !="query-testemployee10231.2819054-0" or meta().cas > 0 or' \
                             ' meta().expiration = 0'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.log.info(f"plan is: {plan}")
                self.assertTrue(plan['~children'][0]['#operator'] == 'PrimaryScan3')
                # self.assertTrue(plan['~children'][0]['scans'][0]['index'] in ['idx3', '#primary'] or
                #                 plan['~children'][0]['scans'][1]['index'] in ['idx4', '#primary'])
                self.query = 'explain SELECT meta().cas, meta().expiration,meta().id FROM {0} ' \
                             'where meta().cas = 1487875768758304768 and meta().expiration > 0'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.log.info(f"plan is: {plan}")
                self.assertTrue(plan['~children'][0]['index'] == 'idx5')
                self.query = 'explain SELECT meta().cas, meta().expiration FROM {0} ' \
                             'where meta().cas !=1487875768758304768 or meta().expiration != 0'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.log.info(f"plan is: {plan}")
                self.assertTrue(plan['~children'][0]['#operator'] == 'UnionScan')
                self.query = 'explain SELECT  meta().id  FROM {0} ' \
                             'where meta().id in ["",null,"query-testemployee10231.2819054-0",' \
                             '"query-testemployee9987.55838821-0"]'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.log.info(f"plan is: {plan}")
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] in ["#primary", "idx4"])
                self.query = 'SELECT  meta().id  FROM {0} ' \
                             'where meta().id in ["",null,"query-testemployee10231.2819054-0",' \
                             '"query-testemployee9987.55838821-0"]'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'SELECT  meta().id  FROM {0} use index(`#primary`) ' \
                             'where meta().id in ["",null,"query-testemployee10231.2819054-0",' \
                             '"query-testemployee9987.55838821-0"]'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = 'explain SELECT  meta().cas,meta().id  FROM {0} ' \
                             'where meta().cas > 1487875768758304768'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.log.info(f"plan is: {plan}")
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] in ["idx", "idx6"])
                self.query = 'explain SELECT  meta().expiration,meta().id  FROM {0} ' \
                             'where meta().expiration > 0'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.log.info(f"plan is: {plan}")
                self.assertTrue("covers" in str(plan))
                self.assertTrue(plan['~children'][0]['index'] == "idx2")
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_suffix(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            idx = "idx"
            self.query = "CREATE INDEX {0} ON {1} (( DISTINCT ARRAY s FOR s IN SUFFIXES" \
                         " ( LOWER( _id )  )".format(idx, query_bucket) + \
                         " END),LOWER(_id),department ,name)  " \
                         " USING {0}".format(self.index_type)
            actual_result = self.run_cbq_query()
            self._wait_for_index_online(bucket, idx)
            self.assertTrue(idx in str(actual_result['results']),
                            f"The index is returning the wrong index, please check {actual_result}")
            created_indexes.append(idx)
            self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
            self.query = "EXPLAIN select name from {0} WHERE ANY s IN SUFFIXES( LOWER( _id ) ) ".format(query_bucket) + \
                         " SATISFIES s LIKE 'query-test%' END" \
                         " AND  department = 'Manager' ORDER BY name "
            self.check_explain_covering_index(idx)
            self.query = "select name from {0} WHERE ANY s IN SUFFIXES( LOWER( _id ) ) ".format(query_bucket) + \
                         " SATISFIES s LIKE  'query-test%'  END" \
                         " AND  department = 'Manager' ORDER BY name "
            actual_result = self.run_cbq_query()
            for idx in created_indexes:
                self.query = "DROP INDEX {0} ON {1} USING {2}".format(idx, query_bucket, self.index_type)
                self.run_cbq_query()
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
            self.query = "select name from {0}  WHERE ANY s IN SUFFIXES( LOWER( _id ) ) ".format(query_bucket) + \
                         " SATISFIES s LIKE  'query-test%'  END" \
                         " AND  department = 'Manager' ORDER BY name "

            expected_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'] == expected_result['results'])

    def test_simple_array_index_distinct(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY v " \
                             "FOR v in {2} END) USING {3}".format(idx, query_bucket, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxVM"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY x.RAM " \
                             "FOR x in {2} END) USING {3}".format(idx2, query_bucket, "VMs", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.query = "EXPLAIN select name from {0} " \
                             "where any v in {0}.join_yr satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.VMs SATISFIES x.RAM between 1 and 5 END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.log.info(plan)
                self.log.info(plan['~children'][0]['~children'][0]['#operator'])
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan'
                                or plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan',
                                "actual=" + plan['~children'][0]['~children'][0]['#operator'])
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                self.query = "EXPLAIN select meta().id from {0} " \
                             "where any v in {0}.join_yr satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY v IN {0}.join_yr SATISFIES v = 2014 END) ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" not in str(plan))
                self.query = "EXPLAIN select meta().id from {0} " \
                             "where any v in {0}.join_yr satisfies v = 2016 END ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "EXPLAIN select meta().id from {0} " \
                             "where any v in {0}.join_yr satisfies v = 2016 END ".format(query_bucket) + \
                             "OR (ANY v IN {0}.join_yr SATISFIES v = 2014 END) ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" not in str(plan))
                self.query = "select meta().id from {0} where ANY v IN join_yr SATISFIES v IN [ 2016,2014] END" \
                             " order by meta().id limit 3".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from {0} use index (`#primary`) where ANY v IN join_yr SATISFIES v " \
                             "IN [ 2016,2014] END order by meta().id limit 3".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = "select meta().id from {0} where ANY v IN join_yr SATISFIES v = 2016 OR v= 2014 END " \
                             "order by meta().id limit 3".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])

                self.query = "select meta().id from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY v IN {0}.join_yr SATISFIES v = 2014 END) order by meta().id " \
                             "limit 3".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from {0} use index(`#primary`) where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY v IN {0}.join_yr SATISFIES v = 2014 END) " \
                             "order by meta().id limit 3".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = "select meta().id from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "OR (ANY v IN {0}.join_yr SATISFIES v = 2014 END) order by meta().id " \
                             "limit 3".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from {0} use index(`#primary`) where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "OR (ANY v IN {0}.join_yr SATISFIES v = 2014 END) order by meta().id " \
                             "limit 3".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                idx8 = "idxjoin_yr4"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY v FOR v in {2} END,join_yr) USING {3}".format(
                    idx8, query_bucket, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx8)
                self.assertTrue(idx8 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx8)
                self.assertTrue(self._is_index_in_list(bucket, idx8), "Index is not in list")
                self.query = "EXPLAIN select meta().id from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY v IN {0}.join_yr SATISFIES v = 2014 END) ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "EXPLAIN select meta().id from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "OR (ANY v IN {0}.join_yr SATISFIES v = 2014 END) ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select meta().id from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY v IN {0}.join_yr SATISFIES v = 2014 END) order by meta().id " \
                             "limit 3".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from {0} use index(`#primary`) where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY v IN {0}.join_yr SATISFIES v = 2014 END) order by meta().id" \
                             " limit 3".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])

                self.query = "select meta().id from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "OR (ANY v IN {0}.join_yr SATISFIES v = 2014 END) order by meta().id " \
                             "limit 3".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from {0} use index(`#primary`) where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "OR (ANY v IN {0}.join_yr SATISFIES v = 2014 END) order by meta().id " \
                             "limit 3".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])

                self.query = "select name from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.VMs SATISFIES x.RAM between 1 and 5 END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} use index (`#primary`)  where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.VMs SATISFIES x.RAM between 1 and 5 END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                expected_result = self.run_cbq_query()
                # self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))
                self.assertTrue(sorted(actual_result['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                created_indexes.remove(idx)
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx2, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx2), "Index is in list")
                created_indexes.remove(idx2)
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx8, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx8), "Index is in list")
                created_indexes.remove(idx8)

                idx3 = "idxjoin_yr2"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY v FOR v within {2} END)" \
                             " USING {3}".format(idx3, query_bucket, "join_yr", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self.assertTrue(idx3 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")

                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY x.RAM FOR x within {2} END) " \
                             "USING {3}".format(idx4, query_bucket, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "EXPLAIN select name from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.VMs SATISFIES x.RAM between 1 and 5 END) ".format(query_bucket) + \
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

                self.query = "EXPLAIN select name from {0} where any v within {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x within {0}.VMs SATISFIES x.RAM between 1 and 5 END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result_within = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result_within)
                self.assertTrue('Distinct' in str(plan),
                                f"Distinct Scan is not being used in and query for 2 array indexes: {plan}")
                self.assertTrue(idx4 in str(plan), f"The wrong index is being used in the plan please check! Plan: {plan}")
                self.query = "select name from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x within {0}.VMs SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                actual_result_within = self.run_cbq_query()
                self.query = "select name from {0} use index (`#primary`) where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x within {0}.VMs SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                expected_result = self.run_cbq_query()
                # self.assertTrue(sorted(actual_result_within['results']) == sorted(expected_result['results']))
                self.assertTrue(sorted(actual_result_within['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_array_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX {0} ON {1}( ARRAY v FOR v in {2} END) " \
                             "USING {3}".format(idx, query_bucket, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxVM"
                self.query = "CREATE INDEX {0} ON {1}( ARRAY x.RAM FOR x in {2} END) " \
                             "USING {3}".format(idx2, query_bucket, "VMs", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.query = "select name from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.VMs SATISFIES x.RAM between 1 and 5 END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                self.run_cbq_query()
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                created_indexes.remove(idx)
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx2, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx2), "Index is in list")
                created_indexes.remove(idx2)

                idx3 = "idxjoin_yr2"
                self.query = "CREATE INDEX {0} ON {1}(  ARRAY v FOR v within {2} END) " \
                             "USING {3}".format(idx3, query_bucket, "join_yr", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self.assertTrue(idx3 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX {0} ON {1}(  ARRAY x.RAM FOR x within {2} END) " \
                             "USING {3}".format(idx4, query_bucket, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")
                self.query = "select name from {0} USE INDEX({1}) where ".format(query_bucket, idx4) + \
                             "(ANY x within {0}.VMs SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                actual_result_within = self.run_cbq_query()
                self.query = "select name from {0} USE INDEX(`#primary`) where ".format(query_bucket) + \
                             "(ANY x within {0}.VMs SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result_within['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_update_arrays(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = "UPDATE {0} SET s.newField = 'newValue' FOR s IN " \
                             "ARRAY_FLATTEN(tasks[*].Marketing, 1) END".format(query_bucket)
                self.run_cbq_query()
                idx = "nested_idx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j for j within i end) FOR i " \
                             "in {2} END,tasks,name) USING {3}".format(idx, query_bucket, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "select name from {0}  WHERE ANY i IN tasks SATISFIES  (ANY j within i " \
                             "SATISFIES j='newValue' END) END ; ".format(query_bucket)
                actual_result = self.run_cbq_query()
                os = self.shell.extract_remote_info().type.lower()
                if os == "windows":
                    number_of_doc = 1680
                else:
                    number_of_doc = 10080
                self.assertEqual(actual_result['metrics']['resultCount'], number_of_doc)
                self.query = "UPDATE {0} SET s.newField = 'newValue' FOR s IN ARRAY_FLATTEN " \
                             "(ARRAY i.Marketing FOR i IN tasks END, 1) END;".format(query_bucket)
                self.run_cbq_query()
                self.query = "select name from {0}  WHERE ANY i IN tasks SATISFIES  (ANY j within i " \
                             "SATISFIES j='newValue' END) END ; ".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['metrics']['resultCount'], number_of_doc)
                self.query = "UPDATE {0} SET i.Marketing = ( ARRAY OBJECT_ADD(s, 'newField', 'test' ) FOR s IN " \
                             "i.Marketing END ) FOR i IN tasks END;".format(query_bucket)
                self.run_cbq_query()
                self.query = "select name from {0}  WHERE ANY i IN tasks SATISFIES  (ANY j within i SATISFIES " \
                             "j='newValue' END) END ; ".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['metrics']['resultCount'], number_of_doc)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_covering_like_array_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        idx = "ix"
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j.region1 for j in" \
                             " i.Marketing end) FOR i in {2} END) where VMs[0].os = 'ubuntu' " \
                             "USING {3}".format(idx, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "explain select meta().id from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN " \
                             "i.Marketing SATISFIES j.region1='South' end) END and" \
                             " VMs[0].os = 'ubuntu'".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['scan']['index'] == idx)
                self.query = 'explain select meta().id from {0} WHERE ANY i IN tasks SATISFIES ' \
                             '(ANY j IN i.Marketing SATISFIES j.region1 like "{1}" end) END and ' \
                             'VMs[0].os = "ubuntu"'.format(query_bucket, 'Sou%')
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['scan']['index'] == idx)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
    def test_covering_case(self):
        for bucket in self.buckets:
            self.cluster.bucket_delete(self.master, bucket=bucket, timeout=180000)
        rest = RestConnection(self.master)
        rest.load_sample(self.sample_bucket)
        time.sleep(5)
        self.wait_for_all_indexes_online()
        self.run_cbq_query(query="CREATE INDEX sample_index ON `default`:`travel-sample`.`inventory`.`route`"
                      "(`id`, CASE WHEN airline='AF' THEN 1 ELSE 0 END, CASE WHEN airline<>'AF' THEN 1 ELSE 0 END)")
        explain_results = self.run_cbq_query(query="EXPLAIN "
                                                   "SELECT CASE WHEN airline='AF' THEN 1 ELSE 0 END AS AF_AIRLINE, "
                                                   "CASE WHEN airline<>'AF' THEN 1 ELSE 0 END AS NOT_AF_AIRLINE "
                                                   "FROM `travel-sample`.`inventory`.`route` WHERE id < 20000 LIMIT 10")
        self.assertTrue('covers' in str(explain_results), f"Index is not covering please check plan {explain_results}")
        self.assertTrue('cover ((`route`.`id`))' in str(explain_results), f"We expect this field to be covered, please check plan {explain_results}")
        self.assertTrue('cover (case when ((`route`.`airline`) = "AF") then 1 else 0 end)' in str(explain_results), f"We expect this field to be covered, please check plan {explain_results}")
        self.assertTrue('cover (case when (not ((`route`.`airline`) = "AF")) then 1 else 0 end)' in str(explain_results), f"We expect this field to be covered, please check plan {explain_results}")
        self.assertTrue('cover ((meta(`route`).`id`))' in str(explain_results), f"We expect this field to be covered, please check plan {explain_results}")

    def test_panic_in_null(self):
        queries = dict()
        query_bucket = self.get_collection_name('default')
        index_1 = {'name': 'idx1', 'bucket': 'default', 'fields': [("join_yr", 0)], 'state': 'online',
                   'using': self.index_type.lower(), 'is_primary': False}
        query_1 = "explain select join_yr from {0} where join_yr IN [NULL]".format(query_bucket)
        verifier = lambda x: self.assertEqual(x['q_res'][0]['results'][0]['plan']['~children'][0]['index'], 'idx1')
        queries["a"] = {"indexes": [index_1], "queries": [query_1],
                        "asserts": [verifier]}
        self.query_runner(queries)

    def test_avoid_intersect_scan(self):
        created_indexes = []
        idx = "idx_country"
        idx2 = "gix_USMales"
        query_bucket = self.get_collection_name('default')
        try:
            self.query = "CREATE INDEX {0} ON {1}(`address`[1][0].`country`) WHERE " \
                         "(`_id` = 'query-testemployee10194.855617-0')".format(idx, query_bucket)
            self.run_cbq_query()
            created_indexes.append(idx)
            self.query = "CREATE INDEX gix_USMales ON {0}(distinct (pairs(self))) WHERE " \
                         "(`_id` = 'query-testemployee10194.855617-0') and (`gender` = 'M') and " \
                         "(`address`[1][0].`country` = 'United States of America')".format(query_bucket)
            self.run_cbq_query()
            created_indexes.append(idx2)
            self.query = "explain select count(*) from {0} where _id = 'query-testemployee10194.855617-0' and " \
                         "address[1][0].country = 'United States of America' and gender = 'M'".format(query_bucket)
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.log.info(f"plan is: {plan}")
            self.assertTrue("cover" in str(plan))
            self.assertTrue("DistinctScan" in str(plan))
            self.assertTrue(idx2 in str(plan))
            self.query = "select count(*) from {0} where _id = 'query-testemployee10194.855617-0' and " \
                         "address[1][0].country = 'United States of America' and gender = 'M'".format(query_bucket)
            actual_result = self.run_cbq_query()
            self.query = "select count(*) from {0} use index(`#primary`) where _id = " \
                         "'query-testemployee10194.855617-0' and address[1][0].country = " \
                         "'United States of America' and gender = 'M'".format(query_bucket)
            expected_result = self.run_cbq_query()
            self.assertTrue(actual_result['results'] == expected_result['results'])
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                actual_result = self.run_cbq_query()

    def test_OR_UnionScan(self):
        created_indexes = []
        query_bucket = self.get_collection_name('default')
        try:
            self.query = "CREATE INDEX `k1` ON {0}(`k01`)".format(query_bucket)
            self.run_cbq_query()
            created_indexes.append('k1')
            self.query = "CREATE INDEX `k2` ON {0}(`k02`)".format(query_bucket)
            self.run_cbq_query()
            created_indexes.append('k2')
            self.query = "CREATE INDEX `k3` ON {0}(`k03`)".format(query_bucket)
            self.run_cbq_query()
            created_indexes.append('k3')
            self.query = 'explain SELECT  k01  FROM {0} where k01 > "abc" OR k02 > 123 or k03>10'.format(query_bucket)
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['#operator'] == 'UnionScan')
            self.query = 'explain select * from {0} where k01 < 10 OR k02 < 20'.format(query_bucket)
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['#operator'] == 'UnionScan')
            self.query = 'explain select * from {0} where k01 < 10 OR k02 < 20 OR k03 < 30'.format(query_bucket)
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue(plan['~children'][0]['#operator'] == 'UnionScan')
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                actual_result = self.run_cbq_query()

    def test_index_join(self):
        created_indexes = []
        idx = "idx_parent_chkey"
        idx2 = "idx_parent_chkeyw"
        try:
            std_bucket = self.get_collection_name('standard_bucket0')
            query_bucket = self.get_collection_name('default')
            self.query = 'insert into {0} values("child1",{{"x":1,"y":9,"z":999}})'.format(std_bucket)
            self.run_cbq_query()
            self.query = 'insert into {0} values("parent1",{{"a":1,"b":9,"c":999,"chkey":"1"}})'.format(query_bucket)
            self.run_cbq_query()
            self.query = 'create index {0} on {1}(chkey)'.format(idx, query_bucket)
            self.run_cbq_query()
            created_indexes.append(idx)
            self.query = 'select {0},{1} from {1} left outer join {0} on key ("child1" || {0}.chkey) for {1}'.format(
                query_bucket, std_bucket)
            try:
                self.run_cbq_query()
            except CBQError as ex:
                self.log.error(ex)
                self.assertTrue(str(ex).find("No index available for join term default") != -1,
                                "Error is incorrect.")
            else:
                self.fail("There was no errors. Error expected: No index available for join term default")

            self.query = 'select {0},{1} from {1} left outer JOIN {0} on key ("{1}" || {0}.chkey) ' \
                         'for {1}'.format(query_bucket, std_bucket)
            try:
                self.run_cbq_query()
            except CBQError as ex:
                self.log.error(ex)
                self.assertTrue(str(ex).find("No index available for join term default") != -1,
                                "Error is incorrect.")
            else:
                self.fail("There was no errors. Error expected: No index available for join term default")

            self.query = 'create index {0} on {1}("{2}" || chkey)'.format(idx2, query_bucket, std_bucket)
            self.run_cbq_query()
            created_indexes.append(idx2)
            self.query = 'select {0},{1} from {1} left outer join {0} on key ("{1}" || {0}.chkey) for {1} ' \
                         'order by meta({0}).id limit 2'.format(query_bucket, std_bucket)
            actual_result = self.run_cbq_query()
            expected_result = [{'standard_bucket0': {'y': 9, 'x': 1, 'z': 999}},
                                                         {'standard_bucket0': {'tasks_points': {'task1': 1, 'task2': 1},
                                                                               'name': 'employee-9',
                                                                               'mutated': 0,
                                                                               'skills': ['skill2010', 'skill2011'],
                                                                               'join_day': 9,
                                                                               'email': '9-mail@couchbase.com',
                                                                               'test_rate': 10.1,
                                                                               'join_mo': 10,
                                                                               'join_yr': 2011,
                                                                               '_id': 'query-testemployee10153.187782713003-0',
                                                                               'VMs': [{'RAM': 10, 'os': 'ubuntu',
                                                                                        'name': 'vm_10', 'memory': 10},
                                                                                       {'RAM': 10, 'os': 'windows',
                                                                                        'name': 'vm_11', 'memory': 10}],
                                                                               'job_title': 'Engineer'}}]
            diffs = DeepDiff(actual_result['results'], expected_result, ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = 'delete from {0} use keys("parent1")'.format(query_bucket)
            self.run_cbq_query()
            self.query = 'delete from {0} use keys("child1")'.format(std_bucket)
            self.run_cbq_query()
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX {1} ON {0} USING {2}".format("default", idx, self.index_type)
                actual_result = self.run_cbq_query()

    def test_covering_index_collections(self):
        created_indexes = []
        idx = "ix"
        idx2 = "ix2"
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = "create index {1} on {0}(x)".format(query_bucket, idx)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = 'INSERT into {0} (key , value) VALUES ("{1}", {2})'.format(query_bucket, "k01", '{"x":10}')
                self.run_cbq_query()
                self.query = 'explain select x1 from {0} let x1 = FIRST c for c IN [2,1,10] WHEN c = x END ' \
                             'where x = 10 '.format(query_bucket)
                self.check_explain_covering_index(idx)
                self.query = 'select x1 from {0} let x1 = FIRST c for c IN [2,1,10] WHEN c = x END ' \
                             'where x = 10 '.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == [{'x1': 10}])
                self.query = 'CREATE INDEX {1} ON {0}(SUBSTR(transDate,0,10),code) WHERE code != "" AND meta().id ' \
                             'LIKE "account-customerXYZ%" '.format(query_bucket, idx2)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.query = 'INSERT into {0} (key,value) values("{1}",{{ "accountNumber": 123456789, ' \
                             '"docId": "account-customerXYZ-123456789", "code": "001" , ' \
                             '"transDate":"2016-07-02"}})'.format(query_bucket, "account-customerXYZ-123456789")
                self.run_cbq_query()
                self.query = 'INSERT into {0} (key,value) values("{1}", '.format(query_bucket, "codes-version-9") + \
                             '{ "version": 9, "docId": "codes-version-9", "codes": [{ "code": "001", "type": "P",' \
                             ' "title": "SYSTEM W MCC", "weight": 26.2466 },' \
                             '{ "code": "166", "type": "P", "title": "SYSTEM W/O MCC", "weight": 14.6448 }] })'
                self.run_cbq_query()
                self.query = 'explain SELECT SUBSTR(account.transDate,0,10) As transDate, AVG(codes.weight) As ' \
                             'avgWeight FROM {0} account JOIN {0} codesDoc ON KEYS "codes-version-9" ' \
                             'LET codes = FIRST c for c IN codesDoc.codes WHEN c.code = account.code END ' \
                             'WHERE account.code != "" AND meta(account).id LIKE "account-customerXYZ-%" ' \
                             'AND SUBSTR(account.transDate,0,10) >= "2016-07-01" AND SUBSTR(account.transDate,0,10) ' \
                             '< "2016-07-03" GROUP BY SUBSTR(account.transDate,0,10)'.format(query_bucket)
                self.run_cbq_query()
                self.check_explain_covering_index(idx2)
                self.query = 'SELECT SUBSTR(account.transDate,0,10) As transDate, AVG(codes.weight) As avgWeight ' \
                             'FROM {0} account JOIN {0} codesDoc ON KEYS "codes-version-9" ' \
                             'LET codes = FIRST c for c IN codesDoc.codes WHEN c.code = account.code END ' \
                             'WHERE account.code != "" AND meta(account).id LIKE "account-customerXYZ-%" ' \
                             'AND SUBSTR(account.transDate,0,10) >= "2016-07-01" AND SUBSTR(account.transDate,0,10) ' \
                             '< "2016-07-03" GROUP BY SUBSTR(account.transDate,0,10)'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'][0]['avgWeight'] == 26.2466)
                self.assertTrue(actual_result['results'][0]['transDate'] == '2016-07-02')
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_pushdown_complex_filter(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            try:
                query_bucket = self.get_collection_name(bucket.name)
                idx = "idxjoining"
                self.query = "create index {1} on {0}(join_yr,join_day)".format(query_bucket, idx)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = 'explain select meta().id from {0} where join_day between 0 and 5 and ' \
                             'join_yr = 2011'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index'] == idx)
                self.assertTrue("covers" in str(plan))
                self.query = 'select meta().id from {0} where join_day between 0 and 5 and ' \
                             'join_yr = 2011'.format(query_bucket)
                actual_result = self.run_cbq_query(query_params={'profile': 'timings'})
                self.assertTrue("covers" in str(actual_result['profile']['executionTimings']))
                self.assertTrue(actual_result['profile']['executionTimings']['~child']['~children'][0]['index'] == idx)
                all_docs_list = self.generate_full_docs_list(self.gens_load)
                expected_result = [doc for doc in all_docs_list if
                                   0 <= doc['join_day'] <= 5 and doc['join_yr'] == 2011]
                num_of_items = len(expected_result)
                for i in [1, 2]:
                    if i == 1:
                        self.assertTrue(
                            actual_result['profile']['executionTimings']['~child']['~children'][i]['~children'][1]['#stats']['#itemsIn'] == num_of_items)
                        self.assertTrue(
                            actual_result['profile']['executionTimings']['~child']['~children'][i]['~children'][1]['#stats']['#itemsOut'] == num_of_items)
                    else:
                        self.assertTrue(actual_result['profile']['executionTimings']['~child']['~children'][i]['#stats']['#itemsIn'] == num_of_items)
                        self.assertTrue(actual_result['profile']['executionTimings']['~child']['~children'][i]['#stats']['#itemsOut'] == num_of_items)

                self.query = 'explain select meta().id from {0} where join_day between 0 and 5 and ' \
                             'join_yr != 2011'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index'] == idx)
                self.assertTrue("covers" in str(plan))
                self.query = 'select meta().id from {0} where join_day between 0 and 5 and ' \
                             'join_yr != 2011'.format(query_bucket)
                actual_result = self.run_cbq_query(query_params={'profile': 'timings'})
                self.assertTrue("covers" in str(
                    actual_result['profile']['executionTimings']))
                self.assertTrue(actual_result['profile']['executionTimings']['~child']['~children'][0]['index'] == idx)
                expected_result = [doc for doc in all_docs_list if
                                   0 <= doc['join_day'] <= 5 and doc['join_yr'] != 2011]
                num_of_items = len(expected_result)
                for i in [1, 2]:
                    if i == 1:
                        self.assertTrue(
                            actual_result['profile']['executionTimings']['~child']['~children'][i]['~children'][1]['#stats']['#itemsIn'] == num_of_items)
                        self.assertTrue(
                            actual_result['profile']['executionTimings']['~child']['~children'][i]['~children'][1]['#stats']['#itemsOut'] == num_of_items)
                    else:
                        self.assertTrue(actual_result['profile']['executionTimings']['~child']['~children'][i]['#stats']['#itemsIn'] == num_of_items)
                        self.assertTrue(actual_result['profile']['executionTimings']['~child']['~children'][i]['#stats']['#itemsOut'] == num_of_items)

                self.query = 'select meta().id from {0} where join_day between 0 and 5 and ' \
                             'join_yr != 2015 order by meta().id'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select meta().id from {0} use index(`#primary`) where join_day between 0 and 5 and ' \
                             'join_yr != 2015 order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])

                self.query = 'select meta().id from {0} where join_day between 0 and 5 and ' \
                             'join_yr != 2011 order by meta().id'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select meta().id from {0} use index(`#primary`) where join_day between 0 and 5 and ' \
                             'join_yr != 2011 order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = 'select meta().id from {0} where join_day between 0 and 5 and ' \
                             'join_yr = 2015 order by meta().id'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select meta().id from {0} use index(`#primary`) where join_day between 0 and 5 and ' \
                             'join_yr = 2015 order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = 'select meta().id from {0} where join_day between 0 and 5 and ' \
                             'join_yr = 2011 order by meta().id'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select meta().id from {0} use index(`#primary`) where join_day between 0 and 5 and ' \
                             'join_yr = 2011 order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_array_index_all(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY v FOR v in {2} END) " \
                             "USING {3}".format(idx, query_bucket, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxVM"
                self.query = "CREATE INDEX {0} ON {1}( All ARRAY x.RAM FOR x in {2} END) " \
                             "USING {3}".format(idx2, query_bucket, "VMs", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.query = "EXPLAIN select name from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.VMs SATISFIES x.RAM between 1 and 5 END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan'
                                or plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan',
                                "actual=" + plan['~children'][0]['~children'][0]['#operator'])
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                self.query = "select name from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.VMs SATISFIES x.RAM between 1 and 5 END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                self.run_cbq_query()
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                created_indexes.remove(idx)
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx2, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx2), "Index is in list")
                created_indexes.remove(idx2)
                idx3 = "idxjoin_yr2"
                self.query = "CREATE INDEX {0} ON {1}( all ARRAY v FOR v within {2} END) " \
                             "USING {3}".format(idx3, query_bucket, "join_yr", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self.assertTrue(idx3 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX {0} ON {1}( aLL ARRAY x.RAM FOR x within {2} END) " \
                             "USING {3}".format(idx4, query_bucket, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")
                self.query = "EXPLAIN select name from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.VMs SATISFIES x.RAM between 1 and 5 END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result_within = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result_within)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan'
                                or plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan' or
                                plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan')
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx3 or result1 == idx4)
                self.assertTrue(result2 == idx4 or result2 == idx3)
                self.query = "EXPLAIN select name from {0} where any v within {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x within {0}.VMs SATISFIES x.RAM between 1 and 5 END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result_within = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result_within)
                self.assertTrue('IntersectScan' in str(plan)
                                or 'UnionScan' in str(plan) or 'DistinctScan' in str(plan),
                                f"Intersect Scan is not being used in and query for 2 array indexes: {plan}")
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result3 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result4 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
                    result3 = plan['~children'][0]['~children'][0]['scan']['index']
                    result4 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result3 == idx4 or result3 == idx3)
                self.assertTrue(result4 == idx3 or result4 == idx4)
                self.query = "select name from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x within {0}.VMs SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                actual_result_within = self.run_cbq_query()
                self.query = "select name from {0} use index (`#primary`) where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x within {0}.VMs SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result_within['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_array_index_all_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX {0} ON {1}(department, DISTINCT ARRAY v FOR v in {2} END,join_yr,name) " \
                             "USING {3}".format(idx, query_bucket, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxVM"
                self.query = "CREATE INDEX {0} ON {1}(name,All ARRAY x.RAM FOR x in {2} END,VMs) " \
                             "USING {3}".format(idx2, query_bucket, "VMs", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.query = "EXPLAIN select name from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == (
                    "cover ((`{0}`.`department`))".format(query_bucket)))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx)

                self.query = "EXPLAIN select name from {0} where any join_yr in {0}.join_yr " \
                             "satisfies join_yr = 2016 END ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == (
                    "cover ((`{0}`.`department`))".format(query_bucket)))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx)

                self.query = "select name from {0} where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from {0} use index(`#primary`) where any v in {0}.join_yr " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                expected_result = self.run_cbq_query()
                # self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))
                self.assertTrue(sorted(actual_result['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))
                self.query = "EXPLAIN select name from {0} where (ANY x within {0}.VMs " \
                             "SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "and name is not null ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == (
                    "cover ((`{0}`.`name`))".format(query_bucket)))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == (
                    "cover ((all (array (`x`.`RAM`) for `x` in (`{0}`.`VMs`) end)))".format(query_bucket)))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][2]) == (
                    "cover ((`{0}`.`VMs`))".format(query_bucket)))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][3]) == (
                    "cover ((meta(`{0}`).`id`))".format(query_bucket)))

                self.query = "EXPLAIN select name from {0} where (ANY foo within {0}.VMs " \
                             "SATISFIES foo.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "and name is not null ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == (
                    "cover ((`{0}`.`name`))".format(query_bucket)))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == (
                    "cover ((all (array (`x`.`RAM`) for `x` in (`{0}`.`VMs`) end)))".format(query_bucket)))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][2]) == (
                    "cover ((`{0}`.`VMs`))".format(query_bucket)))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][3]) == (
                    "cover ((meta(`{0}`).`id`))".format(query_bucket)))

                self.query = "select name from `{0}` where (ANY x within `{0}`.VMs " \
                             "SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "and name is not null ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from `{0}` use index(`#primary`) where (ANY x within {0}.VMs " \
                             "SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "and name is not null ORDER BY name limit 10"
                expected_result = self.run_cbq_query()
                # self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))
                self.assertTrue(sorted(actual_result['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_nested_index_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j for j in i end) " \
                             "FOR i in {2} END,tasks,department,name) USING {3}".format(idx, query_bucket,
                                                                                        "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx2"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j for j in i end) " \
                             "FOR i in {2} END,tasks,name) USING {3}".format(idx2, query_bucket,
                                                                             "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                idx3 = "nested_idx3"
                self.query = "CREATE INDEX {0} ON {1}(tasks,name,department) " \
                             "USING {2}".format(idx3, query_bucket, self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self.assertTrue(idx3 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")

                idx4 = "nested_idx4"
                self.query = "CREATE INDEX {0} ON {1}(tasks,name) " \
                             "USING {2}".format(idx4, query_bucket, self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "EXPLAIN select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i " \
                             "SATISFIES j='Search' end) END ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == 'nested_idx2')
                idx5 = "nested_idx5"
                self.query = ' CREATE INDEX {0} ON {1}((distinct ' \
                             '(array (i.RAM) for i in VMs end))) '.format(idx5, query_bucket) + \
                             'WHERE (_id = "query-testemployee10153.187782713003-0")'
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx5)
                self.assertTrue(idx5 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx5)

                self.query = 'explain SELECT VMs FROM {0}  WHERE ANY i IN VMs SATISFIES i.RAM = 10 END AND' \
                             ' _id="query-testemployee10153.187782713003-0"'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['scan']['index'] == idx5)
                self.query = "EXPLAIN select name from {0} WHERE tasks is not missing and " \
                             "name is not null ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index'] == 'nested_idx4')

                self.query = "EXPLAIN select name from {0} WHERE ANY task IN {0}.tasks SATISFIES  (ANY " \
                             "innertask IN task SATISFIES innertask='Search' end) END ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['scan']['index'] == 'nested_idx2')

                self.query = "EXPLAIN select name from {0} " \
                             "WHERE tasks is not missing and name is not null ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index'] == 'nested_idx4')
                self.query = "EXPLAIN select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i " \
                             "SATISFIES j='Search' end) END ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == (
                    "cover ((distinct (array (distinct (array `j` for `j` in `i` end)) "
                    "for `i` in (`{0}`.`tasks`) end)))".format(query_bucket)))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == (
                    "cover ((`{0}`.`tasks`))".format(query_bucket)))
                self.query = "select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i " \
                             "SATISFIES j='Search' end) END ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} use index(`#primary`) WHERE ANY i IN {0}.tasks SATISFIES  (" \
                             "ANY j IN i SATISFIES j='Search' end) END ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                expected_result = self.run_cbq_query()
                # self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))
                self.assertTrue(sorted(actual_result['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_nested_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "nested_idx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j for j in i end) " \
                             "FOR i in {2} END) USING {3}".format(idx, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxtasks"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY x FOR x in {2} END) " \
                             "USING {3}".format(idx2, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")
                self.query = "EXPLAIN select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i " \
                             "SATISFIES j='Search' end) END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
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
                self.query = "select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i " \
                             "SATISFIES j='Search' end) END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} use index(`#primary`) WHERE ANY i IN {0}.tasks SATISFIES " \
                             "(ANY j IN i SATISFIES j='Search' end) END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                expected_result = self.run_cbq_query()
                # self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))
                self.assertTrue(sorted(actual_result['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_shortest_covering_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            actual_result1 = None
            try:
                for ind in range(self.num_indexes):
                    index_name = f"coveringindexwithwhere{ind}"
                    self.query = "CREATE INDEX {0} ON {1}(email, VMs) where join_day > 10 " \
                                 "USING {2}".format(index_name, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    index_name2 = f"shortcoveringindex{ind}"
                    self.query = "CREATE INDEX {0} ON {1}(email) where join_day > 10 " \
                                 "USING {2}".format(index_name2, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name2)
                    created_indexes.append(index_name2)
                    self.query = "explain select email,VMs[0].RAM from {0} where email ".format(query_bucket) + \
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "select email,join_day from {0} ".format(query_bucket) + \
                                 "where email LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    actual_result1 = self.run_cbq_query()

                    self.query = "explain select email from {0} where email ".format(query_bucket) + \
                                 "LIKE '%@%.%' and join_day > 10"
                    if self.covering_index:
                        self.check_explain_covering_index(index_name2)
                    self.query = " select email from {0} where email ".format(query_bucket) + \
                                 "LIKE '%@%.%' and join_day > 10"
                    actual_result2 = self.run_cbq_query()
                    expected_result = [{"email": doc["email"]}
                                       for doc in self.full_list
                                       if re.match(r'.*@.*\..*', doc['email']) and doc['join_day'] > 10]
                    # self._verify_results(sorted(actual_result2['results']), sorted(expected_result))
                    self._verify_results(actual_result2['results'], expected_result)
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
                    self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
                    self.run_cbq_query()
                    self.sleep(15, 'wait for index')
                    self.query = "select email,join_day from {0} use index(`#primary`) " \
                                 "where email ".format(query_bucket) + \
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    result = self.run_cbq_query()
                    # self.assertEqual(sorted(actual_result1['results']), sorted(result['results']))
                    diffs = DeepDiff(actual_result1['results'], result['results'], ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
                    self.query = " select email from {0} where email ".format(query_bucket) + \
                                 "LIKE '%@%.%' and join_day > 10 order by meta().id limit 10"
                    actual_result2 = self.run_cbq_query()
                    self.query = " select email from {0} use index(`#primary`) where email ".format(query_bucket) + \
                                 "LIKE '%@%.%' and join_day > 10 order by meta().id limit 10"
                    result = self.run_cbq_query()
                    self.assertEqual((actual_result2['results']), (result['results']))
                    self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
                    self.run_cbq_query()

    def test_covering_nonarray_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            created_indexes = []
            try:
                idx = "nested_idx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j for j in i end) " \
                             "FOR i in {2} END) USING {3}".format(idx, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxtasks"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY x FOR x in {2} END) " \
                             "USING {3}".format(idx2, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")
                idx3 = "idxtasks0"
                self.query = "CREATE INDEX {0} ON {1}(tasks[1],department) " \
                             "USING {2}".format(idx3, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self.assertTrue(idx3 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxMarketing"
                self.query = "CREATE INDEX {0} ON {1}(tasks[0].Marketing,department) " \
                             "USING {2}".format(idx4, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")
                self.query = "EXPLAIN select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i " \
                             "SATISFIES j='Search' end) END ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                scan1 = plan['~children'][0]['~children'][0]
                self.assertTrue((scan1['#operator'] == 'IntersectScan') or
                                (scan1['#operator'] == 'UnionScan' and scan1['scans'][0][
                                    '#operator'] == 'IntersectScan' and scan1['scans'][1][
                                     '#operator'] == 'IntersectScan'),
                                "Single InteresectScan or UnionScan of two IntersectScans not being used:" + scan1[
                                    '#operator'])

                if 'scan' in plan['~children'][0]['~children'][0]['scans'][0]:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                else:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']

                if 'scan' in plan['~children'][0]['~children'][0]['scans'][1]:
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                else:
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scans'][0]['scan']['index']

                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                self.run_cbq_query()
                self.query = "explain select department from {0} WHERE  tasks[1]='Sales'".format(query_bucket) + \
                             " AND  NOT (department = 'Manager') limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index'] == idx3)
                self.assertTrue("cover" in str(plan))
                self.query = "select meta().id from {0} WHERE  tasks[1]='Sales'".format(query_bucket) + \
                             " AND  NOT (department = 'Manager') order by department, meta().id  limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select meta().id from {0} use index(`#primary`) " \
                             "WHERE  tasks[1]='Sales'".format(query_bucket) + \
                             " AND  NOT (department = 'Manager') order by department, meta().id limit 10"

                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                str1 = [{"region2": "International", "region1": "South"}, {"region2": "South"}]
                self.query = "explain select meta().id from {0} " \
                             "WHERE tasks[0].Marketing={1}".format(query_bucket, str1) + \
                             "AND  NOT (department = 'Manager') order BY meta().id limit 10"
                actual_result = self.run_cbq_query()

                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['index'] == idx4)
                self.assertTrue("cover" in str(plan))

                self.query = "select meta().id from {0} WHERE  tasks[0].Marketing={1}".format(query_bucket, str1) + \
                             " AND  NOT (department = 'Manager') order BY  meta().id  limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select meta().id from {0} use index(`#primary`) WHERE " \
                             "tasks[0].Marketing={1}".format(query_bucket, str1) + \
                             " AND  NOT (department = 'Manager') order BY meta().id limit 10"

                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `{0}`.`{1}` USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    # Test for MB-25590-Unnest Scan considers wrong array index
    def test_unnest_singlefield_array_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        query_bucket = self.get_collection_name('default')
        try:
            self.query = "create index ix300 on default (VMs)"
            self.run_cbq_query()
            created_indexes.append("ix300")
            self.query = 'explain SELECT v.os FROM {0} USE index (ix300) UNNEST default.VMs AS v ' \
                         'WHERE  v.os = "centos"'.format(query_bucket)
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue("ix300" in str(plan))
            self.assertTrue("covers" in str(plan))
            self.query = 'SELECT v.os FROM {0} USE index (ix300) UNNEST default.VMs AS v ' \
                         'WHERE v.os = "centos"'.format(query_bucket)
            actual_result = self.run_cbq_query()
            self.query = 'SELECT v.os FROM {0} USE index (`#primary`) UNNEST default.VMs AS v ' \
                         'WHERE  v.os = "centos"'.format(query_bucket)
            expected_result = self.run_cbq_query()
            diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
        finally:
            for idx in created_indexes:
                self.query = "DROP INDEX {1} ON {0} USING {2}".format("default", idx, self.index_type)
                actual_result = self.run_cbq_query()

    def test_simple_unnest_index_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "unnest_idx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( ALL array j for j in i end) FOR i in {2} END," \
                             "department,tasks,name) USING {3}".format(idx, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select {0}.name from {0} UNNEST tasks as i UNNEST i as j " \
                             "WHERE j = 'Search' ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(query_bucket)

                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(len(plan['~children'][0]['~children'][0]['scan']['covers']) == 5)

                self.query = "select {0}.name from {0}  UNNEST tasks as i UNNEST i as j " \
                             "WHERE j = 'Search'  ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(
                                 query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select {0}.name from {0} use index (`#primary`)  UNNEST tasks as i UNNEST i as j " \
                             "WHERE j = 'Search'  ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(query_bucket)
                expected_result = self.run_cbq_query()

                self.assertEqual(actual_result['results'],expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `{0}`.`{1}` USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_unnest_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "unnest_idx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( ALL array j for j in i end) FOR i in {2}" \
                             " END) USING {3}".format(idx, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select {0}.name from {0} UNNEST tasks as i UNNEST i as j " \
                             "WHERE j = 'Search' ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select {0}.name from {0}  UNNEST tasks as i UNNEST i as j " \
                             "WHERE j = 'Search'  ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select {0}.name from {0} use index (`#primary`)  UNNEST tasks as i UNNEST i as j " \
                             "WHERE j = 'Search'  ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(query_bucket)
                expected_result = self.run_cbq_query()

                # self.assertEqual(sorted(actual_result['results']), sorted(expected_result['results']))
                self.assertEqual(sorted(actual_result['results'], key=(lambda x: x['name'])),
                                 sorted(expected_result['results'], key=(lambda x: x['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_join_unnest_alias(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX {0} ON {1}( aLL ARRAY x.RAM FOR x within {2} END) " \
                             "USING {3}".format(idx4, query_bucket, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")
                self.query = "explain SELECT x FROM {1} emp1 USE INDEX({0})  UNNEST emp1.VMs as x  JOIN {1} task " \
                             "ON KEYS meta(`emp1`).id where x.RAM > 1 and x.RAM < 5  ;".format(idx4, query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)
                self.query = "SELECT x FROM {1} emp1 USE INDEX({0}) UNNEST emp1.VMs as x JOIN {1} task " \
                             "ON KEYS meta(`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;".format(idx4, query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "SELECT x FROM {0} emp1 USE INDEX(`#primary`)  UNNEST emp1.VMs as x  JOIN {0} task " \
                             "ON KEYS meta(`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;".format(query_bucket)
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_join_unnest_alias_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx4 = "idxVM2"
                self.query = "CREATE INDEX {0} ON {1}( aLL ARRAY x.RAM FOR x within {2} END,VMs) " \
                             "USING {3}".format(idx4, query_bucket, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "explain SELECT x FROM {1} emp1 USE INDEX({0})  UNNEST emp1.VMs as x  JOIN {1} task " \
                             "ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5   ;".format(idx4, query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))

                self.query = "SELECT x FROM {1} emp1 USE INDEX({0}) UNNEST emp1.VMs as x JOIN {1} task " \
                             "ON KEYS meta(`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;".format(idx4, query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "SELECT x FROM {0} emp1 USE INDEX(`#primary`)  UNNEST emp1.VMs as x  JOIN {0} task " \
                             "ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5 ;".format(query_bucket)
                expected_result = self.run_cbq_query()
                # self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_unnest_multilevel_attribute(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "nested_idx_attr_nest"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end)" \
                             " FOR i in {2} END) USING {3}".format(idx, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "explain SELECT emp.name FROM {0} emp  UNNEST emp.tasks as i UNNEST i.Marketing as j " \
                             "where j.region1 = 'South'".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "explain select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j " \
                             "IN i.Marketing SATISFIES j.region1='South' end) END".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i.Marketing " \
                             "SATISFIES j.region1='South' end) END ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} USE INDEX(`#primary`) WHERE ANY i IN {0}.tasks SATISFIES  (ANY j " \
                             "IN i.Marketing SATISFIES j.region1='South' end) END ".format(query_bucket) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                # self.assertTrue(sorted(actual_result['results']) == sorted(expected_result['results']))
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_unnest_multilevel_attribute_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx_attr_nest"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end)" \
                             " FOR i in {2} END,tasks,name) USING {3}".format(idx, query_bucket,
                                                                              "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "explain SELECT emp.name FROM {0} emp  UNNEST emp.tasks as i UNNEST i.Marketing as j" \
                             " where j.region1 = 'South'".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i.Marketing " \
                             "SATISFIES j.region1='South' end) END ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} USE INDEX(`#primary`) WHERE ANY i IN {0}.tasks SATISFIES  (ANY j " \
                             "IN i.Marketing SATISFIES j.region1='South' end) END ".format(query_bucket) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `{0}`.`{1}` USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j.region1 " \
                             "for j in i.Marketing end) FOR i in {2} END) USING {3}".format(idx, query_bucket,
                                                                                            "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY ( All array j.region1 for j in i.Marketing end) " \
                             "FOR i in {2} END) USING {3}".format(idx2, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                idx3 = "nested_idx_attr3"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end)" \
                             " FOR i in {2} END) USING {3}".format(idx3, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self.assertTrue(idx3 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "nested_idx_attr4"
                self.query = "CREATE INDEX {0} ON {1}(DISTINCT ARRAY ( DISTINCT array j.region1 for j in i.Marketing " \
                             "end) FOR i in {2} END,name,tasks) USING {3}".format(idx4, query_bucket,
                                                                                  "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")
                self.query = "EXPLAIN select name from {0} WHERE ANY i IN {0}.tasks SATISFIES " \
                             "(ANY j IN i.Marketing SATISFIES j.region1='South' end) END" \
                             " and name is not null ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)

                self.query = "EXPLAIN select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i.Marketing " \
                             "SATISFIES j.region1='South' end) END ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)

                self.query = "EXPLAIN select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i.Marketing " \
                             "SATISFIES j.region1='South' end) END ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)

                self.query = "EXPLAIN select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i.Marketing " \
                             "SATISFIES j.region1='South' end) END ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)
                self.query = "select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i.Marketing " \
                             "SATISFIES j.region1='South' end) END ".format(query_bucket) + "order BY name limit 10"
                actual_result1 = self.run_cbq_query()
                self.query = "select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i.Marketing " \
                             "SATISFIES j.region1='South' end) END ".format(query_bucket) + "order BY name limit 10"
                actual_result2 = self.run_cbq_query()

                self.query = "select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i.Marketing " \
                             "SATISFIES j.region1='South' end) END ".format(query_bucket) + "order BY name limit 10"
                actual_result3 = self.run_cbq_query()

                self.query = "select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN i.Marketing " \
                             "SATISFIES j.region1='South' end) END ".format(query_bucket) + "order BY name limit 10"
                actual_result4 = self.run_cbq_query()

                self.query = "select name from {0} use index (`#primary`) WHERE ANY i IN {0}.tasks SATISFIES (ANY j " \
                             "IN i.Marketing SATISFIES j.region1='South' end) END ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result5 = self.run_cbq_query()

                # self.assertTrue(sorted(actual_result1['results']) == sorted(actual_result2['results']))
                self.assertTrue(sorted(actual_result1['results'], key=(lambda x: x['name'])) == \
                                sorted(actual_result2['results'], key=(lambda x: x['name'])))

                # self.assertTrue(sorted(actual_result2['results']) == sorted(actual_result3['results']))
                self.assertTrue(sorted(actual_result2['results'], key=(lambda x: x['name'])) == \
                                sorted(actual_result3['results'], key=(lambda x: x['name'])))

                # self.assertTrue(sorted(actual_result4['results']) == sorted(actual_result3['results']))
                self.assertTrue(sorted(actual_result3['results'], key=(lambda x: x['name'])) == \
                                sorted(actual_result4['results'], key=(lambda x: x['name'])))

                # self.assertTrue(sorted(actual_result4['results']) == sorted(actual_result5['results']))
                self.assertTrue(sorted(actual_result4['results'], key=(lambda x: x['name'])) == \
                                sorted(actual_result5['results'], key=(lambda x: x['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_index_within(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY i FOR i within {2} END) " \
                             "USING {3}".format(idx, query_bucket, "hobbies", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY i FOR i within {2} END) " \
                             "USING {3}".format(idx2, query_bucket, "hobbies", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                self.query = "EXPLAIN select name from {0} use index({1}) WHERE ANY i within {0}.hobbies " \
                             "SATISFIES i = 'bhangra' END ".format(query_bucket, idx2) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)

                self.query = "EXPLAIN select name from {0} use index({1}) WHERE ANY i within {0}.hobbies " \
                             "SATISFIES i = 'bhangra' END ".format(query_bucket, idx) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "select name from {0} use index({1}) WHERE ANY i within {0}.hobbies " \
                             "SATISFIES i = 'bhangra' END ".format(query_bucket, idx) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                # actual_result1 = sorted(actual_result['results'])
                actual_result1 = sorted(actual_result['results'], key=(lambda x: x['name']))

                self.query = "select name from {0} use index({1}) WHERE ANY i within {0}.hobbies " \
                             "SATISFIES i = 'bhangra' END ".format(query_bucket, idx2) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                # actual_result2 = sorted(actual_result['results'])
                actual_result2 = sorted(actual_result['results'], key=(lambda x: x['name']))

                self.query = "select name from {0} use index(`#primary`) WHERE ANY i within {0}.hobbies " \
                             "SATISFIES i = 'bhangra' END ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                # actual_result3 = sorted(actual_result['results'])
                actual_result3 = sorted(actual_result['results'], key=(lambda x: x['name']))

                self.assertTrue(actual_result1 == actual_result2 == actual_result3)

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_array_index_in_distinct(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j for j in i.dance end) " \
                             "FOR i in {2} END) USING {3}".format(idx, query_bucket, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from {0} WHERE ANY i IN {0}.hobbies.hobby SATISFIES " \
                             "(ANY j IN i.dance SATISFIES j='contemporary' end) END and" \
                             " department='Support'".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.run_cbq_query()
                self.query = "select name from {0} WHERE ANY i IN {0}.hobbies.hobby SATISFIES  (ANY j IN i.dance " \
                             "SATISFIES j='contemporary' end) END and department='Support'".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                # actual_result = sorted(actual_result['results'])
                self.query = "select name from {0} USE INDEX(`#primary`) WHERE ANY i IN {0}.hobbies.hobby SATISFIES" \
                             "  (ANY j IN i.dance SATISFIES j='contemporary' end) END and " \
                             "department='Support'".format(query_bucket) + "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_array_index_in_all(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY ( all array j for j in i.dance end) " \
                             "FOR i in {2} END) USING {3}".format(idx, query_bucket, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from {0} WHERE ANY i IN {0}.hobbies.hobby SATISFIES  (ANY j IN " \
                             "i.dance SATISFIES j='contemporary' end) END and " \
                             "department='Support'".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "select name from {0} WHERE ANY i IN {0}.hobbies.hobby SATISFIES  (ANY j IN i.dance " \
                             "SATISFIES j='contemporary' end) END and department='Support'".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                # actual_result = sorted(actual_result['results'])
                self.query = "select name from {0} USE INDEX(`#primary`) WHERE ANY i IN {0}.hobbies.hobby SATISFIES " \
                             "(ANY j IN i.dance SATISFIES j='contemporary' end) END and " \
                             "department='Support'".format(query_bucket) + "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_array_index_in_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY ( all array j for j in i.dance end) FOR i in {2} " \
                             "END,hobbies.hobby,department,name) USING {3}".format(idx, query_bucket,
                                                                                   "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from {0} WHERE ANY i IN {0}.hobbies.hobby SATISFIES  (ANY j " \
                             "IN i.dance SATISFIES j='contemporary' end) END and " \
                             "department='Support'".format(query_bucket, ) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select name from {0} WHERE ANY i IN {0}.hobbies.hobby SATISFIES  (ANY j IN i.dance " \
                             "SATISFIES j='contemporary' end) END and department='Support'".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                # actual_result = sorted(actual_result['results'])
                # actual_result = sorted(actual_result['results'], key=(lambda x: x['name']))
                self.query = "select name from {0} USE INDEX(`#primary`) WHERE ANY i IN {0}.hobbies.hobby SATISFIES " \
                             "(ANY j IN i.dance SATISFIES j='contemporary' end) END and " \
                             "department='Support'".format(query_bucket) + "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_partial_index_distinct(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}( distinct array i FOR i in {2} END) " \
                             "WHERE (department = 'Support')  USING {3}".format(idx, query_bucket,
                                                                                "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from {0} WHERE department = 'Support' and (ANY i IN {0}.hobbies.hobby" \
                             " SATISFIES  i = 'art' END) ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from {0} WHERE department = 'Support' and ANY i IN {0}.hobbies.hobby " \
                             "SATISFIES i = 'art' END ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                # actual_result = sorted(actual_result['results'])
                self.query = "select name from {0} use index (`#primary`) WHERE department = 'Support' and ANY i IN " \
                             "{0}.hobbies.hobby SATISFIES i = 'art' END ".format(query_bucket) + "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `{0}`.`{1}` USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_partial_index_distinct_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}( distinct array i FOR i in {2} END,hobbies.hobby,name) WHERE " \
                             "(department = 'Support')  USING {3}".format(idx, query_bucket,
                                                                          "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from {0} WHERE department = 'Support' and (ANY i IN " \
                             "{0}.hobbies.hobby SATISFIES  i = 'art' END) ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select name from {0} WHERE department = 'Support' and ANY i IN {0}.hobbies.hobby " \
                             "SATISFIES i = 'art' END ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                # actual_result = sorted(actual_result['results'])
                self.query = "select name from {0} use index (`#primary`) WHERE department = 'Support' and ANY i IN " \
                             "{0}.hobbies.hobby SATISFIES i = 'art' END ".format(query_bucket) + "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `{0}`.`{1}` USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_indexcountscan(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            created_indexes = []
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}({2}) ".format(idx, query_bucket, 'meta().id') + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select count(1) from {0} WHERE meta().id like '{1}' ".format(query_bucket,
                                                                                                   'query-test%')

                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                result1 = plan['~children'][0]['index']
                self.assertTrue(result1 == idx, "actual_value=" + result1 + " vs " + idx)
                self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
                self.run_cbq_query()
                self.sleep(15, 'wait for index')
                self.query = "select count(1) from {0} use index(idx) WHERE meta().id like '{1}' ".format(
                    query_bucket, 'query-test%')
                actual_result = self.run_cbq_query()
                self.query = "select count(1) from {0} use index(`#primary`) WHERE meta().id like '{1}' ".format(
                    query_bucket, 'query-test%')
                expected_result = self.run_cbq_query()
                # self.assertTrue(actual_result['results'] == expected_result['results'])
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                self.assertTrue(actual_result['results'] == [{'$1': self.docs_per_day * 2016}])
                self.assertTrue("index_group_aggs" in str(plan))
                self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], "COUNT")
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'IndexScan3',
                    "IndexScan3 is not being used")
                self.query = "explain select a.cnt from (select count(1) as cnt from {0} where meta().id " \
                             "is not null) as a".format(query_bucket)
                actual_result2 = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result2)
                self.assertTrue(
                    plan['~children'][0]['#operator'] != 'IndexScan3',
                    "IndexScan3 should not be used in subquery")
                self.query = "select a.cnt from (select count(1) as cnt from {0} where meta().id is not null) " \
                             "as a".format(query_bucket)
                actual_result2 = self.run_cbq_query()
                self.query = "select a.cnt from (select count(1) as cnt from {0} where _id is not null) as a ".format(
                    query_bucket)
                result = self.run_cbq_query()
                # self.assertEqual(sorted(actual_result2['results']), sorted(result['results']))
                diffs = DeepDiff(actual_result2['results'], result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

                self.assertTrue(actual_result2['results'] == [{'cnt': self.docs_per_day * 2016}])
                self.query = "select count(DISTINCT 1) from {0} WHERE meta().id like '{1}' ".format(
                    query_bucket, 'query-test%')
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == [{'$1': 1}])
                self.query = "select count(DISTINCT meta().id) from {0} use index(idx) WHERE meta().id " \
                             "like '{1}' ".format(query_bucket, 'query-test%')
                actual_result = self.run_cbq_query()
                self.query = "select count(DISTINCT meta().id) from {0} use index(`#primary`) WHERE meta().id " \
                             "like '{1}' ".format(query_bucket, 'query-test%')
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.assertTrue(actual_result['results'] == [{'$1': self.docs_per_day * 2016}])
                self.query = "select count(1) from {0} use index(`#primary`) WHERE meta().id like '{1}'  ".format(
                    query_bucket, 'query-test%')
                result = self.run_cbq_query()
                # self.assertEqual(sorted(actual_result['results']), sorted(result['results']))
                diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                    self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
                    self.run_cbq_query()

    def test_index_projection(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}({2},{3}) ".format(idx, query_bucket, 'join_yr', '_id') + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select join_yr from {0} WHERE _id like '{1}' and join_yr=2010 ".format(
                    query_bucket, 'query-test%')
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [0, 1])

                self.query = "EXPLAIN select count(join_yr) from {0} WHERE _id like '{1}' and join_yr=2010 ".format(
                    query_bucket, 'query-test%')
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')

                self.query = "select join_yr from {0} WHERE _id like '{1}' and join_yr=2010 order by meta().id " \
                             "limit 10".format(query_bucket, 'query-test%')
                actual_result = self.run_cbq_query()
                self.query = "select join_yr from {0} use index(`#primary`) WHERE _id like '{1}' and join_yr=2010 " \
                             "order by meta().id limit 10".format(query_bucket, 'query-test%')
                expected_result = self.run_cbq_query()
                # self.assertEqual(actual_result['results'], expected_result['results'])
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

                self.query = "EXPLAIN select count(DISTINCT join_yr) from {0} WHERE _id like '{1}' and" \
                             " join_yr=2010 ".format(query_bucket, 'query-test%')
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = 'EXPLAIN select _id from {0} WHERE _id= "query-testemployee10317.9004497-0"'.format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index'], '#primary')

                self.query = "EXPLAIN select count( DISTINCT join_yr) from {0} WHERE join_yr is not null ".format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "EXPLAIN select count(DISTINCT _id) from {0} WHERE _id is not null ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index'], '#primary')

                self.query = "select join_yr from {0} WHERE join_yr=2010 order by meta().id limit 10".format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select join_yr from {0} use index(`#primary`) WHERE join_yr=2010 order by meta().id " \
                             "limit 10".format(query_bucket)
                expected_result = self.run_cbq_query()
                # self.assertEqual(actual_result['results'], expected_result['results'])
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

                self.query = "select count(join_yr) from {0} WHERE join_yr=2010 ".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(join_yr) from {0} use index(`#primary`) WHERE join_yr=2010 ".format(
                    query_bucket)
                expected_result = self.run_cbq_query()
                # self.assertEqual(sorted(actual_result['results']), sorted(expected_result['results']))
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

                self.query = "EXPLAIN select meta().id,join_yr from {0} WHERE join_yr=2010 ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [0])

                self.query = "EXPLAIN select count(meta().id),count(join_yr) from {0} WHERE join_yr is" \
                             " not missing".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [3, 4])

                self.query = "EXPLAIN select meta().id,join_yr from {0} WHERE join_yr between 2010 and 2012 ".format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [0])

                self.query = "EXPLAIN select count(meta().id),count(join_yr) from {0} WHERE join_yr " \
                             "between 2010 and 2012 ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [3, 4])

                self.query = "select meta().id,join_yr from {0} WHERE join_yr is not missing order by meta().id " \
                             "limit 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id,join_yr from {0} use index(`#primary`)  WHERE join_yr is not missing " \
                             "order by meta().id limit 10".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "select meta().id,join_yr from {0} WHERE join_yr between 2010 and 2012 " \
                             "order by meta().id limit 10".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select meta().id,join_yr from {0} use index(`#primary`)  WHERE join_yr between 2010 " \
                             "and 2012 order by meta().id limit 10".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "select count(meta().id),count(join_yr) from {0} WHERE join_yr between " \
                             "2010 and 2012 ".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(meta().id),count(join_yr) from {0} use index(`#primary`) WHERE join_yr " \
                             "between 2010 and 2012 ".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = 'explain select _id,meta().id from {0} WHERE join_yr > 2010'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("index_projection" not in str(plan))

                self.query = 'select _id,meta().id from {0} WHERE join_yr > 2010 order by meta().id limit 10'.format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select _id,meta().id from {0} use index(`#primary`)  WHERE join_yr > 2010 ' \
                             'order by meta().id limit 10'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = 'explain select join_yr from {0} where join_yr = 2010 or join_yr = 2012'.format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')
                self.assertEqual(plan['~children'][0]['index_projection']['entry_keys'], [0])

                self.query = 'select join_yr from {0} where join_yr = 2010 or join_yr = 2012'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select join_yr from {0} use index(`#primary`) where join_yr = 2010 or ' \
                             'join_yr = 2012'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = 'explain select join_yr,join_day from {0} where join_yr in [2010,2011,2012]'.format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection'], {'primary_key': True})

                self.query = 'explain select count(join_yr),count(join_day) from {0} where join_yr in ' \
                             '[2010,2011,2012]'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['index_projection'], {'primary_key': True})

                self.query = 'select join_yr,join_day from {0} where join_yr in [2010,2011,2012] order by meta().id ' \
                             'limit 10'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select join_yr,join_day from {0} use index(`#primary`) where join_yr in ' \
                             '[2010,2011,2012] order by meta().id limit 10'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_count_distinct(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}({2}) ".format(idx, query_bucket, 'job_title') + \
                             "where email  like '%@%.%' " + \
                             "USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "explain select count(1) from {0} where job_title IN ['Support','',null,'Engineer'] AND " \
                             "email like '%@%.%'".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')

                self.query = "select count(1) from {0} where job_title IN ['Support','',null,'Engineer'] AND " \
                             "email like '%@%.%'".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(1) from {0} use index(`#primary`) where job_title IN " \
                             "['Support','',null,'Engineer'] AND email like '%@%.%'".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(1) from {0} where job_title ='Support' AND  email like " \
                             "'%@%.%'".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')

                self.query = "select count(1) from {0} where job_title ='Support' AND  email like '%@%.%'".format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(1) from {0} use index(`#primary`) where job_title ='Support' AND " \
                             "email like '%@%.%'".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(1) from {0} where (job_title ='Support' or job_title is not null) " \
                             "AND email like '%@%.%'".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(1) from {0} where (job_title ='Support' or job_title is not null) AND " \
                             "email like '%@%.%'".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(1) from {0}  use index(`#primary`) where (job_title ='Support' or " \
                             "job_title is not null) AND email like '%@%.%'".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = "explain select count(1) from {0} where (job_title ='Support' or job_title is not " \
                             "missing) AND email like '%@%.%'".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')

                self.query = "select count(1) from {0} where (job_title ='Support' or job_title is not missing) " \
                             "AND email like '%@%.%'".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(1) from {0} use index(`#primary`) where (job_title ='Support' or job_title" \
                             " is not missing) AND email like '%@%.%'".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(1) from {0} where (job_title ='Support' or job_title = 'Engineer')" \
                             " AND email like '%@%.%'".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(1) from {0} where (job_title ='Support' or job_title = 'Engineer') AND " \
                             "email like '%@%.%'".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(1) from {0} use index(`#primary`)  where (job_title ='Support' or " \
                             "job_title = 'Engineer') AND email like '%@%.%'".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                idx2 = "idx2"
                self.query = "CREATE INDEX {0} ON {1}({2},{3}) ".format(idx2, query_bucket,
                                                                        'VMs[1].os', 'tasks_points.task1') + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)

                idx = "idx3"
                self.query = "CREATE INDEX {0} ON {1}({2},{3}) where " \
                             "tasks_points.task1=1 ".format(idx, query_bucket, 'VMs[1].os', 'name') + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)

                self.query = "explain select count(distinct VMs[1].os) from {0} where VMs[1].os='windows' and " \
                             "tasks_points.task1>1".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "select count(distinct VMs[1].os) from {0} where VMs[1].os='windows' and " \
                             "tasks_points.task1>1".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from {0} use index(`#primary`) where " \
                             "VMs[1].os='windows' and tasks_points.task1>1".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from {0} where VMs[1].os='windows' and " \
                             "tasks_points.task1 in [1,2,3]".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "select count(distinct VMs[1].os) from {0} where VMs[1].os='windows' and " \
                             "tasks_points.task1 in [1,2,3]".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from {0} use index(`#primary`) where " \
                             "VMs[1].os='windows' and tasks_points.task1 in [1,2,3]".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from {0} where VMs[1].os='windows' or " \
                             "tasks_points.task1>1".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(distinct VMs[1].os) from {0} where VMs[1].os='windows' or " \
                             "tasks_points.task1>1".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from {0} use index(`#primary`)  where " \
                             "VMs[1].os='windows' or tasks_points.task1>1".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from {0} where VMs[1].os='windows' and " \
                             "tasks_points.task1!=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(distinct VMs[1].os) from {0} where VMs[1].os='windows' and " \
                             "tasks_points.task1!=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from {0}  use index(`#primary`) where " \
                             "VMs[1].os='windows' and tasks_points.task1!=1".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from {0} where VMs[1].os='windows' and " \
                             "tasks_points.task1=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "select count(distinct VMs[1].os) from {0} where VMs[1].os='windows' and " \
                             "tasks_points.task1=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from {0} use index(`#primary`) where " \
                             "VMs[1].os='windows' and tasks_points.task1=1".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from {0} where VMs[1].os in " \
                             "['centos','windows','ubuntu'] and tasks_points.task1=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "explain select count(distinct VMs[1].os) from {0} where VMs[1].os in " \
                             "['centos','windows','windows','centos','ubuntu','ubuntu',null] and " \
                             "tasks_points.task1=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "select count(distinct VMs[1].os) from {0} where VMs[1].os in " \
                             "['centos','windows','ubuntu'] and tasks_points.task1=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from {0} use index(`#primary`) where VMs[1].os in " \
                             "['centos','windows','ubuntu'] and tasks_points.task1=1".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "select count(distinct VMs[1].os) from {0} where VMs[1].os in " \
                             "['centos','windows','windows','centos','ubuntu','ubuntu',null] and " \
                             "tasks_points.task1=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from {0} use index(`#primary`) where VMs[1].os in " \
                             "['centos','windows','windows','centos','ubuntu','ubuntu',null] and " \
                             "tasks_points.task1=1".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct VMs[1].os) from {0} where VMs[1].os in " \
                             "['$1','centos','windows','ubuntu'] and tasks_points.task1=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "explain select count(distinct VMs[1].os) from {0} where VMs[1].os in " \
                             "['centos','windows','ubuntu'] and tasks_points.task1 in [1,2,3]".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('index_group_aggs' in str(plan))
                self.assertTrue(plan['~children'][0]['index_group_aggs']['aggregates'][0]['distinct'])

                self.query = "select count(distinct VMs[1].os) from {0} where VMs[1].os in " \
                             "['centos','windows','ubuntu'] and tasks_points.task1 in [1,2,3]".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct VMs[1].os) from {0} use index(`#primary`) where VMs[1].os in " \
                             "['centos','windows','ubuntu'] and tasks_points.task1 in [1,2,3]".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct name) from {0} where VMs[1].os='windows' and " \
                             "name = 'employee-1' and tasks_points.task1>1".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(distinct name) from {0} where VMs[1].os='windows' and name = 'employee-1' " \
                             "and tasks_points.task1>1".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct name) from {0} use index(`#primary`) where VMs[1].os='windows' " \
                             "and name = 'employee-1' and tasks_points.task1>1".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct name) from {0}  where VMs[1].os='windows' and " \
                             "name is not null and tasks_points.task1=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(distinct name) from {0}  where VMs[1].os='windows' and name is not null " \
                             "and tasks_points.task1=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct name) from {0} use index(`#primary`) where VMs[1].os='windows' " \
                             "and name is not null and tasks_points.task1=1".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = "explain select count(distinct name) from {0} where VMs[1].os='windows' and name in " \
                             "['employee-1','employee-2','employee-3'] and tasks_points.task1=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'IndexScan3')

                self.query = "select count(distinct name) from {0} where VMs[1].os='windows' and name in " \
                             "['employee-1','employee-2','employee-3'] and tasks_points.task1=1".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select count(distinct name) from {0} use index(`#primary`) where VMs[1].os='windows' " \
                             "and name in ['employee-1','employee-2','employee-3'] and " \
                             "tasks_points.task1=1".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_avoid_full_span(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = 'create index {0} on {1}( join_yr,join_day )'.format(idx, query_bucket)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "explain select * from {0} where join_yr in [2011,2012]".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['spans'][0]['range'],
                                 [{'high': '2011', 'inclusion': 3, 'index_key': '`join_yr`', 'low': '2011'}])
                self.assertEqual(plan['~children'][0]['spans'][1]['range'],
                                 [{'high': '2012', 'inclusion': 3, 'index_key': '`join_yr`', 'low': '2012'}])
                self.query = "select * from {0} where join_yr in [2011,2012] order by meta().id".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select * from {0} use index(`#primary`) where join_yr in [2011,2012] ' \
                             'order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
                self.query = 'explain select * from {0} where join_yr =2011 and join_day in ' \
                             '[1,2,3,$1,$2,null,""]'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['spans'][0]['range'],
                                 [{'high': '2011', 'inclusion': 3, 'index_key': '`join_yr`', 'low': '2011'},
                                  {'high': '1', 'inclusion': 3, 'index_key': '`join_day`', 'low': '1'}])
                self.assertEqual(plan['~children'][0]['spans'][1]['range'],
                                 [{'high': '2011', 'inclusion': 3, 'index_key': '`join_yr`', 'low': '2011'},
                                  {'high': '2', 'inclusion': 3, 'index_key': '`join_day`', 'low': '2'}])
                self.assertEqual(plan['~children'][0]['spans'][2]['range'],
                                 [{'high': '2011', 'inclusion': 3, 'index_key': '`join_yr`', 'low': '2011'},
                                  {'high': '3', 'inclusion': 3, 'index_key': '`join_day`', 'low': '3'}])
                self.assertEqual(plan['~children'][0]['spans'][3]['range'],
                                 [{'high': '2011', 'inclusion': 3, 'index_key': '`join_yr`', 'low': '2011'},
                                  {'high': '$1', 'inclusion': 3, 'index_key': '`join_day`', 'low': '$1'}])
                self.assertEqual(plan['~children'][0]['spans'][4]['range'],
                                 [{'high': '2011', 'inclusion': 3, 'index_key': '`join_yr`', 'low': '2011'},
                                  {'high': '$2', 'inclusion': 3, 'index_key': '`join_day`', 'low': '$2'}])
                self.assertEqual(plan['~children'][0]['spans'][5]['range'],
                                 [{'high': '2011', 'inclusion': 3, 'index_key': '`join_yr`', 'low': '2011'},
                                  {'high': '""', 'inclusion': 3, 'index_key': '`join_day`', 'low': '""'}])
                self.query = 'select * from {0} where join_yr =2011 and join_day in [1,2,3,null,""] ' \
                             'order by meta().id'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'select * from {0} use index(`#primary`) where join_yr =2011 and join_day in ' \
                             '[1,2,3,null,""] order by meta().id'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_distinct_meta(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}({2},meta().id) ".format(idx, query_bucket, '_id') + \
                             " where _id like '{0}' ".format('query-test%') + " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "explain SELECT DISTINCT _id, meta().id from {0} WHERE _id " \
                             "like 'query-test%'".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][1]['~child']['~children'][2]['#operator'], "Distinct")
                self.query = "SELECT DISTINCT _id, meta().id from {0} WHERE _id like 'query-test%'".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "SELECT DISTINCT _id, meta().id from {0} use index(`#primary`) WHERE _id " \
                             "like 'query-test%'".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                idx = "idx2"
                self.query = "CREATE INDEX {0} ON {1}({2},meta().id) ".format(idx, query_bucket, '_id') + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = 'explain SELECT DISTINCT _id, meta().id from {0} WHERE _id > ' \
                             '"query-testemployee10317.9004497-0"'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][1]['~child']['~children'][2]['#operator'], "Distinct")
                self.query = 'SELECT DISTINCT _id, meta().id from {0} WHERE _id > ' \
                             '"query-testemployee10317.9004497-0"'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'SELECT DISTINCT _id, meta().id from {0} use index(`#primary`) WHERE _id > ' \
                             '"query-testemployee10317.9004497-0"'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])

                self.query = 'explain SELECT DISTINCT _id, meta().id from {0} WHERE _id in ' \
                             '["query-testemployee10317.9004497-0","query-testemployee10317.9004497-1",' \
                             '"query-testemployee10317.9004497-2"]'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][1]['~child']['~children'][2]['#operator'], "Distinct")
                self.query = 'SELECT DISTINCT _id, meta().id from {0} WHERE _id in ' \
                             '["query-testemployee10317.9004497-0","query-testemployee10317.9004497-1",' \
                             '"query-testemployee10317.9004497-2"]'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'SELECT DISTINCT _id, meta().id from {0} use index(`#primary`) WHERE _id in ' \
                             '["query-testemployee10317.9004497-0","query-testemployee10317.9004497-1",' \
                             '"query-testemployee10317.9004497-2"]'.format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_create_index_desc(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}({2} DESC) ".format(idx, query_bucket, '_id')
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_partial_like_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            actual_result = None
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}({2}) ".format(idx, query_bucket, '_id') + \
                             " where _id like '{0}' ".format('query-test%') + " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select meta().id from {0} WHERE _id like '{1}' ".format(query_bucket,
                                                                                              'query-test%')
                self.check_explain_covering_index(idx)
                self.query = "EXPLAIN select meta().id from {0} WHERE _id like '{1}' ".format(
                    query_bucket, 'query-testemployee10%')

                self.check_explain_covering_index(idx)
                self.query = "select meta().id from {0} WHERE _id like '{1}' ".format(query_bucket,
                                                                                      'query-testemployee10%')
                actual_result = self.run_cbq_query()
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                    self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
                    self.run_cbq_query()
                    self.sleep(15, 'wait for index')
                    self.query = "select meta().id from {0} use index(`#primary`) WHERE _id like '{1}' ".format(
                        query_bucket, 'query-testemployee10%')
                    result = self.run_cbq_query()
                    diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
                    self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
                    self.run_cbq_query()
                    if diffs:
                        self.assertTrue(False, diffs)

    def test_dynamic_names(self):
        self.fail_if_no_buckets()
        num_docs = self.docs_per_day * 2016
        bucket_doc_map = {"default": num_docs}
        bucket_status_map = {"default": "healthy"}
        query_bucket = self.get_collection_name('default')
        self.wait_for_buckets_status(bucket_status_map, 5, 120)
        self.wait_for_bucket_docs(bucket_doc_map, 5, 120)
        self.run_cbq_query('create primary index on {0} using GSI'.format(query_bucket))
        self._wait_for_index_online("default", "#primary")
        try:
            self.query = 'select { UPPER("foo"):1,"foo"||"bar":2 }'
            actual_result = self.run_cbq_query()
            expected_result = [{'$1': {'foobar': 2, 'FOO': 1}}]
            self.assertEqual(actual_result['results'], expected_result)
            self.query = 'insert into {0} (key k,value doc)  select to_string(name)|| UUID() as k , doc as doc ' \
                         'from {0} where name is not null'.format(query_bucket)
            self.run_cbq_query()
            self.query = 'select * from {0}'.format(query_bucket)
            new_num_docs = num_docs * 2
            bucket_doc_map = {"default": new_num_docs}
            self.wait_for_bucket_docs(bucket_doc_map, 5, 120)
            actual_result = self.run_cbq_query()
            self.assertEqual(actual_result['metrics']['resultCount'], new_num_docs)
        finally:
            self.query = 'delete from {0} where meta().id in (select RAW to_string(name)|| UUID()  from {0} d)'.format(
                query_bucket)
            self.run_cbq_query()
            self.run_cbq_query("DROP PRIMARY INDEX ON {0}".format(query_bucket))

    def test_between_spans(self):
        self.fail_if_no_buckets()
        if self.DGM:
            self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            self.query = 'create index ix5 on {0}(x)'.format(query_bucket)
            self.run_cbq_query()
            self.query = 'insert into {0} (KEY, VALUE) VALUES ("kk02",{{"x":100,"y":101,"z":102,"id":"kk02"}})'.format(
                query_bucket)
            self.run_cbq_query()
            self.query = 'explain select count(1) from {0} where x BETWEEN $1 AND $2'.format(query_bucket)
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue('index_group_aggs' in str(plan))
            self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')
            self.query = 'explain select count(1) from {0} where x >= $1 AND x <= $2'.format(query_bucket)
            actual_result = self.run_cbq_query()
            plan = self.ExplainPlanHelper(actual_result)
            self.assertTrue('index_group_aggs' in str(plan))
            self.assertEqual(plan['~children'][0]['index_group_aggs']['aggregates'][0]['aggregate'], 'COUNT')
            self.query = 'drop index {0}.ix5'.format(query_bucket)
            self.run_cbq_query()
            self.query = 'delete from {0} use keys ["kk02"]'.format(query_bucket)
            self.run_cbq_query()

    def test_and_every_array_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = 'CREATE INDEX `rec1-1_record_by_index_map` ON {0}((distinct (array `i` for `i` ' \
                             'in object_pairs(`indexMap`) end)))'.format(query_bucket)
                self.run_cbq_query()
                created_indexes.append('rec1-1_record_by_index_map')
                self.query = 'CREATE INDEX `rec1-2_record_by_index_map` ON {0}((distinct (array `i` for `i` ' \
                             'in object_pairs(`data`) end)))'.format(query_bucket)
                self.run_cbq_query()
                created_indexes.append('rec1-2_record_by_index_map')
                self.query = 'CREATE INDEX `rec1-3_record_by_index_map` ON {0}((distinct (array `j` for `j` ' \
                             'in object_pairs(`data`) end)))'.format(query_bucket)
                self.run_cbq_query()
                created_indexes.append('rec1-3_record_by_index_map')
                self.query = 'insert into {0} '.format(query_bucket) + '(KEY, VALUE) VALUES ("test",{"type":"testType","indexMap":' \
                             '{"key1":"val1", "key2":"val2"},"data":{"foo":"bar"}})'
                self.run_cbq_query()
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM {0} AS r ' \
                             'WHERE any i in object_pairs(indexMap) satisfies i = {{ "name":"key1", "value":"val1"}} ' \
                             'end AND any i in object_pairs(indexMap) satisfies i = {{ "name":"key2", "val":"val2"}} ' \
                             'end LIMIT 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                #with cbo intersectscan might not happen
                self.assertTrue('IntersectScan' in str(plan) or 'DistinctScan' in str(plan), f"Intersectscan or DistinctScan was expected please check plan: {plan}")
                self.assertTrue('rec1-1_record_by_index_map' in str(plan), f"The right index is not being used, check plan {plan}")
                self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM {0} AS r WHERE any i in ' \
                             'object_pairs(indexMap) satisfies i = {{ "name":"key1", "val":"val1"}} end ' \
                             'AND any i in object_pairs(indexMap) satisfies i = {{ "name":"key2", "val":"val2"}} ' \
                             'end LIMIT 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'][0]['doc'], ({'data': {'foo': 'bar'}, 'indexMap': {'key1': 'val1', 'key2': 'val2'}, 'type': 'testType'}), ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM {0} AS r WHERE every i in' \
                             ' object_pairs(indexMap) satisfies i = {{ "name":"key1", "val":"val1"}} end AND ' \
                             'any i in object_pairs(indexMap) satisfies i = {{ "name":"key2", "val":"val2"}} ' \
                             'end LIMIT 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('DistinctScan' in str(plan), f"Distinctscan was expected please check plan: {plan}")
                self.assertTrue('rec1-1_record_by_index_map' in str(plan), f"The right index is not being used, check plan {plan}")
                self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM {0} AS r WHERE every i in ' \
                             'object_pairs(indexMap) satisfies i = {{ "name":"key1", "val":"val1"}} end or ' \
                             'any i in object_pairs(indexMap) satisfies i = {{ "name":"key2", "val":"val2"}} ' \
                             'end LIMIT 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'][0]['doc'], ({'data': {'foo': 'bar'}, 'indexMap': {'key1': 'val1', 'key2': 'val2'}, 'type': 'testType'}), ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM {0} AS r WHERE ' \
                             'any i in object_pairs(indexMap) satisfies i = {{ "name":"key1", "val":"val1"}} end ' \
                             'AND every i in object_pairs(indexMap) satisfies i = {{ "name":"key2", "val":"val2"}} ' \
                             'end LIMIT 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('DistinctScan' in str(plan), f"Distinctscan was expected please check plan: {plan}")
                self.assertTrue('rec1-1_record_by_index_map' in str(plan), f"The right index is not being used, check plan {plan}")
                self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM {0} AS r WHERE any i in' \
                             ' object_pairs(indexMap) satisfies i = {{ "name":"key1", "val":"val1"}} end ' \
                             'or every i in object_pairs(indexMap) satisfies i = {{ "name":"key2", "val":"val2"}} end' \
                             ' LIMIT 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'][0]['doc'], ({'data': {'foo': 'bar'}, 'indexMap': {'key1': 'val1', 'key2': 'val2'}, 'type': 'testType'}), ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM {0} AS r WHERE some and' \
                             ' every i in object_pairs(indexMap) satisfies i = {{ "name":"key1", "val":"val1"}} end' \
                             ' AND some and every i in object_pairs(indexMap) satisfies i = ' \
                             '{{ "name":"key2", "val":"val2"}} end LIMIT 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                #with cbo intersectscan might not happen
                self.assertTrue('IntersectScan' in str(plan) or 'DistinctScan' in str(plan), f"Intersectscan was expected please check plan: {plan}")
                self.assertTrue('rec1-1_record_by_index_map' in str(plan), f"The right index is not being used, check plan {plan}")
                self.query = 'insert into {0} '.format(query_bucket) + '(KEY, VALUE) VALUES ("test1",{"type":"testType",' \
                             '"indexMap":{"key1":"val1"},"data":{"foo":"bar"}})'
                self.run_cbq_query()
                self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM {0} AS r WHERE SOME AND' \
                             ' EVERY i in object_pairs(indexMap) satisfies i = {{ "name":"key1", "val":"val1"}} end ' \
                             'AND SOME AND EVERY i in object_pairs(data) satisfies i = {{"name":"foo", "val":"bar"}} ' \
                             'end LIMIT 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'][0]['doc'], ({'data': {'foo': 'bar'}, 'indexMap': {'key1': 'val1'}, 'type': 'testType'}), ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM {0} AS r ' \
                             'WHERE any i in object_pairs(indexMap) satisfies i = {{ "name":"key1", "val":"val1"}} ' \
                             'end AND any j in object_pairs(indexMap) satisfies j = {{ "name":"key2", "val":"val2"}}' \
                             ' end LIMIT 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('IntersectScan' in str(plan) or 'DistinctScan' in str(plan), f"Intersectscan was expected please check plan: {plan}")
                self.assertTrue('rec1-1_record_by_index_map' in str(plan), f"The right index is not being used, check plan {plan}")

                self.query = 'SELECT r AS doc, meta(r).cas AS revision FROM {0} AS r ' \
                             'WHERE any i in object_pairs(indexMap) satisfies i = {{ "name":"key1", "val":"val1"}} ' \
                             'end AND any j in object_pairs(indexMap) satisfies j = ' \
                             '{{ "name":"key2", "val":"val2"}} end LIMIT 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'][0]['doc'], ({'data': {'foo': 'bar'}, 'indexMap': {'key1': 'val1', 'key2': 'val2'}, 'type': 'testType'}), ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                self.query = 'explain SELECT r AS doc, meta(r).cas AS revision FROM {0} AS r ' \
                             'WHERE some and every i in object_pairs(indexMap) satisfies i = ' \
                             '{{ "name":"key1", "val":"val1"}} end AND some and every j in ' \
                             'object_pairs(indexMap) satisfies j = {{ "name":"key2", "val":"val2"}} ' \
                             'end LIMIT 100'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue('IntersectScan' in str(plan) or 'DistinctScan' in str(plan), f"Intersectscan was expected please check plan: {plan}")
                self.assertTrue('rec1-1_record_by_index_map' in str(plan), f"The right index is not being used, check plan {plan}")
                self.query = 'delete from {0} use keys["test","test1"]'.format(query_bucket)
                self.run_cbq_query()
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `{0}`.`{1}` USING {2}".format(query_bucket, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_distinct_raw_orderby(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}({2},{3}) ".format(idx, query_bucket, 'name', 'join_day') + \
                             " USING {0}".format(self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select distinct raw join_day from {0} WHERE name like '{0}' order by " \
                             "join_day limit 10".format(query_bucket, 'query-test%')

                self.check_explain_covering_index(idx)
                self.query = "select distinct raw join_day from {0} WHERE name like '{0}' order by join_day " \
                             "limit 10 ".format(query_bucket, 'employee%')

                actual_result = self.run_cbq_query()
                self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
                self.run_cbq_query()
                self.sleep(15, 'wait for index')
                self.query = "select distinct raw join_day from {0} use index(`#primary`) WHERE name like '{0}' " \
                             "order by join_day limit 10 ".format(query_bucket, 'employee%')

                expected_result = self.run_cbq_query()
                self.assertEqual(actual_result['results'], expected_result['results'])
                self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
                self.run_cbq_query()
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_partial_index_all(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}( all array i FOR i in {2} END) WHERE (department = 'Support') " \
                             "USING {3}".format(idx, query_bucket, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from {0} WHERE department = 'Support' and (ANY i IN {0}.hobbies.hobby " \
                             "SATISFIES  i = 'art' END) ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from {0} WHERE department = 'Support' and ANY i IN {0}.hobbies.hobby " \
                             "SATISFIES i = 'art' END ".format(query_bucket) + "order BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from {0} WHERE department = 'Support' and ANY i IN {0}.hobbies.hobby " \
                             "SATISFIES i = 'art' END ".format(query_bucket) + "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `{0}`.`{1}` USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_with_inner_joins(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "nested_inner_join"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) " \
                             "FOR i in {2} END) USING {3}".format(idx, query_bucket, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee  JOIN {0} as new_project_full ".format(query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES " \
                             "(ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan', "DistinctScan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_left_outer_join(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "outer_join"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) " \
                             "FOR i in {2} END) USING {3}".format(idx, query_bucket, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "outer_join_all"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( All array j.city for j in i end) " \
                             "FOR i in {2} END) USING {3}".format(idx2, query_bucket, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee  left JOIN {0} as new_project_full ".format(query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES " \
                             "(ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx or result1 == idx2)
                self.query = "EXPLAIN SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee use index ({1}) left JOIN {0} as new_project_full ".format(
                                 query_bucket, idx2) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES " \
                             "(ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan', "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.query = "SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee left JOIN {0} as new_project_full ".format(query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  " \
                             "(ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result1 = (actual_result['results'])
                self.query = "SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee left JOIN {0} as new_project_full ".format(query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  " \
                             "(ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result2 = (actual_result['results'])
                self.query = "SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee use index (`#primary`) left JOIN {0} " \
                             "as new_project_full ".format(query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  " \
                             "(ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result3 = (actual_result['results'])
                self.assertTrue(actual_result1 == actual_result3)
                self.assertTrue(actual_result2 == actual_result3)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `{0}`.`{1}` USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_with_inner_joins_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_inner_join"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) " \
                             "FOR i in {2} END,address,department) USING {3}".format(idx, query_bucket,
                                                                                     "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT employee.department new_project " + \
                             "FROM {0} as employee  JOIN {0} as new_project_full ".format(query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  (ANY j IN i " \
                             "SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "SELECT employee.department new_project " + \
                             "FROM {0} as employee  JOIN {0} as new_project_full ".format(query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address " \
                             "SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                self.query = "SELECT employee.department new_project " + \
                             "FROM {0} as employee use index (`#primary`)  JOIN {0} as new_project_full ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN employee.address SATISFIES  " \
                             "(ANY j IN i SATISFIES j.city='Delhi' end) END "
                expected_result = self.run_cbq_query()
                # expected_result = expected_result['results']
                # self.assertTrue(sorted(expected_result) == sorted(actual_result['results']))
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_regexp(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "iregex"
                self.query = " CREATE INDEX {0} ON {1}( DISTINCT ARRAY REGEXP_LIKE(v.os,{2})  FOR v IN VMs END )  " \
                             "USING {3}".format(idx, query_bucket, "'ub%'", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from {0}  WHERE ANY v IN VMs SATISFIES " \
                             "REGEXP_LIKE(v.os,{1}) = 1 END ".format(query_bucket, "'ub%'") + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "Distinct Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select * from {0} use index(`#primary`)  WHERE ANY v IN VMs SATISFIES " \
                             "REGEXP_LIKE(v.os,{1}) = 1  END  ".format(query_bucket, "'ub%'") + "order BY name limit 10"
                self.query = "select * from {0}  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,{0}) = 1 END  ".format(
                    query_bucket, "'ub%'") + "order BY name limit 10"
                actual_result = self.run_cbq_query()
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `{0}`.`{1}` USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_regexp_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                idx = "iregex"
                self.query = " CREATE INDEX {0} ON {1}( DISTINCT ARRAY REGEXP_LIKE(v.os,{2})  FOR v IN VMs END,VMs )" \
                             " USING {3}".format(idx, query_bucket, "'ub%'", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select VMs from {0}  WHERE ANY v IN VMs SATISFIES " \
                             "REGEXP_LIKE(v.os,{1}) = 1 END  ".format(query_bucket, "'ub%'") + "limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select VMs from {0}  WHERE ANY v IN VMs SATISFIES REGEXP_LIKE(v.os,{1}) = 1 END  ".format(
                    query_bucket, "'ub%'") + "limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select VMs from {0} use index(`#primary`)  WHERE ANY v IN VMs SATISFIES " \
                             "REGEXP_LIKE(v.os,{1}) = 1  END  ".format(query_bucket, "'ub%'") + "limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `{0}`.`{1}` USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_greatest(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX {0} ON {1}(department, DISTINCT ARRAY GREATEST(v.RAM,100)  " \
                             "FOR v IN VMs END )  USING {2}".format(idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from {0} WHERE department = 'Support' and ANY v IN VMs SATISFIES " \
                             "GREATEST(v.RAM,100) END ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from {0} WHERE department = 'Support' and ANY v IN VMs SATISFIES " \
                             "GREATEST(v.RAM,100) END ".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} USE index(`#primary`) WHERE department = 'Support' and ANY v " \
                             "IN VMs SATISFIES GREATEST(v.RAM,100) END ".format(query_bucket)
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_greatest_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX {0} ON {1}(department, DISTINCT ARRAY GREATEST(v.RAM,100)  " \
                             "FOR v IN VMs END ,VMs,name)  USING {2}".format(idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from {0} WHERE department = 'Support' and ANY v IN VMs " \
                             "SATISFIES GREATEST(v.RAM,100) END ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select name from {0} WHERE department = 'Support' and ANY v IN VMs " \
                             "SATISFIES GREATEST(v.RAM,100) END ".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} USE index(`#primary`) WHERE department = 'Support' and ANY v " \
                             "IN VMs SATISFIES GREATEST(v.RAM,100) END ".format(query_bucket)
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_nest_keys_where(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY j for j in department end) USING {2}".format(
                    idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + "FROM {0} emp NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN emp.department SATISFIES j = 'Support' end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name FROM {0} emp NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN emp.department SATISFIES j = 'Support' end;"
                actual_result = self.run_cbq_query()
                self.query = "select emp.name FROM {0} emp USE INDEX(`#primary`) " \
                             "NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN emp.department SATISFIES j = 'Support' end;"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nest_keys_where_not_equal(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY j for j in department end) USING {2}".format(
                    idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + "FROM {0} emp NEST {0} VMs ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN emp.department SATISFIES j != 'Engineer' end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + "FROM {0} emp NEST {0} VMs ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN emp.department SATISFIES j = 'Support' end;"
                actual_result = self.run_cbq_query()
                self.query = "select emp.name FROM {0} emp USE INDEX(`#primary`)" \
                             " NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id where ANY j IN emp.department SATISFIES j = 'Support' end;"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nest_keys_where_between(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY j for j in join_yr end," \
                             "name,join_yr ) USING {2}".format(idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "idxbetween"
                self.query = "CREATE INDEX {0} ON {1}(name) where join_yr between 2010 and 2012 USING {2}".format(
                    idx2, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")
                self.query = "explain select name from {0} where join_yr between 2010 and 2012 and " \
                             "name is not missing".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['filter_covers'] == {
                    'cover (((`default`.`join_yr`) between 2010 and 2012))': True,
                    'cover (((`default`.`join_yr`) <= 2012))': True, 'cover ((2010 <= (`default`.`join_yr`)))': True})
                self.query = "select name from {0} where join_yr between 2010 and 2012 and name is not missing".format(
                    query_bucket)
                actual_result1 = self.run_cbq_query()
                self.query = "explain select name from {0} where join_yr >= 2010 and join_yr <=2012 and" \
                             " name is not null".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['filter_covers'] == {
                    'cover (((`default`.`join_yr`) between 2010 and 2012))': True,
                    'cover (((`default`.`join_yr`) <= 2012))': True, 'cover ((2010 <= (`default`.`join_yr`)))': True})
                self.query = "select name from {0} where join_yr >= 2010 and join_yr" \
                             " <=2012 and name is not null".format(query_bucket)
                actual_result2 = self.run_cbq_query()
                diffs = DeepDiff(actual_result1['results'], actual_result2['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                self.query = "EXPLAIN select emp.name " + "FROM {0} emp NEST {0} items ".format(query_bucket,
                                                                                                query_bucket) + \
                             "ON KEYS meta(`emp`).id where ANY j IN emp.join_yr SATISFIES j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Distinct Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + "FROM {0} emp NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN emp.join_yr SATISFIES j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                self.query = "select emp.name FROM {0} emp USE INDEX(`#primary`) " \
                             "NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN emp.join_yr SATISFIES j between 2010 and 2012 end;"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nest_keys_where_between_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY j for j in join_yr end,name,join_yr) " \
                             "USING {2}".format(idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + "FROM {0} emp NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id where ANY j IN emp.join_yr SATISFIES j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + \
                             "FROM {0} emp NEST {0} items ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN emp.department SATISFIES j = 'Support' end;"
                actual_result = self.run_cbq_query()
                self.query = "select emp.name " + \
                             "FROM {0} emp USE INDEX(`#primary`)  NEST {0} items ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN emp.department SATISFIES j = 'Support' end;"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nest_keys_where_less_more_equal(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY j for j in join_yr end) USING {2}".format(
                    idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name, ARRAY item.department FOR item in items end departments " + \
                             "FROM {0} emp NEST {0} items ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN emp.join_yr SATISFIES j <= 2014 and j >= 2012 end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name, ARRAY item.department FOR item in items end departments " + \
                             "FROM {0} emp NEST {0} items ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`emp`).id where ANY j IN emp.join_yr SATISFIES j <= 2014 and j >= 2012 end;"
                actual_result = self.run_cbq_query()
                self.query = "select emp.name, ARRAY item.department FOR item in items end departments " + \
                             "FROM {0} emp USE INDEX(`#primary`)  NEST {0} items ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`emp`).id where ANY j IN emp.join_yr SATISFIES j <= 2014 and j >= 2012 end;"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_sum(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = "indexwitharraysum{0}".format(ind)
                self.query = "CREATE INDEX {0} ON {1}(department, DISTINCT ARRAY round(v.memory + v.RAM) FOR v " \
                             "in VMs END ) where join_yr=2012 USING {2}".format(index_name, query_bucket,
                                                                                self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "EXPLAIN SELECT count(name),department" + \
                                 " FROM {0} where join_yr=2012 AND ANY v IN VMs SATISFIES round(v.memory+v.RAM)<100 " \
                                 "END AND department = 'Engineer'  GROUP BY department".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue(
                        plan['~children'][0]['#operator'] == 'DistinctScan',
                        "Union Scan is not being used")
                    result1 = plan['~children'][0]['scan']['index']
                    self.assertTrue(result1 == index_name)
                    self.query = "SELECT count(name),department" + \
                                 " FROM {0} where join_yr=2012 AND ANY v IN VMs SATISFIES round(v.memory+v.RAM)<100 " \
                                 "END AND department = 'Engineer'  GROUP BY department".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self.query = "SELECT count(name),department" + \
                                 " FROM {0} use index(`#primary`) where join_yr=2012 AND ANY v IN VMs " \
                                 "SATISFIES round(v.memory+v.RAM)<100 END AND department = 'Engineer' " \
                                 "GROUP BY department".format(query_bucket)
                    expected_result = self.run_cbq_query()
                    diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_index_sum_non_leading_key(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = "indexwitharraysum{0}".format(ind)
                self.query = "CREATE INDEX {0} ON {1}(name,join_yr, DISTINCT ARRAY round(v.memory + v.RAM) " \
                             "FOR v in VMs END ) where join_yr=2012 USING {2}".format(index_name, query_bucket,
                                                                                      self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "EXPLAIN SELECT count(name) FROM {0} where join_yr=2012 AND name = " \
                                 "'query-testemployee10317.9004497-0'  GROUP BY name".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue(
                        plan['~children'][0]['#operator'] == 'DistinctScan',
                        "IndexScan is not being used")
                    result1 = plan['~children'][0]['scan']['index']
                    self.assertTrue(result1 == index_name)
                    self.query = "SELECT count(name)  FROM {0} where join_yr=2012 AND name = " \
                                 "'query-testemployee10317.9004497-0' GROUP BY name".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self.query = "SELECT count(name) FROM {0} use index(`#primary`) where join_yr=2012 AND name = " \
                                 "'query-testemployee10317.9004497-0'  GROUP BY name".format(query_bucket)
                    expected_result = self.run_cbq_query()
                    self.assertEqual(actual_result['results'], expected_result['results'])
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_index_substring(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idxsubstr"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY SUBSTR(j.FirstName,8) for j in name end) " \
                             "USING {2}".format(idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name, SUBSTR(name.FirstName, 8) as firstname from {0}  where ANY j IN " \
                             "name SATISFIES substr(j.FirstName,8) != 'employee' end".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_update(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "arrayidx_update"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) " \
                             "FOR i in {2} END) USING {3}".format(idx, query_bucket, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                updated_value = 'new_dept' * 30
                self.query = "EXPLAIN update {0} set name={1} where ANY i IN address SATISFIES  (ANY j IN i " \
                             "SATISFIES j.city='Mumbai' end) END  returning element department".format(query_bucket,
                                                                                                       updated_value)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "update {0} set department={1} where ANY i IN address SATISFIES  (ANY j IN i " \
                             "SATISFIES j.city='Mumbai' end) END  returning element department".format(query_bucket,
                                                                                                       updated_value)
                self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_index_delete(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "arrayidx_delete"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY v FOR v in {2} END) USING {3}".format(
                    idx, query_bucket, "join_yr", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = 'select count(*) as actual from {0} where join_yr=2012'.format(query_bucket)
                self.run_cbq_query()
                self.sleep(5, 'wait for index')
                actual_result = self.run_cbq_query()
                current_docs = actual_result['results'][0]['actual']
                self.query = 'EXPLAIN delete from {0} where any v in join_yr SATISFIES v=2012 end LIMIT 1'.format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                result = plan['~children'][0]['scan']['index']
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used in and query for 2 array indexes")
                self.assertTrue(result == idx)
                self.query = 'delete from {0} where any v in join_yr satisfies v=2012 end LIMIT 1'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            finally:
                self.sleep(5, 'sleep for 5 seconds')
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_create_delete_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in range(self.num_indexes):
                    view_name = "my_index%s" % ind
                    self.query = "CREATE INDEX {0} ON {1}({2}) USING GSI".format(
                        view_name, query_bucket, ','.join(self.FIELDS_TO_INDEX[ind - 1]))
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self.assertTrue(view_name in str(actual_result['results']),
                                    f"The index is returning the wrong index, please check {actual_result}")
                    created_indexes.append(view_name)
                    self.assertTrue(self._is_index_in_list(bucket, view_name), "Index is not in list")
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0}".format(query_bucket, view_name)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, view_name), "Index is in list")

    def test_create_delete_index_with_query(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            view_name = None
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in range(self.num_indexes):
                    view_name = "tuq_index%s" % ind
                    self.query = "CREATE INDEX {0} ON {1}({2})  USING GSI".format(
                        view_name, query_bucket, ','.join(self.FIELDS_TO_INDEX[ind - 1]))
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self.assertTrue(view_name in str(actual_result['results']),
                                    f"The index is returning the wrong index, please check {actual_result}")
                    created_indexes.append(view_name)
                    self.query = "select {0} from {1} where {2} is not null and {3} is not null".format(
                        ','.join(self.FIELDS_TO_INDEX[ind - 1]), query_bucket,
                        self.FIELDS_TO_INDEX[ind - 1][0], self.FIELDS_TO_INDEX[ind - 1][1])
                    actual_result = self.run_cbq_query()
                    self.assertTrue(len(actual_result['results']), self.num_items)
            except Exception as ex:
                content = self.cluster.query_view(self.master, "ddl_{0}".format(view_name), view_name, {"stale": "ok"},
                                                  bucket="default", retry_time=1)
                self.log.info("Generated view has {0} items".format(len(content['rows'])))
                raise ex
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0}".format(query_bucket, view_name)
                    actual_result = self.run_cbq_query()
                self.query = "select {0} from {1} where {2} is not null and {3} is not null".format(
                    ','.join(self.FIELDS_TO_INDEX[ind - 1]), query_bucket,
                    self.FIELDS_TO_INDEX[ind - 1][0], self.FIELDS_TO_INDEX[ind - 1][1])
                actual_result = self.run_cbq_query(query_params={'scan_consistency': 'statement_plus'})
                self.assertTrue(len(actual_result['results']), self.num_items)

    def test_create_delete_index_with_query_loop(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for ind in range(self.num_indexes):
                    idx_name = f"tuq_index1{ind}"
                    self.query = "CREATE INDEX {0} ON {1}(a)  USING GSI".format(idx_name, query_bucket)
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, idx_name)
                    self.assertTrue(idx_name in str(actual_result['results']),
                                    f"The index is returning the wrong index, please check {actual_result}")
                    created_indexes.append(idx_name)
                    idx_name = f"tuq_index2{ind}"
                    self.query = "CREATE INDEX {0} ON {1}(b)  USING GSI".format(idx_name, query_bucket)
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, idx_name)
                    self.assertTrue(idx_name in str(actual_result['results']),
                                    f"The index is returning the wrong index, please check {actual_result}")
                    created_indexes.append(idx_name)
                    self.query = "CREATE INDEX `a1s_idx_search_legacy` ON {0}(`_class`,`typeRef`," \
                                 "(`fields`.`accountNumber`),`createdDate`) USING GSI".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, "a1s_idx_search_legacy")
                    self.assertTrue(idx_name in str(actual_result['results']),
                                    f"The index is returning the wrong index, please check {actual_result}")
                    created_indexes.append('a1s_idx_search_legacy')
                    self.query = "create index `idx_class_name` on {0}(`_class`,`name`) USING GSI ".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, "idx_class_name")
                    self.assertTrue("idx_class_name" in str(actual_result['results']),
                                    f"The index is returning the wrong index, please check {actual_result}")
                    created_indexes.append("idx_class_name")

                    self.query = "create index `idx_class` on {0}(`_class`) USING GSI".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, "idx_class")
                    self.assertTrue("idx_class" in str(actual_result['results']),
                                    f"The index is returning the wrong index, please check {actual_result}")
                    created_indexes.append("idx_class")

                    for i in range(0, 100):
                        self.query = "select * from {0} where b is not missing and a is not missing ".format(
                            query_bucket)
                        self.run_cbq_query()
                        self.query = "delete from {0} where a is not missing and b is not missing".format(query_bucket)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("{1}", {{"_class":"com.comcast.esp' \
                                     '.unifiednotes.entity.TypeDefinition", "name":"MemoBillingNote",' \
                                     ' "c":"pq"}})'.format(query_bucket, i + 1)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("{1}", {{"_class":"com.comcast.esp.' \
                                     'unifiednotes.entity.UNote", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f",' \
                                     ' "fields.accountNumber":"8771300052225673", "createdDate":' \
                                     '1456790401005}})'.format(query_bucket, i + 2)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("{1}", {{"_class":"com.comcast.esp.' \
                                     'unifiednotes.entity.UNote", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f", ' \
                                     '"fields.accountNumber":"8771300052225673"})'.format(query_bucket, i + 3)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("{1}", {{"_class":"com.comcast.esp.' \
                                     'unifiednotes.entity.UNote", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f", ' \
                                     '"fields.accountNumber":"8771300052225673"}})'.format(query_bucket, i + 4)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("{1}", {{"_class":"com.comcast.esp.' \
                                     'unifiednotes.entity.UNote", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f", ' \
                                     '"fields.accountNumber":"8771300052225673","createdDate":' \
                                     '1456790401004}})'.format(query_bucket, i + 5)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("{1}", {{"_class":"com.comcast.esp.' \
                                     'unifiednotes.entity.UNote", "typeRef":"dd833af2-405e-11e5-a151-feff819cdc9f",' \
                                     ' "fields.accountNumber":"8771300052225673","createdDate":' \
                                     '1456790401003}})'.format(query_bucket, i + 6)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("{1}", {{"_class":"com.comcast.esp.' \
                                     'unifiednotes.entity.TypeDefinition", "typeRef":' \
                                     '"dd833af2-405e-11e5-a151-feff819cdc9f", "fields.accountNumber":' \
                                     '"8771300052225673","createdDate":1456790401002}})'.format(query_bucket, i + 7)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("{1}", {{"_class":"com.comcast.esp.' \
                                     'unifiednotes.entity.TypeDefinition", "typeRef":' \
                                     '"dd833af2-405e-11e5-a151-feff819cdc9f", "fields.accountNumber":' \
                                     '"8771300052225673","createdDate":1456790401001}})'.format(query_bucket, i + 8)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("{1}", {{"_class":"com.comcast.esp.' \
                                     'unifiednotes.entity.TypeDefinition", "typeRef":' \
                                     '"dd833af2-405e-11e5-a151-feff819cdc9f", "fields.accountNumber":' \
                                     '"8771300052225673","createdDate":1456790401001}})'.format(query_bucket, i + 9)
                        self.run_cbq_query()
                        self.query = 'explain select * from {0} where _class="com.comcast.esp.unifiednotes.entity.' \
                                     'TypeDefinition" and name="MemoBillingNote" limit 1'.format(query_bucket)
                        self.run_cbq_query()
                        self.query = 'EXPLAIN SELECT * FROM {0} WHERE _class = "com.comcast.esp.unifiednotes.entity.' \
                                     'UNote" AND typeRef = "dd833af2-405e-11e5-a151-feff819cdc9f" AND createdDate ' \
                                     'BETWEEN 1456790401000 AND 1490897416000 AND fields.accountNumber = ' \
                                     '"8771300052225673" '.format(query_bucket)
                        self.run_cbq_query()
                        self.query = 'delete from {0} where b = 1'.format(query_bucket)
                        self.run_cbq_query()
                        self.query = 'select * from {0} where a = "m"'.format(query_bucket)
                        self.run_cbq_query()

                        self.query = 'explain select * from {0} where _class="com.comcast.esp.unifiednotes.entity.' \
                                     'TypeDefinition" order by acs'.format(query_bucket)
                        self.run_cbq_query()
                        self.query = 'upsert into {0}(KEY, VALUE) VALUES ("1", {{"a":"m", "b":1, "c":"pq"}})'.format(
                            query_bucket)
                        self.run_cbq_query()
                        self.query = 'select * from {0} where a = "m"'.format(query_bucket)
                        self.run_cbq_query()
                        self.query = 'upsert into {0}(KEY, VALUE) VALUES ("1", {{"a":"k", "b":2, "c":"pq"}})'.format(
                            query_bucket)
                        self.run_cbq_query()
                        self.query = 'upsert into {0}(KEY, VALUE) VALUES ("1", {{"a":"k", "b":1, "c":"pq"}})'.format(
                            query_bucket)
                        self.run_cbq_query()
                        self.query = 'select * from {0} where a = "k"'.format(query_bucket)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("3", {{"a":"k", "b":2, "c":"pq"}})'.format(
                            query_bucket)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("4", {{"a":"k", "b":1, "c":"pq"}})'.format(
                            query_bucket)
                        self.run_cbq_query()

                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("5", {{"a":"k", "b":2, "c":"pq"}})'.format(
                            query_bucket)
                        self.run_cbq_query()
                        self.query = 'select * from {0} where b = 2'.format(query_bucket)
                        self.run_cbq_query()
                        self.query = 'delete from {0} where b = 2'.format(query_bucket)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("5", {{"a":"k", "b":1, "c":"pq"}})'.format(
                            query_bucket)
                        self.run_cbq_query()
                        self.query = 'insert into {0}(KEY, VALUE) VALUES ("6", {{"a":"k", "b":2, "c":"pq"}})'.format(
                            query_bucket)
                        self.run_cbq_query()
                        self.query = 'select * from {0} where a = "k"'.format(query_bucket)
                        self.run_cbq_query()
                        self.query = 'delete from {0} where b = 1'.format(query_bucket)
                        self.run_cbq_query()
                        self.query = 'select * from {0} where a = "k" and b = 1'.format(query_bucket)
                        self.run_cbq_query()
                        self.sleep(5, "sleep for kv operations")
            except Exception as ex:
                for view_name in created_indexes:
                    content = self.cluster.query_view(self.master, "ddl_{0}".format(view_name), view_name,
                                                      {"stale": "ok"},
                                                      bucket="default", retry_time=1)
                    self.log.info("Generated view has {0} items".format(len(content['rows'])))
                    raise ex
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0}".format(query_bucket, view_name)
                    actual_result = self.run_cbq_query()
                self.query = "select {0} from {1} where {2} is not null and {3} is not null".format(
                    ','.join(self.FIELDS_TO_INDEX[ind - 1]), query_bucket,
                    self.FIELDS_TO_INDEX[ind - 1][0], self.FIELDS_TO_INDEX[ind - 1][1])
                actual_result = self.run_cbq_query(query_params={'scan_consistency': 'statement_plus'})
                self.assertTrue(len(actual_result['results']), self.num_items)

    def test_explain_query_count(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            index_name = "my_index_child"
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                self.query = "CREATE INDEX {0} ON {1}(VMs, name)  USING {2}".format(index_name, query_bucket,
                                                                                    self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM {0} where VMs is not null union SELECT count(name) ' \
                             'FROM {0} where name is not null'.format(query_bucket)
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan['~children'][0]["~children"][0]['~children'][0]["index"] == index_name,
                                "Index should be {0}, but is: {0}".format(index_name, plan))
            finally:
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_query_group_by(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            index_name = "my_index_child"
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                self.query = "CREATE INDEX {0} ON {1}(VMs, join_yr)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(*) FROM {0} where VMs is not null GROUP BY VMs, join_yr'.format(
                    query_bucket)
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be {0}, but is: {0}".format(index_name, plan))
            finally:
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_query_meta(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            index_name = "my_index_meta"
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                self.query = "CREATE INDEX {0} ON {1}(meta().id, name)  USING {2}".format(index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT name FROM {0} WHERE meta({0}).id is not null and name is not null'.format(
                    query_bucket)
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be {0}, but is: {1}".format(index_name, plan))
            finally:
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                self.run_cbq_query()

    def test_covering_meta(self):
        self.fail_if_no_buckets()
        query_bucket = self.get_collection_name('default')
        self.query = 'CREATE INDEX `iy` ON {0}((distinct (array (`v`.`os`) for `v` in `VMs` end)))'.format(query_bucket)
        self.run_cbq_query()
        self.query = 'EXPLAIN SELECT meta().id from {0} WHERE ANY v IN VMs SATISFIES v.os = "ubuntu" END'.format(
            query_bucket)
        res = self.run_cbq_query()
        plan = self.ExplainPlanHelper(res)
        self.assertTrue('cover ((meta(`default`).`id`))' in str(plan['~children']))

    def test_covering_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in range(self.num_indexes):
                    index_name = "coveringindex{0}".format(ind)
                    self.query = "CREATE INDEX {0} ON {1}(name, join_day)  USING {2}".format(index_name, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT name, join_day FROM {0} where name = 'employee-9'".format(query_bucket)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                        self.query = "EXPLAIN SELECT * FROM {0} where name = 'employee-9'".format(query_bucket)
                        res = self.run_cbq_query()
                        plan = self.ExplainPlanHelper(res)
                        self.assertTrue(plan["~children"][0]['index'] == index_name, "correct index is not used")
                    self.query = "SELECT name, join_day FROM {0} where name = 'employee-9'".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    expected_result = [{"name": doc["name"], "join_day": doc["join_day"]}
                                       for doc in self.full_list
                                       if doc['name'] == 'employee-9']
                    expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
                    self._verify_results(actual_result['results'], expected_result)
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
                self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
                self.run_cbq_query()
                self.sleep(15, 'wait for index')
                self.query = "SELECT name, join_day FROM {0} where name = 'employee-9'".format(query_bucket)
                result = self.run_cbq_query()
                actual_result1 = sorted(actual_result['results'], key=(lambda x: x['name']))
                result1 = sorted(result['results'], key=(lambda x: x['name']))

                # self.assertEqual(sorted(actual_result['results']), sorted(result['results']))
                self.assertEqual(actual_result1, result1)

                self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
                self.run_cbq_query()

    def test_index_like(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in range(self.num_indexes):
                    index_name = f"index{ind}"
                    self.query = "CREATE INDEX {0} ON {1}(name)  USING {2}".format(index_name, query_bucket,
                                                                                   self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT * FROM {0} where name ".format(query_bucket) + "like 'xyz%'"
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]['index'] == index_name, "correct index is not used")
                    self.assertTrue(plan["~children"][0]['spans'][0]["range"][0]["high"] == "\"xy{\"")
                    self.assertTrue(plan["~children"][0]['spans'][0]["range"][0]["low"] == "\"xyz\"")
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_covering_partial_index(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in range(self.num_indexes):
                    index_name = "coveringindexwithwhere{0}".format(ind)
                    self.query = "CREATE INDEX {0} ON {1}(email, VMs, join_day) where join_day > 10 USING {2}".format(
                        index_name, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain select email,VMs[0].RAM from {0} where email ".format(query_bucket) + \
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "select email,join_day from {0} where email ".format(query_bucket) + \
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    actual_result = self.run_cbq_query()
                    expected_result = [{"join_day": doc["join_day"], "email": doc["email"]}
                                       for doc in self.full_list
                                       if re.match(r'.*@.*\..*', doc['email']) and
                                       doc['join_day'] > 10 and
                                       len([vm for vm in doc["VMs"]
                                            if vm["RAM"] > 5]) > 0]
                    expected_result = sorted(expected_result, key=lambda doc: (doc["join_day"]))
                    self._verify_results(actual_result['results'], expected_result)
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
                    self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
                    self.run_cbq_query()
                    self.sleep(15, 'wait for index')
                    self.query = "select email,join_day from {0} where email ".format(query_bucket) + \
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    result = self.run_cbq_query()
                    diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
                    self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
                    self.run_cbq_query()

    def test_covering_orderby_limit(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in range(self.num_indexes):
                    index_name = "coveringindexwithlimit{0}".format(ind)
                    self.query = "CREATE INDEX {0} ON {1}(skills[0], join_yr, VMs[0].os,name) " \
                                 "where join_yr =2010 USING {2}".format(index_name, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain select  name,skills[0] as skills  from {0} where skills[0]='skill2010' and " \
                                 "join_yr=2010 and ( VMs[0].os IN ['ubuntu','windows','linux'] OR VMs[0].os IN " \
                                 "['ubuntu','windows','linux'] ) order by _id asc LIMIT 10 " \
                                 "OFFSET 0;".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue(plan["~children"][0]["~children"][0]["#operator"] == "IndexScan3")
                    self.assertTrue(plan["~children"][0]["~children"][0]["index"] == index_name)
                    self.query = "select name,skills[0] as skills from {0} where skills[0]='skill2010' and " \
                                 "join_yr=2010 and VMs[0].os IN ['ubuntu','windows','linux','linux'] order by name " \
                                 "LIMIT 15 OFFSET 0;".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    expected_result = [{"name": doc["name"], "skills": doc["skills"][0]}
                                       for doc in self.full_list
                                       if doc['join_yr'] == 2010 and
                                       doc['skills'][0] == 'skill2010' and
                                       len([vm for vm in doc["VMs"]
                                            if
                                            vm["os"] == 'ubuntu' or vm["os"] == 'windows' or vm["os"] == "linux"]) > 0]
                    expected_result = sorted(expected_result, key=lambda doc: (doc["name"]))[:15]
                    self.max_verify = 15
                    self._verify_results(actual_result['results'], expected_result)

            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_covering_groupby(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for ind in range(self.num_indexes):
                    index_name = "coveringindexwithgroupby{0}".format(ind)
                    self.query = "CREATE INDEX {0} ON {1}(join_yr,name) where join_yr >2009 USING {2}".format(
                        index_name, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT count(*),join_yr,name FROM {0} where join_yr > 2009 GROUP BY " \
                                 "join_yr,name ORDER BY name;".format(query_bucket)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT count(*),join_yr FROM {0}  where join_yr > 2009 GROUP BY join_yr,name " \
                                 "ORDER BY name;".format(query_bucket)
                    actual_result = self.run_cbq_query()
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
                self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
                self.run_cbq_query()
                self.sleep(15, 'wait for index')
                self.query = "SELECT count(*),join_yr FROM {0}  where join_yr > 2009 GROUP BY join_yr,name " \
                             "ORDER BY name;".format(query_bucket)
                result = self.run_cbq_query()
                # self.assertEqual(sorted(actual_result['results']), sorted(result['results']))
                diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
                self.run_cbq_query()

    def test_covering_groupby_having(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for ind in range(self.num_indexes):
                    index_name = f"coveringindexwithgroupbyhaving{ind}"
                    self.query = "CREATE INDEX {0} ON {1}(job_title,join_mo,test_rate) USING {2}".format(
                        index_name, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain SELECT join_mo, SUM(test_rate) as rate FROM {0} ".format(query_bucket) + \
                                 "as employees WHERE job_title='Engineer' GROUP BY join_mo " + \
                                 "HAVING SUM(employees.test_rate) > 0 and " + \
                                 "SUM(test_rate) < 100000"
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT join_mo, SUM(test_rate) as rate FROM {0} ".format(query_bucket) + \
                                 "as employees WHERE job_title='Engineer' GROUP BY join_mo " + \
                                 "HAVING SUM(employees.test_rate) > 0 and " + \
                                 "SUM(test_rate) < 100000"
                    actual_result = self.run_cbq_query()
                    actual_result = [{"join_mo": doc["join_mo"], "rate": round(doc["rate"])} for doc in
                                     actual_result['results']]
                    actual_result = sorted(actual_result, key=lambda doc: (doc['join_mo']))
                    tmp_groups = {doc['join_mo'] for doc in self.full_list}
                    expected_result = [{"join_mo": group,
                                        "rate": round(math.fsum([doc['test_rate']
                                                                 for doc in self.full_list
                                                                 if doc['join_mo'] == group and
                                                                 doc['job_title'] == 'Engineer']))}
                                       for group in tmp_groups
                                       if math.fsum([doc['test_rate']
                                                     for doc in self.full_list
                                                     if doc['join_mo'] == group and
                                                     doc['job_title'] == 'Engineer']) > 0 and
                                       math.fsum([doc['test_rate']
                                                  for doc in self.full_list
                                                  if doc['join_mo'] == group and
                                                  doc['job_title'] == 'Engineer']) < 100000]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_covering_groupby_letting(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for ind in range(self.num_indexes):
                    index_name = f"coveringindexwithgroupbyletting{ind}"
                    self.query = "CREATE INDEX {0} ON {1}(join_mo,test_rate) USING {2}".format(
                        index_name, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain SELECT join_mo, sum_test from {0} WHERE join_mo>7 group by join_mo " \
                                 "letting sum_test = sum(test_rate) ".format(query_bucket)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT join_mo, sum_test from {0} WHERE join_mo>7 group by join_mo " \
                                 "letting sum_test = sum(test_rate)".format(query_bucket)
                    actual_list = self.run_cbq_query()
                    actual_result = actual_list['results']
                    tmp_groups = {doc['join_mo'] for doc in self.full_list if doc['join_mo'] > 7}
                    expected_result = [{"join_mo": group,
                                        "sum_test": sum([x["test_rate"] for x in self.full_list
                                                         if x["join_mo"] == group])}
                                       for group in tmp_groups]
                    # expected_result = sorted(expected_result)
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_covering_index_hints_select_explain(self):
        self.fail_if_no_buckets()
        index_name_prefix_list = ['hint' + str(uuid.uuid4())[:4], 'covering_hint' + str(uuid.uuid4())[:4]]
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in index_name_prefix_list:
                for attr in ['join_day', 'join_mo']:
                    ind_name = '{0}_{1}'.format(ind, attr)
                    self.query = "CREATE INDEX {0} ON {1}({2})  USING {3}".format(ind_name,
                                                                                  query_bucket, attr, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, ind_name)
                    created_indexes.append('{0}'.format(ind_name))
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for ind in created_indexes:
                    if "covering" in ind:
                        if "join_day" in ind:
                            self.query = 'EXPLAIN SELECT join_day FROM {0}  USE INDEX({1} using {2}) WHERE join_day>2'.format(
                                query_bucket, ind, self.index_type)
                            self.check_explain_covering_index(ind)
                        elif "join_mo" in ind:
                            self.query = 'EXPLAIN SELECT join_mo FROM {0}  USE INDEX({1} using {2}) WHERE join_mo>3'.format(
                                query_bucket, ind, self.index_type)
                            self.check_explain_covering_index(ind)
                    else:
                        self.query = 'EXPLAIN SELECT name,join_day, join_mo FROM {0}  USE INDEX({1} using {2}) ' \
                                     'WHERE join_day>2 AND join_mo>3'.format(query_bucket, ind, self.index_type)
                        res = self.run_cbq_query()
                        plan = self.ExplainPlanHelper(res)
                        self.assertTrue(plan["~children"][0]["index"] == ind,
                                        "Index should be {0}, but is: {1}".format(ind, plan["~children"][0]["index"]))
                        self.query = 'SELECT name,join_day, join_mo FROM {0}  USE INDEX({1} using {2}) WHERE ' \
                                     'join_day>2 AND join_mo>3'.format(query_bucket, ind, self.index_type)
                        res = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "SELECT name,join_day, join_mo FROM {0} WHERE join_day>2 AND join_mo>3".format(query_bucket)
            result = self.run_cbq_query()
            diffs = DeepDiff(result['results'], res['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def async_monitor_index(self, bucket, index_name=None):
        monitor_index_task = self.cluster.async_monitor_index(
            server=self.n1ql_server, bucket=bucket,
            n1ql_helper=self.n1ql_helper,
            index_name=index_name)
        return monitor_index_task

    ##############################################################################################
    #   SCALAR FN
    ##############################################################################################
    def test_ceil_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwithceil{ind}"
                self.query = "CREATE INDEX {0} ON {1}(name,test_rate)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
            self.query = "explain select name,ceil(test_rate) as rate from {0} where name LIKE '{1}' and " \
                         "ceil(test_rate) > 5".format(query_bucket, "employee")
            if self.covering_index:
                self.check_explain_covering_index(index_name)
            self.query = "select test_rate from {0} where name = 'employee-9' and ceil(test_rate) > 5".format(
                query_bucket)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['test_rate']))
            expected_result = [{"test_rate": doc['test_rate']} for doc in self.full_list
                               if doc['name'] == 'employee-9' and
                               math.ceil(doc['test_rate']) > 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['test_rate']))
            self._verify_results(actual_result, expected_result)
            for index_name in set(created_indexes):
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                self.run_cbq_query()

    def test_floor_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwithfloor{ind}"
                self.query = "CREATE INDEX {0} ON {1}(name,test_rate)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                tasks = []
                for index_name in created_indexes:
                    try:
                        tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                        for task in tasks:
                            task.result()
                    except Exception as ex:
                        self.log.info(ex)

                self.query = "explain select name,floor(test_rate) from {0} where name LIKE '{1}' and " \
                             "floor(test_rate) > 5".format(query_bucket, "employee")
                if self.covering_index:
                    self.check_explain_covering_index(index_name)

                self.query = "select test_rate from {0} where name = 'employee-9' and floor(test_rate) > 5".format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'], key=lambda doc: (doc['test_rate']))
                expected_result = [{"test_rate": doc['test_rate']} for doc in self.full_list
                                   if doc['name'] == 'employee-9' and
                                   math.floor(doc['test_rate']) > 5]
                expected_result = sorted(expected_result, key=lambda doc: (doc['test_rate']))
                self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_greatest_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwithgreatest{ind}"
                self.query = "CREATE INDEX {0} ON {1}(name,skills[0], skills[1])  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain select GREATEST(skills[0], skills[1]) as SKILL from {0} where name " \
                                 "LIKE '{1}' and skills[0]='skill2010'".format(query_bucket, "employee")
                if self.covering_index:
                    self.check_explain_covering_index(index_name)

                self.query = "select GREATEST(skills[0], skills[1]) as SKILL from {0} where name = 'employee-9' " \
                             "and skills[0]='skill2010'".format(query_bucket)
                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'],
                                       key=lambda doc: (doc['SKILL']))

                expected_result = [{"SKILL": (doc['skills'][0], doc['skills'][1])[doc['skills'][0] < doc['skills'][1]]}
                                   for doc in self.full_list
                                   if doc['skills'][0] == 'skill2010' and doc['name'] == 'employee-9']
                expected_result = sorted(expected_result, key=lambda doc: (doc['SKILL']))
                self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_least_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwithleast{ind}"
                self.query = "CREATE INDEX {0} ON {1}(name,skills[0], skills[1])  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain select LEAST(skills[0], skills[1]) as SKILL from {0} where name " \
                                 "LIKE '{1}' and skills[0]='skill2010'".format(query_bucket, "employee")
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "select LEAST(skills[0], skills[1]) as SKILL from {0} where name = 'employee-9' " \
                                 "and skills[0]='skill2010'".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    actual_result = sorted(actual_result['results'],
                                           key=lambda doc: (doc['SKILL']))

                    expected_result = [{"SKILL": (doc['skills'][0],
                                                  doc['skills'][1])[doc['skills'][0] > doc['skills'][1]]}
                                       for doc in self.full_list
                                       if doc['skills'][0] == 'skill2010' and doc['name'] == 'employee-9']
                    expected_result = sorted(expected_result, key=lambda doc: (doc['SKILL']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    ##############################################################################################
    #   AGGR FN
    ##############################################################################################

    def test_min_covering_index(self):
        created_indexes = []
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwithmin{ind}"
                self.query = "CREATE INDEX {0} ON {1}(job_title,join_mo,test_rate)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
                index_name2 = f"coveringindexwithcontains{ind}"
                self.query = "CREATE INDEX {0} ON {1}(test_rate)  USING {2}".format(index_name2, query_bucket,
                                                                                    self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name2)

        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = "explain SELECT join_mo, MIN(test_rate) as rate FROM {0} ".format(query_bucket) + \
                             "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                             "HAVING MIN(employees.test_rate) > 0 and " + \
                             "SUM(test_rate) < 100000"
                if self.covering_index:
                    self.check_explain_covering_index(created_indexes[0])

                self.query = "explain select MIN(test_rate) from {0} where test_rate is not missing and " \
                             "CONTAINS(test_rate,'z')".format(query_bucket)
                if self.covering_index:
                    self.check_explain_covering_index(created_indexes[1])

                self.query = "select MIN(test_rate) from {0} where test_rate is not missing and " \
                             "CONTAINS(test_rate,'z')".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.assertTrue(str(actual_result['results'][0]) == "{'$1': None}",
                                "actual value=" + str(actual_result['results'][0]))

                self.query = "explain select count(test_rate) from {0} where test_rate is not missing and " \
                             "CONTAINS(test_rate,'z')".format(query_bucket)
                if self.covering_index:
                    self.check_explain_covering_index(created_indexes[1])

                self.query = "select count(test_rate) as count from {0} where test_rate is not missing and " \
                             "CONTAINS(test_rate,'z')".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'][0]['count'] == 0)

                self.query = "SELECT join_mo, MIN(test_rate) as rate FROM {0} ".format(query_bucket) + \
                             "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                             "HAVING MIN(employees.test_rate) > 0 and " + \
                             "SUM(test_rate) < 100000"

                actual_result = self.run_cbq_query()
                actual_result = sorted(actual_result['results'], key=lambda doc: (doc['join_mo']))
                tmp_groups = {doc['join_mo'] for doc in self.full_list}
                expected_result = [{"join_mo": group,
                                    "rate": min([doc['test_rate']
                                                 for doc in self.full_list
                                                 if doc['join_mo'] == group and
                                                 doc['job_title'] == 'Sales'])}
                                   for group in tmp_groups
                                   if min([doc['test_rate']
                                           for doc in self.full_list
                                           if doc['join_mo'] == group and
                                           doc['job_title'] == 'Sales']) > 0 and
                                   math.fsum([doc['test_rate']
                                              for doc in self.full_list
                                              if doc['join_mo'] == group and
                                              doc['job_title'] == 'Sales']) < 100000]
                expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
                self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_max_pushdown(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = 'CREATE INDEX idx ON {0}(test_rate DESC,join_mO DESC)'.format(query_bucket)
                self.run_cbq_query()
                created_indexes.append("idx")
                self.query = 'explain SELECT MAX(test_rate) from {0} WHERE test_rate > 10'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['index'] == "idx")
                self.query = 'SELECT MAX(test_rate) from {0} WHERE test_rate > 10'.format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = 'SELECT MAX(test_rate) from {0} use index(`#primary`) WHERE test_rate > 10'.format(
                    query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(actual_result['results'] == expected_result['results'])
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_max_covering_index(self):
        created_indexes = []
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwithmax{ind}"
                self.query = "CREATE INDEX {0} ON {1}(job_title,join_mo,test_rate)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT join_mo, MAX(test_rate) as rate FROM {0} ".format(query_bucket) + \
                                 "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                                 "HAVING MAX(employees.test_rate) > 0 and " + \
                                 "SUM(test_rate) < 100000"
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT join_mo, MAX(test_rate) as rate FROM {0} ".format(query_bucket) + \
                                 "as employees WHERE job_title='Sales' GROUP BY join_mo " + \
                                 "HAVING MAX(employees.test_rate) > 0 and " + \
                                 "SUM(test_rate) < 100000"
                    actual_result = self.run_cbq_query()
                    actual_result = sorted(actual_result['results'], key=lambda doc: (doc['join_mo']))
                    tmp_groups = {doc['join_mo'] for doc in self.full_list}
                    expected_result = [{"join_mo": group,
                                        "rate": max([doc['test_rate']
                                                     for doc in self.full_list
                                                     if doc['join_mo'] == group and
                                                     doc['job_title'] == 'Sales'])}
                                       for group in tmp_groups
                                       if max([doc['test_rate']
                                               for doc in self.full_list
                                               if doc['join_mo'] == group and
                                               doc['job_title'] == 'Sales']) > 0 and
                                       math.fsum([doc['test_rate']
                                                  for doc in self.full_list
                                                  if doc['join_mo'] == group and
                                                  doc['job_title'] == 'Sales']) < 100000]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_agg_distinct_covering_index(self):
        created_indexes = []
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = "coveringindexwitharraydistinct{0}".format(ind)
                self.query = "CREATE INDEX {0} on {1}(join_mo,job_title,test_rate,name) USING {2}".format(index_name,query_bucket,self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_agg(DISTINCT name) as names" + \
                                 " FROM {0}  WHERE join_mo=7 GROUP BY job_title".format(query_bucket)

                    if self.covering_index:
                        self.check_explain_covering_index(index_name)

                    self.query = "SELECT job_title, array_agg(DISTINCT name) as names" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)

                    actual_list = self.run_cbq_query()
                    actual_result = self.sort_nested_list(actual_list['results'])
                    actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

                    tmp_groups = {doc['job_title'] for doc in self.full_list if doc['join_mo'] == 7}
                    expected_list = [{"job_title": group,
                                      "names": {x["name"] for x in self.full_list
                                                if x["job_title"] == group}}
                                     for group in tmp_groups]
                    expected_result = self.sort_nested_list(expected_list)
                    expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_length_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharraylength{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,name)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_length(array_agg(DISTINCT name)) as num_names" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)

                    if self.covering_index:
                        self.check_explain_covering_index(index_name)

                    self.query = "SELECT job_title, array_length(array_agg(DISTINCT name)) as num_names" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)

                    actual_result = self.run_cbq_query()
                    actual_result = sorted(actual_result['results'], key=lambda doc: (doc['job_title']))

                    tmp_groups = {doc['job_title'] for doc in self.full_list if doc['join_mo'] == 7}
                    expected_result = [{"job_title": group,
                                        "num_names": len({x["name"] for x in self.full_list
                                                          if x["job_title"] == group})}
                                       for group in tmp_groups]

                    expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_append_covering_index(self):
        created_indexes = []
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharrayappend{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,name) USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title," + \
                                 " array_append(array_agg(DISTINCT name), 'new_name') as names" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)

                    self.query = "SELECT job_title," + \
                                 " array_append(array_agg(DISTINCT name), 'new_name') as names" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)

                    actual_list = self.run_cbq_query()
                    actual_result = self.sort_nested_list(actual_list['results'])
                    actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

                    tmp_groups = {doc['job_title'] for doc in self.full_list if doc['join_mo'] == 7}
                    expected_result = [{"job_title": group,
                                        "names": sorted(set([x["name"] for x in self.full_list
                                                             if x["job_title"] == group] + ['new_name']))}
                                       for group in tmp_groups]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_concat_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharrayconcat{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,name,email)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title," + \
                                 " array_concat(array_agg(name), array_agg(email)) as names" + \
                                 " FROM {0} WHERE join_mo=7  GROUP BY job_title".format(query_bucket)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_prepend_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharrayprepend{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,test_rate)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title," + \
                                 " array_prepend(1.2, array_agg(test_rate)) as rates" + \
                                 " FROM {0} WHERE join_mo=7  GROUP BY job_title".format(query_bucket)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_remove_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharrayremove{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,name)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        value = 'employee-1'
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title," + \
                                 " array_remove(array_agg(DISTINCT name), '{0}') as names".format(value) + \
                                 " FROM {0} WHERE join_mo=7  GROUP BY job_title".format(query_bucket)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT job_title," + \
                                 " array_remove(array_agg(DISTINCT name), '{0}') as names".format(value) + \
                                 " FROM {0} WHERE join_mo=7  GROUP BY job_title".format(query_bucket)

                    actual_list = self.run_cbq_query()
                    actual_result = self.sort_nested_list(actual_list['results'])
                    actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

                    tmp_groups = {doc['job_title'] for doc in self.full_list if doc['join_mo'] == 7}
                    expected_result = [{"job_title": group,
                                        "names": sorted({x["name"] for x in self.full_list
                                                         if x["job_title"] == group and x["name"] != value})}
                                       for group in tmp_groups]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_avg_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharrayavg{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,test_rate)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_avg(array_agg(test_rate))" + \
                                 " as rates FROM {0}  WHERE join_mo=7  GROUP BY job_title".format(query_bucket)

                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT job_title, array_avg(array_agg(test_rate))" + \
                                 " as rates FROM {0}  WHERE join_mo=7  GROUP BY job_title".format(query_bucket)
                    actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "SELECT job_title, array_avg(array_agg(test_rate))" + \
                         " as rates FROM {0}  WHERE join_mo=7  GROUP BY job_title".format(query_bucket)
            result = self.run_cbq_query()
            diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def test_array_contains_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharraycontains{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,name)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_contains(array_agg(name), 'employee-1')" + \
                                 " as emp_job FROM {0}  WHERE join_mo=7  GROUP BY job_title".format(query_bucket)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT job_title, array_contains(array_agg(name), 'employee-1')" + \
                                 " as emp_job FROM {0}  WHERE join_mo=7  GROUP BY job_title".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    actual_result = actual_result['results']
                    tmp_groups = {doc['job_title'] for doc in self.full_list if doc['join_mo'] == 7}
                    expected_result = [{"job_title": group,
                                        "emp_job": 'employee-1' in [x["name"] for x in self.full_list
                                                                    if x["job_title"] == group]}
                                       for group in tmp_groups]

                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_count_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharraycount{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,name)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_count(array_agg(name)) as names" + \
                                 " FROM {0} WHERE join_mo=7  GROUP BY job_title".format(query_bucket)

                if self.covering_index:
                    self.check_explain_covering_index(index_name)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_distinct_covering_indexes(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharraydistinct{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,name)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_distinct(array_agg(name)) as names" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT job_title, array_distinct(array_agg(name)) as names" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)
                    actual_list = self.run_cbq_query()
                    actual_result = self.sort_nested_list(actual_list['results'])
                    actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

                    tmp_groups = {doc['job_title'] for doc in self.full_list if doc['join_mo'] == 7}
                    expected_result = [{"job_title": group,
                                        "names": sorted({x["name"] for x in self.full_list
                                                         if x["job_title"] == group})}
                                       for group in tmp_groups]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_max_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharraymax{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,test_rate)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_max(array_agg(test_rate)) as rates" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)

                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT job_title, array_max(array_agg(test_rate)) as rates" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)
                    actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "SELECT job_title, array_max(array_agg(test_rate)) as rates" + \
                         " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)
            result = self.run_cbq_query()
            diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def test_array_sum_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharraysum{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,test_rate)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, round(array_sum(array_agg(test_rate))) as rates" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)

                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT job_title, round(array_sum(array_agg(test_rate))) as rates" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)
                    actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "SELECT job_title, round(array_sum(array_agg(test_rate))) as rates" + \
                         " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)
            result = self.run_cbq_query()
            diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def test_subquery_union_intersect_covering(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexforsubquery{ind}"
                self.query = "CREATE INDEX {0} ON {1}(job_title)  USING {2}".format(index_name, query_bucket,
                                                                                    self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT count(*) cnt, 0 randomValue," \
                                 " job_title from {0}".format(query_bucket) + \
                                 " where job_title is not missing and DATE_DIFF_STR(NOW_STR()," \
                                 "datetime_field1,'day') < 30" + \
                                 " group by job_title union all select count(*) cnt, 0 randomValue," \
                                 " job_title from {0}".format(query_bucket) + \
                                 " where job_title is not missing group by job_title;"
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = " SELECT count(*) cnt, 0 randomValue, job_title from {0}".format(query_bucket) + \
                                 " where job_title is not missing and DATE_DIFF_STR(NOW_STR()," \
                                 "datetime_field1,'day') < 30" + \
                                 " group by job_title union all select count(*) cnt, 0 randomValue," \
                                 " job_title from {0}".format(query_bucket) + \
                                 " where job_title is not missing group by job_title;"
                    actual_result = self.run_cbq_query()
                    self.query = "select * from (SELECT count(*) cnt, 0 randomValue, job_title from {0}".format(
                        query_bucket) + \
                                 " where job_title is not missing and DATE_DIFF_STR(NOW_STR()," \
                                 "datetime_field1,'day') < 30" + \
                                 " group by job_title union all select count(*) cnt, 0 randomValue," \
                                 " job_title from {0}".format(query_bucket) + \
                                 " where job_title is not missing group by job_title) c;"
                    expected_result = self.run_cbq_query()
                    self.assertEqual(len(actual_result['results']), len(expected_result['results']))
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "select * from (SELECT count(*) cnt, 0 randomValue, job_title from {0}".format(query_bucket) + \
                         " where job_title is not missing and DATE_DIFF_STR(NOW_STR(),datetime_field1,'day') < 30" + \
                         " group by job_title union all select count(*) cnt, 0 randomValue, job_title from {0}".format(
                             query_bucket) + \
                         " where job_title is not missing group by job_title) c;"
            result = self.run_cbq_query()
            diffs = DeepDiff(expected_result['results'], result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def test_array_min_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharraymin{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,test_rate)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "explain SELECT job_title, array_min(array_agg(test_rate)) as rates" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)

                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "SELECT job_title, array_min(array_agg(test_rate)) as rates" + \
                                 " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)
                    actual_result = self.run_cbq_query()

            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = self.query = "SELECT job_title, array_min(array_agg(test_rate)) as rates" + \
                                      " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)
            result = self.run_cbq_query()
            # self.assertEqual(sorted(actual_result['results']), sorted(result['results']))
            diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def test_array_put_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharrayput{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,name)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = "explain SELECT job_title, array_put(array_agg(distinct name), 'employee-1') as emp_job" + \
                             " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)

                if self.covering_index:
                    self.check_explain_covering_index(index_name)
                self.query = "SELECT job_title, array_put(array_agg(distinct name), 'employee-1') as emp_job" + \
                             " FROM {0} WHERE join_mo=7  GROUP BY job_title".format(query_bucket)
                actual_list = self.run_cbq_query()
                actual_result = self.sort_nested_list(actual_list['results'])
                actual_result = sorted(actual_result,
                                       key=lambda doc: (doc['job_title']))

                tmp_groups = {doc['job_title'] for doc in self.full_list if doc['join_mo'] == 7}
                expected_result = [{"job_title": group,
                                    "emp_job": sorted({x["name"] for x in self.full_list
                                                       if x["job_title"] == group})}
                                   for group in tmp_groups]
                expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                self._verify_results(actual_result, expected_result)

                self.query = "SELECT job_title, array_put(array_agg(distinct name), 'employee-47') as emp_job" + \
                             " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)

                actual_list = self.run_cbq_query()
                actual_result = self.sort_nested_list(actual_list['results'])
                actual_result = sorted(actual_result,
                                       key=lambda doc: (doc['job_title']))

                expected_result = [{"job_title": group,
                                    "emp_job": sorted(set([x["name"] for x in self.full_list
                                                           if x["job_title"] == group] + ['employee-47']))}
                                   for group in tmp_groups]
                expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
                self._verify_results(actual_result, expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_array_replace_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharrayreplace{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,name)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = "explain SELECT job_title, array_replace(array_agg(name), 'employee-1', 'employee-47') " \
                             "as emp_job FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)

                if self.covering_index:
                    self.check_explain_covering_index(index_name)
                self.query = " SELECT job_title, array_replace(array_agg(name), 'employee-1', 'employee-47') as " \
                             "emp_job FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)
                actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = " SELECT job_title, array_replace(array_agg(name), 'employee-1', 'employee-47') as emp_job" + \
                         " FROM {0} WHERE join_mo=7 GROUP BY job_title".format(query_bucket)
            result = self.run_cbq_query()
            # self.assertEqual(sorted(actual_result['results']), sorted(result['results']))
            diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def test_array_sort_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwitharraysort{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,job_title,test_rate)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = "explain SELECT job_title, array_sort(array_agg(distinct test_rate)) as emp_job" + \
                             " FROM {0} WHERE join_mo=7  GROUP BY job_title".format(query_bucket)

                if self.covering_index:
                    self.check_explain_covering_index(index_name)
                self.query = " SELECT job_title, array_sort(array_agg(distinct test_rate)) as emp_job" + \
                             " FROM {0} WHERE join_mo=7  GROUP BY job_title".format(query_bucket)

                actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = " SELECT job_title, array_sort(array_agg(distinct test_rate)) as emp_job" + \
                         " FROM {0} WHERE join_mo=7  GROUP BY job_title".format(query_bucket)
            result = self.run_cbq_query()
            # self.assertEqual(sorted(actual_result['results']), sorted(result['results']))
            diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def test_poly_length_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwithpolylength{ind}"
                self.query = "CREATE INDEX {0} ON {1}(join_mo,tasks_points,VMs,skills)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                query_fields = ['tasks_points', 'VMs', 'skills']
                for query_field in query_fields:
                    self.query = "explain select length({0}) as custom_num from {1} " \
                                 "WHERE join_mo=7".format(query_field, query_bucket)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)
                    self.query = "select length({0}) as custom_num from {1} WHERE join_mo=7".format(query_field,
                                                                                                    query_bucket)
                    actual_result = self.run_cbq_query()
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "select length({0}) as custom_num from {1} WHERE join_mo=7".format(query_field, query_bucket)
            result = self.run_cbq_query()
            # self.assertEqual(sorted(actual_result['results']), sorted(result['results']))
            diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def test_maximumlimit_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = "coveringindexwithmaximumlimit{0}".format(ind)
                self.query = "CREATE INDEX {0} ON {1}(join_mo,name,mutated,skills,join_day,email,test_rate,join_yr," \
                             "_id,VMs[0].RAM,VMs[1].os,job_title)  USING {2}".format(index_name, query_bucket,
                                                                                     self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                query_fields = ['name', 'mutated', 'skills', 'join_day', 'join_mo', 'email', 'test_rate', 'join_yr',
                                '_id', 'VMs[0].RAM', 'VMs[1].os', 'job_title']
                for query_field in query_fields:
                    self.query = "explain Select length({0}) as custom_num from {1} WHERE join_mo=7".format(
                        query_field, query_bucket)
                    if self.covering_index:
                        self.check_explain_covering_index(index_name)

                    self.query = "select length({0}) as custom_num from {1} WHERE join_mo=7".format(query_field,
                                                                                                    query_bucket)
                    actual_result = self.run_cbq_query()
                    expected_result = [{"custom_num": None} for doc in self.full_list if doc['join_mo'] == 7]
                    self._verify_results(actual_result['results'], expected_result)

                    self.query = "select poly_length({0}) as custom_num from {1} WHERE join_mo=7 ".format(
                        query_field, query_bucket)
                    actual_result = self.run_cbq_query()
                    expected_result = [{"custom_num": len(doc[query_field])}
                                       for doc in self.full_list
                                       if doc['join_mo'] == 7]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['custom_num']))
                    self._verify_results(actual_result['results'], expected_result)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_count_covering_index(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = f"coveringindexwithsubstr{ind}"
                self.query = "CREATE INDEX {0} ON {1}(_id,join_mo)  USING {2}".format(index_name, query_bucket,
                                                                                      self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks = []
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket=bucket.name, index_name=index_name))
                    for task in tasks:
                        task.result()
                except Exception as ex:
                    self.log.info(ex)

        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = "explain select count(_id) from {0} where join_mo is not" \
                             "missing and _id is not null".format(query_bucket)
                if self.covering_index:
                    self.check_explain_covering_index(index_name)

                self.query = "select count(_id) as count from {0} where join_mo is not " \
                             "missing and _id is not null".format(query_bucket)
                actual_result1 = self.run_cbq_query()
                self.assertTrue(actual_result1['results'][0]['count'] == 2016)

                self.query = "select count(_id)  as count from {0} where join_mo is not missing and _id is null".format(
                    query_bucket)

                actual_result2 = self.run_cbq_query()
                self.assertTrue(actual_result2['results'][0]['count'] == 0)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()
            self.query = "CREATE PRIMARY INDEX ON {0}".format(query_bucket)
            actual_result = self.run_cbq_query()
            self.sleep(15, 'wait for index')
            self.query = "select count(_id) as count from {0} where join_mo is not missing and _id is not null".format(
                query_bucket)
            result = self.run_cbq_query()
            # self.assertEqual(sorted(actual_result1['results']), sorted(result['results']))
            diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "select count(_id)  as count from {0} where join_mo is not missing and _id is null".format(
                query_bucket)
            result = self.run_cbq_query()
            # self.assertEqual(sorted(actual_result2['results']), sorted(result['results']))
            diffs = DeepDiff(actual_result2['results'], result['results'], ignore_order=True)
            if diffs:
                self.assertTrue(False, diffs)
            self.query = "DROP PRIMARY INDEX ON {0}".format(query_bucket)
            self.run_cbq_query()

    def test_explain_index_join(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                if self.DGM:
                    self.get_dgm_for_plasma(indexer_nodes=[self.server], memory_quota=400)
                for ind in range(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX {0} ON {1}(name, project)  USING {2}".format(
                        index_name, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT employee.name, new_task.project FROM {0} as employee JOIN {0} as" \
                                 " new_task ON KEYS ['key1']".format(query_bucket)
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == "#primary",
                                    "Index should be {0}, but is: {0}".format(index_name, plan))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_index_subquery(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for ind in range(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX {0} ON {1}(join_day, name)  USING {2}".format(
                        index_name, query_bucket, self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN select task_name, (select sum(test_rate) cn from {0} as d use keys " \
                                 "['query-1'] where join_day>2 and name =='abc') as names from {0}".format(query_bucket)
                    res = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(res)
                    self.assertTrue(plan["~children"][0]["index"] == "#primary",
                                    "Index should be {0}, but is: {0}".format(index_name, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_explain_childs_list_objects(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            index_name = "my_index_child2"
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = "CREATE INDEX {0} ON {1}(distinct array vm.RAM for vm in VMs END)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT VMs[0].RAM FROM {0} '.format(query_bucket) + \
                             'WHERE ANY vm IN VMs SATISFIES vm.RAM > 5 end'
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["scan"]['index'] == index_name,
                                "Index should be {0}, but is: {0}".format(index_name, plan))
            finally:
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_childs_objects(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            index_name = "my_index_obj"
            query_bucket = self.get_collection_name(bucket.name)
            try:
                self.query = "CREATE INDEX {0} ON {1}(tasks_points.task1, join_mo)  USING {2}".format(
                    index_name, query_bucket, self.index_type)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from {0} '.format(query_bucket) + \
                             'WHERE task_points.task1 > 0 and join_mo>7 '
                res = self.run_cbq_query()
                plan = self.ExplainPlanHelper(res)
                self.assertTrue(plan["~children"][0]["index"] == index_name,
                                "Index should be {0}, but is: {0}".format(index_name, res["results"]))
            finally:
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
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
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}(tokens({2})) ".format(idx, query_bucket, '_id') + \
                             " USING {0}".format(self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)

                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select count(1) from {0} WHERE tokens(_id) like '{1}' ".format(
                    query_bucket, 'query-test%')

                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                result1 = plan['~children'][0]['index']
                self.assertTrue(result1 == idx)
                self.query = "select count(1) from {0} WHERE tokens(_id) like '{1}' ".format(query_bucket,
                                                                                             'query-test%')
                actual_result = self.run_cbq_query()
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'IndexScan3',
                    "IndexCountScan is not being used")
                self.query = "explain select a.cnt from (select count(1) from {0} where tokens(_id)" \
                             " is not null) as a".format(query_bucket)
                actual_result2 = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result2)
                self.assertTrue(
                    plan['~children'][0]['#operator'] != 'IndexScan3',
                    "IndexCountScan should not be used in subquery")
                self.query = "select a.cnt from (select count(1) from {0} where tokens(_id) is not null) as a".format(
                    query_bucket)
                actual_result2 = self.run_cbq_query()

                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                    created_indexes.remove(idx)
                    self.query = "select count(1) from {0} use index(`#primary`) WHERE tokens(_id) like '{1}'  ".format(
                        query_bucket, 'query-test%')
                    result = self.run_cbq_query()

                    # self.assertEqual(sorted(actual_result['results']), sorted(result['results']))
                    diffs = DeepDiff(actual_result['results'], result['results'], ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
                    self.query = "select a.cnt from (select count(1) from {0} where _id is not null) as a ".format(
                        query_bucket)
                    result = self.run_cbq_query()
                    # self.assertEqual(sorted(actual_result2['results']), sorted(result['results']))diffs =
                    # DeepDiff(actual_result2['results'], result['results'], ignore_order=True)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_join_unnest_tokens_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX {0} ON {1}( aLL ARRAY x.RAM FOR x within tokens({2}) END,VMs)" \
                             " USING {3}".format(idx4, query_bucket, "VMs", self.index_type)

                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "explain SELECT x from {0} emp1 USE INDEX({1})  UNNEST tokens(emp1.VMs) as x  JOIN {0}" \
                             " task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5;".format(query_bucket, idx4)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))

                self.query = "SELECT x from {0} emp1 USE INDEX({1}) UNNEST tokens(emp1.VMs) as x JOIN {0} task " \
                             "ON KEYS meta(`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;".format(query_bucket, idx4)
                actual_result = self.run_cbq_query()
                self.query = "SELECT x from {0} emp1 USE INDEX(`#primary`)  UNNEST tokens(emp1.VMs) as x  JOIN {0} " \
                             "task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5 ;".format(query_bucket)
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_substring(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idxsubstr"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY SUBSTR(j.FirstName,8) for j in " \
                             "tokens(name) end) USING {2}".format(idx, query_bucket, self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name, SUBSTR(name.FirstName, 8) as firstname from {0}  where ANY j " \
                             "IN tokens(name) SATISFIES substr(j.FirstName,8) != 'employee' end".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_update(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "arrayidx_update"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j.city for j in tokens(i) " \
                             "end) FOR i in tokens({2}) END) USING {3}".format(idx, query_bucket,
                                                                               "address", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                updated_value = 'new_dept' * 30
                self.query = "EXPLAIN update {0} set name={1} where ANY i IN tokens(address) SATISFIES  (ANY j IN " \
                             "tokens(i) SATISFIES j.city='Mumbai' end) END  " \
                             "returning element department".format(query_bucket, updated_value)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "DistinctScan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "update {0} set department={1} where ANY i IN tokens(address) SATISFIES  (ANY j IN i " \
                             "SATISFIES j.city='Mumbai' end) END  returning element " \
                             "department".format(query_bucket, updated_value)
                self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_delete(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "arrayidx_delete"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY v FOR v in tokens({2}) END) USING {3}".format(
                    idx, query_bucket, "join_yr", self.index_type)

                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = 'select count(*) as actual from {0} where tokens(join_yr)=2012'.format(query_bucket)
                self.run_cbq_query()
                self.sleep(5, 'wait for index')
                actual_result = self.run_cbq_query()
                self.query = 'EXPLAIN delete from {0} where any v in tokens(join_yr) SATISFIES v=2012 end ' \
                             'LIMIT 1'.format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                result = plan['~children'][0]['scan']['index']
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used in and query for 2 array indexes")
                self.assertTrue(result == idx)
                self.query = 'delete from {0} where any v in tokens(join_yr) satisfies v=2012 end LIMIT 1'.format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                self.assertEqual(actual_result['status'], 'success', 'Query was not run successfully')

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nesttokens_keys_where_between(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY j for j in tokens(join_yr) end) USING {2}".format(
                    idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name FROM {0} emp NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.join_yr) " \
                             "SATISFIES j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertEqual(plan['~children'][0]['#operator'], 'DistinctScan', "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1, idx)
                self.query = "select emp.name FROM {0} emp NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.join_yr)" \
                             " SATISFIES j between 2010 and 2012 end;"
                actual_result = self.run_cbq_query()
                self.query = "select emp.name FROM {0} emp USE INDEX(`#primary`)  " \
                             "NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id where ANY j IN tokens(emp.join_yr) " \
                             "SATISFIES j between 2010 and 2012 end;"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nesttokens_keys_where_less_more_equal(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY j for j in tokens(join_yr) end) USING {2}".format(
                    idx, query_bucket, self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name, ARRAY item.department FOR item in items end departments " + \
                             "FROM {0} emp NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.join_yr) SATISFIES " \
                             "j <= 2014 and j >= 2012 end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name, ARRAY item.department FOR item in items end departments " + \
                             "FROM {0} emp NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.join_yr) SATISFIES " \
                             "j <= 2014 and j >= 2012 end;"
                actual_result = self.run_cbq_query()
                self.query = "select emp.name, ARRAY item.department FOR item in items end departments " + \
                             "FROM {0} emp USE INDEX(`#primary`)  NEST {0} items ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.join_yr) " \
                             "SATISFIES j <= 2014 and j >= 2012 end;"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_sum(self):
        self.fail_if_no_buckets()
        created_indexes = []
        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            for ind in range(self.num_indexes):
                index_name = "indexwitharraysum{0}".format(ind)
                self.query = "CREATE INDEX {0} ON {1}(department, DISTINCT ARRAY round(v.memory + v.RAM) FOR v in" \
                             " tokens(VMs) END ) where join_yr=2012 USING {2}".format(index_name,
                                                                                      query_bucket, self.index_type)
                self.run_cbq_query()
                created_indexes.append(index_name)

        for bucket in self.buckets:
            query_bucket = self.get_collection_name(bucket.name)
            try:
                for index_name in created_indexes:
                    self.query = "EXPLAIN SELECT count(name),department" + \
                                 " FROM {0} where join_yr=2012 AND ANY v IN tokens(VMs) SATISFIES " \
                                 "round(v.memory+v.RAM)<100 END AND department = 'Engineer'  " \
                                 "GROUP BY department".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    plan = self.ExplainPlanHelper(actual_result)
                    self.assertTrue(
                        plan['~children'][0]['#operator'] == 'DistinctScan',
                        "Union Scan is not being used")
                    result1 = plan['~children'][0]['scan']['index']
                    self.assertTrue(result1 == index_name)
                    self.query = "SELECT count(name),department" + \
                                 " FROM {0} where join_yr=2012 AND ANY v IN tokens(VMs) SATISFIES " \
                                 "round(v.memory+v.RAM)<100 END AND department = 'Engineer'  " \
                                 "GROUP BY department".format(query_bucket)
                    actual_result = self.run_cbq_query()
                    self.query = "SELECT count(name),department" + \
                                 " FROM {0} use index(`#primary`) where join_yr=2012 AND ANY v IN tokens(VMs) " \
                                 "SATISFIES round(v.memory+v.RAM)<100 END AND department = 'Engineer' " \
                                 "GROUP BY department".format(query_bucket)
                    expected_result = self.run_cbq_query()
                    diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                    if diffs:
                        self.assertTrue(False, diffs)
            finally:
                for index_name in set(created_indexes):
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, index_name, self.index_type)
                    self.run_cbq_query()

    def test_nesttokens_keys_where_not_equal(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY j for j in tokens(department) end) " \
                             "USING {2}".format(idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + \
                             "FROM {0} emp NEST {0} VMs ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.department) " \
                             "SATISFIES j != 'Engineer' end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + \
                             "FROM {0} emp NEST {0} VMs ".format(query_bucket) + \
                             "ON KEYS meta(`emp`).id where ANY j IN tokens(emp.department) SATISFIES j = 'Support' end;"
                actual_result = self.run_cbq_query()
                self.query = "select emp.name " + \
                             "FROM {0} emp USE INDEX(`#primary`)   NEST {0} items ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`emp`).id where ANY j IN tokens(emp.department) SATISFIES j = 'Support' end;"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_nest_keys_where(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nestidx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY j for j in tokens(department) end) USING {2}".format(
                    idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select emp.name " + \
                             "FROM {0} emp NEST {0} items ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.department) SATISFIES j = 'Support' end;"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select emp.name " + \
                             "FROM {0} emp NEST {0} items ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`emp`).id  where ANY j IN tokens(emp.department) SATISFIES j = 'Support' end;"
                actual_result = self.run_cbq_query()
                self.query = "select emp.name " + \
                             "FROM {0} emp USE INDEX(`#primary`) NEST {0} items ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`emp`).id where ANY j IN tokens(emp.department) SATISFIES j = 'Support' end;"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_regexp(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "iregex"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY REGEXP_LIKE(v.os,{2}) FOR v IN tokens(VMs) END )" \
                             "  USING {3}".format(idx, query_bucket, "'ub%'", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from {0}  WHERE ANY v IN tokens(VMs) SATISFIES " \
                             "REGEXP_LIKE(v.os,{1}) = 1 END  ".format(query_bucket, "'ub%'") + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select * from {0} use index(`#primary`)  WHERE ANY v IN tokens(VMs) " \
                             "SATISFIES REGEXP_LIKE(v.os,{1}) = 1  END  ".format(query_bucket, "'ub%'") + \
                             "order BY tokens(name) limit 10"
                self.query = "select * from {0}  WHERE ANY v IN tokens(VMs) SATISFIES " \
                             "REGEXP_LIKE(v.os,{1}) = 1 END  ".format(query_bucket, "'ub%'") + \
                             "order BY tokens(name) limit 10"
                actual_result = self.run_cbq_query()
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_left_outer_join(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "outer_join"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) " \
                             "FOR i in tokens({2}) END) USING {3}".format(idx, query_bucket, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                idx2 = "outer_join_all"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( All array j.city for j in i end) FOR i in " \
                             "tokens({2}) END) USING {3}".format(idx2, query_bucket, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee use index ({1}) left JOIN {0} as new_project_full ".format(
                                 query_bucket, idx) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES" \
                             "  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "EXPLAIN SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee use index ({1}) left JOIN {0} as new_project_full ".format(
                                 query_bucket, idx2) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES" \
                             "  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)
                self.query = "SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee use index ({1}) left JOIN {0} as new_project_full ".format(
                                 query_bucket, idx) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES" \
                             "  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result1 = (actual_result['results'])
                self.query = "SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee use index ({1}) left JOIN {0} as new_project_full ".format(
                                 query_bucket, idx2) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES " \
                             " (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result2 = (actual_result['results'])
                self.query = "SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee use index (`#primary`) left JOIN {0} as new_project_full ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  " \
                             "(ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                actual_result3 = (actual_result['results'])
                self.assertTrue(actual_result1 == actual_result3)
                self.assertTrue(actual_result2 == actual_result3)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_with_inner_joins(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_inner_join"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) " \
                             "FOR i in tokens({2}) END) USING {3}".format(
                    idx, query_bucket, "address", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT new_project_full.department new_project " + \
                             "FROM {0} as employee  JOIN {0} as new_project_full ".format(query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  " \
                             "(ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_partial_tokens_all(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}( all array i FOR i in tokens({2}) END) WHERE " \
                             "(department = 'Support')  USING {3}".format(
                    idx, query_bucket, "hobbies.hobby", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from {0} WHERE department = 'Support' and (ANY i IN " \
                             "tokens({0}.hobbies.hobby) SATISFIES  i = 'art' END) ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from {0} WHERE department = 'Support' and ANY i IN tokens({0}.hobbies." \
                             "hobby) SATISFIES i = 'art' END ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from {0} WHERE department = 'Support' and ANY i IN tokens({0}.hobbies.hobby" \
                             ") SATISFIES i = 'art' END ".format(query_bucket) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_tokens_greatest(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX {0} ON {1}(department, DISTINCT ARRAY GREATEST(v.RAM,100)  FOR v IN " \
                             "tokens(VMs) END )  USING {2}".format(idx, query_bucket, self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from {0} WHERE department = 'Support' and ANY v IN " \
                             "tokens(VMs) SATISFIES GREATEST(v.RAM,100) END ".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(
                    plan['~children'][0]['#operator'] == 'DistinctScan',
                    "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from {0} WHERE department = 'Support' and ANY v IN tokens(VMs) " \
                             "SATISFIES GREATEST(v.RAM,100) END ".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} USE index(`#primary`) WHERE department = 'Support' and ANY v " \
                             "IN tokens(VMs) SATISFIES GREATEST(v.RAM,100) END ".format(query_bucket)
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_array_partial_tokens_distinct(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}( distinct array i FOR i in tokens({2}) END) WHERE" \
                             " (department = 'Support')  USING {3}".format(idx, query_bucket,
                                                                           "hobbies.hobby", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select * from {0} WHERE department = 'Support' and (ANY i IN tokens({0}." \
                             "hobbies.hobby) SATISFIES  i = 'art' END) ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                self.query = "select name from {0} WHERE department = 'Support' and ANY i IN tokens({0}.hobbies." \
                             "hobby) SATISFIES i = 'art' END ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from {0} use index (`#primary`) WHERE department = 'Support' and ANY i " \
                             "IN tokens({0}.hobbies.hobby) SATISFIES i = 'art' END ".format(query_bucket) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_array_tokens_in_all(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY ( all array j for j in i.dance end) " \
                             "FOR i in tokens({2}) END) USING {3}".format(idx, query_bucket,
                                                                          "hobbies.hobby", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from {0} WHERE ANY i IN tokens({0}.hobbies.hobby) SATISFIES " \
                             "(ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'".format(
                    query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "select name from {0} WHERE ANY i IN tokens({0}.hobbies.hobby) SATISFIES  (ANY j " \
                             "IN i.dance SATISFIES j='contemporary' end) END and department='Support'".format(
                    query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from {0} USE INDEX(`#primary`) WHERE ANY i IN tokens({0}.hobbies.hobby)" \
                             " SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and " \
                             "department='Support'".format(query_bucket) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_token_within(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY i FOR i within tokens({2}) END) USING {3}".format(
                    idx, query_bucket, "hobbies", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY i FOR i within tokens({2}) END) USING {3}".format(
                    idx2, query_bucket, "hobbies", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                self.query = "EXPLAIN select name from {0} use index({1}) WHERE ANY i within tokens({0}.hobbies) " \
                             "SATISFIES i = 'bhangra' END ".format(query_bucket, idx2) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)

                self.query = "EXPLAIN select name from {0} use index({1}) WHERE ANY i within tokens({0}.hobbies)" \
                             " SATISFIES i = 'bhangra' END ".format(query_bucket, idx) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "select name from {0} use index({0}) WHERE ANY i within tokens({1}.hobbies) " \
                             "SATISFIES i = 'bhangra' END ".format(query_bucket, idx) + \
                             "order BY name limit 10"
                actual_result1 = self.run_cbq_query()

                self.query = "select name from {0} use index({1}) WHERE ANY i within tokens({0}.hobbies) " \
                             "SATISFIES i = 'bhangra' END ".format(query_bucket, idx2) + \
                             "order BY name limit 10"
                actual_result2 = self.run_cbq_query()

                self.query = "select name from {0} use index(`#primary`) WHERE ANY i within tokens({0}.hobbies) " \
                             "SATISFIES i = 'bhangra' END ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result3 = self.run_cbq_query()
                diffs = DeepDiff(actual_result2['results'], actual_result3['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_array_tokens_in_distinct(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j for j in i.dance end) " \
                             "FOR i in tokens({2}) END) USING {3}".format(idx, query_bucket,
                                                                          "hobbies.hobby", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from {0} WHERE ANY i IN tokens({0}.hobbies.hobby) SATISFIES  (" \
                             "ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'".format(
                    query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} WHERE ANY i IN tokens({0}.hobbies.hobby) SATISFIES  (" \
                             "ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'".format(
                    query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} USE INDEX(`#primary`) WHERE ANY i IN tokens({0}.hobbies.hobby) " \
                             "SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and " \
                             "department='Support'".format(
                    query_bucket) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_token(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j.region1 for j in i.Marketing" \
                             " end) FOR i in tokens({2}) END) USING {3}".format(
                    idx, query_bucket, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "nested_idx_attr2"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY ( All array j.region1 for j in i.Marketing end)" \
                             " FOR i in tokens({2}) END) USING {3}".format(
                    idx2, query_bucket, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.assertTrue(self._is_index_in_list(bucket, idx2), "Index is not in list")

                idx3 = "nested_idx_attr3"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY ( DISTINCT array j.region1 for j in i.Marketing end)" \
                             " FOR i in tokens({2}) END) USING {3}".format(
                    idx3, query_bucket, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self.assertTrue(idx3 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")

                idx4 = "nested_idx_attr4"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( ALL array j.region1 for j in i.Marketing end" \
                             ") FOR i in tokens({2}) END) USING {3}".format(
                    idx4, query_bucket, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx4)
                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "EXPLAIN select name from {0} USE INDEX({1}) WHERE ANY i IN tokens({0}.tasks) SATISFIES" \
                             "  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END ".format(
                    query_bucket, idx) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "EXPLAIN select name from {0} USE INDEX({1}) WHERE ANY i IN tokens({0}.tasks) SATISFIES" \
                             "  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END ".format(
                    query_bucket, idx2) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "DistinctScan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx2)

                self.query = "EXPLAIN select name from {0} USE INDEX({1}) WHERE ANY i IN tokens({0}.tasks) SATISFIES" \
                             "  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END ".format(
                    query_bucket, idx3) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx3)

                self.query = "EXPLAIN select name from {0} USE INDEX({1}) WHERE ANY i IN tokens({0}.tasks) SATISFIES" \
                             "  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END ".format(
                    query_bucket, idx4) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'DistinctScan',
                                "Union Scan is not being used")
                result1 = plan['~children'][0]['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)

                self.query = "select name from {0} use index ({1}) WHERE ANY i IN tokens({0}.tasks) SATISFIES" \
                             "  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END ".format(
                    query_bucket, idx) + \
                             "order BY name limit 10"
                actual_result1 = self.run_cbq_query()
                self.query = "select name from {0} use index ({1}) WHERE ANY i IN tokens({0}.tasks) SATISFIES" \
                             "  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END ".format(
                    query_bucket, idx2) + \
                             "order BY name limit 10"
                actual_result2 = self.run_cbq_query()

                self.query = "select name from {0} use index ({1}) WHERE ANY i IN tokens({0}.tasks) SATISFIES" \
                             "  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END ".format(
                    query_bucket, idx3) + \
                             "order BY name limit 10"
                actual_result3 = self.run_cbq_query()

                self.query = "select name from {0} use index ({1}) WHERE ANY i IN tokens({0}.tasks) SATISFIES" \
                             "  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END ".format(
                    query_bucket, idx4) + \
                             "order BY name limit 10"
                actual_result4 = self.run_cbq_query()

                self.query = "select name from {0} use index (`#primary`) WHERE ANY i IN tokens({0}.tasks) SATISFIES" \
                             "  (ANY j IN i.Marketing SATISFIES j.region1='South' end) END ".format(
                    query_bucket) + \
                             "order BY name limit 10"
                actual_result5 = self.run_cbq_query()

                diffs = DeepDiff(actual_result1['results'], actual_result2['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

                diffs = DeepDiff(actual_result2['results'], actual_result3['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

                diffs = DeepDiff(actual_result3['results'], actual_result4['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

                diffs = DeepDiff(actual_result4['results'], actual_result5['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_unnest_multilevel_attribute_tokens(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx_attr_nest"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY ( DISTINCT array j.region1 for j in tokens(" \
                             "i.Marketing) end) FOR i in {2} END) USING {3}".format(
                    idx, query_bucket, "tasks", self.index_type)

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "explain SELECT emp.name FROM {0} emp  UNNEST emp.tasks as i UNNEST tokens(" \
                             "i.Marketing) as j where j.region1 = 'South'".format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['#operator'] == 'DistinctScan',
                                "Union Scan is not being used")
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx)

                self.query = "select name from {0} WHERE ANY i IN {0}.tasks SATISFIES  (ANY j IN tokens(" \
                             "i.Marketing) SATISFIES j.region1='South' end) END ".format(
                    query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} USE INDEX(`#primary`) WHERE ANY i IN {0}.tasks SATISFIES  (ANY j " \
                             "IN tokens(i.Marketing) SATISFIES j.region1='South' end) END ".format(
                    query_bucket) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_tokens_join_unnest_alias_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX {0} ON {1}( aLL ARRAY x.RAM FOR x within tokens({2}) END,VMs) " \
                             "USING {3}".format(idx4, query_bucket, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "explain SELECT x from {0} emp1  UNNEST tokens(emp1.VMs) as x  JOIN {0} task ON KEYS " \
                             "meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5   ;".format(query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "SELECT x from {0} emp1 UNNEST tokens(emp1.VMs) as x JOIN {0} task ON KEYS meta(" \
                             "`emp1`).id  where  x.RAM > 1 and x.RAM < 5 ;".format(query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "SELECT x from {0} emp1 USE INDEX(`#primary`)  UNNEST tokens(emp1.VMs) as x  JOIN {0} " \
                             "task ON KEYS meta(`emp1`).id where  x.RAM > 1 and x.RAM < 5 ;".format(query_bucket)
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_tokens_all(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idxjoin_yr"
                self.query = 'CREATE INDEX {0} ON {1}( ALL ARRAY v FOR v in tokens({2},{{"case":"lower"}}) END)' \
                             ' USING {3}'.format(idx, query_bucket, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = 'CREATE INDEX {0} ON {1}( All ARRAY x.RAM FOR x in tokens({2},{{"names":true}}) END) ' \
                             'USING {3}'.format(idx2, query_bucket, "VMs", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)

                self.query = 'EXPLAIN select name from {0} where any v in tokens({0}.join_yr,{{"case":"lower"}}) ' \
                             'satisfies v = 2016 END '.format(query_bucket) + \
                             'AND (ANY x IN tokens({0}.VMs,{{"names":true}}) SATISFIES x.RAM between' \
                             ' 1 and 5 END) '.format(query_bucket) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan' or
                                plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan')
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx2 or result1 == idx)
                self.assertTrue(result2 == idx or result2 == idx2)
                self.query = 'select name from {0} where any v in tokens({0}.join_yr,{{"case":"lower"}}) ' \
                             'satisfies v = 2016 END '.format(query_bucket) + \
                             'AND (ANY x IN tokens({0}.VMs,{{"names":true}}) SATISFIES x.RAM between 1 ' \
                             'and 5 END) '.format(query_bucket) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'
                actual_result = self.run_cbq_query()

                self.query = 'select name from {0} use index (`#primary`) where any v in tokens({0}.join_yr,' \
                             '{{"case":"lower"}}) satisfies v = 2016 END '.format(query_bucket) + \
                             'AND (ANY x IN tokens({0}.VMs,{{"names":true}}) SATISFIES x.RAM ' \
                             'between 1 and 5 END) '.format(query_bucket) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                created_indexes.remove(idx)
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx2, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx2), "Index is in list")
                created_indexes.remove(idx2)

                idx3 = "idxjoin_yr2"
                self.query = 'CREATE INDEX {0} ON {1}( all ARRAY v FOR v within tokens({2},{{"names":true,' \
                             '"case":"lower"}}) END) USING {3}'.format(idx3, query_bucket, "join_yr", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self.assertTrue(idx3 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = 'CREATE INDEX {0} ON {1}( aLL ARRAY x.RAM FOR x within tokens({2},{{"names":true,' \
                             '"case":"lower"}}) END) USING {3}'.format(idx4, query_bucket, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = 'EXPLAIN select name from {0} where any v in tokens({0}.join_yr,{{"case":"lower",' \
                             '"names":true}}) satisfies v = 2016 END '.format(query_bucket) + \
                             'AND (ANY x IN tokens({0}.VMs,{{"names":true,"case":"lower"}}) SATISFIES x.RAM' \
                             ' between 1 and 5 END) '.format(query_bucket) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'
                actual_result_within = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result_within)
                self.assertTrue(plan['~children'][0]['~children'][0]['#operator'] == 'IntersectScan' or
                                plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan')
                if plan['~children'][0]['~children'][0]['#operator'] == 'UnionScan':
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][0]['scans'][1]['scan']['index']
                else:
                    result1 = plan['~children'][0]['~children'][0]['scans'][0]['scan']['index']
                    result2 = plan['~children'][0]['~children'][0]['scans'][1]['scan']['index']
                self.assertTrue(result1 == idx3 or result1 == idx4)
                self.assertTrue(result2 == idx4 or result2 == idx3)

                idx5 = "idxtokens1"
                self.query = 'CREATE INDEX {0} ON {1}( all ARRAY v FOR v within tokens({2})  END) USING {3}'.format(

                    idx5, query_bucket, "join_yr", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx5)
                self.assertTrue(idx5 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx5)
                self.assertTrue(self._is_index_in_list(bucket, idx5), "Index is not in list")

                idx8 = "idxtokens2"
                self.query = 'CREATE INDEX {0} ON {1}( all ARRAY v FOR v within tokens({2},{{"names":true,"case":' \
                             '"lower","specials":false}})  END) USING {3}'.format(idx8, query_bucket,
                                                                                  "join_yr", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx8)
                self.assertTrue(idx8 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx8)
                self.assertTrue(self._is_index_in_list(bucket, idx8), "Index is not in list")

                idx6 = "idxVM4"
                self.query = 'CREATE INDEX {0} ON {1}( aLL ARRAY x.RAM FOR x within tokens({2},' \
                             '{{"specials":false}}) END) USING {3}'.format(idx6, query_bucket, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx6)
                self.assertTrue(idx6 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx6)

                self.assertTrue(self._is_index_in_list(bucket, idx6), "Index is not in list")

                idx7 = "idxVM3"
                self.query = 'CREATE INDEX {0} ON {1}( aLL ARRAY x.RAM FOR x within tokens({2},{{"names":true,' \
                             '"case":"lower"}}) END) USING {3}'.format(idx7, query_bucket, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx7)
                self.assertTrue(idx7 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx7)

                self.assertTrue(self._is_index_in_list(bucket, idx7), "Index is not in list")

                self.query = 'EXPLAIN select name from {0} where any v within tokens({0}.join_yr,{{"case"}}) ' \
                             'satisfies v = 2016 END '.format(query_bucket) + \
                             'AND (ANY x within tokens({0}.VMs,{{"specials"}}) SATISFIES' \
                             ' x.RAM between 1 and 5 END) '.format(query_bucket) + \
                             'AND  NOT (department = "Manager") ORDER BY name limit 10'

                self.query = 'select name from {0} where any v in tokens({0}.join_yr,{{"case":"lower",' \
                             '"names":true,"specials":false}}) satisfies v = 2016 END '.format(
                    query_bucket) + \
                             'AND (ANY x within tokens({0}.VMs,{{"specials":false}}) SATISFIES x.RAM between' \
                             ' 1 and 5  END ) '.format(query_bucket) + \
                             'AND  NOT (department = "Manager") order by name limit 10'
                actual_result_within = self.run_cbq_query()
                self.query = "select name from {0} use index (`#primary`) where any v in tokens({0}.join_yr)" \
                             " satisfies v = 2016 END ".format(query_bucket) + \
                             "AND (ANY x within tokens({0}.VMs) SATISFIES x.RAM between 1 and 5  END ) ".format(
                                 query_bucket) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                expected_result = self.run_cbq_query()

                diffs = DeepDiff(actual_result_within['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX `{0}`.`{1}` USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_attr_tokens(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx_attr"
                self.query = "CREATE INDEX {0} ON {1}( ALL ARRAY ( all array j for j in i.dance end) FOR i in " \
                             "tokens({2}) END,hobbies.hobby,department,name) USING {3}".format(idx, query_bucket,
                                                                                               "hobbies.hobby",
                                                                                               self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select name from {0} WHERE ANY i IN tokens({0}.hobbies.hobby) SATISFIES  " \
                             "(ANY j IN i.dance SATISFIES j='contemporary' end) END and department='Support'".format(
                    query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select name from {0} WHERE ANY i IN tokens({0}.hobbies.hobby) SATISFIES  (ANY j " \
                             "IN i.dance SATISFIES j='contemporary' end) END and department='Support'".format(
                    query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from {0} USE INDEX(`#primary`) WHERE ANY i IN tokens({0}.hobbies.hobby) " \
                             "SATISFIES  (ANY j IN i.dance SATISFIES j='contemporary' end) END and" \
                             " department='Support'".format(query_bucket) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_tokens_partial_index_distinct_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = 'CREATE INDEX {0} ON {1}( distinct array i FOR i in tokens({2},{{"names":true}}) END' \
                             ',hobbies.hobby,name) WHERE (department = "Support")  USING {3}'.format(
                    idx, query_bucket, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = 'EXPLAIN select name from {0} WHERE department = "Support" and (ANY i IN tokens({0}.' \
                             'hobbies.hobby,{{"names":true}}) SATISFIES  i = "art" END) '.format(query_bucket) + \
                             'order BY name limit 10'
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = 'select name from {0} WHERE department = "Support" and ( ANY i IN tokens({0}.' \
                             'hobbies.hobby,{{"names":true}} ) SATISFIES i = "art" END) '.format(query_bucket) + \
                             "order BY name limit 10"

                actual_result = self.run_cbq_query()

                self.query = 'select name from {0} use index (`#primary`) WHERE department = "Support" and (ANY i ' \
                             'IN tokens({0}.hobbies.hobby,{{"names":true}}) SATISFIES i = "art" END) '.format(
                    query_bucket) + \
                             'order BY name limit 10'
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_tokens_distinct_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idx"
                self.query = "CREATE INDEX {0} ON {1}( distinct array i FOR i in tokens({2}) END,hobbies.hobby,name)" \
                             " WHERE (department = 'Support')  USING {3}".format(
                    idx, query_bucket, "hobbies.hobby", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from {0} WHERE department = 'Support' and (ANY i IN " \
                             "tokens({0}.hobbies.hobby) SATISFIES  i = 'art' END) ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select name from {0} WHERE department = 'Support' and ANY i IN tokens({0}.hobbies." \
                             "hobby) SATISFIES i = 'art' END ".format(query_bucket) + \
                             "order BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from {0} use index (`#primary`) WHERE department = 'Support' and ANY i " \
                             "IN tokens({0}.hobbies.hobby) SATISFIES i = 'art' END ".format(
                    query_bucket) + \
                             "order BY name limit 10"
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_tokens_with_inner_joins_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_inner_join"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT array j.city for j in i end) " \
                             "FOR i in tokens({2}) END,address,department) USING {3}".format(
                    idx, query_bucket, "address", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN SELECT employee.department new_project " + \
                             "FROM {0} as employee  JOIN {0} as new_project_full ".format(query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES  " \
                             "(ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "SELECT employee.department new_project " + \
                             "FROM {0} as employee  JOIN {0} as new_project_full ".format(query_bucket) + \
                             "ON KEYS meta(`employee`).id WHERE ANY i IN tokens(employee.address) SATISFIES " \
                             " (ANY j IN i SATISFIES j.city='Delhi' end) END "
                actual_result = self.run_cbq_query()
                self.query = "SELECT employee.department new_project " + \
                             "FROM {0} as employee use index (`#primary`)  JOIN {0} as new_project_full ".format(
                                 query_bucket) + \
                             "ON KEYS meta(`employee`).id  WHERE ANY i IN tokens(employee.address) SATISFIES  (ANY j IN i SATISFIES j.city='Delhi' end) END "
                expected_result = self.run_cbq_query()
                expected_result = expected_result['results']
                self.assertTrue(sorted(actual_result['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result, key=(lambda x: x['name'])))

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_tokens_regexp_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "iregex"
                self.query = " CREATE INDEX {0} ON {1}( DISTINCT ARRAY REGEXP_LIKE(v.os,{2})  FOR v IN tokens(VMs) " \
                             "END,VMs )  USING {3}".format(idx, query_bucket, "'ub%'", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select VMs from {0}  WHERE ANY v IN tokens(VMs) SATISFIES " \
                             "REGEXP_LIKE(v.os,{1}) = 1 END  limit 10".format(
                    query_bucket, "'ub%'")
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)

                self.assertTrue("covers" in str(plan))

                self.query = "select VMs from {0}  WHERE ANY v IN tokens(VMs) SATISFIES REGEXP_LIKE(v.os,{0}) " \
                             "= 1 END  ".format(query_bucket, "'ub%'") + "limit 10"
                actual_result = self.run_cbq_query()
                self.query = "select VMs from {0} use index(`#primary`)  WHERE ANY v IN tokens(VMs) " \
                             "SATISFIES REGEXP_LIKE(v.os,{1}) = 1  END limit 10 ".format(
                    query_bucket, "'ub%'")

                expected_result = self.run_cbq_query()
                expected_result = expected_result['results']

                self.assertTrue(sorted(actual_result['results'], key=(lambda x: x['VMs'])) == \
                                sorted(expected_result, key=(lambda x: x['VMs'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_tokens_greatest_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "igreatest"
                self.query = " CREATE INDEX {0} ON {1}( department, DISTINCT ARRAY GREATEST(v.RAM,100)  " \
                             "FOR v IN tokens(VMs) END ,VMs,name )  USING {2}".format(
                    idx, query_bucket, self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = " EXPLAIN select name from {0} WHERE department = 'Support' and ANY v IN " \
                             "tokens(VMs) SATISFIES GREATEST(v.RAM,100) END ".format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.query = "select name from {0} WHERE department = 'Support' and ANY v IN tokens(VMs) " \
                             "SATISFIES GREATEST(v.RAM,100) END ".format(
                    query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select name from {0} USE index(`#primary`) WHERE department = 'Support' and ANY v IN " \
                             "tokens(VMs) SATISFIES GREATEST(v.RAM,100) END ".format(
                    query_bucket)
                expected_result = self.run_cbq_query()
                diffs = DeepDiff(actual_result['results'], expected_result['results'], ignore_order=True)
                if diffs:
                    self.assertTrue(False, diffs)

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_unnest_tokens_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "unnest_idx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( ALL array j for j in i end) FOR i in " \
                             "tokens({2}) END,department,tasks,name) USING {3}".format(
                    idx, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select {0}.name from {0}  UNNEST TOKENS(tasks) AS i UNNEST i AS j " \
                             "WHERE j = 'Search' ".format(query_bucket) + \
                             "AND (ANY x IN TOKENS(default.tasks) SATISFIES x = 'Sales' END) " + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(
                                 query_bucket)

                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))

                self.assertTrue(
                    str(plan['~children'][0]['~children'][0]['scan']['covers'][2]) == "cover ((`default`.`tasks`))")
                self.assertTrue(
                    str(plan['~children'][0]['~children'][0]['scan']['covers'][3]) == "cover ((`default`.`name`))")

                self.query = "select {0}.name from {0} UNNEST TOKENS(tasks) AS i UNNEST i AS j" \
                             " WHERE j = 'Search'  ".format(query_bucket) + \
                             "AND (ANY x IN tokens({0}.tasks) SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(
                                 query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select {0}.name from {0} use index (`#primary`)  UNNEST TOKENS(tasks) AS i " \
                             "UNNEST i as j WHERE j = 'Search'  ".format(query_bucket) + \
                             "AND (ANY x IN tokens({0}.tasks) SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(
                                 query_bucket)
                expected_result = self.run_cbq_query()

                self.assertEqual(sorted(actual_result['results'], key=(lambda x: x[bucket.name]['name'])),
                                 sorted(expected_result['results'], key=(lambda x: x[bucket.name]['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_nested_token_covering(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "nested_idx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( DISTINCT ARRAY j for j in i end) " \
                             "FOR i in tokens({2}) END,tasks,department,name)".format(
                    idx, query_bucket, "tasks")

                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")
                self.query = "EXPLAIN select name from {0} WHERE ANY i IN tokens(tasks) SATISFIES  " \
                             "(ANY j IN i SATISFIES j='Search' end) END ".format(
                    query_bucket) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx)
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][1]) == (
                    "cover ((`{0}`.`tasks`))".format(query_bucket)))
                self.query = "select name from {0} WHERE ANY i IN tokens(tasks) SATISFIES  (ANY j IN i " \
                             "SATISFIES j='Search' end) END ".format(
                    query_bucket) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from {0} use index(`#primary`) WHERE ANY i IN tokens(tasks) SATISFIES  " \
                             "(ANY j IN i SATISFIES j='Search' end) END ".format(
                    query_bucket) + \
                             "AND  NOT (department = 'Manager') order BY name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_token_nonarrayindex(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX {0} ON {1}( ARRAY v FOR v in tokens({2}) END) USING {3}".format(
                    idx, query_bucket, "join_yr", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = "CREATE INDEX {0} ON {1}( ARRAY x.RAM FOR x in tokens({2}) END) USING {3}".format(
                    idx2, query_bucket, "VMs", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                self.assertTrue(idx2 in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx2)
                self.query = "select name from {0} where any v in tokens({0}.join_yr) " \
                             "satisfies v = 2016 END ".format(query_bucket) + \
                             "AND ( ANY x IN tokens({0}.VMs) SATISFIES x.RAM between 1 and 5 END ) ".format(
                                 query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")
                created_indexes.remove(idx)
                self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx2, self.index_type)
                drop_result = self.run_cbq_query()
                self._verify_results(drop_result['results'], [])
                self.assertFalse(self._is_index_in_list(bucket, idx2), "Index is in list")
                created_indexes.remove(idx2)

                idx3 = "idxjoin_yr2"
                self.query = "CREATE INDEX {0} ON {1}(  ARRAY v FOR v within tokens({2}) END) USING {3}".format(
                    idx3, query_bucket, "join_yr", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx3)
                self.assertTrue(idx3 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx3)
                self.assertTrue(self._is_index_in_list(bucket, idx3), "Index is not in list")
                idx4 = "idxVM2"
                self.query = "CREATE INDEX {0} ON {1}(  ARRAY x.RAM FOR x within tokens({2}) END) USING {3}".format(
                    idx4, query_bucket, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")
                self.query = "select name from {0} USE INDEX({0}) where ".format(query_bucket, idx4) + \
                             "(ANY x within tokens({0}.VMs) SATISFIES x.RAM between 1 and 5  END ) ".format(
                                 query_bucket) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                actual_result_within = self.run_cbq_query()
                self.query = "select name from {0} USE INDEX(`#primary`) where ".format(
                    query_bucket) + \
                             "(ANY x within tokens({0}.VMs) SATISFIES x.RAM between 1 and 5  END ) ".format(
                                 query_bucket) + \
                             "AND  NOT (department = 'Manager') order by name limit 10"
                expected_result = self.run_cbq_query()
                self.assertTrue(sorted(actual_result_within['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_unnest_token(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "unnest_idx"
                self.query = "CREATE INDEX {0} ON {1}( DISTINCT ARRAY ( ALL array j for j in i end)" \
                             " FOR i in tokens({2}) END) USING {3}".format(idx, query_bucket, "tasks", self.index_type)
                actual_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                self.assertTrue(idx in str(actual_result['results']), f"The index is returning the wrong index, please check {actual_result}")
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                self.query = "EXPLAIN select {0}.name from {0} UNNEST tokens(tasks) as i UNNEST i as" \
                             " j WHERE j = 'Search' ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(
                                 query_bucket)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                result1 = plan['~children'][0]['~children'][0]['scan']['index']

                self.assertTrue(result1 == idx)
                self.query = "select {0}.name from {0}  UNNEST tokens(tasks) as i UNNEST i as" \
                             " j WHERE j = 'Search'  ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(
                                 query_bucket)
                actual_result = self.run_cbq_query()
                self.query = "select {0}.name from {0} use index (`#primary`)  UNNEST tokens(tasks) as" \
                             " i UNNEST i as j WHERE j = 'Search'  ".format(query_bucket) + \
                             "AND (ANY x IN {0}.tasks SATISFIES x = 'Sales' END) ".format(query_bucket) + \
                             "AND  NOT ({0}.department = 'Manager') order BY {0}.name limit 10".format(
                                 query_bucket)
                expected_result = self.run_cbq_query()

                self.assertTrue(sorted(actual_result['results'], key=(lambda x: x[bucket.name]['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x[bucket.name]['name'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_join_unnest_alias_tokens(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx4 = "idxVM2"
                self.query = "CREATE INDEX {0} ON {1}( aLL ARRAY x.RAM FOR x within tokens({2}) END)" \
                             " USING {3}".format(idx4, query_bucket, "VMs", self.index_type)
                create_result = self.run_cbq_query()
                self._wait_for_index_online(bucket, idx4)
                self.assertTrue(idx4 in str(create_result['results']), f"The index is returning the wrong index, please check {create_result}")
                created_indexes.append(idx4)

                self.assertTrue(self._is_index_in_list(bucket, idx4), "Index is not in list")

                self.query = "explain SELECT x from {0} emp1 USE INDEX({1})  UNNEST tokens(emp1.VMs) as x  JOIN" \
                             " default task ON KEYS meta(`emp1`).id where x.RAM > 1 and" \
                             " x.RAM < 5  ;".format(query_bucket, idx4)
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                result1 = plan['~children'][0]['scan']['index']
                self.assertTrue(result1 == idx4)
                self.query = "SELECT x from {0} emp1 USE INDEX({1}) UNNEST tokens(emp1.VMs) as x JOIN" \
                             " default task ON KEYS meta(`emp1`).id  where  x.RAM > 1 and" \
                             " x.RAM < 5 ;".format(query_bucket, idx4)
                actual_result = self.run_cbq_query()
                self.query = "SELECT x from {0} emp1 USE INDEX(`#primary`)  UNNEST tokens(emp1.VMs) as x" \
                             "  JOIN {0} task ON KEYS meta(`emp1`).id  where  x.RAM > 1 and" \
                             " x.RAM < 5 ;".format(query_bucket)
                expected_result = self.run_cbq_query()
                self.assertTrue(
                    sorted(actual_result['results'], key=(lambda x: x['x'])) == sorted(expected_result['results'],
                                                                                       key=(lambda x: x['x'])))
            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_simple_token(self):
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            created_indexes = []
            query_bucket = self.get_collection_name(bucket.name)
            try:
                idx = "idxjoin_yr"
                self.query = "CREATE INDEX {0} ON {1} (department, DISTINCT (ARRAY lower(to_string(d))" \
                             " for d in tokens({2}) END),join_yr,name)  ".format(idx, query_bucket, "join_yr")
                self.run_cbq_query()
                self._wait_for_index_online(bucket, idx)
                created_indexes.append(idx)
                self.assertTrue(self._is_index_in_list(bucket, idx), "Index is not in list")

                idx2 = "idxVM"
                self.query = "CREATE INDEX {0} ON {1}(`name`,(DISTINCT (array (`x`.`RAM`)" \
                             " for `x` in tokens({2}) end)),VMs)".format(idx2, query_bucket, "VMs")
                self.run_cbq_query()
                self._wait_for_index_online(bucket, idx2)
                created_indexes.append(idx2)

                self.query = "EXPLAIN select name from {0} WHERE ANY d in tokens(join_yr)" \
                             " satisfies lower(to_string(d)) = '2016' END ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['covers'][0]) == (
                    "cover ((`{0}`.`department`))".format(query_bucket)))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx)
                self.query = "select name from {0} WHERE ANY d in tokens(join_yr) satisfies" \
                             " lower(to_string(d)) = '2016' END ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from {0} use index(`#primary`) WHERE ANY d in tokens(join_yr)" \
                             " satisfies lower(to_string(d)) = '2016' END ".format(query_bucket) + \
                             "AND  NOT (department = 'Manager') ORDER BY name limit 10"
                expected_result = self.run_cbq_query()

                self.assertTrue(sorted(actual_result['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))

                self.query = "EXPLAIN select name from {0} where (ANY x within tokens(VMs)" \
                             " SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "and name is not null ORDER BY name limit 10"
                actual_result = self.run_cbq_query()
                plan = self.ExplainPlanHelper(actual_result)
                self.assertTrue("covers" in str(plan))
                self.assertTrue(str(plan['~children'][0]['~children'][0]['scan']['index']) == idx2)
                self.query = "select name from `{0}` where (ANY x within tokens(VMs)" \
                             " SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "and name is not null ORDER BY name limit 10"
                actual_result = self.run_cbq_query()

                self.query = "select name from `{0}` use index(`#primary`) where (ANY x within tokens(VMs)" \
                             " SATISFIES x.RAM between 1 and 5  END ) ".format(query_bucket) + \
                             "and name is not null ORDER BY name limit 10"
                expected_result = self.run_cbq_query()

                self.assertTrue(sorted(actual_result['results'], key=(lambda x: x['name'])) == \
                                sorted(expected_result['results'], key=(lambda x: x['name'])))

            finally:
                for idx in created_indexes:
                    self.query = "DROP INDEX {1} ON {0} USING {2}".format(query_bucket, idx, self.index_type)
                    actual_result = self.run_cbq_query()
                    self.assertFalse(self._is_index_in_list(bucket, idx), "Index is in list")

    def test_MB63593(self):
        create_index = f'CREATE INDEX ix15 ON {self.buckets[0]}( ALL ARRAY r1.status FOR r1 IN a1 END,id) WHERE type = "xx"'
        explain_query = f'EXPLAIN SELECT 1 FROM {self.buckets[0]} AS d UNNEST d.a1 AS r0 UNNEST d.a1 AS r1 WHERE d.type = "xx" AND d.id = 1 AND r1.status = 1 AND r0.sid = 5'
        self.run_cbq_query(create_index)
        explain = self.run_cbq_query(explain_query)
        plan = explain['results'][0]['plan']
        scan = plan['~children'][0]['scan']
        self.log.info(f'scan is: {scan}')
        self.assertTrue('filter' not in scan)
        self.assertEqual(scan['index'], 'ix15')

    def test_include_keyword(self):
        index_name = "def_test"
        query_bucket = self.buckets[0]
        try:
            # Create index with INCLUDE MISSING
            self.query = f"CREATE INDEX {index_name} ON {query_bucket}(`click` INCLUDE MISSING)"
            self.run_cbq_query()

            # Query system:indexes to verify the index
            self.query = f"SELECT * FROM system:indexes WHERE name = '{index_name}'"
            result = self.run_cbq_query()
            self.log.info(f"Query result: {result}")

            # Assert the index_key
            self.assertEqual(result['results'][0]['indexes']['index_key'], ["`click` INCLUDE MISSING"], 
                             f"Expected ['`click` INCLUDE MISSING'], but got {result['results'][0]['indexes']['index_key']}")

        finally:
            # Drop the index
            self.query = f"DROP INDEX {index_name} ON {query_bucket} USING GSI"
            self.run_cbq_query()

    def test_covering_with_upper(self):
        self.fail_if_no_buckets()
        # create collection1 in default bucket
        self.query = "CREATE COLLECTION collection1"
        self.run_cbq_query(query_context='default._default')

        # insert documents into collection1
        self.query = "insert into collection1 (key, value) values ('key::1', {'name':'John'})"
        self.run_cbq_query(query_context='default._default')

        # create indexes on collection1
        self.query = "create index idx_name_1 on collection1(name, upper(name))"
        self.run_cbq_query(query_context='default._default')
        self.query = "create index idx_name_2 on collection1(name)"
        self.run_cbq_query(query_context='default._default')

        # query collection1 with index index_name_1
        self.query = "select upper(name) from collection1 use index (idx_name_1) where name = 'John' group by name"
        result_index_1 = self.run_cbq_query(query_context='default._default')
        self.log.info(f"Query result: {result_index_1}")

        # query collection1 with index index_name_2
        self.query = "select upper(name) from collection1 use index (idx_name_2) where name = 'John' group by name"
        result_index_2 = self.run_cbq_query(query_context='default._default')
        self.log.info(f"Query result: {result_index_2}")

        # assert the result
        self.assertEqual(result_index_1['results'][0]['$1'], 'JOHN', "Result from index index_name_1 is not correct")
        self.assertEqual(result_index_2['results'][0]['$1'], 'JOHN', "Result from index index_name_2 is not correct")

    def test_MB66129(self):
        self.fail_if_no_buckets()
        self.run_cbq_query("CREATE COLLECTION collection1 IF NOT EXISTS", query_context='default._default')
        self.run_cbq_query("CREATE COLLECTION collection2 IF NOT EXISTS", query_context='default._default')

        # create index on collection2
        self.query = "CREATE INDEX idx_name_1 IF NOT EXISTS on collection2(`known_field` INCLUDE MISSING,`fx`,`jf1021067`,`jf1021072_copy`,`known_field2`)"
        self.run_cbq_query(query_context='default._default')

        # run query 1
        self.query = "SELECT * FROM collection2 AS t1 WHERE t1.known_field2 = 0 or NVL(t1.known_field,'') = 'something'"
        self.run_cbq_query(query_context='default._default', query_params={'use_cbo': True})

        # run query 2
        self.query = '''
        SELECT jf1021075_copy AS p0,known_field2 AS p1,known_field2 AS p2,jf1021075_copy AS p3,
            (SELECT COUNT(1) FROM collection1 AS t2 WHERE t1.fx > t2.f1) AS x 
        FROM collection2 AS t1
        WHERE jf1021070_copy NOT LIKE "%_voBRyciaOpmp4FPm%"
        AND (known_field2 = 0 OR jf1021070_copy > "hFE_DVU4bWA9J4yRP")
        AND jf1021075_copy != false
        AND NVL(t1.known_field,"") != "something"
        AND NVL(t1.known_field2,0)
        '''
        self.run_cbq_query(query_context='default._default', query_params={'use_cbo': True})

    # MB-66160
    def test_invalid_use_index(self):
        self.fail_if_no_buckets()
        self.run_cbq_query("DROP INDEX ix1 IF EXISTS ON default")
        self.run_cbq_query("DROP INDEX ix2 IF EXISTS ON default")
        self.run_cbq_query("DROP INDEX ix3 IF EXISTS ON default")
        self.run_cbq_query("DROP INDEX ix4 IF EXISTS ON default")
        self.run_cbq_query("DROP INDEX ix5 IF EXISTS ON default")
        self.run_cbq_query("DROP INDEX ix6 IF EXISTS ON default")
        try:
            # create index on default bucket
            self.run_cbq_query("CREATE INDEX ix1 IF NOT EXISTS ON default(c2 INCLUDE MISSING, c1, c11, c12, c13, c14)")
            self.run_cbq_query("CREATE INDEX ix2 IF NOT EXISTS ON default(c1, c2, DISTINCT ARRAY FLATTEN_KEYS(v.a1, v.a2) FOR v IN arr1 END)")
            self.run_cbq_query("CREATE INDEX ix3 IF NOT EXISTS ON default(c11, c2, DISTINCT ARRAY FLATTEN_KEYS(v.a1, v.a2) FOR v IN arr1 END)")
            self.run_cbq_query("CREATE INDEX ix4 IF NOT EXISTS ON default(c12, c2, DISTINCT ARRAY FLATTEN_KEYS(v.a1, v.a2) FOR v IN arr1 END)")
            self.run_cbq_query("CREATE INDEX ix5 IF NOT EXISTS ON default(c13, c2, DISTINCT ARRAY FLATTEN_KEYS(v.a1, v.a2) FOR v IN arr1 END)")
            self.run_cbq_query("CREATE INDEX ix6 IF NOT EXISTS ON default(c14, c2, DISTINCT ARRAY FLATTEN_KEYS(v.a1, v.a2) FOR v IN arr1 END)")

            # run explain query
            explain_query = '''
            EXPLAIN SELECT f1, f2
            FROM default
            WHERE c2 = 100
            AND (ANY v IN arr1 SATISFIES v.a1 >= 0 END AND
                 ANY v IN arr1 SATISFIES v.a2 <= 100 END)
            AND (c1 = 0 OR c11 = 1 OR c12 = 2 OR c13 = 3 OR c14 = 4)
            '''
            explain_result = self.run_cbq_query(explain_query, query_params={'use_cbo': False})
            self.log.info(f"Explain without hint result: {explain_result}")

            # check ix1 is used in explain result
            self.assertTrue('ix1' in str(explain_result['results'][0]['plan']))

            explain_query_use_index = '''
            EXPLAIN SELECT f1, f2
            FROM default USE INDEX(dummy)
            WHERE c2 = 100
            AND (ANY v IN arr1 SATISFIES v.a1 >= 0 END AND
                 ANY v IN arr1 SATISFIES v.a2 <= 100 END)
            AND (c1 = 0 OR c11 = 1 OR c12 = 2 OR c13 = 3 OR c14 = 4)
            '''
            explain_result_use_index = self.run_cbq_query(explain_query_use_index, query_params={'use_cbo': False})
            self.log.info(f"Explain with hint result: {explain_result_use_index}")

            # check hint is invalid in explain result
            self.assertTrue('Invalid indexes specified: dummy' in str(explain_result_use_index['results'][0]['optimizer_hints']))

            # check ix1 is used in explain result
            self.assertTrue('ix1' in str(explain_result_use_index['results'][0]['plan']))

        finally:
            # drop index
            self.run_cbq_query("DROP INDEX ix1 IF EXISTS ON default")
            self.run_cbq_query("DROP INDEX ix2 IF EXISTS ON default")
            self.run_cbq_query("DROP INDEX ix3 IF EXISTS ON default")
            self.run_cbq_query("DROP INDEX ix4 IF EXISTS ON default")
            self.run_cbq_query("DROP INDEX ix5 IF EXISTS ON default")
            self.run_cbq_query("DROP INDEX ix6 IF EXISTS ON default")

    def test_MB52090(self):
        self.fail_if_no_buckets()
        self.run_cbq_query("CREATE COLLECTION mb52090 IF NOT EXISTS", query_context='default._default')
        self.run_cbq_query('CREATE INDEX ix2 IF NOT EXISTS ON mb52090(c1) WHERE type LIKE "airport%"', query_context='default._default')

        explain_query = '''
        EXPLAIN SELECT 1 FROM mb52090 WHERE c1 = 10 AND type LIKE $atype
        '''
        explain_result = self.run_cbq_query(explain_query, query_context='default._default', query_params={'$atype': '"airport%"'})
        self.log.info(f"Explain result: {explain_result}")

        # check ix2 is used in explain result
        self.assertTrue('ix2' in str(explain_result['results'][0]['plan']))

    def test_explain_plan_time(self):
        """
        MB-64031: Increased plan time for query when complex WHERE clause exists in indexes
        This test creates a new collection in default._default, sets up the indexes and query as in the JIRA,
        and asserts the query plan time is under 4 seconds.
        """
        self.fail_if_no_buckets()
        collection_name = "mb64031"
        query_context = "default._default"
        # Create collection
        self.run_cbq_query(f"CREATE COLLECTION {collection_name} IF NOT EXISTS", query_context=query_context)
        self.sleep(2)
        # Insert a document to ensure the collection is not empty
        self.run_cbq_query(f"INSERT INTO {collection_name} (KEY, VALUE) VALUES ('k1', {{'type':'docs','status':'new','timestamp':'2024-01-01T00:00:00Z','c0':'c0','c1':'c1','c2':'c2','c3':'c3','c4':'c4','c5':'c5','c6':'c60','c7':'c70','c8':'c80','c21':'v21','c22':'v22'}})", query_context=query_context)
        self.run_cbq_query(f"INSERT INTO {collection_name} (KEY, VALUE) VALUES ('k2', {{'type':'others','c30':'v21','c32':'notempty','c33':null,'c41':'v22','c42':4,'c43':1,'c44':1,'c45':100,'c46':1}})", query_context=query_context)
        # Create indexes as in JIRA
        self.run_cbq_query(f"CREATE INDEX ix20 ON {collection_name}(str_to_millis(timestamp),c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10) WHERE type = 'docs' AND status != 'submitted' AND status != 'queued'", query_context=query_context)
        self.run_cbq_query(f"CREATE INDEX ix22 ON {collection_name}(str_to_millis(timestamp),c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10) WHERE type = 'others' AND c9 IS MISSING AND c2 IS NOT NULL AND c2 != '' AND (c9 IS MISSING OR c9 = false)", query_context=query_context)
        self.run_cbq_query(f"CREATE INDEX ixo01 ON {collection_name}(c30, c35, c37) WHERE type = 'others' AND c32 != '' AND (c33 IS MISSING OR c33 != 100)", query_context=query_context)
        self.run_cbq_query(f"CREATE INDEX ixo02 ON {collection_name}(c41, c43, c44, c46) WHERE type = 'others' AND c42 NOT IN [1,2,3] AND c45 != 120", query_context=query_context)
        # Query to EXPLAIN
        explain_query = f"""
        EXPLAIN SELECT t.*
        FROM {collection_name} AS t
           JOIN {collection_name} as r ON t.c21 = r.c30 AND r.type = 'others' AND r.c32 != '' AND (r.c33 IS MISSING OR r.c33 != 100)
           JOIN {collection_name} as s ON t.c22 = s.c41 AND s.type = 'others' AND s.c42 NOT IN [1,2,3] AND s.c45 != 120
        WHERE  t.type = 'docs'
               AND t.status != 'submitted'
               AND t.status != 'queued'
               AND t.c1  = 'c1'
               AND t.c2  = 'c2'
               AND t.c3  = 'c3'
               AND t.c4  = 'c4'
               AND t.c5 IN ['c5']
               AND t.c6 IN ['c60', 'c61', 'c62', 'c63','c64']
               AND t.c7 IN ['c70', 'c71', 'c72', 'c73','c74']
               AND t.c8 IN ['c80', 'c81', 'c82', 'c83','c84']
               AND t.c0 IS NOT NULL
               AND t.c0 != ''
               AND str_to_millis(t.timestamp) IS NOT NULL
               AND str_to_millis(t.timestamp) <= str_to_millis(clock_str());
        """
        self.run_cbq_query(explain_query, query_context=query_context, query_params={'timeout': '4s'})
        self.log.info("MB-64031: Query plan completed within 4s timeout")
