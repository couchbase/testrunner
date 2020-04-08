import json
import math
import re
import uuid
import time
from .tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError


class QueriesIndexTestsExtended(QueryTests):

    FIELDS_TO_INDEX = [('name', 'job_title'), ('name', 'join_yr'), ('VMs', 'name')]
    COMPLEX_FIELDS_TO_INDEX = ['VMs', 'tasks_points', 'skills']
    def setUp(self):
        super(QueriesIndexTestsExtended, self).setUp()
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
        super(QueriesIndexTestsExtended, self).suite_setUp()
        self.log.info("==============  QueriesIndexTests suite_setup has started ==============")
        self.log.info("==============  QueriesIndexTests suite_setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  QueriesIndexTests tearDown has started ==============")
        self.log.info("==============  QueriesIndexTests tearDown has completed ==============")
        super(QueriesIndexTestsExtended, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  QueriesIndexTests suite_tearDown has started ==============")
        self.log.info("==============  QueriesIndexTests suite_tearDown has completed ==============")
        super(QueriesIndexTestsExtended, self).suite_tearDown()

    def test_meta_indexcountscan(self):
        test_dict = dict()
        index_type = self.index_type.lower()
        self.fail_if_no_buckets()
        for bucket in self.buckets:
            bname = bucket.name

            primary_idx = {'name': '#primary', 'bucket': bname, 'fields': [], 'state': 'online', 'using': index_type, 'is_primary': True}
            idx_1 = {'name': "idx", 'bucket': bname, 'fields': [("name", 0)], 'state': "online", 'using': index_type}
            idx_2 = {'name': "idx2", 'bucket': bname, 'fields': [("join_mo", 0)], 'state': "online", 'using': index_type}
            idx_3 = {'name': "idx3", 'bucket': bname, 'fields': [("VMs[0].memory", 0)], 'state': "online", 'using': index_type}
            idx_4 = {'name': "idx", 'bucket': bname, 'fields': [("name", 0), ("meta().id", 1)], 'state': "online", 'using': index_type}
            idx_5 = {'name': "idx", 'bucket': bname, 'fields': [("meta().id", 0)], 'state': "online", 'using': index_type}
            idx_6 = {'name': "idx", 'bucket': bname, 'fields': [("join_yr", 0)], 'state': "online", 'using': index_type, 'where': 'join_mo = 12'}
            idx_7 = {'name': "idx", 'bucket': bname, 'fields': [("join_day", 0)], 'state': "online", 'using': index_type, 'where': 'join_yr < 2012'}
            idx_8 = {'name': "idx", 'bucket': bname, 'fields': [("name", 0), ("join_day", 1), ("join_yr", 2)], 'state': "online", 'using': index_type}

            query_1 = 'explain select count(1) from {0} where name = employee-23 and meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
            explain_1 = lambda x: self.ExplainPlanHelper(x['q_res'][0])
            assert_1 = lambda x: self.assertTrue("IndexCountScan2" not in x['post_q_res'][0])
            test_dict["%s-01" % (bname)] = {"indexes": [primary_idx, idx_1, idx_2, idx_3], "queries": [query_1], "post_queries": [explain_1], "asserts": [assert_1]}

            query_2 = 'select count(1) from {0} where name = "employee-23" and meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
            assert_2 = lambda x: self.assertEqual(x['q_res'][0]['results'], ([{'$1': 1}]))
            test_dict["%s-02" % (bname)] = {"indexes": [primary_idx, idx_1, idx_2, idx_3], "queries": [query_2], "asserts": [assert_2]}

            query_3 = 'explain select name from {0} where name = "employee-23" and meta().id like "query-testemployee%" limit 3'.format(bucket.name)
            assert_3 = lambda x: self.assertTrue("limit" not in x['post_q_res'][0]['~children'][0])
            test_dict["%s-03" % (bname)] = {"indexes": [primary_idx, idx_1, idx_2, idx_3], "queries": [query_3], "post_queries": [explain_1],  "asserts": [assert_3]}

            query_4 = 'select name from {0} where name = "employee-23" and meta().id like "query-testemployee%" limit 3'.format(bucket.name)
            assert_4 = lambda x: self.assertEqual(x['q_res'][0]['results'], [{'name': 'employee-23'}, {'name': 'employee-23'}, {'name': 'employee-23'}])
            test_dict["%s-04" % (bname)] = {"indexes": [primary_idx, idx_1, idx_2, idx_3], "queries": [query_4], "asserts": [assert_4]}

            query_5 = 'explain select min(join_day) from {0} where VMs[0].memory=12 and meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
            assert_5 = lambda x: self.assertTrue("limit" not in x['post_q_res'][0]['~children'][0])
            test_dict["%s-05" % (bname)] = {"indexes": [primary_idx, idx_1, idx_2, idx_3], "queries": [query_5], "post_queries": [explain_1],"asserts": [assert_5]}

            query_6 = 'select min(VMs[0].memory) from {0} where join_mo=12 and meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
            assert_6 = lambda x: self.assertEqual(x['q_res'][0]['results'], [{'$1': 12}])
            test_dict["%s-06" % (bname)] = {"indexes": [primary_idx, idx_1, idx_2, idx_3], "queries": [query_6], "asserts": [assert_6]}

            query_7 = 'select min(join_mo) from {0} where  VMs[0].memory=12 and meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
            assert_7 = lambda x: self.assertEqual(x['q_res'][0]['results'], [{'$1': 12}])
            test_dict["%s-07" % (bname)] = {"indexes": [primary_idx, idx_1, idx_2, idx_3], "queries": [query_7], "asserts": [assert_7]}

            query_8 = 'explain select count(1) from {0} where name = "employee-23"'.format(bucket.name)
            assert_8 = lambda x: self.assertEqual(x['post_q_res'][0]['~children'][0]['#operator'], "IndexScan3")
            test_dict["%s-08" % (bname)] = {"indexes": [primary_idx, idx_1, idx_2, idx_3], "queries": [query_8], "post_queries": [explain_1], "asserts": [assert_8]}

            query_9 = 'select count(1) from {0} where name = "employee-23"'.format(bucket.name)
            assert_9 = lambda x: self.assertEqual(x['q_res'][0]['results'], [{'$1': 432}])
            test_dict["%s-09" % (bname)] = {"indexes": [primary_idx, idx_1, idx_2, idx_3], "queries": [query_9], "asserts": [assert_9]}

            query_10 = 'explain select count(1) from {0} where name = "employee-23" and join_yr=2010'.format(bucket.name)
            assert_10 = lambda x: self.assertEqual(x['post_q_res'][0]['~children'][0]['#operator'], "IndexScan3")
            test_dict["%s-10" % (bname)] = {"indexes": [primary_idx, idx_1, idx_2, idx_3], "queries": [query_10], "post_queries": [explain_1], "asserts": [assert_10]}

            query_11 = 'select count(1) from {0} where name = "employee-23" and join_mo=12'.format(bucket.name)
            assert_11 = lambda x: self.assertEqual(x['q_res'][0]['results'], [{ "$1": 36}])
            test_dict["%s-11" % (bname)] = {"indexes": [primary_idx, idx_1, idx_2, idx_3], "queries": [query_11], "asserts": [assert_11]}

            query_12 = 'explain select count(1) from {0} where name = "employee-23" and meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
            assert_12 = lambda x: self.assertEqual(x['post_q_res'][0]['~children'][0]['#operator'], "IndexScan3")
            test_dict["%s-12" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_4], "queries": [query_12], "post_queries": [explain_1], "asserts": [assert_12]}

            query_13 = 'select count(1) from {0} where name = "employee-23" and meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
            assert_13 = lambda x: self.assertEqual(x['q_res'][0]['results'], ([{'$1': 1}]))
            test_dict["%s-13" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_4], "queries": [query_13], "asserts": [assert_13]}

            query_14 = 'explain select count(1) from {0} where meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
            assert_14 = lambda x: self.assertEqual(x['post_q_res'][0]['~children'][0]['#operator'], "IndexScan3")
            test_dict["%s-14" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_5], "queries": [query_14], "post_queries": [explain_1], "asserts": [assert_14]}

            query_15 = 'select count(1) from {0} where meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
            assert_15 = lambda x: self.assertEqual(x['q_res'][0]['results'], [{'$1': 1}])
            test_dict["%s-15" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_5], "queries": [query_15], "asserts": [assert_15]}

            query_16 = 'explain select count(1) from {0} where meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
            assert_16 = lambda x: self.assertEqual(x['post_q_res'][0]['~children'][0]['#operator'], "IndexScan3")
            test_dict["%s-16" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3], "queries": [query_16], "post_queries": [explain_1], "asserts": [assert_16]}

            query_17 = 'select count(1) from {0} where meta().id = "query-testemployee10317.9004497-0"'.format(bucket.name)
            assert_17 = lambda x: self.assertEqual(x['q_res'][0]['results'], [{'$1': 1}])
            test_dict["%s-17" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3], "queries": [query_17], "asserts": [assert_17]}

            query_18 = 'explain select count(*) from {0} where join_yr>2010 and join_mo = 12'.format(bucket.name)
            assert_18 = lambda x: self.assertEqual(x['post_q_res'][0]['~children'][0]['#operator'], "IndexScan3")
            test_dict["%s-18" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_6], "queries": [query_18], "post_queries": [explain_1], "asserts": [assert_18]}

            query_19 = 'select count(*) from {0} where join_yr>2010 and join_mo = 12'.format(bucket.name)
            assert_19 = lambda x: self.assertEqual(x['q_res'][0]['results'], [{'$1': 504}])
            test_dict["%s-19" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_6], "queries": [query_19], "asserts": [assert_19]}

            query_20 = 'explain select count(*) from {0} where join_day = 23 and join_yr<2010'.format(bucket.name)
            assert_20 = lambda x: self.assertEqual(x['post_q_res'][0]['~children'][0]['#operator'], "IndexScan3")
            test_dict["%s-20" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_7], "queries": [query_20], "post_queries": [explain_1], "asserts": [assert_20]}

            query_21 = 'select count(*) from {0} where join_day = 23 and join_yr<2011'.format(bucket.name)
            assert_21 = lambda x: self.assertEqual(x['q_res'][0]['results'], [{'$1': 216}])
            test_dict["%s-21" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_7], "queries": [query_21], "asserts": [assert_21]}

            query_22 = 'explain select count(*) from {0} where join_day = 23 and join_yr<2012 limit 10'.format(bucket.name)
            assert_22 = lambda x: self.assertTrue("limit" not in x['post_q_res'][0]['~children'][0])
            test_dict["%s-22" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_7], "queries": [query_22], "post_queries": [explain_1], "asserts": [assert_22]}

            query_23 = 'explain select count(1) from {0} where name = "employee-23" and join_day = 23'.format(bucket.name)
            assert_23 = lambda x: self.assertTrue(x['post_q_res'][0]['~children'][0]['#operator'] != "IndexCountScan2")
            test_dict["%s-23" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_8], "queries": [query_23], "post_queries": [explain_1], "asserts": [assert_23]}

            query_24 = 'explain select count(1) from {0} where name = "employee-23" and join_yr = 2010'.format(bucket.name)
            assert_24 = lambda x: self.assertTrue(x['post_q_res'][0]['~children'][0]['#operator'] != "IndexCountScan2")
            test_dict["%s-24" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_8], "queries": [query_24], "post_queries": [explain_1], "asserts": [assert_24]}

            query_25 = 'explain select tasks from {0} where name = "employee-23" and join_day=23 limit 5'.format(bucket.name)
            assert_25 = lambda x: self.assertTrue("limit" in x['post_q_res'][0]['~children'][0]['~children'][0])
            test_dict["%s-25" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_8], "queries": [query_25], "post_queries": [explain_1], "asserts": [assert_25]}

            query_26 = 'explain select join_day from {0} where name = "employee-23" and join_yr = 2010 limit 5'.format(bucket.name)
            assert_26 = lambda x: self.assertTrue("limit" not in x['post_q_res'][0]['~children'][0])
            test_dict["%s-26" % (bname)] = {"indexes": [primary_idx, idx_2, idx_3, idx_8], "queries": [query_26], "post_queries": [explain_1], "asserts": [assert_26]}
        self.query_runner(test_dict)
