import logging
import threading
import json
import uuid
import time
import os
from TestInput import TestInputSingleton

from .tuq import QueryTests
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError, ReadDocumentException
from remote.remote_util import RemoteMachineShellConnection
from security.rbac_base import RbacBase

class QueryBitwiseTests(QueryTests):
    def setup(self):
        super(QueryBitwiseTests, self).setUp()
        users = TestInputSingleton.input.param("users", None)
        self.all_buckets = TestInputSingleton.input.param("all_buckets", False)
        self.inp_users = []
        if users:
            self.inp_users = eval(eval(users))

        self.create_users()
        self.users = self.get_user_list()
        self.roles = self.get_user_role_list()

    def suite_setUp(self):
        super(QueryBitwiseTests, self).suite_setUp()

    def tearDown(self):
        super(QueryBitwiseTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryBitwiseTests, self).suite_tearDown()

    # This test does bitwise and on zeros and ones.
    def test_bitwise_and_zeros_and_ones(self):
        self.query = "select bitand(0,0,1,0,1,0,1)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 0}])
        self.query = "select bitand(0,0,0,0,0,0,0)"
        self.assertEqual(actual_result['results'], [{'$1': 0}])
        self.query = "select bitand(1,1,1,1,1,1,1)"
        self.assertEqual(actual_result['results'], [{'$1': 0}])


    def test_bitwise_and_no_arguments(self):
        self.query = "select bitand()"
        try:
          self.run_cbq_query()
        except Exception as ex:
          print(ex)
        self.query = "select bitand(0)"
        try:
          self.run_cbq_query()
        except Exception as ex:
          print(ex)


    def test_bitwise_and_long_integers(self):
        self.query = "select bitand(9223372036854775808,1470691191458562048)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': None}])

    def test_bitwise_and_numeric_values(self):
        self.query = "select bitand(11,3)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 3}])

    def test_bitwise_and_nulls_missing_values(self):
        self.query = "select bitand(null,null)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': None}])
        self.query = "select bitand(missing,missing)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{}])
        self.query = "select bitand(null,1)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': None}])
        self.query = "select bitand(0,null)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': None}])

    def test_string_decimal_data_types(self):
        self.query = 'select bitand("test","test")'
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': None}])
        self.query = 'select bitand(0.23,"text")'
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': None}])
        self.query = 'create index idx on default(join_day)'
        self.run_cbq_query()
        self.query = 'select bitand(join_day,join_yr) from default where join_day is not missing order by meta().id limit 1'
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 9}])

    # This test does bitwise or on zeros and ones.
    def test_bitwise_or_zeros_and_ones(self):
        self.query = "select bitor(0,0,1,0,1,0,1)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 1}])
        self.query = "select bitor(0,0,0,0,0,0,0)"
        self.assertEqual(actual_result['results'], [{'$1': 1}])
        self.query = "select bitor(1,1,1,1,1,1,1)"
        self.assertEqual(actual_result['results'], [{'$1': 1}])


    def test_bitwise_or_incorrect_arguments(self):
        self.query = "select bitor()"
        try:
           self.run_cbq_query()
        except Exception as ex:
            print(ex)
        self.query = "select bitor(2)"
        try:
           self.run_cbq_query()
        except Exception as ex:
            print(ex)
        self.query = "select bitor(2,4,5)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 7}])

    def test_bitwise_and_or_nested_levels(self):
        self.query = "select bitand(bitand(BITOR(1,123),1),111,BITOR(1,123))"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 1}])

    def test_bitwise_and_or_not_levels(self):
        self.query = "Select bitnot(bitand(missing,bitnot(BitNOT(bitnot(-4)))))"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{}])

    def test_bitwise_not(self):
        self.query = "Select BitNOT(3)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': -4}])

    def test_bitwise_not_missing_args(self):
        self.query = "Select bitnot(bitand(missing,bitnot(BitNOT(bitnot(null)))))"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{}])

    def test_bitwise_xor(self):
        self.query = "Select BitXOR(3,6)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 5}])

    def test_bitshift(self):
        self.query = "Select BitSHIFT(6,1,false)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 12}])
        self.query = "Select BitSHIFT(8,-2)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 2}])
        self.query = "Select BitSHIFT(123,2,true)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 492}])
        self.query = "select bitshift(123,2)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 492}])
        self.query = "Select BitSHIFT(234,-2,true)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': -9223372036854775750}])
        self.query = "select bitshift(323,-2)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 80}])
        self.query = "Select bitshift(BitSHIFT(432424234434,-2,true),2,true)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 432424234434}])
        self.query = "Select bitshift(BitSHIFT(432424234434,2,true),-2,true)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 432424234434}])

    def test_default_bitand_bitor_bitxor_bitnot(self):
        self.query = "select bitand(VMs[0].RAM,VMs[1].RAM) from default where _id='query-testemployee10153.1877827-0'"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 10}])
        self.query = "select bitand(tasks_points.task1,tasks_points.task2) from default where _id='query-testemployee10153.1877827-0'"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 1}])
        self.query = "select bitor(VMs[0].RAM,VMs[1].RAM) from default where _id='query-testemployee10153.1877827-0'"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 10}])
        self.query = "select bitor(tasks_points.task1,tasks_points.task2) from default where _id='query-testemployee10153.1877827-0'"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 1}])
        self.query = "select bitxor(VMs[0].RAM,VMs[1].RAM) from default where _id='query-testemployee10153.1877827-0'"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 0}])
        self.query = "select bitxor(tasks_points.task1,tasks_points.task2) from default where _id='query-testemployee10153.1877827-0'"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 0}])
        self.query = "select bitnot(VMs[0].RAM) from default where _id='query-testemployee10153.1877827-0'"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': -11}])
        self.query = "select bitnot(tasks_points.task1) from default where _id='query-testemployee10153.1877827-0'"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': -2}])


    def test_more_than_10_fields(self):
        self.query = "select bitand(tasks_points.task1,VMs[0].RAM,VMs[1].RAM,tasks_points.task2,join_day,mutated,join_mo,test_rate,VMs[0].memory,VMs[1].memory) from default where _id='query-testemployee10153.1877827-0'"
        actual_result = self.run_cbq_query()
        print(actual_result)
        self.query = "select bitor(tasks_points.task1,VMs[0].RAM,VMs[1].RAM,tasks_points.task2,join_day,mutated,join_mo,test_rate,VMs[0].memory,VMs[1].memory) from default where _id='query-testemployee10153.1877827-0'"
        actual_result = self.run_cbq_query()
        print(actual_result)
        self.query = "select bitxor(tasks_points.task1,VMs[0].RAM,VMs[1].RAM,tasks_points.task2,join_day,mutated,join_mo,test_rate,VMs[0].memory,VMs[1].memory) from default where _id='query-testemployee10153.1877827-0'"
        actual_result = self.run_cbq_query()
        print(actual_result)

    def test_bitset(self):
        self.query = "Select BitSET(6,1)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 7}])
        self.query = "Select BitSET(6,[1,2])"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 7}])
        self.query = "select bitset(6,[])"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 6}])
        self.query = "select bitset(6,[0])"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': None}])
        self.query = "select bitset(6,[-1])"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': None}])
        self.query = "Select BitSET(6,[1,4])"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 15}])



    def test_is_bitset(self):
        self.query = "Select IsBitSET(6,1,true)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': False}])
        self.query = "Select IsBitSET(6,2,true)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': True}])
        self.query = "Select IsBitSET(6,1,false)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': False}])
        self.query = "Select IsBitSET(6,2,false)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': True}])
        self.query = "Select IsBitSET(6,1)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': False}])
        self.query = "Select IsBitSET(6,2)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': True}])
        self.query = "Select IsBitSET(6,[2,3],true)"
        self.assertEqual(actual_result['results'], [{'$1': True}])


    def test_bitTest(self):
        self.query = "Select BitTest(6,[1,2],false)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': True}])
        self.query = "Select BitTest(6,[1,2],true)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': False}])
        self.query = "Select BitTest(6,[1,2])"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': True}])

    def test_bitclear(self):
        self.query = "Select BitCLEAR(6,1)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 6}])
        self.query = "Select BitCLEAR(6,[1,2])"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': 4}])
        self.query = "Select BitCLEAR(6,0)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': None}])
        self.query = "Select BitCLEAR(6,-1)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': None}])
        self.query = "Select BitCLEAR(6,-23232323232)"
        actual_result = self.run_cbq_query()
        self.assertEqual(actual_result['results'], [{'$1': None}])











