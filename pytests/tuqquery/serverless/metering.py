from serverless.serverless_basetestcase import ServerlessBaseTestCase
from lib.metering_throttling import metering
import math
from lib.Cb_constants.CBServer import CbServer

class QueryMeterSanity(ServerlessBaseTestCase):
    def setUp(self):
        self.doc_count = 10
        self.scope = '_default'
        self.collection = '_default'
        CbServer.capella_run = True
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def test_meter_write(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{self.doc_count}) d')
            self.log.info(f"billingUnits: {result['billingUnits']}")
            self.assertEqual(result['billingUnits']['wu']['kv'], self.doc_count)
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
            self.assertEqual(after_kv_wu - before_kv_wu, self.doc_count)
            self.assertEqual(after_kv_ru, before_kv_ru) # no read units

    def test_meter_read(self):
        self.provision_databases()
        for database in self.databases.values():
            result = self.run_query(database, f'INSERT INTO {self.collection} (key k, value v) select uuid() as k , {{"name": "San Francisco"}} as v from array_range(0,{self.doc_count}) d')
            # Get sequential scan read unit
            result = self.run_query(database, f'SELECT meta().id FROM {self.collection}')
            sc_ru = result['billingUnits']['ru']['kv']
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            before_kv_ru, before_kv_wu = meter.get_kv_rwu(database.id)
            result = self.run_query(database, f'SELECT * FROM {self.collection}')
            self.log.info(f"billingUnits: {result['billingUnits']}")
            self.assertEqual(result['billingUnits']['ru']['kv'], self.doc_count + sc_ru)
            after_kv_ru, after_kv_wu = meter.get_kv_rwu(database.id)
            self.assertEqual(after_kv_ru - before_kv_ru, self.doc_count + sc_ru)
            self.assertEqual(after_kv_wu, before_kv_wu) # no writes

    def test_meter_cu(self):
        self.provision_databases()
        for database in self.databases.values():
            meter = metering(database.rest_host, database.admin_username, database.admin_password)
            expected_cu = 1
            before_cu = meter.get_query_cu(database.id, unbilled='true')
            result = self.run_query(database, 'SELECT 10+10')
            after_cu = meter.get_query_cu(database.id, unbilled='true')
            self.assertEqual(expected_cu, after_cu - before_cu)

            before_cu = meter.get_query_cu(database.id, unbilled='true')
            repeat = 100
            result = self.run_query(database, f'SELECT SUM( {"SQRT("*repeat} t {")"*repeat} ) FROM array_concat(array_repeat(pi(), 30000), array_repeat(pi(), 30000), array_repeat(pi(), 30000), array_repeat(pi(), 19000) ) t')
            after_cu = meter.get_query_cu(database.id, unbilled='true')
            # 1 compute unit (CU) is 32MB per second
            expected_cu = math.ceil(int(result['metrics']['usedMemory']) * float(result['profile']['cpuTime'][:-1]) / (32*1024*1024))
            actual_cu = after_cu - before_cu
            # +- 1 cu to give a slight buffer
            self.assertTrue((expected_cu - 1) <= actual_cu <= (expected_cu + 1), f"The cu we got is not the cu we expected actual: {actual_cu} expected: {expected_cu}")
