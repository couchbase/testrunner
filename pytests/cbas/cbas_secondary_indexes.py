from .cbas_base import *
from couchbase import FMT_BYTES
import threading
import random


class CBASSecondaryIndexes(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket": False})
        super(CBASSecondaryIndexes, self).setUp()

        self.load_sample_buckets(servers=[self.master],
                                 bucketName=self.cb_bucket_name,
                                 total_items=self.beer_sample_docs_count)

        if "add_all_cbas_nodes" in self.input.test_params and \
                self.input.test_params["add_all_cbas_nodes"] and len(
            self.cbas_servers) > 1:
            self.add_all_cbas_node_then_rebalance()

        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        self.create_dataset_on_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

    def tearDown(self):
        super(CBASSecondaryIndexes, self).tearDown()
    
    def verify_index_used(self, statement, index_used=False, index_name=None):
        statement = 'EXPLAIN %s'%statement
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            statement)
        self.assertEqual(status, "success")
        if status == 'success':
            self.assertEqual(errors, None)
            if index_used:
                self.assertTrue("index-search" in str(results))
                self.assertFalse("data-scan" in str(results))
                self.log.info("INDEX-SEARCH is found in EXPLAIN hence indexed data will be scanned to serve %s"%statement)
                if index_name:
                    self.assertTrue(index_name in str(results))
            else:
                self.assertTrue("data-scan" in str(results))
                self.assertFalse("index-search" in str(results))
                self.log.info("DATA-SCAN is found in EXPLAIN hence index is not used to serve %s"%statement)
 
    def test_create_index(self):
        '''
        Steps :
        1. Create bucket in CBAS, create dataset
        2. Create index on various fields as passed in the parameters
        3. Validate if the index is created and the index definition has the expected fields

        Author : Mihir Kamdar
        Created date : 8/1/2017
        '''
        # Create Index
        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

    def test_create_index_without_if_not_exists(self):
        '''
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Again create an index with the same name without using IF_NOT_EXISTS clause
            3. Validate if the error msg is as expected

            Author : Mihir Kamdar
            Created date : 8/1/2017
        '''
        # Create Index
        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

        # Create another index with same name
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)
        self.assertTrue(self.validate_error_in_response(status, errors),
                        "Error msg not matching expected error msg")

    def test_create_index_with_if_not_exists(self):
        '''
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Again create an index with the same name using IF_NOT_EXISTS clause
            3. Validate if that there  is no error

            Author : Mihir Kamdar
            Created date : 8/1/2017
        '''
        # Create Index
        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        create_idx_statement = "create index {0} IF NOT EXISTS on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

        # Create another index with same name
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)
        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

    def test_create_index_with_if_not_exists_different_fields(self):
        '''
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Again create an index with the same name but with different fields using IF_NOT_EXISTS clause
            4. Validate there is no error
            5. The index definition of should not change.

            Author : Mihir Kamdar
            Created date : 8/1/2017
        '''
        index_field1 = "city:string"
        index_field2 = "abv:bigint"

        # Create Index
        create_idx_statement = "create index {0} IF NOT EXISTS on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_field1)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, [index_field1],
                                      self.cbas_dataset_name)[0])

        # Create another index with same name
        create_idx_statement = "create index {0} IF NOT EXISTS on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_field2)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)
        self.assertTrue(status == "success", "Create Index query failed")

        # The index definition should be based on the older field, it should not change
        self.assertTrue(
            self.verify_index_created(self.index_name, [index_field1],
                                      self.cbas_dataset_name)[0])

    def test_multiple_composite_index_with_overlapping_fields(self):
        '''
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Again create a composite index
            4. Now create another composite index with some overlapping fields
            5. Both the indexes should get created successfully

            Author : Mihir Kamdar
            Created date : 8/1/2017
        '''
        index_fields1 = ["city:string", "abv:bigint"]
        index_fields2 = ["abv:bigint", "geo.lat:double"]

        # Create Index
        index_fields = ""
        for index_field in index_fields1:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        create_idx_statement = "create index {0} IF NOT EXISTS on {1}({2});".format(
            self.index_name + "1", self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name + "1", index_fields1,
                                      self.cbas_dataset_name)[0])

        # Create another composite index with overlapping fields
        index_fields = ""
        for index_field in index_fields2:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        create_idx_statement = "create index {0} IF NOT EXISTS on {1}({2});".format(
            self.index_name + "2", self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)
        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name + "2", index_fields2,
                                      self.cbas_dataset_name)[0])

    def test_create_index_non_empty_dataset(self):
        '''
            Steps :
            1. Create bucket in CBAS, create dataset, connect to the bucket, disconnect from bucket
            2. Create index
            3. Validate the index is created correctly

            Author : Mihir Kamdar
            Created date : 8/1/2017
        '''
        # Connect to Bucket
        result = self.connect_to_bucket(cbas_bucket_name=
                                        self.cbas_bucket_name,
                                        cb_bucket_password=self.cb_bucket_password)

        # Allow ingestion to complete
        self.sleep(30)

        # Disconnect from bucket
        result = self.disconnect_from_bucket(cbas_bucket_name=
                                             self.cbas_bucket_name)

        # Create Index
        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

    def test_create_index_with_bucket_connected(self):
        '''
            Steps :
            1. Create bucket in CBAS, create dataset, connect to the bucket
            2. Create index
            3. Create index should fail.
            4. Validate that the error msg is as expected

            Author : Mihir Kamdar
            Created date : 8/1/2017
        '''
        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=
                               self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Allow ingestion to complete
        self.sleep(30)

        # Create Index
        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(self.validate_error_in_response(status, errors))

    def test_drop_index(self):
        '''
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Validate the index is created correctly
            4. Drop index
            5. Validate that the index is dropped

            Author : Mihir Kamdar
            Created date : 8/1/2017
        '''
        # Create Index
        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

        drop_idx_statement = "drop index {0}.{1};".format(
            self.cbas_dataset_name, self.index_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            drop_idx_statement)

        self.assertTrue(status == "success", "Drop Index query failed")

        self.assertFalse(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

    def test_drop_non_existing_index(self):
        '''
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Drop a non-existing index without using IF_EXISTS clause
            3. Validate that the error msg is as expected
            4. Drop a non-existing index using IF_EXISTS clause
            5. Validate there is no error

            Author : Mihir Kamdar
            Created date : 8/1/2017
        '''
        # Drop non-existing index without IF EXISTS
        drop_idx_statement = "drop index {0}.{1};".format(
            self.cbas_dataset_name, self.index_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            drop_idx_statement)

        self.assertTrue(self.validate_error_in_response(status, errors))

        # Drop non-existing index with IF EXISTS
        drop_idx_statement = "drop index {0}.{1} IF EXISTS;".format(
            self.cbas_dataset_name, self.index_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            drop_idx_statement)

        self.assertEqual(status, "success",
                         "Drop non existent index with IF EXISTS fails")

    def test_drop_dataset_drops_index(self):
        '''
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Validate the index is created correctly
            4. Drop dataset
            5. Validate that the index is also dropped

            Author : Mihir Kamdar
            Created date : 8/1/2017
        '''
        # Create Index
        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

        # Drop dataset
        self.drop_dataset(self.cbas_dataset_name)

        # Check that the index no longer exists
        self.assertFalse(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

    def test_drop_non_empty_index(self):
        '''
            Steps :
            1. Create bucket in CBAS, create dataset
            2. Create index
            3. Validate the index is created correctly
            4. Connect dataset, disconnect dataset
            5. Drop index
            6. Validate that the index is dropped

            Author : Mihir Kamdar
            Created date : 8/1/2017
        '''
        # Create Index
        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=
                               self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Allow ingestion to complete
        self.sleep(30)

        # Disconnect from bucket
        self.disconnect_from_bucket(cbas_bucket_name=
                                    self.cbas_bucket_name)

        drop_idx_statement = "drop index {0}.{1};".format(
            self.cbas_dataset_name, self.index_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            drop_idx_statement)

        self.assertTrue(status == "success", "Drop Index query failed")

        self.assertFalse(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

    def _direct_client(self, server, bucket, timeout=30):
        # CREATE SDK CLIENT
        if self.use_sdk_client:
            try:
                from sdk_client import SDKClient
                scheme = "couchbase"
                host = self.master.ip
                if self.master.ip == "127.0.0.1":
                    scheme = "http"
                    host="{0}:{1}".format(self.master.ip, self.master.port)
                return SDKClient(scheme=scheme, hosts = [host], bucket = bucket)
            except Exception as ex:
                self.log.error("cannot load sdk client due to error {0}".format(str(ex)))
        # USE MC BIN CLIENT WHEN NOT USING SDK CLIENT
        return self.direct_mc_bin_client(server, bucket, timeout= timeout)

    def test_index_population(self):
        '''
        Steps :
        1.
        '''
        # Create Index
#         to_verify=0
        search_by = self.input.param("search_by", '')
        exp_number = self.input.param("exp_number", 0)
        not_fit_value = self.input.param("not_fit_value", '')
        expected_status = self.input.param("status", 'success')
        binary = self.input.param("binary", False)
        index_used = self.input.param("index_used", False)
        if ";" in str(not_fit_value):
            not_fit_value = not_fit_value.split(';')

        testuser = [{'id': self.cb_bucket_name, 'name': self.cb_bucket_name, 'password': 'password'}]
        rolelist = [{'id': self.cb_bucket_name, 'name': self.cb_bucket_name, 'roles': 'admin'}]
        self.add_built_in_server_user(testuser=testuser, rolelist=rolelist)
        self.use_sdk_client =True
        self.client = self._direct_client(self.master, self.cb_bucket_name).cb
        k = 'test_index_population'

        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]

        if binary:
            self.client.upsert('utf16_doc', not_fit_value.encode('utf16'),  format=FMT_BYTES)
        else:
            if "." in index_fields.split(":")[0]:
                self.client.upsert(k, {index_fields.split(":")[0].split(".")[0]:{index_fields.split(":")[0].split(".")[1] : not_fit_value}})
            else:
                self.client.upsert(k, {index_fields.split(":")[0] : not_fit_value})
            
        if index_fields.split(":")[1] == "string" and isinstance(not_fit_value, str) or \
            index_fields.split(":")[1] == "double" and isinstance(not_fit_value, (float, int)) or \
            index_fields.split(":")[1] == "bigint" and isinstance(not_fit_value, (float, int)):
            index_used=True
        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])
        self.connect_to_bucket(cbas_bucket_name=
                                        self.cbas_bucket_name,
                                        cb_bucket_password=self.cb_bucket_password)
        self.sleep(20)

        if isinstance(search_by, str):
            statement = 'SELECT count(*) FROM `{0}` where {1}="{2}"'.format(self.cbas_dataset_name, index_fields.split(":")[0], search_by)
        else:
            statement = 'SELECT count(*) FROM `{0}` where {1}={2}'.format(self.cbas_dataset_name,
                                                                            index_fields.split(":")[0], search_by)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            statement)
        self.assertEqual(status, "success")
        self.assertEqual(errors, None)
        self.assertEqual(results, [{'$1': exp_number}])
        if isinstance(not_fit_value, str):
            statement = 'SELECT count(*) FROM `{0}` where {1}="{2}"'.format(self.cbas_dataset_name,
                                                                        index_fields.split(":")[0], not_fit_value)
        else:
            statement = 'SELECT count(*) FROM `{0}` where {1}={2}'.format(self.cbas_dataset_name,
                                                                        index_fields.split(":")[0], not_fit_value)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            statement)
        self.assertEqual(status, expected_status)
        if status == 'success':
            self.assertEqual(errors, None)
            self.assertEqual(results, [{'$1': 1}])
            
        self.log.info("Verify whether statement %s used index or not. Indexed: %s"%(statement, index_fields))
        self.verify_index_used(statement, index_used, self.index_name)
    # https://issues.couchbase.com/browse/MB-25646
    # https://issues.couchbase.com/browse/MB-25657
    
    def test_index_population_thread(self):
        to_verify = 0
        def update_data(client, index_fields):
            for _ in range(100):
                if index_fields.split(":")[-1] == 'double':
                    not_fit_value = random.choice([False, "sdfs", 11111])
                elif index_fields.split(":")[-1] == 'string':
                    not_fit_value = random.choice([False, 11111, 36.6])
                elif index_fields.split(":")[-1] == 'bigint':
                    not_fit_value = random.choice([False, "sdfs", 36.6])
                perc = random.randrange(0, 100)
                if perc > 75:
                    # 25% with binary data
#                     client.upsert('utf16_doc', str(not_fit_value).encode('utf16'), format=FMT_BYTES)
                    client.upsert(k, {index_fields.split(":")[0]: not_fit_value})
                else:
                    # 10% field removed
                    client.upsert(k, {index_fields.split(":")[0] + "_NEW_FIELD": not_fit_value})
                    
        # Create Index
        search_by = self.input.param("search_by", '')
        exp_number = self.input.param("exp_number", 0)
        not_fit_value = self.input.param("not_fit_value", '')
        expected_status = self.input.param("status", 'success')

        if ";" in not_fit_value:
            not_fit_value = not_fit_value.split(';')

        testuser = [{'id': self.cb_bucket_name, 'name': self.cb_bucket_name, 'password': 'password'}]
        rolelist = [{'id': self.cb_bucket_name, 'name': self.cb_bucket_name, 'roles': 'admin'}]
        self.add_built_in_server_user(testuser=testuser, rolelist=rolelist)
        self.use_sdk_client = True
        self.client = self._direct_client(self.master, self.cb_bucket_name).cb
        k = 'test_index_population_thread'

        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]

        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])
        self.connect_to_bucket(cbas_bucket_name=
                               self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)
        self.sleep(10)

        d = threading.Thread(name='daemon', target=update_data, args=(self.client, index_fields,))
        d.setDaemon(True)
        d.start()

        for i in range(10):
            if isinstance(search_by, str):
                statement = 'SELECT count(*) FROM `{0}` where {1}="{2}"'.format(self.cbas_dataset_name,
                                                                                index_fields.split(":")[0], search_by)
            else:
                statement = 'SELECT count(*) FROM `{0}` where {1}={2}'.format(self.cbas_dataset_name,
                                                                              index_fields.split(":")[0], search_by)
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
                statement)
            self.assertEqual(status, "success")
            self.assertEqual(errors, None)
            self.assertEqual(results, [{'$1': exp_number}])
            if isinstance(not_fit_value, str):
                statement = 'SELECT count(*) FROM `{0}` where {1}="{2}"'.format(self.cbas_dataset_name,
                                                                            index_fields.split(":")[0], not_fit_value)
            else:
                statement = 'SELECT count(*) FROM `{0}` where {1}={2}'.format(self.cbas_dataset_name,
                                                                            index_fields.split(":")[0], not_fit_value)
            status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
                statement)
            self.assertEqual(status, expected_status)
            if status == 'success':
                self.assertEqual(errors, None)
                self.assertEqual(results, [{'$1': 0}])
                
            self.log.info("Verify whether statement %s used index or not. Indexed: %s"%(statement, index_fields))
            self.verify_index_used(statement, index_used, self.index_name)

    def test_index_population_where_statements(self):
        exp_number = self.input.param("exp_number", 0)
        where_statement = self.input.param("where_statement", '').replace('_EQ_', '=')
        index_used = self.input.param("index_used", False)
        
        testuser = [{'id': self.cb_bucket_name, 'name': self.cb_bucket_name, 'password': 'password'}]
        rolelist = [{'id': self.cb_bucket_name, 'name': self.cb_bucket_name, 'roles': 'admin'}]
        self.add_built_in_server_user(testuser=testuser, rolelist=rolelist)
        self.use_sdk_client = True
        self.client = self._direct_client(self.master, self.cb_bucket_name).cb

        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]

        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])
        self.connect_to_bucket(cbas_bucket_name=
                               self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)
        self.sleep(20)

        statement = 'SELECT count(*) FROM `{0}` where {1};'.format(self.cbas_dataset_name, where_statement)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            statement)
        self.assertEqual(status, "success")
        self.assertEqual(errors, None)
        self.assertEqual(results, [{'$1': exp_number}])

        self.log.info("Verify whether statement %s used index or not. Indexed: %s"%(statement, index_fields))
        self.verify_index_used(statement, index_used, self.index_name)
            
    def test_index_population_joins(self):
        exp_number = self.input.param("exp_number", 0)
        self.index_name2 = self.input.param('index_name2', None)
        self.index_fields2 = self.input.param('index_fields2', None)
        if self.index_fields2:
            self.index_fields2 = self.index_fields2.split("-")
        statement = self.input.param("statement", '').replace('_EQ_', '=').replace('_COMMA_', ',')

        testuser = [{'id': self.cb_bucket_name, 'name': self.cb_bucket_name, 'password': 'password'}]
        rolelist = [{'id': self.cb_bucket_name, 'name': self.cb_bucket_name, 'roles': 'admin'}]
        self.add_built_in_server_user(testuser=testuser, rolelist=rolelist)
        self.use_sdk_client = True
        self.client = self._direct_client(self.master, self.cb_bucket_name).cb

        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]

        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name)[0])

        index_fields2 = ""
        for index_field in self.index_fields2:
            index_fields2 += index_field + ","
        index_fields2 = index_fields2[:-1]

        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name2, self.cbas_dataset_name, index_fields2)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name2, self.index_fields2,
                                      self.cbas_dataset_name)[0])

        self.connect_to_bucket(cbas_bucket_name=
                               self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)
        self.sleep(20)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            statement)
        self.assertEqual(status, "success")
        self.assertEqual(errors, None)
        self.assertEqual(len(results), exp_number)

    # https://issues.couchbase.com/browse/MB-25695
    
    def test_index_metadata(self):
        self.buckets = [Bucket(name="beer-sample")]
        self.perform_doc_ops_in_all_cb_buckets(100000, "create", start_key=0, end_key=100000)
        index_fields = ""
        for index_field in self.index_fields:
            index_fields += index_field + ","
        index_fields = index_fields[:-1]
        create_idx_statement = "create index {0} on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_fields)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)

        self.assertTrue(status == "success", "Create Index query failed")

        self.connect_to_bucket(cbas_bucket_name=
                               self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)
        self.wait_for_ingestion_complete([self.cbas_dataset_name], 107303)
        statement = 'SELECT count(*) FROM `{0}`'.format(self.cbas_dataset_name)
#        
        _, result = self.verify_index_created(self.index_name, self.index_fields,
                                              self.cbas_dataset_name)

        self.assertEqual(result[0]['Index']['DatasetName'], self.cbas_dataset_name)
        self.assertEqual(result[0]['Index']['DataverseName'], 'Default')
        self.assertEqual(result[0]['Index']['IndexName'], self.index_name)
        self.assertEqual(result[0]['Index']['IndexStructure'], 'BTREE')
        self.assertEqual(result[0]['Index']['IsPrimary'], False)
        self.assertEqual(result[0]['Index']['PendingOp'], 0)
        self.assertEqual(result[0]['Index']['SearchKey'], [index_field.split(":")[:-1]])
        self.assertEqual(result[0]['Index']['SearchKeyType'], index_field.split(":")[1:])

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            statement)
        self.assertEqual(status, "success")
        self.assertEqual(errors, None)
        self.assertEqual(results, [{'$1': 107303}])

        self.disconnect_from_bucket(cbas_bucket_name=
                                    self.cbas_bucket_name)

        drop_idx_statement = "drop index {0}.{1};".format(self.cbas_dataset_name, self.index_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            drop_idx_statement)

        _, result = self.verify_index_created(self.index_name, self.index_fields,
                                              self.cbas_dataset_name)

        self.assertEqual(result, [])

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            statement)
        self.assertEqual(status, "success")
        self.assertEqual(errors, None)
        self.assertEqual(results, [{'$1': 107303}])
        self.drop_dataset(self.cbas_dataset_name)

        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            statement)
        self.assertEqual(errors, [
            {'msg': 'Cannot find dataset beer_ds in dataverse Default nor an alias with name beer_ds!', 'code': 1}])