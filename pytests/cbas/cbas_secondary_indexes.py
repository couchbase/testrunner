from cbas_base import *


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
                                      self.cbas_dataset_name))

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
                                      self.cbas_dataset_name))

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
                                      self.cbas_dataset_name))

        # Create another index with same name
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)
        self.assertTrue(status == "success", "Create Index query failed")

        self.assertTrue(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name))

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
                                      self.cbas_dataset_name))

        # Create another index with same name
        create_idx_statement = "create index {0} IF NOT EXISTS on {1}({2});".format(
            self.index_name, self.cbas_dataset_name, index_field2)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            create_idx_statement)
        self.assertTrue(status == "success", "Create Index query failed")

        # The index definition should be based on the older field, it should not change
        self.assertTrue(
            self.verify_index_created(self.index_name, [index_field1],
                                      self.cbas_dataset_name))

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
                                      self.cbas_dataset_name))

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
                                      self.cbas_dataset_name))

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
                                      self.cbas_dataset_name))

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
                                      self.cbas_dataset_name))

        drop_idx_statement = "drop index {0}.{1};".format(
            self.cbas_dataset_name, self.index_name)
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            drop_idx_statement)

        self.assertTrue(status == "success", "Drop Index query failed")

        self.assertFalse(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name))

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
                                      self.cbas_dataset_name))

        # Drop dataset
        self.drop_dataset(self.cbas_dataset_name)

        # Check that the index no longer exists
        self.assertFalse(
            self.verify_index_created(self.index_name, self.index_fields,
                                      self.cbas_dataset_name))

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
                                      self.cbas_dataset_name))

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
                                      self.cbas_dataset_name))
