from .cbas_base import *
from membase.api.rest_client import RestHelper

class CBASFunctionalTests(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket":False})
        super(CBASFunctionalTests, self).setUp()
        
        self.validate_error = False
        if self.expected_error:
            self.validate_error = True
            
        ''' Considering all the scenarios where:
        1. There can be 1 KV and multiple cbas nodes(and tests wants to add all cbas into cluster.)
        2. There can be 1 KV and multiple cbas nodes(and tests wants only 1 cbas node)
        3. There can be only 1 node running KV,CBAS service.
        NOTE: Cases pending where there are nodes which are running only cbas. For that service check on nodes is needed.
        '''
        if "add_all_cbas_nodes" in self.input.test_params and self.input.test_params["add_all_cbas_nodes"] and len(self.cbas_servers) >= 1:
            self.add_all_cbas_node_then_rebalance()
            result = self.load_sample_buckets(servers=list(set(self.cbas_servers + [self.master, self.cbas_node])),
                                              bucketName="travel-sample",
                                              total_items=self.travel_sample_docs_count)
        else:
            result = self.load_sample_buckets(servers=list({self.master, self.cbas_node}),
                                              bucketName="travel-sample",
                                              total_items=self.travel_sample_docs_count)

        self.assertTrue(result, msg="wait_for_memcached failed while loading sample bucket: travel-sample")
        
    def test_create_bucket_on_cbas(self):
        # Create bucket on CBAS
        result = self.create_bucket_on_cbas(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_name=self.cb_bucket_name,
            cb_server_ip=self.cb_server_ip,
            validate_error_msg=self.validate_error)
        
        if self.otpNodes:
            self.cleanup_cbas()
            self.remove_node(otpnode=self.otpNodes)
            
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")
        
    def test_create_another_bucket_on_cbas(self):
        # Create first bucket on CBAS
        result = self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)
        self.assertTrue(result, "Not able to create first bucket on cbas. Bucket: %s"%self.cbas_bucket_name)
        
        # Create another bucket on CBAS
        result = self.create_bucket_on_cbas(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_name=self.cb_bucket_name,
            cb_server_ip=self.cb_server_ip,
            validate_error_msg=self.validate_error)
        self.assertTrue(result, "Test for second bucket with same name on cbas failed.")
        
#         if self.otpNodes:
#             self.cleanup_cbas()
#             self.remove_node(otpnode=self.otpNodes)
        if not result:
            self.fail("Test failed")

    def test_create_dataset_on_bucket(self):
        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        result = self.create_dataset_on_bucket(
            cbas_bucket_name=self.cbas_bucket_name_invalid,
            cbas_dataset_name=self.cbas_dataset_name,
            validate_error_msg=self.validate_error)
#         if self.otpNodes:
#             self.remove_node(otpnode=self.otpNodes)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_create_another_dataset_on_bucket(self):
        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create first dataset on the CBAS bucket
        self.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                      cbas_dataset_name=self.cbas_dataset_name)

        # Create another dataset on the CBAS bucket
        result = self.create_dataset_on_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cbas_dataset_name=self.cbas_dataset2_name,
            validate_error_msg=self.validate_error)
#         if self.otpNodes:
#             self.remove_node(otpnode=self.otpNodes)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_connect_bucket(self):
        # Create bucket on CBAS
        if self.cb_bucket_password:
            self.cb_server_ip=self.master.ip
            
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        if not self.skip_create_dataset:
            self.create_dataset_on_bucket(
                cbas_bucket_name=self.cbas_bucket_name,
                cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        result = self.connect_to_bucket(cbas_bucket_name=
                                        self.cbas_bucket_name_invalid,
                                        cb_bucket_password=self.cb_bucket_password,
                                        validate_error_msg=self.validate_error)
        if self.otpNodes:
            self.remove_node(otpnode=self.otpNodes)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_connect_bucket_on_a_connected_bucket(self):
        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        self.create_dataset_on_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Create another connection to bucket
        result = self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                        cb_bucket_password=self.cb_bucket_password,
                                        validate_error_msg=self.validate_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_disconnect_bucket(self):
        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        self.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                      cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Disconnect from bucket
        result = self.disconnect_from_bucket(cbas_bucket_name=
                                             self.cbas_bucket_name_invalid,
                                             disconnect_if_connected=
                                             self.disconnect_if_connected,
                                             validate_error_msg=self.validate_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_disconnect_bucket_already_disconnected(self):
        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        self.create_dataset_on_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.connect_to_bucket(
            cbas_bucket_name=self.cbas_bucket_name,
            cb_bucket_password=self.cb_bucket_password)

        # Disconnect from Bucket
        self.disconnect_from_bucket(cbas_bucket_name=self.cbas_bucket_name)

        # Disconnect again from the same bucket
        result = self.disconnect_from_bucket(cbas_bucket_name=
                                             self.cbas_bucket_name,
                                             disconnect_if_connected=
                                             self.disconnect_if_connected,
                                             validate_error_msg=self.validate_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_drop_dataset_on_bucket(self):
        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        self.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                      cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Drop Connection
        if not self.skip_drop_connection:
            self.disconnect_from_bucket(self.cbas_bucket_name)

        # Drop dataset
        result = self.drop_dataset(
            cbas_dataset_name=self.cbas_dataset_name_invalid,
            validate_error_msg=self.validate_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_drop_cbas_bucket(self):
        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        self.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                      cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Drop connection and dataset
        if not self.skip_drop_connection:
            self.disconnect_from_bucket(self.cbas_bucket_name)

        if not self.skip_drop_dataset:
            self.drop_dataset(self.cbas_dataset_name)

        result = self.drop_cbas_bucket(
            cbas_bucket_name=self.cbas_bucket_name_invalid,
            validate_error_msg=self.validate_error)
        if not result:
            self.fail("FAIL : Actual error msg does not match the expected")

    def test_or_predicate_evaluation(self):
        ds_name = "filtered_travel_ds"
        predicates = self.input.param("predicates", None).replace("&eq", "=").replace("&qt", "\"")
        self.log.info("predicates = %s", predicates)

        # Create bucket on CBAS
        self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        self.create_dataset_on_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                      cbas_dataset_name=self.cbas_dataset_name)

        # Create a dataset on the CBAS bucket with filters
        statement_create_dataset = "create dataset {0} on {1}".format(
            ds_name, self.cbas_bucket_name)
        if predicates:
            statement_create_dataset += " where {0}".format(predicates) + ";"
        else:
            statement_create_dataset += ";"

        status, metrics, errors, results, _ = self.execute_statement_on_cbas(
            statement_create_dataset, self.master)
        
        self.log.info("Executing Statement on CBAS: %s", statement_create_dataset)
        
        if errors:
            self.log.info("Statement = %s", statement_create_dataset)
            self.log.info(errors)
            self.fail("Error while creating Dataset with OR predicates")

        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Allow ingestion to complete
        self.sleep(60)

        stmt_count_records_on_full_ds = "select count(*) from {0}".format(
            self.cbas_dataset_name)
        if predicates:
            stmt_count_records_on_full_ds += " where {0}".format(
                predicates) + ";"
        else:
            stmt_count_records_on_full_ds += ";"

        stmt_count_records_on_filtered_ds = "select count(*) from {0};".format(
            ds_name)

        # Run query on full dataset with filters applied in the query
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            stmt_count_records_on_full_ds)
        count_full_ds = results[0]["$1"]
        self.log.info("**Count of records in full dataset = %s", count_full_ds)

        # Run query on the filtered dataset to retrieve all records
        status, metrics, errors, results, _ = self.execute_statement_on_cbas_via_rest(
            stmt_count_records_on_filtered_ds)
        count_filtered_ds = results[0]["$1"]
        self.log.info("**Count of records in filtered dataset ds1 = %s",
                      count_filtered_ds)
        # Validate if the count of records in both cases is the same
        if count_filtered_ds != count_full_ds:
            self.fail("Count not matching for full dataset with filters in "
                      "query vs. filtered dataset")
