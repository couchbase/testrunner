from .cbas_base import *
from threading import Thread
import threading


class CBASCompilationParamsTests(CBASBaseTest):
    def setUp(self):
        super(CBASCompilationParamsTests, self).setUp()
        self.validate_error = False
        if self.expected_error:
            self.validate_error = True
        self.handles = []
        self.rest = RestConnection(self.master)
        self.failed_count = 0
        self.success_count = 0
        self.rejected_count = 0
        self.expected_count = self.num_items
        self.cb_bucket_name = "default"
        self.cbas_bucket_name = "default_bucket"
        self.cbas_dataset_name = "default_ds"
        self.validate_item_count = True
        self.statement = "select sleep(count(*),500) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        if "add_all_cbas_nodes" in self.input.test_params and self.input.test_params["add_all_cbas_nodes"] and len(self.cbas_servers) > 1:
            self.add_all_cbas_node_then_rebalance()
            
    def tearDown(self):
        super(CBASCompilationParamsTests, self).tearDown()

    def _setupForTest(self):
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

        # Load CB bucket
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0,
                                               self.num_items)

        # Wait while ingestion is completed
        total_items, _ = self.get_num_items_in_cbas_dataset(
            self.cbas_dataset_name)
        while (self.num_items > total_items):
            self.sleep(5)
            total_items, _ = self.get_num_items_in_cbas_dataset(
                self.cbas_dataset_name)

    def test_compilation_params(self):
        self._setupForTest()
        compiler_param_statement = "SET `{0}` \"{1}\";".format(
            self.compiler_param, self.compiler_param_val)
        group_by_query_statement = "select first_name, sleep(count(*),500) from {0} GROUP BY first_name;".format(
            self.cbas_dataset_name)
        order_by_query_statement = "select first_name from {0} ORDER BY first_name;".format(
            self.cbas_dataset_name)
        default_query_statement = "select sleep(count(*),500) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        join_query_statement = "select a.firstname, b.firstname from {0} a, {0} b where a.number=b.number;".format(
            self.cbas_dataset_name)

        if self.compiler_param == "compiler.groupmemory":
            self.statement = compiler_param_statement + group_by_query_statement
        elif self.compiler_param == "compiler.joinmemory":
            self.statement = compiler_param_statement + join_query_statement
        elif self.compiler_param == "compiler.sortmemory":
            self.statement = compiler_param_statement + order_by_query_statement
        else:
            self.statement = compiler_param_statement + default_query_statement

        try:
            status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
                self.statement, mode=self.mode, timeout=3600)
            self.log.info("Status = %s", status)
            if not self.expect_failure and status != "success":
                self.fail("Unexpected failure")
            elif self.expect_failure and status == "success":
                self.fail("Unexpected success")
        except Exception as e:
            if str(e) == "Request Rejected":
                self.log.info("Error 503 : Request Rejected")
            elif str(e) == "Capacity cannot meet job requirement":
                self.log.info(
                    "Error 500 : Capacity cannot meet job requirement")
            else:
                self.log.error(str(e))

            if not self.expect_failure:
                self.fail("Unexpected Error")

    def test_all_params(self):
        self._setupForTest()
        group_order_query_statement = "select count(*), first_name Name from {0} GROUP BY first_name ORDER BY first_name;".format(
            self.cbas_dataset_name)
        compiler_param_statement = "SET `compiler.groupmemory` \"64MB\"; SET `compiler.sortmemory` \"64MB\"; SET `compiler.joinmemory` \"64MB\"; SET `compiler.parallelism` \"0\";"
        join_query_statement = "select a.firstname, b.firstname from {0} a, {0} b where a.number=b.number;".format(
            self.cbas_dataset_name)
        self.statement = compiler_param_statement + group_order_query_statement + join_query_statement

        try:
            status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
                self.statement, mode=self.mode, timeout=3600)
            self.log.info("Status = %s", status)
            if not self.expect_failure and status != "success":
                self.fail("Unexpected failure")
            elif self.expect_failure and status == "success":
                self.fail("Unexpected success")
        except Exception as e:
            if str(e) == "Request Rejected":
                self.log.info("Error 503 : Request Rejected")
            elif str(e) == "Capacity cannot meet job requirement":
                self.log.info(
                    "Error 500 : Capacity cannot meet job requirement")
            else:
                self.log.error(str(e))

            if not self.expect_failure:
                self.fail("Unexpected Error")
