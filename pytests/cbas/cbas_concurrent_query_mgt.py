from .cbas_base import *
from threading import Thread
import threading


class CBASConcurrentQueryMgtTests(CBASBaseTest):
    def setUp(self):
        super(CBASConcurrentQueryMgtTests, self).setUp()
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

    def tearDown(self):
        super(CBASConcurrentQueryMgtTests, self).tearDown()

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

    def test_concurrent_query_mgmt(self):
        self._setupForTest()
        self._run_concurrent_queries(self.statement, self.mode, self.num_concurrent_queries)

    def test_resource_intensive_queries_queue_mgmt(self):
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
        elif self.compiler_param == "compiler.parallelism":
            self.statement = compiler_param_statement + default_query_statement

        self.validate_item_count = False

        self._run_concurrent_queries(self.statement, self.mode, self.num_concurrent_queries)

        if self.expect_reject:
            if self.rejected_count < self.num_concurrent_queries:
                self.fail("Not all queries rejected")
        else:
            if self.rejected_count:
                self.fail("Some queries rejected")

    def test_cancel_ongoing_request(self):
        self._setupForTest()

        client_context_id = "abcd1234"
        statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
            statement, mode="async", client_context_id=client_context_id)

        status = self.delete_request(client_context_id)
        if str(status) != "200":
            self.fail ("Status is not 200")

    def test_cancel_cancelled_request(self):
        self._setupForTest()

        client_context_id = "abcd1234"
        statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
            statement, mode="async", client_context_id=client_context_id)

        status = self.delete_request(client_context_id)
        if str(status) != "200":
            self.fail("Status is not 200")

        status = self.delete_request(client_context_id)
        if str(status) != "404":
            self.fail("Status is not 404")

    def test_cancel_invalid_context(self):
        client_context_id = "abcd1235"
        status = self.delete_request(client_context_id)
        if str(status) != "404":
            self.fail ("Status is not 404")

    def test_cancel_completed_request(self):
        self._setupForTest()

        client_context_id = "abcd1234"

        statement = "select count(*) from {0};".format(self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
            statement, mode="immediate", client_context_id=client_context_id)

        status = self.delete_request(client_context_id)
        if str(status) != "404":
            self.fail ("Status is not 404")

    def test_cancel_request_in_queue(self):
        client_context_id = "query_thread_{0}".format(int(self.num_concurrent_queries)-1)
        self._setupForTest()
        self._run_concurrent_queries(self.statement, self.mode, self.num_concurrent_queries)
        status = self.delete_request(client_context_id)
        if str(status) != "200":
            self.fail ("Status is not 200")

    def test_cancel_ongoing_request_null_contextid(self):
        self._setupForTest()

        client_context_id = None
        statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
            statement, mode="async", client_context_id=client_context_id)

        status = self.delete_request(client_context_id)
        if str(status) != "404":
            self.fail("Status is not 404")

    def test_cancel_ongoing_request_empty_contextid(self):
        self._setupForTest()

        client_context_id = ""
        statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
            statement, mode="async", client_context_id=client_context_id)

        status = self.delete_request(client_context_id)
        if str(status) != "200":
            self.fail("Status is not 200")

    def test_cancel_multiple_ongoing_request_same_contextid(self):
        self._setupForTest()

        client_context_id = "abcd1234"
        statement = "select sleep(count(*),10000) from {0} where mutated=0;".format(
            self.cbas_dataset_name)
        status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
            statement, mode="async", client_context_id=client_context_id)
        status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
            statement, mode="async", client_context_id=client_context_id)

        status = self.delete_request(client_context_id)
        if str(status) != "200":
            self.fail("Status is not 200")

    def test_rest_api_authorization_cancel_request(self):
        validation_failed = False

        self._setupForTest()

        roles = [{"role": "ro_admin",
                  "expected_status": 401},
                 {"role": "cluster_admin",
                  "expected_status": 200},
                 {"role": "admin",
                  "expected_status": 200},
                 {"role": "analytics_manager[*]",
                  "expected_status": 200},
                 {"role": "analytics_reader",
                  "expected_status": 200}]

        for role in roles:
            self._create_user_and_grant_role("testuser", role["role"])
            self.sleep(5)

            client_context_id = "abcd1234"
            statement = "select sleep(count(*),5000) from {0} where mutated=0;".format(
                self.cbas_dataset_name)
            status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
                statement, mode="async", client_context_id=client_context_id)

            status = self.delete_request(client_context_id)
            if str(status) != str(role["expected_status"]):
                self.log.info(
                    "Error cancelling request as user with {0} role. Response = {1}".format(
                        role["role"], status))
                validation_failed = True
            else:
                self.log.info(
                    "Cancelling request as user with {0} role worked as expected".format(
                        role["role"]))

            self._drop_user("testuser")

        self.assertFalse(validation_failed,
                         "Authentication errors with some APIs. Check the test log above.")
