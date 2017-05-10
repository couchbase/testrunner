import threading

from cbas_base import *


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

    def _run_concurrent_queries(self):
        # Run queries concurrently
        self.log.info("Running queries concurrently now...")
        threads = []
        for i in range(0, self.num_concurrent_queries):
            threads.append(Thread(target=self._run_query,
                                  name="query_thread_{0}".format(i), args=()))
        i = 0
        for thread in threads:
            # Send requests in batches, and sleep for 5 seconds before sending another batch of queries.
            i += 1
            if i % self.concurrent_batch_size == 0:
                self.sleep(5, "submitted {0} queries".format(i))
            thread.start()
        for thread in threads:
            thread.join()

        self.log.info(
            "%s queries submitted, %s failed, %s passed, %s rejected" % (
                self.num_concurrent_queries, self.failed_count,
                self.success_count, self.rejected_count))

        if self.failed_count + self.success_count + self.rejected_count != self.num_concurrent_queries:
            self.fail("Some queries errored out. Check logs.")

        if self.failed_count > 0:
            self.fail("Some queries failed.")

    def _run_query(self):
        # Execute query (with sleep induced)
        name = threading.currentThread().getName();
        client_context_id = name

        try:
            status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
                self.statement, mode=self.mode, rest=self.rest, timeout=3600,
                client_context_id=client_context_id)
            # Validate if the status of the request is success, and if the count matches num_items
            if self.mode == "immediate":
                if status == "success":
                    if self.validate_item_count:
                        if results[0]['$1'] != self.expected_count:
                            self.log.info("Query result : %s", results[0]['$1'])
                            self.log.info(
                                "********Thread %s : failure**********",
                                name)
                            self.failed_count += 1
                        else:
                            self.log.info(
                                "--------Thread %s : success----------",
                                name)
                            self.success_count += 1
                    else:
                        self.log.info("--------Thread %s : success----------",
                                      name)
                        self.success_count += 1
                else:
                    self.log.info("Status = %s", status)
                    self.log.info("********Thread %s : failure**********", name)
                    self.failed_count += 1

            elif self.mode == "async":
                if status == "running" and handle:
                    self.log.info("--------Thread %s : success----------", name)
                    self.success_count += 1
                else:
                    self.log.info("Status = %s", status)
                    self.log.info("********Thread %s : failure**********", name)
                    self.failed_count += 1

            elif self.mode == "deferred":
                if status == "success" and handle:
                    self.log.info("--------Thread %s : success----------", name)
                    self.success_count += 1
                else:
                    self.log.info("Status = %s", status)
                    self.log.info("********Thread %s : failure**********", name)
                    self.failed_count += 1
        except Exception, e:
            if str(e) == "Request Rejected":
                self.log.info("Error 503 : Request Rejected")
                self.rejected_count += 1
            elif str(e) == "Capacity cannot meet job requirement":
                self.log.info(
                    "Error 500 : Capacity cannot meet job requirement")
                self.rejected_count += 1
            else:
                self.log.error(str(e))

    def test_concurrent_query_mgmt(self):
        self._setupForTest()
        self._run_concurrent_queries()

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

        self._run_concurrent_queries()

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
        self._run_concurrent_queries()
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
