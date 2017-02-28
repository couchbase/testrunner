from cbas_base import *
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
        self.statement = "select sleep(count(*),500) from {0} where mutated=0;".format(
            self.cbas_dataset_name)

    def tearDown(self):
        super(CBASConcurrentQueryMgtTests, self).tearDown()

    def setupForTest(self):
        # Delete Default bucket and load travel-sample bucket
        self.cluster.bucket_delete(server=self.master, bucket="default")
        self.load_sample_buckets(server=self.master,
                                 bucketName="travel-sample")

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

    def test_concurrent_query_mgmt(self):
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

        # Run queries concurrently
        threads = []
        for i in range(0, self.num_concurrent_queries):
            threads.append(Thread(target=self._run_query,
                                  name="query thread {0}".format(i), args=()))
        i = 0
        for thread in threads:
            # Send requests in batches, and sleep for 5 seconds before sending another batch of queries.
            i += 1
            if i % self.concurrent_batch_size == 0:
                self.sleep(5, "submitted {0} queries".format(i))
            thread.start()
        for thread in threads:
            thread.join()

        self.log.info("%s queries submitted, %s failed, %s passed, %s rejected" % (
        self.num_concurrent_queries, self.failed_count, self.success_count, self.rejected_count))

        if self.failed_count + self.success_count + self.rejected_count != self.num_concurrent_queries:
            self.fail("Some queries errored out. Check logs.")

        if self.failed_count > 0:
            self.fail("Some queries failed.")

    def _run_query(self):
        # Execute query (with sleep induced)
        name = threading.currentThread().getName();

        try:
            status, metrics, errors, results, handle = self.execute_statement_on_cbas_via_rest(
                self.statement, mode=self.mode, rest=self.rest, timeout=3600)
            # Validate if the status of the request is success, and if the count matches num_items
            if self.mode == "immediate":
                if status == "success":
                    if results[0]['$1'] != self.num_items:
                        self.log.info("Query result : %s", results[0]['$1'])
                        self.log.info("********Thread %s : failure**********",
                                      name)
                        self.failed_count += 1
                    else:
                        self.log.info("--------Thread %s : success----------",
                                      name)
                        self.success_count += 1
                else:
                    self.log.info("Status = %s", status)
                    self.log.info("********Thread %s : failure**********", name)
                    self.failed_count += 1
            elif self.mode == "async":
                if status == "started" and handle:
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
            if str(e)=="Request Rejected":
                self.log.info("Error 503 : Request Rejected")
                self.rejected_count += 1
            else:
                self.log.error(str(e))



