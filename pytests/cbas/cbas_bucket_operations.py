from cbas_base import *


class CBASBucketOperations(CBASBaseTest):
    def setUp(self):
        super(CBASBucketOperations, self).setUp()

    def tearDown(self):
        super(CBASBucketOperations, self).tearDown()

    def setup_for_test(self, skip_data_loading=False):
        if not skip_data_loading:
            # Load Couchbase bucket first.
            self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0,
                                                    self.num_items)

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

        if not skip_data_loading:
            # Validate no. of items in CBAS dataset
            if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items):
                self.fail(
                    "No. of items in CBAS dataset do not match that in the CB bucket")

    def load_docs_in_cb_bucket_before_cbas_connect(self):
        self.setup_for_test()

    def load_docs_in_cb_bucket_before_and_after_cbas_connect(self):
        self.setup_for_test()

        # Load more docs in Couchbase bucket.
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def load_docs_in_cb_bucket_after_cbas_connect(self):
        self.setup_for_test(skip_data_loading=True)

        # Load Couchbase bucket first.
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0,
                                               self.num_items)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def delete_some_docs_in_cb_bucket(self):
        self.setup_for_test(skip_data_loading=True)

        # Delete some docs in Couchbase bucket.
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "delete", 0,
                                               self.num_items / 2)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items / 2):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def delete_all_docs_in_cb_bucket(self):
        self.setup_for_test(skip_data_loading=True)

        # Delete all docs in Couchbase bucket.
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "delete", 0,
                                               self.num_items)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def update_some_docs_in_cb_bucket(self):
        self.setup_for_test(skip_data_loading=True)

        # Update some docs in Couchbase bucket
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "update", 0,
                                               self.num_items / 10)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items,
                                                      self.num_items / 10):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def update_all_docs_in_cb_bucket(self):
        self.setup_for_test(skip_data_loading=False)

        # Update all docs in Couchbase bucket
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "update", 0,
                                               self.num_items)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items,
                                                      self.num_items):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def create_update_delete_cb_bucket_then_cbas_connect(self):
        self.setup_for_test(skip_data_loading=True)

        # Disconnect from bucket
        self.disconnect_from_bucket(self.cbas_bucket_name)

        # Perform Create, Update, Delete ops in the CB bucket
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "update", 0,
                                               self.num_items)
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "delete", 0,
                                               self.num_items / 2)

        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 3 / 2,
                                                      self.num_items / 2):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def create_update_delete_cb_bucket_with_cbas_connected(self):
        self.setup_for_test(skip_data_loading=True)

        # Perform Create, Update, Delete ops in the CB bucket
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create",
                                               self.num_items,
                                               self.num_items * 2)
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "update", 0,
                                               self.num_items)
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "delete", 0,
                                               self.num_items / 2)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 3 / 2,
                                                      self.num_items / 2):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def flush_cb_bucket_with_cbas_connected(self):
        self.setup_for_test(skip_data_loading=True)

        # Flush the CB bucket
        self.cluster.bucket_flush(server=self.master,
                                  bucket=self.cb_bucket_name)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def flush_cb_bucket_then_cbas_connect(self):
        self.setup_for_test(skip_data_loading=True)

        # Disconnect from bucket
        self.disconnect_from_bucket(self.cbas_bucket_name)

        # Flush the CB bucket
        self.cluster.bucket_flush(server=self.master,
                                  bucket=self.cb_bucket_name)

        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def delete_cb_bucket_with_cbas_connected(self):
        self.setup_for_test(skip_data_loading=True)

        # Delete the CB bucket
        self.cluster.bucket_delete(server=self.master,
                                   bucket=self.cb_bucket_name)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def delete_cb_bucket_then_cbas_connect(self):
        self.setup_for_test(skip_data_loading=True)

        # Disconnect from bucket
        self.disconnect_from_bucket(self.cbas_bucket_name)

        # Delete the CB bucket
        self.cluster.bucket_delete(server=self.master,
                                   bucket=self.cb_bucket_name)

        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def compact_cb_bucket_with_cbas_connected(self):
        self.setup_for_test(skip_data_loading=True)

        # Compact the CB bucket
        self.cluster.compact_bucket(server=self.master,
                                    bucket=self.cb_bucket_name)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def compact_cb_bucket_then_cbas_connect(self):
        self.setup_for_test(skip_data_loading=True)

        # Disconnect from bucket
        self.disconnect_from_bucket(self.cbas_bucket_name)

        # Compact the CB bucket
        self.cluster.compact_bucket(server=self.master,
                                    bucket=self.cb_bucket_name)

        # Connect to Bucket
        self.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        # Validate no. of items in CBAS dataset
        if not self.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")
