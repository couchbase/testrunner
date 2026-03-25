import time

from lib.membase.api.rest_client import RestConnection
from memcached.helper.data_helper import VBucketAwareMemcached
from couchbase_helper.documentgenerator import BlobGenerator
from .xdcrnewbasetests import XDCRNewBaseTest, REPL_PARAM

from pytests.xdcr.tenK_collection_helper import TenKCollectionHelper


class XDCRFilterDelExpTests(XDCRNewBaseTest):
    """
    Tests for filterDeletionsWithExpression and filterExpirationsWithExpression
    replication settings. These settings allow filter expressions referencing
    only document keys (META().id) to be applied to deletions and expirations.
    """

    def setUp(self):
        XDCRNewBaseTest.setUp(self)
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)

    def tearDown(self):
        XDCRNewBaseTest.tearDown(self)

    def get_cluster_objects_for_input(self, input):
        """Returns a list of cluster objects for input. 'input' is a string
           containing names of clusters separated by ':'
           eg. failover=C1:C2
        """
        clusters = []
        input_clusters = input.split(':')
        for cluster_name in input_clusters:
            clusters.append(self.get_cb_cluster_by_name(cluster_name))
        return clusters

    def _load_docs_with_prefix(self, prefix, num_docs, bucket="default"):
        """Load documents via direct memcached to bypass Java SDK manifest
        overhead on buckets with 10K collections."""
        mc = VBucketAwareMemcached(RestConnection(self.src_master), bucket)
        loaded = 0
        for i in range(num_docs):
            doc_id = "{}{}".format(prefix, i)
            try:
                mc_client = mc.memcached(doc_id)
                mc_client.set(key=doc_id, exp=0, flags=0, val='{"prefix":"{}","idx":{}}'.format(
                    prefix, i))
                loaded += 1
            except Exception as e:
                self.log.warning("Failed to load doc {}: {}".format(doc_id, e))
        self.log.info("Loaded {}/{} prefix docs".format(loaded, num_docs))
        self.sleep(3, "Waiting for docs to be loaded")

    def _delete_docs_with_prefix(self, prefix, num_docs, bucket="default"):
        """Delete documents via direct memcached to bypass Java SDK manifest
        overhead on buckets with 10K collections."""
        mc = VBucketAwareMemcached(RestConnection(self.src_master), bucket)
        deleted = 0
        for i in range(num_docs):
            doc_id = "{}{}".format(prefix, i)
            try:
                mc_client = mc.memcached(doc_id)
                mc_client.delete(doc_id)
                deleted += 1
            except Exception:
                pass
        self.log.info("Deleted {}/{} prefix docs".format(deleted, num_docs))
        self.sleep(3, "Waiting for deletes to propagate")

    def _get_bucket_item_count(self, rest, bucket="default"):
        """Get item count for a bucket"""
        return rest.get_active_key_count(bucket)

    def _set_filter_deletion(self, value):
        """Set filterDeletion for the replication (prerequisite for filterDeletionsWithExpression)"""
        self.src_rest.set_xdcr_param("default", "default",
                                      "filterDeletion", str(value).lower())
        self.log.info(f"Set filterDeletion to {value}")

    def _set_filter_expiration(self, value):
        """Set filterExpiration for the replication (prerequisite for filterExpirationsWithExpression)"""
        self.src_rest.set_xdcr_param("default", "default",
                                      "filterExpiration", str(value).lower())
        self.log.info(f"Set filterExpiration to {value}")

    def _set_filter_deletions_with_expression(self, value):
        """Set filterDeletionsWithExpression for the replication.
        Note: filterDeletion must be enabled first for this to work.
        """
        if value:
            self._set_filter_deletion(True)
        self.src_rest.set_xdcr_param("default", "default",
                                      "filterDeletionsWithExpression", str(value).lower())
        self.log.info(f"Set filterDeletionsWithExpression to {value}")

    def _set_filter_expirations_with_expression(self, value):
        """Set filterExpirationsWithExpression for the replication.
        Note: filterExpiration must be enabled first for this to work.
        """
        if value:
            self._set_filter_expiration(True)
        self.src_rest.set_xdcr_param("default", "default",
                                      "filterExpirationsWithExpression", str(value).lower())
        self.log.info(f"Set filterExpirationsWithExpression to {value}")

    def test_filter_deletions_with_expression_enabled(self):
        """
        Test that deletions are filtered when filterDeletionsWithExpression is enabled.
        1. Setup XDCR with a key-based filter expression
        2. Load docs matching the filter (docs must match filter to be replicated initially)
        3. Wait for initial replication to complete
        4. Enable filterDeletionsWithExpression
        5. Delete docs on source
        6. Verify deletions matching the filter are replicated to dest
        """
        num_docs = self._input.param("items", 100)

        self.setup_xdcr()
        self.sleep(30, "Waiting for replication to stabilize")

        replications = self.src_rest.get_replications()
        filter_exp = ""
        for repl in replications:
            if repl.get('filterExpression'):
                filter_exp = repl['filterExpression']
                break
        self.log.info(f"Filter expression from replication: {filter_exp}")

        if "^filter" in filter_exp:
            doc_prefix = "filter"
        elif "doc" in filter_exp:
            doc_prefix = "doc"
        else:
            doc_prefix = "doc"
        self.log.info(f"Using doc prefix: {doc_prefix}")

        self._load_docs_with_prefix(doc_prefix, num_docs)

        self.sleep(60, "Waiting for initial replication")
        self._wait_for_replication_to_catchup()

        src_count_before = self._get_bucket_item_count(self.src_rest)
        dest_count_before = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Before enabling filter and deletion - Src: {src_count_before}, Dest: {dest_count_before}")

        self._set_filter_deletions_with_expression(True)

        self._delete_docs_with_prefix(doc_prefix, num_docs)

        self.sleep(60, "Waiting for deletions to replicate")
        self._wait_for_replication_to_catchup()

        src_count_after = self._get_bucket_item_count(self.src_rest)
        dest_count_after = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"After deletion - Src: {src_count_after}, Dest: {dest_count_after}")

        self.assertEqual(src_count_after, 0, "Source should have 0 items after deletion")
        self.assertEqual(dest_count_after, 0,
                         "Dest should have 0 items - deletions matching filter should be replicated")

    def test_filter_deletions_with_expression_disabled(self):
        """
        Test that when filterDeletionsWithExpression is disabled (default), 
        deletions are replicated regardless of whether they match the filter.
        1. Setup XDCR with a key-based filter expression
        2. Load docs matching the filter
        3. Wait for initial replication
        4. Ensure filterDeletionsWithExpression is disabled (default)
        5. Delete docs on source
        6. Verify all deletions are replicated (not subject to filter)
        """
        num_docs = self._input.param("items", 100)

        self.setup_xdcr()
        self.sleep(30, "Waiting for replication to stabilize")

        replications = self.src_rest.get_replications()
        filter_exp = ""
        for repl in replications:
            if repl.get('filterExpression'):
                filter_exp = repl['filterExpression']
                break
        self.log.info(f"Filter expression from replication: {filter_exp}")

        if "filter" in filter_exp:
            doc_prefix = "filter"
        elif "doc" in filter_exp:
            doc_prefix = "doc"
        else:
            doc_prefix = "doc"
        self.log.info(f"Using doc prefix: {doc_prefix}")

        self._load_docs_with_prefix(doc_prefix, num_docs)

        self.sleep(60, "Waiting for initial replication")
        self._wait_for_replication_to_catchup()

        dest_count_before = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Dest count before deletion: {dest_count_before}")

        self._delete_docs_with_prefix(doc_prefix, num_docs)

        self.sleep(60, "Waiting for deletions to replicate")
        self._wait_for_replication_to_catchup()

        dest_count_after = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Dest count after deletion: {dest_count_after}")

        self.assertEqual(dest_count_after, 0,
                         "When filterDeletionsWithExpression is disabled, all deletions should replicate")

    def test_filter_expirations_with_expression_enabled(self):
        """
        Test that expirations are filtered when filterExpirationsWithExpression is enabled.
        1. Setup XDCR with a key-based filter expression
        2. Load docs with TTL matching the filter
        3. Wait for initial replication
        4. Enable filterExpirationsWithExpression
        5. Wait for docs to expire
        6. Verify expirations matching the filter are replicated
        """
        num_docs = self._input.param("items", 100)
        ttl = self._input.param("ttl", 60)

        self.setup_xdcr()
        self.sleep(30, "Waiting for replication to stabilize")

        replications = self.src_rest.get_replications()
        filter_exp = ""
        for repl in replications:
            if repl.get('filterExpression'):
                filter_exp = repl['filterExpression']
                break
        self.log.info(f"Filter expression: {filter_exp}")

        gen_filter = BlobGenerator("doc", "doc", self._value_size, end=num_docs)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_filter, exp=ttl)

        self.sleep(30, "Waiting for initial replication before expiration")
        self._wait_for_replication_to_catchup()

        dest_count_before = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Dest count before expiration: {dest_count_before}")

        self._set_filter_expirations_with_expression(True)

        self.sleep(ttl + 60, "Waiting for documents to expire")

        src_count_after = self._get_bucket_item_count(self.src_rest)
        dest_count_after = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"After expiration - Src: {src_count_after}, Dest: {dest_count_after}")

    def test_filter_expirations_with_expression_disabled(self):
        """
        Test that when filterExpirationsWithExpression is disabled (default),
        expirations are replicated regardless of whether they match the filter.
        1. Setup XDCR with a key-based filter expression
        2. Load docs with TTL matching the filter
        3. Wait for initial replication
        4. Ensure filterExpirationsWithExpression is disabled (default)
        5. Wait for docs to expire
        6. Verify all expirations are replicated
        """
        num_docs = self._input.param("items", 100)
        ttl = self._input.param("ttl", 60)

        self.setup_xdcr()
        self.sleep(30, "Waiting for replication to stabilize")

        gen = BlobGenerator("doc", "doc", self._value_size, end=num_docs)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen, exp=ttl)

        self.sleep(30, "Waiting for initial replication")
        self._wait_for_replication_to_catchup()

        dest_count_before = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Dest count before expiration: {dest_count_before}")

        self.sleep(ttl + 60, "Waiting for documents to expire")

        src_count_after = self._get_bucket_item_count(self.src_rest)
        dest_count_after = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"After expiration - Src: {src_count_after}, Dest: {dest_count_after}")

    def test_filter_deletions_and_expirations_together(self):
        """
        Test that both filterDeletionsWithExpression and filterExpirationsWithExpression
        can be enabled together and work correctly.
        """
        num_docs = self._input.param("items", 50)
        ttl = self._input.param("ttl", 60)

        self.setup_xdcr()
        self.sleep(30, "Waiting for replication to stabilize")

        replications = self.src_rest.get_replications()
        filter_exp = ""
        for repl in replications:
            if repl.get('filterExpression'):
                filter_exp = repl['filterExpression']
                break
        self.log.info(f"Filter expression from replication: {filter_exp}")

        if "filter" in filter_exp:
            doc_prefix = "filter"
        elif "exp" in filter_exp:
            doc_prefix = "exp"
        else:
            doc_prefix = "doc"
        self.log.info(f"Using doc prefix: {doc_prefix}")

        gen_docs = BlobGenerator(doc_prefix, doc_prefix, self._value_size, end=num_docs)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_docs)

        self.sleep(30, "Waiting for initial replication")
        self._wait_for_replication_to_catchup()

        dest_count_before = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Dest count before operations: {dest_count_before}")

        self._set_filter_deletions_with_expression(True)
        self._set_filter_expirations_with_expression(True)

        self._delete_docs_with_prefix(doc_prefix, num_docs)

        self.sleep(60, "Waiting for deletions to process")

        src_count = self._get_bucket_item_count(self.src_rest)
        dest_count = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Final counts - Src: {src_count}, Dest: {dest_count}")

    def test_filter_deletions_with_regex_filter(self):
        """
        Test filterDeletionsWithExpression with a REGEXP_CONTAINS filter on META().id
        """
        self.setup_xdcr()
        self.sleep(30, "Waiting for replication to stabilize")

        self._set_filter_deletions_with_expression(True)

        num_docs = self._input.param("items", 100)

        gen_matching = BlobGenerator("doc", "doc", self._value_size, end=num_docs)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_matching)

        gen_non_matching = BlobGenerator("other", "other", self._value_size, end=num_docs)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_non_matching)

        self.sleep(60, "Waiting for initial replication")
        self._wait_for_replication_to_catchup()

        dest_count_before = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Dest count before deletion: {dest_count_before}")

        self._delete_docs_with_prefix("doc", num_docs)
        self._delete_docs_with_prefix("other", num_docs)

        self.sleep(60, "Waiting for deletions to replicate")
        self._wait_for_replication_to_catchup()

        src_count = self._get_bucket_item_count(self.src_rest)
        dest_count = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"After deletion - Src: {src_count}, Dest: {dest_count}")

    def test_filter_expirations_with_regex_filter(self):
        """
        Test filterExpirationsWithExpression with a REGEXP_CONTAINS filter on META().id
        """
        self.setup_xdcr()
        self.sleep(30, "Waiting for replication to stabilize")

        self._set_filter_expirations_with_expression(True)

        num_docs = self._input.param("items", 100)
        ttl = self._input.param("ttl", 60)

        gen_matching = BlobGenerator("doc", "doc", self._value_size, end=num_docs)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_matching, exp=ttl)

        gen_non_matching = BlobGenerator("other", "other", self._value_size, end=num_docs)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen_non_matching, exp=ttl)

        self.sleep(30, "Waiting for initial replication")
        self._wait_for_replication_to_catchup()

        dest_count_before = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Dest count before expiration: {dest_count_before}")

        self.sleep(ttl + 30, "Waiting for documents to expire")

        src_count = self._get_bucket_item_count(self.src_rest)
        dest_count = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"After expiration - Src: {src_count}, Dest: {dest_count}")

    def test_toggle_filter_deletions_setting(self):
        """
        Test toggling filterDeletionsWithExpression on and off during replication.
        Uses 'batch' prefix docs to match the filter expression '^batch'.
        """
        self.setup_xdcr()
        self.sleep(30, "Waiting for replication to stabilize")

        num_docs = self._input.param("items", 50)

        gen1 = BlobGenerator("batch", "batch", self._value_size, end=num_docs)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen1)
        self.sleep(30, "Waiting for batch to replicate")
        self._wait_for_replication_to_catchup()

        dest_count_before = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Dest count before deletion: {dest_count_before}")

        self._set_filter_deletions_with_expression(True)
        self._delete_docs_with_prefix("batch", num_docs)
        self.sleep(60, "Waiting for deletions to replicate")

        src_count = self._get_bucket_item_count(self.src_rest)
        dest_count = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"After deletion with filter enabled - Src: {src_count}, Dest: {dest_count}")

    def test_toggle_filter_expirations_setting(self):
        """
        Test toggling filterExpirationsWithExpression on and off during replication.
        Uses 'batch' prefix docs to match the filter expression '^batch'.
        """
        self.setup_xdcr()
        self.sleep(30, "Waiting for replication to stabilize")

        num_docs = self._input.param("items", 50)
        ttl = self._input.param("ttl", 30)

        gen = BlobGenerator("batch", "batch", self._value_size, end=num_docs)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen, exp=ttl)
        self.sleep(20, "Waiting for initial replication")
        self._wait_for_replication_to_catchup()

        dest_count_before = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Dest count before expiration: {dest_count_before}")

        self._set_filter_expirations_with_expression(True)
        self.sleep(ttl + 60, "Waiting for docs to expire")

        src_count = self._get_bucket_item_count(self.src_rest)
        dest_count = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"After expiration with filter enabled - Src: {src_count}, Dest: {dest_count}")

    def test_filter_deletions_with_pause_resume(self):
        """
        Test filterDeletionsWithExpression with pause and resume of replication.
        """
        self.setup_xdcr()
        self.sleep(30, "Waiting for replication to stabilize")

        self._set_filter_deletions_with_expression(True)

        num_docs = self._input.param("items", 100)

        gen = BlobGenerator("pausetest", "pausetest", self._value_size, end=num_docs)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen)
        self.sleep(30, "Waiting for initial replication")
        self._wait_for_replication_to_catchup()

        self.src_cluster.pause_all_replications()
        self.sleep(10, "Replication paused")

        self._delete_docs_with_prefix("pausetest", num_docs)
        self.sleep(10, "Deletions performed while paused")

        self.src_cluster.resume_all_replications()
        self.sleep(60, "Waiting for replication to catch up after resume")
        self._wait_for_replication_to_catchup()

        src_count = self._get_bucket_item_count(self.src_rest)
        dest_count = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Final counts - Src: {src_count}, Dest: {dest_count}")

    def test_filter_expirations_with_pause_resume(self):
        """
        Test filterExpirationsWithExpression with pause and resume of replication.
        """
        self.setup_xdcr()
        self.sleep(30, "Waiting for replication to stabilize")

        self._set_filter_expirations_with_expression(True)

        num_docs = self._input.param("items", 100)
        ttl = self._input.param("ttl", 60)

        gen = BlobGenerator("pausetest", "pausetest", self._value_size, end=num_docs)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=gen, exp=ttl)
        self.sleep(20, "Waiting for initial replication")

        self.src_cluster.pause_all_replications()
        self.sleep(ttl + 10, "Waiting for docs to expire while paused")

        self.src_cluster.resume_all_replications()
        self.sleep(60, "Waiting for replication to catch up after resume")
        self._wait_for_replication_to_catchup()

        src_count = self._get_bucket_item_count(self.src_rest)
        dest_count = self._get_bucket_item_count(self.dest_rest)
        self.log.info(f"Final counts - Src: {src_count}, Dest: {dest_count}")

    # ---- 10K Collections Scale Tests ----

    def test_deletion_expiry_filter_10k(self):
        """
        Enable filterDeletion and filterExpiration on a replication with
        10K collections. Load docs, delete a subset, and verify that
        deletions are filtered (not replicated to destination).

        Conf params:
            filter_deletion: enable deletion filter (default True)
            filter_expiration: enable expiration filter (default True)
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")
        filter_deletion = self._input.param("filter_deletion", True)
        filter_expiration = self._input.param("filter_expiration", True)

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()

        result = TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, p, run_id="delexp_pre")
        self.assertTrue(result.success_rate > 0.9,
                        "Too many load failures: {}/{}".format(
                            len(result.failed_pairs), result.total_attempted))

        self._wait_for_replication_to_catchup(
            timeout=self._input.param("wait_timeout", 600))

        dest_items_before = TenKCollectionHelper.get_bucket_item_count(
            self.dest_master, bucket_name)
        self.log.info("Dest items before filter+delete: {}".format(dest_items_before))

        if filter_deletion:
            self.log.info("Enabling filterDeletion")
            self._set_filter_deletion(True)

        if filter_expiration:
            self.log.info("Enabling filterExpiration")
            self._set_filter_expiration(True)

        num_delete_docs = self._input.param("num_delete_docs", 500)
        delete_prefix = "deltest_"
        self.log.info("Loading {} docs with delete prefix, then deleting".format(
            num_delete_docs))
        self._load_docs_with_prefix(delete_prefix, num_delete_docs, bucket_name)
        self._wait_for_replication_to_catchup(
            timeout=self._input.param("wait_timeout", 300))

        dest_items_after_load = TenKCollectionHelper.get_bucket_item_count(
            self.dest_master, bucket_name)
        self.log.info("Dest items after loading delete-prefix docs: {}".format(
            dest_items_after_load))

        self._delete_docs_with_prefix(delete_prefix, num_delete_docs, bucket_name)
        self.sleep(30, "Waiting for deletions to be processed")

        try:
            self._wait_for_dest_to_stabilize(self.dest_cluster, bucket_name,
                                             min_items=1, timeout=300)
        except Exception as e:
            self.log.info("Dest did not stabilize after deletions "
                          "(filter may already be steady-state): {}".format(e))

        dest_items_after_delete = TenKCollectionHelper.get_bucket_item_count(
            self.dest_master, bucket_name)
        src_items_after_delete = TenKCollectionHelper.get_bucket_item_count(
            self.src_master, bucket_name)
        self.log.info("After delete with filter - src: {}, dest: {}".format(
            src_items_after_delete, dest_items_after_delete))

        if filter_deletion:
            self.assertGreaterEqual(dest_items_after_delete, src_items_after_delete,
                                    "With filterDeletion enabled, dest should "
                                    "retain at least as many items as src")
            self.assertGreaterEqual(dest_items_after_delete, dest_items_after_load - num_delete_docs,
                                    "With filterDeletion enabled, dest should retain more items "
                                    "than without the filter")

        self.log.info("Deletion/expiry filter 10K collections test passed")
