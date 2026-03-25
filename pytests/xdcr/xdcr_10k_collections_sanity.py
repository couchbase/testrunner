
from .xdcrnewbasetests import XDCRNewBaseTest, NodeHelper
from membase.helper.bucket_helper import BucketOperationHelper

from pytests.xdcr.tenK_collection_helper import TenKCollectionHelper


class XDCR10KCollectionsSanity(XDCRNewBaseTest):

    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name("C1")
        self.dest_cluster = self.get_cb_cluster_by_name("C2")
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.p = TenKCollectionHelper.read_10k_params(self._input)

    def tearDown(self):
        super().tearDown()

    # ---- Test A.1 ----
    def test_create_10k_collections_manifest_src_dest(self):
        bucket_name = self._input.param("bucket_name", "default")
        expected_total = self.p["num_scopes"] * self.p["collections_per_scope"]

        self.log.info("Creating {} collections on source cluster".format(expected_total))
        src_status = TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: self.p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        self.assertTrue(src_status, "Failed to create 10K collections on source")

        self.log.info("Creating {} collections on destination cluster".format(expected_total))
        dest_status = TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: self.p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        self.assertTrue(dest_status, "Failed to create 10K collections on destination")

        src_count = TenKCollectionHelper.get_manifest_collection_count(
            self.src_master, bucket_name)
        self.log.info("Manifest on {}: {} collections (expected >= {})".format(
            self.src_master.ip, src_count, expected_total))
        self.assertGreaterEqual(src_count, expected_total,
                                "Source manifest has fewer collections than expected: {} < {}".format(
                                    src_count, expected_total))

        dest_count = TenKCollectionHelper.get_manifest_collection_count(
            self.dest_master, bucket_name)
        self.log.info("Manifest on {}: {} collections (expected >= {})".format(
            self.dest_master.ip, dest_count, expected_total))
        self.assertGreaterEqual(dest_count, expected_total,
                                "Destination manifest has fewer collections than expected: {} < {}".format(
                                    dest_count, expected_total))

        self.log.info("10K collections manifest verified on both clusters "
                      "(src={}, dest={})".format(src_count, dest_count))

    # ---- Test A.2 ----
    def test_xdcr_implicit_mapping_10k_collections_smoke(self):
        bucket_name = self._input.param("bucket_name", "default")

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: self.p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: self.p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()

        result = TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, self.p, run_id="smoke")
        self.assertTrue(result.success_rate > 0.9,
                        "Too many load failures: {}/{}".format(
                            len(result.failed_pairs), result.total_attempted))

        self._wait_for_collection_manifest_sync(
            self.src_master, self.dest_master, bucket_name,
            timeout=self._input.param("manifest_sync_timeout", 180))

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 600))
        except Exception as e:
            self.fail("Replication catchup wait status: {}".format(e))

        self.log.info("Implicit mapping smoke test with 10K collections passed")

    # ---- Test B.1 ----
    def test_pause_resume_xdcr_with_10k_collections(self):
        bucket_name = self._input.param("bucket_name", "default")

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: self.p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: self.p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()

        TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, self.p, run_id="pause_pre")

        self.sleep(15, "Allowing initial replication before pause")

        self.log.info("Pausing all replications")
        for remote_cluster_ref in self.src_cluster.get_remote_clusters():
            remote_cluster_ref.pause_all_replications(verify=True)

        self.sleep(10, "Replication paused, loading more data")

        TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, self.p, run_id="pause_extra",
            sample_size=min(50, self.p["sample_collections"]))

        self.log.info("Resuming all replications")
        for remote_cluster_ref in self.src_cluster.get_remote_clusters():
            remote_cluster_ref.resume_all_replications(verify=True)

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 600))
        except Exception as e:
            self.fail("Replication catchup status wait after resume: {}".format(e))

        self.log.info("Pause/resume test with 10K collections passed")

    # ---- Test B.2 ----
    def test_restart_goxdcr_with_10k_collections(self):
        bucket_name = self._input.param("bucket_name", "default")

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: self.p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: self.p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()

        TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, self.p, run_id="goxdcr_pre")

        self.sleep(15, "Allowing initial replication before goxdcr kill")

        self.log.info("Killing goxdcr on source master")
        NodeHelper.kill_goxdcr(self.src_master)

        self.sleep(30, "Waiting for goxdcr to restart and pipelines to recover")

        TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, self.p, run_id="goxdcr_post",
            sample_size=min(50, self.p["sample_collections"]))

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 600))
        except Exception as e:
            self.fail("Replication catchup wait after goxdcr restart: {}".format(e))

        self.log.info("goxdcr restart test with 10K collections passed")

    # ---- Test C.1 ----
    def test_verify_replication_correctness_10k_collections(self):
        """
        Verify that implicit mapping is correct across 10K collections by
        comparing per-collection doc counts on source vs destination, ensuring
        each collection's data was replicated to the matching namespace.
        """
        bucket_name = self._input.param("bucket_name", "default")

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: self.p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: self.p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()

        result = TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, self.p,
            run_id="correctness", mode="all")
        self.assertTrue(result.success_rate > 0.9,
                        "Too many load failures: {}/{}".format(
                            len(result.failed_pairs), result.total_attempted))

        self._wait_for_replication_to_catchup(
            timeout=self._input.param("wait_timeout", 900))

        src_manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
            self.src_master, bucket_name)
        dest_manifest = BucketOperationHelper.get_api_manifest_json_from_bucket(
            self.dest_master, bucket_name)

        src_scope_names = {s["name"] for s in src_manifest.get("scopes", [])
                          if not s["name"].startswith("_")}
        dest_scope_names = {s["name"] for s in dest_manifest.get("scopes", [])
                           if not s["name"].startswith("_")}

        missing_scopes = src_scope_names - dest_scope_names
        if missing_scopes:
            self.log.warning("Scopes on source but not dest: {}".format(
                list(missing_scopes)[:20]))

        src_col_counts = TenKCollectionHelper.get_collection_doc_counts(
            self.src_cluster.get_nodes(), bucket_name)
        dest_col_counts = TenKCollectionHelper.get_collection_doc_counts(
            self.dest_cluster.get_nodes(), bucket_name)

        mismatches = []
        verified = 0
        for scope_name, col_name in result.success_pairs:
            key = (scope_name, col_name)
            src_c = src_col_counts.get(key, 0)
            dest_c = dest_col_counts.get(key, 0)
            if src_c != dest_c:
                mismatches.append((scope_name, col_name, src_c, dest_c))
            verified += 1

        if mismatches:
            for scope, col, s, d in mismatches[:20]:
                self.log.error("Correctness mismatch in {}.{}: src={}, dest={}".format(
                    scope, col, s, d))
            self.fail("Replication correctness failure: {}/{} collections have "
                      "mismatched counts".format(len(mismatches), verified))

        self.log.info("Replication correctness verified: {}/{} collections match "
                      "between source and destination".format(verified, verified))
