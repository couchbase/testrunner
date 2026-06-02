"""
fts_encryption_at_rest.py: Tests for FTS (Search) with Encryption at Rest enabled.

This module holds only test_* methods. All encryption setUp/tearDown and helper /
wrapper methods live in FTSEncryptionBaseTest (fts_encryption_base.py).

__author__ = "Dananjay S"
__created_on__ = "02/06/26"
"""

import time

from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from .fts_encryption_base import FTSEncryptionBaseTest


class FTSEncryptionAtRest(FTSEncryptionBaseTest):

    def test_fts_encryption_at_rest_sanity(self):
        """
        End-to-end FTS encryption-at-rest sanity:
          1. Verify FTS nodes + encryption enabled
          2. Load data and create a default FTS index
          3. Run a term query and a match_all query (queries work under encryption)
          4. Validate .zap segments + metadata files are encrypted on disk
          5. Negative check: cbft.uuid NOT encrypted (index_meta.json IS encrypted)
          6. pindex directory names do not leak sensitive names
        """
        self.log.info("=" * 80)
        self.log.info("STARTING TEST: test_fts_encryption_at_rest_sanity")
        self.log.info("=" * 80)

        # ===== STEP 1: prerequisites =====
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.log.info(f"[STEP 1] FTS nodes: {[n.ip for n in fts_nodes]}")
        self.assertGreaterEqual(len(fts_nodes), 1, "Need at least 1 FTS node")
        if not self.enable_encryption_at_rest:
            self.skipTest("Encryption at rest not enabled. Set enable_encryption_at_rest=True")
        self.assertIsNotNone(self.encryption_at_rest_id, "encryption_at_rest_id not set")

        # ===== STEP 2: load data + create index =====
        self.log.info(f"[STEP 2] Loading {self._num_items} documents and creating FTS index")
        self.load_data(num_items=self._num_items)
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name(self.default_bucket_name),
            index_name="fts_ear_idx",
        )
        self.wait_for_indexing_complete()
        self.log.info("[STEP 2] PASSED - index built")

        # ===== STEP 3: queries work under encryption =====
        self.log.info("[STEP 3] Running queries against the encrypted index")
        match_all_hits, _, _, _ = index.execute_query(
            {"match_all": {}}, zero_results_ok=False
        )
        self.assertGreater(match_all_hits, 0, "match_all returned 0 hits on encrypted index")
        term_hits, _, _, _ = index.execute_query(
            {"match": "Safiya Morgan", "field": "name"}, zero_results_ok=True
        )
        self.log.info(f"[STEP 3] PASSED - match_all hits={match_all_hits}, term hits={term_hits}")

        # ===== STEP 4: on-disk encryption validation =====
        self.log.info("[STEP 4] Validating .zap segment encryption on disk")
        seg_results = self.encryption_helper.verify_fts_segment_files_encrypted(
            fts_nodes,
            sensitive_terms=self.sensitive_terms,
            expected_key_ids=[self.encryption_at_rest_id],
        )
        self._assert_encryption_results(seg_results, "segments")

        self.log.info("[STEP 4] Validating FTS metadata file encryption on disk")
        meta_results = self.encryption_helper.verify_fts_metadata_files_encrypted(
            fts_nodes, sensitive_terms=self.sensitive_terms
        )
        self._assert_encryption_results(meta_results, "metadata")
        self.log.info("[STEP 4] PASSED - segments and metadata encrypted")

        # ===== STEP 4b: fts.log encryption (only when log encryption is enabled) =====
        if self.enable_log_encryption_at_rest:
            self._validate_fts_log_encryption(fts_nodes, step="[STEP 4b] ")
        else:
            self.log.info("[STEP 4b] SKIPPED - log encryption not enabled")

        # ===== STEP 5: negative checks (must NOT be encrypted) =====
        # Only cbft.uuid is plaintext. index_meta.json IS encrypted (same as PINDEX_META,
        # per dev) and is validated by verify_fts_metadata_files_encrypted in STEP 4.
        self.log.info("[STEP 5] Validating cbft.uuid is NOT encrypted")
        for node in fts_nodes:
            for file_name in ("cbft.uuid",):
                result = self.encryption_helper.verify_fts_file_not_encrypted(node, file_name)
                self.assertNotEqual(
                    result.get("status"), "failed",
                    f"{file_name} unexpectedly encrypted on {node.ip}: {result}",
                )
                self.log.info(f"[STEP 5] {node.ip} {file_name}: {result.get('status')}")
        self.log.info("[STEP 5] PASSED - negative checks ok")

        # ===== STEP 6: pindex directory names sanitized =====
        self.log.info("[STEP 6] Validating pindex directory names do not leak sensitive names")
        forbidden_tokens = [self.default_bucket_name, "fts_ear_idx", self.scope, "_default"]
        for node in fts_nodes:
            result = self.encryption_helper.verify_fts_pindex_dir_names_sanitized(
                node, forbidden_tokens
            )
            self.assertNotEqual(
                result.get("status"), "failed",
                f"pindex dir names leak sensitive tokens on {node.ip}: {result}",
            )
            self.log.info(f"[STEP 6] {node.ip}: {result.get('status')} ({result})")
        self.log.info("[STEP 6] PASSED - directory names sanitized")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: test_fts_encryption_at_rest_sanity")
        self.log.info("=" * 80)

    def test_fts_ear_toggle_disable_enable(self):
        """Toggle encryption OFF then ON and confirm segments follow.

        Encrypted baseline -> disable bucket encryption -> force merge + new flushes ->
        segments are rewritten as PLAINTEXT (negative validation) -> re-enable -> force
        merge + new flushes -> segments encrypted again.
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        bucket_name = self.default_bucket_name

        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_toggle_idx")
        self.assertGreater(self._match_all_hits(index), 0, "no hits on encrypted index")
        self._validate_segments_encrypted(fts_nodes, "segments(baseline-encrypted)")

        # --- disable ---
        self.log.info("Disabling bucket encryption")
        rest = RestConnection(self.master)
        status, resp = rest.disable_bucket_encryption(bucket_name)
        self.assertTrue(status, f"disable_bucket_encryption failed: {resp}")
        self._run_emp_data_op(max(1000, self._num_items // 10), "update", start=0, end=max(1000, self._num_items // 10))
        self._force_merge_and_wait(index.name)
        self.wait_for_indexing_complete()
        self.sleep(30, "letting segments rewrite without encryption")
        dec_results = self.encryption_helper.verify_fts_segment_files_not_encrypted(
            fts_nodes, sensitive_terms=self.sensitive_terms
        )
        self._assert_encryption_results(dec_results, "segments(after-disable-plaintext)")
        # GetInUseKeys should now report the unencrypted ("") marker for the bucket.
        merged_off, _ = self._fts_in_use_deks(bucket_name)
        self.assertTrue(
            self.encryption_helper.fts_has_unencrypted_marker(
                merged_off, f"service_bucket {self._bucket_uuid(bucket_name)}"),
            "expected an unencrypted ('') marker in GetInUseKeys after disabling encryption",
        )

        # --- re-enable ---
        self.log.info("Re-enabling bucket encryption")
        self._enable_encryption_on_buckets([bucket_name])
        self._run_emp_data_op(max(1000, self._num_items // 10), "update", start=0, end=max(1000, self._num_items // 10))
        self._force_merge_and_wait(index.name)
        self.wait_for_indexing_complete()
        self.sleep(30, "letting segments rewrite with encryption")
        self._validate_segments_encrypted(fts_nodes, "segments(after-reenable-encrypted)")
        self.assertGreater(self._match_all_hits(index), 0, "no hits after re-enable")
        # The unencrypted ("") marker should clear once everything is re-encrypted.
        merged_on, deks_on = self._fts_in_use_deks(bucket_name)
        self.assertTrue(deks_on, "no in-use DEK for the bucket after re-enabling encryption")
        self.assertFalse(
            self.encryption_helper.fts_has_unencrypted_marker(
                merged_on, f"service_bucket {self._bucket_uuid(bucket_name)}"),
            "unencrypted ('') marker still present after re-enabling encryption",
        )
        self.log.info("TEST PASSED: test_fts_ear_toggle_disable_enable")

    def test_fts_ear_force_reencryption(self):
        """Drop bucket DEKs (force re-encryption -> DEK rotation); a NEW DEK comes into
        use, the KEK stays the same, and data stays queryable and encrypted.

        Uses the FTS GetInUseKeys endpoint to prove the in-use DEK actually changed.
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        bucket_name = self.default_bucket_name

        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_reencrypt_idx")
        baseline_hits = self._match_all_hits(index)
        self.assertGreater(baseline_hits, 0, "no hits before re-encryption")
        self._validate_segments_encrypted(fts_nodes, "segments(before-reencryption)")

        # Baseline in-use DEK(s) for the bucket
        _, baseline_deks = self._fts_in_use_deks(bucket_name)
        self.assertTrue(baseline_deks, "GetInUseKeys returned no DEK for the bucket pre-reencryption")
        self._check_dek_in_segments(fts_nodes, baseline_deks, label="[pre] ")

        self.log.info(f"Triggering force re-encryption (drop DEKs) on '{bucket_name}'")
        rest = RestConnection(self.master)
        status, resp = rest.trigger_data_reencryption(bucket_name)
        self.assertTrue(status, f"trigger_data_reencryption failed: {resp}")

        # A new DEK must come into use; KEK (secret id) must stay the same
        new_deks = self._poll_fts_for_new_dek(bucket_name, baseline_deks, label="[reencrypt] ")
        kek_after = rest.get_bucket_json(bucket_name).get("encryptionAtRestKeyId")
        self.assertEqual(kek_after, self.encryption_at_rest_id,
                         f"KEK id changed unexpectedly during DEK re-encryption: {kek_after}")

        self._force_merge_and_wait(index.name)
        self.sleep(30, "letting re-encryption settle")

        self.assertEqual(self._match_all_hits(index), baseline_hits,
                         "hit count changed after re-encryption")
        self._validate_segments_encrypted(fts_nodes, "segments(after-reencryption)")
        self._check_dek_in_segments(fts_nodes, new_deks, label="[post] ")
        self.log.info("TEST PASSED: test_fts_ear_force_reencryption")

    def test_fts_ear_field_types(self):
        """Encrypted dynamic index over emp fields; query text and number field types.

        emp docs carry a text field ('name') and a numeric field ('mutated'); a dynamic
        index types them automatically. (datetime/vector field types are covered by the
        dedicated vector test / future date-dataset coverage -- emp has no date field.)
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()

        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_fieldtypes_idx")

        # text/dynamic
        self.assertGreater(self._match_all_hits(index), 0, "match_all returned 0 hits")
        # number field type: numeric range over 'mutated'
        num_hits, _, _, _ = index.execute_query(
            {"min": -1, "max": 100000000, "inclusive_min": True,
             "inclusive_max": True, "field": "mutated"},
            zero_results_ok=True,
        )
        self.log.info(f"numeric-range(mutated) hits={num_hits}")
        self.assertGreater(num_hits, 0, "numeric range query returned 0 hits")
        # text field type: term match on 'name' (best-effort -- value may not exist)
        txt_hits, _, _, _ = index.execute_query(
            {"match": "Safiya Morgan", "field": "name"}, zero_results_ok=True
        )
        self.log.info(f"text-match(name) hits={txt_hits}")

        self._validate_segments_encrypted(fts_nodes, "segments(field-types)")
        self.log.info("TEST PASSED: test_fts_ear_field_types")

    def test_fts_ear_mutations_and_bulk(self):
        """Bulk load + updates + deletes on an encrypted index; counts and segments hold."""
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()

        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_mutations_idx")
        initial = index.get_indexed_doc_count()
        self.assertEqual(initial, self._num_items, f"indexed {initial} != loaded {self._num_items}")

        batch = max(1000, self._num_items // 10)
        self.log.info(f"Updating {batch} docs")
        self._run_emp_data_op(batch, "update", start=0, end=batch)
        self.log.info(f"Deleting {batch} docs")
        self._run_emp_data_op(batch, "delete", start=batch, end=2 * batch)
        self.wait_for_indexing_complete()

        after = index.get_indexed_doc_count()
        self.log.info(f"indexed doc count: initial={initial}, after mutations={after}")
        self.assertEqual(after, initial - batch, "indexed count did not reflect deletes")
        self.assertGreater(self._match_all_hits(index), 0, "no hits after mutations")
        self._validate_segments_encrypted(fts_nodes, "segments(after-mutations)")
        self.log.info("TEST PASSED: test_fts_ear_mutations_and_bulk")

    def test_fts_ear_index_delete_cleanup(self):
        """Deleting an encrypted index removes its pindex partition dirs from disk."""
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()

        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_delete_idx")
        index_name = index.name
        self._validate_segments_encrypted(fts_nodes, "segments(before-delete)")

        before = {n.ip: self.encryption_helper.count_fts_pindex_dirs(n) for n in fts_nodes}
        self.log.info(f"pindex dir count before delete: {before}")
        self.assertTrue(any(c > 0 for c in before.values()), "no pindex dirs found before delete")

        self.log.info(f"Deleting index '{index_name}'")
        index.delete()

        deadline = time.time() + 120
        remaining = before
        while time.time() < deadline:
            remaining = {n.ip: self.encryption_helper.count_fts_pindex_dirs(n) for n in fts_nodes}
            if all(c == 0 for c in remaining.values()):
                break
            self.sleep(5, "waiting for pindex dirs to be removed from disk")
        self.log.info(f"pindex dir count after delete: {remaining}")
        self.assertTrue(all(c == 0 for c in remaining.values()),
                        f"pindex dirs not cleaned up after delete: {remaining}")
        self.assertTrue(self._cb_cluster.are_index_files_deleted_from_disk(index_name),
                        "are_index_files_deleted_from_disk reported leftover files")
        self.log.info("TEST PASSED: test_fts_ear_index_delete_cleanup")

    def test_fts_ear_kek_rotation_query_continuity(self):
        """KEK rotation does not interrupt queries; data stays encrypted under a valid DEK.

        KEK rotation re-wraps the DEKs (it does NOT necessarily generate new DEKs -- that
        is DEK rotation, covered by test_fts_ear_force_reencryption). So this test
        validates: query continuity across the rotation, segments stay encrypted, and the
        bucket still has an in-use DEK with no unencrypted ("") marker afterward. It also
        does the Q4 best-effort check that the in-use DEK id is grep-able in segments.
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        bucket_name = self.default_bucket_name

        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_rotation_idx")
        baseline_hits = self._match_all_hits(index)
        self.assertGreater(baseline_hits, 0, "no hits before rotation")

        # Baseline in-use DEK(s) + Q4 grep
        merged_before, baseline_deks = self._fts_in_use_deks(bucket_name)
        self.assertTrue(baseline_deks, "GetInUseKeys returned no DEK for the bucket pre-rotation")
        self.assertFalse(
            self.encryption_helper.fts_has_unencrypted_marker(
                merged_before, f"service_bucket {self._bucket_uuid(bucket_name)}"),
            "bucket shows an unencrypted ('') marker before rotation",
        )
        self._check_dek_in_segments(fts_nodes, baseline_deks, label="[pre-rotation] ")

        self.log.info(f"Triggering KEK rotation on secret {self.encryption_at_rest_id}")
        rest = RestConnection(self.master)
        status, resp = rest.trigger_kek_rotation(self.encryption_at_rest_id)
        self.assertTrue(status, f"trigger_kek_rotation failed: {resp}")

        # Query continuously across the rotation window; expect zero failures and
        # consistent hit counts.
        failures = []
        deadline = time.time() + 60
        attempts = 0
        while time.time() < deadline:
            attempts += 1
            try:
                hits = self._match_all_hits(index, zero_ok=True)
                if hits != baseline_hits:
                    failures.append(f"hits={hits} != baseline={baseline_hits}")
            except Exception as err:
                failures.append(f"query error: {err}")
            self.sleep(5, "querying during/after KEK rotation")
        self.log.info(f"Ran {attempts} queries during rotation window; failures={failures}")
        self.assertEqual(failures, [], f"query continuity broken during rotation: {failures}")

        self._validate_segments_encrypted(fts_nodes, "segments(after-rotation)")

        # Post-rotation: bucket must still have an in-use DEK and no unencrypted marker.
        merged_after, deks_after = self._fts_in_use_deks(bucket_name)
        self.assertTrue(deks_after, "no in-use DEK for the bucket after KEK rotation")
        self.assertFalse(
            self.encryption_helper.fts_has_unencrypted_marker(
                merged_after, f"service_bucket {self._bucket_uuid(bucket_name)}"),
            "bucket shows an unencrypted ('') marker after KEK rotation",
        )
        self.log.info("TEST PASSED: test_fts_ear_kek_rotation_query_continuity")

    def test_fts_ear_vector_index(self):
        """KNN query on an ENCRYPTED vector (FAISS) index; segments encrypted on disk.

        Uses its own b1.s1.c1 structure (pass kv=b1.s1.c1, no_buckets=True). Encryption
        is enabled on the vector bucket BEFORE data load so the vector/FAISS sections are
        encrypted from the start.

        REQUIRES the vector test toolchain: the VectorLoader (Docker
        sequoiatools/vectorloader) and a dataset download (e.g. siftsmall from
        ann-benchmarks.com). If that infra is unavailable the test self-skips rather than
        hard-failing, since it is an environment dependency, not an FTS-encryption bug.
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()

        dataset = self._input.param("vector_dataset", "siftsmall")
        dimension = self._input.param("dimension", 128)
        similarity = self._input.param("similarity", "l2_norm")
        k = self._input.param("k", 10)

        # Build the b1.s1.c1 structure, then enable encryption on the new bucket(s)
        # before loading any vector data.
        try:
            containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        except Exception as err:
            self.skipTest(f"Could not set up vector bucket structure (kv param?): {err}")
        buckets = [b.name for b in self._cb_cluster.get_buckets()]
        if not buckets:
            self.skipTest("No buckets created for vector test -- pass kv=b1.s1.c1")
        self._enable_encryption_on_buckets(buckets)

        # Load vector data (Docker + dataset download). Skip on infra failure.
        try:
            self.load_vector_data(containers, dataset=[dataset])
        except Exception as err:
            self.skipTest(f"Vector data load failed (Docker/network infra?): {err}")

        indexes = self._create_fts_index_parameterized(
            field_name="vector_data",
            field_type="vector",
            test_indexes=[("fts_ear_vector_idx", "b1.s1.c1")],
            vector_fields={"dims": dimension, "similarity": similarity},
            create_vector_index=True,
        )
        index_obj = indexes[0]["index_obj"]
        self.wait_for_indexing_complete()

        try:
            query_vectors = self.get_query_vectors(dataset)
            query_vector = query_vectors[0].tolist()
        except Exception as err:
            self.skipTest(f"Could not obtain query vectors for dataset {dataset}: {err}")

        hits, matches, _, status = index_obj.execute_query(
            query={"match_none": {}},
            knn=[{"field": "vector_data", "vector": query_vector, "k": k}],
            return_raw_hits=True,
        )
        self.log.info(f"Encrypted vector KNN: hits={hits}, status={status}")
        self.assertGreaterEqual(hits, 1, f"vector KNN returned no hits (status={status})")

        self._validate_segments_encrypted(fts_nodes, "segments(vector-index)")
        self.log.info("TEST PASSED: test_fts_ear_vector_index")

    def test_fts_ear_dek_auto_rotation_ttl(self):
        """Automatic DEK rotation on a short TTL brings a new DEK into use.

        Pin DEK interval high -> drop interval+lifetime low to trigger auto-rotation ->
        poll GetInUseKeys for a new DEK -> re-pin high. KEK stays unchanged.
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        bucket_name = self.default_bucket_name

        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_dekttl_idx")
        # Pin high so nothing rotates during setup.
        self._set_bucket_dek_rotation(bucket_name, self.STABLE_INTERVAL_S, self.STABLE_INTERVAL_S)
        _, baseline_deks = self._fts_in_use_deks(bucket_name)
        self.assertTrue(baseline_deks, "no baseline DEK before TTL rotation")

        # Drop interval+lifetime to trigger automatic rotation.
        self.log.info("Dropping DEK rotation interval/lifetime to trigger auto-rotation")
        self._set_bucket_dek_rotation(bucket_name, self.ROTATION_INTERVAL_S, self.ROTATION_INTERVAL_S)
        new_deks = self._poll_fts_for_new_dek(bucket_name, baseline_deks, label="[dek-ttl] ")
        # Re-pin high immediately to stop further churn.
        self._set_bucket_dek_rotation(bucket_name, self.STABLE_INTERVAL_S, self.STABLE_INTERVAL_S)

        kek_after = RestConnection(self.master).get_bucket_json(bucket_name).get("encryptionAtRestKeyId")
        self.assertEqual(kek_after, self.encryption_at_rest_id,
                         f"KEK id changed during DEK TTL rotation: {kek_after}")

        # Write segments under the new DEK and validate.
        self._run_emp_data_op(max(1000, self._num_items // 10), "update", start=0, end=max(1000, self._num_items // 10))
        self._force_merge_and_wait(index.name)
        self.wait_for_indexing_complete()
        self._validate_segments_encrypted(fts_nodes, "segments(after-dek-ttl-rotation)")
        self._check_dek_in_segments(fts_nodes, new_deks, label="[dek-ttl] ")
        self.assertGreater(self._match_all_hits(index), 0, "no hits after DEK TTL rotation")
        self.log.info("TEST PASSED: test_fts_ear_dek_auto_rotation_ttl")

    def test_fts_ear_dropkeys_old_key_retired(self):
        """After rotation + force merge, the old DEK is dropped from GetInUseKeys.

        DropKeys forces a re-merge of every segment still using the old key, so once
        that completes the old DEK id no longer appears in the in-use list.
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        bucket_name = self.default_bucket_name

        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_dropkeys_idx")
        _, old_deks = self._fts_in_use_deks(bucket_name)
        self.assertTrue(old_deks, "no baseline DEK before drop-keys")

        # Rotate (drop DEKs) then force the re-merge that retires the old key.
        rest = RestConnection(self.master)
        status, resp = rest.trigger_data_reencryption(bucket_name)
        self.assertTrue(status, f"trigger_data_reencryption failed: {resp}")
        new_deks = self._poll_fts_for_new_dek(bucket_name, old_deks, label="[dropkeys] ")
        self._run_emp_data_op(max(1000, self._num_items // 10), "update", start=0, end=max(1000, self._num_items // 10))
        self._force_merge_and_wait(index.name)
        self.wait_for_indexing_complete()

        # The old DEK(s) must eventually be retired from the in-use list.
        self._poll_fts_until_deks_gone(bucket_name, old_deks, label="[dropkeys] ")
        _, current_deks = self._fts_in_use_deks(bucket_name)
        self.assertTrue(set(new_deks).issubset(set(current_deks)),
                        f"new DEK(s) {new_deks} not in current in-use set {current_deks}")
        self.assertGreater(self._match_all_hits(index), 0, "no hits after drop-keys")
        self._validate_segments_encrypted(fts_nodes, "segments(after-dropkeys)")
        self.log.info("TEST PASSED: test_fts_ear_dropkeys_old_key_retired")

    def test_fts_ear_multi_bucket_dek_isolation(self):
        """Rotating one bucket's DEK must not change another bucket's DEK.

        Needs >=2 encrypted buckets (run with standard_buckets>=1); self-skips otherwise.
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        buckets = self._cb_cluster.get_buckets()
        if len(buckets) < 2:
            self.skipTest("multi-bucket isolation needs >=2 buckets (set standard_buckets>=1)")

        # Index every bucket, load all, wait.
        for bucket in buckets:
            self.create_index(bucket=bucket, index_name=f"idx_{bucket.name}")
        self.load_data(num_items=self._num_items)
        self.wait_for_indexing_complete()

        bucket_a, bucket_b = buckets[0].name, buckets[1].name
        _, deks_a0 = self._fts_in_use_deks(bucket_a)
        _, deks_b0 = self._fts_in_use_deks(bucket_b)
        self.assertTrue(deks_a0 and deks_b0, f"missing baseline DEKs: a={deks_a0}, b={deks_b0}")

        # Rotate ONLY bucket A.
        self.log.info(f"Rotating DEK for bucket '{bucket_a}' only")
        rest = RestConnection(self.master)
        status, resp = rest.trigger_data_reencryption(bucket_a)
        self.assertTrue(status, f"trigger_data_reencryption({bucket_a}) failed: {resp}")
        new_a = self._poll_fts_for_new_dek(bucket_a, deks_a0, label="[multi-bucket] ")

        # Bucket B's DEK must be unchanged.
        _, deks_b1 = self._fts_in_use_deks(bucket_b)
        self.assertEqual(sorted(deks_b1), sorted(deks_b0),
                         f"bucket '{bucket_b}' DEK changed ({deks_b0} -> {deks_b1}) when only "
                         f"'{bucket_a}' was rotated (new A DEK={new_a})")
        self._validate_segments_encrypted(fts_nodes, "segments(multi-bucket)")
        self.log.info("TEST PASSED: test_fts_ear_multi_bucket_dek_isolation")

    def test_fts_ear_getinusekeys_during_boot(self):
        """GetInUseKeys errors while the service is booting, and recovers afterward.

        Per the FTS encryption-manager design, GetKeysInUse throws during booting. We
        restart an FTS node and confirm: (best-effort) the endpoint errors during the
        boot window, and (asserted) it recovers to a valid response listing the bucket's
        DEK once the service is back up.
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        bucket_name = self.default_bucket_name

        self.load_data(num_items=self._num_items)
        self._create_default_index("fts_ear_boot_idx")
        _, baseline_deks = self._fts_in_use_deks(bucket_name)
        self.assertTrue(baseline_deks, "no baseline DEK before restart")

        target = fts_nodes[0]
        bucket_uuid = self._bucket_uuid(bucket_name)
        self.log.info(f"Restarting couchbase-server on FTS node {target.ip}")
        shell = RemoteMachineShellConnection(target)
        try:
            shell.restart_couchbase()
        finally:
            shell.disconnect()

        # Best-effort: observe a boot-time error from GetInUseKeys on the restarted node.
        boot_error_seen = False
        recovered = False
        deadline = time.time() + 300
        target_rest = RestConnection(target)
        while time.time() < deadline:
            try:
                status, response = target_rest.get_fts_in_use_encryption_keys()
                if not status or not isinstance(response, dict):
                    boot_error_seen = True
                    self.log.info(f"GetInUseKeys boot-window error observed: status={status}")
                else:
                    deks = self.encryption_helper.fts_dek_ids_for_bucket(response, bucket_uuid)
                    if deks:
                        recovered = True
                        self.log.info(f"GetInUseKeys recovered after boot: {response}")
                        break
            except Exception as err:
                boot_error_seen = True
                self.log.info(f"GetInUseKeys boot-window exception observed: {err}")
            self.sleep(10, "waiting for FTS node to finish booting")

        self.assertTrue(recovered,
                        "GetInUseKeys did not recover to a valid response after node restart")
        self.log.info(
            f"TEST PASSED: test_fts_ear_getinusekeys_during_boot "
            f"(boot-window error observed={boot_error_seen})"
        )
