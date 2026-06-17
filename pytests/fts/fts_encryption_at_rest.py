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

    # ==================================================================
    # Rebalance / failover under encryption (R-A topology, R-B failover)
    # FTS uses file-transfer rebalance: partitions (encrypted) move with their KEK,
    # the receiver ImportKeys; we validate segments stay encrypted, counts/queries
    # hold, and (best-effort until the GetInUseKeys build lands) the receiving node
    # reports the bucket DEK in use.
    # ==================================================================
    def test_fts_ear_rebalance_in(self):
        """Rebalance an FTS node IN; moved partitions stay encrypted on the new node."""
        self._require_encryption_enabled()
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_rebin_idx")
        baseline_hits = self._match_all_hits(index)
        self.assertGreater(baseline_hits, 0, "no hits before rebalance-in")
        self._validate_segments_encrypted(self._cb_cluster.get_fts_nodes(), "segments(before-rebalance-in)")

        spare = self._free_server()
        self.assertIsNotNone(spare, "no free server available to rebalance in (need a spare node in the ini)")
        self.log.info(f"Rebalancing in FTS node {spare.ip}")
        self._cb_cluster.rebalance_in_node(nodes_in=[spare], services=["fts"])

        self._post_topology_validation("segments(after-rebalance-in)", baseline_hits, index)
        # FTS file-transfer rebalance copies the KEK; the receiver ImportKeys -> the new
        # node should report the bucket DEK in use (hard assert once the endpoint lands).
        new_node_deks = self._bucket_dek_on_node(spare)
        self.log.info(f"new node {spare.ip} in-use DEKs (best-effort): {new_node_deks}")
        self.log.info("TEST PASSED: test_fts_ear_rebalance_in")

    def test_fts_ear_rebalance_out(self):
        """Rebalance an FTS node OUT; partitions relocate and stay encrypted, no data loss."""
        self._require_encryption_enabled()
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_rebout_idx")
        baseline_hits = self._match_all_hits(index)
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.assertGreaterEqual(len(fts_nodes), 2, "need >=2 FTS nodes to rebalance one out")
        self._validate_segments_encrypted(fts_nodes, "segments(before-rebalance-out)")

        out_node = fts_nodes[-1]   # non-master FTS node
        self.log.info(f"Rebalancing out FTS node {out_node.ip}")
        self._cb_cluster.rebalance_out_node(node=out_node)

        self._post_topology_validation("segments(after-rebalance-out)", baseline_hits, index)
        self.log.info("TEST PASSED: test_fts_ear_rebalance_out")

    def test_fts_ear_swap_rebalance(self):
        """Swap-rebalance an FTS node; data stays encrypted on the swapped-in node."""
        self._require_encryption_enabled()
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_swap_idx")
        baseline_hits = self._match_all_hits(index)
        self.assertIsNotNone(self._free_server(), "no free server available to swap in")
        self._validate_segments_encrypted(self._cb_cluster.get_fts_nodes(), "segments(before-swap)")

        self.log.info("Swap-rebalancing an FTS node")
        self._cb_cluster.swap_rebalance(services=["fts"], num_nodes=1)

        self._post_topology_validation("segments(after-swap)", baseline_hits, index)
        self.log.info("TEST PASSED: test_fts_ear_swap_rebalance")

    def test_fts_ear_hard_failover_full_recovery(self):
        """Hard failover an FTS node, add back with FULL recovery; reindexed data encrypted."""
        self._require_encryption_enabled()
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_failover_full_idx")
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.assertGreaterEqual(len(fts_nodes), 2, "need >=2 FTS nodes for failover")
        baseline_hits = self._match_all_hits(index)
        self._validate_segments_encrypted(fts_nodes, "segments(before-hard-failover)")

        fo_node = fts_nodes[-1]
        self.log.info(f"Hard failover of FTS node {fo_node.ip}")
        task = self._cb_cluster.async_failover(node=fo_node, graceful=False)
        task.result()
        self.log.info("Adding node back with FULL recovery")
        self._cb_cluster.add_back_node(recovery_type='full', services=["fts"])

        self._post_topology_validation("segments(after-full-recovery)", baseline_hits, index)
        self.log.info("TEST PASSED: test_fts_ear_hard_failover_full_recovery")

    def test_fts_ear_hard_failover_delta_recovery(self):
        """Hard failover a DATA (KV) node, add it back with DELTA recovery; FTS data
        stays encrypted and no data is lost.

        Delta recovery replays only the delta accumulated since failover and therefore
        REQUIRES a node with persisted vbucket data -- i.e. a KV/data node. An FTS-only
        node holds no vbuckets, so ns_server rejects it for delta recovery ("invalid node
        name or node can't be used for delta recovery"). We therefore fail over a
        non-master DATA node (which needs >=2 data nodes so the bucket survives the
        failover), delta-recover it, then validate FTS segments stay encrypted and the
        hit count is unchanged.

        Run with a 2-data-node topology, e.g. cluster=D,D,F.
        """
        self._require_encryption_enabled()
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_failover_delta_idx")
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.assertGreaterEqual(len(fts_nodes), 1, "need >=1 FTS node")
        kv_nodes = self._cb_cluster.get_kv_nodes()
        self.assertGreaterEqual(
            len(kv_nodes), 2,
            "delta recovery needs a data node to fail over while the bucket stays up -- "
            "run with >=2 data nodes (e.g. cluster=D,D,F)")
        baseline_hits = self._match_all_hits(index)
        self._validate_segments_encrypted(fts_nodes, "segments(before-failover)")

        # Fail over a NON-master data node (self.master is the orchestrator).
        fo_node = next((n for n in kv_nodes if n.ip != self.master.ip), kv_nodes[-1])
        self.log.info(f"Hard failover of DATA node {fo_node.ip}")
        task = self._cb_cluster.async_failover(node=fo_node, graceful=False)
        task.result()
        self.log.info("Adding node back with DELTA recovery")
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv"])

        self._post_topology_validation("segments(after-delta-recovery)", baseline_hits, index)
        self.log.info("TEST PASSED: test_fts_ear_hard_failover_delta_recovery")

    # ==================================================================
    # R-C: rebalance / failover concurrent with active key rotation (test plan 10c).
    # Novel -- GSI/eventing do these sequentially. We deliberately overlap a topology
    # change with an in-flight rotation and assert no corruption / data loss, queries
    # keep working, and data ends up encrypted under the right key.
    # ==================================================================
    def test_fts_ear_rebalance_in_during_dek_rotation(self):
        """Rebalance an FTS node IN while a DEK rotation (force re-encryption) is in flight."""
        self._require_encryption_enabled()
        bucket_name = self.default_bucket_name
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_rebin_rot_idx")
        baseline_hits = self._match_all_hits(index)
        _, baseline_deks = self._fts_in_use_deks(bucket_name)
        self.assertTrue(baseline_deks, "no baseline DEK before concurrent rebalance-in + rotation")

        spare = self._free_server()
        self.assertIsNotNone(spare, "no free server available to rebalance in")

        # Start rebalance-in async, then immediately drop DEKs so re-encryption overlaps.
        self.log.info(f"Concurrent: rebalance-in {spare.ip} + DEK rotation (drop DEKs)")
        reb_task = self._cb_cluster.async_rebalance_in_node(nodes_in=[spare], services=["fts"])
        rest = RestConnection(self.master)
        status, resp = rest.trigger_data_reencryption(bucket_name)
        self.assertTrue(status, f"trigger_data_reencryption failed: {resp}")
        reb_task.result()

        # Both done: a new DEK must be in use; KEK unchanged.
        new_deks = self._poll_fts_for_new_dek(bucket_name, baseline_deks, label="[rebin+dek] ")
        kek_after = rest.get_bucket_json(bucket_name).get("encryptionAtRestKeyId")
        self.assertEqual(kek_after, self.encryption_at_rest_id,
                         f"KEK id changed during concurrent rebalance-in + DEK rotation: {kek_after}")

        # Write under the new DEK + merge so segments reflect it, then validate.
        self._run_emp_data_op(max(1000, self._num_items // 10), "update", start=0, end=max(1000, self._num_items // 10))
        self._force_merge_and_wait(index.name)
        self._post_topology_validation("segments(rebalance-in-during-dek-rotation)", baseline_hits, index)
        self._check_dek_in_segments(self._cb_cluster.get_fts_nodes(), new_deks, label="[rebin+dek] ")
        self.log.info("TEST PASSED: test_fts_ear_rebalance_in_during_dek_rotation")

    def test_fts_ear_rebalance_out_during_kek_rotation(self):
        """Rebalance an FTS node OUT while a KEK rotation is in flight; queries keep working."""
        self._require_encryption_enabled()
        bucket_name = self.default_bucket_name
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_rebout_kek_idx")
        baseline_hits = self._match_all_hits(index)
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.assertGreaterEqual(len(fts_nodes), 2, "need >=2 FTS nodes to rebalance one out")
        _, baseline_deks = self._fts_in_use_deks(bucket_name)
        self.assertTrue(baseline_deks, "no baseline DEK before concurrent rebalance-out + KEK rotation")

        out_node = fts_nodes[-1]
        # Start rebalance-out async, then immediately rotate the KEK so they overlap.
        self.log.info(f"Concurrent: rebalance-out {out_node.ip} + KEK rotation")
        reb_task = self._cb_cluster.async_rebalance_out_node(node=out_node)
        rest = RestConnection(self.master)
        status, resp = rest.trigger_kek_rotation(self.encryption_at_rest_id)
        self.assertTrue(status, f"trigger_kek_rotation failed: {resp}")

        # Queries must keep working across the concurrent window.
        self._run_queries_no_error(index, duration_s=45, label="[rebout+kek] ")
        reb_task.result()

        # Post: encrypted, balanced, no data loss, bucket still has a DEK and no "" marker.
        self._post_topology_validation("segments(rebalance-out-during-kek-rotation)", baseline_hits, index)
        merged_after, deks_after = self._fts_in_use_deks(bucket_name)
        self.assertTrue(deks_after, "no in-use DEK after concurrent rebalance-out + KEK rotation")
        self.assertFalse(
            self.encryption_helper.fts_has_unencrypted_marker(
                merged_after, f"service_bucket {self._bucket_uuid(bucket_name)}"),
            "unencrypted ('') marker present after concurrent rebalance-out + KEK rotation",
        )
        self.log.info("TEST PASSED: test_fts_ear_rebalance_out_during_kek_rotation")

    def test_fts_ear_failover_during_rotation(self):
        """Hard failover an FTS node while re-encryption is in flight, then recover; no loss."""
        self._require_encryption_enabled()
        bucket_name = self.default_bucket_name
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_failover_rot_idx")
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.assertGreaterEqual(len(fts_nodes), 2, "need >=2 FTS nodes for failover")
        baseline_hits = self._match_all_hits(index)
        _, baseline_deks = self._fts_in_use_deks(bucket_name)
        self.assertTrue(baseline_deks, "no baseline DEK before failover-during-rotation")

        # Start re-encryption, then immediately fail a node over while it is in flight.
        self.log.info("Concurrent: DEK rotation (drop DEKs) + hard failover")
        rest = RestConnection(self.master)
        status, resp = rest.trigger_data_reencryption(bucket_name)
        self.assertTrue(status, f"trigger_data_reencryption failed: {resp}")
        fo_node = fts_nodes[-1]
        task = self._cb_cluster.async_failover(node=fo_node, graceful=False)
        task.result()
        self.log.info("Adding node back with FULL recovery")
        self._cb_cluster.add_back_node(recovery_type='full', services=["fts"])

        # Recovery reindexes; a new DEK should be in use, data intact and encrypted.
        new_deks = self._poll_fts_for_new_dek(bucket_name, baseline_deks, label="[failover+rot] ")
        self._force_merge_and_wait(index.name)
        self._post_topology_validation("segments(failover-during-rotation)", baseline_hits, index)
        self._check_dek_in_segments(self._cb_cluster.get_fts_nodes(), new_deks, label="[failover+rot] ")
        self.log.info("TEST PASSED: test_fts_ear_failover_during_rotation")

    def test_fts_ear_enable_encryption_during_rebalance(self):
        """Enable bucket encryption WHILE an FTS node rebalances IN; data ends up encrypted.

        Start from an UNENCRYPTED baseline, kick off rebalance-in async, flip bucket
        encryption ON mid-rebalance, then confirm the moved/rewritten segments are
        encrypted, a DEK is in use (no '' marker), and no data is lost.
        """
        self._require_encryption_enabled()
        bucket_name = self.default_bucket_name
        fts_nodes = self._cb_cluster.get_fts_nodes()
        rest = RestConnection(self.master)

        # Baseline: turn encryption OFF and rewrite segments to plaintext.
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_enable_reb_idx")
        baseline_hits = self._match_all_hits(index)
        self.assertGreater(baseline_hits, 0, "no hits before enable-during-rebalance")
        status, resp = rest.disable_bucket_encryption(bucket_name)
        self.assertTrue(status, f"disable_bucket_encryption failed: {resp}")
        self._run_emp_data_op(max(1000, self._num_items // 10), "update", start=0, end=max(1000, self._num_items // 10))
        self._force_merge_and_wait(index.name)
        self.wait_for_indexing_complete()
        self.sleep(30, "letting segments rewrite as plaintext")
        dec = self.encryption_helper.verify_fts_segment_files_not_encrypted(
            fts_nodes, sensitive_terms=self.sensitive_terms)
        self._assert_encryption_results(dec, "segments(baseline-plaintext)")

        spare = self._free_server()
        self.assertIsNotNone(spare, "no free server available to rebalance in")

        # Concurrent: rebalance-in async, then enable encryption mid-flight.
        self.log.info(f"Concurrent: rebalance-in {spare.ip} + ENABLE bucket encryption")
        reb_task = self._cb_cluster.async_rebalance_in_node(nodes_in=[spare], services=["fts"])
        self._enable_encryption_on_buckets([bucket_name])
        reb_task.result()

        # Write under the new DEK + merge so segments reflect encryption, then validate.
        self._run_emp_data_op(max(1000, self._num_items // 10), "update", start=0, end=max(1000, self._num_items // 10))
        self._force_merge_and_wait(index.name)
        self._post_topology_validation("segments(enable-during-rebalance)", baseline_hits, index)
        merged_on, deks_on = self._fts_in_use_deks(bucket_name)
        self.assertTrue(deks_on, "no in-use DEK after enabling encryption during rebalance")
        self.assertFalse(
            self.encryption_helper.fts_has_unencrypted_marker(
                merged_on, f"service_bucket {self._bucket_uuid(bucket_name)}"),
            "unencrypted ('') marker still present after enabling encryption during rebalance",
        )
        self.log.info("TEST PASSED: test_fts_ear_enable_encryption_during_rebalance")

    def test_fts_ear_disable_encryption_during_rebalance(self):
        """Disable bucket encryption WHILE an FTS node rebalances OUT; data ends up plaintext.

        Start encrypted, kick off rebalance-out async, flip bucket encryption OFF
        mid-rebalance, then confirm segments are rewritten as plaintext, GetInUseKeys
        reports the unencrypted ('') marker, and no data is lost.
        """
        self._require_encryption_enabled()
        bucket_name = self.default_bucket_name
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_disable_reb_idx")
        baseline_hits = self._match_all_hits(index)
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.assertGreaterEqual(len(fts_nodes), 2, "need >=2 FTS nodes to rebalance one out")
        self._validate_segments_encrypted(fts_nodes, "segments(before-disable-during-rebalance)")

        out_node = fts_nodes[-1]
        rest = RestConnection(self.master)
        # Concurrent: rebalance-out async, then disable encryption mid-flight.
        self.log.info(f"Concurrent: rebalance-out {out_node.ip} + DISABLE bucket encryption")
        reb_task = self._cb_cluster.async_rebalance_out_node(node=out_node)
        status, resp = rest.disable_bucket_encryption(bucket_name)
        self.assertTrue(status, f"disable_bucket_encryption failed: {resp}")
        reb_task.result()

        # Rewrite segments to plaintext + merge, then validate no loss and plaintext on disk.
        self._run_emp_data_op(max(1000, self._num_items // 10), "update", start=0, end=max(1000, self._num_items // 10))
        self._force_merge_and_wait(index.name)
        self.wait_for_indexing_complete()
        self.sleep(30, "letting segments rewrite as plaintext")
        self.assertEqual(self._match_all_hits(index), baseline_hits,
                         "doc count changed after disable-during-rebalance")
        remaining = self._cb_cluster.get_fts_nodes()
        dec = self.encryption_helper.verify_fts_segment_files_not_encrypted(
            remaining, sensitive_terms=self.sensitive_terms)
        self._assert_encryption_results(dec, "segments(after-disable-during-rebalance-plaintext)")
        merged_off, _ = self._fts_in_use_deks(bucket_name)
        self.assertTrue(
            self.encryption_helper.fts_has_unencrypted_marker(
                merged_off, f"service_bucket {self._bucket_uuid(bucket_name)}"),
            "expected an unencrypted ('') marker in GetInUseKeys after disabling encryption during rebalance",
        )
        self.log.info("TEST PASSED: test_fts_ear_disable_encryption_during_rebalance")

    # ==================================================================
    # Index-ops breadth under encryption (test plan section 3)
    # ==================================================================
    def test_fts_ear_query_types(self):
        """Term / match / phrase / prefix / wildcard / conjunction / disjunction queries
        all execute against the encrypted inverted index (TF + locations from encrypted
        segments); segments stay encrypted."""
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_querytypes_idx")
        self.assertGreater(self._match_all_hits(index), 0, "no hits on encrypted index")

        queries = {
            "term": {"term": "Safiya", "field": "name"},
            "match": {"match": "Safiya Morgan", "field": "name"},
            "match_phrase": {"match_phrase": "Safiya Morgan", "field": "name"},
            "prefix": {"prefix": "Saf", "field": "name"},
            "wildcard": {"wildcard": "Saf*", "field": "name"},
            "numeric_range": {"min": -1, "max": 100000000, "inclusive_min": True,
                              "inclusive_max": True, "field": "mutated"},
            "conjunction": {"conjuncts": [
                {"match": "Safiya", "field": "name"},
                {"min": -1, "max": 100000000, "field": "mutated"}]},
            "disjunction": {"disjuncts": [
                {"match": "Safiya", "field": "name"},
                {"match": "Morgan", "field": "name"}]},
        }
        for label, query in queries.items():
            try:
                hits, _, _, _ = index.execute_query(query, zero_results_ok=True)
                self.log.info(f"query[{label}] hits={hits}")
            except Exception as err:
                self.fail(f"query type '{label}' failed on encrypted index: {err}")

        self._validate_segments_encrypted(fts_nodes, "segments(query-types)")
        self.log.info("TEST PASSED: test_fts_ear_query_types")

    def test_fts_ear_custom_analyzer(self):
        """Encrypted index built with a non-default analyzer; queries work, segments encrypted."""
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        analyzer = self._input.param("analyzer", "keyword")
        self.load_data(num_items=self._num_items)
        index = self.create_index(
            bucket=self._default_bucket(), index_name="fts_ear_analyzer_idx", analyzer=analyzer
        )
        self.wait_for_indexing_complete()
        self.assertGreater(self._match_all_hits(index), 0, f"no hits with analyzer={analyzer}")
        self._validate_segments_encrypted(fts_nodes, f"segments(analyzer={analyzer})")
        self.log.info(f"TEST PASSED: test_fts_ear_custom_analyzer (analyzer={analyzer})")

    def test_fts_ear_collection_scoped_index(self):
        """Encrypted index scoped to a non-default scope/collection (run with container_type=collection)."""
        self._require_encryption_enabled()
        if getattr(self, "container_type", "bucket") != "collection":
            self.skipTest("run with container_type=collection to exercise a collection-scoped index")
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.load_data(num_items=self._num_items)
        collections = self.collection if isinstance(self.collection, list) else [self.collection]
        type_mapping = f"{self.scope}.{collections[0]}"
        index = self.create_index(
            bucket=self._default_bucket(), index_name="fts_ear_coll_idx",
            collection_index=True, scope=self.scope, collections=collections, _type=type_mapping,
        )
        self.wait_for_indexing_complete()
        self.assertGreater(self._match_all_hits(index), 0, "no hits on collection-scoped encrypted index")
        self._validate_segments_encrypted(fts_nodes, "segments(collection-scoped)")
        self.log.info("TEST PASSED: test_fts_ear_collection_scoped_index")

    # ==================================================================
    # Remaining chaos / concurrency (test plan section 10 a,b,e-i)
    # ==================================================================
    def test_fts_ear_rotation_during_merge(self):
        """Trigger key rotation (drop DEKs) while a segment merge/compaction is in flight."""
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        bucket_name = self.default_bucket_name
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_rot_merge_idx")
        # Generate extra segments so there's real merge work to do.
        self._run_emp_data_op(max(1000, self._num_items // 5), "update", start=0, end=max(1000, self._num_items // 5))
        self.wait_for_indexing_complete()
        _, baseline_deks = self._fts_in_use_deks(bucket_name)
        baseline_hits = self._match_all_hits(index)

        # Kick off a merge AND a DEK rotation so they overlap.
        # Compaction is an FTS (:8094) endpoint -> use an FTS node; re-encryption is an
        # ns_server (:8091) endpoint -> use master.
        self.log.info("Concurrent: segment merge + DEK rotation (drop DEKs)")
        self._fts_rest().start_fts_index_compaction(index.name)
        status, resp = RestConnection(self.master).trigger_data_reencryption(bucket_name)
        self.assertTrue(status, f"trigger_data_reencryption failed: {resp}")

        self._wait_merge_complete(index.name)
        new_deks = self._poll_fts_for_new_dek(bucket_name, baseline_deks, label="[rot+merge] ")
        self._force_merge_and_wait(index.name)
        self.wait_for_indexing_complete()

        self.assertEqual(self._match_all_hits(index), baseline_hits,
                         "hit count changed after rotation-during-merge (possible data loss)")
        self._validate_segments_encrypted(fts_nodes, "segments(rotation-during-merge)")
        self._check_dek_in_segments(fts_nodes, new_deks, label="[rot+merge] ")
        self.log.info("TEST PASSED: test_fts_ear_rotation_during_merge")

    def test_fts_ear_mutations_during_rotation(self):
        """High-rate KV mutations while re-encryption is in flight; no data loss, queues drain.

        Pragmatic stand-in for the DCP-backpressure scenario: load under rotation and
        confirm the index reconciles with the bucket (no loss) and stays encrypted.
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        bucket_name = self.default_bucket_name
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_mut_rot_idx")
        _, baseline_deks = self._fts_in_use_deks(bucket_name)

        # Start re-encryption, then drive a burst of mutations while it runs.
        self.log.info("Concurrent: DEK rotation + high-rate mutations")
        rest = RestConnection(self.master)
        status, resp = rest.trigger_data_reencryption(bucket_name)
        self.assertTrue(status, f"trigger_data_reencryption failed: {resp}")
        for _ in range(3):
            self._run_emp_data_op(max(1000, self._num_items // 5), "update",
                                  start=0, end=max(1000, self._num_items // 5))

        new_deks = self._poll_fts_for_new_dek(bucket_name, baseline_deks, label="[mut+rot] ")
        self._force_merge_and_wait(index.name)
        # Index must reconcile with the bucket doc count (no data loss / drained queues).
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        self._validate_segments_encrypted(fts_nodes, "segments(mutations-during-rotation)")
        self._check_dek_in_segments(fts_nodes, new_deks, label="[mut+rot] ")
        self.log.info("TEST PASSED: test_fts_ear_mutations_during_rotation")

    # ==================================================================
    # cbcollect (test plan section 9)
    # ==================================================================
    def test_fts_ear_cbcollect(self):
        """cbcollect_info runs cleanly with encrypted FTS files present and produces a bundle.

        (Deep validation that the bundle's fts.log is decrypted is a follow-up that needs
        the bundle extracted/inspected; here we confirm cbcollect doesn't choke on
        encrypted files and produces a non-trivial diag zip.)
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.load_data(num_items=self._num_items)
        self._create_default_index("fts_ear_cbcollect_idx")
        self._validate_segments_encrypted(fts_nodes, "segments(before-cbcollect)")

        node = fts_nodes[0]
        shell = RemoteMachineShellConnection(node)
        try:
            bin_dir = self._cb_bin_path(node)
            out, err = shell.execute_command(
                f"{bin_dir}cbcollect_info /tmp/fts_ear_diag.zip", use_channel=True, timeout=900
            )
            self.log.info(f"cbcollect tail: {(out or [])[-5:]}, err: {(err or [])[-3:]}")
            listing, _ = shell.execute_command("ls -l /tmp/fts_ear_diag.zip 2>/dev/null")
            self.assertTrue(any("fts_ear_diag.zip" in line for line in (listing or [])),
                            f"cbcollect_info did not produce the diag bundle: out={out}, err={err}")
        finally:
            shell.disconnect()
        self.log.info("TEST PASSED: test_fts_ear_cbcollect")

    # ==================================================================
    # SDK + CLI tools (test plan section 6)
    # ==================================================================
    def test_fts_ear_bleve_zap_cli_errors_on_encrypted(self):
        """Per design, the bleve/zap CLI must error on encrypted data (no key access).

        NOTE: the exact CLI binary/subcommand may need dev confirmation; the test
        self-skips if no bleve/zap binary is found on the node.
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.load_data(num_items=self._num_items)
        self._create_default_index("fts_ear_cli_idx")
        node = fts_nodes[0]
        fts_dir = self.encryption_helper.get_fts_data_path(node)
        shell = RemoteMachineShellConnection(node)
        try:
            zaps, _ = shell.execute_command(f"find {fts_dir} -type f -name '*.zap' 2>/dev/null | head -1")
            zap = (zaps or [""])[0].strip()
            if not zap:
                self.skipTest("no .zap segment found to exercise the bleve/zap CLI")
            bin_dir = self._cb_bin_path(node)
            tool = None
            for candidate in ("cbft-bleve", "bleve", "cbft-zap"):
                chk, _ = shell.execute_command(f"ls {bin_dir}{candidate} 2>/dev/null")
                if chk and any(candidate in line for line in chk):
                    tool = f"{bin_dir}{candidate}"
                    break
            if not tool:
                self.skipTest("no bleve/zap CLI binary found on the node")
            # The exact zap subcommand syntax varies by build (e.g. `zap <version> explore`,
            # `zap explore`, or just passing the file). Try a few forms; the first one that
            # actually reads the segment (rather than printing usage/help) is the real attempt.
            invocations = (
                f"{tool} zap explore {zap}",
                f"{tool} zap {zap}",
                f"{tool} explore {zap}",
                f"{tool} {zap}",
            )
            combined = ""
            for cmd in invocations:
                out, err = shell.execute_command(f"{cmd} 2>&1", use_channel=True)
                combined = " ".join((out or []) + (err or [])).lower()
                self.log.info(f"bleve/zap CLI [{cmd}] -> {combined[:300]}")
                # usage/help means we picked the wrong subcommand form; try the next one.
                if not any(h in combined for h in ("usage:", "available commands", "flags:", "--help")):
                    break
            else:
                self.skipTest("could not find a working bleve/zap read invocation "
                              "(all forms printed usage/help; exact CLI syntax needs dev confirmation)")
            self.assertTrue(
                any(k in combined for k in ("error", "encrypt", "cannot", "fail", "unable", "invalid")),
                f"bleve/zap CLI did not error on encrypted segment (expected an error): {combined[:300]}",
            )
        finally:
            shell.disconnect()
        self.log.info("TEST PASSED: test_fts_ear_bleve_zap_cli_errors_on_encrypted")

    def test_fts_ear_sdk_query(self):
        """SDK query parity on an encrypted index.

        The SDK and REST queries hit the same FTS query path (already validated by the
        REST query tests). A dedicated SDK run needs the python couchbase SDK /
        java_sdk_client wiring; self-skips if unavailable.
        """
        self._require_encryption_enabled()
        try:
            import couchbase  # noqa: F401
        except Exception:
            self.skipTest("python couchbase SDK not available; FTS query path is covered by the REST query tests")
        self.skipTest("SDK FTS query coverage pending SDK-client wiring; REST query path validated elsewhere")

    # ==================================================================
    # Community Edition negative (test plan section 11)
    # Run with enable_encryption_at_rest=False so setUp does not attempt encryption.
    # ==================================================================
    def test_fts_ear_ce_encryption_rejected(self):
        """On Community Edition, creating an encryption secret must be rejected."""
        rest = RestConnection(self.master)
        if rest.is_enterprise_edition():
            self.skipTest("Community-Edition-only negative test")
        from lib.membase.helper.encryption_at_rest_helper import EncryptionUtil
        params = EncryptionUtil.create_secret_params(
            name=EncryptionUtil.generate_random_name("CEReject")
        )
        status, response = rest.create_secret(params)
        self.assertFalse(status, f"creating an encryption secret unexpectedly succeeded on CE: {response}")
        self.log.info(f"CE correctly rejected encryption secret creation: {response}")
        self.log.info("TEST PASSED: test_fts_ear_ce_encryption_rejected")

    # ==================================================================
    # Windows sanity (test plan section 12)
    # ==================================================================
    def test_fts_ear_windows_sanity(self):
        """Windows-only sanity: encryption enabled, index built + queried, segments encrypted."""
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        if self._os_type(fts_nodes[0]) != "windows":
            self.skipTest("Windows-only sanity test")
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_win_idx")
        self.assertGreater(self._match_all_hits(index), 0, "no hits on Windows encrypted index")
        self._validate_segments_encrypted(fts_nodes, "segments(windows)")
        self.log.info("TEST PASSED: test_fts_ear_windows_sanity")

    # ==================================================================
    # Upgrade / migration (test plan section 8) -- scaffolds.
    # These need the upgrade framework (NewUpgradeBaseTest) + two builds, which is a
    # different base than FTSEncryptionBaseTest, so they self-skip here and document the
    # intended flow for a dedicated upgrade test class.
    # ==================================================================
    def test_fts_ear_mixed_mode_encryption_blocked(self):
        """Encryption must NOT be enableable on a mixed-version (pre-8.1 + 8.1) cluster.

        Intended flow: start a mixed-version cluster (some nodes < 8.1), attempt to enable
        bucket encryption / create FTS index with encryption, assert it is rejected.
        Requires the upgrade framework + two builds.
        """
        self.skipTest("mixed-mode negative requires the upgrade framework (NewUpgradeBaseTest) "
                      "+ two builds; scaffold pending dedicated upgrade test class")

    def test_fts_ear_enable_encryption_post_upgrade(self):
        """After upgrading 8.0->8.1, enabling encryption re-indexes segments encrypted (v17).

        Intended flow: install initial_version (8.0) with unencrypted FTS indexes, upgrade
        to 8.1, enable encryption, verify MergeObsoleteSegments re-encrypts and no data
        loss. Requires the upgrade framework + initial_version/upgrade_version builds.
        """
        self.skipTest("post-upgrade enable-encryption requires the upgrade framework + two "
                      "builds; scaffold pending dedicated upgrade test class")

    # ==================================================================
    # Backup / restore (test plan section 4) -- FTS index-definition backup via REST.
    # cbbackupmgr full-data backup/restore is a heavier follow-up.
    # ==================================================================
    def test_fts_ear_index_def_backup_restore(self):
        """FTS index-definition backup/restore preserves encryption.

        Backup the index defs (REST), delete the index, restore; the restored index
        reindexes on the still-encrypted bucket and its segments stay encrypted.
        """
        self._require_encryption_enabled()
        fts_nodes = self._cb_cluster.get_fts_nodes()
        self.load_data(num_items=self._num_items)
        index = self._create_default_index("fts_ear_backup_idx")
        index_name = index.name
        self._validate_segments_encrypted(fts_nodes, "segments(before-backup)")

        try:
            from .fts_backup_restore import FTSIndexBackupClient
        except Exception as err:
            self.skipTest(f"FTSIndexBackupClient not importable: {err}")
        backup_client = FTSIndexBackupClient(fts_nodes[0])
        status, content = backup_client.backup()
        self.assertTrue(status, f"FTS index backup failed: {content}")

        self.log.info("Deleting all FTS indexes, then restoring from backup")
        self._cb_cluster.delete_all_fts_indexes()
        restore_result = backup_client.restore()
        restored_ok = restore_result if isinstance(restore_result, bool) else restore_result[0]
        self.assertTrue(restored_ok, "FTS index restore failed")

        # Restore recreates indexes server-side (no FTSIndex object), so poll by name.
        # get_fts_index_doc_count is an FTS (:8094) endpoint -> use an FTS node.
        rest = self._fts_rest()
        deadline = time.time() + 600
        count = 0
        while time.time() < deadline:
            try:
                count = rest.get_fts_index_doc_count(index_name)
            except Exception as err:
                self.log.info(f"waiting for restored index '{index_name}': {err}")
            if count >= self._num_items:
                break
            self.sleep(10, f"reindexing restored '{index_name}' ({count}/{self._num_items})")
        self.assertGreaterEqual(count, self._num_items,
                                f"restored index '{index_name}' did not reindex fully ({count}/{self._num_items})")

        self._validate_segments_encrypted(self._cb_cluster.get_fts_nodes(), "segments(after-restore)")
        self.log.info("TEST PASSED: test_fts_ear_index_def_backup_restore")
