"""
fts_encryption_base.py: Base class for FTS (Search) Encryption-at-Rest tests.

Holds all the encryption-specific setUp/tearDown and helper/wrapper methods (secret
setup, on-disk validation, GetInUseKeys/DEK helpers, rotation helpers). Test classes
should subclass FTSEncryptionBaseTest and contain ONLY test_* methods.

__author__ = "Dananjay S"
__created_on__ = "02/06/26"
"""

import json
import os
import random
import time

from .fts_base import FTSBaseTest
from couchbase_helper.documentgenerator import SDKDataLoader
from membase.api.rest_client import RestConnection
from lib.membase.helper.encryption_at_rest_helper import EncryptionUtil
from lib.couchbase_helper.encryption_at_rest_helper import EncryptionAtRestHelper


class FTSEncryptionBaseTest(FTSBaseTest):

    # Distinctive emp-dataset tokens (field names + indexed values) that represent
    # user data and therefore MUST NOT appear as plaintext in encrypted segments.
    DEFAULT_SENSITIVE_TERMS = [
        "mcdiabetes",        # email domain (indexed value)
        "languages_known",   # distinctive field name
        "is_manager",        # field name
        "emp_id",            # field name
        "team_size",         # nested field name
        "Safiya",            # known indexed name value
        "Morgan",            # known indexed name value
    ]

    # DEK rotation interval pinning (mirrors the GSI/eventing pattern): pin high to
    # avoid accidental rotation, drop low to trigger one, then re-pin high.
    STABLE_INTERVAL_S = 36000   # 10h
    ROTATION_INTERVAL_S = 120   # 2m

    def setUp(self):
        super(FTSEncryptionBaseTest, self).setUp()

        # IDs populated by EncryptionUtil.set_encryption_ids() after setup.
        # Initialized up-front so pseudo-tests / tearDown never hit AttributeError.
        self.encryption_at_rest_id = None
        self.config_encryption_at_rest_id = None
        self.log_encryption_at_rest_id = None
        self.audit_encryption_at_rest_id = None
        self.KMIP_id = None

        # suite_setUp and suite_tearDown are dispatched as pseudo-tests that also run
        # setUp(). We must NOT create encryption secrets during them -- doing so on
        # teardown both wastes secrets and trips ns_server's "name must be unique".
        if self._testMethodName in ('suite_setUp', 'suite_tearDown'):
            self.log.info(f"Skipping encryption setup for pseudo-test {self._testMethodName}")
            return

        self.log.info("============== FTS-EAR setUp started ==============")

        # --- which encryption-at-rest types to enable (mirrors basetestcase.py params) ---
        # data/bucket encryption drives the .zap / metadata encryption we validate;
        # config / log / audit are the "other" cluster-wide encryption types. For FTS,
        # log encryption is what encrypts fts.log on disk.
        self.enable_encryption_at_rest = self._input.param("enable_encryption_at_rest", True)
        self.enable_config_encryption_at_rest = self._input.param("enable_config_encryption_at_rest", False)
        self.enable_log_encryption_at_rest = self._input.param("enable_log_encryption_at_rest", False)
        self.enable_audit_encryption_at_rest = self._input.param("enable_audit_encryption_at_rest", False)
        # None -> create_secret_params falls back to day-based rotation (stable for a test run)
        self.secret_rotation_interval = self._input.param("secret_rotation_interval", None)

        # KMIP (external KMS) support -- mirrors basetestcase flags
        self.create_KMIP_secret = self._input.param("create_KMIP_secret", False)
        self.KMIP_for_config_encryption = self._input.param("KMIP_for_config_encryption", False)
        self.KMIP_for_log_encryption = self._input.param("KMIP_for_log_encryption", False)
        self.KMIP_for_audit_encryption = self._input.param("KMIP_for_audit_encryption", False)

        # DEK lifetimes / rotation intervals for the "other" encryption types (None -> server default)
        self.config_dekLifetime = self._input.param("config_dekLifetime", None)
        self.config_dekRotationInterval = self._input.param("config_dekRotationInterval", None)
        self.log_dekLifetime = self._input.param("log_dekLifetime", None)
        self.log_dekRotationInterval = self._input.param("log_dekRotationInterval", None)
        self.audit_dekLifetime = self._input.param("audit_dekLifetime", None)
        self.audit_dekRotationInterval = self._input.param("audit_dekRotationInterval", None)

        self.default_bucket_name = self._input.param("default_bucket_name", "default")
        # Gzipped fts.log rotations are currently not encrypted (known ns_server gap).
        # Default: report it as a tracked finding (logged + counted) but do NOT fail.
        # Set enforce_log_rotation_encryption=True to make it a hard failure once fixed.
        self.enforce_log_rotation_encryption = self._input.param(
            "enforce_log_rotation_encryption", False)
        sensitive_param = self._input.param("sensitive_terms", None)
        if sensitive_param:
            self.sensitive_terms = [t.strip() for t in str(sensitive_param).split(";") if t.strip()]
        else:
            self.sensitive_terms = list(self.DEFAULT_SENSITIVE_TERMS)

        self.encryption_util = EncryptionUtil(task_manager=None)
        self.encryption_helper = EncryptionAtRestHelper(self.log)

        self.any_encryption_enabled = any([
            self.enable_encryption_at_rest,
            self.enable_config_encryption_at_rest,
            self.enable_log_encryption_at_rest,
            self.enable_audit_encryption_at_rest,
        ])
        # Secrets we create are tracked and cleaned up in tearDown (mirrors the
        # eventing EAR suite) so they don't accumulate across runs / trip ns_server's
        # "name must be unique".
        self.created_secret_ids = []
        if self.any_encryption_enabled:
            self._setup_encryption_at_rest()
        self.log.info("============== FTS-EAR setUp completed ==============")

    def tearDown(self):
        self.log.info("============== FTS-EAR tearDown ==============")
        # Clean up the cluster (buckets/indexes) first, then drop the secrets we made
        # (a bucket-encryption secret can't be deleted while a bucket still uses it).
        super(FTSEncryptionBaseTest, self).tearDown()
        for secret_id in getattr(self, "created_secret_ids", []):
            try:
                RestConnection(self.master).delete_secret(secret_id)
                self.log.info(f"Deleted encryption secret {secret_id}")
            except Exception as err:
                self.log.warning(f"Failed to delete secret {secret_id}: {err}")

    # ==================================================================
    # Encryption setup / secret management
    # ==================================================================
    def _kmip_params(self):
        """Load KMIP connection params from pytests/kmip_config.json (only when KMIP is requested)."""
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "kmip_config.json")
        with open(config_path, "r") as handle:
            kmip_config = json.load(handle)
        return {
            "kmip_key_uuid": self._input.param("kmip_key_uuid", None),
            "client_certs_path": "/etc/couchbase/certs/",
            "KMIP_pkcs8_file_name": self._input.param("KMIP_pkcs8_file_name", "client-key-pkcs8.pem"),
            "KMIP_cert_file_name": self._input.param("KMIP_cert_file_name", "client_cert_with_appid2.pem"),
            "private_key_passphrase": self._input.param("private_key_passphrase", None),
            "kmip_host_name": kmip_config["host_name"],
        }

    def _setup_encryption_at_rest(self):
        """Create the requested encryption secrets and configure encryption at rest.

        Uses the shared EncryptionUtil.setup_encryption_at_rest() so FTS behaves
        consistently with the other services. This enables the "other" encryption
        types too -- config / log / audit (cluster-wide) and the optional KMIP KEK --
        not just bucket/data encryption. set_encryption_ids() writes the resulting
        secret IDs (encryption_at_rest_id / config_/log_/audit_encryption_at_rest_id /
        KMIP_id) back onto this test object.

        FTSBaseTest.setUp() has already created the (empty) buckets, so enabling bucket
        encryption now means every FTS segment written during indexing is encrypted
        from the start.
        """
        # The emp document generator calls random.seed(0, 1) (documentgenerator.py),
        # which pins the global PRNG to a deterministic state. EncryptionUtil generates
        # secret names from that same PRNG, so after the first data load the generated
        # names start repeating and ns_server rejects them with "name must be unique".
        # Reseed from OS entropy so each secret gets a genuinely unique name.
        random.seed()

        kmip = {}
        if self.create_KMIP_secret or any([
            self.KMIP_for_config_encryption,
            self.KMIP_for_log_encryption,
            self.KMIP_for_audit_encryption,
        ]):
            kmip = self._kmip_params()

        result = self.encryption_util.setup_encryption_at_rest(
            cluster_master=self.master,
            bypass_encryption_func=lambda: self.encryption_util.bypass_encryption_restrictions(self.master),
            create_KMIP_secret=self.create_KMIP_secret,
            enable_encryption_at_rest=self.enable_encryption_at_rest,
            enable_config_encryption_at_rest=self.enable_config_encryption_at_rest,
            enable_log_encryption_at_rest=self.enable_log_encryption_at_rest,
            enable_audit_encryption_at_rest=self.enable_audit_encryption_at_rest,
            secret_rotation_interval=self.secret_rotation_interval,
            kmip_key_uuid=kmip.get("kmip_key_uuid"),
            client_certs_path=kmip.get("client_certs_path"),
            KMIP_pkcs8_file_name=kmip.get("KMIP_pkcs8_file_name"),
            KMIP_cert_file_name=kmip.get("KMIP_cert_file_name"),
            private_key_passphrase=kmip.get("private_key_passphrase"),
            kmip_host_name=kmip.get("kmip_host_name"),
            KMIP_for_config_encryption=self.KMIP_for_config_encryption,
            config_dekLifetime=self.config_dekLifetime,
            config_dekRotationInterval=self.config_dekRotationInterval,
            KMIP_for_log_encryption=self.KMIP_for_log_encryption,
            log_dekLifetime=self.log_dekLifetime,
            log_dekRotationInterval=self.log_dekRotationInterval,
            KMIP_for_audit_encryption=self.KMIP_for_audit_encryption,
            audit_dekLifetime=self.audit_dekLifetime,
            audit_dekRotationInterval=self.audit_dekRotationInterval,
        )
        # Write encryption_at_rest_id / config_/log_/audit_encryption_at_rest_id / KMIP_id onto self
        self.encryption_util.set_encryption_ids(self, result)
        # Track every secret we created so tearDown can clean them up.
        for secret_id in (self.encryption_at_rest_id, self.config_encryption_at_rest_id,
                          self.log_encryption_at_rest_id, self.audit_encryption_at_rest_id,
                          self.KMIP_id):
            if secret_id is not None and secret_id not in self.created_secret_ids:
                self.created_secret_ids.append(secret_id)
        self.log.info(
            "Encryption-at-rest IDs -> data={0}, config={1}, log={2}, audit={3}, kmip={4}".format(
                self.encryption_at_rest_id, self.config_encryption_at_rest_id,
                self.log_encryption_at_rest_id, self.audit_encryption_at_rest_id, self.KMIP_id,
            )
        )

        if self.enable_encryption_at_rest:
            self.assertIsNotNone(
                self.encryption_at_rest_id,
                "Failed to create bucket encryption KEK (encryption_at_rest_id is None)",
            )
            # Enable on buckets that already exist. Tests that create buckets later
            # (e.g. the vector test, run with no_buckets=True) call
            # _enable_encryption_on_buckets() themselves after creating them.
            if self._cb_cluster.get_buckets():
                self._enable_encryption_on_buckets()
            else:
                self.log.info(
                    "No buckets present at setUp; bucket encryption will be enabled "
                    "by the test after it creates its buckets"
                )

    def _enable_encryption_on_buckets(self, buckets=None):
        """Enable encryption at rest on the given buckets (default: all current buckets).

        Exposed separately so tests that create buckets AFTER setUp (e.g. the vector
        test, which builds its own bucket/scope/collection structure) can encrypt those
        buckets too, using the same KEK created in setUp.
        """
        if not self.enable_encryption_at_rest or self.encryption_at_rest_id is None:
            return
        rest = RestConnection(self.master)
        if buckets is None:
            buckets = self._cb_cluster.get_buckets()
        self.assertTrue(len(buckets) > 0, "No buckets present to enable encryption on")
        for bucket in buckets:
            bucket_name = bucket if isinstance(bucket, str) else bucket.name
            status, response = rest.enable_bucket_encryption(bucket_name, self.encryption_at_rest_id)
            if not status:
                raise Exception(
                    f"Failed to enable encryption at rest on bucket {bucket_name}: {response}"
                )
            self.log.info(f"Enabled bucket encryption at rest on '{bucket_name}'")

    # ==================================================================
    # On-disk validation helpers
    # ==================================================================
    def _assert_encryption_results(self, results, label, require_pass=True):
        """Fail the test if any node reported a failed encryption validation.

        When require_pass=True, also fail if NO node actually passed (e.g. every node
        was skipped because the @fts path / files were not found) -- that would
        otherwise silently mask a broken validation for files we know must exist.
        """
        all_failed = []
        passed_nodes = 0
        for node_ip, result in results.items():
            status = result.get("status", "unknown")
            if status == "passed":
                passed_nodes += 1
                self.log.info(f"[{label}] {node_ip}: PASSED ({result})")
            elif status == "skipped":
                self.log.warning(f"[{label}] {node_ip}: SKIPPED - {result.get('reason')}")
            else:
                self.log.error(f"[{label}] {node_ip}: FAILED - {result}")
                for entry in result.get("failed_files", []):
                    all_failed.append((node_ip, entry))
        self.assertEqual(
            all_failed, [],
            f"[{label}] file encryption validation failed: {all_failed}",
        )
        if require_pass:
            self.assertGreater(
                passed_nodes, 0,
                f"[{label}] no node passed validation (all skipped/empty) - "
                f"check the @fts data path and that files exist: {results}",
            )

    def _validate_fts_log_encryption(self, fts_nodes, step=""):
        """Validate fts.log encryption, handling the gzip-rotation finding softly.

        Active/uncompressed logs must be encrypted (hard). Gzipped rotations that are
        not encrypted are a known ns_server gap: logged + counted as a tracked finding,
        and only fail the test when enforce_log_rotation_encryption=True.
        """
        self.log.info(f"{step}Validating fts.log encryption on disk (log encryption enabled)")
        log_key_ids = [self.log_encryption_at_rest_id] if self.log_encryption_at_rest_id else None
        results = self.encryption_helper.verify_fts_log_files_encrypted(
            fts_nodes, expected_key_ids=log_key_ids
        )

        rotation_findings = []
        for node_ip, result in results.items():
            for entry in result.get("rotation_findings", []):
                rotation_findings.append((node_ip, entry))

        if rotation_findings:
            self.log.warning(
                f"{step}TRACKED FINDING: {len(rotation_findings)} gzip log rotation(s) "
                f"not encrypted (ns_server log-rotation gap): {rotation_findings}"
            )

        # Hard failures (a non-gzip plaintext log) always fail; rotation findings only
        # fail when enforcement is explicitly turned on.
        self._assert_encryption_results(results, "fts.log")
        if self.enforce_log_rotation_encryption:
            self.assertEqual(
                rotation_findings, [],
                f"{step}log rotation encryption enforced but found unencrypted gzip "
                f"rotations: {rotation_findings}",
            )
        self.log.info(f"{step}PASSED - active fts.log encrypted")

    def _validate_segments_encrypted(self, fts_nodes, label="segments"):
        results = self.encryption_helper.verify_fts_segment_files_encrypted(
            fts_nodes, sensitive_terms=self.sensitive_terms,
            expected_key_ids=[self.encryption_at_rest_id],
        )
        self._assert_encryption_results(results, label)

    # ==================================================================
    # Index / data / query helpers
    # ==================================================================
    def _default_bucket(self):
        return self._cb_cluster.get_bucket_by_name(self.default_bucket_name)

    def _create_default_index(self, index_name="fts_ear_idx"):
        index = self.create_index(bucket=self._default_bucket(), index_name=index_name)
        self.wait_for_indexing_complete()
        return index

    def _run_emp_data_op(self, num_ops, op_type, start=0, end=0):
        """Run a create/update/delete op against the default bucket's emp data."""
        gen = SDKDataLoader(
            num_ops=num_ops,
            percent_create=100 if op_type == "create" else 0,
            percent_update=100 if op_type == "update" else 0,
            percent_delete=100 if op_type == "delete" else 0,
            scope="_default", collection="_default", json_template="emp",
            op_type=op_type, start=start, end=end,
            username=self._input.membase_settings.rest_username,
            password=self._input.membase_settings.rest_password,
        )
        tasks = self._cb_cluster.async_load_bucket_from_generator(self._default_bucket(), gen)
        for task in tasks:
            task.result()

    def _match_all_hits(self, index, zero_ok=False):
        hits, _, _, _ = index.execute_query({"match_all": {}}, zero_results_ok=zero_ok)
        return hits

    def _force_merge_and_wait(self, index_name, timeout=180):
        """Trigger an FTS segment merge (compaction) and wait until it completes.

        Disabling encryption / dropping DEKs only rewrites segments on a merge or flush,
        so tests force a merge rather than assume the rewrite is instant.
        """
        rest = RestConnection(self.master)
        status, resp = rest.start_fts_index_compaction(index_name)
        self.log.info(f"Triggered merge on '{index_name}': status={status}, resp={resp}")
        deadline = time.time() + timeout
        while time.time() < deadline:
            status, tasks = rest.get_fts_index_compactions(index_name)
            if not (isinstance(tasks, dict) and tasks.get("tasks")):
                self.log.info(f"Merge/compaction complete on '{index_name}'")
                return
            self.sleep(5, f"waiting for merge on '{index_name}' to finish")
        self.log.warning(f"Merge on '{index_name}' did not report completion within {timeout}s")

    def _require_encryption_enabled(self):
        if not self.enable_encryption_at_rest:
            self.skipTest("Encryption at rest not enabled. Set enable_encryption_at_rest=True")
        self.assertIsNotNone(self.encryption_at_rest_id, "encryption_at_rest_id not set")

    # ==================================================================
    # FTS GetInUseKeys (DEK) helpers (:8094/api/encryption/GetInUseKeys)
    # ==================================================================
    def _bucket_uuid(self, bucket_name=None):
        bucket_name = bucket_name or self.default_bucket_name
        return RestConnection(self.master).get_bucket_json(bucket_name).get("uuid")

    def _fts_in_use_deks(self, bucket_name=None):
        """Return (merged_in_use_map, dek_ids_for_bucket) from FTS GetInUseKeys."""
        fts_nodes = self._cb_cluster.get_fts_nodes()
        merged = self.encryption_helper.get_fts_in_use_keys(fts_nodes)
        bucket_uuid = self._bucket_uuid(bucket_name)
        deks = self.encryption_helper.fts_dek_ids_for_bucket(merged, bucket_uuid)
        self.log.info(
            f"FTS in-use DEKs for bucket '{bucket_name or self.default_bucket_name}' "
            f"({bucket_uuid}): {deks} | full map: {merged}"
        )
        return merged, deks

    def _poll_fts_for_new_dek(self, bucket_name, baseline_deks, timeout=300, label=""):
        """Poll GetInUseKeys until a DEK appears for the bucket that wasn't in baseline."""
        baseline = set(baseline_deks)
        deadline = time.time() + timeout
        while time.time() < deadline:
            _, deks = self._fts_in_use_deks(bucket_name)
            new = sorted(set(deks) - baseline)
            if new:
                self.log.info(f"{label}New FTS DEK(s) detected: {new} (baseline {sorted(baseline)})")
                return new
            self.sleep(15, f"{label}waiting for a new FTS DEK (baseline {sorted(baseline)})")
        self.fail(f"{label}No new FTS DEK appeared within {timeout}s (baseline {sorted(baseline)})")

    def _poll_fts_until_deks_gone(self, bucket_name, old_deks, timeout=300, label=""):
        """Poll GetInUseKeys until none of old_deks are in use for the bucket anymore."""
        old = set(old_deks)
        deadline = time.time() + timeout
        while time.time() < deadline:
            _, deks = self._fts_in_use_deks(bucket_name)
            still_present = old.intersection(deks)
            if not still_present:
                self.log.info(f"{label}old DEK(s) {sorted(old)} retired (current {deks})")
                return
            self.sleep(15, f"{label}waiting for old DEK(s) {sorted(still_present)} to retire")
        self.fail(f"{label}old DEK(s) {sorted(old)} still in use after {timeout}s")

    def _set_bucket_dek_rotation(self, bucket_name, interval_s, lifetime_s):
        rest = RestConnection(self.master)
        status, resp = rest.set_bucket_dek_rotation_interval(bucket_name, interval_s)
        self.assertTrue(status, f"set_bucket_dek_rotation_interval failed: {resp}")
        status, resp = rest.set_bucket_dek_lifetime(bucket_name, lifetime_s)
        self.assertTrue(status, f"set_bucket_dek_lifetime failed: {resp}")
        self.log.info(f"bucket '{bucket_name}' DEK interval={interval_s}s lifetime={lifetime_s}s")

    def _check_dek_in_segments(self, fts_nodes, dek_ids, label=""):
        """Q4 (best-effort): is an in-use DEK id embedded verbatim inside a .zap segment?

        Logged, not asserted -- the on-disk key-id encoding (raw UUID vs binary) is still
        being confirmed. This surfaces whether the GetInUseKeys DEK id appears as-is.
        """
        if not dek_ids:
            return False
        for node in fts_nodes:
            fts_dir = self.encryption_helper.get_fts_data_path(node)
            for seg in self.encryption_helper._find_files(node, fts_dir, "-type f -name '*.zap'", limit=10):
                ok, matched = self.encryption_helper.file_contains_any(node, seg, dek_ids)
                if ok:
                    self.log.info(f"{label}in-use DEK id '{matched}' found inside {seg} on {node.ip}")
                    return True
        self.log.warning(
            f"{label}none of the in-use DEK ids {dek_ids} found verbatim in .zap segments "
            f"(embedded key-id form may differ -- best-effort check)"
        )
        return False

    # ==================================================================
    # Rebalance / failover helpers
    # ==================================================================
    def _free_server(self):
        """Return an ini server that is NOT currently in the cluster (for rebalance-in)."""
        in_cluster = {s.ip for s in self._cb_cluster.get_nodes()}
        for server in self._input.servers:
            if server.ip not in in_cluster:
                return server
        return None

    def _bucket_dek_on_node(self, node, bucket_name=None):
        """Per-node GetInUseKeys DEK ids for a bucket.

        Best-effort: returns [] (and logs) if the endpoint isn't present yet, so the
        rebalance/failover tests can still run their segment/count/query validations on
        builds without the GetInUseKeys endpoint. Becomes a hard assertion in the caller
        once the endpoint build is available.
        """
        bucket_name = bucket_name or self.default_bucket_name
        try:
            merged = self.encryption_helper.get_fts_in_use_keys([node])
            return self.encryption_helper.fts_dek_ids_for_bucket(merged, self._bucket_uuid(bucket_name))
        except Exception as err:
            self.log.warning(
                f"per-node GetInUseKeys on {node.ip} failed (endpoint not in this build yet?): {err}"
            )
            return []

    def _post_topology_validation(self, label, baseline_hits=None, index=None):
        """Common post-rebalance/failover validation.

        Indexing settles, partitions stay balanced, doc counts reconcile with the bucket,
        all current FTS nodes' segments are encrypted, and (optionally) hit count is
        unchanged vs a captured baseline.
        """
        self.wait_for_indexing_complete()
        for idx in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(idx)
        self.validate_index_count(equal_bucket_doc_count=True)
        self._validate_segments_encrypted(self._cb_cluster.get_fts_nodes(), label)
        if baseline_hits is not None and index is not None:
            self.assertEqual(self._match_all_hits(index), baseline_hits,
                             f"{label}: hit count changed (expected {baseline_hits})")

    def _run_queries_no_error(self, index, duration_s=60, label=""):
        """Query the index repeatedly for duration_s; assert none of them error.

        Used during a concurrent operation (rebalance/rotation) where exact hit counts
        may transiently vary, but queries must never fail. Exact-count is asserted
        afterward by _post_topology_validation.
        """
        errors = []
        deadline = time.time() + duration_s
        attempts = 0
        while time.time() < deadline:
            attempts += 1
            try:
                self._match_all_hits(index, zero_ok=True)
            except Exception as err:
                errors.append(str(err))
            self.sleep(5, f"{label}querying during concurrent operation")
        self.log.info(f"{label}ran {attempts} queries during operation; errors={errors}")
        self.assertEqual(errors, [], f"{label}queries errored during concurrent operation: {errors}")
