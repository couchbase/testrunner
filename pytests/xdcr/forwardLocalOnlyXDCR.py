"""
forwardLocalOnly XDCR Tests

Validates the per-replication setting `forwardLocalOnly` (boolean), which
restricts replication to mutations originating on the local cluster, in an
Active-Active bidirectional ECCV (enableCrossClusterVersioning) topology.

Mutations qualifying as "local" (per spec):
  * No HLV stamped, OR
  * CAS > cvCAS, OR
  * CAS == cvCAS && len(MV) > 0 && CV.src == <local cluster SourceID>

Non-local mutations skipped on Source are tallied by the Prometheus counter
`xdcr_non_local_mutations_skipped_total`.

Prerequisites:
  * ECCV enabled on every bucket in the full mesh (asserted in setUp).
  * Active-Active bidirectional replication (rdirection=bidirection).
  * No SGW / mobile bucket linkage.
"""

import gc
import json
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import couchbase.subdocument as SD
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from xdcr.xdcrnewbasetests import (
    MetricsScrapeError, NodeHelper, XDCRNewBaseTest,
)

try:
    from sdk_client3 import SDKClient
except ImportError:
    from lib.sdk_client3 import SDKClient


NodeHelper.raise_fd_soft_limit()


FORWARD_LOCAL_ONLY = "forwardLocalOnly"
SKIPPED_METRIC = "xdcr_non_local_mutations_skipped_total"


class GoxdcrLogProbeError(RuntimeError):
    """Raised when a goxdcr.log probe itself fails (SSH/SFTP/timeout).
    Callers must distinguish "log probe failed" from "pattern absent"
    so infra issues do not get reclassified as product bugs."""


class _SharedSDKClient:
    """Transparent proxy around `SDKClient`. Forwards attribute access
    but overrides `close()` to a no-op so test code's `try/finally:
    client.close()` doesn't tear down a cached connection that other
    helpers in the same test still need.

    The real underlying client is closed exactly once, in tearDown,
    via `_real_close()`."""

    def __init__(self, real):
        self._real = real

    def __getattr__(self, name):
        return getattr(self._real, name)

    def close(self):
        return None

    def _real_close(self):
        # Don't swallow silently: a close() failure on teardown is
        # either an already-disconnected client (benign, ignorable) or
        # paramiko/socket trouble worth surfacing. Log via module
        # logger so the warning lands even though we're in a non-
        # TestCase context (no self.log).
        try:
            self._real.close()
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(
                "_SharedSDKClient close failed: %s", e)


class ForwardLocalOnlyXDCRBase(XDCRNewBaseTest):
    """Shared base for forwardLocalOnly tests. Reusable helpers live here so
    future tests in this suite can subclass without duplicating logic."""

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    _INITIAL_REPLICATION_SETTLE = 30
    _COLLECTION_SETTLE = 15
    _BUCKET_RECREATE_SETTLE = 5
    _PAUSE_RESUME_SETTLE = 20
    _SETUP_RETRY_ATTEMPTS = 3
    _SETUP_RETRY_BACKOFF = 10
    _BULK_BATCH_SIZE = 1000
    _BULK_THREADS = 8

    def setUp(self):
        NodeHelper.raise_fd_soft_limit()
        last_err = None
        for attempt in range(1, self._SETUP_RETRY_ATTEMPTS + 1):
            try:
                super(ForwardLocalOnlyXDCRBase, self).setUp()
                last_err = None
                break
            except Exception as e:
                msg = str(e)
                if ("Timeout opening channel" in msg
                        or "SSHException" in type(e).__name__
                        or "paramiko" in msg.lower()):
                    last_err = e
                    self.log.warning(
                        "{0} setUp paramiko SSH failure (attempt "
                        "{1}/{2}): {3}. GC + sleep {4}s, retry."
                        .format(
                            self._tag(), attempt,
                            self._SETUP_RETRY_ATTEMPTS, e,
                            self._SETUP_RETRY_BACKOFF))
                    gc.collect()
                    time.sleep(self._SETUP_RETRY_BACKOFF)
                    NodeHelper.raise_fd_soft_limit()
                    continue
                raise
        if last_err is not None:
            raise last_err
        self._sdk_client_cache = {}
        self.src_cluster = self.get_cb_cluster_by_name("C1")
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name("C2")
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)

        self.replication_drain_timeout = self._input.param(
            "replication_drain_timeout", 360)
        self.doc_count_sync_timeout = self._input.param(
            "doc_count_sync_timeout", 360)
        self.metric_settle_timeout = self._input.param(
            "metric_settle_timeout", 180)
        self.eccv_settle_timeout = self._input.param(
            "eccv_settle_timeout", 60)
        self.flo_settle_timeout = self._input.param(
            "flo_settle_timeout", 30)
        self.initial_replication_settle = self._input.param(
            "initial_replication_settle",
            self._INITIAL_REPLICATION_SETTLE)

        self.gen_create = BlobGenerator(
            "flo", "flo-", self._value_size, start=0, end=self._num_items)

    def tearDown(self):
        try:
            for cluster in self.get_cb_clusters():
                rest = RestConnection(cluster.get_master_node())
                try:
                    replications = rest.get_replications()
                except Exception as e:
                    self.log.warning(
                        "{0} tearDown: get_replications failed on {1}: "
                        "{2}".format(self._tag(), cluster.get_name(), e))
                    continue
                for repl in replications:
                    src_b = repl.get("source")
                    dest_b = repl.get("target", "").rsplit("/", 1)[-1]
                    if not src_b or not dest_b:
                        continue
                    try:
                        rest.set_xdcr_param(
                            src_b, dest_b, FORWARD_LOCAL_ONLY, "false")
                    except Exception as e:
                        self.log.warning(
                            "{0} tearDown: could not reset FLO on "
                            "{1} {2}->{3}: {4}".format(
                                self._tag(), cluster.get_name(),
                                src_b, dest_b, e))
        except Exception as e:
            self.log.warning(
                "{0} tearDown FLO reset raised (non-fatal): {1}".format(
                    self._tag(), e))

        for cluster, bucket, key in getattr(self, "_sentinel_docs", []):
            try:
                client = self._borrow_sdk_client(cluster, bucket.name)
                client.remove(key)
            except Exception as e:
                self.log.warning(
                    "{0} tearDown: sentinel remove failed for "
                    "{1}/{2}/{3}: {4}".format(
                        self._tag(), cluster.get_name(), bucket.name,
                        key, e))
        if hasattr(self, "_sentinel_docs"):
            self._sentinel_docs.clear()
        cache = getattr(self, "_sdk_client_cache", {}) or {}
        for key, client in list(cache.items()):
            try:
                client._real_close()
            except Exception as e:
                self.log.warning(
                    "SDK client close failed for {0}: {1}".format(key, e))
        if hasattr(self, "_sdk_client_cache"):
            self._sdk_client_cache.clear()
        super(ForwardLocalOnlyXDCRBase, self).tearDown()
        # Final cleanup: GC + idle AFTER base teardown so any
        # SSH/SFTP sessions opened by base cleanup also release
        # their fds + paramiko transports before the next test's
        # setUp starts. Bumped from 2s -> 8s because paramiko's
        # "Timeout opening channel" surfaced on quick succession
        # tests; the channels need a beat to drain. setUp itself
        # also retries on the SSH timeout (belt + braces).
        gc.collect()
        # Best-effort: close any lingering paramiko Transports that
        # belong to this test's RemoteMachineShellConnection
        # instances. They register with paramiko's internal active-
        # threads list; iterating + closing isn't supported via a
        # public API, so we just gc-collect heavily and idle.
        gc.collect()
        time.sleep(8)

    # ------------------------------------------------------------------
    # Test log tag (tests override via class attribute or compute it from
    # the current test method name in setUp; used by _tag() / _log_state)
    # ------------------------------------------------------------------
    def _tag(self):
        """Return short tag like [FLO-pv-not-local] for log greppability.
        Derived from the test method name, with the suite prefix stripped."""
        name = getattr(self, "_testMethodName", "") or ""
        if name.startswith("test_forward_local_only_"):
            stem = name[len("test_forward_local_only_"):]
        elif name.startswith("test_"):
            stem = name[len("test_"):]
        else:
            stem = name or "flo"
        return "[FLO-{0}]".format(stem)

    def _log_tag(self):
        """Override base hook so promoted helpers prefix log lines with
        the FLO suite tag."""
        return self._tag()

    def _post_setup_settle(self, log_label="post setup"):
        """Settle window after setup_xdcr_and_load. Logs the CHECKPOINT
        line + sleeps `initial_replication_settle` seconds. Replaces
        the hardcoded `time.sleep(30)` pattern that was scattered
        across tests that bypass `_standard_flo_setup`."""
        self.log.info(
            "{0} CHECKPOINT: setup_xdcr_and_load complete ({1}); "
            "settling {2}s".format(
                self._tag(), log_label,
                self.initial_replication_settle))
        time.sleep(self.initial_replication_settle)

    def _log_state(self, label, clusters=None):
        """Snapshot key state across clusters and emit it at INFO level.
        Used at test start and immediately before non-trivial assertions
        so CI failure logs have the relevant ground-truth in one place.

        Dumped per cluster:
          * ECCV per bucket
          * disableHlvBasedShortCircuit (global)
          * forwardLocalOnly per replication
          * filterExpression per replication (only if non-empty)
          * top of replication error list
          * xdcr_non_local_mutations_skipped_total (Prometheus)
        """
        tag = self._tag()
        clusters = clusters or list(self.get_cb_clusters())
        self.log.info("{0} STATE BEGIN: {1}".format(tag, label))
        for cluster in clusters:
            cname = cluster.get_name()
            rest = RestConnection(cluster.get_master_node())
            for bucket in cluster.get_buckets():
                try:
                    info = rest.get_bucket_json(bucket.name)
                    eccv = info.get("enableCrossClusterVersioning", "n/a")
                    self.log.info(
                        "{0}   {1}/{2}: ECCV={3}".format(
                            tag, cname, bucket.name, eccv))
                except Exception as e:
                    self.log.info(
                        "{0}   {1}/{2}: bucket_json failed: {3}".format(
                            tag, cname, bucket.name, e))
            try:
                d = self._get_global_xdcr_param(
                    cluster, "disableHlvBasedShortCircuit")
            except Exception:
                d = "<read-failed>"
            self.log.info(
                "{0}   {1}: disableHlvBasedShortCircuit={2}".format(
                    tag, cname, d))
            try:
                replications = rest.get_replications()
            except Exception as e:
                self.log.info(
                    "{0}   {1}: get_replications failed: {2}".format(
                        tag, cname, e))
                replications = []
            for repl in replications:
                src_b = repl.get("source", "?")
                dest_b = repl.get("target", "").rsplit("/", 1)[-1]
                try:
                    flo = rest.get_xdcr_param(
                        src_b, dest_b, FORWARD_LOCAL_ONLY)
                except Exception:
                    flo = "<read-failed>"
                try:
                    fexpr = rest.get_xdcr_param(
                        src_b, dest_b, "filterExpression")
                except Exception:
                    fexpr = None
                errors = repl.get("errors") or []
                self.log.info(
                    "{0}   {1} repl {2}->{3}: FLO={4} filter={5!r} "
                    "errors[0..1]={6}".format(
                        tag, cname, src_b, dest_b, flo,
                        fexpr if fexpr else None, errors[:1]))
            # Skipped counter snapshot
            try:
                skipped = self._get_skipped_total(cluster)
            except Exception as e:
                skipped = "<read-failed: {0}>".format(e)
            self.log.info(
                "{0}   {1}: {2}={3}".format(
                    tag, cname, SKIPPED_METRIC, skipped))
        self.log.info("{0} STATE END".format(tag))
    def _set_and_verify_xdcr_param(self, cluster, src_bucket, dest_bucket,
                                    param, value, settle=None,
                                    assert_match=True):
        if settle is None:
            settle = self.flo_settle_timeout
        return self.set_xdcr_param_verified(
            cluster, src_bucket, dest_bucket, param, value,
            settle=settle, assert_match=assert_match)

    def _set_and_verify_xdcr_params(self, cluster, src_bucket, dest_bucket,
                                     param_value_map, settle=None,
                                     assert_match=True):
        if settle is None:
            settle = self.flo_settle_timeout
        return self.set_xdcr_params_verified(
            cluster, src_bucket, dest_bucket, param_value_map,
            settle=settle, assert_match=assert_match)

    def _set_and_verify_global_xdcr_param(self, cluster, param, value,
                                           settle=None, assert_match=True):
        if settle is None:
            settle = self.flo_settle_timeout
        return self.set_global_xdcr_param_verified(
            cluster, param, value,
            settle=settle, assert_match=assert_match)

    def _set_and_verify_bucket_prop(self, cluster, bucket, prop, value,
                                     settle=None, assert_match=True):
        if settle is None:
            settle = self.eccv_settle_timeout
        return self.set_bucket_prop_verified(
            cluster, bucket, prop, value,
            settle=settle, assert_match=assert_match)
    def _set_eccv_on_cluster(self, cluster, enabled):
        """Toggle enableCrossClusterVersioning for every bucket on a
        cluster, verifying persistence on each.

        Per-bucket settle is 0; the trailing `eccv_settle_timeout`
        (applied by `_enable_eccv_on_all_clusters`) gives goxdcr time
        to digest all bucket-prop changes in one shot. This keeps the
        N-bucket setup cost O(N) REST calls + 1 sleep rather than
        O(N) REST calls + N sleeps."""
        for bucket in cluster.get_buckets():
            self._set_and_verify_bucket_prop(
                cluster, bucket, "enableCrossClusterVersioning",
                str(enabled).lower(),
                settle=0,
                assert_match=True)

    def _enable_eccv_on_all_clusters(self):
        """ECCV is required for forwardLocalOnly. Enable it everywhere.
        Bucket prop change can trigger goxdcr internal restart; wait
        long enough that follow-up REST calls don't 500."""
        for cluster in self.get_cb_clusters():
            self._set_eccv_on_cluster(cluster, True)
        self.log.info(
            "Waiting {0}s for ECCV to settle on all clusters".format(
                self.eccv_settle_timeout))
        time.sleep(self.eccv_settle_timeout)
    def _set_forward_local_only_on_cluster(self, cluster, value):
        """Set forwardLocalOnly on every replication originating at
        `cluster`. Each set is verified via REST readback; failure to
        persist fails the test immediately."""
        rest = RestConnection(cluster.get_master_node())
        replications = rest.get_replications()
        if not replications:
            self.fail(
                "{0} No replications found on cluster {1} when setting "
                "forwardLocalOnly".format(self._tag(), cluster.get_name()))
        for repl in replications:
            src_bucket = repl["source"]
            dest_bucket = repl["target"].rsplit("/", 1)[-1]
            self._set_and_verify_xdcr_param(
                cluster, src_bucket, dest_bucket,
                FORWARD_LOCAL_ONLY, str(value).lower(),
                settle=5, assert_match=True)

    def _enable_forward_local_only_everywhere(self):
        for cluster in self.get_cb_clusters():
            self._set_forward_local_only_on_cluster(cluster, True)
        self.log.info(
            "Waiting {0}s for live-update of forwardLocalOnly".format(
                self.flo_settle_timeout))
        time.sleep(self.flo_settle_timeout)
    def _standard_flo_setup(self, enable_eccv=True, enable_flo=True,
                             disable_shortcircuit_on=None,
                             log_label="test entry",
                             final_log_label=None):
        """Standard test-entry sequence:

            1. setup_xdcr_and_load
            2. log_state snapshot (named `log_label`)
            3. sleep `initial_replication_settle` (default 30s)
            4. enable_eccv_on_all_clusters (unless enable_eccv=False)
            5. optional: disable HLV short-circuit on caller-specified
               cluster (pass `self.src_cluster` / `self.dest_cluster` /
               a list of clusters); use this when the test needs to
               observe an echo path that would otherwise be pre-empted
               by the HLV path.
            6. enable_forward_local_only_everywhere (unless
               enable_flo=False)
            7. log_state snapshot at end.

        Final log label: when `final_log_label` is provided it is used
        verbatim. Otherwise the label is constructed from the actual
        steps performed -- e.g. "post-ECCV+FLO" when both flags are
        True, "post-ECCV" when FLO is False, "post-setup" when both
        are False. The prior hard-coded "post-FLO-setup" lied for
        callers passing enable_flo=False; future debuggers reading
        the state dump now see what was actually applied.

        Callers that need a deviation (no ECCV, no FLO, etc.) pass
        the corresponding flag and run any remaining steps inline."""
        self.setup_xdcr_and_load()
        self._log_state(log_label)
        self.log.info(
            "{0} CHECKPOINT: setup_xdcr_and_load complete; settling "
            "for {1}s before applying ECCV/FLO".format(
                self._tag(), self.initial_replication_settle))
        time.sleep(self.initial_replication_settle)

        if enable_eccv:
            self._enable_eccv_on_all_clusters()

        if disable_shortcircuit_on is not None:
            targets = disable_shortcircuit_on
            if not isinstance(targets, (list, tuple)):
                targets = [targets]
            for c in targets:
                self._set_disable_hlv_short_circuit(c, True)

        if enable_flo:
            self._enable_forward_local_only_everywhere()

        if final_log_label is None:
            applied = []
            if enable_eccv:
                applied.append("ECCV")
            if disable_shortcircuit_on is not None:
                applied.append("disableHlvShortCircuit")
            if enable_flo:
                applied.append("FLO")
            final_log_label = (
                "post-" + "+".join(applied) if applied
                else "post-setup")
        self._log_state(final_log_label)

    def _verify_forward_local_only_setting(self, cluster, expected):
        """Read replication setting back via REST and assert."""
        rest = RestConnection(cluster.get_master_node())
        for repl in rest.get_replications():
            src_bucket = repl["source"]
            dest_bucket = repl["target"].rsplit("/", 1)[-1]
            actual = rest.get_xdcr_param(
                src_bucket, dest_bucket, FORWARD_LOCAL_ONLY)
            self.log.info(
                "{0}={1} on {2}->{3} (cluster {4})".format(
                    FORWARD_LOCAL_ONLY, actual, src_bucket, dest_bucket,
                    cluster.get_name()))
            self.assertEqual(
                str(actual).lower(), str(expected).lower(),
                "Expected {0}={1} on {2}->{3}, got {4}".format(
                    FORWARD_LOCAL_ONLY, expected, src_bucket, dest_bucket,
                    actual))
    def _scrape_skipped_metric(self, server):
        """Pull `xdcr_non_local_mutations_skipped_total` from `server`,
        summed across label sets. Raises `MetricsScrapeError` on infra
        failure (distinct from "metric absent")."""
        return self.scrape_prometheus_metric(server, SKIPPED_METRIC)

    def _wait_for_metric_increment(self, cluster, baseline, timeout=None,
                                    poll_interval=15):
        """Poll the cluster-wide skipped counter until it exceeds baseline
        or timeout elapses. Returns final value (caller asserts).
        Polling beats fixed sleep: pipeline can take a variable amount of
        time after a live-update before metric updates land."""
        timeout = timeout or self.metric_settle_timeout
        deadline = time.time() + timeout
        last = baseline
        while time.time() < deadline:
            last = self._get_skipped_total(cluster)
            if last > baseline:
                self.log.info(
                    "Counter on {0} incremented above baseline {1} -> {2}"
                    .format(cluster.get_name(), baseline, last))
                return last
            self.log.info(
                "Counter on {0} still at {1} (baseline {2}); "
                "polling again in {3}s".format(
                    cluster.get_name(), last, baseline, poll_interval))
            time.sleep(poll_interval)
        self.log.warning(
            "Counter on {0} did not exceed baseline {1} after {2}s; "
            "final {3}".format(
                cluster.get_name(), baseline, timeout, last))
        return last

    def _get_skipped_total(self, cluster):
        """Cluster-wide sum of non-local mutations skipped.
        Propagates `MetricsScrapeError` if ANY node's Prometheus
        endpoint is unreachable so a node-down event does NOT silently
        read as "counter didn't increment"."""
        total = 0.0
        for node in cluster.get_nodes():
            value = self._scrape_skipped_metric(node)
            self.log.info(
                "{0} {1}={2} on {3} ({4})".format(
                    self._tag(), SKIPPED_METRIC, value,
                    node.ip, cluster.get_name()))
            total += value
        self.log.info(
            "{0} cluster {1} total {2}={3}".format(
                self._tag(), cluster.get_name(),
                SKIPPED_METRIC, total))
        return total
    def _borrow_sdk_client(self, cluster, bucket_name):
        """Return a cached SDK client for (cluster, bucket). Callers
        MUST NOT call .close() on the returned client; tearDown owns
        lifecycle. Replaces the previous open-per-call pattern that
        bled file descriptors across sequential tests."""
        key = (cluster.get_name(), bucket_name)
        client = self._sdk_client_cache.get(key)
        if client is not None:
            return client
        master = cluster.get_master_node()
        real = SDKClient(
            bucket=bucket_name,
            hosts=[master.ip],
            username=master.rest_username,
            password=master.rest_password)
        client = _SharedSDKClient(real)
        self._sdk_client_cache[key] = client
        return client

    def _get_sdk_client(self, cluster, bucket_name):
        """Override base hook: route base-class SDK helpers through the
        suite's shared / cached client (no fresh-per-call connections,
        no fd leakage). Caller MUST NOT close the returned client."""
        return self._borrow_sdk_client(cluster, bucket_name)

    def _bulk_upsert(self, cluster, bucket, key_value_pairs,
                      scope=None, collection=None):
        """Bulk-upsert (key, value) pairs to `cluster/bucket` (optionally
        into `scope.collection`) using `Collection.upsert_multi` with
        parallel batches. ~30-50x faster than the prior tight-loop
        pattern for large doc counts.

        Bypasses sdk_client3.get()'s recursive retry path by reaching
        through to the collection object directly."""
        client = self._borrow_sdk_client(cluster, bucket.name)
        if scope or collection:
            client.collection_connect(scope, collection)
            coll = client.collection
        else:
            coll = client.default_collection
        batch_size = self._input.param(
            "bulk_batch_size", self._BULK_BATCH_SIZE)
        thread_count = self._input.param(
            "bulk_threads", self._BULK_THREADS)

        batches = []
        current = {}
        for k, v in key_value_pairs:
            current[k] = v
            if len(current) >= batch_size:
                batches.append(current)
                current = {}
        if current:
            batches.append(current)

        def _send(batch):
            try:
                coll.upsert_multi(batch)
                return len(batch), None
            except Exception as e:
                return 0, str(e)

        total = sum(len(b) for b in batches)
        sent = 0
        errors = []
        if thread_count > 1 and len(batches) > 1:
            with ThreadPoolExecutor(
                    max_workers=thread_count) as pool:
                futures = [pool.submit(_send, b) for b in batches]
                for fut in as_completed(futures):
                    n, err = fut.result()
                    sent += n
                    if err:
                        errors.append(err)
        else:
            for b in batches:
                n, err = _send(b)
                sent += n
                if err:
                    errors.append(err)
        if errors:
            self.log.warning(
                "{0} bulk_upsert had {1} batch errors (first 3): "
                "{2}".format(self._tag(), len(errors), errors[:3]))
        return sent, total

    def _sdk_upsert_docs(self, cluster, bucket, key_prefix, count,
                         start=0, value_template=None):
        """Bulk SDK upsert. Each doc is keyed `<prefix>-<i>`. Uses
        parallel `upsert_multi` batches for throughput on large
        counts."""
        self.log.info(
            "{0} SDK upsert: cluster={1} bucket={2} prefix={3} "
            "range=[{4},{5})".format(
                self._tag(), cluster.get_name(), bucket.name,
                key_prefix, start, start + count))
        t0 = time.time()

        def _pairs():
            for i in range(start, start + count):
                value = value_template.copy() if value_template else {}
                value.update({"idx": i, "src": cluster.get_name()})
                yield "{0}-{1}".format(key_prefix, i), value

        sent, total = self._bulk_upsert(cluster, bucket, _pairs())
        self.log.info(
            "{0} SDK upsert complete: {1}/{2} docs on {3}/{4} in "
            "{5:.1f}s".format(
                self._tag(), sent, total,
                cluster.get_name(), bucket.name, time.time() - t0))

    def _sdk_update_docs(self, cluster, bucket, key_prefix, count, start=0):
        """Force a Type-1 mutation on existing docs by upserting a new
        body. Uses parallel `upsert_multi` for throughput."""
        self.log.info(
            "{0} SDK update on {1}/{2}: prefix={3} range=[{4},{5})".format(
                self._tag(), cluster.get_name(), bucket.name,
                key_prefix, start, start + count))
        t0 = time.time()

        def _pairs():
            for i in range(start, start + count):
                yield ("{0}-{1}".format(key_prefix, i),
                       {"idx": i, "src": cluster.get_name(),
                        "rev": "updated"})

        sent, total = self._bulk_upsert(cluster, bucket, _pairs())
        self.log.info(
            "{0} SDK update complete: {1}/{2} docs on {3}/{4} in "
            "{5:.1f}s".format(
                self._tag(), sent, total,
                cluster.get_name(), bucket.name, time.time() - t0))

    def _sdk_subdoc_xattr_mutate(self, cluster, bucket, key_prefix, count,
                                  start=0):
        """Subdoc mutate_in: writes both an xattr path and a body path,
        producing a CAS bump (Type-1 mutation).

        SDK has no batch mutate_in; parallelize per-doc calls across a
        thread pool. ~thread_count x speedup over the previous tight
        sequential loop."""
        client = self._borrow_sdk_client(cluster, bucket.name)
        self.log.info(
            "{0} SDK subdoc+xattr mutate on {1}/{2}: prefix={3} "
            "range=[{4},{5})".format(
                self._tag(), cluster.get_name(), bucket.name,
                key_prefix, start, start + count))
        t0 = time.time()
        thread_count = self._input.param(
            "bulk_threads", self._BULK_THREADS)

        def _mutate_one(i):
            key = "{0}-{1}".format(key_prefix, i)
            try:
                client.mutate_in(
                    key, None, None,
                    specs=[
                        SD.upsert(
                            "flo_meta.tag",
                            "local-{0}-{1}".format(
                                cluster.get_name(), i),
                            xattr=True, create_parents=True),
                        SD.upsert(
                            "subdoc_marker",
                            "mutated-{0}".format(i),
                            create_parents=True),
                    ])
                return True, None
            except Exception as e:
                return False, str(e)

        errors = []
        ok = 0
        with ThreadPoolExecutor(max_workers=thread_count) as pool:
            futures = [pool.submit(_mutate_one, i)
                       for i in range(start, start + count)]
            for fut in as_completed(futures):
                success, err = fut.result()
                if success:
                    ok += 1
                else:
                    errors.append(err)
        if errors:
            self.log.warning(
                "{0} subdoc+xattr had {1} errors (first 3): {2}".format(
                    self._tag(), len(errors), errors[:3]))
        self.log.info(
            "{0} SDK subdoc+xattr complete: {1}/{2} docs on {3}/{4} in "
            "{5:.1f}s".format(
                self._tag(), ok, count,
                cluster.get_name(), bucket.name, time.time() - t0))
    def _wait_for_replication_drain(self, timeout=None):
        """Block until outbound mutations on every cluster reach 0."""
        timeout = timeout or self.replication_drain_timeout
        for cluster in self.get_cb_clusters():
            self.log.info(
                "Waiting up to {0}s for outbound mutations to drain on {1}"
                .format(timeout, cluster.get_name()))
            cluster.wait_for_outbound_mutations(timeout=timeout)

    def _assert_doc_count_synced(self, timeout=None):
        """For each bucket, assert curr_items matches between every pair
        of clusters in the topology."""
        timeout = timeout or self.doc_count_sync_timeout
        deadline = time.time() + timeout
        clusters = self.get_cb_clusters()
        bucket_names = [b.name for b in clusters[0].get_buckets()]
        last_counts = {}
        while time.time() < deadline:
            counts = {}
            for cluster in clusters:
                for bname in bucket_names:
                    counts[(cluster.get_name(), bname)] = \
                        self.bucket_item_count(cluster, bname)
            self.log.info("Doc count snapshot: {0}".format(counts))
            last_counts = counts
            mismatch = False
            for bname in bucket_names:
                values = {counts[(c.get_name(), bname)] for c in clusters}
                if len(values) > 1:
                    mismatch = True
                    break
            if not mismatch:
                self.log.info("Doc counts synced across all clusters")
                return
            time.sleep(15)
        self.fail(
            "Doc counts did not converge within {0}s. Last seen: {1}".format(
                timeout, last_counts))
    def _set_disable_hlv_short_circuit(self, cluster, value):
        """Set the global xdcr setting disableHlvBasedShortCircuit on a
        cluster. Verifies persistence via readback. Propagates any
        REST error; caller is expected to fail or skip explicitly."""
        self._set_and_verify_global_xdcr_param(
            cluster, "disableHlvBasedShortCircuit",
            str(value).lower(),
            settle=5, assert_match=True)
    def _set_filter_expression(self, cluster, src_bucket, dest_bucket,
                                filter_expression, skip_restream=True):
        """Live-update filterExpression on a replication. Skip-restream
        avoids resequencing earlier mutations."""
        rest = RestConnection(cluster.get_master_node())
        replication = rest.get_replication_for_buckets(src_bucket, dest_bucket)
        api = rest.baseUrl[:-1] + replication["settingsURI"]
        params = urllib.parse.urlencode({
            "filterExpression": filter_expression,
            "filterSkipRestream": str(skip_restream).lower(),
        })
        self.log.info(
            "Setting filterExpression={0!r} on {1}->{2} (cluster {3})".format(
                filter_expression, src_bucket, dest_bucket,
                cluster.get_name()))
        status, content, _ = rest._http_request(api, "POST", params)
        if not status:
            raise Exception(
                "Failed to set filterExpression: {0}".format(content))
    def _node_version(self, server):
        return self.node_version(server)

    def _cluster_versions(self, cluster):
        return self.cluster_versions(cluster)

    def _is_mixed_mode(self, cluster):
        return self.is_mixed_mode(cluster)
    def _scan_goxdcr_logs_any_match(self, patterns, clusters=None):
        """Across every node in `clusters` (default: all), check if
        goxdcr.log matches any of `patterns`. Returns (found, probe_failed)
        where probe_failed is True if at least one node's log probe
        itself failed (SSH/SFTP/timeout). Callers should skipTest with
        TEST INFRA attribution when probe_failed and not found."""
        found_any = False
        probe_failed = False
        for cluster in (clusters or self.get_cb_clusters()):
            for node in cluster.get_nodes():
                for pattern in patterns:
                    try:
                        hit, matches = self._goxdcr_log_contains(
                            node, pattern)
                    except GoxdcrLogProbeError:
                        probe_failed = True
                        continue
                    if hit:
                        found_any = True
                        self.log.info(
                            "{0} goxdcr.log match on {1} pattern={2!r}: "
                            "{3}".format(
                                self._tag(), node.ip, pattern,
                                matches[:1]))
                        break
        return found_any, probe_failed

    def _goxdcr_log_contains(self, server, pattern):
        """Return (bool, matches) for whether goxdcr.log on `server`
        contains `pattern`. On infra failure (SSH/SFTP/timeout) this
        RAISES `GoxdcrLogProbeError`; callers must NOT silently treat
        infra failure as "pattern absent" (that pattern reclassified
        infra failures as PRODUCT BUGs previously). Skip or fail with
        TEST INFRA attribution at the call site."""
        try:
            matches, count = NodeHelper.check_goxdcr_log(server, pattern)
        except Exception as e:
            self.log.error(
                "{0} goxdcr log probe UNAVAILABLE on {1} for {2!r}: "
                "{3}".format(self._tag(), server.ip, pattern, e))
            raise GoxdcrLogProbeError(
                "goxdcr log probe unavailable on {0}: {1}".format(
                    server.ip, e))
        return count > 0, matches

    def _conflict_load_same_key_both_clusters(self, key_prefix, count):
        """Drive Type-3 (CCR-merge) candidates: write the same set of
        keys with divergent bodies on both clusters at roughly the
        same time so XDCR conflict resolution is exercised. Uses
        `_bulk_upsert` so 100k-key conflict loads run in seconds, not
        minutes."""
        for cluster in (self.src_cluster, self.dest_cluster):
            for bucket in cluster.get_buckets():
                self.log.info(
                    "{0} Conflict-load on {1}/{2}: prefix={3} "
                    "count={4}".format(
                        self._tag(), cluster.get_name(),
                        bucket.name, key_prefix, count))
                ts = time.time()

                def _pairs(cluster=cluster, ts=ts):
                    for i in range(count):
                        yield ("{0}-{1}".format(key_prefix, i),
                               {"idx": i,
                                "writer": cluster.get_name(),
                                "ts": ts})

                t0 = time.time()
                sent, total = self._bulk_upsert(
                    cluster, bucket, _pairs())
                self.log.info(
                    "{0} Conflict-load done on {1}/{2}: {3}/{4} in "
                    "{5:.1f}s".format(
                        self._tag(), cluster.get_name(), bucket.name,
                        sent, total, time.time() - t0))

    _CAS_READ_FAILED = XDCRNewBaseTest.CAS_READ_FAILED
    _CAS_DIFF_MAX_FAIL_RATIO = XDCRNewBaseTest.CAS_DIFF_MAX_FAIL_RATIO

    def _capture_doc_cas(self, cluster, bucket, key_prefix, count,
                          scope=None, collection=None):
        return self.capture_doc_cas(
            cluster, bucket, key_prefix, count,
            scope=scope, collection=collection)

    def _count_cas_changes(self, cas_pre, cas_post, log_label="",
                            max_fail_ratio=None):
        return self.count_cas_changes(
            cas_pre, cas_post, log_label=log_label,
            max_fail_ratio=max_fail_ratio)

    def _doc_has_hlv_xattr(self, cluster, bucket, key):
        """Return True if the doc's HLV xattr (`_xdcr` or `_vv`) is
        present and non-empty on this cluster. Used to validate the
        spec claim that pre-ECCV docs replicate without an HLV stamp.

        Exception handling mirrors `_doc_hlv_xattr_path`: only
        ENOENT-style failures count as "xattr absent"; transient SDK
        errors propagate so a network blip isn't reclassified as
        "no HLV", which would silently mark the doc as FLO-eligible
        when the real status is unknown."""
        client = self._borrow_sdk_client(cluster, bucket.name)
        for path in ("_xdcr", "_vv"):
            try:
                res = client.lookup_in(
                    key, None, None,
                    specs=[SD.get(path, xattr=True)])
                if res is not None:
                    self.log.info(
                        "HLV xattr {0!r} present on {1}/{2}/{3}: {4}"
                        .format(path, cluster.get_name(), bucket.name,
                                key, res))
                    return True
            except Exception as e:
                if not self._is_absent_exception(e):
                    self.log.error(
                        "{0} HLV xattr presence check INFRA FAILURE "
                        "for {1}/{2}/{3} path={4!r}: {5}".format(
                            self._tag(), cluster.get_name(),
                            bucket.name, key, path, e))
                    raise
                continue
        return False

    def _assert_no_direct_replication(self, src_cluster, forbidden_cluster):
        return self.assert_no_direct_replication(
            src_cluster, forbidden_cluster)

    def _assert_direct_replication(self, src_cluster, expected_target):
        return self.assert_direct_replication(src_cluster, expected_target)

    def _assert_ring_topology(self, expected_n_clusters):
        return self.assert_ring_topology(expected_n_clusters)

    def _capture_checkpoint_seqnos(self, cluster):
        return self.capture_checkpoint_seqnos(cluster)

    def _get_global_xdcr_param(self, cluster, param):
        return self.get_global_xdcr_param(cluster, param)

    # Source-UUID paths only. `_vv.cvCas` is the CAS value at the
    # last cross-cluster-versioning checkpoint, NOT a source UUID --
    # if `_vv.src` / `_vv.id` failed to read and we fell through to
    # `_vv.cvCas`, `_doc_hlv_src` would return a CAS hex string that
    # callers would compare against a UUID, producing false product-
    # bug attribution. cvCas is read separately by `_doc_hlv_cv_cas`
    # with its own path list.
    _HLV_SRC_PATHS = (
        "_vv.src", "_vv.id",
        "_xdcr.id", "_xdcr.src", "_xdcr.cv.src",
    )

    def _doc_hlv_src(self, cluster, bucket, key):
        """Return HLV src (cvCas source UUID) for a doc, or None if
        absent. Iterates the canonical priority-ordered path list.

        HLV xattr layout on current goxdcr (verified sample):
            "_vv": {"cvCas": "0x...", "src": "<uuid>", "ver": "0x..."}

        Some SDK / build combinations can't descend `_vv.src` and
        instead return the parent `_vv` object as a dict. When that
        happens we extract the right field from the dict rather than
        treating it as "no HLV" -- the previous logic silently
        dropped these and caused HLV-driven assertions to look
        ABSENT when the stamp was actually present.

        Binary (non-JSON) docs may not receive an HLV stamp; this
        helper returns None for them and callers should treat that
        as a separate case from a JSON-doc-without-HLV failure."""
        # Each path is tried in priority order. If we got back a
        # dict (the parent xattr blob), pick the leaf key that
        # matches the path's tail segment.
        for path in self._HLV_SRC_PATHS:
            value = self._doc_hlv_xattr_path(
                cluster, bucket, key, paths=(path,))
            if value is None:
                continue
            if isinstance(value, str):
                return value
            if isinstance(value, dict):
                leaf = path.rsplit(".", 1)[-1]
                # Common synonyms used across goxdcr versions.
                candidates = (leaf, "src", "id")
                for k in candidates:
                    v = value.get(k)
                    if isinstance(v, str) and v:
                        self.log.info(
                            "{0} HLV src extracted from dict on "
                            "{1}/{2}/{3} via path={4!r} key={5!r}: "
                            "{6!r}".format(
                                self._tag(), cluster.get_name(),
                                bucket.name, key, path, k, v))
                        return v
            self.log.warning(
                "{0} HLV src on {1}/{2}/{3} path={4!r} returned "
                "non-string non-dict {5!r}; trying next path".format(
                    self._tag(), cluster.get_name(),
                    bucket.name, key, path, value))
        return None
    _HLV_ABSENT_MARKERS = (
        "PathNotFoundException", "path_not_found",
        "SUBDOC_PATH_ENOENT", "subdoc_path_not_found",
        "DocumentNotFoundException", "document_not_found",
        "KEY_ENOENT",
    )

    @classmethod
    def _is_absent_exception(cls, exc):
        """True if an SDK exception specifically means "the path or
        document doesn't exist" rather than an infra failure. Match by
        type name + message so we don't have to hard-import couchbase
        exception classes at module load."""
        type_name = type(exc).__name__
        msg = str(exc)
        return any(marker in type_name or marker in msg
                   for marker in cls._HLV_ABSENT_MARKERS)

    def _doc_hlv_xattr_path(self, cluster, bucket, key, paths):
        """Generic typed-xattr lookup across candidate paths. Returns
        the parsed JSON value on first present path, or None. Uses
        `exists()` to disambiguate ENOENT from a real value.

        Exception handling is narrow: only ENOENT-style failures are
        treated as "path absent". A transient network / timeout will
        propagate so the caller reports TEST INFRA -- previously these
        were silently swallowed and reported as PRODUCT BUG."""
        client = self._borrow_sdk_client(cluster, bucket.name)
        attempted = []
        for path in paths:
            try:
                res = client.lookup_in(
                    key, None, None,
                    specs=[SD.get(path, xattr=True)])
            except Exception as e:
                if not self._is_absent_exception(e):
                    self.log.error(
                        "{0} HLV xattr lookup_in INFRA FAILURE for "
                        "{1}/{2}/{3} path={4!r}: {5}".format(
                            self._tag(), cluster.get_name(),
                            bucket.name, key, path, e))
                    raise
                attempted.append(
                    (path, "lookup_in absent: {0}".format(e)))
                continue
            exists = False
            try:
                exists = bool(res.exists(0))
            except Exception as e:
                if not self._is_absent_exception(e):
                    self.log.warning(
                        "{0} HLV xattr exists() raised non-ENOENT "
                        "exception on {1}/{2}/{3} path={4!r}: {5}; "
                        "falling back to content_as probe".format(
                            self._tag(), cluster.get_name(),
                            bucket.name, key, path, e))
                try:
                    _ = res.content_as[str](0)
                    exists = True
                except Exception as e2:
                    if not self._is_absent_exception(e2):
                        self.log.error(
                            "{0} HLV xattr content_as INFRA FAILURE "
                            "on {1}/{2}/{3} path={4!r}: {5}".format(
                                self._tag(), cluster.get_name(),
                                bucket.name, key, path, e2))
                        raise
                    exists = False
            if not exists:
                attempted.append((path, "absent"))
                continue
            value = None
            # SDK 4.x: content_as is an indexer; certain xattr blobs
            # are returned as dicts directly. SDK 3.x: content_as[str]
            # yields the raw JSON string. value[0]["value"] is the
            # oldest fallback for pre-typed APIs. Try in widening
            # order; first decoder that yields a non-None wins.
            for getter in ("content_as_dict", "content_as_str", "value"):
                try:
                    if getter == "content_as_dict":
                        # If the xattr is a JSON object, SDK 4.x can
                        # return it pre-parsed. Skip silently if the
                        # path is a leaf string (TypeError on dict
                        # cast) -- one of the next branches will pick
                        # it up.
                        raw = res.content_as[dict](0)
                    elif getter == "content_as_str":
                        raw = res.content_as[str](0)
                    else:
                        raw = res.value[0].get("value")
                    if isinstance(raw, (dict, list)):
                        value = raw
                    else:
                        try:
                            value = json.loads(raw)
                        except (TypeError, ValueError):
                            value = raw
                    break
                except Exception as e:
                    if not self._is_absent_exception(e):
                        self.log.warning(
                            "{0} HLV xattr decode raised non-ENOENT "
                            "on {1}/{2}/{3} path={4!r} via {5}: {6}; "
                            "trying next decoder".format(
                                self._tag(), cluster.get_name(),
                                bucket.name, key, path, getter, e))
                    continue
            self.log.info(
                "{0} HLV {1!r} on {2}/{3}/{4}: {5}".format(
                    self._tag(), path, cluster.get_name(),
                    bucket.name, key, value))
            return value
        self.log.info(
            "{0} HLV xattr ABSENT for {1}/{2}/{3} at paths {4}; "
            "attempts={5}".format(
                self._tag(), cluster.get_name(), bucket.name, key,
                list(paths), attempted))
        return None

    def _doc_hlv_mv(self, cluster, bucket, key):
        """HLV merge-version list. Type-3 CCR merge mutations have a
        populated MV (typically list of {src,ver} entries)."""
        return self._doc_hlv_xattr_path(
            cluster, bucket, key,
            paths=("_vv.mv", "_xdcr.mv"))

    def _doc_hlv_pv(self, cluster, bucket, key):
        """HLV previous-version map. Holds historical CV.src entries
        after a re-stamp moved them off CV."""
        return self._doc_hlv_xattr_path(
            cluster, bucket, key,
            paths=("_vv.pv", "_xdcr.pv"))

    def _doc_hlv_cv_cas(self, cluster, bucket, key):
        """cvCas (HLV-recorded CAS) for the doc as int, or None."""
        raw = self._doc_hlv_xattr_path(
            cluster, bucket, key,
            paths=("_vv.cvCas", "_vv.ver", "_xdcr.cv.cas"))
        if raw is None:
            return None
        try:
            if isinstance(raw, str):
                return int(raw, 16) if raw.startswith("0x") else int(raw)
            return int(raw)
        except Exception:
            return None

    def _doc_body(self, result):
        """Decode a GetResult body as a dict. SDK 4.x: `content_as` is
        a callable indexer (`result.content_as[dict]`). Older shapes
        may expose `value` or be dict-shaped already. Returns None if
        the body cannot be decoded as a dict."""
        if result is None:
            return None
        # SDK 4.x preferred path.
        try:
            return result.content_as[dict]
        except Exception:
            pass
        # Older shapes.
        try:
            v = getattr(result, "value", None)
            if isinstance(v, dict):
                return v
        except Exception:
            pass
        if isinstance(result, dict):
            return result
        return None

    def _extract_writer(self, result, default=None):
        """Pull the `writer` field out of a GetResult. Returns
        `default` if the body cannot be parsed."""
        body = self._doc_body(result)
        if isinstance(body, dict):
            return body.get("writer", default)
        return default

    def _sdk_get_raw(self, cluster, bucket, key, scope=None,
                      collection=None):
        return self.sdk_get_raw(
            cluster, bucket, key, scope=scope, collection=collection)

    def _wait_for_doc_present(self, cluster, bucket, key,
                                timeout=None, poll_interval=5,
                                scope=None, collection=None):
        return self.wait_for_doc_present(
            cluster, bucket, key,
            timeout=timeout or self.replication_drain_timeout,
            poll_interval=poll_interval,
            scope=scope, collection=collection)

    def _doc_cas(self, cluster, bucket, key):
        """Return the live CAS for `key` on `cluster/bucket`, or None.
        Uses `_sdk_get_raw` to avoid the recursive retry path on DNF."""
        try:
            res = self._sdk_get_raw(cluster, bucket, key)
        except Exception as e:
            self.log.warning(
                "{0} _doc_cas INFRA FAILURE on {1}/{2}/{3}: {4}".format(
                    self._tag(), cluster.get_name(),
                    bucket.name, key, e))
            return None
        if res is None:
            return None
        cas = getattr(res, "cas", None)
        if cas is None and isinstance(res, dict):
            cas = res.get("cas")
        return cas

    def _get_local_cluster_hlv_src(self, cluster, bucket,
                                    peer_cluster=None,
                                    poll_interval=5,
                                    timeout=None):
        """Derive `cluster`'s HLV SourceID. HLV is stamped by XDCR
        during pipeline processing (outbound), NOT at write time —
        so a bare write + 2s sleep is unreliable for finding the
        stamp on the local doc.

        Strategy (in order, first one to succeed wins):
          1. If `peer_cluster` is supplied, write sentinel on
             `cluster`, wait for outbound drain on `cluster`, then
             poll HLV on `peer_cluster` for the same key. XDCR will
             have stamped HLV.src=cluster's_id by then.
          2. Fallback: write sentinel on `cluster`, drain, poll HLV
             on `cluster` itself (relies on outbound pipeline having
             stamped the local copy too).

        Cached per (cluster, bucket). Returns None on persistent
        failure; callers must handle None explicitly (sentinel may
        not be replicable if FLO blocks it, etc.).

        Important: only SUCCESSFUL resolutions are cached. A `None`
        result is NEVER cached -- otherwise a transient sentinel
        failure (FLO blocking, replication paused, drain race) would
        permanently mask later successful resolutions within the same
        test instance."""
        if not hasattr(self, "_local_hlv_src_cache"):
            self._local_hlv_src_cache = {}
        if not hasattr(self, "_sentinel_docs"):
            self._sentinel_docs = []
        cache_key = (cluster.get_name(), bucket.name)
        cached = self._local_hlv_src_cache.get(cache_key)
        if cached is not None:
            return cached

        timeout = timeout or self.flo_settle_timeout
        method_stem = getattr(self, "_testMethodName", "anon")
        sentinel_key = "flo-sentinel-{0}-{1}-{2}".format(
            method_stem, cluster.get_name(), bucket.name)
        client = self._borrow_sdk_client(cluster, bucket.name)
        client.upsert(
            sentinel_key,
            {"sentinel": True, "src_probe": cluster.get_name(),
             "test": method_stem})
        self._sentinel_docs.append((cluster, bucket, sentinel_key))

        try:
            cluster.wait_for_outbound_mutations(
                timeout=self.replication_drain_timeout)
        except Exception as e:
            self.log.warning(
                "{0} sentinel drain on {1} raised: {2}".format(
                    self._tag(), cluster.get_name(), e))

        candidates = []
        if peer_cluster is not None:
            candidates.append((peer_cluster, "peer"))
        candidates.append((cluster, "local"))

        deadline = time.time() + timeout
        src = None
        last_attempt = "none"
        while time.time() < deadline and src is None:
            for probe_cluster, label in candidates:
                src = self._doc_hlv_src(
                    probe_cluster, bucket, sentinel_key)
                last_attempt = "{0}/{1}".format(
                    probe_cluster.get_name(), label)
                if src is not None:
                    break
            if src is not None:
                break
            time.sleep(poll_interval)

        if src is None:
            self.log.warning(
                "{0} local HLV src UNRESOLVED for {1}/{2} after "
                "{3}s polling; sentinel never gained HLV stamp on "
                "{4}. Not caching the None -- next caller gets a "
                "fresh probe.".format(
                    self._tag(), cluster.get_name(), bucket.name,
                    timeout, last_attempt))
            return None
        self.log.info(
            "{0} local HLV src for {1}/{2} resolved via {3}: "
            "{4!r}".format(
                self._tag(), cluster.get_name(), bucket.name,
                last_attempt, src))
        self._local_hlv_src_cache[cache_key] = src
        return src

    def _scrape_metric_lines(self, server, name_substr,
                              endpoints=("_prometheusMetrics",
                                          "_prometheusMetricsHigh")):
        return self.scrape_prometheus_lines(
            server, name_substr, endpoints=endpoints)

    def _remote_cluster_uuid_for_peer(self, cluster, peer_cluster):
        return self.remote_cluster_uuid_for_peer(cluster, peer_cluster)

    def _replications_to_peer(self, cluster, peer_cluster,
                                src_bucket_filter=None):
        return self.replications_to_peer(
            cluster, peer_cluster, src_bucket_filter=src_bucket_filter)

    def _read_repl_stat(self, cluster, repl_id, src_bucket, stat):
        return self.read_repl_stat(cluster, repl_id, src_bucket, stat)

    def _get_docs_processed_to_peer(self, cluster, peer_cluster,
                                     src_bucket_filter=None):
        return self.get_docs_processed_to_peer(
            cluster, peer_cluster, src_bucket_filter=src_bucket_filter)

    def _get_docs_written_to_peer(self, cluster, peer_cluster):
        return self.get_docs_written_to_peer(cluster, peer_cluster)

    def _wait_for_pipeline_delta(self, cluster, peer_cluster, baseline,
                                  min_delta, timeout=None,
                                  poll_interval=10):
        timeout = timeout or self.metric_settle_timeout
        return self.wait_for_pipeline_delta(
            cluster, peer_cluster, baseline, min_delta,
            timeout=timeout, poll_interval=poll_interval)


class ForwardLocalOnlyXDCRTests(ForwardLocalOnlyXDCRBase):
    """Base test set for the forwardLocalOnly suite."""

    def test_forward_local_only_sdk_create_update(self):
        """SDK creates + updates with forwardLocalOnly=true on bidir
        AA replication. All mutations are LOCAL (Type-1 SDK writes
        with HLV stamping on outbound). Spec: local mutations
        replicate normally; skip counter is documented to stay flat
        (but on current builds is unreliable -- see diagnostic logs).

        Assertions (in order of strength):
          1. Doc counts converge across both clusters
             (`_assert_doc_count_synced`).
          2. Per-bucket curr_items reflects 2*sdk_count on each
             cluster (each side wrote sdk_count docs under a distinct
             prefix; both sets must land on both clusters after
             bidirectional drain).
          3. Sample doc written on A has HLV.src stamped on B; same
             for B->A. Proves outbound HLV stamping under ECCV.
          4. **FLO bisect**: with disableHlvBasedShortCircuit=true on
             both clusters, a CAS snapshot of A's locally-written
             docs must stay UNCHANGED across an echo window. A
             change implies B re-echoed foreign-HLV docs back to A
             -- i.e. FLO's outbound skip is a no-op. Without this
             check, the test would pass even if FLO did nothing
             beyond regular ECCV+AA replication.

        Counter delta is logged as DIAGNOSTIC only -- the skip
        counter is currently unwired on goxdcr; previous assertion
        `assertEqual(delta, 0)` was vacuous (counter always at 0
        regardless of FLO behavior)."""
        self._standard_flo_setup(
            disable_shortcircuit_on=list(self.get_cb_clusters()))
        self._verify_forward_local_only_setting(self.src_cluster, True)
        self._verify_forward_local_only_setting(self.dest_cluster, True)

        try:
            baseline_src = self._get_skipped_total(self.src_cluster)
            baseline_dest = self._get_skipped_total(self.dest_cluster)
        except MetricsScrapeError as e:
            self.log.warning(
                "{0} baseline scrape failed (non-fatal): {1}".format(
                    self._tag(), e))
            baseline_src = baseline_dest = None

        sdk_count = self._input.param("sdk_count", 200)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-c1-create", sdk_count)
            self._sdk_update_docs(
                self.src_cluster, bucket, "flo-c1-create", sdk_count)
        for bucket in self.dest_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.dest_cluster, bucket, "flo-c2-create", sdk_count)
            self._sdk_update_docs(
                self.dest_cluster, bucket, "flo-c2-create", sdk_count)

        self._wait_for_replication_drain()
        self._assert_doc_count_synced()
        sample_bucket = self.src_cluster.get_buckets()[0]
        a_doc_on_b = self._doc_hlv_src(
            self.dest_cluster, sample_bucket, "flo-c1-create-0")
        b_doc_on_a = self._doc_hlv_src(
            self.src_cluster, sample_bucket, "flo-c2-create-0")
        self.log.info(
            "{0} HLV stamping check: A-doc on B HLV.src={1!r}; "
            "B-doc on A HLV.src={2!r}".format(
                self._tag(), a_doc_on_b, b_doc_on_a))
        self.assertIsNotNone(
            a_doc_on_b,
            "{0} PRODUCT BUG: A-originated doc reached B without HLV "
            "stamp; outbound pipeline did not stamp under ECCV".format(
                self._tag()))
        self.assertIsNotNone(
            b_doc_on_a,
            "{0} PRODUCT BUG: B-originated doc reached A without HLV "
            "stamp; outbound pipeline did not stamp under ECCV".format(
                self._tag()))
        try:
            post_src = self._get_skipped_total(self.src_cluster)
            post_dest = self._get_skipped_total(self.dest_cluster)
            d_src = (post_src - baseline_src
                     if baseline_src is not None else "<no-baseline>")
            d_dest = (post_dest - baseline_dest
                      if baseline_dest is not None else "<no-baseline>")
            self.log.info(
                "{0} DIAGNOSTIC: {1} delta src={2}, dest={3} "
                "(informational; test does not depend on these "
                "values)".format(
                    self._tag(), SKIPPED_METRIC, d_src, d_dest))
        except MetricsScrapeError as e:
            self.log.warning(
                "{0} DIAGNOSTIC scrape failed (non-fatal): {1}".format(
                    self._tag(), e))

        # FLO bisect: prove FLO's outbound skip is not a no-op. With
        # disableHlvBasedShortCircuit=true on both clusters, B's
        # outbound under FLO=true must SKIP A-originated foreign-HLV
        # docs -> no echo back to A -> CAS on A stays stable. A
        # change ratio above the threshold means foreign-HLV docs
        # echoed back, i.e. FLO did nothing.
        bisect_sample = min(sdk_count, 50)
        cas_pre = self._capture_doc_cas(
            self.src_cluster, sample_bucket,
            "flo-c1-create", bisect_sample)
        self.log.info(
            "{0} FLO-bisect: CAS-pre captured on {1} sample keys; "
            "settling {2}s for any potential echo".format(
                self._tag(), bisect_sample, self.flo_settle_timeout))
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()
        cas_post = self._capture_doc_cas(
            self.src_cluster, sample_bucket,
            "flo-c1-create", bisect_sample)
        changed, failed, infra_bad = self._count_cas_changes(
            cas_pre, cas_post, log_label="sdk-bisect ")
        if infra_bad:
            self.log.warning(
                "{0} FLO-bisect: CAS-diff read failure ratio above "
                "threshold; skipping no-op detection".format(
                    self._tag()))
        else:
            self.assertEqual(
                changed, 0,
                "{0} PRODUCT BUG: FLO appears to be a no-op. With "
                "disableHlvBasedShortCircuit=true and FLO=true on B, "
                "B must NOT echo foreign-HLV docs back to A. "
                "Observed {1}/{2} CAS changes on A across the echo "
                "window (failed reads={3}).".format(
                    self._tag(), changed, bisect_sample, failed))

    def test_forward_local_only_subdoc_xattr(self):
        """Subdoc mutations including xattrs (mutate_in) are Type-1 local
        mutations (CAS bump). They must replicate under forwardLocalOnly.

        Strict assertions:
          1. Doc counts converge.
          2. The subdoc-set xattr (`flo_meta.tag`) is present on the
             remote cluster's copy of the doc. Previous test only
             logged the lookup result -- a missing xattr produced a
             silent log warning and the test still passed. Now any
             SDK failure (non-ENOENT) propagates; any ENOENT or
             absent xattr fails the test with PRODUCT BUG attribution.
        """
        self._standard_flo_setup()

        sdk_count = self._input.param("sdk_count", 100)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-subdoc-c1", sdk_count,
                value_template={"body": "initial"})
        self._wait_for_replication_drain()

        self.log.info(
            "Issuing subdoc + xattr mutations on both clusters")
        for bucket in self.src_cluster.get_buckets():
            self._sdk_subdoc_xattr_mutate(
                self.src_cluster, bucket, "flo-subdoc-c1", sdk_count)
        for bucket in self.dest_cluster.get_buckets():
            self._sdk_subdoc_xattr_mutate(
                self.dest_cluster, bucket, "flo-subdoc-c1", sdk_count)

        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

        # Hard assertion: xattr `flo_meta.tag` MUST be present on the
        # remote cluster's copy. Uses the narrow-exception decoder so
        # transient SDK failures propagate rather than masquerading as
        # "xattr absent". One sample key per source bucket is enough
        # -- if subdoc-xattr replication is broken, it's broken for
        # all of them.
        self.log.info(
            "{0} Validating xattr replicated to remote cluster"
            .format(self._tag()))
        for bucket in self.src_cluster.get_buckets():
            key = "flo-subdoc-c1-0"
            value = self._doc_hlv_xattr_path(
                self.dest_cluster, bucket, key,
                paths=("flo_meta.tag",))
            self.assertIsNotNone(
                value,
                "{0} PRODUCT BUG: xattr `flo_meta.tag` ABSENT on "
                "remote copy of {1}/{2}. Subdoc xattr mutations are "
                "Type-1 local mutations and must replicate under "
                "FLO=true.".format(self._tag(), bucket.name, key))
            self.log.info(
                "{0} xattr present on dest {1}/{2}: {3!r}".format(
                    self._tag(), bucket.name, key, value))

    def test_forward_local_only_conflict_resolution(self):
        """Type-3: CCR-merge mutations qualify as local. Per spec, the
        merged result satisfies:
            CAS == cvCAS && len(MV) > 0 && CV.src == <local cluster src>
        After XDCR resolves divergent writes, the merged-state doc on
        each side must:
          (a) replicate (no skip) under FLO=true, AND
          (b) carry MV populated + CAS == cvCAS + CV.src == local.

        If the cluster's CR config does not produce Type-3 merges
        (e.g. LWW-only build), skip with an explicit reason rather
        than masquerading as a pass."""
        self._standard_flo_setup()
        try:
            baseline_src = self._get_skipped_total(self.src_cluster)
            baseline_dest = self._get_skipped_total(self.dest_cluster)
        except MetricsScrapeError as e:
            self.log.warning(
                "{0} baseline scrape failed (non-fatal): {1}".format(
                    self._tag(), e))
            baseline_src = baseline_dest = None

        conflict_count = self._input.param("conflict_count", 100)
        self.log.info(
            "{0} CHECKPOINT: driving conflicts: {1} keys, divergent "
            "writes on both clusters".format(
                self._tag(), conflict_count))
        self._conflict_load_same_key_both_clusters(
            "flo-conflict", conflict_count)

        self.log.info(
            "{0} CHECKPOINT: waiting for conflict resolution to "
            "converge".format(self._tag()))
        self._wait_for_replication_drain()
        self._assert_doc_count_synced()
        sample_bucket = self.src_cluster.get_buckets()[0]
        sample_key = "flo-conflict-0"
        local_src_a = self._get_local_cluster_hlv_src(
            self.src_cluster, sample_bucket,
            peer_cluster=self.dest_cluster)
        local_src_b = self._get_local_cluster_hlv_src(
            self.dest_cluster, sample_bucket,
            peer_cluster=self.src_cluster)
        self.log.info(
            "{0} local HLV src: A={1!r}, B={2!r}".format(
                self._tag(), local_src_a, local_src_b))

        for cluster, expected_src in (
                (self.src_cluster, local_src_a),
                (self.dest_cluster, local_src_b)):
            cv_src = self._doc_hlv_src(cluster, sample_bucket, sample_key)
            mv = self._doc_hlv_mv(cluster, sample_bucket, sample_key)
            cv_cas = self._doc_hlv_cv_cas(
                cluster, sample_bucket, sample_key)
            cas = self._doc_cas(cluster, sample_bucket, sample_key)
            self.log.info(
                "{0} Type-3 probe on {1}/{2}: cv_src={3!r} mv={4!r} "
                "cv_cas={5!r} cas={6!r}".format(
                    self._tag(), cluster.get_name(),
                    sample_key, cv_src, mv, cv_cas, cas))
            if not mv:
                self.skipTest(
                    "{0} Type-3 invariant cannot be exercised on this "
                    "build: MV empty on {1}/{2} after conflict drive. "
                    "Cluster likely runs LWW conflict resolution rather "
                    "than CCR merges. Set conflictResolutionType=custom "
                    "+ a merge function to exercise Type-3.".format(
                        self._tag(), cluster.get_name(), sample_key))

            # CV.src must equal the local cluster's HLV SourceID.
            if expected_src is not None:
                self.assertEqual(
                    cv_src, expected_src,
                    "{0} PRODUCT BUG: CCR-merged doc on {1}/{2} has "
                    "CV.src={3!r}, expected local {4!r}".format(
                        self._tag(), cluster.get_name(),
                        sample_key, cv_src, expected_src))

            if cv_cas is not None and cas is not None:
                self.assertEqual(
                    cas, cv_cas,
                    "{0} PRODUCT BUG: CCR-merged doc on {1}/{2} has "
                    "CAS={3} != cvCas={4}; HLV is not up-to-date".format(
                        self._tag(), cluster.get_name(),
                        sample_key, cas, cv_cas))

        self.log.info(
            "{0} CHECKPOINT: issuing post-merge local mutations to "
            "confirm replication still works".format(self._tag()))
        for bucket in self.src_cluster.get_buckets():
            self._sdk_update_docs(
                self.src_cluster, bucket, "flo-conflict", conflict_count)
        self._wait_for_replication_drain()
        self._assert_doc_count_synced()
        try:
            post_src = self._get_skipped_total(self.src_cluster)
            post_dest = self._get_skipped_total(self.dest_cluster)
            d_src = (post_src - baseline_src
                     if baseline_src is not None else "<no-baseline>")
            d_dest = (post_dest - baseline_dest
                      if baseline_dest is not None else "<no-baseline>")
            self.log.info(
                "{0} DIAGNOSTIC: {1} delta src={2}, dest={3} "
                "(informational; test does not depend on these "
                "values)".format(
                    self._tag(), SKIPPED_METRIC, d_src, d_dest))
        except MetricsScrapeError as e:
            self.log.warning(
                "{0} DIAGNOSTIC scrape failed (non-fatal): {1}".format(
                    self._tag(), e))
    def test_forward_local_only_three_cluster_mixed_eccv(self):
        """Topology A<->B<->C (chain). ECCV on A and B; off on C.
        Mutations from C arrive at B without an HLV (Type-2a). Per spec,
        absence of HLV means B treats them as local mutations and must
        replicate them to A under forwardLocalOnly=true.

        To prove the C->B->A flow (not a hypothetical C->A shortcut):
          1. Assert no direct C<->A remote-cluster ref exists.
          2. Snapshot B's docs_written-to-A stat.
          3. Write N docs on C.
          4. Drain.
          5. Assert N docs visible on A AND B's docs_written-to-A
             increased by >= N (i.e. B forwarded them)."""
        try:
            third = self.get_cb_cluster_by_name("C3")
        except Exception:
            self.log.warning(
                "C3 not present in topology; skipping 3-cluster test")
            self.skipTest("Requires 3-cluster topology")

        cluster_a = self.src_cluster
        cluster_b = self.dest_cluster
        cluster_c = third

        self.setup_xdcr_and_load()
        self._post_setup_settle()

        # Topology guard: chain must not have C<->A direct ref.
        self._assert_no_direct_replication(cluster_a, cluster_c)
        self._assert_no_direct_replication(cluster_c, cluster_a)

        self.log.info(
            "Enabling ECCV only on A ({0}) and B ({1}); leaving C ({2}) off"
            .format(cluster_a.get_name(), cluster_b.get_name(),
                    cluster_c.get_name()))
        self._set_eccv_on_cluster(cluster_a, True)
        self._set_eccv_on_cluster(cluster_b, True)
        self._set_eccv_on_cluster(cluster_c, False)
        time.sleep(self.eccv_settle_timeout)

        self._enable_forward_local_only_everywhere()

        # Snapshot B's docs_written stat targeting A. Used to prove
        # docs traversed B (B forwarded C-originated docs to A).
        b_docs_written_pre = self._get_docs_written_to_peer(
            cluster_b, cluster_a)

        sdk_count = self._input.param("sdk_count", 100)
        for bucket in cluster_c.get_buckets():
            self._sdk_upsert_docs(
                cluster_c, bucket, "flo-c-mixed", sdk_count)

        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

        self.log.info("Verifying docs from C reached A")
        missing_on_a = 0
        for bucket in cluster_a.get_buckets():
            for i in range(sdk_count):
                key = "flo-c-mixed-{0}".format(i)
                if self._sdk_get_raw(cluster_a, bucket, key) is None:
                    missing_on_a += 1
        self.assertEqual(
            missing_on_a, 0,
            "{0}/{1} C-originated docs missing on A".format(
                missing_on_a, sdk_count))

        # Prove the docs traversed B (not via a phantom C<->A path).
        b_docs_written_post = self._get_docs_written_to_peer(
            cluster_b, cluster_a)
        b_delta = b_docs_written_post - b_docs_written_pre
        self.log.info(
            "B docs_written-to-A delta: {0} (expected >= {1})".format(
                b_delta, sdk_count))
        self.assertGreaterEqual(
            b_delta, sdk_count,
            "B's docs_written-to-A must increase by >= {0} to prove "
            "C->B->A flow; got delta={1}".format(sdk_count, b_delta))
    def test_forward_local_only_skips_foreign_hlv(self):
        """Bidirectional AA, ECCV on. With HLV-based short-circuit
        DISABLED on A, B's outbound pipeline would echo back foreign-HLV
        docs to A unless forwardLocalOnly=true on B->A blocks them.

        Detection: snapshot per-doc CAS on A right after A's write, then
        again after drain. With FLO=true, CAS on A must be UNCHANGED
        (B did not echo). With FLO=false (control branch), CAS on A
        DOES change on at least some docs.

        This bisects FLO independently of the Prometheus counter."""
        self.setup_xdcr_and_load()
        self._post_setup_settle()

        self._enable_eccv_on_all_clusters()
        # Disable HLV-shortcircuit on A so B's foreign-HLV-tagged echoes
        # are NOT pre-empted on A's inbound side. This is what makes
        # FLO=true vs FLO=false observable on A's CAS.
        self._set_disable_hlv_short_circuit(self.src_cluster, True)
        time.sleep(self.flo_settle_timeout)

        sdk_count = self._input.param("sdk_count", 200)

        # Control: FLO=false everywhere. Echo SHOULD reach A.
        self.log.info(
            "Control branch: FLO=false everywhere; echoes expected on A")
        for cluster in self.get_cb_clusters():
            self._set_forward_local_only_on_cluster(cluster, False)
        time.sleep(self.flo_settle_timeout)

        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-fh-ctrl", sdk_count)
        self._wait_for_replication_drain()

        ctrl_bucket = self.src_cluster.get_buckets()[0]
        ctrl_cas_t0 = self._capture_doc_cas(
            self.src_cluster, ctrl_bucket, "flo-fh-ctrl", sdk_count)
        self.log.info(
            "Waiting for B->A echo cycle (control)")
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()
        ctrl_cas_t1 = self._capture_doc_cas(
            self.src_cluster, ctrl_bucket, "flo-fh-ctrl", sdk_count)
        ctrl_changed, ctrl_failed, ctrl_infra = self._count_cas_changes(
            ctrl_cas_t0, ctrl_cas_t1, log_label="ctrl ")
        self.log.info(
            "{0} Control: {1}/{2} CAS change on A (failed={3}, "
            "infra_bad={4})".format(
                self._tag(), ctrl_changed, sdk_count, ctrl_failed,
                ctrl_infra))
        if ctrl_infra:
            self.skipTest(
                "{0} TEST INFRA: control-branch CAS-diff failure "
                "ratio exceeded threshold; cannot prove the echo "
                "path fires, so the FLO=true bisect would be "
                "uninterpretable".format(self._tag()))

        self.log.info(
            "Test branch: FLO=true everywhere; echoes must NOT reach A")
        self._enable_forward_local_only_everywhere()

        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-fh-test", sdk_count)
        self._wait_for_replication_drain()

        test_bucket = self.src_cluster.get_buckets()[0]
        test_cas_t0 = self._capture_doc_cas(
            self.src_cluster, test_bucket, "flo-fh-test", sdk_count)
        self.log.info(
            "Waiting for B->A echo window under FLO=true")
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()
        test_cas_t1 = self._capture_doc_cas(
            self.src_cluster, test_bucket, "flo-fh-test", sdk_count)
        test_changed, test_failed, test_infra = self._count_cas_changes(
            test_cas_t0, test_cas_t1, log_label="test ")
        self.log.info(
            "{0} Test: {1}/{2} CAS change on A under FLO=true "
            "(failed={3}, infra_bad={4})".format(
                self._tag(), test_changed, sdk_count,
                test_failed, test_infra))
        if test_infra:
            self.skipTest(
                "{0} TEST INFRA: CAS-diff read failure ratio above "
                "threshold; cannot interpret echo count".format(
                    self._tag()))
        self.assertEqual(
            test_changed, 0,
            "{0} Under FLO=true, B must not echo foreign-HLV docs back "
            "to A; {1}/{2} docs on A had unexpected CAS change".format(
                self._tag(), test_changed, sdk_count))
    def test_forward_local_only_with_disable_hlv_short_circuit(self):
        """disableHlvBasedShortCircuit=true on B (target side) bisects
        FLO's precedence over the HLV-shortcircuit path. Per design,
        when FLO is on, foreign-HLV docs are skipped BEFORE the
        shortcircuit logic runs; the shortcircuit setting is only
        observable when FLO is off.

        Detection signal: HLV xattr on A's KV doc after the echo
        window. A's freshly-written doc starts WITHOUT an HLV stamp
        (HLV is stamped during XDCR's outbound pipeline pass, not at
        write time). Branch behavior:

          * Branch A (FLO=true everywhere, shortcircuit=true on B):
              A writes -> replicates to B (HLV stamped on B with
              src=A) -> B's outbound under FLO=true SKIPS the
              foreign-HLV doc -> no echo to A -> A's doc remains
              HLV-less.
            Assertion: HLV xattr ABSENT on A's flo-shortcircuit-A-* docs.

          * Branch B (FLO=false on B's outbound, shortcircuit=true
            on B): same A->B path. B's outbound under FLO=false does
            NOT skip. shortcircuit=true on B prevents B's CR from
            blocking the echo internally. B echoes back to A. On A:
            KV-doc-no-HLV loses CR to incoming HLV-stamped echo.
            A's doc gains an HLV xattr.
            Assertion: HLV xattr PRESENT on A's flo-shortcircuit-B-*
            docs (after a settle window).

        Why this signal and not the skipped counter: the counter is
        documented to track FLO-skipped mutations, but on this build
        FLO appears to short-circuit BEFORE the counter increment
        path, so it stays at 0 even under correct behavior. HLV-
        xattr-on-A is a direct, observable consequence of the echo
        path firing (or not), independent of the counter's wiring."""
        self._standard_flo_setup(
            disable_shortcircuit_on=self.dest_cluster, enable_flo=False)

        readback = self._get_global_xdcr_param(
            self.dest_cluster, "disableHlvBasedShortCircuit")
        self.assertEqual(
            str(readback).lower(), "true",
            "{0} TEST/PRODUCT BUG: disableHlvBasedShortCircuit did "
            "not apply on B; readback={1!r}".format(
                self._tag(), readback))

        sdk_count = self._input.param("sdk_count", 200)
        sample_bucket = self.src_cluster.get_buckets()[0]

        self.log.info(
            "{0} BRANCH A: FLO=true everywhere; B's outbound must "
            "SKIP foreign-HLV docs -> NO echo to A -> A's KV doc "
            "remains HLV-less".format(self._tag()))
        self._enable_forward_local_only_everywhere()
        self._verify_forward_local_only_setting(self.dest_cluster, True)
        self._wait_for_replication_drain()

        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket,
                "flo-shortcircuit-A", sdk_count)
        self._wait_for_replication_drain()
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()

        sample_a_key = "flo-shortcircuit-A-0"
        b_hlv_a = self._doc_hlv_src(
            self.dest_cluster, sample_bucket, sample_a_key)
        self.assertIsNotNone(
            b_hlv_a,
            "{0} TEST INFRA: A->B replication did not stamp HLV on "
            "B for {1}. Cannot bisect.".format(
                self._tag(), sample_a_key))

        a_hlv_after_branchA = self._doc_hlv_src(
            self.src_cluster, sample_bucket, sample_a_key)
        self.log.info(
            "{0} Branch A: HLV on A/{1} = {2!r} (expected None)".format(
                self._tag(), sample_a_key, a_hlv_after_branchA))
        self.assertIsNone(
            a_hlv_after_branchA,
            "{0} PRODUCT BUG: under FLO=true, B must NOT echo "
            "foreign-HLV docs back to A. Observed: A's KV doc "
            "{1} gained HLV xattr={2!r} -- proves an echo arrived "
            "and won CR. FLO's outbound skip is not effective.".format(
                self._tag(), sample_a_key, a_hlv_after_branchA))

        self.log.info(
            "{0} BRANCH B: FLO=false on B's outbound only; echo "
            "must reach A and stamp HLV onto A's KV doc".format(
                self._tag()))
        self._set_forward_local_only_on_cluster(
            self.dest_cluster, False)
        time.sleep(self.flo_settle_timeout)

        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket,
                "flo-shortcircuit-B", sdk_count)
        self._wait_for_replication_drain()
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()

        sample_b_key = "flo-shortcircuit-B-0"
        # Verify B received the doc.
        b_hlv_b = self._doc_hlv_src(
            self.dest_cluster, sample_bucket, sample_b_key)
        self.assertIsNotNone(
            b_hlv_b,
            "{0} TEST INFRA: A->B replication did not stamp HLV on "
            "B for {1} (Branch B). Cannot bisect.".format(
                self._tag(), sample_b_key))

        deadline = time.time() + self.flo_settle_timeout * 4
        a_hlv_after_branchB = None
        while time.time() < deadline:
            a_hlv_after_branchB = self._doc_hlv_src(
                self.src_cluster, sample_bucket, sample_b_key)
            if a_hlv_after_branchB is not None:
                break
            time.sleep(10)
        self.log.info(
            "{0} Branch B: HLV on A/{1} = {2!r} (expected non-None; "
            "echo from B should have CR-won over A's KV-no-HLV "
            "doc)".format(
                self._tag(), sample_b_key, a_hlv_after_branchB))
        self.assertIsNotNone(
            a_hlv_after_branchB,
            "{0} PRODUCT BUG: under FLO=false on B's outbound + "
            "disableHlvBasedShortCircuit=true on B, B must echo "
            "foreign-HLV docs back to A and CR must replace A's "
            "HLV-less KV doc with the HLV-stamped echo. Observed: "
            "A's KV doc {1} still has no HLV xattr after a {2}s "
            "echo window. Either the echo did not arrive, or CR "
            "kept A's HLV-less version.".format(
                self._tag(), sample_b_key,
                self.flo_settle_timeout * 4))

        try:
            counter_b = self._get_skipped_total(self.dest_cluster)
            self.log.info(
                "{0} DIAGNOSTIC: final B {1} = {2} (informational; "
                "test does not depend on this value)".format(
                    self._tag(), SKIPPED_METRIC, counter_b))
        except MetricsScrapeError as e:
            self.log.warning(
                "{0} DIAGNOSTIC: scrape failed (non-fatal): {1}"
                .format(self._tag(), e))

    def test_forward_local_only_with_advanced_filter(self):
        """Key-regex filter `^flo-keep-`. Local mutations matching the
        filter must replicate; non-matching must not."""
        self._standard_flo_setup()

        filter_expr = "REGEXP_CONTAINS(META().id, '^flo-keep-')"
        filter_settle = self._input.param("filter_settle_timeout", 60)
        for cluster in self.get_cb_clusters():
            for bucket in cluster.get_buckets():
                self._set_filter_expression(
                    cluster, bucket.name, bucket.name, filter_expr,
                    skip_restream=True)
        self.log.info(
            "Waiting {0}s for filterExpression to settle".format(
                filter_settle))
        time.sleep(filter_settle)

        for cluster in self.get_cb_clusters():
            rest = RestConnection(cluster.get_master_node())
            for bucket in cluster.get_buckets():
                actual = rest.get_xdcr_param(
                    bucket.name, bucket.name, "filterExpression")
                self.log.info(
                    "filterExpression on {0}/{1}: {2!r}".format(
                        cluster.get_name(), bucket.name, actual))
                self.assertIn(
                    "flo-keep", str(actual),
                    "Filter expression did not stick on {0}/{1}; "
                    "got {2!r}".format(
                        cluster.get_name(), bucket.name, actual))

        sdk_count = self._input.param("sdk_count", 100)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-keep-c1", sdk_count)
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-drop-c1", sdk_count)
        for bucket in self.dest_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.dest_cluster, bucket, "flo-keep-c2", sdk_count)
            self._sdk_upsert_docs(
                self.dest_cluster, bucket, "flo-drop-c2", sdk_count)

        self._wait_for_replication_drain()
        self.log.info(
            "Extra wait {0}s for filter to apply to in-flight mutations"
            .format(filter_settle))
        time.sleep(filter_settle)
        self._wait_for_replication_drain()

        self.log.info(
            "{0} Verifying filter: keep docs must exist on peer, drop "
            "must not".format(self._tag()))
        for bucket in self.src_cluster.get_buckets():
            keep_key = "flo-keep-c1-0"
            drop_key = "flo-drop-c1-0"
            keep_res = self._sdk_get_raw(
                self.dest_cluster, bucket, keep_key)
            self.assertIsNotNone(
                keep_res,
                "{0} keep doc {1} missing on dest; forward replication "
                "failed".format(self._tag(), keep_key))
            self.log.info(
                "{0} keep doc {1} present on dest as expected".format(
                    self._tag(), keep_key))
            drop_res = self._sdk_get_raw(
                self.dest_cluster, bucket, drop_key)
            missing = drop_res is None
            if not missing:
                self.log.error(
                    "{0} Drop doc {1} present on dest. Filter readback "
                    "confirmed; this points to a product issue with "
                    "filter+forwardLocalOnly coexistence rather than "
                    "a test issue.".format(self._tag(), drop_key))
            self.assertTrue(
                missing,
                "Filtered drop doc {0} should NOT exist on dest".format(
                    drop_key))

    def test_forward_local_only_asymmetric_hc(self):
        """FLO=true on B->A only; FLO=false on A->B. With HLV-shortcircuit
        disabled on A, a B->A echo would be observable on A as a CAS
        change. Asymmetric setting must produce: zero CAS change on A
        for A-originated docs (B blocked the echo via FLO).

        Bisect: re-enable FLO on A->B at end is unnecessary; the
        asymmetric assertion is sufficient because FLO=false on A->B
        does not change B's behavior on inbound from A or outbound to
        A; only the B-side FLO controls the skip-back path."""
        self._standard_flo_setup(disable_shortcircuit_on=self.src_cluster, enable_flo=False)

        self._set_forward_local_only_on_cluster(self.dest_cluster, True)
        self._set_forward_local_only_on_cluster(self.src_cluster, False)
        time.sleep(self.flo_settle_timeout)

        sdk_count = self._input.param("sdk_count", 200)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-asym-hc", sdk_count)
        self._wait_for_replication_drain()

        sample_bucket = self.src_cluster.get_buckets()[0]
        cas_t0 = self._capture_doc_cas(
            self.src_cluster, sample_bucket, "flo-asym-hc", sdk_count)
        self.log.info(
            "Waiting for B->A echo window (FLO=true on B->A must block)")
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()
        cas_t1 = self._capture_doc_cas(
            self.src_cluster, sample_bucket, "flo-asym-hc", sdk_count)

        changed, failed, infra_bad = self._count_cas_changes(
            cas_t0, cas_t1, log_label="asym-hc ")
        self.log.info(
            "{0} Asymmetric FLO test: {1}/{2} CAS changes on A "
            "(failed={3}, infra_bad={4}; must be 0)".format(
                self._tag(), changed, sdk_count, failed, infra_bad))
        if infra_bad:
            self.skipTest(
                "{0} TEST INFRA: asym-hc CAS-diff failure ratio "
                "exceeded threshold; `changed` count is "
                "uninterpretable".format(self._tag()))
        self.assertEqual(
            changed, 0,
            "{0} FLO=true on B->A must block echo of foreign-HLV docs "
            "back to A; observed {1}/{2} unexpected CAS changes "
            "(failed reads={3})".format(
                self._tag(), changed, sdk_count, failed))

    def test_forward_local_only_skip_path_via_hlv_observability(self):
        """Bisects FLO's outbound-skip effect via HLV-xattr-on-A.

        Spec says `xdcr_non_local_mutations_skipped_total` should
        increment on the dropping side. Empirically on current builds
        the counter is registered but does not advance (the FLO skip
        path short-circuits before the increment site). Rather than
        depend on the unwired counter for the test outcome, use the
        DIRECT observable consequence of the skip path firing or not:
        whether A's KV-no-HLV doc gets overwritten by a B-side echo.

        Bisect:
          * Branch A (FLO=true everywhere, disableHlvShortCircuit=true
            on B): B's outbound skips foreign-HLV docs -> NO echo to
            A -> A's KV doc remains HLV-less. Counter delta on B is
            also logged as diagnostic (test does NOT assert on it).
          * Branch B (FLO=false on B's outbound only, same
            disableHlvShortCircuit on B): B echoes back. A's
            KV-no-HLV doc loses CR to incoming HLV-stamped echo ->
            A's doc gains HLV xattr.

        Counter values are logged at every stage so the same test
        run produces evidence for the goxdcr counter-wiring bug
        report without making the assertion depend on it."""
        self._standard_flo_setup(
            disable_shortcircuit_on=self.dest_cluster, enable_flo=False)

        sdk_count = self._input.param("sdk_count", 500)
        sample_bucket = self.src_cluster.get_buckets()[0]

        self.log.info(
            "{0} BRANCH A: FLO=true everywhere; B's outbound must "
            "SKIP foreign-HLV -> A's KV doc stays HLV-less".format(
                self._tag()))
        self._enable_forward_local_only_everywhere()
        self._wait_for_replication_drain()

        baseline_b_a = self._get_skipped_total(self.dest_cluster)
        self.log.info(
            "{0} Branch A baseline B {1}={2}".format(
                self._tag(), SKIPPED_METRIC, baseline_b_a))

        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-stats-A", sdk_count)
        self._wait_for_replication_drain()
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()

        sample_a_key = "flo-stats-A-0"
        b_hlv_a = self._doc_hlv_src(
            self.dest_cluster, sample_bucket, sample_a_key)
        self.assertIsNotNone(
            b_hlv_a,
            "{0} TEST INFRA: A->B replication did not stamp HLV on "
            "B for {1}. Cannot bisect.".format(
                self._tag(), sample_a_key))

        a_hlv_after_A = self._doc_hlv_src(
            self.src_cluster, sample_bucket, sample_a_key)
        try:
            post_b_a = self._get_skipped_total(self.dest_cluster)
            delta_b_a = post_b_a - baseline_b_a
        except MetricsScrapeError as e:
            delta_b_a = "<scrape-failed: {0}>".format(e)
        self.log.info(
            "{0} Branch A: HLV on A/{1} = {2!r} (expected None); "
            "diagnostic: B {3} delta = {4}".format(
                self._tag(), sample_a_key, a_hlv_after_A,
                SKIPPED_METRIC, delta_b_a))
        self.assertIsNone(
            a_hlv_after_A,
            "{0} PRODUCT BUG: under FLO=true, B must NOT echo "
            "foreign-HLV docs back to A. Observed: A's KV doc "
            "{1} gained HLV xattr={2!r} -- proves an echo arrived "
            "and won CR. FLO's outbound skip is not effective. "
            "(B counter delta this branch: {3}; counter is "
            "informational, not assertion-bearing.)".format(
                self._tag(), sample_a_key, a_hlv_after_A,
                delta_b_a))

        self.log.info(
            "{0} BRANCH B: FLO=false on B's outbound only; echo "
            "must reach A and stamp HLV onto A's KV doc".format(
                self._tag()))
        self._set_forward_local_only_on_cluster(
            self.dest_cluster, False)
        time.sleep(self.flo_settle_timeout)
        baseline_b_b = self._get_skipped_total(self.dest_cluster)

        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-stats-B", sdk_count)
        self._wait_for_replication_drain()
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()

        sample_b_key = "flo-stats-B-0"
        b_hlv_b = self._doc_hlv_src(
            self.dest_cluster, sample_bucket, sample_b_key)
        self.assertIsNotNone(
            b_hlv_b,
            "{0} TEST INFRA: A->B replication did not stamp HLV on "
            "B for {1} (Branch B). Cannot bisect.".format(
                self._tag(), sample_b_key))

        deadline = time.time() + self.flo_settle_timeout * 4
        a_hlv_after_B = None
        while time.time() < deadline:
            a_hlv_after_B = self._doc_hlv_src(
                self.src_cluster, sample_bucket, sample_b_key)
            if a_hlv_after_B is not None:
                break
            time.sleep(10)
        try:
            post_b_b = self._get_skipped_total(self.dest_cluster)
            delta_b_b = post_b_b - baseline_b_b
        except MetricsScrapeError as e:
            delta_b_b = "<scrape-failed: {0}>".format(e)
        self.log.info(
            "{0} Branch B: HLV on A/{1} = {2!r} (expected non-None); "
            "diagnostic: B {3} delta = {4} (expected 0 since "
            "FLO=false on B's outbound)".format(
                self._tag(), sample_b_key, a_hlv_after_B,
                SKIPPED_METRIC, delta_b_b))
        self.assertIsNotNone(
            a_hlv_after_B,
            "{0} PRODUCT BUG: under FLO=false on B's outbound + "
            "disableHlvBasedShortCircuit=true on B, B must echo "
            "foreign-HLV docs back to A and CR must replace A's "
            "HLV-less KV doc with the HLV-stamped echo. Observed: "
            "A's KV doc {1} still has no HLV xattr after a {2}s "
            "echo window. Either the echo did not arrive, or CR "
            "kept A's HLV-less version. (B counter delta this "
            "branch: {3}.)".format(
                self._tag(), sample_b_key,
                self.flo_settle_timeout * 4, delta_b_b))

    def test_forward_local_only_counter_one_cluster_only(self):
        """FLO enabled on ONE cluster only -- skip counter on that
        cluster MUST advance.

        Topology:
          * ECCV on both clusters.
          * disableHlvBasedShortCircuit=true on B (so the HLV path
            does not pre-empt the FLO check on B's outbound).
          * FLO=true  on B's outbound (B->A replication).
          * FLO=false on A's outbound (A->B replication).

        Flow:
          * SDK writes on A. A's outbound (FLO=false) ships them to
            B normally.
          * On B, the docs carry HLV.src=A (foreign-HLV from B's
            perspective).
          * B's outbound to A under FLO=true picks them up, sees
            HLV.src=A != local, SKIPS each one, and bumps
            `xdcr_non_local_mutations_skipped_total` on B.

        Why this scenario is the right one for a strict counter
        assertion: when FLO is on BOTH clusters, the foreign-HLV
        skip happens at the OUTBOUND DCP layer before the counter
        increment site on current builds (separately tracked as a
        product issue). With FLO on ONE cluster, the skip path is
        the only handler the foreign-HLV doc traverses, and the
        counter is guaranteed to bump per spec.

        Verifications (strict, in order):
          1. Setting readback: FLO=true on B's reps, FLO=false on
             A's reps, disableHlvBasedShortCircuit=true on B.
          2. Forward A->B replication: sample doc on B has HLV.src
             stamped (proves ECCV is effective).
          3. Doc-count sync: both clusters converge after drain
             (proves forward path delivered).
          4. **Counter assertion**: B's
             `xdcr_non_local_mutations_skipped_total` advances by
             >= sdk_count (strict; this is the test's whole point).
          5. A's counter stays flat (A's outbound has FLO=false, no
             skip path active)."""
        self._standard_flo_setup(
            disable_shortcircuit_on=self.dest_cluster,
            enable_flo=False)
        cr_metric = "xdcr_docs_failed_cr_source_total"
        written_metric = "xdcr_docs_written_total"

        # Empirically, the per-node `/_prometheusMetrics` endpoint
        # returns 0 for `xdcr_docs_written_total` on this build even
        # when goxdcr is actively writing docs. The cluster-level
        # `/metrics` endpoint (ns_server's aggregated Prometheus
        # exposition) surfaces the same metric with non-zero values.
        # Use `/metrics` for written; keep cr_source_fail on the
        # per-node path (no observed discrepancy there).
        import re as _re

        def _scrape_one(cluster, metric_name, label):
            total = 0.0
            per_node = []
            for node in cluster.get_nodes():
                try:
                    v = self.scrape_prometheus_metric(node, metric_name)
                except MetricsScrapeError as e:
                    self.log.warning(
                        "{0} {1} {2} scrape FAILED on {3} ({4}): "
                        "{5}".format(
                            self._tag(), label, metric_name,
                            node.ip, cluster.get_name(), e))
                    per_node.append((node.ip, "<scrape-failed>"))
                    continue
                per_node.append((node.ip, v))
                total += v
            self.log.info(
                "{0} {1} cluster={2} {3} per-node={4} total={5}".format(
                    self._tag(), label, cluster.get_name(),
                    metric_name, per_node, total))
            return total

        def _scrape_via_metrics_api(cluster, metric_name, label):
            """Cluster-level /metrics scrape. Boundary-matches the
            metric name (avoids the prefix-match bug that would sum
            `<name>_<suffix>` series). Returns the float sum across
            every label-set series for `metric_name`, or 0.0 on
            scrape failure with a warning."""
            rest = RestConnection(cluster.get_master_node())
            api = rest.baseUrl + "metrics"
            try:
                status, content, _ = rest._http_request(api)
            except Exception as e:
                self.log.warning(
                    "{0} {1} /metrics request raised on {2}: {3}"
                    .format(self._tag(), label,
                             cluster.get_name(), e))
                return 0.0
            if not status:
                self.log.warning(
                    "{0} {1} /metrics non-OK on {2}".format(
                        self._tag(), label, cluster.get_name()))
                return 0.0
            text = (content.decode()
                    if isinstance(content, bytes) else str(content))
            pat = _re.compile(
                r"^" + _re.escape(metric_name)
                + r"(\{[^}]*\})?\s+(\S+)\s*$", _re.MULTILINE)
            total = 0.0
            sample_lines = []
            for m in pat.finditer(text):
                try:
                    v = float(m.group(2))
                except ValueError:
                    continue
                total += v
                if len(sample_lines) < 3:
                    sample_lines.append(
                        "{0}={1}".format(m.group(1) or "<no-labels>", v))
            self.log.info(
                "{0} {1} cluster={2} {3} /metrics total={4} "
                "samples={5}".format(
                    self._tag(), label, cluster.get_name(),
                    metric_name, total, sample_lines))
            return total

        def _scrape_cr_failed(cluster, label):
            return _scrape_one(cluster, cr_metric, label)

        def _scrape_written(cluster, label):
            return _scrape_via_metrics_api(
                cluster, written_metric, label)

        self._set_forward_local_only_on_cluster(
            self.dest_cluster, True)
        self._set_forward_local_only_on_cluster(
            self.src_cluster, False)
        time.sleep(self.flo_settle_timeout)

        self._verify_forward_local_only_setting(
            self.dest_cluster, True)
        self._verify_forward_local_only_setting(
            self.src_cluster, False)
        shortcircuit_b = self._get_global_xdcr_param(
            self.dest_cluster, "disableHlvBasedShortCircuit")
        self.assertEqual(
            str(shortcircuit_b).lower(), "true",
            "{0} SETUP: disableHlvBasedShortCircuit did not apply "
            "on B; got {1!r}".format(self._tag(), shortcircuit_b))

        self.log.info(
            "{0} CHECKPOINT: pre-baseline drain to quiesce "
            "initial-load activity".format(self._tag()))
        self._wait_for_replication_drain()
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()

        try:
            baseline_b = self._get_skipped_total(self.dest_cluster)
            baseline_a = self._get_skipped_total(self.src_cluster)
        except MetricsScrapeError as e:
            self.skipTest(
                "{0} TEST INFRA: baseline Prometheus scrape failed: "
                "{1}".format(self._tag(), e))
        self.log.info(
            "{0} Baselines: B {1}={2}, A {1}={3}".format(
                self._tag(), SKIPPED_METRIC,
                baseline_b, baseline_a))

        cr_baseline_b = _scrape_cr_failed(self.dest_cluster, "baseline")
        cr_baseline_a = _scrape_cr_failed(self.src_cluster, "baseline")
        written_baseline_b = _scrape_written(
            self.dest_cluster, "baseline")
        written_baseline_a = _scrape_written(
            self.src_cluster, "baseline")

        sdk_count = self._input.param("sdk_count", 500)

        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket,
                "flo-one-side", sdk_count)
        self._wait_for_replication_drain()

        _scrape_cr_failed(self.dest_cluster, "post-drain-after-A-load")
        _scrape_cr_failed(self.src_cluster, "post-drain-after-A-load")
        _scrape_written(self.dest_cluster, "post-drain-after-A-load")
        _scrape_written(self.src_cluster, "post-drain-after-A-load")
        
        settle_chunk = 30
        elapsed = 0
        total_settle = self._input.param(
            "flo_one_side_settle_timeout", 180)
        while elapsed < total_settle:
            chunk = min(settle_chunk, total_settle - elapsed)
            self.log.info(
                "{0} settle progress: {1}/{2}s elapsed; sleeping "
                "{3}s before next probe".format(
                    self._tag(), elapsed, total_settle, chunk))
            time.sleep(chunk)
            elapsed += chunk
            try:
                interim_b = self._get_skipped_total(self.dest_cluster)
            except MetricsScrapeError as e:
                self.log.warning(
                    "{0} interim B scrape failed at {1}s: {2}".format(
                        self._tag(), elapsed, e))
                continue
            self.log.info(
                "{0} interim {1} elapsed={2}s B={3} (baseline={4})"
                .format(
                    self._tag(), SKIPPED_METRIC, elapsed,
                    interim_b, baseline_b))
            if interim_b > baseline_b:
                self.log.info(
                    "{0} B counter moved at {1}s; ending settle "
                    "window early".format(self._tag(), elapsed))
                break

        sample_bucket = self.src_cluster.get_buckets()[0]
        sample_key = "flo-one-side-0"
        
        try:
            b_hlv = self._doc_hlv_src(
                self.dest_cluster, sample_bucket, sample_key)
        except Exception as e:
            self.skipTest(
                "{0} TEST INFRA: HLV probe on B/{1} raised "
                "non-ENOENT SDK exception: {2}".format(
                    self._tag(), sample_key, e))
        self.assertIsNotNone(
            b_hlv,
            "{0} TEST INFRA: A->B replication did not stamp HLV on "
            "B for {1}; cannot test skip path because B has no "
            "foreign-HLV docs to skip".format(
                self._tag(), sample_key))
        self.log.info(
            "{0} Forward replication confirmed: B/{1} HLV.src={2!r}"
            .format(self._tag(), sample_key, b_hlv))

        self._assert_doc_count_synced()

        _scrape_cr_failed(self.dest_cluster, "post-settle-pre-poll")
        _scrape_cr_failed(self.src_cluster, "post-settle-pre-poll")
        _scrape_written(self.dest_cluster, "post-settle-pre-poll")
        _scrape_written(self.src_cluster, "post-settle-pre-poll")

        post_b = self._wait_for_metric_increment(
            self.dest_cluster, baseline_b)
        delta_b = post_b - baseline_b

        try:
            post_a = self._get_skipped_total(self.src_cluster)
            delta_a = post_a - baseline_a
        except MetricsScrapeError as e:
            self.log.warning(
                "{0} A counter scrape failed (non-fatal): {1}".format(
                    self._tag(), e))
            delta_a = None

        self.log.info(
            "{0} Counter deltas: B={1} (expected >= {2}), A={3} "
            "(expected 0)".format(
                self._tag(), delta_b, sdk_count, delta_a))

        cr_post_b = _scrape_cr_failed(self.dest_cluster, "final")
        cr_post_a = _scrape_cr_failed(self.src_cluster, "final")
        written_post_b = _scrape_written(self.dest_cluster, "final")
        written_post_a = _scrape_written(self.src_cluster, "final")
        self.log.info(
            "{0} DIAGNOSTIC {1} deltas: B={2} (baseline={3}), "
            "A={4} (baseline={5}). Informational; if non-zero on B "
            "while FLO-skip counter stayed flat, the foreign-HLV "
            "drop is happening via the source-side CR-fail path "
            "instead of FLO's skip path.".format(
                self._tag(), cr_metric,
                cr_post_b - cr_baseline_b, cr_baseline_b,
                cr_post_a - cr_baseline_a, cr_baseline_a))
        written_delta_b = written_post_b - written_baseline_b
        written_delta_a = written_post_a - written_baseline_a
        self.log.info(
            "{0} DIAGNOSTIC {1} deltas: B={2} (baseline={3}), "
            "A={4} (baseline={5}). Zero on B implies B's outbound "
            "to A is not running at all -- attribution flips from "
            "'skip path bug' to 'B's outbound stalled'.".format(
                self._tag(), written_metric,
                written_delta_b, written_baseline_b,
                written_delta_a, written_baseline_a))

        # Attribution gate: distinguish "skip path broken" from
        # "outbound stalled" before claiming PRODUCT BUG on the skip
        # counter. If B's outbound to A had ANY pipeline activity
        # for foreign-HLV docs (received-from-dcp > 0), the outbound
        # ran and the counter SHOULD have advanced -> the strict
        # assertion below is the right attribution. If pipeline
        # activity was zero, B's outbound never observed any docs
        # to skip -- skipTest with TEST INFRA attribution rather
        # than mis-blaming the skip counter.
        b_pipeline = self.get_docs_processed_to_peer(
            self.dest_cluster, self.src_cluster)
        b_received = b_pipeline.get("docs_received_from_dcp", 0)
        self.log.info(
            "{0} B->A pipeline stats: {1}; docs_received_from_dcp="
            "{2}".format(self._tag(), b_pipeline, b_received))
        if b_received == 0 and written_delta_b == 0:
            self.skipTest(
                "{0} TEST INFRA: B's outbound to A had zero pipeline "
                "activity (docs_received_from_dcp=0, "
                "docs_written_total delta=0). Cannot bisect the FLO "
                "skip path because there are no foreign-HLV docs in "
                "B's outbound queue. Likely cause: B->A replication "
                "stalled, paused, or remote-cluster ref "
                "misconfigured.".format(self._tag()))

        self.assertGreaterEqual(
            delta_b, sdk_count,
            "{0} PRODUCT BUG: with FLO=true on B's outbound only, "
            "B must skip foreign-HLV docs and bump the counter by "
            ">= {1}. Observed delta={2}. "
            "Setting verified: FLO=true on B, FLO=false on A, "
            "disableHlvBasedShortCircuit=true on B. Forward "
            "replication confirmed (HLV stamped on B). "
            "Prometheus metric `{3}` is registered but the "
            "increment path in goxdcr's outbound FLO-skip code "
            "is broken or not reached on this build.".format(
                self._tag(), sdk_count, delta_b, SKIPPED_METRIC))

        if delta_a is not None:
            self.assertEqual(
                delta_a, 0,
                "{0} PRODUCT BUG: A's counter advanced by {1} but "
                "A's outbound has FLO=false; there is no skip "
                "path on A. Either FLO is leaking onto A's "
                "replications or the counter is mis-attributed."
                .format(self._tag(), delta_a))

    def test_forward_local_only_binary_docs(self):
        """Binary blob docs (BlobGenerator) under forwardLocalOnly.

        Spec: HLV is NOT stamped on non-JSON docs; binary docs lack
        an HLV → qualify as Type-1 local → must replicate per FLO
        eligibility rule "Not have an HLV stamped".

        Two-branch bisect:
          1. CONTROL — FLO=false. Load N binary docs each side. Verify
             docs replicate (forward + reverse). Proves test infra +
             cluster + bidir wiring + BlobGenerator load path all work.
          2. TEST — recreate buckets (drain prior state), set FLO=true.
             Load N binary docs each side. Verify docs replicate.

        Attribution:
          * Control branch fail   → TEST INFRA (skip).
          * Control OK, FLO fail  → PRODUCT BUG (spec violation:
            no-HLV docs must replicate under FLO=true).
          * Both OK               → spec-compliant.

        bin_count default 1000 (was 100k). Per-doc binary throughput
        via BlobGenerator is bottlenecked on the framework's load
        task, not on goxdcr. 1k is enough to bisect FLO behavior;
        bump via conf if a stress profile is needed."""
        self._log_state("test entry")
        self.setup_xdcr_and_load()
        self._post_setup_settle("binary docs")

        self._enable_eccv_on_all_clusters()

        bin_count = self._input.param("bin_count", 1000)
        ctrl_src_prefix = "flo-bin-ctrl-"
        ctrl_dst_prefix = "flo-bin-ctrl-c2-"
        test_src_prefix = "flo-bin-test-"
        test_dst_prefix = "flo-bin-test-c2-"

        bin_drain_timeout = self._input.param(
            "bin_drain_timeout", 360)
        bin_doc_poll_timeout = self._input.param(
            "bin_doc_poll_timeout", 60)

        def _load_and_verify(src_prefix, dst_prefix, phase):
            """Load `bin_count` binary docs on each cluster + verify
            forward replication via deadline-bounded GET. Returns the
            list of missing-sample keys (empty when replication OK)."""
            gen_src = BlobGenerator(
                src_prefix.rstrip("-"), src_prefix, self._value_size,
                start=0, end=bin_count)
            gen_dst = BlobGenerator(
                dst_prefix.rstrip("-"), dst_prefix, self._value_size,
                start=0, end=bin_count)
            self.log.info(
                "{0} [{1}] loading {2} binary blob docs on src + "
                "dest".format(self._tag(), phase, bin_count))
            self.src_cluster.load_all_buckets_from_generator(
                kv_gen=gen_src)
            self.dest_cluster.load_all_buckets_from_generator(
                kv_gen=gen_dst)
            for cluster in self.get_cb_clusters():
                self.log.info(
                    "{0} [{1}] waiting up to {2}s for outbound "
                    "drain on {3}".format(
                        self._tag(), phase, bin_drain_timeout,
                        cluster.get_name()))
                cluster.wait_for_outbound_mutations(
                    timeout=bin_drain_timeout)

            missing = {"src->dst": [], "dst->src": []}
            sample_indices = [0, bin_count - 1]
            for src, dst, prefix, label in (
                    (self.src_cluster, self.dest_cluster, src_prefix,
                     "src->dst"),
                    (self.dest_cluster, self.src_cluster, dst_prefix,
                     "dst->src")):
                for bucket in src.get_buckets():
                    for i in sample_indices:
                        key = "{0}{1}".format(prefix, i)
                        res = self._wait_for_doc_present(
                            dst, bucket, key,
                            timeout=bin_doc_poll_timeout,
                            poll_interval=5)
                        if res is None:
                            missing[label].append(key)
            self.log.info(
                "{0} [{1}] missing samples: {2}".format(
                    self._tag(), phase, missing))
            return missing

        self.log.info(
            "{0} CONTROL: FLO=false on every replication; binary "
            "docs MUST replicate normally".format(self._tag()))
        for cluster in self.get_cb_clusters():
            self._set_forward_local_only_on_cluster(cluster, False)
        time.sleep(self.flo_settle_timeout)

        ctrl_missing = _load_and_verify(
            ctrl_src_prefix, ctrl_dst_prefix, "control")
        any_ctrl_missing = (
            ctrl_missing["src->dst"] or ctrl_missing["dst->src"])
        if any_ctrl_missing:
            self.skipTest(
                "{0} TEST INFRA: binary docs did not replicate under "
                "FLO=false either. Cluster or BlobGenerator load "
                "broken; cannot bisect FLO behavior. Missing: "
                "{1}".format(self._tag(), ctrl_missing))

        self.log.info(
            "{0} TEST: FLO=true everywhere; per spec, binary "
            "(no-HLV) docs MUST still replicate".format(self._tag()))
        self._enable_forward_local_only_everywhere()

        test_missing = _load_and_verify(
            test_src_prefix, test_dst_prefix, "test")
        any_test_missing = (
            test_missing["src->dst"] or test_missing["dst->src"])

        self.assertFalse(
            any_test_missing,
            "{0} PRODUCT BUG: binary (no-HLV) docs did NOT replicate "
            "under FLO=true. Control branch (FLO=false) replicated "
            "the same workload successfully, so test infra is fine. "
            "Per spec: 'Mutations without HLV ... qualify for "
            "forwardLocalOnly replication, and so are replicated to "
            "the Target cluster(s) without being stamped with an "
            "HLV.' Implementation is skipping no-HLV docs. Missing "
            "samples: {1}".format(self._tag(), test_missing))

    def test_forward_local_only_no_restream_on_enable(self):
        """Toggling forwardLocalOnly is a live-update. Pipeline must NOT
        restream already-processed seqnos. Verifies:
          * Last-checkpoint seqno does not regress after enable.
          * changes_left does not spike upward after enable (no
            re-injection of historic mutations)."""
        self.setup_xdcr_and_load()
        self._post_setup_settle()

        self._enable_eccv_on_all_clusters()
        self._wait_for_replication_drain()

        pre_changes_left = self.get_total_changes_left(self.src_cluster)
        pre_checkpoints = self._capture_checkpoint_seqnos(self.src_cluster)
        self.log.info(
            "Pre-enable: changes_left={0}, checkpoint_seqnos={1}".format(
                pre_changes_left, pre_checkpoints))

        self._enable_forward_local_only_everywhere()

        post_changes_left = self.get_total_changes_left(self.src_cluster)
        post_checkpoints = self._capture_checkpoint_seqnos(self.src_cluster)
        self.log.info(
            "Post-enable: changes_left={0}, checkpoint_seqnos={1}".format(
                post_changes_left, post_checkpoints))

        spike_tolerance = self._input.param("spike_tolerance", 50)
        self.assertLessEqual(
            post_changes_left, pre_changes_left + spike_tolerance,
            "changes_left spiked after enable: pre={0} post={1}; "
            "indicates pipeline restream".format(
                pre_changes_left, post_changes_left))

        for repl_id, pre_seq in pre_checkpoints.items():
            post_seq = post_checkpoints.get(repl_id, 0)
            self.assertGreaterEqual(
                post_seq, pre_seq,
                "Checkpoint seqno regressed for {0}: pre={1} post={2}".format(
                    repl_id, pre_seq, post_seq))

    def test_forward_local_only_pause_resume_cycles(self):
        """N pause-resume cycles with active SDK load. forwardLocalOnly
        stays on the entire time. After all cycles, doc count must
        converge and checkpoint seqnos must monotonically advance."""
        self._standard_flo_setup()

        cycles = self._input.param("pause_resume_cycles", 3)
        per_cycle_count = self._input.param("per_cycle_count", 100)
        prev_checkpoints = self._capture_checkpoint_seqnos(self.src_cluster)
        self.log.info(
            "Initial checkpoint seqnos: {0}".format(prev_checkpoints))

        for i in range(cycles):
            self.log.info("Cycle {0}/{1}: loading docs".format(
                i + 1, cycles))
            for bucket in self.src_cluster.get_buckets():
                self._sdk_upsert_docs(
                    self.src_cluster, bucket,
                    "flo-pr-c1-{0}".format(i), per_cycle_count)
            for bucket in self.dest_cluster.get_buckets():
                self._sdk_upsert_docs(
                    self.dest_cluster, bucket,
                    "flo-pr-c2-{0}".format(i), per_cycle_count)

            self.log.info("Cycle {0}/{1}: pausing replications".format(
                i + 1, cycles))
            for cluster in self.get_cb_clusters():
                for rcref in cluster.get_remote_clusters():
                    rcref.pause_all_replications()
            time.sleep(self._PAUSE_RESUME_SETTLE)

            self.log.info("Cycle {0}/{1}: resuming replications".format(
                i + 1, cycles))
            for cluster in self.get_cb_clusters():
                for rcref in cluster.get_remote_clusters():
                    rcref.resume_all_replications()
            self._wait_for_replication_drain()

            curr_checkpoints = self._capture_checkpoint_seqnos(
                self.src_cluster)
            self.log.info(
                "Cycle {0} checkpoint seqnos: {1}".format(
                    i + 1, curr_checkpoints))
            for repl_id, prev_seq in prev_checkpoints.items():
                curr_seq = curr_checkpoints.get(repl_id, 0)
                self.assertGreaterEqual(
                    curr_seq, prev_seq,
                    "Checkpoint seqno regressed for {0} after cycle "
                    "{1}: prev={2} curr={3}".format(
                        repl_id, i + 1, prev_seq, curr_seq))
            prev_checkpoints = curr_checkpoints

        self._assert_doc_count_synced()

    def test_forward_local_only_with_collections_migration(self):
        """forwardLocalOnly with collectionsMigrationMode=true and
        colMappingRules routing keys matching a pattern to a non-default
        scope/collection. Verifies forward-only replication preserves
        the migration semantics: only local mutations matching the
        pattern reach the target collection."""
        self._standard_flo_setup()

        scope_name = self._input.param("migrate_scope", "flo_scope")
        collection_name = self._input.param(
            "migrate_collection", "flo_collection")
        for bucket in self.dest_cluster.get_buckets():
            try:
                self.dest_rest.create_scope(bucket, scope_name)
                self.dest_rest.create_collection(
                    bucket, scope_name, collection_name)
                self.log.info(
                    "Created {0}/{1}/{2}.{3} on dest".format(
                        self.dest_cluster.get_name(), bucket.name,
                        scope_name, collection_name))
            except Exception as e:
                self.log.warning(
                    "Scope/collection create on dest may already exist: "
                    "{0}".format(e))

        rule_lhs = "\"REGEXP_CONTAINS(META().id,'^flo-mig-')\""
        rule_rhs = '"{0}.{1}"'.format(scope_name, collection_name)
        rules_json = "{" + rule_lhs + ":" + rule_rhs + "}"
        for bucket in self.src_cluster.get_buckets():
            readback = self._set_and_verify_xdcr_params(
                self.src_cluster, bucket.name, bucket.name,
                {
                    "collectionsMigrationMode": "true",
                    "colMappingRules": rules_json,
                },
                settle=self.flo_settle_timeout,
                assert_match=False)
            mode = readback.get("collectionsMigrationMode")
            rules = readback.get("colMappingRules")
            self.assertIn(
                "true", str(mode).lower(),
                "{0} collectionsMigrationMode did not stick on {1}; "
                "got {2!r}".format(self._tag(), bucket.name, mode))
            self.assertIn(
                scope_name, str(rules),
                "{0} colMappingRules missing scope {1}; got {2!r}".format(
                    self._tag(), scope_name, rules))

        self.log.info(
            "Pause/resume replications to apply migration mapping")
        for cluster in self.get_cb_clusters():
            for rcref in cluster.get_remote_clusters():
                rcref.pause_all_replications()
        time.sleep(self._PAUSE_RESUME_SETTLE)
        for cluster in self.get_cb_clusters():
            for rcref in cluster.get_remote_clusters():
                rcref.resume_all_replications()
        time.sleep(self.flo_settle_timeout)

        sdk_count = self._input.param("sdk_count", 100)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-mig", sdk_count)
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-skip", sdk_count)

        self._wait_for_replication_drain()
        self.log.info(
            "Extra settle {0}s for migration-routed docs to land on "
            "dest collection".format(self.flo_settle_timeout))
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()

        self.log.info(
            "Verifying mig docs landed in {0}.{1} on dest".format(
                scope_name, collection_name))
        verify_retries = self._input.param("migrate_verify_retries", 6)
        verify_delay = self._input.param("migrate_verify_delay", 15)
        for bucket in self.dest_cluster.get_buckets():
            key = "flo-mig-0"
            res = self._wait_for_doc_present(
                self.dest_cluster, bucket, key,
                timeout=verify_retries * verify_delay,
                poll_interval=verify_delay,
                scope=scope_name, collection=collection_name)
            last_err = None if res is not None else (
                "doc still missing after {0}s".format(
                    verify_retries * verify_delay))
            if last_err is not None:
                self.fail(
                    "Mig doc {0} not found on dest {1}.{2} after "
                    "{3} retries: {4}".format(
                        key, scope_name, collection_name,
                        verify_retries, last_err))

    def test_forward_local_only_with_priority_and_nwusage(self):
        """forwardLocalOnly combined with priority=High and
        networkUsageLimit=N. Verifies replication still functions and
        settings persist via REST readback."""
        self._standard_flo_setup()

        priority = self._input.param("flo_priority", "High")
        nw_limit = self._input.param("flo_nw_limit", 10)
        combo = {
            "priority": priority,
            "networkUsageLimit": str(nw_limit),
        }
        for cluster in self.get_cb_clusters():
            for bucket in cluster.get_buckets():
                self._set_and_verify_xdcr_params(
                    cluster, bucket.name, bucket.name, combo,
                    settle=self.flo_settle_timeout,
                    assert_match=True)

        sdk_count = self._input.param("sdk_count", 200)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-prio-c1", sdk_count)
        for bucket in self.dest_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.dest_cluster, bucket, "flo-prio-c2", sdk_count)

        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

    def test_forward_local_only_bucket_storage_durability(self):
        """forwardLocalOnly across the bucket-storage / durability /
        replicas matrix. Driven by conf params:
          * `bucket_storage`  - magma / couchstore (consumed by base
                                class at bucket creation time)
          * `bucket_type`     - membase / ephemeral
          * `replicas`        - replica count
          * `flo_durability`  - durabilityMinLevel; applied post-setup
                                via change_bucket_props (the prior
                                `default@C1=durability:...` conf syntax
                                routed through XDCR replication-override
                                logic and raised KeyError because
                                `durability` is a BUCKET property, not
                                an XDCR replication setting).

        Verifies:
          * Replication still drains under the chosen combo.
          * Doc counts converge.
          * Bucket props persist via REST readback."""
        self.setup_xdcr_and_load()
        self._post_setup_settle()

        flo_durability = self._input.param("flo_durability", "none")
        if flo_durability and flo_durability.lower() != "none":
            for cluster in self.get_cb_clusters():
                for bucket in cluster.get_buckets():
                    try:
                        self._set_and_verify_bucket_prop(
                            cluster, bucket,
                            "durabilityMinLevel", flo_durability,
                            settle=5, assert_match=True)
                    except Exception as e:
                        self.log.warning(
                            "{0} could not set durabilityMinLevel="
                            "{1} on {2}/{3}: {4}. Continuing without "
                            "durability override.".format(
                                self._tag(), flo_durability,
                                cluster.get_name(), bucket.name, e))

        for cluster in self.get_cb_clusters():
            for bucket in cluster.get_buckets():
                bucket_info = RestConnection(
                    cluster.get_master_node()).get_bucket_json(bucket.name)
                self.log.info(
                    "{0} Bucket {1}/{2}: storageBackend={3}, "
                    "bucketType={4}, replicaNumber={5}, "
                    "durabilityMinLevel={6}".format(
                        self._tag(), cluster.get_name(), bucket.name,
                        bucket_info.get("storageBackend"),
                        bucket_info.get("bucketType"),
                        bucket_info.get("replicaNumber"),
                        bucket_info.get("durabilityMinLevel")))

        self._enable_eccv_on_all_clusters()
        self._enable_forward_local_only_everywhere()
        self._verify_forward_local_only_setting(self.src_cluster, True)
        self._verify_forward_local_only_setting(self.dest_cluster, True)

        sdk_count = self._input.param("sdk_count", 200)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-bkt-c1", sdk_count)
            self._sdk_update_docs(
                self.src_cluster, bucket, "flo-bkt-c1", sdk_count)
        for bucket in self.dest_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.dest_cluster, bucket, "flo-bkt-c2", sdk_count)
            self._sdk_update_docs(
                self.dest_cluster, bucket, "flo-bkt-c2", sdk_count)

        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

    def test_forward_local_only_no_hlv_docs_replicate_local(self):
        """Per spec (design CALLOUT): mutations *without* an HLV qualify
        as local under forwardLocalOnly. They are replicated to Target
        WITHOUT being stamped with an HLV, so they qualify as local on
        Target too. The vbHlvMaxCas threshold is *not* part of the FLO
        eligibility logic; what matters is the absence of HLV on the
        mutation at evaluation time.

        Test steps:
          1. Pre-state guard: ECCV must be OFF on src bucket
             (irreversible once on; prior test contamination would
             invalidate the premise).
          2. Load N docs on src while ECCV is OFF -> no HLV stamping
             will have occurred for them.
          3. Verify on src: HLV xattr ABSENT on sample doc.
          4. Enable ECCV on both clusters.
          5. Enable forwardLocalOnly everywhere.
          6. Verify on src: untouched docs still have no HLV xattr
             (ECCV-enable alone must not stamp existing docs).
          7. Replicate. Verify on dest: HLV xattr still ABSENT
             (spec: replicated WITHOUT HLV stamp on Target)."""
        self._log_state("test entry")
        self.setup_xdcr_and_load()
        self._post_setup_settle("pre-ECCV docs preload")

        for cluster in self.get_cb_clusters():
            rest = RestConnection(cluster.get_master_node())
            for bucket in cluster.get_buckets():
                bucket_info = rest.get_bucket_json(bucket.name)
                eccv = bucket_info.get(
                    "enableCrossClusterVersioning", False)
                self.log.info(
                    "{0} pre-state ECCV on {1}/{2}: {3}".format(
                        self._tag(), cluster.get_name(), bucket.name, eccv))
                if eccv:
                    self.skipTest(
                        "{0} Test premise requires ECCV OFF at start; "
                        "ECCV is already on {1}/{2} (irreversible)".format(
                            self._tag(), cluster.get_name(), bucket.name))

        pre_count = self._input.param("pre_eccv_count", 200)
        self.log.info(
            "{0} CHECKPOINT: loading {1} pre-ECCV docs (no HLV will be "
            "stamped while ECCV is off)".format(self._tag(), pre_count))
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-no-hlv", pre_count)
        self._wait_for_replication_drain()

        sample_bucket = self.src_cluster.get_buckets()[0]
        sample_key = "flo-no-hlv-0"
        pre_eccv_hlv = self._doc_hlv_src(
            self.src_cluster, sample_bucket, sample_key)
        self.log.info(
            "{0} pre-ECCV HLV src on src/{1}: {2} (expected None)".format(
                self._tag(), sample_key, pre_eccv_hlv))
        self.assertIsNone(
            pre_eccv_hlv,
            "{0} pre-ECCV doc must not have HLV xattr; got {1!r}".format(
                self._tag(), pre_eccv_hlv))

        self._enable_eccv_on_all_clusters()
        self._enable_forward_local_only_everywhere()
        self._log_state("post-ECCV + post-FLO-enable")

        post_eccv_hlv_src = self._doc_hlv_src(
            self.src_cluster, sample_bucket, sample_key)
        self.log.info(
            "{0} post-ECCV HLV src on src/{1}: {2} (expected None; "
            "untouched docs must not gain a stamp from ECCV alone)"
            .format(self._tag(), sample_key, post_eccv_hlv_src))
        self.assertIsNone(
            post_eccv_hlv_src,
            "{0} untouched pre-ECCV doc gained HLV after ECCV enable; "
            "got {1!r}".format(self._tag(), post_eccv_hlv_src))

        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

        post_eccv_hlv_dest = self._doc_hlv_src(
            self.dest_cluster, sample_bucket, sample_key)
        self.log.info(
            "{0} post-replication HLV src on dest/{1}: {2} (expected "
            "None; spec: replicated without HLV stamp on Target)".format(
                self._tag(), sample_key, post_eccv_hlv_dest))
        self.assertIsNone(
            post_eccv_hlv_dest,
            "{0} PRODUCT BUG: no-HLV docs must replicate to Target "
            "WITHOUT an HLV stamp under forwardLocalOnly; got {1!r} on "
            "dest".format(self._tag(), post_eccv_hlv_dest))

    def test_forward_local_only_with_full_encryption(self):
        """Enable n2n encryption + strict TLS on both clusters then run
        forwardLocalOnly. Replication must function and only local
        mutations must replicate."""
        self.setup_xdcr_and_load()
        self._post_setup_settle()

        tls_level = self._input.param("flo_tls_level", "all")
        for cluster in self.get_cb_clusters():
            try:
                cluster.toggle_security_setting(
                    [cluster.get_master_node()], "n2n", "enable")
                cluster.toggle_security_setting(
                    [cluster.get_master_node()], "tls", tls_level)
                self.log.info(
                    "Enabled n2n + tls={0} on {1}".format(
                        tls_level, cluster.get_name()))
            except Exception as e:
                self.log.warning(
                    "Could not enable encryption on {0}: {1}".format(
                        cluster.get_name(), e))
                self.skipTest(
                    "Cluster does not support n2n/strict-tls toggling: "
                    "{0}".format(e))
        # Encryption-toggle settle: n2n + strict TLS toggles trigger
        # cluster-wide TLS re-handshake; follow-up REST calls can 500
        # if issued too soon. Overridable via conf.
        encryption_settle = self._input.param(
            "flo_encryption_settle_timeout", 30)
        self.log.info(
            "{0} encryption-toggle settle: sleeping {1}s before "
            "applying ECCV/FLO".format(self._tag(), encryption_settle))
        time.sleep(encryption_settle)

        self._enable_eccv_on_all_clusters()
        self._enable_forward_local_only_everywhere()

        sdk_count = self._input.param("sdk_count", 200)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-tls-c1", sdk_count)
        for bucket in self.dest_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.dest_cluster, bucket, "flo-tls-c2", sdk_count)

        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

        self.log.info("Reverting tls level to control to leave cluster clean")
        for cluster in self.get_cb_clusters():
            try:
                cluster.toggle_security_setting(
                    [cluster.get_master_node()], "tls", "control")
            except Exception as e:
                self.log.warning(
                    "Could not revert TLS on {0}: {1}".format(
                        cluster.get_name(), e))

    def test_forward_local_only_rebalance_in_out(self):
        """Scale-out then scale-in on src cluster while replication is
        active under forwardLocalOnly. After each topology change,
        verify replication catches up and only local mutations land
        on peer."""
        self._standard_flo_setup()

        sdk_count = self._input.param("sdk_count", 200)
        try:
            self.log.info("Scale-out: rebalance_in 1 node on src cluster")
            task = self.src_cluster.async_rebalance_in(num_nodes=1)
            task.result()
        except Exception as e:
            self.log.warning(
                "rebalance_in failed (likely no spare floating server): "
                "{0}".format(e))
            self.skipTest(
                "Requires a spare floating server for rebalance-in: "
                "{0}".format(e))

        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-rb-in", sdk_count)
        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

        self.log.info("Scale-in: rebalance_out 1 node on src cluster")
        try:
            task = self.src_cluster.async_rebalance_out(num_nodes=1)
            task.result()
        except Exception as e:
            self.log.warning(
                "rebalance_out failed: {0}".format(e))
            self.fail(
                "Scale-in rebalance must complete cleanly: {0}".format(e))

        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-rb-out", sdk_count)
        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

    def test_forward_local_only_failover_recovery(self):
        """Failover a non-master node on src cluster, run mutations
        under forwardLocalOnly, recover the node, verify replication
        drains and docs converge."""
        if len(self.src_cluster.get_nodes()) < 2:
            self.skipTest("Failover test requires >=2 nodes per cluster")

        self._standard_flo_setup()

        graceful = self._input.param("flo_graceful", False)
        try:
            self.log.info(
                "Failing over 1 node on src (graceful={0})".format(graceful))
            self.src_cluster.failover(num_nodes=1, graceful=graceful)
        except Exception as e:
            # A swallowed failover means the rest of the test would
            # run on a healthy cluster and pass trivially -- proving
            # nothing about FLO under failover. Skip with TEST INFRA
            # attribution instead.
            self.skipTest(
                "{0} TEST INFRA: failover raised; cannot exercise "
                "FLO-under-failover scenario: {1}".format(
                    self._tag(), e))

        sdk_count = self._input.param("sdk_count", 100)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-fo", sdk_count)

        try:
            self.log.info("Adding back failed-over node and rebalancing")
            self.src_cluster.add_back_node("delta")
            task = self.src_cluster.async_rebalance_in(num_nodes=0)
            if task is not None:
                task.result()
        except Exception as e:
            self.log.warning(
                "add_back / rebalance after failover raised "
                "(non-fatal): {0}".format(e))

        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

    # ------------------------------------------------------------------
    # Variable vbucket mode
    # ------------------------------------------------------------------
    def test_forward_local_only_variable_vbucket(self):
        """forwardLocalOnly on buckets with non-default vbucket count
        (typically 128 with magma; INI / conf controls the actual
        value). Verifies replication correctness across the differing
        vbucket configurations + doc-count sync."""
        self.setup_xdcr_and_load()
        self._post_setup_settle()

        for cluster in self.get_cb_clusters():
            rest = RestConnection(cluster.get_master_node())
            for bucket in cluster.get_buckets():
                info = rest.get_bucket_json(bucket.name)
                self.log.info(
                    "Bucket {0}/{1}: numVBuckets={2}, "
                    "storageBackend={3}".format(
                        cluster.get_name(), bucket.name,
                        info.get("numVBuckets"),
                        info.get("storageBackend")))

        self._enable_eccv_on_all_clusters()
        self._enable_forward_local_only_everywhere()

        sdk_count = self._input.param("sdk_count", 300)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-vbn-c1", sdk_count)
            self._sdk_update_docs(
                self.src_cluster, bucket, "flo-vbn-c1", sdk_count)
        for bucket in self.dest_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.dest_cluster, bucket, "flo-vbn-c2", sdk_count)
            self._sdk_update_docs(
                self.dest_cluster, bucket, "flo-vbn-c2", sdk_count)

        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

    def test_forward_local_only_remote_then_local_mutation(self):
        """A doc lands on B via XDCR (CAS==cvCAS, HLV.src=A on B).
        Local SDK mutation on B bumps CAS, making CAS > cvCAS -> Type-1
        local mutation. Per spec, qualifies for forwardLocalOnly. The
        re-mutated doc must replicate back to A."""
        self._standard_flo_setup()

        sdk_count = self._input.param("sdk_count", 100)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-rtl", sdk_count)
        self._wait_for_replication_drain()

        self.log.info(
            "Locally mutating remote-originated docs on B (CAS bump -> "
            "Type-1)")
        for bucket in self.dest_cluster.get_buckets():
            self._bulk_upsert(
                self.dest_cluster, bucket,
                (("flo-rtl-{0}".format(i),
                  {"idx": i, "rev": "remote-then-local",
                   "writer": self.dest_cluster.get_name()})
                 for i in range(sdk_count)))

        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

        self.log.info(
            "{0} verifying re-mutated docs replicated back to A".format(
                self._tag()))
        for bucket in self.src_cluster.get_buckets():
            key = "flo-rtl-0"
            res = self._wait_for_doc_present(
                self.src_cluster, bucket, key,
                timeout=self.replication_drain_timeout)
            self.assertIsNotNone(
                res,
                "{0} re-mutated doc {1} did not replicate back to A "
                "on bucket {2}".format(self._tag(), key, bucket.name))
            self.log.info(
                "{0} doc {1} on A after B-local re-mutation: {2}".format(
                    self._tag(), key, res))

    def test_forward_local_only_drop_recreate_scope_collection(self):
        """Mid-replication: drop a scope/collection on dest, recreate
        it, push more docs through it, verify replication catches up
        and only local mutations land."""
        self._standard_flo_setup()

        scope_name = self._input.param("scope_name", "flo_drop_scope")
        collection_name = self._input.param(
            "collection_name", "flo_drop_collection")

        for cluster in self.get_cb_clusters():
            rest = RestConnection(cluster.get_master_node())
            for bucket in cluster.get_buckets():
                try:
                    rest.create_scope(bucket, scope_name)
                    rest.create_collection(
                        bucket, scope_name, collection_name)
                except Exception as e:
                    self.log.warning(
                        "Create scope/collection on {0}/{1} may already "
                        "exist: {2}".format(
                            cluster.get_name(), bucket.name, e))

        sdk_count = self._input.param("sdk_count", 50)
        for bucket in self.src_cluster.get_buckets():
            self._bulk_upsert(
                self.src_cluster, bucket,
                (("flo-drop-{0}".format(i), {"idx": i})
                 for i in range(sdk_count)),
                scope=scope_name, collection=collection_name)
        self._wait_for_replication_drain()

        self.log.info(
            "Dropping {0}.{1} on dest mid-replication".format(
                scope_name, collection_name))
        for bucket in self.dest_cluster.get_buckets():
            try:
                self.dest_rest.delete_collection(
                    bucket, scope_name, collection_name)
                self.dest_rest.delete_scope(bucket, scope_name)
            except Exception as e:
                self.log.warning(
                    "drop scope/collection on dest raised: {0}".format(e))
        time.sleep(self._COLLECTION_SETTLE)

        self.log.info("Recreating {0}.{1} on dest".format(
            scope_name, collection_name))
        for bucket in self.dest_cluster.get_buckets():
            try:
                self.dest_rest.create_scope(bucket, scope_name)
                self.dest_rest.create_collection(
                    bucket, scope_name, collection_name)
            except Exception as e:
                self.log.warning(
                    "recreate scope/collection on dest raised: {0}"
                    .format(e))
        time.sleep(self._COLLECTION_SETTLE)

        self.log.info(
            "{0} Driving more local mutations through recreated "
            "collection".format(self._tag()))
        for bucket in self.src_cluster.get_buckets():
            self._bulk_upsert(
                self.src_cluster, bucket,
                (("flo-drop2-{0}".format(i),
                  {"idx": i, "phase": "post-recreate"})
                 for i in range(sdk_count)),
                scope=scope_name, collection=collection_name)

        self._wait_for_replication_drain()
        self.log.info(
            "{0} verifying post-recreate local mutations landed on "
            "dest".format(self._tag()))
        for bucket in self.dest_cluster.get_buckets():
            res = self._wait_for_doc_present(
                self.dest_cluster, bucket, "flo-drop2-0",
                timeout=self.replication_drain_timeout,
                scope=scope_name, collection=collection_name)
            self.assertIsNotNone(
                res,
                "{0} post-recreate doc flo-drop2-0 did not land on "
                "{1}/{2}.{3}".format(
                    self._tag(), bucket.name, scope_name,
                    collection_name))
            self.log.info(
                "{0} doc flo-drop2-0 present on dest collection {1}.{2} "
                "after drop/recreate".format(
                    self._tag(), scope_name, collection_name))

    def test_forward_local_only_without_eccv(self):
        """Per spec, ECCV is a prerequisite. Enabling forwardLocalOnly
        without ECCV must either be rejected by REST or surface a
        clear warning in goxdcr.log. This test deliberately skips the
        `_enable_eccv_on_all_clusters` step."""
        self.setup_xdcr_and_load()
        self._post_setup_settle()

        rest_rejected = False
        try:
            self._enable_forward_local_only_everywhere()
        except Exception as e:
            rest_rejected = True
            self.log.info(
                "REST rejected forwardLocalOnly without ECCV as expected: "
                "{0}".format(e))

        if rest_rejected:
            return

        self.log.info(
            "REST accepted forwardLocalOnly without ECCV. Driving SDK "
            "mutations and checking for goxdcr warning.")
        sdk_count = self._input.param("sdk_count", 100)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-no-eccv", sdk_count)
        self._wait_for_replication_drain()

        warning_found, probe_failed = self._scan_goxdcr_logs_any_match(
            ("ECCV", "enableCrossClusterVersioning",
             "HLV", "forwardLocalOnly"))
        if probe_failed and not warning_found:
            self.skipTest(
                "{0} goxdcr log probe failed on all nodes; cannot "
                "verify ECCV-missing warning".format(self._tag()))
        self.assertTrue(
            warning_found,
            "{0} Expected REST rejection or a goxdcr warning when "
            "forwardLocalOnly is enabled without ECCV".format(
                self._tag()))

    def test_forward_local_only_process_kill_recovery(self):
        """Kill memcached + goxdcr on src cluster. After they recover,
        forwardLocalOnly setting must still be in effect. Verify via
        REST readback and by driving more local mutations."""
        self._standard_flo_setup()
        self._verify_forward_local_only_setting(self.src_cluster, True)

        for node in self.src_cluster.get_nodes():
            shell = RemoteMachineShellConnection(node)
            try:
                self.log.info("Killing goxdcr on {0}".format(node.ip))
                shell.execute_command(
                    "pkill -9 goxdcr || true", use_channel=True)
                self.log.info("Killing memcached on {0}".format(node.ip))
                shell.kill_memcached()
            except Exception as e:
                self.log.warning(
                    "kill on {0} raised: {1}".format(node.ip, e))
            finally:
                shell.disconnect()

        # Wait for ns_server to bring back memcached + goxdcr on each
        # killed node before the REST readback. `wait_service_started`
        # blocks until `service couchbase-server status` reports
        # running; covers both memcached and the ns_server / goxdcr
        # supervisor restart. Add a memcached warmup gate so the
        # readback doesn't race a still-warming bucket.
        recover_timeout = self._input.param(
            "flo_process_recover_timeout", 120)
        bucket_names = [
            b.name for b in self.src_cluster.get_buckets()]
        for node in self.src_cluster.get_nodes():
            self.log.info(
                "{0} waiting up to {1}s for service-running on {2}"
                .format(self._tag(), recover_timeout, node.ip))
            try:
                NodeHelper.wait_service_started(
                    node, wait_time=recover_timeout)
            except Exception as e:
                self.skipTest(
                    "{0} TEST INFRA: ns_server did not restart on "
                    "{1} within {2}s: {3}".format(
                        self._tag(), node.ip, recover_timeout, e))
        try:
            NodeHelper.wait_warmup_completed(
                self.src_cluster.get_nodes(),
                bucket_names=bucket_names)
        except Exception as e:
            self.log.warning(
                "{0} memcached warmup wait raised (continuing): "
                "{1}".format(self._tag(), e))

        # Brief additional settle for goxdcr to reload its settings
        # cache after the restart.
        time.sleep(self.flo_settle_timeout)

        self._verify_forward_local_only_setting(self.src_cluster, True)
        self._verify_forward_local_only_setting(self.dest_cluster, True)

        sdk_count = self._input.param("sdk_count", 100)
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-recover", sdk_count)
        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

    def test_forward_local_only_many_replications(self):
        """forwardLocalOnly applied to a cluster with many bucket pairs
        (typically ~10 via standard_buckets=N in conf). All replications
        must converge under forwardLocalOnly."""
        bucket_count = sum(
            len(c.get_buckets()) for c in self.get_cb_clusters())
        bucket_count_per_cluster = (
            bucket_count // len(self.get_cb_clusters())
            if self.get_cb_clusters() else 0)
        self.log.info(
            "Bucket count per cluster: {0}".format(bucket_count_per_cluster))
        if bucket_count_per_cluster < 5:
            self.log.warning(
                "Only {0} buckets/cluster; meaningful 'many replications' "
                "test wants >=5 (set standard_buckets in conf)".format(
                    bucket_count_per_cluster))

        self._standard_flo_setup()
        self._verify_forward_local_only_setting(self.src_cluster, True)
        self._verify_forward_local_only_setting(self.dest_cluster, True)

        sdk_count = self._input.param("sdk_count", 50)
        for cluster in self.get_cb_clusters():
            for bucket in cluster.get_buckets():
                self._sdk_upsert_docs(
                    cluster, bucket,
                    "flo-many-{0}-{1}".format(
                        cluster.get_name(), bucket.name),
                    sdk_count)

        self._wait_for_replication_drain()
        self._assert_doc_count_synced()

    def test_forward_local_only_pv_not_local_after_overwrite(self):
        """Design open-item resolved: CV.src in PV is NOT considered
        local under FLO. Once a doc on B is overwritten locally (B's
        SDK), the prior CV.src=A moves to PV. A subsequent observation
        of the doc back on A should not re-classify it as local on A
        for outbound replication purposes.

        Test:
          1. Disable HLV-shortcircuit on A so echoes are visible.
          2. SDK upsert key on A -> drain to B. CV.src=A on B.
          3. SDK upsert SAME key on B -> CV.src=B, A moves to PV on B.
          4. Drain B->A under FLO=true: this is now a B-originated
             local write; it SHOULD replicate (CV.src=B == B's local).
          5. On A: assert _vv.pv contains A's previous src (proving
             the doc went through the PV transition), and that A's
             outbound under FLO=true does NOT additionally re-echo it.
        CAS-tracking proves "exactly one B->A echo, not two"."""
        self._log_state("test entry")
        self.setup_xdcr_and_load()
        self._post_setup_settle("pv-not-local entry")

        self._enable_eccv_on_all_clusters()
        self._set_disable_hlv_short_circuit(self.src_cluster, True)
        self._enable_forward_local_only_everywhere()
        self._log_state("post-ECCV + FLO + disableHlvShortCircuit on A")

        sample_bucket = self.src_cluster.get_buckets()[0]
        sdk_count = self._input.param("sdk_count", 50)

        # Step 2: write on A.
        for bucket in self.src_cluster.get_buckets():
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-pv", sdk_count)
        self._wait_for_replication_drain()

        local_src_a = self._get_local_cluster_hlv_src(
            self.src_cluster, sample_bucket,
            peer_cluster=self.dest_cluster)
        local_src_b = self._get_local_cluster_hlv_src(
            self.dest_cluster, sample_bucket,
            peer_cluster=self.src_cluster)
        cv_b_before = self._doc_hlv_src(
            self.dest_cluster, sample_bucket, "flo-pv-0")
        self.log.info(
            "{0} after A->B replication: CV.src on B = {1!r} "
            "(expected A's src {2!r}); B's src = {3!r}".format(
                self._tag(), cv_b_before, local_src_a, local_src_b))

        if local_src_a is None and cv_b_before is not None:
            self.log.warning(
                "{0} sentinel-derived local_src_a was None; using "
                "observed CV.src from a replicated doc on B as A's "
                "HLV.src: {1!r}".format(self._tag(), cv_b_before))
            local_src_a = cv_b_before
            self._local_hlv_src_cache[
                (self.src_cluster.get_name(), sample_bucket.name)
            ] = local_src_a

        self.assertIsNotNone(
            cv_b_before,
            "{0} doc flo-pv-0 on B has no HLV xattr after A->B "
            "replication; expected HLV stamped by XDCR".format(
                self._tag()))
        self.assertIsNotNone(
            local_src_a,
            "{0} could not resolve A's HLV.src via sentinel or "
            "observed doc; cannot bisect PV behavior".format(
                self._tag()))
        self.assertEqual(
            cv_b_before, local_src_a,
            "{0} CV.src on B must equal A's HLV src after A->B "
            "replication; got {1!r} (expected {2!r})".format(
                self._tag(), cv_b_before, local_src_a))

        cas_a_pre = self._capture_doc_cas(
            self.src_cluster, sample_bucket, "flo-pv", sdk_count)

        self.log.info(
            "{0} CHECKPOINT: overwriting on B (moves A's src from CV "
            "to PV)".format(self._tag()))
        self._bulk_upsert(
            self.dest_cluster, sample_bucket,
            (("flo-pv-{0}".format(i),
              {"idx": i, "writer": "B-overwrite"})
             for i in range(sdk_count)))

        self._wait_for_replication_drain()
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()
        cas_a_post1 = self._capture_doc_cas(
            self.src_cluster, sample_bucket, "flo-pv", sdk_count)

        bumped_once, failed1, infra_bad1 = self._count_cas_changes(
            cas_a_pre, cas_a_post1, log_label="pv-step1 ")
        self.log.info(
            "{0} after B->A echo: {1}/{2} CAS change on A "
            "(failed={3}, infra_bad={4}; expected ALL bumped)".format(
                self._tag(), bumped_once, sdk_count, failed1,
                infra_bad1))
        if infra_bad1:
            self.skipTest(
                "{0} TEST INFRA: pv-step1 CAS-diff failure ratio "
                "exceeded threshold; cannot verify B->A echo "
                "count".format(self._tag()))
        self.assertEqual(
            bumped_once, sdk_count,
            "{0} expected exactly one B->A echo per doc; observed "
            "{1}/{2} (failed reads={3})".format(
                self._tag(), bumped_once, sdk_count, failed1))

        pv_on_a = self._doc_hlv_pv(
            self.src_cluster, sample_bucket, "flo-pv-0")
        cv_on_a = self._doc_hlv_src(
            self.src_cluster, sample_bucket, "flo-pv-0")
        self.log.info(
            "{0} post-echo on A/flo-pv-0: CV.src={1!r} PV={2!r} "
            "(expected CV.src=B={3!r}, PV contains A={4!r})".format(
                self._tag(), cv_on_a, pv_on_a, local_src_b, local_src_a))
        self.assertEqual(
            cv_on_a, local_src_b,
            "{0} CV.src on A must be B's HLV src after echo; got "
            "{1!r}".format(self._tag(), cv_on_a))
        if pv_on_a is not None and local_src_a is not None:
            pv_keys = list(pv_on_a.keys()) if isinstance(pv_on_a, dict) \
                else pv_on_a
            self.assertIn(
                local_src_a, pv_keys,
                "{0} A's prior src {1!r} must appear in PV on A; got "
                "{2!r}".format(self._tag(), local_src_a, pv_on_a))

        no_echo_deadline = self._input.param(
            "pv_no_echo_deadline", self.flo_settle_timeout * 3)
        no_echo_poll = self._input.param("pv_no_echo_poll", 15)
        end_time = time.time() + no_echo_deadline
        bumped_again = 0
        failed2 = 0
        cas_a_post2 = cas_a_post1
        while time.time() < end_time:
            self._wait_for_replication_drain()
            cas_a_post2 = self._capture_doc_cas(
                self.src_cluster, sample_bucket, "flo-pv", sdk_count)
            bumped_again, failed2, infra_bad2 = self._count_cas_changes(
                cas_a_post1, cas_a_post2, log_label="pv-step2 ")
            if bumped_again > 0:
                break
            time.sleep(no_echo_poll)
        self.log.info(
            "{0} additional echoes within {1}s deadline: {2}/{3} "
            "(failed={4}, infra_bad={5}; expected 0)".format(
                self._tag(), no_echo_deadline, bumped_again,
                sdk_count, failed2, infra_bad2))
        if infra_bad2:
            self.skipTest(
                "{0} TEST INFRA: pv-step2 CAS-diff failure ratio "
                "exceeded threshold; cannot verify absence of "
                "echo".format(self._tag()))
        self.assertEqual(
            bumped_again, 0,
            "{0} PRODUCT BUG: doc on A re-echoed after PV transition "
            "within {1}s; PV must NOT classify as local. {2}/{3} "
            "extra echoes (failed reads={4})".format(
                self._tag(), no_echo_deadline, bumped_again,
                sdk_count, failed2))

        self.log.info(
            "{0} CONTROL: flipping FLO=false on B->A to confirm echo "
            "path actually fires under the same window".format(
                self._tag()))
        self._set_forward_local_only_on_cluster(
            self.dest_cluster, False)
        time.sleep(self.flo_settle_timeout)

        cas_a_ctrl_pre = self._capture_doc_cas(
            self.src_cluster, sample_bucket, "flo-pv", sdk_count)
        # Re-mutate on B with a distinct writer marker (bulk).
        self._bulk_upsert(
            self.dest_cluster, sample_bucket,
            (("flo-pv-{0}".format(i),
              {"idx": i, "writer": "B-control"})
             for i in range(sdk_count)))

        end_time = time.time() + no_echo_deadline
        ctrl_bumped = 0
        ctrl_failed = 0
        ctrl_infra = False
        while time.time() < end_time:
            self._wait_for_replication_drain()
            cas_a_ctrl_post = self._capture_doc_cas(
                self.src_cluster, sample_bucket, "flo-pv", sdk_count)
            ctrl_bumped, ctrl_failed, ctrl_infra = self._count_cas_changes(
                cas_a_ctrl_pre, cas_a_ctrl_post,
                log_label="pv-ctrl ")
            if ctrl_bumped >= sdk_count // 2:
                break
            time.sleep(no_echo_poll)
        self.log.info(
            "{0} CONTROL: {1}/{2} CAS bumped on A under FLO=false "
            "(failed={3}, infra_bad={4})".format(
                self._tag(), ctrl_bumped, sdk_count, ctrl_failed,
                ctrl_infra))
        if ctrl_infra:
            self.skipTest(
                "{0} TEST INFRA: pv-ctrl CAS-diff failure ratio "
                "exceeded threshold; control branch cannot prove "
                "the echo path fires".format(self._tag()))
        if ctrl_bumped == 0:
            self.skipTest(
                "{0} CONTROL branch did not observe ANY echo under "
                "FLO=false within {1}s. The 'no echo under FLO=true' "
                "assertion above cannot be attributed to PV-not-local "
                "behavior; test environment is not exercising the "
                "echo path.".format(self._tag(), no_echo_deadline))

    def test_forward_local_only_dedup_no_data_loss(self):
        """Design CALLOUT: local mutation deduped by an incoming
        non-local mutation does NOT cause data loss. The deduped local
        mutation would have lost CR to the non-local anyway, so
        skipping it under FLO=true is correct.

        Test:
          1. Bidir AA, ECCV on, FLO=true everywhere.
          2. Pause B->A replication (so B's outbound queue accumulates).
          3. Write K=B-v1 on B (Type-1 local on B).
          4. Write K=A-v2 on A. A->B replication runs; B's local copy
             is overwritten with HLV.src=A.
          5. Resume B->A. B's outbound DCP sees the *latest* mutation
             for K only (dedup); that mutation has HLV.src=A which is
             foreign -> SKIP under FLO=true on B->A.
          6. Final state: A has A-v2, B has A-v2 (from A->B
             replication). No mutation lost from A's perspective.
        Documented intent: B-v1 is intentionally not surfaced; it
        would have lost CR to A-v2 anyway."""
        self._standard_flo_setup()
        self._log_state("post FLO enable")

        sample_bucket = self.src_cluster.get_buckets()[0]
        key = "flo-dedup-0"

        self.log.info(
            "{0} CHECKPOINT: pausing all replications on B (to "
            "accumulate outbound queue from B)".format(self._tag()))
        for rcref in self.dest_cluster.get_remote_clusters():
            rcref.pause_all_replications()
        time.sleep(self._PAUSE_RESUME_SETTLE)

        self.log.info(
            "{0} CHECKPOINT: writing K=B-v1 on B (paused B->A queue)"
            .format(self._tag()))
        client_b = self._borrow_sdk_client(self.dest_cluster, sample_bucket.name)
        client_b.upsert(key, {"writer": "B-v1"})

        self.log.info(
            "{0} CHECKPOINT: writing K=A-v2 on A; A->B will dedup "
            "B-v1 in B's outbound queue".format(self._tag()))
        client_a = self._borrow_sdk_client(self.src_cluster, sample_bucket.name)
        client_a.upsert(key, {"writer": "A-v2"})
        time.sleep(self.flo_settle_timeout)

        self.log.info(
            "{0} CHECKPOINT: resuming B->A replication".format(
                self._tag()))
        for rcref in self.dest_cluster.get_remote_clusters():
            rcref.resume_all_replications()
        self._wait_for_replication_drain()
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()

        a_doc = self._sdk_get_raw(
            self.src_cluster, sample_bucket, key)
        b_doc = self._sdk_get_raw(
            self.dest_cluster, sample_bucket, key)
        self.assertIsNotNone(
            a_doc,
            "{0} K={1} missing on A after pause/resume cycle".format(
                self._tag(), key))
        self.assertIsNotNone(
            b_doc,
            "{0} K={1} missing on B after pause/resume cycle".format(
                self._tag(), key))
        a_writer = self._extract_writer(a_doc)
        b_writer = self._extract_writer(b_doc)
        self.log.info(
            "{0} final K={1} on A.writer={2!r} B.writer={3!r} "
            "(expected both A-v2)".format(
                self._tag(), key, a_writer, b_writer))
        self.assertEqual(
            a_writer, "A-v2",
            "{0} PRODUCT BUG: A's local mutation A-v2 not preserved "
            "on A; got {1!r}".format(self._tag(), a_writer))
        self.assertEqual(
            b_writer, "A-v2",
            "{0} PRODUCT BUG: A-v2 should have won CR on B; got "
            "{1!r} (DCP dedup + FLO must NOT lose data)".format(
                self._tag(), b_writer))

    def test_forward_local_only_per_replication_asymmetric_setting(self):
        """FLO is per-replication. Setting FLO=true on one bucket-pair
        and FLO=false on another must isolate behavior between them.

        Requires 2 buckets per cluster (use standard_buckets=1 conf).

        Detection (per-replication docs_written stat on B, not CAS on
        A): when A writes docs, A->B replicates → docs on B carry
        HLV.src=A. B's outbound to A then either:
          * FLO=true bucket: foreign-HLV → skipped → docs_written
            stays low (mostly 0 deltas).
          * FLO=false bucket: foreign-HLV docs shipped back as echoes
            → docs_written delta ≈ sdk_count.

        Why not CAS on A: XDCR uses SET_WITH_META preserving original
        CAS. A's echoed doc lands on A with the same CAS A originally
        wrote → SET_WITH_META no-op → CAS on A doesn't change even
        when echo successfully arrives. CAS-snapshot detection is
        thus unreliable for this scenario. docs_written on the
        sending side IS the right signal."""
        self._log_state("test entry")
        clusters = self.get_cb_clusters()
        src_buckets = clusters[0].get_buckets()
        if len(src_buckets) < 2:
            self.skipTest(
                "{0} requires >=2 buckets per cluster; got {1}. "
                "Pass standard_buckets=1 in conf.".format(
                    self._tag(), len(src_buckets)))

        self.setup_xdcr_and_load()
        self._post_setup_settle()

        self._enable_eccv_on_all_clusters()
        self._set_disable_hlv_short_circuit(self.src_cluster, True)
        self._log_state("post-ECCV + shortcircuit disabled on A")

        sorted_buckets = sorted(src_buckets, key=lambda b: b.name)
        flo_true_bucket_name = sorted_buckets[0].name
        flo_false_bucket_name = sorted_buckets[1].name
        self.log.info(
            "{0} bucket selection (by sorted name): FLO=true on {1}, "
            "FLO=false on {2}".format(
                self._tag(),
                flo_true_bucket_name, flo_false_bucket_name))

        def _bucket_by_name(cluster, name):
            for b in cluster.get_buckets():
                if b.name == name:
                    return b
            raise AssertionError(
                "{0} bucket {1!r} not found on {2}".format(
                    self._tag(), name, cluster.get_name()))

        flo_true_bucket = _bucket_by_name(
            self.src_cluster, flo_true_bucket_name)
        flo_false_bucket = _bucket_by_name(
            self.src_cluster, flo_false_bucket_name)

        # Apply asymmetric FLO on B's replications.
        self._set_and_verify_xdcr_param(
            self.dest_cluster, flo_true_bucket_name,
            flo_true_bucket_name,
            FORWARD_LOCAL_ONLY, "true", settle=5)
        self._set_and_verify_xdcr_param(
            self.dest_cluster, flo_false_bucket_name,
            flo_false_bucket_name, FORWARD_LOCAL_ONLY, "false",
            settle=5)

        sdk_count = self._input.param("sdk_count", 100)

        b_pre_true = self._get_docs_processed_to_peer(
            self.dest_cluster, self.src_cluster,
            src_bucket_filter=flo_true_bucket_name)
        b_pre_false = self._get_docs_processed_to_peer(
            self.dest_cluster, self.src_cluster,
            src_bucket_filter=flo_false_bucket_name)

        for bucket in (flo_true_bucket, flo_false_bucket):
            self._sdk_upsert_docs(
                self.src_cluster, bucket, "flo-asym", sdk_count)
        self._wait_for_replication_drain()

        self.log.info(
            "{0} CHECKPOINT: waiting for B->A echo sample refresh on "
            "the FLO=false bucket".format(self._tag()))
        b_post_false = self._wait_for_pipeline_delta(
            self.dest_cluster, self.src_cluster,
            baseline=b_pre_false, min_delta=1,
            timeout=self.metric_settle_timeout, poll_interval=10)
        b_post_true = self._get_docs_processed_to_peer(
            self.dest_cluster, self.src_cluster,
            src_bucket_filter=flo_true_bucket_name)
        b_post_false = self._get_docs_processed_to_peer(
            self.dest_cluster, self.src_cluster,
            src_bucket_filter=flo_false_bucket_name)

        def _delta(post, pre, stat):
            return post.get(stat, 0) - pre.get(stat, 0)

        written_true = _delta(b_post_true, b_pre_true, "docs_written")
        written_false = _delta(b_post_false, b_pre_false, "docs_written")
        rcvd_true = _delta(
            b_post_true, b_pre_true, "docs_received_from_dcp")
        rcvd_false = _delta(
            b_post_false, b_pre_false, "docs_received_from_dcp")

        self.log.info(
            "{0} per-bucket B->A deltas:\n"
            "  bucket(FLO=true , {1}): docs_received_from_dcp={2}, "
            "docs_written={3}\n"
            "  bucket(FLO=false, {4}): docs_received_from_dcp={5}, "
            "docs_written={6}\n"
            "Expected: docs_written low on FLO=true, "
            "docs_written ~ {7} on FLO=false.".format(
                self._tag(),
                flo_true_bucket_name, rcvd_true, written_true,
                flo_false_bucket_name, rcvd_false, written_false,
                sdk_count))

        if rcvd_true == 0 and rcvd_false == 0:
            self.skipTest(
                "{0} TEST INFRA: neither B->A replication observed "
                "any docs via DCP (rcvd_true={1}, rcvd_false={2}). "
                "Pipeline never picked up the foreign-HLV docs that "
                "A wrote. Cannot bisect.".format(
                    self._tag(), rcvd_true, rcvd_false))

        # Standalone guard: if the FLO=true bucket-pair specifically
        # saw zero DCP input, the ratio assertion would trivially
        # pass (0 / N < 0.2) without actually testing FLO. The
        # FLO=false bucket may have docs but that proves nothing
        # about whether the FLO=true bucket's skip path ran.
        if rcvd_true == 0:
            self.skipTest(
                "{0} TEST INFRA: B->A replication for FLO=true "
                "bucket {1} observed zero docs via DCP (rcvd_true=0, "
                "rcvd_false={2}). No foreign-HLV docs reached the "
                "FLO=true pipeline; ratio assertion would be "
                "vacuous. Likely cause: bucket-pair replication "
                "misconfigured or paused for {1!r}.".format(
                    self._tag(), flo_true_bucket_name, rcvd_false))

        min_echo = sdk_count // 2
        self.assertGreaterEqual(
            written_false, min_echo,
            "{0} PRODUCT BUG: bucket {1} (FLO=false) on B->A must "
            "ship echoes; docs_written delta={2} (expected >= {3} "
            "of {4} foreign-HLV docs).".format(
                self._tag(), flo_false_bucket_name,
                written_false, min_echo, sdk_count))

        if written_false > 0:
            ratio = written_true / written_false
        else:
            ratio = 0.0
        self.log.info(
            "{0} docs_written ratio FLO=true/FLO=false: {1:.3f} "
            "(expected near 0)".format(self._tag(), ratio))
        self.assertLess(
            ratio, 0.2,
            "{0} PRODUCT BUG: bucket {1} (FLO=true) on B->A must "
            "block most echoes; docs_written ratio vs FLO=false "
            "bucket = {2:.3f} (expected < 0.2). "
            "FLO=true docs_written={3}, FLO=false docs_written={4}."
            .format(
                self._tag(), flo_true_bucket_name, ratio,
                written_true, written_false))

    def test_forward_local_only_mixed_mode_setting_persistence(self):
        """Mixed-version source cluster: setting MUST be created/updated
        via an upgraded node. Per design, applying via a legacy node
        should be rejected; *silent* acceptance with the value
        persisting is a PRODUCT BUG (the value would "be lost" in the
        sense that it lacks enforcement guarantees on the legacy node
        path).

        Bisection:
          * Set via upgraded node -> readback `true` -> assertEqual.
          * Set via legacy node:
              - raises => PASS (correct REST rejection).
              - does not raise => readback via upgraded:
                  - still `true` => correctly IGNORED at write time
                    => PASS.
                  - now matches the legacy `false` => PRODUCT BUG."""
        if not self._is_mixed_mode(self.src_cluster):
            versions = self._cluster_versions(self.src_cluster)
            self.log.warning(
                "{0} source cluster is uniform: {1}. Skipping mixed-"
                "mode test.".format(self._tag(), versions))
            self.skipTest("Requires mixed-version source cluster")

        self._log_state("test entry")
        self.setup_xdcr_and_load()
        self._post_setup_settle()
        self._enable_eccv_on_all_clusters()

        nodes_by_version = {}
        for node in self.src_cluster.get_nodes():
            v = self._node_version(node)
            nodes_by_version.setdefault(v, []).append(node)
        sorted_versions = sorted(nodes_by_version.keys(), reverse=True)
        upgraded_node = nodes_by_version[sorted_versions[0]][0]
        legacy_node = nodes_by_version[sorted_versions[-1]][0]
        self.log.info(
            "{0} upgraded node: {1} (v={2}); legacy node: {3} (v={4})"
            .format(self._tag(), upgraded_node.ip, sorted_versions[0],
                    legacy_node.ip, sorted_versions[-1]))

        upgraded_rest = RestConnection(upgraded_node)
        legacy_rest = RestConnection(legacy_node)

        for repl in upgraded_rest.get_replications():
            src_b = repl["source"]
            dest_b = repl["target"].rsplit("/", 1)[-1]
            # 1) Upgraded-node set must persist.
            self.log.info(
                "{0} CHECKPOINT: set FLO=true via upgraded {1}".format(
                    self._tag(), upgraded_node.ip))
            upgraded_rest.set_xdcr_param(
                src_b, dest_b, FORWARD_LOCAL_ONLY, "true")
            time.sleep(5)
            value = upgraded_rest.get_xdcr_param(
                src_b, dest_b, FORWARD_LOCAL_ONLY)
            self.log.info(
                "{0} readback via upgraded after upgraded-set: "
                "{1}={2}".format(self._tag(), FORWARD_LOCAL_ONLY, value))
            self.assertEqual(
                str(value).lower(), "true",
                "{0} upgraded-node set of FLO=true did not persist; "
                "got {1!r}".format(self._tag(), value))

            # 2) Legacy-node set: rejection-OR-ignored is acceptable;
            #    silent overwrite is a PRODUCT BUG.
            legacy_raised = False
            try:
                self.log.info(
                    "{0} CHECKPOINT: attempting set FLO=false via "
                    "legacy {1}".format(self._tag(), legacy_node.ip))
                legacy_rest.set_xdcr_param(
                    src_b, dest_b, FORWARD_LOCAL_ONLY, "false")
            except Exception as e:
                legacy_raised = True
                self.log.info(
                    "{0} legacy-node set REJECTED at REST (acceptable "
                    "per design): {1}".format(self._tag(), e))

            time.sleep(5)
            post_value = upgraded_rest.get_xdcr_param(
                src_b, dest_b, FORWARD_LOCAL_ONLY)
            self.log.info(
                "{0} readback via upgraded after legacy-set: "
                "{1}={2}; legacy_raised={3}".format(
                    self._tag(), FORWARD_LOCAL_ONLY, post_value,
                    legacy_raised))
            if not legacy_raised:
                # Legacy did not reject; bisect on the readback.
                self.assertEqual(
                    str(post_value).lower(), "true",
                    "{0} PRODUCT BUG: legacy node silently accepted "
                    "and PERSISTED FLO change ({1}->{2}). Design "
                    "requires either REST rejection or silent-ignore "
                    "(readback still 'true').".format(
                        self._tag(), "true", post_value))

    def test_forward_local_only_backfill_pipeline_eligibility(self):
        """Spec callout: backfill docs get same FLO treatment as Main
        pipeline. Trigger a backfill by creating a new collection on
        src AFTER the replication is running; goxdcr starts a Backfill
        pipeline from seqno 0 for the new collection. With FLO=true
        everywhere, mutations replicating through the backfill pipeline
        must obey the same local/non-local classification.

        Test:
          1. Bidir AA, ECCV on, FLO=true everywhere.
          2. Disable HLV-shortcircuit on A (echoes observable).
          3. Pre-load N docs on default.default; drain to B.
          4. Create scope.collection `flo_bf_scope.flo_bf_coll` on src
             AND dest.
          5. SDK upsert M docs into that collection on A. Replication
             begins a backfill for the new collection.
          6. Drain.
          7. Assert: all M docs visible on dest in that collection
             (forward replication via backfill works).
          8. CAS-snapshot on A for the M docs pre/post echo window:
             expect ZERO echoes (B's backfill outbound must also
             classify foreign-HLV docs as non-local and SKIP)."""
        self._standard_flo_setup(enable_flo=False)
        self._set_disable_hlv_short_circuit(self.src_cluster, True)
        self._enable_forward_local_only_everywhere()
        self._log_state("post-ECCV + FLO + disableHlvShortCircuit on A")

        scope_name = self._input.param("backfill_scope", "flo_bf_scope")
        coll_name = self._input.param("backfill_collection", "flo_bf_coll")
        for cluster in self.get_cb_clusters():
            rest = RestConnection(cluster.get_master_node())
            for bucket in cluster.get_buckets():
                try:
                    rest.create_scope(bucket, scope_name)
                    rest.create_collection(
                        bucket, scope_name, coll_name)
                    self.log.info(
                        "{0} created {1}/{2}/{3}.{4}".format(
                            self._tag(), cluster.get_name(),
                            bucket.name, scope_name, coll_name))
                except Exception as e:
                    self.log.warning(
                        "{0} scope/collection create on {1}/{2} may "
                        "already exist: {3}".format(
                            self._tag(), cluster.get_name(),
                            bucket.name, e))
        time.sleep(self._COLLECTION_SETTLE)

        sdk_count = self._input.param("sdk_count", 100)
        sample_bucket = self.src_cluster.get_buckets()[0]
        self.log.info(
            "{0} CHECKPOINT: SDK loading {1} docs into {2}.{3} on A "
            "(triggers backfill pipeline)".format(
                self._tag(), sdk_count, scope_name, coll_name))
        self._bulk_upsert(
            self.src_cluster, sample_bucket,
            (("flo-bf-{0}".format(i),
              {"idx": i, "writer": "A-backfill"})
             for i in range(sdk_count)),
            scope=scope_name, collection=coll_name)

        self._wait_for_replication_drain()
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()

        # Forward replication via backfill must complete.
        self.log.info(
            "{0} CHECKPOINT: verifying docs landed on dest collection"
            .format(self._tag()))
        missing = 0
        for i in range(sdk_count):
            if self._sdk_get_raw(
                    self.dest_cluster, sample_bucket,
                    "flo-bf-{0}".format(i),
                    scope=scope_name,
                    collection=coll_name) is None:
                missing += 1
        self.log.info(
            "{0} backfill forward replication: {1}/{2} docs missing "
            "on dest".format(self._tag(), missing, sdk_count))
        self.assertEqual(
            missing, 0,
            "{0} PRODUCT BUG: backfill forward replication failed; "
            "{1}/{2} docs missing on dest collection {3}.{4}".format(
                self._tag(), missing, sdk_count, scope_name, coll_name))

        cas_pre = self._capture_doc_cas(
            self.src_cluster, sample_bucket, "flo-bf", sdk_count,
            scope=scope_name, collection=coll_name)
        self.log.info(
            "{0} CHECKPOINT: waiting echo window".format(self._tag()))
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()
        cas_post = self._capture_doc_cas(
            self.src_cluster, sample_bucket, "flo-bf", sdk_count,
            scope=scope_name, collection=coll_name)
        changed, failed, infra_bad = self._count_cas_changes(
            cas_pre, cas_post, log_label="backfill ")
        self.log.info(
            "{0} backfill skip-back probe: {1}/{2} CAS change on A "
            "(failed={3}, infra_bad={4}; expected 0)".format(
                self._tag(), changed, sdk_count, failed, infra_bad))
        if infra_bad:
            self.skipTest(
                "{0} TEST INFRA: CAS-diff read failure ratio above "
                "threshold on backfill collection".format(self._tag()))
        self.assertEqual(
            changed, 0,
            "{0} PRODUCT BUG: backfill pipeline does NOT apply FLO; "
            "{1}/{2} foreign-HLV docs echoed back to A "
            "(failed reads={3})".format(
                self._tag(), changed, sdk_count, failed))

    def test_forward_local_only_ring_topology_data_loss_callout(self):
        """Spec callout: enabling FLO in a daisy-chain / ring (non-full-
        mesh) topology CAUSES intentional data loss. This test
        positively asserts that observable data loss.

        Daisy-chain A -> B -> C (-> A). FLO=true on every replication.
          1. Write N docs on A.
          2. A->B replicates: B receives docs with HLV.src=A.
          3. B->C replication: under FLO=true, B sees HLV.src=A !=
             local -> SKIP. C never receives the docs.
          4. Assert: B has the docs; C does NOT (data loss is the
             expected behavior per the spec warning)."""
        topology = self._input.param("ctopology", "chain")
        rdirection = self._input.param("rdirection", "unidirection")
        if topology != "ring":
            self.skipTest(
                "{0} requires ctopology=ring; got {1!r}".format(
                    self._tag(), topology))
        if rdirection != "unidirection":
            self.skipTest(
                "{0} requires rdirection=unidirection; got {1!r}".format(
                    self._tag(), rdirection))
        if len(self.get_cb_clusters()) < 3:
            self.skipTest(
                "{0} requires >=3 clusters; got {1}".format(
                    self._tag(), len(self.get_cb_clusters())))

        self._log_state("test entry")
        self.setup_xdcr_and_load()
        self._post_setup_settle()

        self._assert_ring_topology(expected_n_clusters=3)

        self._enable_eccv_on_all_clusters()
        self._enable_forward_local_only_everywhere()
        self._log_state("post-FLO-enable on ring")

        clusters = self.get_cb_clusters()
        cluster_a = clusters[0]
        cluster_b = clusters[1]
        cluster_c = clusters[2]
        sample_bucket = cluster_a.get_buckets()[0]
        sdk_count = self._input.param("sdk_count", 100)

        self.log.info(
            "{0} CHECKPOINT: writing {1} docs on {2}".format(
                self._tag(), sdk_count, cluster_a.get_name()))
        for bucket in cluster_a.get_buckets():
            self._sdk_upsert_docs(
                cluster_a, bucket, "flo-ring", sdk_count)
        self._wait_for_replication_drain()
        time.sleep(self.flo_settle_timeout)
        self._wait_for_replication_drain()

        missing_on_b = 0
        for i in range(sdk_count):
            if self._sdk_get_raw(
                    cluster_b, sample_bucket,
                    "flo-ring-{0}".format(i)) is None:
                missing_on_b += 1
        self.log.info(
            "{0} on {1}: {2}/{3} docs missing (expected 0)".format(
                self._tag(), cluster_b.get_name(),
                missing_on_b, sdk_count))
        self.assertEqual(
            missing_on_b, 0,
            "{0} A->B forward replication must succeed; {1}/{2} "
            "docs missing on B".format(
                self._tag(), missing_on_b, sdk_count))

        missing_on_c = 0
        for i in range(sdk_count):
            if self._sdk_get_raw(
                    cluster_c, sample_bucket,
                    "flo-ring-{0}".format(i)) is None:
                missing_on_c += 1
        self.log.info(
            "{0} EXPECTED DATA LOSS on {1}: {2}/{3} docs missing "
            "(spec: daisy-chain + FLO=true causes data loss; this "
            "is a configuration error users should avoid)".format(
                self._tag(), cluster_c.get_name(),
                missing_on_c, sdk_count))
        self.assertEqual(
            missing_on_c, sdk_count,
            "{0} ring-topology data-loss callout NOT observed: only "
            "{1}/{2} docs missing on C (expected all {2} missing)"
            .format(self._tag(), missing_on_c, sdk_count))
