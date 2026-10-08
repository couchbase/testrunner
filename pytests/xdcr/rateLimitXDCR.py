"""XDCR under KV data-service rate limiting (Totoro / 8.5, MB-64385, CBQE-8920).

How KV throttles, as far as these tests depend on it:

- Node level, one cluster-wide POST to /pools/default/settings/memcached/global:
  throttle_enabled (default false), node_capacity (units/s per node, default
  uint64 max), read_unit_size (4096 B) and write_unit_size (1024 B).
- Bucket level: throttleReserved (default 0) and throttleHardLimit (default
  uint64 max), units/s, enforced by each node's memcached on its own.
- A command runs when the bucket is under its reservation, is throttled at its
  hard limit, and otherwise borrows from the node's free pool
  (node_capacity - sum of reservations); with the pool empty it is throttled.
  The budget refills every second.
- A throttled command is parked and the connection stops reading until the
  next tick. No status is returned unless the client negotiated
  NonBlockingThrottlingMode, which goxdcr does not, so XDCR only sees latency.
- Only @ns_server (intra-cluster replication, rebalance) is unthrottled.
  XDCR's source DCP (@goxdcr) is charged read units on the source bucket and
  its SetWithMeta/DelWithMeta/GetMeta are charged on the target bucket.
  MB-70229 (an "unthrottled" privilege for XDCR) was resolved Won't Do, so
  CBQE-8920's original "XDCR is excluded" expectation is inverted here.

The node and bucket limits are set from the test with rl_* params rather than
the conf params of the shared rate-limiting infra (node_capacity,
throttle_enabled, bucket_throttle_*): the infra applies those identically to
every node and bucket of every cluster during setUp, and most cases here need
one side throttled and the other not.

Counters are read straight from memcached ('all' stats group, per node) rather
than Prometheus, so a reading taken right after a phase is not a scrape behind.
"""
import time

from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from .xdcrnewbasetests import XDCRNewBaseTest, NodeHelper, OPS

MAX_U64 = 18446744073709551615

# Per-bucket memcached counters, summed over the nodes of a cluster
KV_COUNTERS = ("throttle_count_total", "ru_total", "wu_total",
               "reject_count_total")

# goxdcr.log lines worth counting while a target is throttled. Only a pipeline
# restart fails a test; the others are reported so a run shows how hard xmem
# had to work (resends that ran out of retries, connections it repaired)
GOXDCR_SIGNALS = {
    "resend_exhausted": "maximum retry",
    "conn_repaired": "is broken due to",
    "xmem_stuck": "Xmem is stuck",
    "pipeline_restart": "Try to fix Pipeline",
}


class RateLimitXDCR(XDCRNewBaseTest):

    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        # The base replaces _num_items with the collection density's doc
        # count (1000 for the default "low"), so read the conf value directly
        self.rl_items = int(self._input.param("items", 20000))
        self.rl_hard_limit = int(self._input.param("rl_hard_limit", 200))
        self.rl_second_hard_limit = int(self._input.param(
            "rl_second_hard_limit", self.rl_hard_limit * 2))
        self.rl_reserved = int(self._input.param("rl_reserved", 50))
        self.rl_reserved_b = int(self._input.param("rl_reserved_b", 1000000))
        self.rl_free_pool = int(self._input.param("rl_free_pool", 50))
        self.rl_dest_hard_limit = int(self._input.param(
            "rl_dest_hard_limit", self.rl_hard_limit // 2))
        self.convergence_timeout = int(self._input.param(
            "convergence_timeout", 1800))
        self.settle_secs = int(self._input.param("settle_secs", 10))
        self.pause_secs = int(self._input.param("pause_secs", 60))
        self.soak_secs = int(self._input.param("soak_secs", 600))
        self.soak_window_secs = int(self._input.param("soak_window_secs", 60))

        # unittest skips tearDown when setUp raises, which would leave the
        # clusters uncleaned and possibly throttled
        try:
            # A run that died mid-test leaves throttling on: the node settings
            # live in chronicle and outlive the buckets
            for cluster in self.get_cb_clusters():
                try:
                    self._reset_throttling(cluster)
                except Exception as e:
                    self.fail("Cannot configure KV throttling on {0}, which "
                              "needs an Enterprise cluster at 8.5 compat: {1}"
                              .format(cluster.get_name(), e))
            self._goxdcr_baseline = self._goxdcr_signal_counts()
        except Exception:
            self.tearDown()
            raise

    def tearDown(self):
        for cluster in self.get_cb_clusters():
            try:
                self._reset_throttling(cluster)
            except Exception as e:
                self.log.warning("Could not reset throttling on {0}: {1}"
                                 .format(cluster.get_name(), e))
        super().tearDown()

    # ------------------------------------------------------------------
    # Throttle configuration
    # ------------------------------------------------------------------
    def _set_node_throttle(self, cluster, enabled, node_capacity=None):
        """Set the cluster-wide node settings and read them back on every
        node of the cluster"""
        rest = RestConnection(cluster.get_master_node())
        rest.set_node_throttle_settings(throttle_enabled=enabled,
                                        node_capacity=node_capacity)
        for node in cluster.get_nodes():
            settings = RestConnection(node).get_node_throttle_settings()
            self.assertEqual(
                settings.get("throttle_enabled"), enabled,
                "throttle_enabled on {0} reads {1} after setting {2}".format(
                    node.ip, settings.get("throttle_enabled"), enabled))
            if node_capacity is not None:
                self.assertEqual(
                    int(settings.get("node_capacity", -1)), int(node_capacity),
                    "node_capacity on {0} reads {1} after setting {2}".format(
                        node.ip, settings.get("node_capacity"), node_capacity))

    def _set_bucket_limits(self, cluster, bucket_name, reserved=None,
                           hard_limit=None):
        """Set a bucket's limits, then check ns_server reports them and that
        every node's memcached actually received them (MB-72661)"""
        rest = RestConnection(cluster.get_master_node())
        rest.set_bucket_throttle_limits(bucket_name,
                                        throttle_reserved=reserved,
                                        throttle_hard_limit=hard_limit)
        limits = rest.get_bucket_throttle_limits(bucket_name)
        if reserved is not None:
            self.assertEqual(int(limits.get("throttleReserved", -1)), reserved,
                             "{0}/{1} throttleReserved reads {2}".format(
                                 cluster.get_name(), bucket_name, limits))
        if hard_limit is not None:
            self.assertEqual(int(limits.get("throttleHardLimit", -1)),
                             hard_limit,
                             "{0}/{1} throttleHardLimit reads {2}".format(
                                 cluster.get_name(), bucket_name, limits))
        self._wait_for_kv_limits(cluster, bucket_name, reserved, hard_limit)

    def _wait_for_kv_limits(self, cluster, bucket_name, reserved, hard_limit,
                            timeout=60):
        expected = {}
        if reserved is not None:
            expected["throttle_reserved"] = reserved
        if hard_limit is not None:
            expected["throttle_hard_limit"] = hard_limit
        end_time = time.time() + timeout
        while True:
            mismatches = []
            for node in cluster.get_nodes():
                stats = self._kv_stats(node, bucket_name)
                for stat, value in expected.items():
                    if int(stats.get(stat, -1)) != value:
                        mismatches.append("{0} {1}={2}".format(
                            node.ip, stat, stats.get(stat)))
            if not mismatches:
                return
            if time.time() > end_time:
                self.fail("KV never received the limits of {0}/{1} set over "
                          "REST (expected {2}): {3}".format(
                              cluster.get_name(), bucket_name, expected,
                              mismatches))
            self.sleep(2, "Waiting for KV to pick up {0}".format(expected))

    def _throttle(self, cluster, hard_limit=None, reserved=None,
                  node_capacity=None, enabled=True, buckets=None):
        """Enable (or disable) throttling on a cluster and set the limits of
        its buckets. Capacity goes first: ns_server rejects a reservation the
        node capacity cannot hold"""
        self.log.info("Throttling {0}: enabled={1}, node_capacity={2}, "
                      "reserved={3}, hard_limit={4}".format(
                          cluster.get_name(), enabled, node_capacity,
                          reserved, hard_limit))
        self._set_node_throttle(cluster, enabled, node_capacity)
        for bucket in buckets or cluster.get_buckets():
            self._set_bucket_limits(cluster, bucket.name, reserved, hard_limit)

    def _reset_throttling(self, cluster):
        """Back to the shipped defaults. Node settings first: they outlive the
        buckets, so a failing bucket must not leave them behind, and capacity
        restored to its maximum can never undercut a reservation"""
        rest = RestConnection(cluster.get_master_node())
        rest.set_node_throttle_settings(throttle_enabled=False,
                                        node_capacity=MAX_U64)
        errors = []
        for bucket in cluster.get_buckets():
            try:
                rest.set_bucket_throttle_limits(bucket.name,
                                                throttle_reserved=0,
                                                throttle_hard_limit=MAX_U64)
            except Exception as e:
                errors.append("{0}: {1}".format(bucket.name, e))
        if errors:
            raise Exception("Could not reset the bucket limits on {0}: {1}"
                            .format(cluster.get_name(), errors))

    # ------------------------------------------------------------------
    # KV counters
    # ------------------------------------------------------------------
    def _kv_stats(self, node, bucket_name):
        client = MemcachedClientHelper.direct_client(node, bucket_name)
        try:
            return client.stats()
        finally:
            client.close()

    def _kv_counters(self, cluster, bucket_name):
        totals = dict.fromkeys(KV_COUNTERS, 0)
        for node in cluster.get_nodes():
            stats = self._kv_stats(node, bucket_name)
            missing = [key for key in KV_COUNTERS if key not in stats]
            if missing:
                self.fail("memcached on {0} reports no {1} for bucket {2}: "
                          "the build predates the KV rate-limiting stats"
                          .format(node.ip, missing, bucket_name))
            for key in KV_COUNTERS:
                totals[key] += int(stats[key])
        return totals

    def _snapshot(self, clusters=None):
        """{(cluster name, bucket name): counters} for every bucket"""
        snap = {}
        for cluster in clusters or self.get_cb_clusters():
            for bucket in cluster.get_buckets():
                snap[(cluster.get_name(), bucket.name)] = \
                    self._kv_counters(cluster, bucket.name)
        return snap

    def _delta(self, before, after, label=""):
        delta = {key: {stat: after[key][stat] - before[key][stat]
                       for stat in KV_COUNTERS}
                 for key in after if key in before}
        for (cluster_name, bucket_name), counters in sorted(delta.items()):
            self.log.info("{0} KV counter delta {1}/{2}: {3}".format(
                label, cluster_name, bucket_name, counters))
        return delta

    def _matching(self, delta, cluster, bucket_name):
        """The delta entries for the cluster (and bucket), refusing an empty
        match so a wrong name cannot make an assertion pass vacuously"""
        matched = [((cluster_name, name), counters)
                   for (cluster_name, name), counters in delta.items()
                   if cluster_name == cluster.get_name()
                   and (not bucket_name or name == bucket_name)]
        self.assertTrue(matched, "No KV counters were captured for {0}/{1}"
                        .format(cluster.get_name(), bucket_name or "*"))
        return matched

    def _assert_throttled(self, delta, cluster, bucket_name=None,
                          unit="ru_total"):
        """Every (or the given) bucket of the cluster was throttled and was
        charged `unit`"""
        for (cluster_name, name), counters in self._matching(
                delta, cluster, bucket_name):
            self.assertGreater(
                counters["throttle_count_total"], 0,
                "{0}/{1} was never throttled: {2}".format(
                    cluster_name, name, counters))
            self.assertGreater(
                counters[unit], 0,
                "{0}/{1} was charged no {2}: {3}".format(
                    cluster_name, name, unit, counters))

    def _assert_not_throttled(self, delta, cluster, bucket_name=None):
        for (cluster_name, name), counters in self._matching(
                delta, cluster, bucket_name):
            self.assertEqual(
                counters["throttle_count_total"], 0,
                "{0}/{1} was throttled although it should not have been: "
                "{2}".format(cluster_name, name, counters))

    # ------------------------------------------------------------------
    # goxdcr signals
    # ------------------------------------------------------------------
    def _cluster_nodes(self):
        return [node for cluster in self.get_cb_clusters()
                for node in cluster.get_nodes()]

    def _goxdcr_signal_counts(self):
        counts = dict.fromkeys(GOXDCR_SIGNALS, 0)
        for node in self._cluster_nodes():
            for signal, pattern in GOXDCR_SIGNALS.items():
                _, count = NodeHelper.check_goxdcr_log(node, pattern)
                counts[signal] += count
        return counts

    def _goxdcr_signal_delta(self, label=""):
        now = self._goxdcr_signal_counts()
        delta = {signal: now[signal] - self._goxdcr_baseline[signal]
                 for signal in GOXDCR_SIGNALS}
        self.log.info("{0} goxdcr signals since setUp: {1}".format(
            label, delta))
        return delta

    def _assert_no_pipeline_restart(self, label=""):
        """The agreed bar under throttling: xmem may resend and repair
        connections, but the pipeline must not have to be restarted"""
        delta = self._goxdcr_signal_delta(label)
        if delta["pipeline_restart"] > 0:
            for node in self._cluster_nodes():
                matches, _ = NodeHelper.check_goxdcr_log(
                    node, GOXDCR_SIGNALS["pipeline_restart"])
                for line in matches[-5:]:
                    self.log.error("{0}: {1}".format(node.ip, line))
            self.fail("{0}: {1} pipeline restart(s) while throttled, "
                      "see the goxdcr.log lines above".format(
                          label, delta["pipeline_restart"]))
        return delta

    # ------------------------------------------------------------------
    # Load, convergence and verification
    # ------------------------------------------------------------------
    def _async_load(self, cluster, bucket, prefix, start, end, op=OPS.CREATE):
        """Load one bucket with its own generator, recording into the
        bucket's kv store so verify_results covers it. A throttled batch has
        the loading task's own 60s per batch: Cluster.async_load_gen_docs
        does not forward timeout_secs"""
        gen = BlobGenerator(prefix, prefix, self._value_size,
                            start=start, end=end)
        return self.get_cluster_op().async_load_gen_docs(
            cluster.get_master_node(), bucket.name, gen, bucket.kvs[1], op,
            batch_size=1000, compression=self._sdk_compression)

    def _timed_reads(self, cluster, bucket, prefix, count):
        """Read `count` keys of the cluster's initial load and return the
        elapsed seconds"""
        start = time.time()
        self._async_load(cluster, bucket, prefix, 0, count,
                         op="read").result()
        elapsed = time.time() - start
        self.log.info("Read {0} docs from {1}/{2} in {3:.1f}s ({4:.0f} "
                      "docs/s)".format(count, cluster.get_name(), bucket.name,
                                       elapsed, count / max(elapsed, 0.001)))
        return elapsed

    def _docs_written(self):
        """docs_written summed over the replications C1 -> C2. A failed read
        fails the test rather than reading as zero progress"""
        stats = self.get_docs_processed_to_peer(self.src_cluster,
                                                self.dest_cluster)
        failed = [f for f in stats["_failed_reads"] if "docs_written" in f]
        if failed:
            self.fail("Could not read docs_written: {0}".format(failed))
        return stats["docs_written"]

    def _wait_for_docs_written(self, baseline, min_delta, timeout=300,
                               poll_interval=10):
        """Wait until docs_written has advanced by min_delta over baseline;
        fails instead of returning a stale sample on timeout"""
        end_time = time.time() + timeout
        while True:
            current = self._docs_written()
            if current - baseline >= min_delta:
                return current
            if time.time() > end_time:
                self.fail("docs_written advanced {0} (< {1}) within {2}s"
                          .format(current - baseline, min_delta, timeout))
            self.sleep(poll_interval, "docs_written={0}, waiting for +{1}"
                       .format(current, min_delta))

    def _wait_for_quiet(self, cluster, timeout=120, poll_interval=5):
        """Wait until no bucket of the cluster is charged any more units, so
        writes parked or in flight when replication paused have landed"""
        end_time = time.time() + timeout
        previous = self._snapshot([cluster])
        while True:
            self.sleep(poll_interval, "Waiting for {0} to go quiet".format(
                cluster.get_name()))
            current = self._snapshot([cluster])
            if all(current[key]["ru_total"] == previous[key]["ru_total"]
                   and current[key]["wu_total"] == previous[key]["wu_total"]
                   for key in current):
                return
            if time.time() > end_time:
                self.fail("{0} was still being charged units {1}s after "
                          "replication paused".format(cluster.get_name(),
                                                      timeout))
            previous = current

    def _total_changes_left(self):
        return sum(self.get_total_changes_left(cluster)
                   for cluster in self.get_cb_clusters()
                   if cluster.get_remote_clusters())

    def _active_items(self, cluster, bucket_name):
        return sum(int(self._kv_stats(node, bucket_name)["curr_items"])
                   for node in cluster.get_nodes())

    def _expected_items(self, bucket_name):
        """Docs every cluster holds once replication has caught up. The
        tests load disjoint key prefixes and only the bidirectional one loads
        the target, so the union of the kv stores is their sum"""
        return sum(len(cluster.get_bucket_by_name(bucket_name).kvs[1])
                   for cluster in self.get_cb_clusters())

    def _wait_for_convergence(self, label, timeout=None, poll_interval=15):
        """Wait until every bucket on every cluster holds the docs its kv
        stores say were loaded and no replication has changes left, on two
        polls in a row. Item counts come from memcached: right after a load
        the REST stats samples and changes_left can both still show the
        previous, equal values, which once read as converged with most of
        a phase unreplicated. Returns the elapsed seconds"""
        timeout = timeout or self.convergence_timeout
        start = time.time()
        confirmed = False
        while True:
            lagging = []
            for bucket in self.src_cluster.get_buckets():
                expected = self._expected_items(bucket.name)
                for cluster in self.get_cb_clusters():
                    actual = self._active_items(cluster, bucket.name)
                    if actual != expected:
                        lagging.append("{0}/{1} {2}/{3}".format(
                            cluster.get_name(), bucket.name, actual,
                            expected))
            changes_left = self._total_changes_left()
            elapsed = time.time() - start
            if not lagging and changes_left == 0:
                if confirmed:
                    self.log.info("{0}: converged in {1:.0f}s".format(
                        label, elapsed))
                    return elapsed
                confirmed = True
            else:
                confirmed = False
            if elapsed > timeout:
                self.fail("{0}: no convergence within {1}s, changes_left={2}, "
                          "items actual/expected {3}".format(
                              label, timeout, changes_left, lagging))
            self.sleep(5 if confirmed else poll_interval,
                       "{0}: changes_left={1}, items actual/expected {2}"
                       .format(label, changes_left, lagging or "all match"))

    def _log_budget(self, label, cluster, items, elapsed):
        """What the configured limit allows against what was achieved, for the
        log only: units per doc depend on doc size and on which side is
        throttled, so the two are not asserted against each other"""
        budget = self.rl_hard_limit * len(cluster.get_nodes())
        self.log.info("{0}: {1} docs in {2:.0f}s = {3:.0f} docs/s against a "
                      "budget of {4} units/s on {5} ({6} units/s x {7} nodes)"
                      .format(label, items, elapsed,
                              items / max(elapsed, 0.001), budget,
                              cluster.get_name(), self.rl_hard_limit,
                              len(cluster.get_nodes())))

    def _lift_throttling_and_verify(self):
        """Data checks read every doc, so run them unthrottled once the
        throttled phase has been measured"""
        for cluster in self.get_cb_clusters():
            self._reset_throttling(cluster)
        self.verify_results()

    # ------------------------------------------------------------------
    # P0
    # ------------------------------------------------------------------
    def test_source_throttled_backfill(self):
        """CBQE-8920 (a), corrected: XDCR's source DCP is charged read units
        on the source bucket and is throttled with it. The backfill still
        converges, the unthrottled target is never throttled, and the
        pipeline is not restarted"""
        self.src_cluster.load_all_buckets(self.rl_items, self._value_size)
        self._throttle(self.src_cluster, hard_limit=self.rl_hard_limit)

        before = self._snapshot()
        self.setup_xdcr()
        elapsed = self._wait_for_convergence("source-throttled backfill")
        delta = self._delta(before, self._snapshot(),
                            "source-throttled backfill")

        self._log_budget("source-throttled backfill", self.src_cluster,
                         self.rl_items, elapsed)
        self._assert_throttled(delta, self.src_cluster, unit="ru_total")
        self._assert_not_throttled(delta, self.dest_cluster)
        self._assert_no_pipeline_restart("source-throttled backfill")
        self._lift_throttling_and_verify()

    def test_target_throttled_backfill(self):
        """XDCR's SetWithMeta (and GetMeta above the optimistic threshold) are
        charged to the target bucket and blocked at its hard limit. The
        backfill converges without a pipeline restart and the unthrottled
        source is never throttled"""
        self.src_cluster.load_all_buckets(self.rl_items, self._value_size)
        self._throttle(self.dest_cluster, hard_limit=self.rl_hard_limit)

        before = self._snapshot()
        self.setup_xdcr()
        elapsed = self._wait_for_convergence("target-throttled backfill")
        delta = self._delta(before, self._snapshot(),
                            "target-throttled backfill")

        self._log_budget("target-throttled backfill", self.dest_cluster,
                         self.rl_items, elapsed)
        self.get_docs_processed_to_peer(self.src_cluster, self.dest_cluster)
        self._assert_throttled(delta, self.dest_cluster, unit="wu_total")
        self._assert_not_throttled(delta, self.src_cluster)
        self._assert_no_pipeline_restart("target-throttled backfill")
        self._lift_throttling_and_verify()

    def test_client_load_with_replication_throttled(self):
        """CBQE-8920 (b), corrected: client writes and XDCR share each
        bucket's budget on both clusters. The client load completes without
        errors, replication converges, and both clusters were throttled.
        A replicated doc costs about as many units on the target (GetMeta +
        SetWithMeta) as on the source (the write + its DCP read), so the
        target gets the lower limit (rl_dest_hard_limit) or the source's
        throttled rate would never push it past its own"""
        self._throttle(self.src_cluster, hard_limit=self.rl_hard_limit)
        self._throttle(self.dest_cluster, hard_limit=self.rl_dest_hard_limit)

        self.setup_xdcr()
        before = self._snapshot()
        for task in self.src_cluster.async_load_all_buckets(
                self.rl_items, self._value_size):
            task.result()
        self.async_perform_update_delete()
        self._wait_for_convergence("client load + XDCR, both throttled")
        delta = self._delta(before, self._snapshot(),
                            "client load + XDCR, both throttled")

        self._assert_throttled(delta, self.src_cluster, unit="wu_total")
        self._assert_throttled(delta, self.dest_cluster, unit="wu_total")
        self._assert_no_pipeline_restart("client load + XDCR, both throttled")
        self._lift_throttling_and_verify()

    def test_units_counted_with_throttling_disabled(self):
        """Negative control: with throttle_enabled=false a hard limit that
        would stall everything is not enforced, yet units are still counted
        (MB-70228), so the throttle counters in the other tests are known to
        come from throttling and not from the load itself"""
        for cluster in self.get_cb_clusters():
            self._throttle(cluster, hard_limit=1, enabled=False)

        self.setup_xdcr()
        before = self._snapshot()
        self.src_cluster.load_all_buckets(self.rl_items, self._value_size)
        self._wait_for_convergence("throttling disabled")
        delta = self._delta(before, self._snapshot(), "throttling disabled")

        for cluster in self.get_cb_clusters():
            self._assert_not_throttled(delta, cluster)
        for (cluster_name, bucket_name), counters in delta.items():
            self.assertGreater(
                counters["wu_total"], 0,
                "{0}/{1} counted no write units with throttling disabled: {2}"
                .format(cluster_name, bucket_name, counters))
        self._assert_no_pipeline_restart("throttling disabled")
        self._lift_throttling_and_verify()

    def test_backfill_charges_source_bucket_units(self):
        """The trade-off agreed on MB-70229: an initial backfill competes with
        application reads for the source bucket's budget. The same reads are
        timed alone and then during the backfill; the read units of the
        second window beyond the reads' own are the backfill's"""
        bucket = self.src_cluster.get_bucket_by_name("default")
        prefix = "{0}-key-".format(self.src_cluster.get_name())
        self.src_cluster.load_all_buckets(self.rl_items, self._value_size)
        self._throttle(self.src_cluster, hard_limit=self.rl_hard_limit)

        dest_before = self._snapshot([self.dest_cluster])
        before = self._kv_counters(self.src_cluster, bucket.name)
        alone_secs = self._timed_reads(self.src_cluster, bucket, prefix,
                                       self.rl_items)
        after_alone = self._kv_counters(self.src_cluster, bucket.name)

        self.setup_xdcr()
        with_backfill_secs = self._timed_reads(self.src_cluster, bucket,
                                               prefix, self.rl_items)
        self._wait_for_convergence("backfill alongside reads")
        after_backfill = self._kv_counters(self.src_cluster, bucket.name)

        reads_ru = after_alone["ru_total"] - before["ru_total"]
        window_ru = after_backfill["ru_total"] - after_alone["ru_total"]
        backfill_ru = window_ru - reads_ru
        self.log.info(
            "Reads alone: {0:.1f}s, {1} RU, throttled {2}x. Reads during "
            "backfill: {3:.1f}s; that window charged {4} RU, {5} of them the "
            "backfill's ({6:.2f} RU per replicated doc), throttled {7}x".format(
                alone_secs, reads_ru,
                after_alone["throttle_count_total"]
                - before["throttle_count_total"],
                with_backfill_secs, window_ru, backfill_ru,
                backfill_ru / float(self.rl_items),
                after_backfill["throttle_count_total"]
                - after_alone["throttle_count_total"]))
        # Only the read units are attributable: the reads are throttled on
        # their own, so the window's throttle count says nothing about the
        # backfill. test_source_throttled_backfill covers that part
        self.assertGreater(
            backfill_ru, 0,
            "The backfill charged no read units to the source bucket: "
            "reads alone {0} RU, reads + backfill {1} RU".format(
                reads_ru, window_ru))
        self._assert_not_throttled(
            self._delta(dest_before, self._snapshot([self.dest_cluster]),
                        "unthrottled target"),
            self.dest_cluster)
        self._assert_no_pipeline_restart("backfill alongside reads")
        self._lift_throttling_and_verify()

    # ------------------------------------------------------------------
    # P1
    # ------------------------------------------------------------------
    def test_toggle_throttling_mid_replication(self):
        """Disabling throttling and changing the hard limit take effect on a
        running replication without restarting it: the target is throttled
        while enabled, never while disabled, and the new limit reaches KV"""
        self._throttle(self.dest_cluster, hard_limit=self.rl_hard_limit)
        self.setup_xdcr()
        phases = (("enabled", True, self.rl_hard_limit),
                  ("disabled", False, self.rl_hard_limit),
                  ("re-enabled", True, self.rl_second_hard_limit))
        for index, (label, enabled, hard_limit) in enumerate(phases):
            if index:
                self._set_node_throttle(self.dest_cluster, enabled)
                for bucket in self.dest_cluster.get_buckets():
                    self._set_bucket_limits(self.dest_cluster, bucket.name,
                                            hard_limit=hard_limit)
                self.sleep(self.settle_secs,
                           "Letting ops parked before the toggle drain")
            before = self._snapshot([self.dest_cluster])
            tasks = [self._async_load(self.src_cluster, bucket,
                                      "phase{0}-".format(index), 0,
                                      self.rl_items)
                     for bucket in self.src_cluster.get_buckets()]
            for task in tasks:
                task.result()
            self._wait_for_convergence("phase " + label)
            delta = self._delta(before, self._snapshot([self.dest_cluster]),
                                "phase " + label)
            if enabled:
                self._assert_throttled(delta, self.dest_cluster,
                                       unit="wu_total")
            else:
                self._assert_not_throttled(delta, self.dest_cluster)
        self._assert_no_pipeline_restart("toggle mid-replication")
        self._lift_throttling_and_verify()

    def test_free_pool_isolation(self):
        """The guidance MB-70229 settled on: capacity left unreserved is what
        XDCR borrows beyond its bucket's reservation, and it must never come
        out of another bucket's reservation. Bucket A (replicated, small
        reservation, no hard limit) runs out of reservation plus free pool
        and is throttled (MB-73910); bucket B, loaded at the same time
        within its large reservation, is never throttled.
        Needs standard_buckets=1"""
        bucket_a = self.src_cluster.get_bucket_by_name("default")
        bucket_b = self.src_cluster.get_bucket_by_name("standard_bucket_1")
        self.assertTrue(bucket_a and bucket_b,
                        "Needs the default bucket and standard_buckets=1")
        capacity = self.rl_reserved + self.rl_reserved_b + self.rl_free_pool
        self.src_cluster.load_all_buckets(self.rl_items, self._value_size)

        self._set_node_throttle(self.src_cluster, True, node_capacity=capacity)
        self._set_bucket_limits(self.src_cluster, bucket_a.name,
                                reserved=self.rl_reserved, hard_limit=MAX_U64)
        self._set_bucket_limits(self.src_cluster, bucket_b.name,
                                reserved=self.rl_reserved_b,
                                hard_limit=MAX_U64)

        before = self._snapshot([self.src_cluster])
        self.setup_xdcr()
        self._async_load(self.src_cluster, bucket_b, "app-b-", 0,
                         self.rl_items).result()
        self._wait_for_convergence("free pool isolation")
        delta = self._delta(before, self._snapshot([self.src_cluster]),
                            "free pool isolation")

        self._assert_throttled(delta, self.src_cluster, bucket_a.name,
                               unit="ru_total")
        self._assert_not_throttled(delta, self.src_cluster, bucket_b.name)
        self._assert_no_pipeline_restart("free pool isolation")
        self._lift_throttling_and_verify()

    def test_bidirectional_throttled(self):
        """Both clusters throttled, each taking client writes and the other's
        replication into the same bucket budget. Needs rdirection=bidirection"""
        for cluster in self.get_cb_clusters():
            self._throttle(cluster, hard_limit=self.rl_hard_limit)

        self.setup_xdcr()
        before = self._snapshot()
        tasks = []
        for cluster in self.get_cb_clusters():
            tasks.extend(cluster.async_load_all_buckets(
                self.rl_items, self._value_size))
        for task in tasks:
            task.result()
        self.async_perform_update_delete()
        self._wait_for_convergence("bidirectional, both throttled")
        delta = self._delta(before, self._snapshot(),
                            "bidirectional, both throttled")

        for cluster in self.get_cb_clusters():
            self._assert_throttled(delta, cluster, unit="wu_total")
        self._assert_no_pipeline_restart("bidirectional, both throttled")
        self._lift_throttling_and_verify()

    def test_pause_resume_throttled(self):
        """Pausing a replication into a throttled target stops it being
        throttled there, and resuming carries on from the checkpoint to
        convergence without a pipeline restart"""
        self._throttle(self.dest_cluster, hard_limit=self.rl_hard_limit)
        self.setup_xdcr()
        baseline = self._docs_written()
        tasks = self.src_cluster.async_load_all_buckets(
            self.rl_items, self._value_size)
        self._wait_for_docs_written(baseline, max(self.rl_items // 10, 1))

        self.src_cluster.pause_all_replications(verify=True)
        self._wait_for_quiet(self.dest_cluster)
        paused_before = self._snapshot([self.dest_cluster])
        self.sleep(self.pause_secs, "Replication paused")
        paused_delta = self._delta(paused_before,
                                   self._snapshot([self.dest_cluster]),
                                   "while paused")
        self._assert_not_throttled(paused_delta, self.dest_cluster)

        self.src_cluster.resume_all_replications(verify=True)
        for task in tasks:
            task.result()
        self._wait_for_convergence("resumed into a throttled target")
        self._assert_no_pipeline_restart("pause/resume throttled")
        self._lift_throttling_and_verify()

    def test_target_throttle_soak(self):
        """A target limit far below what XDCR offers, held for soak_secs: the
        timeout hazard raised on MB-70229. xmem keeps writes parked for many
        seconds, so it may resend and repair connections (reported), but
        every window must show progress and the pipeline must not restart.
        Lifting the limit then drains the rest"""
        self.src_cluster.load_all_buckets(self.rl_items, self._value_size)
        self._throttle(self.dest_cluster, hard_limit=self.rl_hard_limit)
        baseline = self._docs_written()
        self.setup_xdcr()
        # changes_left reads 0 until the pipeline has reported a backlog, so
        # the soak is timed from the first doc written, not from creation
        previous = self._wait_for_docs_written(baseline, 1)
        deadline = time.time() + self.soak_secs
        window = 0
        while time.time() < deadline:
            self.sleep(self.soak_window_secs, "Soaking under a throttled "
                       "target, window {0}".format(window))
            current = self._docs_written()
            written = current - previous
            changes_left = self._total_changes_left()
            self.log.info("Soak window {0}: {1} docs written ({2:.1f} docs/s),"
                          " changes_left={3}".format(
                              window, written,
                              written / float(self.soak_window_secs),
                              changes_left))
            self._goxdcr_signal_delta("soak window {0}".format(window))
            if not written and not changes_left:
                break
            self.assertGreater(
                written, 0,
                "Soak window {0}: no doc written in {1}s with {2} changes left "
                "under a hard limit of {3} units/s per node".format(
                    window, self.soak_window_secs, changes_left,
                    self.rl_hard_limit))
            previous = current
            window += 1
        self.assertGreater(
            window, 0,
            "The backlog drained before the first soak window ended: items "
            "and rl_hard_limit do not keep the target throttled")
        self.log.info("Soaked {0} window(s) of {1}s".format(
            window, self.soak_window_secs))
        self._assert_no_pipeline_restart("target throttle soak")

        for cluster in self.get_cb_clusters():
            self._reset_throttling(cluster)
        self._wait_for_convergence("soak, limit lifted")
        self.verify_results()
