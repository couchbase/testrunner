"""
FTS coverage for KV rate-limiting (8.5+, Totoro).

Per the decision on MB-70229 there are no blanket throttling exemptions: DCP
readers are throttled, and FTS builds its indexes by consuming DCP from KV.
This module checks whether an FTS index build is in fact subject to a bucket's
KV rate limit, and that it still produces a complete index when it is.

Shape of the test follows the rest of the throttling suites rather than the
"measure a baseline, then limit to a fraction of it" methodology in the
original ask. Everything else in both repos - TAF epengine/rate_limiting_tests,
testrunner fts|gsi/serverless/throttling, tuqquery/tuq_throttling - sets a
known limit from the conf first and then drives a workload past it, asserting
either that the throttle counters moved or that the work took materially
longer. That is the pattern here.

The baseline-derived version was tried first and could not work: on 8.5.0-1257
a complete 100k document index build moved ru_total not at all, in the same
window where the client load moved wu_total by exactly 100000. FTS's DCP reads
are not metered into the bucket's read units, so there is no footprint to
derive a limit from. Elapsed time needs no meters, hence the two runs below.

  reference   throttling off, build the index, record how long it took
  throttled   node_capacity + bucket limits from the conf, rebuild, and look
              for evidence the limit bit: throttle_count_total moving, or a
              build materially slower than the reference

If neither shows up, that is itself the result worth having - it means FTS's
DCP traffic is passing through a bucket limit untouched, which is what
MB-70229 says should not happen - and the failure says so explicitly rather
than reporting "FTS survived throttling".
"""

import time

from lib.metering_throttling import KVRateLimiting

from .fts_base import FTSBaseTest


class FTSThrottling(FTSBaseTest):

    def setUp(self):
        super(FTSThrottling, self).setUp()
        kv_nodes = self._cb_cluster.get_kv_nodes()
        if not kv_nodes:
            self.fail("No KV nodes in the cluster - a throttling test needs a "
                      "data service node to throttle")
        self.throttle = KVRateLimiting(kv_nodes, self.log)

        # Limits come from the conf, under the names the TAF suites use so
        # that conf rows port across unchanged
        self.node_capacity = self._input.param("node_capacity", 500)
        self.bucket_throttle_reserved = self._input.param(
            "bucket_throttle_reserved", 0)
        self.bucket_throttle_hard_limit = self._input.param(
            "bucket_throttle_hard_limit", 200)
        # The node level toggle is the documented master switch; the bucket
        # level one is only sent when a conf explicitly asks for it
        self.bucket_throttle_enabled = self._input.param(
            "bucket_throttle_enabled", None)

        # A throttled build is expected to be slower. This is how much slower
        # it has to be before slowness on its own counts as evidence the limit
        # was enforced
        self.throttle_slowdown_factor = self._input.param(
            "throttle_slowdown_factor", 1.5)
        # Throttled builds are legitimately slow, so the deadline scales.
        # Slow is expected; making no progress at all is the bug, and
        # index_stall_window catches that independently
        self.throttled_timeout_factor = self._input.param(
            "throttled_timeout_factor", 20)
        self.index_stall_window = self._input.param("index_stall_window", 300)
        self.reference_build_timeout = self._input.param(
            "reference_build_timeout", 1800)

        self._throttled_bucket_names = []
        # Snapshot so teardown can put the node settings back as they were
        # instead of leaving this cluster carrying our capacity
        try:
            self._original_node_settings = self.throttle.get_node_settings()
            self.log.info("Node throttle settings before this test: %s"
                          % self._original_node_settings)
        except Exception as err:
            self.log.warning("Could not read the node throttle settings: %s"
                             % err)
            self._original_node_settings = {}

    def tearDown(self):
        # Unconditional: the throttled run turns the master toggle on, so a
        # failure anywhere after that would otherwise leave this cluster
        # throttled for whatever suite runs on it next
        try:
            if getattr(self, "throttle", None) is not None:
                self.throttle.reset(
                    getattr(self, "_throttled_bucket_names", []),
                    restore=getattr(self, "_original_node_settings", None))
        except Exception as err:
            self.log.error("Throttle teardown failed: %s" % err)
        super(FTSThrottling, self).tearDown()

    # ------------------------------------------------------------------ #
    # the test
    # ------------------------------------------------------------------ #

    def test_fts_index_build_under_throttling(self):
        """Build the same index unthrottled, then under a KV rate limit"""
        buckets = self._cb_cluster.get_buckets()
        if not buckets:
            self.fail("No buckets to throttle")
        bucket_names = [bucket.name for bucket in buckets]

        self._validate_limits(bucket_names)

        # ---------------- reference: throttling off ---------------------
        self.log.info("=" * 70)
        self.log.info("RUN 1/2  reference - throttling off")
        self.log.info("=" * 70)
        self.throttle.set_node_settings(throttle_enabled=False)

        self.load_data(num_items=self._num_items)
        if self.bucket_storage != "magma":
            self.wait_till_items_in_bucket_equal(self._num_items)

        reference_elapsed, reference_deltas = self._build_run(
            "reference", bucket_names, self.reference_build_timeout)

        # ---------------- throttled -------------------------------------
        self.log.info("=" * 70)
        self.log.info("RUN 2/2  throttled - reserved=%s hard_limit=%s "
                      "node_capacity=%s"
                      % (self.bucket_throttle_reserved,
                         self.bucket_throttle_hard_limit, self.node_capacity))
        self.log.info("=" * 70)
        self._apply_limits(bucket_names)

        throttled_timeout = int(
            reference_elapsed * self.throttled_timeout_factor) + 600
        throttled_elapsed, throttled_deltas = self._build_run(
            "throttled", bucket_names, throttled_timeout)

        # ---------------- did the limit actually bite? ------------------
        throttled_count = self._total(throttled_deltas, "throttle_count")
        rejected = self._total(throttled_deltas, "reject_count")
        slowdown = (throttled_elapsed / reference_elapsed) \
            if reference_elapsed else 0

        self.log.info("=" * 70)
        self.log.info("FTS index build, unthrottled vs rate limited")
        self.log.info("  run         build time   throttled  rejected")
        self.log.info("  reference   %7.0fs    %9s  %8s"
                      % (reference_elapsed,
                         self._total(reference_deltas, "throttle_count"),
                         self._total(reference_deltas, "reject_count")))
        self.log.info("  throttled   %7.0fs    %9s  %8s"
                      % (throttled_elapsed, throttled_count, rejected))
        self.log.info("  slowdown: %.2fx (needs >= %.2fx to count on its own)"
                      % (slowdown, self.throttle_slowdown_factor))
        self.log.info("  ru/wu seen - reference %s, throttled %s"
                      % (reference_deltas, throttled_deltas))
        self.log.info("=" * 70)

        if throttled_count == 0 and slowdown < self.throttle_slowdown_factor:
            seen = dict((name, self.throttle.available_counters(name))
                        for name in bucket_names)
            self.fail(
                "The KV rate limit had no observable effect on the FTS index "
                "build. With throttleReserved=%s and throttleHardLimit=%s "
                "against node_capacity=%s, throttle_count_total did not move "
                "and the build took %.0fs against an unthrottled %.0fs "
                "(%.2fx, below the %.2fx threshold).\n"
                "FTS reads its source data over DCP, and MB-70229 closed "
                "'Won't Do' on exemptions precisely so that DCP readers are "
                "throttled - so either that is not happening on this build, "
                "or FTS's DCP traffic is not being accounted against this "
                "bucket's limit. This is a result about the server, not a "
                "flaky test: it must not be read as 'FTS survived "
                "throttling'.\n"
                "Throttle-related stats cbstats reports: %s"
                % (self.bucket_throttle_reserved,
                   self.bucket_throttle_hard_limit, self.node_capacity,
                   throttled_elapsed, reference_elapsed, slowdown,
                   self.throttle_slowdown_factor, seen))

        self.log.info("Rate limit enforced on the FTS build: "
                      "throttle_count_total +%s, %.2fx slower"
                      % (throttled_count, slowdown))

    # ------------------------------------------------------------------ #
    # run plumbing
    # ------------------------------------------------------------------ #

    def _validate_limits(self, bucket_names):
        """Reject a conf whose limits the cluster would refuse outright."""
        if self.bucket_throttle_reserved > self.bucket_throttle_hard_limit:
            self.fail(
                "bucket_throttle_reserved (%s) must be <= "
                "bucket_throttle_hard_limit (%s)"
                % (self.bucket_throttle_reserved,
                   self.bucket_throttle_hard_limit))
        required = self.bucket_throttle_reserved * len(bucket_names)
        if required > self.node_capacity:
            self.fail(
                "The conf reserves %s units across %s bucket(s) but "
                "node_capacity is only %s. The cluster rejects a bucket whose "
                "reservations oversubscribe the node, so raise node_capacity "
                "or lower bucket_throttle_reserved."
                % (required, len(bucket_names), self.node_capacity))

    def _apply_limits(self, bucket_names):
        """Turn the feature on, then put the conf's limits on each bucket."""
        self.throttle.set_node_settings(throttle_enabled=True,
                                        node_capacity=self.node_capacity)
        for name in bucket_names:
            self.throttle.set_bucket_limits(
                name,
                throttle_reserved=self.bucket_throttle_reserved,
                throttle_hard_limit=self.bucket_throttle_hard_limit,
                throttle_enabled=self.bucket_throttle_enabled)
            if name not in self._throttled_bucket_names:
                self._throttled_bucket_names.append(name)
            self.log.info("Bucket %s now reports %s"
                          % (name, self.throttle.get_bucket_limits(name)))

    def _build_run(self, label, bucket_names, timeout):
        """Drop any existing indexes, build them again, and time it.

        Indexes are dropped and recreated per run so that each build is a
        fresh DCP backfill of the whole bucket - an index left in place would
        have nothing to stream and the run would measure nothing.
        """
        if self._cb_cluster.get_indexes():
            self.log.info("Dropping indexes before the %s run" % label)
            self._cb_cluster.delete_all_fts_indexes()
            self.sleep(10, "Letting the index drops settle")

        before = dict((name, self.throttle.get_counters(name))
                      for name in bucket_names)
        started = time.time()

        self.create_fts_indexes_all_buckets(
            plan_params=self.construct_plan_params())
        self._wait_for_index_build(label, timeout)

        elapsed = time.time() - started
        after = dict((name, self.throttle.get_counters(name))
                     for name in bucket_names)
        deltas = dict((name,
                       self.throttle.counters_delta(before[name], after[name]))
                      for name in bucket_names)

        # Completeness, not just termination: a throttled build that quietly
        # drops mutations is the failure mode worth catching here
        self.validate_index_count(equal_bucket_doc_count=True,
                                  zero_rows_ok=False)
        self.log.info("%s run built in %.0fs; counters %s"
                      % (label, elapsed, deltas))
        return elapsed, deltas

    def _wait_for_index_build(self, label, timeout):
        """Wait for every index to reach its source doc count.

        Deliberately not wait_for_indexing_complete(): that has a single flat
        deadline, and under a rate limit slow is the expected outcome while
        stalled is the bug. These are separated here - a build that keeps
        making progress is given the whole (scaled) deadline, and one that
        stops making progress fails as soon as index_stall_window has passed.
        """
        indexes = [index for index in self._cb_cluster.get_indexes()
                   if index.index_type != "fulltext-alias"]
        if not indexes:
            self.fail("No FTS indexes to wait on in the %s run" % label)

        counts = dict((index.name, -1) for index in indexes)
        targets = dict((index.name, 0) for index in indexes)
        last_progress = dict((index.name, time.time()) for index in indexes)
        deadline = time.time() + timeout
        started = time.time()

        while True:
            pending = []
            now = time.time()
            for index in indexes:
                # Target is re-read every poll rather than pinned up front:
                # the magma path skips the wait for the bucket count to
                # settle, so a target captured early could be short of the
                # final doc count and let a build look complete when it is not
                targets[index.name] = self._expected_doc_count(index)
                indexed = index.get_indexed_doc_count()
                if indexed > counts[index.name]:
                    counts[index.name] = indexed
                    last_progress[index.name] = now
                if indexed < targets[index.name]:
                    pending.append(index)
                    stalled_for = now - last_progress[index.name]
                    if stalled_for > self.index_stall_window:
                        self.fail(
                            "FTS index '%s' stalled during the %s run: %s of "
                            "%s docs indexed and no progress for %.0fs. Under "
                            "a rate limit a build is expected to be slow, not "
                            "to stop - this looks like a stalled DCP stream "
                            "rather than back-pressure."
                            % (index.name, label, counts[index.name],
                               targets[index.name], stalled_for))
            if not pending:
                self.log.info("%s run: all %s index(es) caught up in %.0fs"
                              % (label, len(indexes), time.time() - started))
                return
            if time.time() > deadline:
                elapsed = time.time() - started
                self.fail(
                    "Timed out after %.0fs waiting for the %s run to index: "
                    "%s. The builds were still progressing, so this is a "
                    "throughput result rather than a hang - raise the "
                    "deadline (throttled_timeout_factor) if the limit is "
                    "legitimately this tight."
                    % (elapsed, label,
                       ", ".join("'%s' at %s/%s" % (index.name,
                                                    counts[index.name],
                                                    targets[index.name])
                                 for index in pending)))
            self.sleep(15, "%s run: %s index(es) still catching up"
                       % (label, len(pending)))

    def _expected_doc_count(self, index):
        """Docs the index is expected to end up with.

        Mirrors validate_index_count(): a bucket level index with no custom
        mapping only covers _default._default, so the bucket wide key count
        would be the wrong target.
        """
        if index.collections:
            return index.get_src_collections_doc_count()
        return self._cb_cluster.get_doc_count_in_collections(
            index.source_bucket, "_default", ["_default"])

    @staticmethod
    def _total(deltas, key):
        return sum(delta[key] for delta in deltas.values())
