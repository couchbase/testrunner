from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from ..xdcrnewbasetests import REPL_PARAM, NodeHelper

import logger

log = logger.Logger.get_logger()


class ReplicationManager:
    """Create/start/pause/resume replications, tune params, drive load.

    CNG tests bypass setup_xdcr() and inherit the Couchbase Server default
    checkpoint_interval (1800s). That is too long for any test phase to
    see a checkpoint fire, so CNG replications are created with a shorter
    default and assertions poll for increments. apply_ckpt_default is the
    only CNG-specific behaviour on this manager — the rest are wrappers
    that delegate to base helpers.

    Callbacks injected through the constructor:
      get_total_changes_left_fn:      XDCRNewBaseTest.get_total_changes_left
      wait_for_backlog_fn:            XDCRNewBaseTest.wait_for_backlog_to_drain
      bucket_item_count_fn:           XDCRNewBaseTest.bucket_item_count
    """

    DEFAULT_CHECKPOINT_INTERVAL_SECS = 60

    def __init__(self, find_matching_bucket, sleep_fn, value_size, num_items,
                 get_total_changes_left_fn=None,
                 wait_for_backlog_fn=None,
                 bucket_item_count_fn=None):
        self._find_matching_bucket = find_matching_bucket
        self._sleep = sleep_fn
        self._value_size = value_size
        self._num_items = num_items
        self._get_total_changes_left = get_total_changes_left_fn
        self._wait_for_backlog = wait_for_backlog_fn
        self._bucket_item_count = bucket_item_count_fn

    def apply_ckpt_default(self, xdcr_params):
        """Merge the CNG default checkpoint interval unless caller supplied one."""
        params = dict(xdcr_params or {})
        params.setdefault(REPL_PARAM.CHECKPOINT_INTERVAL,
                          self.DEFAULT_CHECKPOINT_INTERVAL_SECS)
        return params

    def start_for_buckets(self, src_cluster, dest_cluster, rc_name,
                          xdcr_params=None):
        """Start replications for every matching bucket pair."""
        rest = RestConnection(src_cluster.get_master_node())
        src_buckets = src_cluster.get_buckets()
        dest_buckets = dest_cluster.get_buckets()
        xdcr_params = self.apply_ckpt_default(xdcr_params)
        rep_ids = []
        for src_bucket in src_buckets:
            dest_bucket = self._find_matching_bucket(src_bucket, dest_buckets)
            rep_id = rest.start_replication(
                "continuous", src_bucket.name, rc_name,
                rep_type="xmem", toBucket=dest_bucket.name,
                xdcr_params=xdcr_params)
            rep_ids.append(rep_id)
            log.info("Replication started: {0}/{1} -> {2}/{3}, id={4}".format(
                src_cluster.get_name(), src_bucket.name,
                rc_name, dest_bucket.name, rep_id))
        return rep_ids

    def create_single(self, src_cluster, remote_cluster_name,
                      src_bucket, dest_bucket=None, xdcr_params=None):
        """Create and start a single replication via REST API."""
        rest = RestConnection(src_cluster.get_master_node())
        to_bucket = dest_bucket or src_bucket
        rep_id = rest.start_replication(
            "continuous", src_bucket.name, remote_cluster_name,
            rep_type="xmem", toBucket=to_bucket.name,
            xdcr_params=self.apply_ckpt_default(xdcr_params))
        log.info("Replication started: {0} -> {1}, id={2}".format(
            src_bucket.name, to_bucket.name, rep_id))
        return rep_id

    def set_param(self, rest, src_bucket, dest_bucket, param, value):
        rest.set_xdcr_params(src_bucket, dest_bucket, {param: value})

    def get_param(self, rest, src_bucket, dest_bucket, param):
        return rest.get_xdcr_param(src_bucket, dest_bucket, param)

    def pause(self, rest, src_bucket, dest_bucket):
        self.set_param(rest, src_bucket, dest_bucket,
                       REPL_PARAM.PAUSE_REQUESTED, "true")

    def resume(self, rest, src_bucket, dest_bucket):
        self.set_param(rest, src_bucket, dest_bucket,
                       REPL_PARAM.PAUSE_REQUESTED, "false")

    def pause_resume_cycle(self, rest, src_bucket, dest_bucket,
                           pause_duration=5, reason=""):
        """Pause then resume; useful for interleaved stress."""
        self.pause(rest, src_bucket, dest_bucket)
        self._sleep(pause_duration, "Paused: {0}".format(reason))
        self.resume(rest, src_bucket, dest_bucket)

    def load_async(self, cluster, count=None):
        """Async document load on all buckets; returns task list."""
        return cluster.async_load_all_buckets(
            count or self._num_items, self._value_size)

    def load_into_source_clusters(self, edges, items, prefix):
        """Load docs into every unique source cluster in an edge list."""
        seen = set()
        for src, _, _ in edges:
            if src.get_name() in seen:
                continue
            seen.add(src.get_name())
            gen = BlobGenerator("{0}-{1}-".format(prefix, src.get_name()),
                                "{0}-{1}-".format(prefix, src.get_name()),
                                self._value_size, end=items)
            src.load_all_buckets_from_generator(gen)

    def get_changes_left_total(self, src_cluster):
        """Sum replication_changes_left across all source buckets."""
        return self._get_total_changes_left(src_cluster)

    def wait_for_backlog_to_drain(self, src_cluster, timeout, poll_interval=60):
        """Poll replication_changes_left until 0 or timeout. Returns final backlog."""
        return self._wait_for_backlog(src_cluster, timeout, poll_interval)

    def start_burst_loaders(self, src_master, src_buckets, items_per_worker,
                            docsize, rate_limit, workers_per_bucket):
        """Launch pillowfight on every source bucket in parallel."""
        return NodeHelper.start_pillowfight_burst_loaders(
            src_master, src_buckets, items_per_worker,
            docsize, rate_limit, workers_per_bucket)

    def wait_for_burst_completion(self, loaders, timeout):
        """Wait for pillowfight workers; force-kills after timeout."""
        return NodeHelper.wait_for_pillowfight_completion(loaders, timeout)

    def sample_under_pressure(self, src_cluster, rest_src, rc_name,
                              get_connectivity_status, ok_status,
                              duration, interval=5):
        """Sample backlog and connectivity for `duration` seconds.
        Returns dict(peak_backlog, samples, connectivity_drops)."""
        import time as _time
        peak = 0
        samples = 0
        drops = 0
        end_time = _time.time() + duration
        while _time.time() < end_time:
            backlog = self.get_changes_left_total(src_cluster)
            status = get_connectivity_status(rest_src, rc_name=rc_name)
            if backlog > peak:
                peak = backlog
            if status != ok_status:
                drops += 1
                log.warning("Connectivity dropped to '{0}' at backlog={1}".format(
                    status, backlog))
            else:
                log.info("Burst sample: backlog={0}, status={1}".format(
                    backlog, status))
            samples += 1
            _time.sleep(interval)
        return {"peak_backlog": peak, "samples": samples,
                "connectivity_drops": drops}

    def bucket_item_count(self, cluster, bucket_name):
        """Current bucket item count via REST stats."""
        return self._bucket_item_count(cluster, bucket_name)

    def wait_for_edges_source_drained(self, edges, timeout=600, interval=20):
        """For each unique source in the edge list, poll until every bucket's
        replication_changes_left is zero. Raises on timeout."""
        import time as _time
        sources = {e[0].get_name(): e[0] for e in edges}
        end_time = _time.time() + timeout
        while _time.time() < end_time:
            all_drained = True
            for src in sources.values():
                backlog = self.get_changes_left_total(src)
                if backlog > 0:
                    all_drained = False
                    log.info("{0} changes_left={1}, waiting...".format(
                        src.get_name(), backlog))
                    break
            if all_drained:
                log.info("All source backlogs drained to 0")
                return
            _time.sleep(interval)
        raise Exception(
            "Timeout: sources did not drain in {0}s (per-source backlog: "
            "{1})".format(timeout,
                          {n: self.get_changes_left_total(s)
                           for n, s in sources.items()}))

    def verify_edges_converged(self, edges, expected_per_source, timeout=600):
        """Wait for every edge's destination to reach the source's count."""
        import time as _time
        self.wait_for_edges_source_drained(edges, timeout=timeout)
        end_time = _time.time() + timeout
        mismatches = []
        while _time.time() < end_time:
            mismatches = []
            for src, dst, _ in edges:
                for bucket in src.get_buckets():
                    db = self._find_matching_bucket(bucket, dst.get_buckets())
                    dest_count = self.bucket_item_count(dst, db.name)
                    if expected_per_source is not None:
                        target = expected_per_source
                    else:
                        target = self.bucket_item_count(src, bucket.name)
                    if dest_count < target:
                        mismatches.append(
                            "{0}/{1}: dest={2} < target={3}".format(
                                dst.get_name(), db.name, dest_count, target))
            if not mismatches:
                log.info("All edges converged")
                return
            log.info("Pending convergence: {0}".format(mismatches))
            _time.sleep(20)
        raise Exception("Edges did not converge: {0}".format(mismatches))
