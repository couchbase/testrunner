import json
from time import sleep
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.couchbase_helper.documentgenerator import BlobGenerator
from lib.membase.api.rest_client import RestConnection
from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest
import re
import subprocess
import multiprocessing
import os
import time

from pytests.xdcr.tenK_collection_helper import TenKCollectionHelper

class StatsXDCR(XDCRNewBaseTest):
    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.initial_load = self._input.param("inital_load", False)
        self.verify_backfill = self._input.param("verify_backfill", False)
        self.pipeline_type = self._input.param("pipeline_type", "Main")
        if self.pipeline_type not in ("Main", "Backfill"):
            self.fail("pipeline_type must be 'Main' or 'Backfill', got '{0}'".format(
                self.pipeline_type))

    def load_binary_docs_using_cbc_pillowfight(self, server, items, bucket, batch=1000, docsize=100, rate_limit=100000, scope="_default", collection="_default", command_timeout = 10):
        server_shell = RemoteMachineShellConnection(server)
        cmd = f"/opt/couchbase/bin/cbc-pillowfight -u Administrator -P password -U couchbase://localhost/"\
            f"{bucket} -I {items} -m {docsize} -M {docsize} -B {batch} --rate-limit={rate_limit} --populate-only --collection {scope}.{collection}"
        self.log.info("Executing '{0}'...".format(cmd))
        output, error  = server_shell.execute_command(cmd, timeout=command_timeout, use_channel=True)
        if error :
            self.log.info("Error: {0}".format(error))
            self.fail(f"Failed to load docs in source cluster in {bucket}.{scope}.{collection}")
        server_shell.disconnect()
        self.log.info(f"Data loaded into {bucket}.{scope}.{collection} successfully")

    def _extract_stats(self, data, stat):
        """
        Extracts specified stats from Prometheus metrics data.
        Returns:
            list: A list of dictionaries, where each dictionary represents a metric
                  and contains 'labels' (dictionary) and 'value' (float or int).
        """
        result = []
        lines = data.strip().split('\n')
        for line in lines:
            if line.startswith(stat):
                match = re.match(r'([a-zA-Z0-9_]+)\s*\{(.*?)\}\s*(-?\d+(\.\d+)?([eE][-+]?\d+)?)', line)
                if not match:
                    match = re.match(r'([a-zA-Z0-9_]+)\s+(-?\d+(\.\d+)?([eE][-+]?\d+)?)', line)
                    if not match:
                        continue  # skip if no match.

                if match.group(2):  # labels exist
                    labels_str = match.group(2)
                    value = float(match.group(3))
                    labels = {}
                    label_pairs = re.findall(r'([a-zA-Z0-9_]+)="([^"]+)"', labels_str)
                    for key, val in label_pairs:
                        labels[key] = val
                    result.append({"labels": labels, "value": value})
                else:  # no labels
                    value = float(match.group(2))
                    result.append({"labels": {}, "value": value})
        return result


    def _sample_pipeline_stat(self, server, stat_name):
        """Read one sample of `stat_name` and return {pipelineType: value}."""
        sample = {}
        for d in self.get_stats(server, stat_name):
            pt = d['labels'].get('pipelineType', '_')
            sample[pt] = d['value']
        return sample

    def _poll_until_peak_above(self, server, stat_name, pipeline_type,
                               threshold=0.0, timeout=120, interval=2):
        """Poll `stat_name`, tracking per-pipelineType peaks, until the peak
        for `pipeline_type` exceeds `threshold` or `timeout` elapses.
        Returns the peaks dict {pipelineType: max value seen}.
        """
        deadline = time.time() + timeout
        peaks = {}
        while time.time() < deadline:
            sample = self._sample_pipeline_stat(server, stat_name)
            for pt, v in sample.items():
                peaks[pt] = max(peaks.get(pt, 0.0), v)
            self.log.info("polling {0} (peak-above): cur={1} peaks={2}".format(
                stat_name, sample, peaks))
            if peaks.get(pipeline_type, 0.0) > threshold:
                return peaks
            self.wait_interval(interval, "polling " + stat_name)
        return peaks

    def _poll_until_below(self, server, stat_name, pipeline_types,
                          threshold, timeout=120, interval=2):
        """Poll `stat_name` until the latest value for every pipelineType in
        `pipeline_types` is strictly less than `threshold`, or `timeout`
        elapses. Returns the last sample dict {pipelineType: value}.
        """
        deadline = time.time() + timeout
        last = {}
        while time.time() < deadline:
            last = self._sample_pipeline_stat(server, stat_name)
            self.log.info("polling {0} (below {1}): cur={2}".format(
                stat_name, threshold, last))
            if all(last.get(pt, 0.0) < threshold for pt in pipeline_types):
                return last
            self.wait_interval(interval, "polling " + stat_name)
        return last

    def get_stats(self,server, stat_name):
        server_rest = RestConnection(server)
        api = server_rest.baseUrl + "metrics"
        status, content, _ = server_rest._http_request(api=api, method="GET", timeout=30)
        if not status:
            self.fail(f"Could not retrieve stats from {api}")
        try:
            content = content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                content = content.decode('latin-1')
            except UnicodeDecodeError:
                    self.fail(f"Exception  while decoding response from {api}")
        extracted = self._extract_stats(content, stat_name)
        return extracted

    def test_xdcr_data_replicated_uncompress(self):
        if self.initial_load:
            self.load_and_setup_xdcr()
        else:
            self.setup_xdcr_and_load()
        # verify replication and stats
        src_rest = RestConnection(self.src_master)
        outgoing_repls = self.get_outgoing_replications(src_rest)
        if outgoing_repls is None:
            self.fail("No outgoing replication found on src cluster")
        doc_gen = BlobGenerator("docs", "seed", 512, end=1000)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=doc_gen)
        self.wait_interval(10, "Wait for replication stats to be updated")
        stats = self.get_stats(self.src_master, "xdcr_data_replicated_uncompress_bytes")
        metrics = {}
        for data in stats:
            pipelineType = data['labels']['pipelineType']
            value = data['value']
            self.log.info(f"Pipeline type: {pipelineType}, Value:{value}")
            metrics[pipelineType] = value
        self.assertTrue(metrics['Main']>0.0, "Expected metrics value did not match")

        # pause the replication
        self.src_cluster.pause_all_replications()
        self.wait_interval(20, "Wait for replication stats to be updated")
        # verify it went to zero
        stats = self.get_stats(self.src_master, "xdcr_data_replicated_uncompress_bytes")

        for data in stats:
            pipelineType = data['labels']['pipelineType']
            value = data['value']
            self.log.info(f"Pipeline type: {pipelineType}, Value:{value}")
            metrics[pipelineType] = value
        self.assertTrue(metrics['Main']==0.0, "Expected metrics value did not match")

    def test_xdcr_data_replicated_uncompress_backfill(self):
        new_scope = "new_scope"
        new_collection = "new_collection"

        self.setup_xdcr()

        src_rest = RestConnection(self.src_master)
        dest_rest = RestConnection(self.dest_master)

        status = src_rest.create_scope("default",new_scope)
        if not status:
            self.fail("Failed to create scope on src")
        status = src_rest.create_collection("default", new_scope, new_collection)
        if not status:
            self.fail("Failed to create collection on src")
        self.load_binary_docs_using_cbc_pillowfight(self.src_master, 100000, "default", 100, 300)
        self.load_binary_docs_using_cbc_pillowfight(self.src_master, 200000, "default", 100, 300, scope=new_scope, collection=new_collection)

        self.wait_interval(10, "Waiting for docs to be inserted")

        # verify replication and stats
        outgoing_repls = self.get_outgoing_replications(src_rest)
        if outgoing_repls is None:
            self.fail("No outgoing replication found on src cluster")

        status = dest_rest.create_scope("default",new_scope)
        if not status:
            self.fail("Failed to create scope on dest")
        status = dest_rest.create_collection("default", new_scope, new_collection)
        if not status:
            self.fail("Failed to create collection on dest")
        self.load_binary_docs_using_cbc_pillowfight(self.src_master, 10000, "default", 100, 300)
        self.load_binary_docs_using_cbc_pillowfight(self.dest_master, 1000, "default", 100, 300, scope=new_scope, collection=new_collection)
        self.load_binary_docs_using_cbc_pillowfight(self.src_master, 1000, "default", 100, 300, scope=new_scope, collection=new_collection)

        # xdcr_data_replicated_uncompress_bytes is a rate-like metric: it
        # reads > 0 only while bytes are actively flowing, then drops back to
        # 0 once the pipeline drains. Poll for the peak rather than a single
        # snapshot — the Backfill window can close in seconds.
        stat_name = "xdcr_data_replicated_uncompress_bytes"
        peaks = self._poll_until_peak_above(
            self.src_master, stat_name, pipeline_type="Backfill",
            threshold=0.0, timeout=180, interval=2)
        self.assertTrue(peaks.get('Backfill', 0.0) > 0.0,
            "Expected metrics value did not match for backfill pipeline "
            "(peak Backfill never exceeded 0 over 180s window)")

        # verify it went to zero
        # pause_all_replications returns when pauseRequested is set, but the
        # pipeline tear-down (and the rate dropping to 0) happens asynchronously.
        self.src_cluster.pause_all_replications()
        zero_threshold = 1.0
        last = self._poll_until_below(
            self.src_master, stat_name,
            pipeline_types=("Main", "Backfill"),
            threshold=zero_threshold, timeout=120, interval=5)
        self.assertLess(last.get('Main', 0.0), zero_threshold,
            "{0} Main did not drop to ~0 after pause: last={1}".format(
                stat_name, last.get('Main', 0.0)))
        self.assertLess(last.get('Backfill', 0.0), zero_threshold,
            "{0} Backfill did not drop to ~0 after pause: last={1}".format(
                stat_name, last.get('Backfill', 0.0)))

    # ---- MB-69383: stats for non-replicated data sizes ----

    def _get_pipeline_value(self, stats, pipeline_type="Main"):
        for s in stats:
            if s["labels"].get("pipelineType") == pipeline_type:
                return s["value"]
        return None

    def _setup_for_pipeline(self):
        """Set up XDCR so that the requested pipelineType emits stats.

        Main: setup_xdcr, force optimisticReplicationThreshold=0, then load.
        Backfill: setup_xdcr, force optimisticReplicationThreshold=0,
                  create+load a new collection on src, then create the
                  matching collection on dest so a Backfill pipeline spins up.
        """
        self.setup_xdcr()
        src_rest = RestConnection(self.src_master)
        # Force every doc through source-side CR (GET_META / SubdocGet) so
        # xdcr_metadata_transferred_bytes is exercised. Must be set BEFORE
        # any docs replicate and BEFORE dest collections are created.
        src_rest.set_xdcr_param("default", "default",
                                "optimisticReplicationThreshold", 0)

        if self.pipeline_type == "Main":
            self.load_data_topology()
            return

        new_scope, new_collection = "new_scope", "new_collection"
        dest_rest = RestConnection(self.dest_master)

        if not src_rest.create_scope("default", new_scope):
            self.fail("Failed to create scope on src")
        if not src_rest.create_collection("default", new_scope, new_collection):
            self.fail("Failed to create collection on src")
        # Pre-existing Backfill volume — once the dest collection is created
        # below, the Backfill pipeline replicates these docs. Volume must be
        # large enough that the pipeline stays alive across each test's
        # warm-up + pause/resume + post-resume windows; otherwise the
        # pipeline tears down and Backfill-labelled series stop emitting.
        self.load_binary_docs_using_cbc_pillowfight(
            self.src_master, 200000, "default", 100, 300,
            scope=new_scope, collection=new_collection)
        self.wait_interval(10, "let docs settle on src before backfill trigger")

        if not dest_rest.create_scope("default", new_scope):
            self.fail("Failed to create scope on dest")
        if not dest_rest.create_collection("default", new_scope, new_collection):
            self.fail("Failed to create collection on dest")

    def test_xdcr_metadata_transferred_bytes(self):
        """[MB-69383] xdcr_metadata_transferred_bytes > 0 for the pipelineType
        under test (Main by default, Backfill if pipeline_type=Backfill).

        Trigger: GET_META / SubdocGet from XMEM nozzle during source-side
        conflict resolution. optimisticReplicationThreshold=0 is applied in
        _setup_for_pipeline before any docs replicate.
        """
        self._setup_for_pipeline()
        doc_gen = BlobGenerator("metadocs", "seed", 512, end=2000)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=doc_gen)
        self.dest_cluster.load_all_buckets_from_generator(kv_gen=doc_gen)
        self.wait_interval(30, "wait for xdcr_metadata_transferred_bytes to update")
        stats = self.get_stats(self.src_master, "xdcr_metadata_transferred_bytes")
        self.log.info("xdcr_metadata_transferred_bytes samples: {0}".format(stats))
        value = self._get_pipeline_value(stats, self.pipeline_type)
        self.assertIsNotNone(value,
            "no pipelineType={0} series for xdcr_metadata_transferred_bytes".format(
                self.pipeline_type))
        self.assertGreater(value, 0.0,
            "xdcr_metadata_transferred_bytes {0} expected > 0, got {1}".format(
                self.pipeline_type, value))

    def test_xdcr_sys_metadata_transferred_bytes(self):
        """[MB-69383] xdcr_sys_metadata_transferred_bytes > 0 for the pipelineType
        under test (Main by default, Backfill if pipeline_type=Backfill).

        Trigger: collection-manifest fetch + checkpoint-mgr failoverlog
        reads from target. Wait at least one checkpoint_interval so
        the ckptmgr has fetched failoverlogs at least once.
        """
        self._setup_for_pipeline()
        ckpt_interval = int(self._input.param("checkpoint_interval", 60))
        self.wait_interval(ckpt_interval + 10,
                           "wait one checkpoint_interval for sys-metadata fetch")
        stats = self.get_stats(self.src_master, "xdcr_sys_metadata_transferred_bytes")
        self.log.info("xdcr_sys_metadata_transferred_bytes samples: {0}".format(stats))
        value = self._get_pipeline_value(stats, self.pipeline_type)
        self.assertIsNotNone(value,
            "no pipelineType={0} series for xdcr_sys_metadata_transferred_bytes".format(
                self.pipeline_type))
        self.assertGreater(value, 0.0,
            "xdcr_sys_metadata_transferred_bytes {0} expected > 0, got {1}".format(
                self.pipeline_type, value))

    def test_metadata_bytes_reset_on_pause_resume(self):
        """[MB-69383] xdcr_sys_metadata_transferred_bytes resets on resume.

        Per etc/metrics_metadata.json on this stat (verbatim):
            'The metric will be re-initialized to 0 when a paused pipeline
             is resumed. The metadata checkpoint from the act of pausing a
             pipeline will not be accounted.'

        Note: this reset behaviour is documented ONLY for
        xdcr_sys_metadata_transferred_bytes. xdcr_metadata_transferred_bytes
        makes no such promise, so it is intentionally NOT exercised here.
        """
        stat = "xdcr_sys_metadata_transferred_bytes"
        ckpt_interval = int(self._input.param("checkpoint_interval", 60))

        self._setup_for_pipeline()
        doc_gen = BlobGenerator("resetmd", "seed", 512, end=1000)
        self.src_cluster.load_all_buckets_from_generator(kv_gen=doc_gen)
        self.dest_cluster.load_all_buckets_from_generator(kv_gen=doc_gen)
        self.wait_interval(ckpt_interval + 10,
                           "wait one checkpoint interval to drive sys-metadata")

        pre_pause = self._get_pipeline_value(
            self.get_stats(self.src_master, stat), self.pipeline_type) or 0.0
        self.log.info("pre-pause {0} {1} = {2}".format(
            stat, self.pipeline_type, pre_pause))
        self.assertGreater(pre_pause, 0.0,
            "pre-pause {0} {1} expected > 0 (warm-up too short?)".format(
                stat, self.pipeline_type))

        self.src_cluster.pause_all_replications()
        self.wait_interval(20, "settle while paused")
        self.src_cluster.resume_all_replications()

        reset_threshold = max(pre_pause * 0.1, 1.0)
        last = self._poll_until_below(
            self.src_master, stat,
            pipeline_types=(self.pipeline_type,),
            threshold=reset_threshold,
            timeout=ckpt_interval * 2, interval=3)
        post_resume_zero = last.get(self.pipeline_type, 0.0)
        self.assertLess(post_resume_zero, reset_threshold,
            "{0} {1} did not reset on resume within poll window: "
            "pre={2} last-sample={3}".format(
                stat, self.pipeline_type, pre_pause, post_resume_zero))

        # Drive fresh load + one ckpt interval to confirm the counter resumes
        # counting from zero (still strictly less than the pre-pause peak).
        self.src_cluster.load_all_buckets_from_generator(
            kv_gen=BlobGenerator("postresume", "seed", 512, end=200))
        self.wait_interval(ckpt_interval + 10,
                           "post-resume drive + checkpoint")

        post = self._get_pipeline_value(
            self.get_stats(self.src_master, stat), self.pipeline_type) or 0.0
        self.log.info("post-resume {0} {1} = {2}".format(
            stat, self.pipeline_type, post))
        self.assertLess(post, pre_pause,
            "{0} {1} did not re-initialize on resume: pre={2} post={3}".format(
                stat, self.pipeline_type, pre_pause, post))

    def test_source_cluster_heartbeat_recv_bytes(self):
        """[MB-69383] target node sees > 0 heartbeat bytes from source.

        Trigger: P2P heartbeat from a source-cluster node to a target-cluster
        node. The stat lives on the TARGET, labelled by sourceClusterUUID.
        Default heartbeat interval is ~60s.
        """
        self.setup_xdcr()
        self.wait_interval(120, "wait for at least one heartbeat round")
        stats = self.get_stats(self.dest_master,
                               "xdcr_source_cluster_heartbeat_recv_bytes")
        self.log.info("xdcr_source_cluster_heartbeat_recv_bytes samples: {0}".format(stats))
        self.assertTrue(any(s["value"] > 0.0 for s in stats),
            "no xdcr_source_cluster_heartbeat_recv_bytes > 0 on dest_master")

    def test_remote_cluster_monitoring_metadata_transferred_bytes(self):
        """[MB-69383] counter increments without an active replication.

        Trigger: the source's remote-cluster-reference refresh cycle.
        Only labelled by targetClusterUUID, so this is per remote-ref,
        not per replication.
        """
        self.setup_xdcr()
        self.wait_interval(60, "first refresh cycle")
        s1 = self.get_stats(self.src_master,
            "xdcr_remote_cluster_monitoring_metadata_transferred_bytes")
        v1 = max((s["value"] for s in s1), default=0.0)
        self.log.info("monitoring_metadata sample 1: {0} (max={1})".format(s1, v1))
        self.assertGreater(v1, 0.0,
            "xdcr_remote_cluster_monitoring_metadata_transferred_bytes expected > 0")

        self.wait_interval(60, "second refresh cycle")
        s2 = self.get_stats(self.src_master,
            "xdcr_remote_cluster_monitoring_metadata_transferred_bytes")
        v2 = max((s["value"] for s in s2), default=0.0)
        self.log.info("monitoring_metadata sample 2: {0} (max={1})".format(s2, v2))
        self.assertGreater(v2, v1,
            "monitoring_metadata counter did not increase: {0} -> {1}".format(v1, v2))

    # ---- 10K Collections Scale Tests ----
    def test_xdcr_stats_latency_10k_collections(self):
        """
        Verify XDCR stats REST API remains responsive with 10K collections.
        Queries bucket stats endpoint and measures response time to detect
        stats lag that could cause UI timeouts.

        Conf params:
            stats_timeout: max acceptable response time in seconds (default 30)
        """
        p = TenKCollectionHelper.read_10k_params(self._input)
        bucket_name = self._input.param("bucket_name", "default")
        stats_timeout = self._input.param("stats_timeout", 30)

        TenKCollectionHelper.create_10k_collections(
            self.src_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})
        TenKCollectionHelper.create_10k_collections(
            self.dest_master, bucket_name, **{k: p[k] for k in
            ("num_scopes", "collections_per_scope", "scope_prefix", "collection_prefix")})

        self.setup_xdcr()

        TenKCollectionHelper.select_and_load(
            self.src_master, bucket_name, p, run_id="stats10k")

        rest = RestConnection(self.src_master)
        start_time = time.time()
        try:
            stats = rest.fetch_bucket_xdcr_stats(bucket_name)
        except Exception:
            stats = rest.get_bucket_stats(bucket_name)
        elapsed = time.time() - start_time
        self.log.info("Stats query took {:.2f}s (threshold: {}s)".format(
            elapsed, stats_timeout))
        self.assertLess(elapsed, stats_timeout,
                        "Stats query took {:.2f}s, exceeds {}s threshold".format(
                            elapsed, stats_timeout))

        try:
            self._wait_for_replication_to_catchup(
                timeout=self._input.param("wait_timeout", 900))
        except Exception as e:
            self.fail("Stats latency 10K catch-up failed: {}".format(e))

        start_time = time.time()
        try:
            stats = rest.fetch_bucket_xdcr_stats(bucket_name)
        except Exception:
            stats = rest.get_bucket_stats(bucket_name)
        elapsed = time.time() - start_time
        self.log.info("Post-replication stats query took {:.2f}s".format(elapsed))
        self.assertLess(elapsed, stats_timeout,
                        "Post-replication stats query too slow: {:.2f}s".format(elapsed))
        self.log.info("Stats latency 10K test passed")