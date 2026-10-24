"""XDCR checkpoint resume in legacy pre-replicate mode (MB-71545).

When a pipeline starts, the checkpoint manager has to confirm that the target
still holds what each checkpoint says it does -- the target vbuuid and seqno
recorded in it -- before resuming from it. That check is pre-replicate. A
checkpoint that fails it is skipped, and the vbucket falls back to an older
checkpoint or to seqno 0.

- From 8.1 (MB-68368) the check is local by default: the checkpoint manager
  fetches the target bucket's failover log over memcached once and validates
  every checkpoint against it (goxdcr pipeline_svc/checkpointRemoteOp.go).
- The goxdcr internal setting UseLegacyPreReplicate=true (MB-69793) restores
  the pre-8.1 path: one POST /_goxdcr/_pre_replicate/<bucket> per vbucket to
  the target node that owns it, which the target's ns_server answers with 200
  for a match, or 400 and the current vbuuid for a mismatch
  (service_impl/capi_service.go).

The setting is global and kept in metakv. goxdcr reads it once at start-up,
and changing it makes goxdcr checkpoint its pipelines and restart on every
node of the cluster. Only the source cluster's value matters.

What the tests assert on, as measured on 8.5.0-1294 with a 2+2 cluster:

- the target's access logs: in legacy mode a source node's pipeline start
  sends one _pre_replicate per vbucket it owns, answered 200 when the
  checkpoint holds and 400 when it does not, and the default mode sends none.
  That is how a test knows which path ran;
- the source's goxdcr.log line "StartSeqnoGetter ... skipped ckpt records",
  whose "preRepl=" entries are checkpoint records pre-replicate rejected, on
  either path;
- docs_received_from_dcp, which starts from 0 at every pipeline start rather
  than being restored from the checkpoints. Once the target has caught up it
  is the number of docs DCP sent since the resume, so anything above the docs
  loaded since then was sent again: a vbucket that resumed from seqno 0. The
  CR counters cannot show that: these docs are small enough to be replicated
  optimistically, so a resent doc is rejected by the target, not counted in
  docs_failed_cr_source, and docs_failed_cr_target is not in the REST stats.
  test_restream_is_detected keeps this measure honest;
- the data itself, through verify_results.
"""
import json
import time
import urllib.parse

from TestInput import TestInputSingleton
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from .xdcrnewbasetests import XDCRNewBaseTest, NodeHelper, OPS, \
    REPLICATION_TYPE

LEGACY_PRE_REPLICATE = "UseLegacyPreReplicate"

# By default a target topology change restarts the pipeline only after 30
# checks 10s apart. With 3, the restart -- and the pre-replicate calls a
# failover test waits for -- comes within a minute of the rebalance
FAST_TOPOLOGY_RESTART = {"MaxTopologyChangeCountBeforeRestart": 3,
                         "MaxTopologyStableCountBeforeRestart": 3}

# Shipped values (8.5.0-1294) that tearDown restores whatever the cluster held
# before, so a run that died part-way does not leave its settings behind
SHIPPED_DEFAULTS = {LEGACY_PRE_REPLICATE: False,
                    "MaxTopologyChangeCountBeforeRestart": 30,
                    "MaxTopologyStableCountBeforeRestart": 30}

# Counts the _pre_replicate POSTs in a target node's access logs by client
# address and HTTP status. A line reads
# <client> - <user> [<time> <tz>] "POST <path> HTTP/1.1" <status> ...
COUNT_PRE_REPLICATE_CALLS = (
    r"""awk '/"POST \/_goxdcr\/_pre_replicate/ {n[$1 " " $9]++} """
    r"""END {for (k in n) print k, n[k]}'""")

# goxdcr.log line naming the checkpoint records a pipeline start skipped; the
# ones pre-replicate rejected carry a "preRepl=" reason
SKIPPED_BY_PRE_REPLICATE = "skipped ckpt records.*preRepl="

# goxdcr.log line of every pipeline restart, a topology change's included
PIPELINE_RESTART = "Try to fix Pipeline"


class LegacyPreReplicateXDCR(XDCRNewBaseTest):

    # A settings POST restarts goxdcr on every node, and the 8091 proxy
    # answers "Unexpected server error" while that is in flight
    SETTINGS_ATTEMPTS = 4

    def setUp(self):
        # goxdcr is found and killed by process name, which on cluster_run
        # would hit every cluster's goxdcr at once
        if len({server.ip for server in TestInputSingleton.input.servers}) \
                == 1:
            self.skipTest("Needs real nodes: cluster_run shares one host "
                          "between the clusters")
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_bucket = self.src_cluster.get_bucket_by_name('default')
        # The base replaces _num_items with the collection density's doc
        # count (1000 for the default "low"), so read the conf value directly
        self.items = int(self._input.param("items", 10000))
        self.legacy = self._input.param("legacy_prereplicate", True)
        self.convergence_timeout = int(self._input.param(
            "convergence_timeout", 600))
        self.goxdcr_restart_timeout = int(self._input.param(
            "goxdcr_restart_timeout", 180))
        # {cluster name: {setting: value tearDown restores}}
        self._restore_settings = {}

        # unittest skips tearDown when setUp raises, which would leave the
        # source in legacy mode
        try:
            self._set_internal_settings(self.src_cluster,
                                        {LEGACY_PRE_REPLICATE: self.legacy})
            self.setup_xdcr()
            self.repl_id = self.src_cluster.get_remote_clusters()[0] \
                .get_replications()[0].get_repl_id()
            self.num_vbuckets = len(self._vbucket_map(self.src_cluster)[1])
            self.log.info("Replication {0}, {1} vbuckets, {2}={3}".format(
                self.repl_id, self.num_vbuckets, LEGACY_PRE_REPLICATE,
                self.legacy))
        except Exception:
            self.tearDown()
            raise

    def tearDown(self):
        # Before the base deletes the replications: a settings change restarts
        # goxdcr, and a DELETE that lands while it restarts fails. A restore
        # that fails still fails the test, after the cleanup, since it leaves
        # a shared cluster running with the test's settings
        failures = []
        for cluster_name, settings in getattr(
                self, "_restore_settings", {}).items():
            try:
                self._set_internal_settings(
                    self.get_cb_cluster_by_name(cluster_name), settings,
                    record=False)
            except Exception as e:
                failures.append("{0} on {1}: {2}".format(settings,
                                                         cluster_name, e))
        super().tearDown()
        if failures:
            raise Exception("Could not restore the goxdcr internal settings: "
                            "{0}".format("; ".join(failures)))

    # ------------------------------------------------------------------
    # goxdcr internal settings and process
    # ------------------------------------------------------------------
    def _internal_settings(self, rest):
        status, content, _ = rest._http_request(
            rest.baseUrl + "xdcr/internalSettings", "GET", timeout=30)
        if not status:
            raise Exception("GET xdcr/internalSettings on {0} failed: {1}"
                            .format(rest.ip, content))
        return json.loads(content)

    def _read_internal_settings(self, cluster):
        """The cluster's internal settings, retried through the window in
        which the 8091 proxy fails while goxdcr restarts"""
        last = None
        for attempt in range(1, self.SETTINGS_ATTEMPTS + 1):
            try:
                return self._internal_settings(self.get_live_rest(cluster))
            except Exception as e:
                last = e
            if attempt < self.SETTINGS_ATTEMPTS:
                self.sleep(15, "GET xdcr/internalSettings on {0} failed "
                               "({1}/{2}): {3}".format(
                                   cluster.get_name(), attempt,
                                   self.SETTINGS_ATTEMPTS, last))
        raise Exception("GET xdcr/internalSettings on {0} failed {1} times, "
                        "last error: {2}".format(cluster.get_name(),
                                                 self.SETTINGS_ATTEMPTS, last))

    def _set_internal_settings(self, cluster, settings, record=True):
        """Set goxdcr internal settings on `cluster` and wait until every node
        runs with them. One POST covers the cluster, since they live in
        metakv, but goxdcr reads them only at start-up: a change has taken
        effect once every node has restarted goxdcr and reads the new values
        back. With `record`, tearDown restores what the test changed"""
        current = self._read_internal_settings(cluster)
        missing = sorted(set(settings) - set(current))
        if missing:
            self.fail("{0} has no goxdcr internal setting {1}: the build "
                      "predates it".format(cluster.get_name(), missing))
        if record:
            restore = self._restore_settings.setdefault(cluster.get_name(), {})
            for key in settings:
                restore.setdefault(key, SHIPPED_DEFAULTS.get(key, current[key]))
        changes = {key: value for key, value in settings.items()
                   if current[key] != value}
        if not changes:
            self.log.info("{0} already runs with {1}".format(
                cluster.get_name(), settings))
            return
        pids = self._goxdcr_pids(cluster)
        self._post_internal_settings(cluster, changes)
        self._wait_for_goxdcr_restart(cluster, pids)
        for node in cluster.get_nodes():
            self._wait_for_internal_settings(node, changes)
        self.log.info("goxdcr restarted on every node of {0} with {1}".format(
            cluster.get_name(), changes))

    def _post_internal_settings(self, cluster, settings):
        body = urllib.parse.urlencode(
            {key: str(value).lower() if isinstance(value, bool) else value
             for key, value in settings.items()})
        last = None
        for attempt in range(1, self.SETTINGS_ATTEMPTS + 1):
            try:
                rest = self.get_live_rest(cluster)
                status, content, _ = rest._http_request(
                    rest.baseUrl + "xdcr/internalSettings", "POST", body,
                    timeout=30)
            except Exception as e:
                status, content = False, repr(e)
            if status:
                applied = json.loads(content)
                for key, value in settings.items():
                    self.assertEqual(
                        applied.get(key), value,
                        "{0} reports {1}={2!r} after setting it to {3!r}"
                        .format(cluster.get_name(), key, applied.get(key),
                                value))
                return
            last = content
            if attempt < self.SETTINGS_ATTEMPTS:
                self.sleep(15, "POST xdcr/internalSettings on {0} failed "
                               "({1}/{2}): {3}".format(
                                   cluster.get_name(), attempt,
                                   self.SETTINGS_ATTEMPTS, last))
        self.fail("POST xdcr/internalSettings {0} on {1} failed {2} times, "
                  "last error: {3}".format(settings, cluster.get_name(),
                                           self.SETTINGS_ATTEMPTS, last))

    def _wait_for_internal_settings(self, node, expected, timeout=120):
        end_time = time.time() + timeout
        while True:
            try:
                current = self._internal_settings(RestConnection(node))
                seen = {key: current.get(key) for key in expected}
            except Exception as e:
                seen = repr(e)
            if seen == expected:
                return
            if time.time() > end_time:
                self.fail("goxdcr on {0} reads {1} {2}s after {3} was set"
                          .format(node.ip, seen, timeout, expected))
            self.sleep(5, "Waiting for goxdcr on {0} to read {1}".format(
                node.ip, expected))

    def _run(self, node, command):
        """(output lines, exit code) of `command` on `node`"""
        shell = RemoteMachineShellConnection(node)
        try:
            output, _, exit_code = shell.execute_command(
                command, debug=False, get_exit_code=True)
        finally:
            shell.disconnect()
        return output, exit_code

    def _goxdcr_pids(self, cluster):
        """{node ip: pid of its goxdcr, or None while none runs}"""
        pids = {}
        for node in cluster.get_nodes():
            output, _ = self._run(node, "pgrep -x goxdcr")
            pids[node.ip] = output[0].strip() if output else None
        return pids

    def _wait_for_goxdcr_restart(self, cluster, before):
        """Wait until every node of `cluster` runs a goxdcr other than the
        one `before` recorded"""
        end_time = time.time() + self.goxdcr_restart_timeout
        while True:
            now = self._goxdcr_pids(cluster)
            waiting = sorted(ip for ip, pid in now.items()
                             if pid is None or pid == before.get(ip))
            if not waiting:
                return
            if time.time() > end_time:
                self.fail("goxdcr did not restart on {0} within {1}s: pids "
                          "{2} before, {3} now".format(
                              waiting, self.goxdcr_restart_timeout, before,
                              now))
            self.sleep(5, "Waiting for goxdcr to restart on {0}".format(
                waiting))

    def _kill_goxdcr(self, cluster):
        """kill -9 goxdcr on every node of `cluster`; the babysitter starts
        it again"""
        for node in cluster.get_nodes():
            _, exit_code = self._run(node, "pkill -9 -x goxdcr")
            self.assertEqual(exit_code, 0, "pkill found no goxdcr to kill on "
                                           "{0}".format(node.ip))

    # ------------------------------------------------------------------
    # Load and convergence
    # ------------------------------------------------------------------
    def _load(self, prefix, count):
        """Start loading `count` new docs into the source bucket, recording
        them in its kv store so verify_results covers them"""
        gen = BlobGenerator(prefix, prefix, self._value_size, start=0,
                            end=count)
        return self.get_cluster_op().async_load_gen_docs(
            self.src_cluster.get_master_node(), self.src_bucket.name, gen,
            self.src_bucket.kvs[1], OPS.CREATE, batch_size=1000,
            compression=self._sdk_compression)

    def _active_items(self, cluster):
        total = 0
        for node in cluster.get_nodes():
            client = MemcachedClientHelper.direct_client(
                node, self.src_bucket.name)
            try:
                total += int(client.stats()["curr_items"])
            finally:
                client.close()
        return total

    def _wait_for_items(self, cluster, expected, label, timeout=None):
        """Wait until `cluster` holds `expected` docs on two polls in a row.
        Counts come from memcached: right after a load the REST samples can
        still show the previous, equal value"""
        end_time = time.time() + (timeout or self.convergence_timeout)
        confirmed = False
        while True:
            try:
                actual = self._active_items(cluster)
            except (OSError, EOFError) as e:
                # A memcached connection from the test host can drop mid-poll
                # (a BrokenPipeError on 8.5.0-1294 from a Mac); the next poll
                # opens a new one
                self.log.warning("{0}: could not read the item count of {1}: "
                                 "{2!r}".format(label, cluster.get_name(), e))
                actual = None
            if actual == expected:
                if confirmed:
                    return
                confirmed = True
            else:
                confirmed = False
                if time.time() > end_time:
                    self.fail("{0}: {1} holds {2} docs, expected {3}".format(
                        label, cluster.get_name(), actual, expected))
            self.sleep(10, "{0}: {1} holds {2} of {3} docs".format(
                label, cluster.get_name(), actual, expected))

    def _wait_for_convergence(self, label, timeout=None):
        """Wait until both clusters hold every doc loaded so far"""
        expected = len(self.src_bucket.kvs[1])
        for cluster in (self.src_cluster, self.dest_cluster):
            self._wait_for_items(cluster, expected, label, timeout)
        self.log.info("{0}: both clusters hold {1} docs".format(
            label, expected))

    def _vbucket_map(self, cluster):
        """(server list, vbucket map) of the test bucket on `cluster`"""
        server_map = RestConnection(cluster.get_master_node()).get_bucket_json(
            self.src_bucket.name)["vBucketServerMap"]
        return server_map["serverList"], server_map["vBucketMap"]

    def _active_vbuckets(self, cluster, node):
        servers, vbucket_map = self._vbucket_map(cluster)
        owner = [i for i, server in enumerate(servers)
                 if server.rsplit(":", 1)[0] == node.ip]
        self.assertTrue(owner, "{0} is not in the vbucket map of {1}: {2}"
                        .format(node.ip, cluster.get_name(), servers))
        return sum(1 for chain in vbucket_map if chain[0] == owner[0])

    # ------------------------------------------------------------------
    # Log and stat signals
    # ------------------------------------------------------------------
    def _count_in_goxdcr_log(self, node, pattern):
        """goxdcr.log lines matching `pattern` on `node`, the rotated logs
        included. grep -a: an unclean stop can leave NULs in goxdcr.log, and
        grep then prints "binary file matches" instead of the lines"""
        log_dir = NodeHelper.get_goxdcr_log_dir(node)
        output, exit_code = self._run(
            node, "ls {0}/goxdcr.log > /dev/null && zcat -f {0}/goxdcr.log* "
                  "| grep -a -c -e '{1}'".format(log_dir, pattern))
        # grep -c exits 1 when it counts nothing
        if exit_code not in (0, 1) or not output:
            self.fail("Could not count '{0}' in goxdcr.log on {1}: exit {2}"
                      .format(pattern, node.ip, exit_code))
        return int(output[0])

    def _goxdcr_log_counts(self, pattern):
        """{source node ip: {pattern: lines}}"""
        return {node.ip: {pattern: self._count_in_goxdcr_log(node, pattern)}
                for node in self.src_cluster.get_nodes()}

    def _pre_replicate_calls(self):
        """{target node ip: {(client address, HTTP status): _pre_replicate
        POSTs}}, the rotated access logs included"""
        calls = {}
        for node in self.dest_cluster.get_nodes():
            log_dir = NodeHelper.get_goxdcr_log_dir(node)
            # ls first: a glob that matched nothing would read as no calls
            output, exit_code = self._run(
                node, "ls {0}/http_access.log > /dev/null && zcat -f "
                      "{0}/http_access*.log* | {1}".format(
                          log_dir, COUNT_PRE_REPLICATE_CALLS))
            if exit_code != 0:
                self.fail("Could not read the access logs of {0}: exit {1}"
                          .format(node.ip, exit_code))
            calls[node.ip] = {}
            for line in output:
                client, status, count = line.split()
                calls[node.ip][(client, status)] = int(count)
        return calls

    def _growth(self, before, after, what):
        """{node: {key: growth}} from `before` to `after`, for the nodes in
        `after`. A count that shrank means its log was rotated away since
        `before`, and the comparison means nothing"""
        growth = {}
        for node, counts in after.items():
            old = before.get(node, {})
            shrunk = {key: (n, counts.get(key, 0)) for key, n in old.items()
                      if counts.get(key, 0) < n}
            if shrunk:
                self.fail("{0} on {1} went down since the baseline {2}: the "
                          "log was rotated away, so the count is unusable"
                          .format(what, node, shrunk))
            growth[node] = {key: n - old.get(key, 0)
                            for key, n in counts.items()
                            if n > old.get(key, 0)}
        return growth

    def _skipped_by_pre_replicate(self, before):
        growth = self._growth(before["skipped"],
                              self._goxdcr_log_counts(SKIPPED_BY_PRE_REPLICATE),
                              "'{0}' lines".format(SKIPPED_BY_PRE_REPLICATE))
        return sum(sum(counts.values()) for counts in growth.values())

    def _docs_streamed(self):
        """docs_received_from_dcp of the replication: what DCP sent since its
        pipelines last started. Read until two samples 10s apart agree, as the
        stat trails the pipeline by a scrape"""
        previous = None
        for _ in range(6):
            try:
                value = int(self.read_repl_stat(
                    self.src_cluster, self.repl_id, self.src_bucket.name,
                    "docs_received_from_dcp"))
            except Exception as e:
                self.log.warning("docs_received_from_dcp unreadable: {0}"
                                 .format(e))
                value = None
            if value is not None and value == previous:
                return value
            previous = value
            self.sleep(10, "Waiting for docs_received_from_dcp to settle")
        if previous is None:
            self.fail("docs_received_from_dcp of {0} could not be read"
                      .format(self.repl_id))
        return previous

    def _snapshot(self):
        """The signals a resume is judged by, taken while the replication
        runs and has caught up"""
        snapshot = {
            "calls": self._pre_replicate_calls(),
            "skipped": self._goxdcr_log_counts(SKIPPED_BY_PRE_REPLICATE),
            "restarts": self._goxdcr_log_counts(PIPELINE_RESTART),
            "on_target": self._active_items(self.dest_cluster)}
        self.log.info("Baseline: {0}".format(snapshot))
        return snapshot

    # ------------------------------------------------------------------
    # Replication control
    # ------------------------------------------------------------------
    def _pause(self, timeout=120):
        """Pause the replication and wait until every source node has
        stopped its pipeline. goxdcr acknowledges the pause at once, but a
        pipeline writes its stop checkpoint before it stops, and a target
        change made before then would be recorded in that checkpoint"""
        stopped = "Pipeline {0} has been stopped".format(self.repl_id)
        before = self._goxdcr_log_counts(stopped)
        self.src_cluster.pause_all_replications(verify=True)
        end_time = time.time() + timeout
        while True:
            growth = self._growth(before, self._goxdcr_log_counts(stopped),
                                  "'{0}' lines".format(stopped))
            waiting = sorted(node for node, counts in growth.items()
                             if not counts)
            if not waiting:
                return
            if time.time() > end_time:
                self.fail("The pipeline did not stop on {0} within {1}s of "
                          "the pause".format(waiting, timeout))
            self.sleep(5, "Waiting for the pipeline to stop on {0}".format(
                waiting))

    def _wait_for_checkpoint(self):
        """Wait for a periodic checkpoint on every source node from now on,
        so that a goxdcr killed next has one covering everything replicated
        so far. num_checkpoints is summed over the nodes and each node
        checkpoints on its own timer"""
        rest = RestConnection(self.src_cluster.get_master_node())
        baseline = self.get_successful_checkpoint_count(rest)
        expected = baseline + len(self.src_cluster.get_nodes())
        timeout = self._checkpoint_interval * 3
        end_time = time.time() + timeout
        while True:
            current = self.get_successful_checkpoint_count(rest)
            if current >= expected:
                return
            if time.time() > end_time:
                self.fail("num_checkpoints went from {0} to {1} in {2}s at a "
                          "{3}s checkpoint interval, expected {4}: one per "
                          "source node".format(baseline, current, timeout,
                                               self._checkpoint_interval,
                                               expected))
            self.sleep(10, "Waiting for a checkpoint on every source node "
                           "({0} of {1})".format(current, expected))

    def _wait_for_pipeline_restart(self, before, label, timeout=300):
        """Wait until a source node restarts its pipeline after `before`"""
        end_time = time.time() + timeout
        while True:
            growth = self._growth(before["restarts"],
                                  self._goxdcr_log_counts(PIPELINE_RESTART),
                                  "'{0}' lines".format(PIPELINE_RESTART))
            if any(growth.values()):
                return
            if time.time() > end_time:
                self.fail("{0}: no source node restarted its pipeline within "
                          "{1}s".format(label, timeout))
            self.sleep(10, "{0}: waiting for a pipeline restart".format(
                label))

    # ------------------------------------------------------------------
    # Assertions
    # ------------------------------------------------------------------
    def _calls_since(self, before, status=None):
        """{client address: _pre_replicate calls since `before`} over the
        target nodes still in the cluster, only those answered `status` when
        it is given"""
        growth = self._growth(before["calls"], self._pre_replicate_calls(),
                              "_pre_replicate calls")
        calls = {}
        for counts in growth.values():
            for (client, answer), n in counts.items():
                if status is None or answer == status:
                    calls[client] = calls.get(client, 0) + n
        self.log.info("_pre_replicate calls since the baseline: {0}, "
                      "answered {1}: {2}".format(growth, status or "anything",
                                                 calls))
        return calls

    def _assert_pre_replicate_path(self, before, legacy, label, status=None,
                                   expected=None):
        """Legacy mode: the target received at least `expected` (one per
        vbucket by default) _pre_replicate calls since `before`, answered
        `status`, from every source node. Default mode: none at all"""
        if not legacy:
            calls = sum(self._calls_since(before).values())
            self.assertEqual(
                calls, 0,
                "{0}: the default mode validates checkpoints locally, but the "
                "target received {1} _pre_replicate calls".format(label,
                                                                  calls))
            return
        expected = expected or self.num_vbuckets
        calls = self._calls_since(before, status)
        self.assertGreaterEqual(
            sum(calls.values()), expected,
            "{0}: in legacy mode a pipeline start calls _pre_replicate on the "
            "target once per vbucket, but the target received {1} calls "
            "answered {2} for {3} vbuckets".format(
                label, calls, status or "anything", expected))
        sources = len(self.src_cluster.get_nodes())
        self.assertGreaterEqual(
            len(calls), sources,
            "{0}: _pre_replicate calls came from {1}, but {2} source nodes "
            "own vbuckets: a node validated its checkpoints some other way"
            .format(label, sorted(calls), sources))

    def _restreamed(self, new_docs):
        """Docs DCP sent since the last pipeline start beyond the `new_docs`
        loaded since the baseline, i.e. sent again"""
        streamed = self._docs_streamed()
        self.log.info("{0} docs streamed since the pipelines started, {1} of "
                      "them new".format(streamed, new_docs))
        return streamed - new_docs

    def _assert_resumed_from_checkpoints(self, before, new_docs, legacy,
                                         label):
        """Since `before`, the pipelines validated their checkpoints on the
        path `legacy` selects, accepted them, and resumed from them"""
        self._assert_pre_replicate_path(before, legacy, label, status="200")
        skipped = self._skipped_by_pre_replicate(before)
        self.assertEqual(
            skipped, 0,
            "{0}: pre-replicate rejected checkpoint records on {1} pipeline "
            "start(s), see '{2}' in goxdcr.log".format(
                label, skipped, SKIPPED_BY_PRE_REPLICATE))
        # One vbucket's share of the docs: a single vbucket that resumed from
        # seqno 0 rather than its checkpoint goes over it
        bound = max(1, before["on_target"] // self.num_vbuckets)
        restreamed = self._restreamed(new_docs)
        self.assertLess(
            restreamed, bound,
            "{0}: {1} docs the target already held were streamed again, at "
            "least one vbucket's worth ({2}): the pipelines resumed from "
            "seqno 0 rather than from their checkpoints".format(
                label, restreamed, bound))

    def _wait_for_pre_replicate_calls(self, before, expected, label,
                                      timeout=300):
        """Wait until the target nodes still in the cluster have received
        `expected` _pre_replicate calls since `before`: a restart for a
        target topology change comes on its own schedule"""
        end_time = time.time() + timeout
        while True:
            calls = sum(self._calls_since(before).values())
            if calls >= expected:
                return
            if time.time() > end_time:
                self.fail("{0}: the target received {1} _pre_replicate calls "
                          "in {2}s, expected at least {3}".format(
                              label, calls, timeout, expected))
            self.sleep(15, "{0}: {1} of {2} _pre_replicate calls".format(
                label, calls, expected))

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------
    def test_resume_after_pause(self):
        """Pause, load, resume: the resume validates the checkpoints the
        pause wrote -- through the target in legacy mode, locally in the
        default mode -- and continues from them"""
        self._load("before-pause-", self.items).result()
        self._wait_for_convergence("initial load")
        before = self._snapshot()
        self._pause()
        new_docs = self.items // 2
        self._load("while-paused-", new_docs).result()
        self.src_cluster.resume_all_replications(verify=True)
        self._wait_for_convergence("after resume")
        self._assert_resumed_from_checkpoints(before, new_docs, self.legacy,
                                              "pause/resume")
        self.verify_results()

    def test_resume_after_goxdcr_crash(self):
        """kill -9 goxdcr on every source node while a load runs: the
        restarted pipelines resume from the last periodic checkpoint"""
        self._load("before-crash-", self.items).result()
        self._wait_for_convergence("initial load")
        # A killed goxdcr takes no checkpoint on the way down, so the resume
        # is measured against periodic ones covering the initial load
        self._wait_for_checkpoint()
        before = self._snapshot()
        pids = self._goxdcr_pids(self.src_cluster)
        new_docs = self.items // 2
        load = self._load("during-crash-", new_docs)
        self._kill_goxdcr(self.src_cluster)
        self._wait_for_goxdcr_restart(self.src_cluster, pids)
        load.result()
        self._wait_for_convergence("after the crash")
        self._assert_resumed_from_checkpoints(before, new_docs, self.legacy,
                                              "goxdcr crash")
        self.verify_results()

    def test_target_flush_while_paused(self):
        """Flush the target while the replication is paused. Every
        checkpoint now names a target vbuuid the target has lost, so
        pre-replicate must reject them all -- in legacy mode the target
        answers 400 -- and the resume re-replicates from seqno 0. Accepting
        one would leave the target without the docs it covers"""
        self._load("before-flush-", self.items).result()
        self._wait_for_convergence("initial load")
        before = self._snapshot()
        self._pause()
        self.dest_cluster.flush_buckets(
            [self.dest_cluster.get_bucket_by_name(self.src_bucket.name)])
        self._wait_for_items(self.dest_cluster, 0, "target flush")
        self.src_cluster.resume_all_replications(verify=True)
        self._wait_for_convergence("after resume")
        self._assert_pre_replicate_path(before, self.legacy, "target flush",
                                        status="400")
        skipped = self._skipped_by_pre_replicate(before)
        self.assertGreater(
            skipped, 0,
            "The target was flushed, so every checkpoint was stale, yet no "
            "pipeline start logged a record rejected by pre-replicate "
            "('{0}' in goxdcr.log)".format(SKIPPED_BY_PRE_REPLICATE))
        self.verify_results()

    def test_target_failover_during_replication(self):
        """Hard-fail over a target node while a load runs and rebalance it
        out. Only the source nodes whose target vbuckets moved restart their
        pipelines for the topology change, and in legacy mode those
        vbuckets' pre-replicate calls follow them to their new owner"""
        self._set_internal_settings(self.src_cluster, FAST_TOPOLOGY_RESTART)
        self._load("before-failover-", self.items).result()
        self._wait_for_convergence("initial load")
        # failover_and_rebalance_nodes takes the last node of the cluster
        failed = self.dest_cluster.get_nodes()[-1]
        moved = self._active_vbuckets(self.dest_cluster, failed)
        before = self._snapshot()
        load = self._load("during-failover-", self.items // 2)
        self.dest_cluster.failover_and_rebalance_nodes(num_nodes=1,
                                                       graceful=False)
        load.result()
        self._wait_for_convergence("after the failover")
        self._wait_for_pipeline_restart(before, "target failover")
        if self.legacy:
            self._wait_for_pre_replicate_calls(
                before, moved, "{0}'s {1} vbuckets moving".format(failed.ip,
                                                                 moved))
        else:
            self._assert_pre_replicate_path(before, False, "target failover")
        self.verify_results()

    def test_switch_prereplicate_mode(self):
        """Flip UseLegacyPreReplicate on a live replication, then flip it
        back. Each flip makes goxdcr checkpoint and restart, and the resume
        has to accept the checkpoints the other mode wrote"""
        self._load("phase-0-", self.items).result()
        self._wait_for_convergence("initial load")
        new_docs = self.items // 2
        for phase, legacy in enumerate((not self.legacy, self.legacy), 1):
            label = "switch to {0}={1}".format(LEGACY_PRE_REPLICATE, legacy)
            before = self._snapshot()
            self._set_internal_settings(self.src_cluster,
                                        {LEGACY_PRE_REPLICATE: legacy})
            self._load("phase-{0}-".format(phase), new_docs).result()
            self._wait_for_convergence(label)
            self._assert_resumed_from_checkpoints(before, new_docs, legacy,
                                                  label)
        self.verify_results()

    def test_restream_is_detected(self):
        """Keeps the resume tests honest: they pass only while a pipeline
        that streams again from seqno 0 is seen doing it. A replication
        recreated without checkpoints does exactly that, and the docs it
        streams have to show up as sent again. If a build starts restoring
        docs_received_from_dcp from checkpoints, this fails before the
        resume tests silently stop meaning anything"""
        self._load("before-recreate-", self.items).result()
        self._wait_for_convergence("initial load")
        before = self._snapshot()
        rest = RestConnection(self.src_cluster.get_master_node())
        rest.stop_replication(rest.get_replications()[0]["cancelURI"])
        self.assertTrue(self.wait_for_replications_to_clear(rest),
                        "The replication was not deleted")
        rest.start_replication(
            REPLICATION_TYPE.CONTINUOUS, self.src_bucket.name,
            self.src_cluster.get_remote_clusters()[0].get_name())
        new_docs = self.items // 2
        self._load("after-recreate-", new_docs).result()
        self._wait_for_convergence("after recreate")
        restreamed = self._restreamed(new_docs)
        self.assertGreaterEqual(
            restreamed, before["on_target"],
            "The recreated replication had no checkpoints, so it must have "
            "streamed the {0} docs already on the target again, but only {1} "
            "show as sent again: the resume tests cannot see a restream"
            .format(before["on_target"], restreamed))
        self.verify_results()
