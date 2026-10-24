"""CBSE-23458: broken_mappings.py, support's script that removes a stuck XDCR
broken collection mapping by patching metakv (goxdcr change 254675,
tools/broken_mappings.py; procedure in the doc "CBSE-23458: Patching metakv to
remove broken mapping").

Every test follows the doc: pause, wait until every source node has
checkpointed (main and backfill), run view, prep and patch (or restore) on
every source KV node against its own 127.0.0.1:8091, restart goxdcr on every
source node, resume. The broken mapping is genuine -- a target scope or
collection that does not exist -- unless induce=false_positive, which
reproduces MB-73479 the way goxdcr test 2j does. That works only on builds
without the MB-73479 fix (8.0.x, 7.6.x): on totoro the false positive is not
reproducible, so its rows use a genuine one.

Besides the script's exit codes and output, the tests check:
- live state: the (source, target) pair in the replication's UI errors
  (/pools/default/tasks) and in every source node's replicationInfos, the main
  and backfill pipelines told apart;
- persisted state: every source node's metakv brokenMappings doc and the
  brokenCollectionsMapSha256 of every checkpoint doc, main and backfill
  (checkpoints are node-local, so each node is read on its own);
- data: per-collection item counts. A broken mapping drops mutations while
  changes_left still drains, so an empty error list proves nothing by itself.

The script is not in this repo: pass its path as brokenmap_script (default
tmp/cbse23458/broken_mappings.py, which .gitignore's tmp* keeps out of git).
Buckets are created here, not by the base class, so every row runs with
default_bucket=False and names the vbucket layout it tests:
src_storage/src_vbuckets/dest_storage/dest_vbuckets, plus variable_vb to
assert the layout really is (or is not) a variable-VB replication.
"""

import hashlib
import json
import os
import shlex
import threading
import time
import urllib.parse

from lib.collection.collections_stats import CollectionsStats
from lib.membase.api.rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection
from pytests.xdcr import brokenmap_utils as bmu
from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest, NodeHelper

SRC_SCOPE = "S1"
HEALTHY = "col0"   # its target exists throughout: proves the procedure costs nothing elsewhere
BROKEN = "col1"    # its target is missing (or, induce=false_positive, falsely reported missing)
SAME = "col2"      # scenario 2's second broken mapping, to a same-named target
BUSY = "col3"      # scenario 4: shares one backfill with col1 and keeps it running

# With explicit mapping col0 and col1 go to a differently named scope, and col2
# keeps its own name, so while S1.col2 is missing on the target so is scope S1.
# With implicit mapping every target is the same-named S1.<coll>.
EXPLICIT_TARGETS = {HEALTHY: ("S3", HEALTHY), BROKEN: ("S3", BROKEN), SAME: ("S1", SAME)}

SCRIPT_NAME = "broken_mappings.py"
PILLOWFIGHT = "/opt/couchbase/bin/cbc-pillowfight"
BUNDLED_PYTHON = "/opt/couchbase/lib/python/interp/bin/python3"
# goxdcr re-fetches a target manifest at most this often (s); default 60, internal setting
MANIFEST_REFRESH = "ManifestRefreshTgtInterval"
MANIFEST_REFRESH_DEFAULT = 60
# broken_mappings.py's refusals (each "Error: ..." on stderr, exit 1)
PAUSED_REFUSAL = "replication must be paused"
NOTHING_WRITTEN = "Nothing has been written"
NO_BACKUP_REFUSAL = "run prep first"
VARVB_REFUSAL = "set CB_ENABLE_VARVB=true"
BACKUP_EXISTS_REFUSAL = "Move it away and retry"


class BrokenMappingPatchXDCR(XDCRNewBaseTest):

    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name("C1")
        self.dest_cluster = self.get_cb_cluster_by_name("C2")
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)
        self.bucket = "default"
        self.mapping = self._input.param("mapping", "explicit")
        self.induce = self._input.param("induce", "genuine")
        self.ns_items = self._input.param("ns_items", 2000)
        self.post_items = self._input.param("post_items", 1000)
        self.backfill_items = self._input.param("backfill_items", 40000)
        # scenario 4: col1 and col3 each get this many docs, so their shared backfill
        # runs for minutes under the bandwidth cap (MiB/s)
        self.backfill_mbps = self._input.param("backfill_mbps", 1)
        self.backfill_doc_size = self._input.param("backfill_doc_size", 8192)
        self.broken_timeout = self._input.param("broken_timeout", 180)
        self.heal_timeout = self._input.param("heal_timeout", 600)
        self.hold_secs = self._input.param("hold_secs", 120)
        self.ckpt_timeout = self._input.param("ckpt_timeout", 300)
        self.expect_on_every_node = self._input.param("expect_broken_on_every_node", True)
        self.install_snappy = self._input.param("install_snappy", True)
        self.induce_attempts = self._input.param("induce_attempts", 10)
        self.induce_poll_secs = self._input.param("induce_poll_secs", 60)
        # induce/reload rounds for the false positive to survive a reload
        self.induce_rounds = self._input.param("induce_rounds", 3)
        self.script = self._input.param("brokenmap_script", "tmp/cbse23458/broken_mappings.py")
        # auto: python3 on PATH, else Couchbase's bundled interpreter (_pick_python)
        self.node_python_param = self._input.param("node_python", "auto")
        self.node_python = {}
        # The doc sets CB_ENABLE_VARVB only for variable-VB replications, and so does
        # this suite by default. enable_varvb=True sets it always, for the script's
        # patchset e1c19f3: it refused every same-VB replication on 8.0.1 and 8.5.0,
        # because goxdcr writes an empty globalInfo doc for each (fixed in 20fa5d4).
        self.enable_varvb = self._input.param("enable_varvb", None)
        # Every row deletes and recreates the same replication id against new buckets. On
        # 8.0.1 the deleted replication's ckpt/ docs (checkpoints, manifests, backfill) stayed
        # in the metakv of the source nodes other than the one it was created on, and the
        # next row's replication started from them: backfills raised for one node's vbuckets
        # only, and docs of a created target collection not written (measured 2026-10-20).
        # fresh_goxdcr=True deletes those leftovers and restarts goxdcr on the source.
        self.fresh_goxdcr = self._input.param("fresh_goxdcr", True)
        self.workdir = None
        self.repl = None
        self.repl_id = None
        self._batch = 0
        if self.mapping not in ("explicit", "implicit"):
            self.fail("mapping must be explicit or implicit, not {}".format(self.mapping))
        if self.induce not in ("genuine", "false_positive"):
            self.fail("induce must be genuine or false_positive, not {}".format(self.induce))
        if len(self.src_cluster.get_nodes()) < 2:
            self.fail("the script runs once per source KV node; test it on a multi-node source")
        self._create_buckets()
        if self.fresh_goxdcr:
            self._drop_stale_ckpt_docs()
            self._restart_goxdcr()
        # reads the GlobalTimestamps of variable-VB checkpoints: python-snappy when the
        # test host has it, else brokenmap_utils' stdlib decoder
        try:
            import snappy
            self.snappy = snappy.decompress
        except ImportError:
            self.snappy = bmu.snappy_decompress

    # ---- buckets ---------------------------------------------------------------

    def _create_buckets(self):
        if self.src_cluster.get_buckets() or self.dest_cluster.get_buckets():
            self.fail("run with default_bucket=False: this suite creates buckets with the "
                      "vbucket layout the row names")
        layouts = {}
        for cluster, side in ((self.src_cluster, "src"), (self.dest_cluster, "dest")):
            storage = self._input.param("{}_storage".format(side), "magma")
            vbuckets = self._input.param("{}_vbuckets".format(side), None)
            size = min(3072, cluster.get_mem_quota() - 500)
            cluster.create_default_bucket(
                size, self._num_replicas,
                eviction_policy=self._input.param("eviction_policy", "fullEviction"),
                bucket_storage=storage,
                num_vbuckets=int(vbuckets) if vbuckets else None)
            layouts[side] = (storage, self._num_vbuckets(cluster))
        self.src_vbs = layouts["src"][1]
        self.dest_vbs = layouts["dest"][1]
        self.variable_vb = self.src_vbs != self.dest_vbs
        self.log.info("source {} {} vbuckets -> target {} {} vbuckets: {}variable-VB replication".format(
            layouts["src"][0], self.src_vbs, layouts["dest"][0], self.dest_vbs,
            "" if self.variable_vb else "not a "))
        expected = self._input.param("variable_vb", None)
        if expected is not None and bool(expected) != self.variable_vb:
            self.fail("row says variable_vb={} but the buckets have {} and {} vbuckets".format(
                expected, self.src_vbs, self.dest_vbs))

    def _num_vbuckets(self, cluster, timeout=120):
        rest = RestConnection(cluster.get_master_node())
        end = time.time() + timeout
        while True:
            info = rest.get_bucket_json(self.bucket)
            vb_map = (info.get("vBucketServerMap") or {}).get("vBucketMap") or []
            count = info.get("numVBuckets") or len(vb_map)
            if count:
                return int(count)
            if time.time() > end:
                self.fail("no vbucket map for {} on {}".format(self.bucket, cluster.get_name()))
            time.sleep(5)

    # ---- namespaces and replication --------------------------------------------

    def _target(self, coll):
        if self.mapping == "implicit":
            return SRC_SCOPE, coll
        return EXPLICIT_TARGETS[coll]

    def _pair(self, coll):
        return "{}.{}".format(SRC_SCOPE, coll), "{}.{}".format(*self._target(coll))

    def _create_target(self, coll):
        scope, tcoll = self._target(coll)
        self.dest_rest.create_scope(self.bucket, scope)
        self.dest_rest.create_collection(self.bucket, scope, tcoll)
        self.assertIsNotNone(self.dest_rest.get_collection_uid(self.bucket, scope, tcoll),
                             "target {}.{} not created".format(scope, tcoll))

    def _drop_target(self, coll):
        scope, tcoll = self._target(coll)
        self.assertTrue(self.dest_rest.delete_collection(self.bucket, scope, tcoll),
                        "drop of target {}.{} failed".format(scope, tcoll))

    def _setup_replication(self, collections, missing=()):
        """Source S1.<collections>; every target but `missing` created; XMEM replication.

        Nothing is loaded yet, so no mutation is routed (and no broken mapping
        recorded) before the explicit rules are in place.
        """
        self.collections = tuple(collections)
        self.src_rest.create_scope(self.bucket, SRC_SCOPE)
        self.src_rest.create_collection(self.bucket, SRC_SCOPE, list(collections))
        for coll in collections:
            if coll not in missing:
                self._create_target(coll)
        self.setup_xdcr()
        self.repl = self.src_cluster.get_remote_clusters()[0].get_replications()[0]
        self.repl_id = self.repl.get_repl_id()
        if self.mapping == "explicit":
            self._set_rules(collections)
        self.log.info("replication {} ({} mapping), targets missing: {}".format(
            self.repl_id, self.mapping, [self._pair(c)[1] for c in missing]))

    def _set_rules(self, collections):
        """Explicit mapping rules for exactly these collections."""
        rules = {"{}.{}".format(SRC_SCOPE, c): "{}.{}".format(*self._target(c)) for c in collections}
        self.src_rest.set_xdcr_params(self.bucket, self.bucket, {
            "collectionsExplicitMapping": "true", "colMappingRules": json.dumps(rules)})

    def _set_repl_settings(self, settings):
        self.src_rest.set_xdcr_params(self.bucket, self.bucket, settings)

    # ---- data --------------------------------------------------------------------

    def _load(self, coll, count, size=256):
        """Write `count` new `size`-byte docs into source S1.<coll> with pillowfight on the
        source master."""
        self._batch += 1
        prefix = "cbse23458-{}-{}-".format(coll, self._batch)
        before = self._count(self.src_cluster, SRC_SCOPE, coll)
        cmd = ("{pf} -U couchbase://localhost/{bucket} -u {user} -P {pw} "
               "--collection {scope}.{coll} --key-prefix {prefix} --num-items {n} "
               "--populate-only --batch-size 100 --min-size {size} --max-size {size} --num-threads 1").format(
            pf=PILLOWFIGHT, bucket=self.bucket, user=shlex.quote(self.src_master.rest_username),
            pw=shlex.quote(self.src_master.rest_password), scope=SRC_SCOPE, coll=coll,
            prefix=prefix, n=count, size=size)
        shell = RemoteMachineShellConnection(self.src_master)
        try:
            out, err, rc = shell.execute_command(cmd, debug=False, get_exit_code=True)
        finally:
            shell.disconnect()
        self.assertEqual(rc, 0, "pillowfight into {}.{} exited {}: {} {}".format(
            SRC_SCOPE, coll, rc, out[-5:], err[-5:]))
        self._wait_count(self.src_cluster, SRC_SCOPE, coll, before + count, 120,
                         "source {}.{} after writing {} docs".format(SRC_SCOPE, coll, count))

    def _load_together(self, colls, count, size):
        """Write `count` docs into each of `colls` at the same time, so their seqnos
        interleave in every vbucket (one backfill task then streams both)."""
        self._batch += 1
        before = {c: self._count(self.src_cluster, SRC_SCOPE, c) for c in colls}
        one = ("{pf} -U couchbase://localhost/{bucket} -u {user} -P {pw} --collection {scope}.{coll} "
               "--key-prefix cbse23458-{coll}-{batch}- --num-items {n} --populate-only "
               "--batch-size 100 --min-size {size} --max-size {size} --num-threads 1")
        cmd = " & ".join(one.format(
            pf=PILLOWFIGHT, bucket=self.bucket, user=shlex.quote(self.src_master.rest_username),
            pw=shlex.quote(self.src_master.rest_password), scope=SRC_SCOPE, coll=c,
            batch=self._batch, n=count, size=size) for c in colls) + " & wait"
        shell = RemoteMachineShellConnection(self.src_master)
        try:
            shell.execute_command(cmd, debug=False, timeout=1800)
        finally:
            shell.disconnect()
        for c in colls:
            self._wait_count(self.src_cluster, SRC_SCOPE, c, before[c] + count, 300,
                             "source {}.{} after writing {} docs".format(SRC_SCOPE, c, count))

    def _count(self, cluster, scope, coll, attempts=6):
        rest = RestConnection(cluster.get_master_node())
        if rest.get_collection_uid(self.bucket, scope, coll) is None:
            return 0
        for attempt in range(1, attempts + 1):
            try:
                return CollectionsStats(cluster.get_master_node()).get_collection_item_count_cumulative(
                    self.bucket, scope, coll, cluster.get_nodes())
            except (AttributeError, TypeError, IndexError) as e:
                # a node's collection stats can lag a just-created collection
                if attempt == attempts:
                    raise
                self.log.info("no collection stats for {}.{} on every node yet ({}), "
                              "retrying".format(scope, coll, e))
                time.sleep(5)

    def _target_count(self, coll):
        return self._count(self.dest_cluster, *self._target(coll))

    def _wait_count(self, cluster, scope, coll, at_least, timeout, what):
        end = time.time() + timeout
        while True:
            count = self._count(cluster, scope, coll)
            if count >= at_least:
                return count
            if time.time() > end:
                self.fail("{}: {} docs, expected at least {} within {}s".format(
                    what, count, at_least, timeout))
            time.sleep(5)

    def _settled_target_count(self, coll, quiet_secs=30, timeout=600):
        """Target count once it has not moved for quiet_secs."""
        end = time.time() + timeout
        last, since = self._target_count(coll), time.time()
        while time.time() < end:
            time.sleep(10)
            count = self._target_count(coll)
            if count != last:
                last, since = count, time.time()
            elif time.time() - since >= quiet_secs:
                return count
        self.fail("target count of {} still moving after {}s".format(self._pair(coll)[1], timeout))

    def _assert_converged(self, coll, timeout=None):
        """Source and target hold the same number of docs (strict parity)."""
        src = self._count(self.src_cluster, SRC_SCOPE, coll)
        end = time.time() + (timeout or self.heal_timeout)
        while True:
            dst = self._target_count(coll)
            if dst == src or time.time() > end:
                break
            time.sleep(10)
        self.assertEqual(src, dst, "{} -> {}: source has {} docs, target {}".format(
            *self._pair(coll), src, dst))

    def _assert_new_docs_replicate(self, coll):
        """Docs written to `coll` from now on reach the target, all of them.

        The target count is read once it has settled, so docs re-streamed from
        before the procedure are not mistaken for new ones.
        """
        before = self._settled_target_count(coll)
        self._load(coll, self.post_items)
        after = self._wait_count(self.dest_cluster, *self._target(coll), before + self.post_items,
                                 self.heal_timeout, "{} after the procedure".format(self._pair(coll)))
        self.log.info("{}: {} new docs replicated (target {} -> {})".format(
            self._pair(coll), self.post_items, before, after))

    def _log_gap(self, coll, written_while_broken):
        """Docs on the source that never reached the target.

        Patching a genuine broken mapping drops the record XDCR would backfill
        from (the doc's CAUTION), so docs written while the mapping was broken
        stay missing unless they are mutated again. Logged, and bounded.
        """
        gap = self._count(self.src_cluster, SRC_SCOPE, coll) - self._target_count(coll)
        self.log.warning("CBSE-23458 data gap: {} of the {} docs written to {} while its mapping "
                         "was broken are not on the target".format(gap, written_while_broken,
                                                                   self._pair(coll)[0]))
        self.assertGreaterEqual(gap, 0, "target holds more docs than the source")
        self.assertLessEqual(gap, written_while_broken,
                             "{} docs missing on the target, more than were written while the "
                             "mapping was broken ({})".format(gap, written_while_broken))
        return gap

    # ---- live broken-mapping state ------------------------------------------------

    def _ui_messages(self):
        """The replication's error texts in /pools/default/tasks, which the UI shows."""
        messages = []
        for task in self.src_rest.get_replications():
            if task.get("id") == self.repl_id:
                messages.extend(bmu.task_error_messages(task))
        return messages

    def _ui_pairs(self):
        return bmu.parse_broken_mappings(self._ui_messages())

    def _replication_infos(self, node):
        """goxdcr's per-pipeline state on `node`. It listens on 127.0.0.1:9998 only and
        ns_server does not proxy this path, hence ssh + curl (-f: an HTTP error is an error)."""
        cmd = "curl -sSf -u {}:{} http://127.0.0.1:9998/pools/default/replicationInfos".format(
            shlex.quote(node.rest_username), shlex.quote(node.rest_password))
        shell = RemoteMachineShellConnection(node)
        try:
            out, err, rc = shell.execute_command(cmd, debug=False, get_exit_code=True)
        finally:
            shell.disconnect()
        self.assertEqual(rc, 0, "replicationInfos on {} failed (curl exit {}): {}".format(
            node.ip, rc, err))
        try:
            return json.loads("".join(out) or "[]")
        except ValueError:
            self.fail("replicationInfos on {} is not JSON: {}".format(node.ip, "".join(out)[:300]))

    def _live_pairs(self):
        """{"ui", "main"} -> broken pairs; main is unioned over source nodes.

        There is no live view of the backfill pipeline's broken map: goxdcr attaches
        broken-map events to the main pipeline's replicationInfos entry only
        (replication_manager.go populateReplInfos), so a break on the backfill pipeline
        is visible only once persisted under ckpt/backfill_<replId>/ (_persisted).
        """
        state = {"ui": self._ui_pairs(), "main": set()}
        for node in self.src_cluster.get_nodes():
            state["main"] |= bmu.parse_replication_infos(self._replication_infos(node), self.repl_id)
        return state

    def _wait_live(self, pairs, present, timeout, where=("ui", "main"), interval=5):
        """Wait until every pair is (present) or no pair is (absent) in each of `where`."""
        end = time.time() + timeout
        while True:
            state = self._live_pairs()
            if present:
                done = all(set(pairs) <= state[w] for w in where)
            else:
                done = not any(set(pairs) & state[w] for w in where)
            if done:
                return state
            if time.time() > end:
                ui = " | UI error texts: {}".format(self._ui_messages()) if "ui" in where else ""
                self.fail("{} {} in {} within {}s; live state {}{}".format(
                    sorted(pairs), "not reported" if present else "still reported",
                    where, timeout, state, ui))
            time.sleep(interval)

    def _hold_absent(self, pairs, secs):
        """No pair comes back on the UI or the main pipeline for `secs`."""
        end = time.time() + secs
        while time.time() < end:
            state = self._live_pairs()
            for where, live in state.items():
                self.assertFalse(set(pairs) & live, "{} broken again in {}: {}".format(
                    sorted(set(pairs) & live), where, state))
            time.sleep(10)

    def _hold_present(self, pairs, secs, where=("ui", "main")):
        end = time.time() + secs
        while time.time() < end:
            state = self._live_pairs()
            for w in where:
                self.assertTrue(set(pairs) <= state[w], "{} cleared from {} on its own: {}".format(
                    sorted(pairs), w, state))
            time.sleep(10)

    # ---- persisted broken-mapping state (metakv, node-local) ----------------------------

    def _metakv_get(self, node, path):
        rest = RestConnection(node)
        status, content, header = rest._http_request(rest.baseUrl + "_metakv/" + path)
        if status:
            return content.decode("utf-8") if isinstance(content, bytes) else content
        if header and str(header.get("status")) == "404":
            return None
        self.fail("GET /_metakv/{} on {} failed: {}".format(path, node.ip, content))

    def _pipeline_keys(self):
        return (("main", self.repl_id), ("backfill", bmu.BACKFILL_PREFIX + self.repl_id))

    def _persisted(self):
        """{ip: {pipeline: {"shas", "ckpt_shas", "unverified", "dangling", "decoded"}}}
        on every source node (see bmu.checkpoint_broken_shas)."""
        state = {}
        for node in self.src_cluster.get_nodes():
            state[node.ip] = {}
            for pipeline, key in self._pipeline_keys():
                doc = bmu.decode_broken_mappings_doc(
                    self._metakv_get(node, "ckpt/{}/brokenMappings".format(key)))
                children = bmu.parse_metakv_listing(self._metakv_get(node, "ckpt/{}/".format(key)))
                ckpt_shas, unverified, dangling = bmu.checkpoint_broken_shas(children, self.snappy)
                state[node.ip][pipeline] = {
                    "shas": bmu.broken_map_shas(doc),
                    "ckpt_shas": ckpt_shas,
                    "unverified": unverified,
                    "dangling": dangling,
                    "global_info": bmu.global_info_records(children),
                    "decoded": bmu.decoded_children(children),
                }
        return state

    def _summary(self, persisted):
        return {ip: {p: (len(s["shas"]), len(s["ckpt_shas"]), s["global_info"])
                     for p, s in pipelines.items()}
                for ip, pipelines in persisted.items()}

    def _assert_persisted_broken(self, pipeline="main"):
        persisted = self._persisted()
        self.log.info("persisted (broken-map records, checkpoint docs pointing at one, globalInfo "
                      "records or None) per node: {}".format(self._summary(persisted)))
        nodes = [ip for ip, p in persisted.items() if p[pipeline]["shas"]]
        self.assertTrue(nodes, "no source node persisted a {} broken map".format(pipeline))
        # a false positive is raised only where XMEM met the lagging target node
        if self.expect_on_every_node and self.induce == "genuine":
            self.assertEqual(sorted(nodes), sorted(persisted),
                             "{} broken map persisted on {} only, not on every source "
                             "node".format(pipeline, nodes))
        # goxdcr garbage-collects a record no checkpoint points at, so a node with a
        # record has checkpoint docs that reference it; seeing them here is what makes
        # the "no checkpoint doc points at a broken map" check after patch a real one.
        # A false positive is the exception: on 7.6.11-8495 a node that never raised it
        # held the record with no checkpoint pointing at it (measured 2026-10-21), so
        # there one referencing node is enough.
        unreferenced = [ip for ip in nodes if not persisted[ip][pipeline]["ckpt_shas"]]
        if self.induce == "false_positive" and len(unreferenced) < len(nodes):
            for ip in unreferenced:
                self.log.info("{} {}: a broken-map record no checkpoint references".format(
                    ip, pipeline))
            unreferenced = []
        for ip in unreferenced:
            self._unverifiable(ip, pipeline, persisted[ip][pipeline],
                               "no checkpoint references its broken map")
        return persisted

    def _log_backfill_references(self, persisted):
        """A backfill broken-map record need not be referenced: on 8.5.0-1308 a variable-VB
        backfill held one with no checkpoint pointing at it, directly or through a
        GlobalTimestamp (cbvm, 2026-10-20). Logged, not judged; the patch removes the
        record either way."""
        for ip, pipelines in persisted.items():
            state = pipelines["backfill"]
            if state["shas"]:
                self.log.info("{} backfill: {} broken-map record(s), referenced by {}".format(
                    ip, len(state["shas"]), sorted(state["ckpt_shas"]) or "no checkpoint"))

    def _unverifiable(self, ip, pipeline, state, why):
        """Every checkpoint doc and GlobalTimestamp can be read (bmu.snappy_decompress),
        so an unreadable one means the format is not what the suite expects: fail
        rather than check nothing."""
        self.fail("{} {}: {} (not verified: {})".format(ip, pipeline, why, state["unverified"]))

    def _assert_persisted_clean(self):
        persisted = self._persisted()
        for ip, pipelines in persisted.items():
            for pipeline, state in pipelines.items():
                self.assertFalse(state["shas"], "{} {}: brokenMappings still holds {}".format(
                    ip, pipeline, state["shas"]))
                self.assertFalse(state["ckpt_shas"], "{} {}: checkpoints still point at a "
                                 "broken map: {}".format(ip, pipeline, state["ckpt_shas"]))
                self.assertFalse(state["dangling"], "{} {}: checkpoints name GlobalTimestamps the "
                                 "globalInfo doc lacks: {}".format(ip, pipeline, state["dangling"]))
                if state["unverified"]:
                    self._unverifiable(ip, pipeline, state, "checkpoints not all readable")
        return persisted

    # ---- goxdcr.log --------------------------------------------------------------------

    def _log_path(self, node):
        return NodeHelper.get_goxdcr_log_dir(node) + "/goxdcr.log"

    def _log_lines(self, shell, path, attempts=5):
        # goxdcr.log is briefly absent while it rotates (8.0.1, 2026-10-23)
        for attempt in range(attempts):
            out, err = shell.execute_command("wc -l < " + path, debug=False)
            try:
                return int(out[0].strip())
            except (IndexError, ValueError):
                if attempt + 1 < attempts:
                    time.sleep(2)
        self.fail("cannot count lines of {}: {} {}".format(path, out, err))

    def _log_marks(self):
        marks = {}
        for node in self.src_cluster.get_nodes():
            shell = RemoteMachineShellConnection(node)
            try:
                marks[node.ip] = self._log_lines(shell, self._log_path(node))
            finally:
                shell.disconnect()
        return marks

    def _log_since(self, node, mark):
        """goxdcr.log lines written since `mark` that these tests read (with their continuations)."""
        path = self._log_path(node)
        pattern = "|".join((bmu.CKPT_STOP_START, bmu.CKPT_STOP_DONE, bmu.CKPT_STOP_TIMEOUT,
                            bmu.CKPT_STOP_SKIPPED, "has been stopped", "Loaded brokenMap"))
        shell = RemoteMachineShellConnection(node)
        try:
            start = bmu.tail_start_line(mark, self._log_lines(shell, path))
            source = "tail -n +{} {}".format(start, path)
            if start == 1:
                # rotated since the mark: the rest of the old file is the newest rotated one
                source = "{{ tail -n +{} \"$(ls -t {p}.* 2>/dev/null | grep -v '[.]gz$' | head -1)\" " \
                         "2>/dev/null; cat {p}; }}".format(mark + 1, p=path)
            # -a: goxdcr.log holds binary bytes, and without it grep prints "binary file
            # matches" in place of every line after the first of them
            out, _ = shell.execute_command("{} | grep -a -E -A 40 '{}'".format(source, pattern),
                                           debug=False)
        finally:
            shell.disconnect()
        return "\n".join(out)

    def _loaded_pairs_since(self, marks, timeout=90):
        """({ip: "Loaded brokenMap" blocks}, pairs they list) since `marks`.

        Waits until every source node has logged one, or the timeout: after a
        resume the nodes load their checkpoints seconds apart, and the first
        node's block says nothing about the others.
        """
        end = time.time() + timeout
        while True:
            blocks, pairs = {}, set()
            for node in self.src_cluster.get_nodes():
                node_blocks, node_pairs = bmu.loaded_broken_map_pairs(
                    self._log_since(node, marks[node.ip]))
                blocks[node.ip] = node_blocks
                pairs |= node_pairs
            if all(blocks.values()) or time.time() > end:
                return blocks, pairs
            time.sleep(5)

    # ---- process control -----------------------------------------------------------------

    def _goxdcr_ready(self, node):
        rest = RestConnection(node)
        status, _, _ = rest._http_request(rest.baseUrl + "xdcr/internalSettings")
        return status

    def _drop_stale_ckpt_docs(self):
        """Delete what deleted replications left under ckpt/ in each source node's metakv.

        Called before this row creates its replication, so with no replication on the
        source every ckpt/ doc is a leftover (see fresh_goxdcr).
        """
        if self.src_rest.get_replications():
            self.fail("a replication already exists on the source; its ckpt/ docs are not leftovers")
        for node in self.src_cluster.get_nodes():
            rest = RestConnection(node)
            paths = [doc["path"].lstrip("/")
                     for doc in bmu.parse_metakv_listing(self._metakv_get(node, "ckpt/"))]
            for path in paths:
                status, content, _ = rest._http_request(rest.baseUrl + "_metakv/" + path, "DELETE")
                self.assertTrue(status, "DELETE /_metakv/{} on {} failed: {}".format(path, node.ip, content))
            if paths:
                self.log.warning("{}: deleted {} ckpt/ docs left by deleted replications".format(
                    node.ip, len(paths)))

    def _restart_goxdcr(self, nodes=None, timeout=180):
        """Doc step 7: `pkill -x goxdcr` on each node; ns_server starts it again."""
        nodes = nodes or self.src_cluster.get_nodes()
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            try:
                old, _ = shell.execute_command("pgrep -x goxdcr", debug=False)
                _, err, rc = shell.execute_command("pkill -x goxdcr", get_exit_code=True)
                self.assertEqual(rc, 0, "pkill goxdcr on {} exited {}: {}".format(node.ip, rc, err))
                end = time.time() + timeout
                while True:
                    new, _ = shell.execute_command("pgrep -x goxdcr", debug=False)
                    if new and set(new) != set(old):
                        break
                    if time.time() > end:
                        self.fail("goxdcr on {} not restarted within {}s".format(node.ip, timeout))
                    time.sleep(2)
            finally:
                shell.disconnect()
        for node in nodes:
            end = time.time() + timeout
            while not self._goxdcr_ready(node):
                if time.time() > end:
                    self.fail("goxdcr on {} not answering {}s after restart".format(node.ip, timeout))
                time.sleep(3)

    def _set_manifest_refresh(self, secs):
        """goxdcr's target manifest refresh interval. An internal setting: cluster-wide,
        and writing it restarts goxdcr on every node, so wait for it to answer again."""
        # through ns_server's 8091 proxy, not curl to 9998: the base helpers log the
        # Authorization header they build into the command line
        rest = RestConnection(self.src_master)
        api = rest.baseUrl + "xdcr/internalSettings"
        status, content, _ = rest._http_request(api, "POST", urllib.parse.urlencode({MANIFEST_REFRESH: secs}))
        if not status:
            # the write restarts goxdcr, and ns_server's proxy can answer 500 while the
            # setting still lands; the read-back below decides
            self.log.warning("POST {}={} answered {}; reading it back".format(
                MANIFEST_REFRESH, secs, content))
        end = time.time() + 180
        while True:
            status, content, _ = rest._http_request(api)
            try:
                if status and str(json.loads(content).get(MANIFEST_REFRESH)) == str(secs):
                    break
            except ValueError:
                pass
            if time.time() > end:
                self.fail("{} did not become {} within 180s".format(MANIFEST_REFRESH, secs))
            time.sleep(5)
        self.sleep(15, "goxdcr restarts after an internal-settings change")
        for node in self.src_cluster.get_nodes():
            end = time.time() + 180
            while not self._goxdcr_ready(node):
                if time.time() > end:
                    self.fail("goxdcr on {} not answering after {}={}".format(
                        node.ip, MANIFEST_REFRESH, secs))
                time.sleep(3)
        self.log.info("{}={}".format(MANIFEST_REFRESH, secs))

    def _backfill_spec_docs(self):
        """Docs under ckpt/backfill/<replId>/ on any source node: there exactly while a
        backfill is pending or running. (replicationInfos lists a backfill_<replId> entry
        whether or not one exists, so it cannot say.)"""
        return sum(len([k for k in bmu.parse_metakv_listing(
            self._metakv_get(node, "ckpt/backfill/{}/".format(self.repl_id))) if k.get("value")])
            for node in self.src_cluster.get_nodes())

    def _wait_backfill_running(self, colls):
        end = time.time() + self.broken_timeout
        while True:
            spec = self._backfill_spec_docs()
            counts = {c: self._target_count(c) for c in colls}
            if spec and all(counts.values()):
                self.log.info("backfill under way: {} spec docs, target counts {}".format(spec, counts))
                return counts
            if time.time() > end:
                self.fail("no backfill under way for {} within {}s: {} spec docs, target counts "
                          "{}".format(colls, self.broken_timeout, spec, counts))
            time.sleep(3)

    def _pause_and_wait_checkpoint(self):
        """Doc step 3: pause once, then wait on every source node until goxdcr has
        stopped the replication's pipelines with no checkpoint still in flight.

        The doc says to wait for each pipeline's checkpoint-before-stop line. On
        8.5.0-1308 a pause can stop a node's pipeline without logging one at all
        (cbvm, 2026-10-18: nodes 1 and 2 stopped 58s after the pause, node 3
        checkpointed at once), so a node that stops without one is a warning,
        and the stop itself is what is waited for.
        """
        marks = self._log_marks()
        self.repl.pause(verify=True)
        pending = {node.ip: node for node in self.src_cluster.get_nodes()}
        state = {}
        end = time.time() + self.ckpt_timeout
        while pending:
            for ip, node in list(pending.items()):
                state[ip] = bmu.pipeline_stop_state(self._log_since(node, marks[ip]), self.repl_id)
                self.assertFalse(state[ip]["timed_out"], "{}: checkpointing before stop timed out; "
                                 "the doc says not to run the script then".format(ip))
                in_flight = state[ip]["started"] - state[ip]["completed"]
                if state[ip]["stopped"] and in_flight <= 0:
                    if not state[ip]["started"]:
                        self.log.warning("{}: the pause stopped the pipeline with no "
                                         "checkpoint-before-stop line, which the doc's step 3 "
                                         "waits for: {}".format(ip, state[ip]))
                    else:
                        self.log.info("{}: stopped after checkpointing: {}".format(ip, state[ip]))
                    del pending[ip]
            if pending and time.time() > end:
                self.fail("pipeline not stopped with its checkpoints done on {} within {}s of the "
                          "pause: {}".format(sorted(pending), self.ckpt_timeout,
                                             {ip: state.get(ip) for ip in pending}))
            if pending:
                time.sleep(5)
        self._wait_task_status("paused")

    def _wait_task_status(self, want, timeout=120):
        """Doc step 3: "Ensure that UI says that the replication is paused". The script
        reads the same /pools/default/tasks status, which ns_server refreshes from
        goxdcr with a lag: a patch 3s after every pipeline had stopped still read
        status=running and refused (8.5.0-1308, cbvm 2026-10-20)."""
        end = time.time() + timeout
        while True:
            status = [t.get("status") for t in self.src_rest.get_replications()
                      if t.get("id") == self.repl_id]
            if status and str(status[0]).lower() == want:
                return
            if time.time() > end:
                self.fail("replication task status is {} {}s after the pause, not {}".format(
                    status, timeout, want))
            time.sleep(2)

    def _resume(self):
        marks = self._log_marks()
        self.repl.resume(verify=True)
        return marks

    def _reload(self):
        """Pause (which checkpoints), resume. Returns goxdcr.log marks from before the resume."""
        self._pause_and_wait_checkpoint()
        return self._resume()

    # ---- the script ----------------------------------------------------------------

    def _push_script(self):
        """Doc step 2: a fresh directory on every source KV node holding the script."""
        if not os.path.isfile(self.script):
            self.fail("brokenmap_script={} not found; save broken_mappings.py from goxdcr "
                      "change 254675 there".format(self.script))
        with open(self.script, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        self.workdir = "/tmp/cbse23458-{}".format(int(time.time()))
        for node in self.src_cluster.get_nodes():
            shell = RemoteMachineShellConnection(node)
            try:
                self.node_python[node.ip] = self._pick_python(shell, node)
                shell.execute_command("mkdir -p " + self.workdir)
                shell.copy_file_local_to_remote(self.script, "{}/{}".format(self.workdir, SCRIPT_NAME))
                # copy_file_local_to_remote only logs a failed copy
                out, _ = shell.execute_command("sha256sum {}/{}".format(self.workdir, SCRIPT_NAME))
                self.assertTrue(out and out[0].split()[0] == digest,
                                "copy of the script to {} failed".format(node.ip))
                if self._varvb():
                    self._ensure_snappy(shell, node)
            finally:
                shell.disconnect()
        self.log.info("{} (sha256 {}) in {} on every source node".format(
            SCRIPT_NAME, digest[:12], self.workdir))

    def _varvb(self):
        """Whether the script runs with CB_ENABLE_VARVB=true (see enable_varvb)."""
        return bool(self.enable_varvb) or self.variable_vb

    def _pick_python(self, shell, node):
        """The interpreter the script runs with on `node` (doc pre-check 1: "The node
        must have Python 3"). node_python=auto takes python3 from PATH, else the one
        Couchbase Server bundles, which on 8.5.0-1308 is 3.14 with python-snappy
        already installed (measured on cbvm, whose nodes have no python3)."""
        candidates = [self.node_python_param]
        if self.node_python_param == "auto":
            candidates = ["python3", BUNDLED_PYTHON]
        for python in candidates:
            out, err, rc = shell.execute_command("{} --version".format(python), get_exit_code=True)
            if rc == 0:
                if python == BUNDLED_PYTHON:
                    self.log.warning("{} has no python3 on PATH: running the script with "
                                     "Couchbase's bundled {} ({}), not what the doc "
                                     "says".format(node.ip, python, (out or err)[0]))
                else:
                    self.log.info("{}: {} is {}".format(node.ip, python, (out or err)[0]))
                return python
        self.fail("{} has no Python 3 for the script (tried {})".format(node.ip, candidates))

    def _ensure_snappy(self, shell, node):
        """Doc pre-check 5: variable-VB replications need python-snappy on the node."""
        check = "{} -c 'import snappy'".format(self.node_python[node.ip])
        _, _, rc = shell.execute_command(check, get_exit_code=True)
        if rc and self.install_snappy:
            shell.execute_command("apt-get install -y python3-snappy || dnf install -y python3-snappy "
                                  "|| pip3 install python-snappy", get_exit_code=True)
            _, _, rc = shell.execute_command(check, get_exit_code=True)
        self.assertEqual(rc, 0, "{} cannot import snappy; variable-VB replications need "
                         "python-snappy (install_snappy={})".format(node.ip, self.install_snappy))

    def _run_script(self, node, command, varvb=None, workdir=None):
        """python3 ./broken_mappings.py 127.0.0.1 <port> <command> <replid>, from the work dir.

        Returns (output, exit code). The password goes in CB_PASSWORD, so the
        command line is not logged.
        """
        varvb = self._varvb() if varvb is None else varvb
        env = "CB_PASSWORD={}".format(shlex.quote(node.rest_password))
        if varvb:
            env += " CB_ENABLE_VARVB=true"
        user = "" if node.rest_username == "Administrator" else \
            "-u {} ".format(shlex.quote(node.rest_username))
        target = "" if command == "list-repl" else " " + shlex.quote(self.repl_id)
        cmd = "cd {wd} && {env} {py} -u ./{script} {user}127.0.0.1 {port} {command}{target} 2>&1".format(
            wd=workdir or self.workdir, env=env, py=self.node_python[node.ip], script=SCRIPT_NAME,
            user=user, port=node.port, command=command, target=target)
        shell = RemoteMachineShellConnection(node)
        try:
            out, _, rc = shell.execute_command(cmd, debug=False, get_exit_code=True)
        finally:
            shell.disconnect()
        text = "\n".join(out)
        self.log.info("{} {}{} on {} exited {}:\n{}".format(
            SCRIPT_NAME, command, " (CB_ENABLE_VARVB)" if varvb else "", node.ip, rc, text))
        return text, rc

    def _run_ok(self, node, command):
        out, rc = self._run_script(node, command)
        self.assertEqual(rc, 0, "{} {} on {} exited {}".format(SCRIPT_NAME, command, node.ip, rc))
        return out

    def _assert_refused(self, what, out, rc, *texts):
        """The script refuses with "Error: ..." on stderr and exit code 1."""
        self.assertEqual(rc, 1, "{} was not refused (exit {}):\n{}".format(what, rc, out))
        for text in texts:
            self.assertIn(text, out, "{} refused without saying '{}':\n{}".format(what, text, out))

    def _backup_digest(self, node, workdir=None):
        """sha256 of every file under the work dir's data/ (the prep backup)."""
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(
                "cd {} && find data -type f 2>/dev/null | sort | xargs -r sha256sum".format(
                    workdir or self.workdir))
        finally:
            shell.disconnect()
        return "\n".join(out)

    def _view_shows_patched(self, out):
        return '"NsMappingRecords":null' in out.replace(" ", "")

    def _assert_view(self, node, out, shas, patched):
        """view prints each pipeline's brokenMappings doc decoded, Sha256Digest included,
        so it must name exactly the records metakv holds: all of them before the patch
        (and after a restore), none after it."""
        shown = {sha for sha in shas if sha in out}
        if patched:
            self.assertFalse(shown, "view on {} after patch still shows {}".format(node.ip, shown))
            self.assertTrue(self._view_shows_patched(out), "view on {} after patch does not show "
                            "NsMappingRecords null".format(node.ip))
        else:
            self.assertEqual(shown, set(shas), "view on {} shows {} of the broken-map records "
                             "metakv holds ({})".format(node.ip, sorted(shown), sorted(shas)))

    def _node_shas(self, persisted, ip):
        return set().union(*(state["shas"] for state in persisted[ip].values()))

    def _prep_and_patch(self, persisted_before):
        """Doc steps 4-6 on every source node: view, prep, patch, view again."""
        for node in self.src_cluster.get_nodes():
            shas = self._node_shas(persisted_before, node.ip)
            out = self._run_ok(node, "view")
            self.assertIn("status=paused", out, "view on {} did not see the pause".format(node.ip))
            self._assert_view(node, out, shas, patched=False)
            self._run_ok(node, "prep")
            self._run_ok(node, "patch")
            if shas:
                self._assert_view(node, self._run_ok(node, "view"), shas, patched=True)
        return self._assert_persisted_clean()

    def _apply_procedure(self, create=(), expect_backfill=False, before_restart=None):
        """The whole CBSE-23458 procedure. Returns goxdcr.log marks from before the restart.

        create: collections whose missing target is created while still paused
        (the test plan's "create the missing scopes/collections before resuming").
        expect_backfill: the backfill pipeline must have persisted a broken map too.
        """
        self._push_script()
        self._pause_and_wait_checkpoint()
        persisted = self._assert_persisted_broken("main")
        if expect_backfill:
            self.assertTrue(any(p["backfill"]["shas"] for p in persisted.values()),
                            "no source node persisted a backfill broken map: {}".format(
                                self._summary(persisted)))
            self._log_backfill_references(persisted)
        self._prep_and_patch(persisted)
        for coll in create:
            self._create_target(coll)
        if before_restart:
            before_restart()
        marks = self._log_marks()
        self._restart_goxdcr()
        self.repl.resume(verify=True)
        return marks

    def _assert_not_reloaded(self, pairs, marks, when):
        blocks, loaded = self._loaded_pairs_since(marks)
        silent = sorted(ip for ip, count in blocks.items() if not count)
        if silent:
            self.log.warning("no 'Loaded brokenMap' line on {} since {}; only the live state is "
                             "checked there".format(silent, when))
        self.assertFalse(set(pairs) & loaded, "goxdcr loaded a patched broken map {}: {}".format(
            when, sorted(set(pairs) & loaded)))

    def _assert_healthy_after(self, pairs, marks, written_while_broken):
        """After the procedure: the pairs stay gone across a further reload, new docs
        replicate, the healthy collection is untouched, and any gap is bounded.

        written_while_broken: {collection: source docs it held when the procedure began}.
        """
        self._assert_not_reloaded(pairs, marks, "the goxdcr restart")
        self._hold_absent(pairs, self.hold_secs)
        for coll in written_while_broken:
            self._assert_new_docs_replicate(coll)
        self._load(HEALTHY, self.post_items)
        self._assert_converged(HEALTHY)
        for coll, written in written_while_broken.items():
            self._log_gap(coll, written)
        marks = self._reload()
        self._assert_not_reloaded(pairs, marks, "a further pause/resume")
        self._hold_absent(pairs, 60)

    def _written(self, collections):
        return {coll: self._count(self.src_cluster, SRC_SCOPE, coll) for coll in collections}

    # ---- inducing a broken mapping -------------------------------------------------------

    def _induce_genuine(self, colls):
        """Write to collections whose target is missing until the UI reports them."""
        for coll in (HEALTHY,) + tuple(colls):
            self._load(coll, self.ns_items)
        self._wait_live([self._pair(c) for c in colls], True, self.broken_timeout)
        self._assert_converged(HEALTHY)

    def _kill_memcached_nowait(self, node):
        # Not shell.kill_memcached(): it waits for the respawn, which closes the
        # window the false positive needs.
        shell = RemoteMachineShellConnection(node)
        try:
            _, err, rc = shell.execute_command("pkill -9 -x memcached", get_exit_code=True)
        finally:
            shell.disconnect()
        self.assertEqual(rc, 0, "pkill memcached on {} exited {}: {}".format(node.ip, rc, err))

    def _start_writer(self, coll, batch=200):
        stop, errors = threading.Event(), []

        def run():
            while not stop.is_set():
                try:
                    self._load(coll, batch)
                except Exception as e:
                    errors.append(e)
                    return

        thread = threading.Thread(target=run, name="writer-" + coll, daemon=True)
        thread.start()
        return thread, stop, errors

    def _stop_writer(self, writer):
        thread, stop, errors = writer
        stop.set()
        thread.join(180)
        self.assertFalse(thread.is_alive(), "collection writer did not stop")
        return errors

    def _induce_false_positive(self):
        """MB-73479 (goxdcr test 2j): kill -9 memcached on the target's second KV
        node, drop and recreate the target collection while that node is down,
        and keep writing. XMEM gets UNKNOWN_COLLECTION from the lagging node for
        a collection the cluster view XDCR reads already has."""
        if len(self.dest_cluster.get_nodes()) < 2:
            self.fail("induce=false_positive needs two target KV nodes")
        lagging = self.dest_cluster.get_nodes()[1]
        scope, tcoll = self._target(BROKEN)
        pair = self._pair(BROKEN)
        for attempt in range(1, self.induce_attempts + 1):
            if pair in self._ui_pairs():
                # left from an earlier round: it must clear, or this attempt proves nothing
                self._wait_live([pair], False, 120, where=("ui",))
            old_uid = self.dest_rest.get_collection_uid(self.bucket, scope, tcoll)
            self.log.info("[attempt {}] kill -9 memcached on {}, recreate {}.{}".format(
                attempt, lagging.ip, scope, tcoll))
            self._kill_memcached_nowait(lagging)
            self._drop_target(BROKEN)
            self._create_target(BROKEN)
            self.assertNotEqual(old_uid, self.dest_rest.get_collection_uid(self.bucket, scope, tcoll),
                                "the recreated target collection must get a new id")
            writer = self._start_writer(BROKEN)
            try:
                end, caught = time.time() + self.induce_poll_secs, False
                while not caught and time.time() < end:
                    caught = pair in self._ui_pairs()
                    time.sleep(2)
            finally:
                errors = self._stop_writer(writer)
            self.assertFalse(errors, "collection writer failed: {}".format(errors))
            NodeHelper.wait_warmup_completed([lagging], [self.bucket])
            if caught:
                self.assertIsNotNone(self.dest_rest.get_collection_uid(self.bucket, scope, tcoll))
                self.log.info("[attempt {}] false positive raised for {}".format(attempt, pair))
                return
            self.sleep(10, "let the target node settle before the next attempt")
        self.fail("no false positive in {} attempts: the manifest-lag window is a race (goxdcr "
                  "2j); re-run or raise induce_attempts".format(self.induce_attempts))

    def _induce_stuck_false_positive(self):
        """Induce, then reload until the reload re-raises the false positive and it
        stays: the stuck state CBSE-23458's customer was in."""
        pair = self._pair(BROKEN)
        for rnd in range(1, self.induce_rounds + 1):
            self._induce_false_positive()
            marks = self._reload()
            _, loaded = self._loaded_pairs_since(marks)
            if pair in loaded:
                self._hold_present([pair], 60, where=("ui",))
                self.log.info("round {}: false positive stuck after a reload".format(rnd))
                return
            self.log.warning("round {}: the reload did not load {} (loaded {}); the live "
                             "double-check won the race, retrying".format(rnd, pair, sorted(loaded)))
        self.fail("could not get a false positive stuck across a reload in {} rounds; on a "
                  "build with the MB-73479 fix it cannot be, use induce=genuine".format(
                      self.induce_rounds))

    # ---- tests -----------------------------------------------------------------------------

    def test_patch_clears_broken_mapping(self):
        """Test plan scenario 1: apply the procedure; the error goes and docs replicate after.

        induce=genuine (default): the target collection is missing and is created
        while paused, before the resume (test plan setup 3).
        induce=false_positive: MB-73479's stuck false positive; 8.0.x and 7.6.x only.
        """
        pair = self._pair(BROKEN)
        if self.induce == "genuine":
            self._setup_replication((HEALTHY, BROKEN), missing=(BROKEN,))
            self._induce_genuine((BROKEN,))
            create = (BROKEN,)
        else:
            self._setup_replication((HEALTHY, BROKEN))
            for coll in (HEALTHY, BROKEN):
                self._load(coll, self.ns_items)
                self._assert_converged(coll)
            self._induce_stuck_false_positive()
            create = ()
        written = self._written((BROKEN,))
        marks = self._apply_procedure(create=create)
        self._assert_healthy_after([pair], marks, written)

    def test_patch_same_and_different_then_create_missing(self):
        """Test plan scenario 2: genuine broken mappings to a same-named and a
        differently named target (explicit mapping: S1.col2 -> S1.col2, whose
        scope is missing too, and S1.col1 -> S3.col1), both on the UI; patch,
        create the missing scope and collections, and the cluster works normally."""
        broken = (BROKEN, SAME)
        pairs = [self._pair(c) for c in broken]
        self._setup_replication((HEALTHY,) + broken, missing=broken)
        for coll in (HEALTHY,) + broken:
            self._load(coll, self.ns_items)
        # goxdcr's own broken-map event must hold both; the UI must show a broken-mapping
        # error. Whether its text lists every pair is judged at the end, so the rest of
        # the scenario still runs: on 8.5.0-1308 the text kept only the first broken
        # mapping it raised (S1.col1) for 900s while the event held both (cbvm, 2026-10-18)
        self._wait_live(pairs, True, self.broken_timeout, where=("main",))
        ui_end = time.time() + self.broken_timeout
        while not set(pairs) <= self._ui_pairs() and time.time() < ui_end:
            time.sleep(5)
        on_ui = self._ui_pairs()
        self.assertTrue(on_ui & set(pairs), "no broken-mapping error on the UI: {}".format(
            self._ui_messages()))
        not_on_ui = sorted(set(pairs) - on_ui)
        if not_on_ui:
            self.log.warning("the UI does not list {} {}s after goxdcr reported them; UI error "
                             "texts: {}".format(not_on_ui, self.broken_timeout, self._ui_messages()))
        self._assert_converged(HEALTHY)
        written = self._written(broken)
        marks = self._apply_procedure(create=broken)
        self._assert_healthy_after(pairs, marks, written)
        self._restart_goxdcr(self.src_cluster.get_nodes() + self.dest_cluster.get_nodes())
        self._hold_absent(pairs, 60)
        for coll in (HEALTHY,) + broken:
            self._assert_new_docs_replicate(coll)
        self.assertFalse(not_on_ui, "the procedure worked, but the UI never listed {} as broken "
                         "while goxdcr's broken-map event did (test plan scenario 2: each should "
                         "be reported as an error on the UI)".format(not_on_ui))

    def test_restore_undoes_patch(self):
        """Test plan scenario 3: restore puts back exactly what patch changed, and
        goxdcr then treats the mapping as broken again -- including the backfill
        it owes once the target exists."""
        pair = self._pair(BROKEN)
        self._setup_replication((HEALTHY, BROKEN), missing=(BROKEN,))
        self._induce_genuine((BROKEN,))
        self._push_script()
        self._pause_and_wait_checkpoint()
        before = self._assert_persisted_broken()
        self._prep_and_patch(before)
        for node in self.src_cluster.get_nodes():
            self._run_ok(node, "restore")
        after = self._persisted()
        for ip in before:
            for pipeline in before[ip]:
                self.assertEqual(before[ip][pipeline]["decoded"], after[ip][pipeline]["decoded"],
                                 "{} {}: metakv after restore differs from before prep".format(
                                     ip, pipeline))
        for node in self.src_cluster.get_nodes():
            self._assert_view(node, self._run_ok(node, "view"), self._node_shas(before, node.ip),
                              patched=False)
        marks = self._log_marks()
        self._restart_goxdcr()
        self.repl.resume(verify=True)
        _, loaded = self._loaded_pairs_since(marks)
        self.assertIn(pair, loaded, "goxdcr did not reload the restored broken map")
        self._load(BROKEN, self.post_items)
        self._wait_live([pair], True, self.broken_timeout)
        self._create_target(BROKEN)
        self._wait_live([pair], False, self.heal_timeout, where=("ui", "main"))
        repaired = bmu.parse_repaired_mappings([e["text"] for e in self.src_rest.get_logs(200)])
        self.assertIn(pair, repaired, "no 'now repaired and replicating' UI log for {}".format(pair))
        # the restored broken map is what XDCR backfills from: nothing may be lost
        self._assert_converged(BROKEN)
        self._assert_converged(HEALTHY)

    def test_main_and_backfill_broken_patch(self):
        """Test plan scenario 4: the main and the backfill pipeline both hold a broken
        mapping; the procedure clears both on every node, and the backfill then
        finishes from its patched checkpoints.

        Recipe, measured on 8.5.0-1308 (cbvm probes, 2026-10-20) and read from goxdcr:
        - implicit mapping: under explicit mapping a dropped target becomes a mapping
          removal and goxdcr deletes the backfill ("... cleaned up");
        - ManifestRefreshTgtInterval=5: goxdcr notices a created target only on its
          next manifest fetch (60s by default), and a create/drop inside one window
          raises no backfill at all;
        - col1 and col3 loaded together and their targets created together, so one
          backfill streams both; col3's share keeps it running, held back by
          networkUsageLimit on uncompressed large docs (xdcrDev* knobs are dev-build
          only). A backfill of col1 alone finishes the moment col1's docs are
          discarded, and its checkpoints go with it;
        - collectionsOSOMode=false: under OSO a backfill vbucket's through-seqno does
          not move until the session ends, so no checkpoint record would reference
          the broken map and goxdcr would drop it;
        - pause, drop col1's target, resume: the backfill router records col1 as
          unroutable; new col1 writes break the main pipeline too. The backfill
          pipeline's break is not shown live (replicationInfos, UI), only persisted
          under ckpt/backfill_<replId>/, which the procedure's checks read.
        """
        if self.mapping != "implicit":
            self.fail("scenario 4 needs mapping=implicit: under explicit mapping goxdcr deletes "
                      "the backfill when its target is dropped")
        pair = self._pair(BROKEN)
        # registered first: a write that fails can still land
        self.addCleanup(self._set_manifest_refresh, MANIFEST_REFRESH_DEFAULT)
        self._set_manifest_refresh(5)
        self._setup_replication((HEALTHY, BROKEN, BUSY), missing=(BROKEN, BUSY))
        self._set_repl_settings({"networkUsageLimit": self.backfill_mbps, "compressionType": "None",
                                 "collectionsOSOMode": "false"})
        self._load(HEALTHY, self.ns_items)
        self._load_together((BROKEN, BUSY), self.backfill_items, self.backfill_doc_size)
        self._wait_live([pair, self._pair(BUSY)], True, self.broken_timeout, where=("main",))
        self._create_target(BROKEN)
        self._create_target(BUSY)
        self._wait_backfill_running((BROKEN, BUSY))
        self._pause_and_wait_checkpoint()
        self._drop_target(BROKEN)
        self.sleep(20, "let goxdcr fetch the target manifest without {}".format(pair[1]))
        self.repl.resume(verify=True)
        self._load(BROKEN, self.ns_items)
        self._wait_live([pair], True, self.broken_timeout, where=("main",))
        self.sleep(30, "let the backfill route {}'s docs to the missing target".format(pair[0]))
        busy_on_target = self._target_count(BUSY)
        self.log.info("backfill still running: {} of {} {} docs on the target".format(
            busy_on_target, self.backfill_items, BUSY))
        self.assertLess(busy_on_target, self.backfill_items,
                        "the backfill finished before the procedure; lower backfill_mbps or raise "
                        "backfill_items / backfill_doc_size")
        self.assertTrue(self._backfill_spec_docs(), "the backfill spec is gone before the procedure")
        written = self._written((BROKEN,))
        # the cap was only needed to keep the backfill running; lift it while paused
        marks = self._apply_procedure(
            create=(BROKEN,), expect_backfill=True,
            before_restart=lambda: self._set_repl_settings({"networkUsageLimit": 0}))
        self._assert_healthy_after([pair], marks, written)
        # the backfill pipeline resumes from the checkpoints the script patched
        self._assert_converged(BUSY)

    def test_patch_does_not_mask_genuine(self):
        """The patch removes a record, not the detection: with the target still
        missing, new writes raise the broken mapping again, and once the target is
        created they are backfilled."""
        pair = self._pair(BROKEN)
        self._setup_replication((HEALTHY, BROKEN), missing=(BROKEN,))
        self._induce_genuine((BROKEN,))
        written_while_broken = self._count(self.src_cluster, SRC_SCOPE, BROKEN)
        self._apply_procedure()
        self._load(BROKEN, self.post_items)
        self._wait_live([pair], True, self.broken_timeout)
        self._create_target(BROKEN)
        self._wait_live([pair], False, self.heal_timeout, where=("ui", "main"))
        # the docs written after the patch were recorded in a new broken map and are backfilled
        self._wait_count(self.dest_cluster, *self._target(BROKEN), self.post_items,
                         self.heal_timeout, "{} after the target was created".format(pair))
        self._log_gap(BROKEN, written_while_broken)
        self._assert_converged(HEALTHY)

    def test_script_guardrails(self):
        """The script's own safety checks: prep, patch, restore and delete refuse while
        the replication runs; patch, restore and delete refuse without a prep backup;
        prep refuses over an existing backup and (variable VB) without CB_ENABLE_VARVB,
        leaving no partial backup, and does not refuse a same-VB replication without it.
        Then the full procedure still succeeds."""
        pair = self._pair(BROKEN)
        self._setup_replication((HEALTHY, BROKEN), missing=(BROKEN,))
        self._induce_genuine((BROKEN,))
        self._push_script()
        master = self.src_master

        out, rc = self._run_script(master, "list-repl")
        self.assertEqual(rc, 0, "list-repl exited {}".format(rc))
        self.assertIn(self.repl_id, out, "list-repl does not list the replication")

        # running replication: nothing may be written (goxdcr itself may checkpoint
        # meanwhile, so only the broken-map records are compared)
        shas = {ip: p["main"]["shas"] for ip, p in self._persisted().items()}
        for command in ("prep", "patch", "restore", "delete"):
            out, rc = self._run_script(master, command)
            self._assert_refused(command, out, rc, PAUSED_REFUSAL, NOTHING_WRITTEN)
        self.assertEqual(shas, {ip: p["main"]["shas"] for ip, p in self._persisted().items()},
                         "the broken-map records changed while the script refused")
        self.assertEqual(self._backup_digest(master), "", "a refused prep left a backup")

        self._pause_and_wait_checkpoint()
        snapshot = self._persisted()
        for command in ("patch", "restore"):
            out, rc = self._run_script(master, command)
            self._assert_refused(command, out, rc, NO_BACKUP_REFUSAL, NOTHING_WRITTEN)
        # delete removes the brokenMappings doc outright; it too needs a prep backup first
        out, rc = self._run_script(master, "delete")
        self._assert_refused("delete", out, rc, "file not found")
        if self.variable_vb:
            out, rc = self._run_script(master, "prep", varvb=False)
            self._assert_refused("prep without CB_ENABLE_VARVB", out, rc, VARVB_REFUSAL)
            # prep downloads every pipeline before writing a file
            self.assertEqual(self._backup_digest(master), "", "a refused prep left a partial backup")
            self._run_ok(master, "prep")
        else:
            # patchset e1c19f3 refused this: goxdcr writes an empty globalInfo doc for
            # every replication, which it took for global checkpoints
            out, rc = self._run_script(master, "prep", varvb=False)
            self.assertNotIn(VARVB_REFUSAL, out, "prep refused a same-VB replication")
            self.assertEqual(rc, 0, "{} prep on {} exited {}".format(SCRIPT_NAME, master.ip, rc))
        backup = self._backup_digest(master)
        self.assertTrue(backup, "prep wrote no backup under data/")
        out, rc = self._run_script(master, "prep")
        self._assert_refused("a second prep", out, rc, BACKUP_EXISTS_REFUSAL)
        self.assertEqual(backup, self._backup_digest(master), "a second prep changed the backup")
        self.assertEqual(snapshot, self._persisted(), "a refused command changed metakv")

        for node in self.src_cluster.get_nodes():
            if node.ip != master.ip:
                self._run_ok(node, "prep")
            self._run_ok(node, "patch")
        self._assert_persisted_clean()
        self._create_target(BROKEN)
        self._restart_goxdcr()
        self.repl.resume(verify=True)
        self._hold_absent([pair], self.hold_secs)
        self._assert_new_docs_replicate(BROKEN)
        self._assert_converged(HEALTHY)
