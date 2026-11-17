import itertools
import json
import os
import re
import time
import random
import threading

import requests

import logger

from TestInput import TestInputSingleton
from lib.membase.api.rest_client import RestConnection
from .fts_base import NodeHelper
from .fts_vector_search import VectorSearch


class FastMergeVectorSearch(VectorSearch):
    """
    Tests that are unique to FastMerge and cannot be covered by re-running
    existing VectorSearch/BQVectorSearch tests with fastmerge=True.

    Covers:
      - vector_index_fast_merge presence in params.store (definition validation)
      - Disk/memory comparison: fastmerge vs legacy on same dataset
      - Invalid config values (negative)
      - CE negative test

    All other scenarios (kNN recall, topology, backup/restore, updates, etc.)
    are covered by re-running existing VectorSearch tests with fastmerge=True
    in the conf file.

    Feature permutation design (extensible):
    ---------------------------------------------------------------
    | Feature      | Config location           | Param            |
    |--------------|---------------------------|------------------|
    | FastMerge    | params.store              | fastmerge=True   |
    | BQ           | per-field                 | bq_index_type=X  |
    | GPU          | per-field (gpu: true)     | gpu=True  (TODO) |
    | Encryption   | cluster-level             | cluster config   |
    ---------------------------------------------------------------
    To add GPU: read self.gpu param in setUp, inject into vector_fields
                in add_child_field_to_default_collection_mapping.
    To add Encryption: handle in FTSBaseTest.setUp cluster config.
    Conf file drives all permutations -- no new test classes needed.
    """

    def setUp(self):
        super(FastMergeVectorSearch, self).setUp()
        self.fastmerge = True

    def tearDown(self):
        super(FastMergeVectorSearch, self).tearDown()

    # ---------------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------------

    def _setup_fm_data_and_index(self, index_name="fm_idx", extra_fields=None):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        idx = [(index_name, "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}

        index = self._create_fts_index_parameterized(
            field_name=self.vector_field_name,
            field_type=self.vector_field_type,
            test_indexes=idx,
            vector_fields=vector_fields,
            create_vector_index=True,
            extra_fields=extra_fields or [{"sno": "number"}]
        )

        index_obj = index[0]['index_obj']
        dataset_name = bucketvsdataset['bucket_name']
        index_obj.faiss_index = self.create_faiss_index_from_train_data(dataset_name)

        return index[0], index_obj, dataset_name

    def _validate_fm_in_store(self, index_obj):
        """Assert vector_index_fast_merge=True lives in params.store."""
        status, index_def = index_obj.get_index_defn()
        self.assertTrue(status, "Failed to get index definition")
        store = index_def["indexDef"].get('params', {}).get('store', {})
        self.assertTrue(store.get('vector_index_fast_merge', False),
                        f"vector_index_fast_merge not True in params.store: {store}")
        self.log.info("Validated: vector_index_fast_merge=True in params.store")
        return index_def["indexDef"]

    def _get_fts_ram_usage(self):
        ram_by_node = {}
        rest = RestConnection(self._cb_cluster.get_master_node())
        for node in self._cb_cluster.get_fts_nodes():
            stats_text = rest.get_nsserver_stats(node.ip).text
            match = re.search(
                r'sysproc_mem_resident\{proc="cbft",category="system-processes"\} (\d+)',
                stats_text)
            ram_by_node[node.ip] = int(match.group(1)) if match else 0
        return ram_by_node

    # ---------------------------------------------------------------------------
    # FastMerge-unique: Definition validation
    # ---------------------------------------------------------------------------

    def test_fm_index_definition_validation(self):
        """Verify vector_index_fast_merge is in params.store via REST API."""
        index_info, index_obj, dataset_name = self._setup_fm_data_and_index("fm_defval")
        self._validate_fm_in_store(index_obj)

        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        status, rest_def = rest.get_fts_index_definition(
            "fm_defval", bucket=index_obj._source_name, scope=index_obj.scope)
        self.assertTrue(status, "Failed to get index def via REST")

        rest_store = rest_def.get('indexDef', {}).get('params', {}).get('store', {})
        self.assertTrue(rest_store.get('vector_index_fast_merge', False),
                        "vector_index_fast_merge missing from REST response store config")

        self.assertEqual(rest_def['indexDef'].get('type'), 'fulltext-index')
        self.log.info("test_fm_index_definition_validation passed")

    # ---------------------------------------------------------------------------
    # FastMerge-unique: Disk footprint comparison (fastmerge vs legacy)
    # ---------------------------------------------------------------------------

    def test_fm_disk_footprint_comparison(self):
        """Create identical indexes with and without fastmerge, compare disk."""
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        self.load_vector_data(containers, dataset=self.vector_dataset)

        vector_fields = {"dims": self.dimension, "similarity": self.similarity}

        fm_idx = [("fm_disk", "b1.s1.c1")]
        fm_index = self._create_fts_index_parameterized(
            field_name=self.vector_field_name,
            field_type=self.vector_field_type,
            test_indexes=fm_idx,
            vector_fields=vector_fields,
            create_vector_index=True,
            extra_fields=[{"sno": "number"}]
        )
        self._validate_fm_in_store(fm_index[0]['index_obj'])

        saved = self.fastmerge
        self.fastmerge = False
        nonfm_idx = [("nonfm_disk", "b1.s1.c1")]
        nonfm_index = self._create_fts_index_parameterized(
            field_name=self.vector_field_name,
            field_type=self.vector_field_type,
            test_indexes=nonfm_idx,
            vector_fields=vector_fields,
            create_vector_index=True,
            extra_fields=[{"sno": "number"}]
        )
        self.fastmerge = saved

        time.sleep(15)
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])

        _, fm_stats = rest.get_fts_stats(index_name="fm_disk",
                                          stat_name="num_bytes_used_disk")
        _, nonfm_stats = rest.get_fts_stats(index_name="nonfm_disk",
                                             stat_name="num_bytes_used_disk")

        fm_disk = float(fm_stats) if fm_stats else 0
        nonfm_disk = float(nonfm_stats) if nonfm_stats else 0

        self.log.info(f"Disk: FastMerge={fm_disk/(1024*1024):.2f}MB, "
                      f"Legacy={nonfm_disk/(1024*1024):.2f}MB")
        if nonfm_disk > 0:
            self.log.info(f"Ratio: {fm_disk / nonfm_disk:.2f}")

        self._cb_cluster.delete_fts_index("nonfm_disk")
        self.log.info("test_fm_disk_footprint_comparison passed")

    # ---------------------------------------------------------------------------
    # FastMerge-unique: Memory footprint comparison
    # ---------------------------------------------------------------------------

    def test_fm_memory_footprint(self):
        """Measure RAM delta during fastmerge index build."""
        ram_before = self._get_fts_ram_usage()
        self.log.info(f"RAM before: {ram_before}")

        index_info, index_obj, dataset_name = self._setup_fm_data_and_index("fm_mem")
        self._validate_fm_in_store(index_obj)
        time.sleep(10)

        ram_after = self._get_fts_ram_usage()
        self.log.info(f"RAM after: {ram_after}")

        for ip in ram_before:
            delta = ram_after.get(ip, 0) - ram_before.get(ip, 0)
            self.log.info(f"Node {ip}: delta={delta/(1024*1024):.2f} MB")

        queries = self.get_query_vectors(dataset_name)
        neighbours = self.get_groundtruth_file(dataset_name)
        for q in queries[:3]:
            _, _, _, ra = self.run_vector_query(
                vector=q.tolist(), index=index_obj, neighbours=neighbours[0])
        self.log.info("test_fm_memory_footprint passed")

    # ---------------------------------------------------------------------------
    # FastMerge-unique: Negative tests
    # ---------------------------------------------------------------------------

    def test_fm_invalid_config_value(self):
        """Set vector_index_fast_merge to a non-boolean -- expect rejection or graceful handling."""
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        self.load_vector_data(containers, dataset=self.vector_dataset)

        saved = self.fastmerge
        self.fastmerge = False
        idx = [("fm_invalid", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}
        index = self._create_fts_index_parameterized(
            field_name=self.vector_field_name,
            field_type=self.vector_field_type,
            test_indexes=idx,
            vector_fields=vector_fields,
            create_vector_index=True,
            extra_fields=[{"sno": "number"}]
        )
        self.fastmerge = saved
        index_obj = index[0]['index_obj']

        if 'store' not in index_obj.index_definition['params']:
            index_obj.index_definition['params']['store'] = {}
        index_obj.index_definition['params']['store']['vector_index_fast_merge'] = "invalid"
        index_obj.index_definition['uuid'] = index_obj.get_uuid()
        try:
            index_obj.update()
            self.log.warning("Invalid vector_index_fast_merge accepted by server")
        except Exception as e:
            self.log.info(f"Correctly rejected: {e}")

        self.log.info("test_fm_invalid_config_value passed")

    def test_fm_community_edition_negative(self):
        """FastMerge should not be available on Community Edition."""
        rest = RestConnection(self._cb_cluster.get_master_node())
        if rest.is_enterprise_edition():
            self.log.info("Skipping -- running on Enterprise Edition")
            return

        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        self.load_vector_data(containers, dataset=self.vector_dataset)

        idx = [("fm_ce", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}
        try:
            self._create_fts_index_parameterized(
                field_name=self.vector_field_name,
                field_type=self.vector_field_type,
                test_indexes=idx,
                vector_fields=vector_fields,
                create_vector_index=True
            )
            self.log.warning("FastMerge succeeded on CE -- may be expected")
        except Exception as e:
            self.log.info(f"Correctly rejected on CE: {e}")

        self.log.info("test_fm_community_edition_negative passed")

    # ---------------------------------------------------------------------------
    # MB-75211: index-level doc count stuck on a stale cached alias
    #
    # GET /api/index/{name}/count answers from a cached alias built over the
    # pindexes registered *at the time of the call*; pindexes not yet registered
    # are treated as missing. cbftx clears that cache from the pindex
    # registration callback. cbft change 256024 replaced that callback instead of
    # wrapping it, so a pindex registering after the first count call is never
    # added to the cached alias and the count stays short by exactly that
    # pindex's docs (perf: 16,659,000 of 20,000,000 = all but one of 6 pindexes),
    # which hangs anything that waits for the count to reach the source count.
    #
    # Repro: hit the count API on every FTS node continuously from before the
    # index exists until its partitions have registered, then check that the
    # index-level count converges on the sum of the per-pindex counts.
    # ---------------------------------------------------------------------------

    def _fts_get(self, rest, path, timeout=5):
        """GET an FTS endpoint without urllib_request's per-call logging/retries.
        Returns (http status or exception name, parsed JSON or None)."""
        try:
            resp = requests.get(rest.fts_baseUrl + path,
                                headers=rest._create_capi_headers(),
                                timeout=timeout, verify=False)
        except requests.RequestException as e:
            return type(e).__name__, None
        if resp.status_code != 200:
            return resp.status_code, None
        try:
            return 200, resp.json()
        except ValueError:
            return "bad-json", None

    def _fts_get_json(self, rest, path, timeout=5):
        return self._fts_get(rest, path, timeout)[1]

    @staticmethod
    def _index_count_paths(index_obj):
        """Count endpoints to try, in order. On 8.x an index created in b1.s1 is
        stored as b1.s1.<name>, so the legacy endpoint needs the full name (that
        is what perfrunner calls); the scoped endpoint takes the short name. The
        last entry covers an index in _default._default."""
        bucket, scope, name = index_obj._source_name, index_obj.scope or "_default", index_obj.name
        short = name.split(".")[-1]
        return [f"api/bucket/{bucket}/scope/{scope}/index/{short}/count",
                f"api/index/{bucket}.{scope}.{short}/count",
                f"api/index/{name}/count"]

    def _index_count_on(self, rest, index_obj, misses=None):
        """Index-level doc count from one node, or None. Remembers the first
        endpoint that answers; records the last failure per endpoint in `misses`."""
        paths = self._index_count_paths(index_obj)
        chosen = getattr(self, "_count_path_idx", None)
        for i in ([chosen] if chosen is not None else range(len(paths))):
            status, body = self._fts_get(rest, paths[i])
            if body is not None and "count" in body:
                self._count_path_idx = i
                return body["count"]
            if misses is not None:
                misses[f"{rest.ip} {paths[i]}"] = status
        return None

    def _pindex_counts(self, fts_rests, index_obj):
        """{planPIndex name: count or None} for the index's current UUID, each
        count read from whichever FTS node hosts that pindex."""
        uuid = index_obj.get_uuid()
        cfg = self._fts_get_json(fts_rests[0], "api/cfg") or {}
        plan = (cfg.get("planPIndexes") or {}).get("planPIndexes") or {}
        names = [p["name"] for p in plan.values() if p.get("indexUUID") == uuid]
        counts = {}
        for name in names:
            counts[name] = None
            for rest in fts_rests:
                body = self._fts_get_json(rest, f"api/pindex/{name}/count")
                if body and body.get("status") == "ok":
                    counts[name] = body["count"]
                    break
        return counts

    @staticmethod
    def _explain_gap(gap, pindex_counts):
        """Names of the pindex subsets whose doc counts sum exactly to `gap`."""
        known = {n: c for n, c in pindex_counts.items() if c is not None}
        hits = []
        for r in range(1, len(known) + 1):
            for combo in itertools.combinations(known, r):
                if sum(known[n] for n in combo) == gap:
                    hits.append([n.split("_")[-1] for n in combo])
        return hits

    def test_fm_index_count_late_partition_registration(self):
        """MB-75211: index doc count must include partitions that register after
        the count API has already been served.

        Params:
          num_partitions           partitions for the index (perf: 6)
          count_poll_interval      seconds between count calls per node during
                                   registration (default 0.2)
          rebalance_in_fts         rebalance an extra FTS node in while the index
                                   builds, so pindexes register on it late
                                   (perf: "Dynamic Service Rebalance, Adding FTS")
          stale_check_secs         how long the index count may lag the pindex
                                   total once indexing is done (default 180)
          fastmerge                default True; False is the non-FM control
        """
        self.fastmerge = self.input.param("fastmerge", True)
        poll_interval = float(self.input.param("count_poll_interval", 0.2))
        rebalance_in_fts = self.input.param("rebalance_in_fts", False)
        stale_check_secs = int(self.input.param("stale_check_secs", 180))
        wait_timeout = int(self.input.param("index_wait_timeout", 1200))
        index_name = "fm_cnt"

        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        self.load_vector_data(containers, dataset=self.vector_dataset)

        # Placeholder so the poller can build the count path before the index exists.
        class _Target:
            name, _source_name, scope = index_name, "b1", "s1"
        target = _Target()

        stop = threading.Event()
        seen = {}     # ip:port -> list of (elapsed, count) whenever the count changes
        misses = {}   # "ip path" -> last non-200 status, for when nothing is seen
        poller_errors = []
        t0 = time.time()

        def poll_counts():
            rests, refreshed = [], 0
            while not stop.is_set():
                try:
                    if time.time() - refreshed > 5:   # pick up a rebalanced-in node
                        rests = [RestConnection(n) for n in self._cb_cluster.get_fts_nodes()]
                        refreshed = time.time()
                    for rest in rests:
                        count = self._index_count_on(rest, target, misses)
                        if count is None:
                            continue
                        hist = seen.setdefault(f"{rest.ip}:{rest.fts_port}", [])
                        if not hist or hist[-1][1] != count:
                            hist.append((round(time.time() - t0, 2), count))
                except Exception as e:   # keep polling; a dead poller looks like "no answers"
                    if len(poller_errors) < 20:
                        poller_errors.append(repr(e))
                stop.wait(poll_interval)

        poller = threading.Thread(target=poll_counts, name="mb75211-count-poller", daemon=True)
        poller.start()
        rebalance_task = None
        try:
            index = self._create_fts_index_parameterized(
                field_name=self.vector_field_name,
                field_type=self.vector_field_type,
                test_indexes=[(index_name, "b1.s1.c1")],
                vector_fields={"dims": self.dimension, "similarity": self.similarity},
                create_vector_index=True,
                extra_fields=[{"sno": "number"}],
                wait_for_index_complete=False)
            index_obj = index[0]['index_obj']
            target.name, target._source_name, target.scope = \
                index_obj.name, index_obj._source_name, index_obj.scope
            if self.fastmerge:
                self._validate_fm_in_store(index_obj)

            if rebalance_in_fts:
                rebalance_task = self._cb_cluster.async_rebalance_in(num_nodes=1, services=["fts"])

            # Ground truth: per-pindex counts summing to the source count.
            source_count = index_obj.get_src_collections_doc_count()
            deadline = time.time() + wait_timeout
            while True:
                fts_rests = [RestConnection(n) for n in self._cb_cluster.get_fts_nodes()]
                pindex_counts = self._pindex_counts(fts_rests, index_obj)
                pindex_total = sum(c or 0 for c in pindex_counts.values())
                self.log.info(f"pindex total {pindex_total}/{source_count}: "
                              f"{ {n.split('_')[-1]: c for n, c in pindex_counts.items()} }")
                if pindex_counts and None not in pindex_counts.values() \
                        and pindex_total >= source_count \
                        and (rebalance_task is None or rebalance_task.done()):
                    break
                if time.time() > deadline:
                    self.fail(f"Per-pindex counts never reached the source count "
                              f"({pindex_total}/{source_count}) within {wait_timeout}s; "
                              f"this is an indexing problem, not MB-75211")
                time.sleep(10)
        finally:
            stop.set()
            poller.join(timeout=30)
            if rebalance_task is not None:
                rebalance_task.result()

        self.log.info(f"Count history during registration (node -> [(t, count)]): {seen}")
        if poller_errors:
            self.log.warning(f"Count poller errors: {poller_errors}")
        self.assertTrue(seen, f"Count API never answered during registration, so the "
                              f"cache was never primed; the race was not exercised. "
                              f"Last failures per endpoint: {misses}; poller errors: "
                              f"{poller_errors}")
        self.assertGreater(len(pindex_counts), 1,
                           f"Need several pindexes registering independently to race; "
                           f"found {len(pindex_counts)}")

        # Every node's index-level count must converge on the pindex total.
        fts_rests = [RestConnection(n) for n in self._cb_cluster.get_fts_nodes()]
        deadline = time.time() + stale_check_secs
        while True:
            node_counts = {f"{r.ip}:{r.fts_port}": self._index_count_on(r, index_obj)
                           for r in fts_rests}
            if all(c == pindex_total for c in node_counts.values()):
                self.log.info(f"Index count matches pindex total {pindex_total} on all "
                              f"nodes: {node_counts}")
                break
            if time.time() > deadline:
                pindex_counts = self._pindex_counts(fts_rests, index_obj)
                pindex_total = sum(c or 0 for c in pindex_counts.values())
                gaps = {node: {"count": c,
                               "missing": pindex_total - c if c is not None else None,
                               "missing_pindexes": self._explain_gap(pindex_total - c, pindex_counts)
                               if c is not None and c < pindex_total else []}
                        for node, c in node_counts.items() if c != pindex_total}
                self.fail(
                    f"MB-75211 reproduced: /count still disagrees with the per-pindex "
                    f"total ({pindex_total}, source {source_count}) {stale_check_secs}s "
                    f"after indexing finished. Per node: {gaps}. Per-pindex: "
                    f"{ {n.split('_')[-1]: c for n, c in pindex_counts.items()} }. "
                    f"Count history during registration: {seen}")
            time.sleep(5)
