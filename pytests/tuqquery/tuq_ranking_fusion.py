import threading

import numpy as np
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from deepdiff import DeepDiff

from lib.vector.vector import (FAISSVector as faiss, IndexVector, LoadVector,
                                SiftVector as sift, SparseVector as sparse)
from membase.api.exception import CBQError
from membase.api.rest_client import RestConnection
from pytests.fts.fts_base import CouchbaseCluster
from .tuq import QueryTests


class QueryRankingFusionTests(QueryTests):
    bucket = "default"
    fts_index_name = "idx_default_fusion_fts"
    dense_field = "dense_vec"
    sparse_field = "sparse_vec"
    dense_index_name = "vector_index_dense_fusion"
    sparse_index_name = "vector_index_sparse_fusion"
    doc_count = 11000

    def setUp(self):
        super(QueryRankingFusionTests, self).setUp()
        self.distance = self.input.param("distance", "L2_SQUARED")
        self.dimension = self.input.param("dimension", 128)
        self.nprobes = self.input.param("nprobes", 3)
        self.description = self.input.param("description", "IVF,SQ8")
        self.train = self.input.param("train", 10000)
        self.k = self.input.param("k", 20)

        auth = PasswordAuthenticator(self.master.rest_username, self.master.rest_password)
        self.database = Cluster(f"couchbase://{self.master.ip}", ClusterOptions(auth))

        sift().download_sift()
        self.xb_dense = sift().read_base()
        self.xq_dense = sift().read_query()

        sparse().download_dataset()
        self.xb_sparse = sparse().read_vector()
        self.xq_sparse = sparse().read_query()

        if self.dimension > 128:
            add = self.dimension - 128
            self.xb_dense = np.append(self.xb_dense, np.ones((len(self.xb_dense), add), "float32"), axis=1)
            self.xq_dense = np.append(self.xq_dense, np.ones((len(self.xq_dense), add), "float32"), axis=1)

        self.cbcluster = CouchbaseCluster(name="cluster", nodes=self.servers, log=self.log)
        fts_nodes = self.cbcluster.get_fts_nodes()
        if fts_nodes:
            RestConnection(fts_nodes[0]).set_fts_ram_quota(700)

    def suite_setUp(self):
        super(QueryRankingFusionTests, self).suite_setUp()
        self._create_fts_index()
        self._load_hybrid_documents(self.doc_count)
        IndexVector().create_index(
            self.database, bucket=self.bucket, vector_field=self.dense_field,
            dimension=self.dimension, train=self.train, description=self.description,
            similarity=self.distance, nprobes=self.nprobes, vector_type="dense",
            custom_index_fields=f"brand, {self.dense_field} VECTOR",
            custom_name=self.dense_index_name,
        )
        IndexVector().create_index(
            self.database, bucket=self.bucket, vector_field=self.sparse_field,
            train=self.train, nprobes=self.nprobes, vector_type="sparse",
            custom_index_fields=f"brand, {self.sparse_field} sparse VECTOR",
            custom_name=self.sparse_index_name,
        )

    def tearDown(self):
        super(QueryRankingFusionTests, self).tearDown()

    def suite_tearDown(self):
        try:
            self.run_cbq_query(f"DROP INDEX `{self.bucket}`.{self.dense_index_name}")
        except Exception:
            pass
        try:
            self.run_cbq_query(f"DROP INDEX `{self.bucket}`.{self.sparse_index_name}")
        except Exception:
            pass
        try:
            self.cbcluster.delete_fts_index(self.fts_index_name)
        except Exception:
            pass
        super(QueryRankingFusionTests, self).suite_tearDown()

    # Loads `doc_count` docs into `default` with both a dense and sparse vector
    # per document, plus the same auxiliary fields tuq_vectorsearch.py adds via
    # its `update_data` block (price, null_field, nested, broader brand/size
    # variance) so a single run can exercise FTS, dense, sparse, AND filtered
    # fusion.
    def _load_hybrid_documents(self, count):
        bucket = self.database.bucket(self.bucket)
        coll = bucket.scope("_default").collection("_default")
        brands = ["nike", "adidas", "reebok", "puma", "asics"]
        sizes = [6, 7, 8, 9, 10]
        prices = [100, 150, 200, 250]
        nested_variants = [{"a": 1, "b": [1, 2, 3]}, {"a": 2, "b": [3, 2, 1]}]
        batch = {}
        for i in range(count):
            brand = brands[i % len(brands)]
            size = sizes[i % len(sizes)]
            dense_vec = self.xb_dense[i % len(self.xb_dense)].tolist()
            sp = self.xb_sparse[i % self.xb_sparse.shape[0]]
            sparse_vec = [sp.indices.tolist(), sp.data.tolist()]
            doc = {
                "id": i,
                "brand": brand,
                "size": size,
                "description": f"premium {brand} running shoe size {size}",
                "nested": nested_variants[i % 2],
                self.dense_field: dense_vec,
                self.sparse_field: sparse_vec,
            }
            # mirror tuq_vectorsearch: some docs missing `price`, some have `null_field`
            if i % 5 != 0:
                doc["price"] = prices[i % len(prices)]
            if i % 3 == 0:
                doc["null_field"] = None
            batch[f"doc_{i}"] = doc
            if len(batch) >= 200:
                coll.upsert_multi(batch)
                batch = {}
        if batch:
            coll.upsert_multi(batch)

    def _create_fts_index(self):
        fts_index = self.cbcluster.create_fts_index(name=self.fts_index_name, source_name=self.bucket)
        return fts_index

    # ---------- helpers for building fusion queries ----------

    def _fts_subquery(self, term="nike", size=50, where=None):
        extra = f" AND ({where})" if where else ""
        return (
            f"(SELECT META(t).id, s.score FROM `{self.bucket}` AS t "
            f"LET s = SEARCH_META() "
            f"WHERE SEARCH(t, "
            f"{{'query': {{'field': 'brand', 'match': '{term}'}}, 'size': {size}}}, "
            f"{{'index': '{self.fts_index_name}'}}){extra} "
            f"ORDER BY s.score DESC, META(t).id)"
        )

    def _dense_subquery(self, query_idx=0, k=50, where=None):
        qvec = self.xq_dense[query_idx].tolist()
        where_clause = f"WHERE {where} " if where else ""
        return (
            f"(SELECT RAW {{'id': META(t).id, 'score': "
            f"ANN_DISTANCE(t.{self.dense_field}, {qvec}, '{self.distance}')}} "
            f"FROM `{self.bucket}` AS t "
            f"{where_clause}"
            f"ORDER BY ANN_DISTANCE(t.{self.dense_field}, {qvec}, '{self.distance}') LIMIT {k})"
        )

    def _sparse_subquery(self, query_idx=0, k=50, where=None):
        sp = self.xq_sparse[query_idx]
        qvec = [sp.indices.tolist(), sp.data.tolist()]
        where_clause = f"WHERE {where} " if where else ""
        return (
            f"(SELECT RAW {{'id': META(t).id, 'score': "
            f"SPARSE_VECTOR_DISTANCE(t.{self.sparse_field}, {qvec}, {self.nprobes})}} "
            f"FROM `{self.bucket}` AS t "
            f"{where_clause}"
            f"ORDER BY SPARSE_VECTOR_DISTANCE(t.{self.sparse_field}, {qvec}, {self.nprobes}) LIMIT {k})"
        )

    def _run_fusion(self, opt, *subqueries):
        args = ",\n".join(subqueries)
        self.query = f"WITH opt AS ({opt}) SELECT RAW r FROM reciprocal_fusion(opt, {args}) AS r"
        self.log.info(f"[ranking_fusion] inline-subquery form, {len(subqueries)} arm(s):\n{self.query}")
        results = self.run_cbq_query()["results"]
        self.log.info(f"[ranking_fusion] returned {len(results)} row(s)")
        return results

    # Hoists every subquery into its own CTE and references the CTEs by name in
    # the reciprocal_fusion() call. Exercises the WITH-binding pattern the user
    # explicitly asked about: WITH q1 AS (...), q2 AS (...), opt AS ({...})
    # SELECT RAW r FROM reciprocal_fusion(opt, q1, q2) AS r.
    def _run_fusion_with_ctes(self, opt, **ctes):
        cte_parts = []
        for name, body in ctes.items():
            body = body.strip()
            if body.startswith("(") and body.endswith(")"):
                body = body[1:-1]
            cte_parts.append(f"{name} AS ({body})")
        cte_block = ",\n".join(cte_parts)
        args = ", ".join(ctes.keys())
        self.query = (
            f"WITH {cte_block},\nopt AS ({opt}) "
            f"SELECT RAW r FROM reciprocal_fusion(opt, {args}) AS r"
        )
        self.log.info(f"[ranking_fusion] CTE form, arms={list(ctes.keys())}:\n{self.query}")
        results = self.run_cbq_query()["results"]
        self.log.info(f"[ranking_fusion] returned {len(results)} row(s)")
        return results

    # ---------- helpers for validating fusion correctness ----------

    # Runs a single arm subquery on its own so we can capture its ranked output
    # (the exact rows the fusion engine sees as input for that arm).
    def _run_arm(self, subquery):
        s = subquery.strip()
        if s.startswith("(") and s.endswith(")"):
            s = s[1:-1]
        self.log.info(f"[ranking_fusion] running arm subquery for expected-result calc:\n{s}")
        rows = self.run_cbq_query(s)["results"]
        self.log.info(f"[ranking_fusion] arm returned {len(rows)} row(s)")
        return rows

    # Pure-Python RRF over already-ranked arms.
    # arms: list[list[{"id": ..., "score": ...}]] — order = rank, rank 0 first.
    # Formula from the test plan: score = sum_i(weight_i / (penalty_i + rank_i)).
    def _rrf_expected(self, arms, k=60, weights=None, limit=None):
        weights = weights or [1.0] * len(arms)
        by_id = {}
        for arm_idx, arm in enumerate(arms):
            seen = set()
            for rank, row in enumerate(arm):
                rid = row["id"]
                if rid in seen:
                    continue
                seen.add(rid)
                contrib = weights[arm_idx] / (k + rank)
                if rid not in by_id:
                    by_id[rid] = {"id": rid, "score": 0.0}
                by_id[rid]["score"] += contrib
                by_id[rid][f"score_{arm_idx}"] = contrib
        ordered = sorted(by_id.values(), key=lambda x: -x["score"])
        return ordered[:limit] if limit else ordered

    # Pure-Python RSF with normalization="none" and isScore=true:
    # contribution = weight * raw_score.
    def _rsf_expected_raw(self, arms, weights=None, limit=None):
        weights = weights or [1.0] * len(arms)
        by_id = {}
        for arm_idx, arm in enumerate(arms):
            for row in arm:
                rid, s = row["id"], row["score"]
                contrib = weights[arm_idx] * s
                if rid not in by_id:
                    by_id[rid] = {"id": rid, "score": 0.0}
                by_id[rid]["score"] += contrib
                by_id[rid][f"score_{arm_idx}"] = contrib
        ordered = sorted(by_id.values(), key=lambda x: -x["score"])
        return ordered[:limit] if limit else ordered

    # Pure-Python RSF with normalization="minMaxScaler". Per-arm min-max into
    # [0,1]; for isScore=false we invert (1 - normalized) so distance becomes
    # score, matching the test plan's `isScore` semantics.
    def _rsf_expected_minmax(self, arms, weights=None, limit=None, is_scores=None):
        weights = weights or [1.0] * len(arms)
        is_scores = is_scores if is_scores is not None else [True] * len(arms)
        by_id = {}
        for arm_idx, arm in enumerate(arms):
            scores = [r["score"] for r in arm]
            if not scores:
                continue
            smin, smax = min(scores), max(scores)
            span = (smax - smin) if smax > smin else 1.0
            for row in arm:
                rid = row["id"]
                norm = (row["score"] - smin) / span
                value = norm if is_scores[arm_idx] else (1 - norm)
                contrib = weights[arm_idx] * value
                if rid not in by_id:
                    by_id[rid] = {"id": rid, "score": 0.0}
                by_id[rid]["score"] += contrib
                by_id[rid][f"score_{arm_idx}"] = contrib
        ordered = sorted(by_id.values(), key=lambda x: -x["score"])
        return ordered[:limit] if limit else ordered

    # Compare actual fusion output against the Python-computed expected output.
    # Validates set of ids, per-id score (and per-arm component scores when
    # present), and that the actual result is monotonically non-increasing.
    # Tie-order between equal-score rows is intentionally not validated since
    # it is implementation-defined.
    def _assert_fusion_equal(self, actual, expected, tol=1e-9):
        self.log.info(f"[ranking_fusion] validating: actual={actual}")
        self.log.info(f"[ranking_fusion] validating: expected={expected}")
        self.assertEqual(len(actual), len(expected),
                         f"row count: actual={len(actual)} expected={len(expected)}")
        act_by_id = {r["id"]: r for r in actual}
        exp_by_id = {r["id"]: r for r in expected}
        self.assertEqual(set(act_by_id), set(exp_by_id),
                         f"id set mismatch: missing={set(exp_by_id) - set(act_by_id)} "
                         f"extra={set(act_by_id) - set(exp_by_id)}")
        for rid, exp in exp_by_id.items():
            act = act_by_id[rid]
            self.assertAlmostEqual(act["score"], exp["score"], delta=tol,
                                   msg=f"id={rid} fused score mismatch")
            for key, expected_val in exp.items():
                if key.startswith("score_") and key in act:
                    self.assertAlmostEqual(act[key], expected_val, delta=tol,
                                           msg=f"id={rid} {key} mismatch")
        for i in range(1, len(actual)):
            self.assertLessEqual(actual[i]["score"], actual[i - 1]["score"] + tol,
                                 f"ranking not descending at index {i}")

    def _assert_results_equal_ignore_order(self, actual, expected):
        diffs = DeepDiff(
            actual,
            expected,
            ignore_order=True,
            ignore_numeric_type_changes=True,
        )
        self.assertFalse(diffs, f"result diff: {diffs}")

    # ============================================================
    # Algorithm validation against the test plan's worked examples
    # ============================================================

    def test_rrf_dummy_data(self):
        opt = """
            {"fusion":"unionall","scorer":"rrf","score":true,"limit":4,
             "args":[
                {"aggregator":"sum","normalization":"none","weight":1.0,"penalty":60,
                 "pathId":"id","pathScore":"score","isScore":true},
                {"aggregator":"sum","normalization":"none","weight":1.0,"penalty":60,
                 "pathId":"id","pathScore":"score","isScore":true}
             ]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10},{"id":"k02","score":20},{"id":"k03","score":30}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60},{"id":"k12","score":70},{"id":"k13","score":80}] AS r1)'
        results = self._run_fusion(opt, a, b)
        expected = [
            {"id": "k01", "score": 0.016666666666666666, "score_0": 0.016666666666666666},
            {"id": "k11", "score": 0.016666666666666666, "score_1": 0.016666666666666666},
            {"id": "k12", "score": 0.01639344262295082, "score_1": 0.01639344262295082},
            {"id": "k02", "score": 0.01639344262295082, "score_0": 0.01639344262295082}
            ]
        self._assert_results_equal_ignore_order(results, expected)

    def test_rsf_dummy_data(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":4,
             "args":[
                {"aggregator":"sum","normalization":"none","weight":1.0,"penalty":60,
                 "pathId":"id","pathScore":"score","isScore":true},
                {"aggregator":"sum","normalization":"none","weight":1.0,"penalty":60,
                 "pathId":"id","pathScore":"score","isScore":true}
             ]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10},{"id":"k02","score":20},{"id":"k03","score":30}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60},{"id":"k12","score":70},{"id":"k13","score":80}] AS r1)'
        results = self._run_fusion(opt, a, b)
        expected = [
            {"id": "k13", "score": 80, "score_1": 80},
            {"id": "k12", "score": 70, "score_1": 70},
            {"id": "k11", "score": 60, "score_1": 60},
            {"id": "k03", "score": 30, "score_0": 30},
        ]
        self._assert_results_equal_ignore_order(results, expected)

    # ============================================================
    # RRF over real FTS + vector data
    #
    # Each test independently runs each arm subquery to capture its ranked
    # rows, computes the RRF result in Python via the test plan's formula
    # (sum of weight/(penalty+rank)), and asserts the fusion engine's output
    # matches id-for-id and score-for-score.
    # ============================================================

    def test_rrf_fts_dense(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":10,"args":[{},{}]}'
        arm_a, arm_b = self._fts_subquery(), self._dense_subquery()
        results = self._run_fusion(opt, arm_a, arm_b)
        expected = self._rrf_expected([self._run_arm(arm_a), self._run_arm(arm_b)], limit=10)
        self._assert_fusion_equal(results, expected)

    def test_rrf_fts_sparse(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":10,"args":[{},{}]}'
        arm_a, arm_b = self._fts_subquery(), self._sparse_subquery()
        results = self._run_fusion(opt, arm_a, arm_b)
        expected = self._rrf_expected([self._run_arm(arm_a), self._run_arm(arm_b)], limit=10)
        self._assert_fusion_equal(results, expected)

    def test_rrf_dense_sparse(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":10,"args":[{},{}]}'
        arm_a, arm_b = self._dense_subquery(), self._sparse_subquery()
        results = self._run_fusion(opt, arm_a, arm_b)
        expected = self._rrf_expected([self._run_arm(arm_a), self._run_arm(arm_b)], limit=10)
        self._assert_fusion_equal(results, expected)

    def test_rrf_three_inputs_fts_dense_sparse(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":10,"args":[{},{},{}]}'
        a, b, c = self._fts_subquery(), self._dense_subquery(), self._sparse_subquery()
        results = self._run_fusion(opt, a, b, c)
        expected = self._rrf_expected(
            [self._run_arm(a), self._run_arm(b), self._run_arm(c)], limit=10
        )
        self._assert_fusion_equal(results, expected)

    # ============================================================
    # RSF over real FTS + vector data
    #
    # Same validation strategy as the RRF tests, using the per-arm
    # min-max-normalized RSF formula. Vector arms use isScore=false so
    # distance gets inverted to a score (1 - normalized) per the test plan.
    # ============================================================

    def test_rsf_fts_dense(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":10,
             "args":[
                {"normalization":"minMaxScaler","isScore":true},
                {"normalization":"minMaxScaler","isScore":false}
             ]}
        """
        arm_a, arm_b = self._fts_subquery(), self._dense_subquery()
        results = self._run_fusion(opt, arm_a, arm_b)
        expected = self._rsf_expected_minmax(
            [self._run_arm(arm_a), self._run_arm(arm_b)],
            is_scores=[True, False], limit=10,
        )
        self._assert_fusion_equal(results, expected)

    def test_rsf_fts_sparse(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":10,
             "args":[
                {"normalization":"minMaxScaler","isScore":true},
                {"normalization":"minMaxScaler","isScore":false}
             ]}
        """
        arm_a, arm_b = self._fts_subquery(), self._sparse_subquery()
        results = self._run_fusion(opt, arm_a, arm_b)
        expected = self._rsf_expected_minmax(
            [self._run_arm(arm_a), self._run_arm(arm_b)],
            is_scores=[True, False], limit=10,
        )
        self._assert_fusion_equal(results, expected)

    def test_rsf_dense_sparse(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":10,
             "args":[
                {"normalization":"minMaxScaler","isScore":false},
                {"normalization":"minMaxScaler","isScore":false}
             ]}
        """
        arm_a, arm_b = self._dense_subquery(), self._sparse_subquery()
        results = self._run_fusion(opt, arm_a, arm_b)
        expected = self._rsf_expected_minmax(
            [self._run_arm(arm_a), self._run_arm(arm_b)],
            is_scores=[False, False], limit=10,
        )
        self._assert_fusion_equal(results, expected)

    # ============================================================
    # Normalization methods
    # ============================================================

    def test_normalization_none(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":5,
             "args":[
                {"normalization":"none","isScore":true},
                {"normalization":"none","isScore":true}
             ]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10},{"id":"k02","score":20}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60},{"id":"k12","score":70}] AS r1)'
        results = self._run_fusion(opt, a, b)
        scores = [r["score"] for r in results]
        self.assertEqual(scores, sorted(scores, reverse=True))
        self.assertTrue(max(scores) > 1)

    def test_normalization_sigmoid(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":5,
             "args":[
                {"normalization":"sigmoid","isScore":true},
                {"normalization":"sigmoid","isScore":true}
             ]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10},{"id":"k02","score":20}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60},{"id":"k12","score":70}] AS r1)'
        results = self._run_fusion(opt, a, b)
        for r in results:
            self.assertGreaterEqual(r["score"], 0)
            self.assertLessEqual(r["score"], 1)

    def test_normalization_minmaxscaler(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":5,
             "args":[
                {"normalization":"minMaxScaler","isScore":true},
                {"normalization":"minMaxScaler","isScore":true}
             ]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10},{"id":"k02","score":20},{"id":"k03","score":30}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60},{"id":"k12","score":70},{"id":"k13","score":80}] AS r1)'
        results = self._run_fusion(opt, a, b)
        for r in results:
            self.assertGreaterEqual(r["score"], 0)
            self.assertLessEqual(r["score"], 1)

    # ============================================================
    # Options-object behavior
    # ============================================================

    def test_score_flag_true_emits_component_scores(self):
        opt = """
            {"fusion":"unionall","scorer":"rrf","score":true,"limit":4,
             "args":[{},{}]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        results = self._run_fusion(opt, a, b)
        keys = {k for r in results for k in r.keys()}
        self.assertIn("score", keys)
        self.assertTrue("score_0" in keys or "score_1" in keys)

    def test_score_flag_false_omits_component_scores(self):
        opt = """
            {"fusion":"unionall","scorer":"rrf","score":false,"limit":4,
             "args":[{},{}]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        results = self._run_fusion(opt, a, b)
        for r in results:
            self.assertNotIn("score_0", r)
            self.assertNotIn("score_1", r)

    def test_limit_caps_results(self):
        opt = """
            {"fusion":"unionall","scorer":"rrf","score":true,"limit":2,
             "args":[{},{}]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10},{"id":"k02","score":20},{"id":"k03","score":30}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60},{"id":"k12","score":70}] AS r1)'
        results = self._run_fusion(opt, a, b)
        self.assertEqual(len(results), 2)

    def test_weight_skews_ordering(self):
        opt_balanced = """
            {"fusion":"unionall","scorer":"rrf","score":true,"limit":4,
             "args":[
                {"weight":1.0},
                {"weight":1.0}
             ]}
        """
        opt_heavy_first = """
            {"fusion":"unionall","scorer":"rrf","score":true,"limit":4,
             "args":[
                {"weight":10.0},
                {"weight":1.0}
             ]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10},{"id":"k02","score":20}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60},{"id":"k12","score":70}] AS r1)'
        balanced = self._run_fusion(opt_balanced, a, b)
        heavy = self._run_fusion(opt_heavy_first, a, b)
        self.assertNotEqual([r["id"] for r in balanced], [r["id"] for r in heavy])

    def test_penalty_changes_rrf_ranking(self):
        opt_small = """
            {"fusion":"unionall","scorer":"rrf","score":true,"limit":4,
             "args":[{"penalty":1},{"penalty":1}]}
        """
        opt_large = """
            {"fusion":"unionall","scorer":"rrf","score":true,"limit":4,
             "args":[{"penalty":1000},{"penalty":1000}]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10},{"id":"k02","score":20}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60},{"id":"k12","score":70}] AS r1)'
        small = self._run_fusion(opt_small, a, b)
        large = self._run_fusion(opt_large, a, b)
        self.assertGreater(small[0]["score"], large[0]["score"])

    def test_aggregator_sum_dedupes_within_input(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":4,
             "args":[{"aggregator":"sum","isScore":true},{"aggregator":"sum","isScore":true}]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10},{"id":"k01","score":20}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        results = self._run_fusion(opt, a, b)
        k01 = [r for r in results if r["id"] == "k01"]
        self.assertEqual(len(k01), 1)
        self.assertEqual(k01[0]["score"], 30)

    def test_aggregator_max_dedupes_within_input(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":4,
             "args":[{"aggregator":"max","isScore":true},{"aggregator":"max","isScore":true}]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10},{"id":"k01","score":20}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        results = self._run_fusion(opt, a, b)
        k01 = [r for r in results if r["id"] == "k01"]
        self.assertEqual(len(k01), 1)
        self.assertEqual(k01[0]["score"], 20)

    def test_pathid_pathscore_custom_fields(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":4,
             "args":[
                {"pathId":"docId","pathScore":"relevance","isScore":true},
                {"pathId":"docId","pathScore":"relevance","isScore":true}
             ]}
        """
        a = '(SELECT RAW r1 FROM [{"docId":"k01","relevance":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"docId":"k11","relevance":60}] AS r1)'
        results = self._run_fusion(opt, a, b)
        ids = sorted(r["id"] for r in results)
        self.assertEqual(ids, ["k01", "k11"])

    # ============================================================
    # Fusion with WHERE-clause filters on the loaded auxiliary fields
    # (price, null_field, nested, brand, size)
    # ============================================================

    # only docs that have a price field (i % 5 != 0 in the loader)
    def test_rrf_fts_dense_filtered_by_price_present(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":20,"args":[{},{}]}'
        fts = self._fts_subquery(term="nike", where="t.price IS NOT MISSING")
        dense = self._dense_subquery(where="price IS NOT MISSING")
        results = self._run_fusion(opt, fts, dense)
        self.assertTrue(len(results) > 0)
        for r in results:
            doc = self.run_cbq_query(f"SELECT RAW price FROM `{self.bucket}` USE KEYS '{r['id']}'")
            self.assertNotEqual(doc["results"], [None])

    # price bucket filter mirrors the [100,150,200,250] distribution
    def test_rrf_dense_sparse_filtered_by_price_range(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":20,"args":[{},{}]}'
        dense = self._dense_subquery(where="price >= 150 AND price <= 200")
        sparse_q = self._sparse_subquery(where="price >= 150 AND price <= 200")
        results = self._run_fusion(opt, dense, sparse_q)
        self.assertTrue(len(results) > 0)

    # brand text filter on FTS arm, brand+size composite filter on vector arm
    def test_rrf_fts_dense_brand_size_composite(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":20,"args":[{},{}]}'
        fts = self._fts_subquery(term="adidas")
        dense = self._dense_subquery(where='brand = "adidas" AND size = 8')
        results = self._run_fusion(opt, fts, dense)
        self.assertTrue(len(results) > 0)

    # the new brands added (reebok, puma, asics) should be addressable
    def test_rrf_dense_sparse_new_brand_filter(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":20,"args":[{},{}]}'
        dense = self._dense_subquery(where='brand IN ["reebok","puma","asics"]')
        sparse_q = self._sparse_subquery(where='brand IN ["reebok","puma","asics"]')
        results = self._run_fusion(opt, dense, sparse_q)
        self.assertTrue(len(results) > 0)

    # array predicate over the nested.b array, mirrors test_ann_array_any
    def test_rrf_sparse_nested_array_any(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":20,"args":[{},{}]}'
        sparse_a = self._sparse_subquery(where="ANY b IN nested.b SATISFIES b > 1 END")
        sparse_b = self._sparse_subquery(query_idx=1, where="ANY b IN nested.b SATISFIES b > 1 END")
        results = self._run_fusion(opt, sparse_a, sparse_b)
        self.assertTrue(len(results) > 0)

    # null_field IS NOT MISSING — mirrors test_ann_nulls
    def test_rsf_fts_dense_null_field_present(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":20,
             "args":[
                {"normalization":"minMaxScaler","isScore":true},
                {"normalization":"minMaxScaler","isScore":false}
             ]}
        """
        fts = self._fts_subquery(term="nike", where="t.null_field IS NOT MISSING")
        dense = self._dense_subquery(where="null_field IS NOT MISSING")
        results = self._run_fusion(opt, fts, dense)
        self.assertTrue(len(results) > 0)

    # filter on nested object's scalar member
    def test_rrf_dense_sparse_nested_scalar(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":20,"args":[{},{}]}'
        dense = self._dense_subquery(where="nested.a = 1")
        sparse_q = self._sparse_subquery(where="nested.a = 1")
        results = self._run_fusion(opt, dense, sparse_q)
        self.assertTrue(len(results) > 0)

    # all three arms filtered, brand variance across the new brand set
    def test_rrf_three_inputs_all_filtered(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":15,"args":[{},{},{}]}'
        where = 'size IN [6,7,8] AND price IS NOT MISSING'
        fts = self._fts_subquery(term="puma", where=f"t.size IN [6,7,8] AND t.price IS NOT MISSING")
        dense = self._dense_subquery(where=where)
        sparse_q = self._sparse_subquery(where=where)
        results = self._run_fusion(opt, fts, dense, sparse_q)
        self.assertLessEqual(len(results), 15)

    # ============================================================
    # CTE-bound input arms
    #
    # Each test hoists the per-arm subqueries into named CTEs in the outer
    # WITH clause and references them positionally inside reciprocal_fusion().
    # Validation is identical to the inline-subquery variants: each arm is
    # also run on its own to compute the expected fusion result.
    # ============================================================

    def test_rrf_dummy_via_cte(self):
        opt = """
            {"fusion":"unionall","scorer":"rrf","score":true,"limit":4,
             "args":[
                {"aggregator":"sum","normalization":"none","weight":1.0,"penalty":60,
                 "pathId":"id","pathScore":"score","isScore":true},
                {"aggregator":"sum","normalization":"none","weight":1.0,"penalty":60,
                 "pathId":"id","pathScore":"score","isScore":true}
             ]}
        """
        a = 'SELECT RAW r1 FROM [{"id":"k01","score":10},{"id":"k02","score":20},{"id":"k03","score":30}] AS r1'
        b = 'SELECT RAW r1 FROM [{"id":"k11","score":60},{"id":"k12","score":70},{"id":"k13","score":80}] AS r1'
        results = self._run_fusion_with_ctes(opt, q1=a, q2=b)
        expected = [
            {"id": "k11", "score": 1/60, "score_1": 1/60},
            {"id": "k01", "score": 1/60, "score_0": 1/60},
            {"id": "k12", "score": 1/61, "score_1": 1/61},
            {"id": "k02", "score": 1/61, "score_0": 1/61},
        ]
        self._assert_fusion_equal(results, expected)

    def test_rrf_fts_dense_via_cte(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":10,"args":[{},{}]}'
        fts, dense = self._fts_subquery(), self._dense_subquery()
        results = self._run_fusion_with_ctes(opt, fts_arm=fts, dense_arm=dense)
        expected = self._rrf_expected(
            [self._run_arm(fts), self._run_arm(dense)], limit=10
        )
        self._assert_fusion_equal(results, expected)

    def test_rrf_dense_sparse_via_cte(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":10,"args":[{},{}]}'
        dense, sparse_q = self._dense_subquery(), self._sparse_subquery()
        results = self._run_fusion_with_ctes(opt, dense_arm=dense, sparse_arm=sparse_q)
        expected = self._rrf_expected(
            [self._run_arm(dense), self._run_arm(sparse_q)], limit=10
        )
        self._assert_fusion_equal(results, expected)

    def test_rsf_dense_sparse_via_cte(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":10,
             "args":[
                {"normalization":"minMaxScaler","isScore":false},
                {"normalization":"minMaxScaler","isScore":false}
             ]}
        """
        dense, sparse_q = self._dense_subquery(), self._sparse_subquery()
        results = self._run_fusion_with_ctes(opt, dense_arm=dense, sparse_arm=sparse_q)
        expected = self._rsf_expected_minmax(
            [self._run_arm(dense), self._run_arm(sparse_q)],
            is_scores=[False, False], limit=10,
        )
        self._assert_fusion_equal(results, expected)

    def test_rrf_three_inputs_via_cte(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":15,"args":[{},{},{}]}'
        fts, dense, sparse_q = self._fts_subquery(), self._dense_subquery(), self._sparse_subquery()
        results = self._run_fusion_with_ctes(
            opt, fts_arm=fts, dense_arm=dense, sparse_arm=sparse_q
        )
        expected = self._rrf_expected(
            [self._run_arm(fts), self._run_arm(dense), self._run_arm(sparse_q)],
            limit=15,
        )
        self._assert_fusion_equal(results, expected)

    # CTE arms with WHERE-clause filters applied per-arm.
    def test_rrf_fts_dense_filtered_via_cte(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":true,"limit":10,"args":[{},{}]}'
        fts = self._fts_subquery(term="adidas", where="t.price IS NOT MISSING")
        dense = self._dense_subquery(where='brand = "adidas" AND price IS NOT MISSING')
        results = self._run_fusion_with_ctes(opt, fts_arm=fts, dense_arm=dense)
        expected = self._rrf_expected(
            [self._run_arm(fts), self._run_arm(dense)], limit=10
        )
        self._assert_fusion_equal(results, expected)

    # ============================================================
    # Negative cases
    # ============================================================

    def test_invalid_scorer(self):
        opt = '{"fusion":"unionall","scorer":"bogus","args":[{},{}]}'
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        with self.assertRaises(CBQError):
            self._run_fusion(opt, a, b)

    def test_invalid_normalization(self):
        opt = '{"fusion":"unionall","scorer":"rsf","args":[{"normalization":"bogus"},{}]}'
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        with self.assertRaises(CBQError):
            self._run_fusion(opt, a, b)

    def test_invalid_aggregator(self):
        opt = '{"fusion":"unionall","scorer":"rsf","args":[{"aggregator":"bogus"},{}]}'
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        with self.assertRaises(CBQError):
            self._run_fusion(opt, a, b)

    def test_invalid_fusion_type(self):
        opt = '{"fusion":"bogus","scorer":"rrf","args":[{},{}]}'
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        with self.assertRaises(CBQError):
            self._run_fusion(opt, a, b)

    def test_missing_options_argument(self):
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        self.query = f"SELECT RAW r FROM reciprocal_fusion({a}, {b}) AS r"
        self.log.info(f"[ranking_fusion] negative test (missing options):\n{self.query}")
        with self.assertRaises(CBQError):
            self.run_cbq_query()

    def test_no_input_subqueries(self):
        self.query = 'SELECT RAW r FROM reciprocal_fusion({"fusion":"unionall","scorer":"rrf","args":[]}) AS r'
        self.log.info(f"[ranking_fusion] negative test (no arms):\n{self.query}")
        with self.assertRaises(CBQError):
            self.run_cbq_query()

    def test_negative_limit(self):
        opt = '{"fusion":"unionall","scorer":"rrf","limit":-1,"args":[{},{}]}'
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        with self.assertRaises(CBQError):
            self._run_fusion(opt, a, b)

    def test_negative_penalty(self):
        opt = '{"fusion":"unionall","scorer":"rrf","args":[{"penalty":-10},{"penalty":-10}]}'
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        with self.assertRaises(CBQError):
            self._run_fusion(opt, a, b)

    def test_non_boolean_score_flag(self):
        opt = '{"fusion":"unionall","scorer":"rrf","score":42,"args":[{},{}]}'
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        with self.assertRaises(CBQError):
            self._run_fusion(opt, a, b)

    def test_pathid_points_at_missing_field(self):
        opt = """
            {"fusion":"unionall","scorer":"rsf","score":true,"limit":4,
             "args":[
                {"pathId":"nonexistent","pathScore":"score","isScore":true},
                {"pathId":"nonexistent","pathScore":"score","isScore":true}
             ]}
        """
        a = '(SELECT RAW r1 FROM [{"id":"k01","score":10}] AS r1)'
        b = '(SELECT RAW r1 FROM [{"id":"k11","score":60}] AS r1)'
        results = self._run_fusion(opt, a, b)
        self.assertEqual(results, [])
