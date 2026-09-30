import base64
import concurrent.futures
import datetime
import decimal
import functools
import gzip
import itertools
import json
import random
import re
import time
from collections import Counter

from collection.collections_n1ql_client import CollectionsN1QL
from lib.Cb_constants.CBServer import CbServer
from membase.api.exception import CBQError
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from security.ntonencryptionBase import ntonencryptionBase
from sdk_client3 import SDKClient
from tuqquery.tuq import (MIB, POLYMORPHIC_VALUE_TYPE, SEQSCAN_OFF_BIT, SEQUENTIAL_SCAN,
                          QueryTests)

# queryN1QLFeatCtrl bits.
LARGE_INLIST_SPANS_BIT = 0x2000000  # IN-list span limit 8192 -> 128K (MB-64696, MB-67805)
CBO_WITHOUT_STATS_BIT = 0x4000000   # CBO plans without statistics (MB-67035 repro, MB-65619)
IGNORE_IDXR_META_BIT = 0x800000     # bit 23, N1QL_IGNORE_IDXR_META (MB-74774, MB-74849)
DEFAULT_INLIST_SPAN_LIMIT = 8192

# The suite has no primary index and no sequential scan: this _type index is the
# reference access path for result checks.
REF_INDEX = 'ix_ref_type'
# DocLoader keys: KEY_PREFIX zero-padded to KEY_SIZE characters, e.g. reg-0000000000000042.
KEY_PREFIX = 'reg-'
KEY_SIZE = 20

SYNTAX_ERROR_CODE = 3000
MB69973_ERROR_CODE = 10106
MB69973_ERROR_TEXT = 'ns_server _metakv returned: 500'
MB75052_ERROR_CODE = 5000
MB75052_ERROR_TEXT = 'Requested Partial Aggr false Not Supported For Given Scan'
MEMORY_QUOTA_ERROR_CODE = 5500      # execution.memory_quota.exceeded (request quota)
KV_CONNECTION_ERROR_CODE = 12008    # the query service could not read from KV
# The application reads of the KV-restart test: documents of one _type, one group.
TYPE_FETCH_SQL = "SELECT META(d).id AS k, d AS v FROM {} AS d WHERE d._type = $t"
GROUP_SCAN_SQL = "SELECT META(c).id AS k, c AS v FROM {} AS c WHERE c.grp = $g"

# Characters for the MB-69973 UDF names. Every name gets a Latin-1 one (the bug's range).
NON_ASCII_CHARS = {
    'latin1': 'àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿßÀÁÂÄÅÆÇÉÊÑÖØÜÝ',
    'latin_ext': 'ąćčďęěğıłńňőřśşšťůűźżžĀĒĪŌŪ',
    'greek': 'αβγδεζηθλμξπρστφχψωΔΣΩ',
    'cyrillic': 'абвгдежзийклмнопрстуфхцчшщюяЖЩЯ',
    'cjk': '函数测试数据查询索引名字',
    'other': 'אבגדהابتثجअआइकखกขคぁあいアイウ한글',
}


def _path(doc, *names):
    """Nested field, or None when a step is missing or not an object (N1QL MISSING)."""
    for name in names:
        if not isinstance(doc, dict):
            return None
        doc = doc.get(name)
    return doc


def _iso_to_millis(ts):
    """Epoch millis of an ISO-8601 timestamp, fraction rounded half-up to milliseconds."""
    match = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)', ts)
    whole, fraction, zone = match.groups()
    seconds = datetime.datetime.strptime(
        whole + ('+00:00' if zone == 'Z' else zone), '%Y-%m-%dT%H:%M:%S%z').timestamp()
    millis = decimal.Decimal('0.' + (fraction or '0')) * 1000
    return int(seconds) * 1000 + int(millis.quantize(decimal.Decimal(1), decimal.ROUND_HALF_UP))


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


class QueryCbseTests(QueryTests):

    def setUp(self):
        super().setUp()
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)
        self.query_bucket = self.query_buckets[0]
        self._written = {}
        self._scopes = set()
        # suite_setUp is where sequential scans get turned off.
        if self._testMethodName not in ('suite_setUp', 'suite_tearDown'):
            self.wait_cluster_healthy("the previous test", timeout=120)
            self.wait_feat_ctrl_bit(SEQSCAN_OFF_BIT, True)

    def suite_setUp(self):
        super().suite_setUp()
        bucket = self.buckets[0]
        # Cross-node checks and index replicas need 2 query and 2 index nodes.
        self.require_two_nodes()
        # No primary index (the conf sets skip_primary_index=True, so QueryTests.setUp does
        # not create one) and no sequential scan: every query needs a secondary index.
        primary = self.run_cbq_query("SELECT RAW name FROM system:indexes "
                                     "WHERE is_primary = true")['results']
        self.assertEqual(primary, [], "Primary indexes exist: {}".format(primary))
        type(self)._orig_feat_ctrl = self.disable_sequential_scan()
        # PolymorphicDoc data only (the conf runs with skip_load=True).
        value_type = self.input.param("value_type", POLYMORPHIC_VALUE_TYPE)
        num_docs = self._num_docs()
        self.load_polymorphic_docs(bucket, num_docs, value_type=value_type, key_prefix=KEY_PREFIX)
        replicas = 1
        self.run_cbq_query('CREATE INDEX {} ON `{}`(_type) WITH {{"num_replica": {}}}'.format(
            REF_INDEX, bucket.name, replicas))
        self._wait_for_index_online(bucket, REF_INDEX)
        # Exact item count, every key readable over KV, and the per-type counts from the
        # query service equal the ones from the documents.
        self.assert_item_count(bucket, num_docs)
        kv_docs = self._kv_dataset()
        n1ql_counts = self.verify_polymorphic_distribution(bucket, num_docs, value_type=value_type)
        kv_counts = dict(Counter(doc.get('_type') for doc in kv_docs.values()))
        self.assertEqual(n1ql_counts, kv_counts,
                         "Per-type counts differ: query {} vs KV {}".format(n1ql_counts, kv_counts))
        self._validate_indexes([self._index_spec(
            REF_INDEX, "_type IS NOT MISSING", sum(1 for d in kv_docs.values() if '_type' in d),
            replicas=replicas)])

    def suite_tearDown(self):
        original = getattr(type(self), '_orig_feat_ctrl', None)
        if original is None:
            self.set_feat_ctrl_bit(SEQSCAN_OFF_BIT, False)
        else:
            self.set_n1ql_feat_ctrl(original)
        super().suite_tearDown()

    # --- shared helpers ---

    def _num_docs(self):
        return int(self.input.param("num_docs", 20000))

    def _kv_dataset(self):
        """{key: document} for every loaded key, read once per suite over KV (SDK).
        Fails if any key is missing."""
        cls = type(self)
        if getattr(cls, '_kv_docs', None) is None:
            num_docs = self._num_docs()
            keys = [KEY_PREFIX + str(i).rjust(KEY_SIZE - len(KEY_PREFIX), '0')
                    for i in range(num_docs)]
            client = SDKClient(bucket=self.buckets[0].name, scheme="couchbase",
                               hosts=[self.master.ip], username=self.master.rest_username,
                               password=self.master.rest_password)
            docs = {}
            try:
                for i in range(0, num_docs, 1000):
                    # SDK 4 MultiGetResult: .results holds the keys that were found.
                    found = client.default_collection.get_multi(keys[i:i + 1000]).results
                    for key, result in found.items():
                        docs[key] = result.content_as[dict]
            finally:
                client.close()
            missing = [k for k in keys if k not in docs]
            self.assertFalse(missing, "{} of {} keys not readable over KV, e.g. {}".format(
                len(missing), num_docs, missing[:5]))
            self.log.info("Read %d documents over KV for result validation", len(docs))
            cls._kv_docs = docs
        return cls._kv_docs

    def _kv_keys(self, matches):
        """Keys of the loaded documents for which `matches` is true."""
        return {key for key, doc in self._kv_dataset().items() if matches(doc)}

    def _insert_docs(self, docs, inserted, batch=500, keyspace=None):
        """INSERT {key: document} with VALUES (no index needed); every document must be
        written. Written keys are appended to `inserted` for the cleanup. The statements
        are not logged (debug_query=False): one summary line instead of every document."""
        items = list(docs.items())
        keyspace = keyspace or self.query_bucket
        for i in range(0, len(items), batch):
            chunk = items[i:i + batch]
            values = ", ".join("({}, {})".format(json.dumps(k), json.dumps(v)) for k, v in chunk)
            self.assert_mutations(self.run_cbq_query("INSERT INTO {} (KEY, VALUE) VALUES {}".format(
                keyspace, values), debug_query=False), len(chunk), "INSERT")
            inserted.extend(k for k, _ in chunk)
        self.log.info("INSERT %d documents into %s (e.g. %s)", len(items), keyspace,
                      items[0] if items else None)

    def _delete_keys(self, keys, batch=500):
        """DELETE ... USE KEYS (no index needed); every key must be deleted."""
        keys = list(keys)
        for i in range(0, len(keys), batch):
            chunk = keys[i:i + batch]
            self.assert_mutations(self.run_cbq_query("DELETE FROM {} USE KEYS {}".format(
                self.query_bucket, json.dumps(chunk)), debug_query=False), len(chunk), "DELETE")
        self.log.info("DELETE %d documents from %s by key", len(keys), self.query_bucket)

    def _insert_generated(self, doc_type, fields, num):
        """INSERT num documents of doc_type generated server-side from ARRAY_RANGE (no
        keyspace is read); `fields` are the other fields, as expressions over d."""
        result = self.run_cbq_query(
            'INSERT INTO {} (KEY k, VALUE v) SELECT "{}::" || TO_STR(d) AS k, '
            '{{"_type": {}, {}}} AS v FROM ARRAY_RANGE(0, {}) AS d'.format(
                self.query_bucket, doc_type, self.n1ql_literal(doc_type), fields, num))
        self._written[doc_type] = result['metrics'].get('mutationCount')
        self.assert_mutations(result, num, "INSERT")

    def _delete_generated(self, doc_type):
        """DELETE every document of doc_type (via REF_INDEX), also leftovers of a failed
        run; when this run's INSERT succeeded, exactly the documents it wrote."""
        delete = "DELETE FROM {} WHERE _type = {}".format(self.query_bucket,
                                                          self.n1ql_literal(doc_type))
        self.assert_index_plan(self.explain(delete), REF_INDEX)
        result = self.run_cbq_query(delete)
        written = self._written.pop(doc_type, None)
        if written is not None:
            self.assert_mutations(result, written, "DELETE")

    def _create_scope(self, scope, collections, indexes=()):
        """scope with its collections and (name, collection, definition[, with]) indexes in
        the suite bucket; returns {collection: keyspace}. Dropped with _drop_scope."""
        bucket = self.buckets[0].name
        helper = CollectionsN1QL(self.master)
        self.assertTrue(helper.create_scope(bucket_name=bucket, scope_name=scope),
                        "Scope {} was not created".format(scope))
        self._scopes.add(scope)
        for name in collections:
            self.assertTrue(helper.create_collection(bucket_name=bucket, scope_name=scope,
                                                     collection_name=name),
                            "Collection {}.{} was not created".format(scope, name))
        ks = {n: 'default:`{}`.`{}`.`{}`'.format(bucket, scope, n) for n in collections}
        for index, collection, definition, *with_ in indexes:
            self.run_cbq_query("CREATE INDEX {} ON {}{} USING {}{}".format(
                index, ks[collection], definition, self.index_type,
                " WITH {}".format(with_[0]) if with_ else ""))
            # system:indexes reports a collection index with keyspace_id = collection.
            self._wait_for_index_online(collection, index)
        return ks

    def _drop_scope(self, scope):
        if scope in self._scopes:
            self._scopes.discard(scope)
            CollectionsN1QL(self.master).delete_scope(bucket_name=self.bucket_name,
                                                      scope_name=scope)

    # ==================================================================
    # PREPARED PLAN STABILITY: across disturbances, the prepared plan keeps its index
    # (no sequential or primary scan) and returns the rows REF_INDEX returns.
    # ==================================================================

    ALL_DISTURBANCES = ('cbq_kill', 'server_restart')

    def _expected_rows(self, matches):
        """SELECT * rows for the KV documents where `matches` (the WHERE clause) is true."""
        bucket = self.buckets[0].name
        return [{bucket: doc} for doc in self._kv_dataset().values() if matches(doc)]

    def _assert_rows(self, rows, what):
        self.assert_same_rows(rows, self._expected, what + " (expected rows from the documents)")

    def _reference_rows(self, reference_query):
        rows = self.run_cbq_query(reference_query)['results']
        self._assert_rows(rows, "reference query via {}".format(REF_INDEX))
        return rows

    def _check_prepared(self, stmt_name, query, index_name, reference_query, force=False):
        """PREPARE (FORCE) uses index_name; EXECUTE returns the reference rows."""
        # The PREPARE response carries the plan (there is no EXPLAIN EXECUTE).
        plan = self.PreparePlanHelper(self.run_cbq_query(
            "PREPARE {}{} AS {}".format("FORCE " if force else "", stmt_name, query)))
        self.assert_index_plan(plan, index_name)
        self._check_execute(stmt_name, index_name, reference_query)

    def _check_execute(self, stmt_name, index_name, reference_query):
        """EXECUTE (which may re-prepare inside the server after a disturbance) ran
        through index_name, read from its execution timings, and returns the rows."""
        self._reference_rows(reference_query)
        result = self.run_cbq_query("EXECUTE {}".format(stmt_name),
                                    query_params={'profile': 'timings'})
        self.assert_index_plan(result['profile']['executionTimings'], index_name)
        self._assert_rows(result['results'], "EXECUTE {}".format(stmt_name))

    def _check_plain_query(self, query_plain, index_name, reference_query):
        """No hint: our index is picked, CBO does not regress, nodes agree, and the
        rows are right with CBO on and off."""
        self.assert_index_plan(self.explain(query_plain), index_name)
        self.assert_no_cbo_index_regression(query_plain)
        self.assert_same_plan_across_nodes(query_plain)
        self._reference_rows(reference_query)
        # Rows from the documents with CBO on and with CBO off.
        for use_cbo in (True, False):
            self._assert_rows(self.run_cbq_query(query_plain, query_params={'use_cbo': use_cbo})
                              ['results'], "plain query, use_cbo={}".format(use_cbo))

    def _disturb(self, disturbance, index_spec):
        """cbq_kill: kill cbq-engine on the master, wait for the new one. server_restart:
        restart couchbase-server on every node in turn (a rolling restart), then the
        index must hold the same items and serve the same scan."""
        if disturbance == 'cbq_kill':
            self.restart_query_service(self.master)
        elif disturbance == 'server_restart':
            for node in self.get_nodes_in_cluster():
                self.restart_couchbase_server(node)
            self._wait_for_index_online(self.buckets[0], index_spec['name'])
            self._validate_indexes([index_spec], restart=False)
        else:
            self.fail("Unknown disturbance: {}".format(disturbance))

    def _run_prepared_plan_sweep(self, ddl, index_name, stmt_name, where, matches, index_spec,
                                 disturbances=None):
        """Check the prepared and plain plans and rows, then after each disturbance:
        EXECUTE the existing statement, re-PREPARE FORCE, and check the plain query again."""
        if disturbances is None:
            param = self.input.param("disturbances", ",".join(self.ALL_DISTURBANCES))
            disturbances = [d for d in param.split(",") if d]
        b = self.query_bucket
        query_hint = "SELECT * FROM {} USE INDEX ({}) {}".format(b, index_name, where)
        query_plain = "SELECT * FROM {} {}".format(b, where)
        reference = "SELECT * FROM {} USE INDEX ({}) {}".format(b, REF_INDEX, where)
        self.require_two_nodes()
        self._expected = self._expected_rows(matches)
        self.assertTrue(self._expected, "No document matches {} - the template values in "
                        "this test no longer fit the loaded data".format(where))
        self.log.info("%s: %d documents match (from KV)", stmt_name, len(self._expected))
        try:
            self._create_index(ddl, index_name)
            self._validate_indexes([index_spec])
            self._check_prepared(stmt_name, query_hint, index_name, reference)
            self._check_plain_query(query_plain, index_name, reference)
            for disturbance in disturbances:
                self.log.info("=== %s: disturbance %s ===", stmt_name, disturbance)
                self._disturb(disturbance, index_spec)
                self._check_execute(stmt_name, index_name, reference)
                self._check_prepared(stmt_name, query_hint, index_name, reference, force=True)
                self._check_plain_query(query_plain, index_name, reference)
        finally:
            self.run_cbq_query("DELETE FROM system:prepareds WHERE name = '{}'".format(stmt_name))
            self.drop_index_safe(self.bucket_name, index_name)

    def _random_doc(self, rng, matches, what):
        """A random loaded document (read over KV) for which `matches` is true."""
        docs = self._kv_dataset()
        keys = sorted(k for k, d in docs.items() if matches(d))
        self.assertTrue(keys, "{}: no loaded document fits - did the template change?".format(what))
        return docs[rng.choice(keys)]

    def test_mb70453_prepared_no_sequentialscan(self):
        """MB-70453 (CBSE-22009, CBSE-22033): when the query service could not get the
        collections manifest (no retry, no discarded connection on an i/o timeout), a
        prepared statement fell back to #sequentialscan although a matching index
        existed. This test covers the prepared-plan side; the manifest-after-KV-restart
        side is in test_mb72084_kv_restart_query_connections_recover. storeId and the
        BusinessUnit lower bound are taken from a random transaction document (seed
        logged)."""
        rng, _ = self.seeded_rng("MB-70453")

        def bu(d):
            return _path(d, 'BusinessUnit', 'UnitID', '_value')

        doc = self._random_doc(rng, lambda d: (d.get('_type') == 'transaction'
                                               and isinstance(d.get('storeId'), str)
                                               and isinstance(bu(d), str)), "MB-70453")
        store = doc['storeId']
        low = rng.choice(sorted({bu(d) for d in self._kv_dataset().values()
                                 if isinstance(bu(d), str) and bu(d) <= bu(doc)}))
        self._run_prepared_plan_sweep(
            ddl=("CREATE INDEX ix_mb70453 ON {b}(storeId, BusinessUnit.UnitID._value) "
                 "WHERE _type = 'transaction' USING {t}").format(
                b=self.query_bucket, t=self.index_type),
            index_name='ix_mb70453', stmt_name='mb70453_stmt',
            where="WHERE _type = 'transaction' AND storeId = {} "
                  "AND BusinessUnit.UnitID._value >= {}".format(self.n1ql_literal(store), self.n1ql_literal(low)),
            matches=lambda d: (d.get('_type') == 'transaction'
                               and d.get('storeId') == store
                               and isinstance(bu(d), str) and bu(d) >= low),
            index_spec=self._index_spec(
                'ix_mb70453', "_type = 'transaction' AND storeId IS NOT MISSING",
                sum(1 for d in self._kv_dataset().values()
                    if d.get('_type') == 'transaction' and 'storeId' in d)))

    def test_mb66050_prepared_useindex_array_stability(self):
        """MB-66050 (CBSE-21144, CBSE-19599):
        a flatten_keys array index stopped being used, even with USE INDEX.
        lastName and the msg.ts window come from a random profile document (seed logged)."""
        rng, _ = self.seeded_rng("MB-66050")

        def messages(d):
            return [m for m in (d.get('msg') if isinstance(d.get('msg'), list) else [])
                    if _is_number(_path(m, 'ts')) and _path(m, 'text') is not None]

        doc = self._random_doc(rng, lambda d: (d.get('_type') == 'profile'
                                               and isinstance(d.get('lastName'), str)
                                               and messages(d)), "MB-66050")
        last_name = doc['lastName']
        profiles = [d for d in self._kv_dataset().values()
                    if d.get('_type') == 'profile' and 'lastName' in d]
        ts = rng.choice(messages(doc))['ts']
        low, high = ts - rng.randint(0, 90) * 86400000, ts + rng.randint(0, 90) * 86400000
        self._run_prepared_plan_sweep(
            ddl=("CREATE INDEX ix_mb66050 ON {b}(lastName, "
                 "(ALL ARRAY FLATTEN_KEYS(m.`ts`, m.`text`) FOR m IN msg END), firstName) "
                 "WHERE _type = 'profile' USING {t}").format(
                b=self.query_bucket, t=self.index_type),
            index_name='ix_mb66050', stmt_name='mb66050_stmt',
            where="WHERE _type = 'profile' AND lastName = {} AND ANY m IN msg SATISFIES "
                  "m.ts BETWEEN {} AND {} AND m.`text` IS VALUED END".format(
                      self.n1ql_literal(last_name), low, high),
            matches=lambda d: (d.get('_type') == 'profile'
                               and d.get('lastName') == last_name
                               and any(low <= m['ts'] <= high for m in messages(d))),
            # One index item per msg element (ALL array key); a document with no msg
            # still has one item for its lastName.
            index_spec=self._index_spec(
                'ix_mb66050', "_type = 'profile' AND lastName IS NOT MISSING",
                len(profiles), sum(max(1, len(d['msg']) if isinstance(d.get('msg'), list) else 1)
                                   for d in profiles)))

    def test_mb73680_prepared_plan_stability_after_qsvc_restart(self):
        """MB-73680 (CBSE-23616): statements re-prepared right after a query-service restart fell
        back to #sequentialscan. The caseId range starts at a random media_element
        document's caseId (CNNNN ids; seed logged)."""
        rng, _ = self.seeded_rng("MB-73680")

        def case_id(d):
            c = d.get('caseId')
            return c if isinstance(c, str) and len(c) == 5 and c[0] == 'C' and c[1:].isdigit() else None

        doc = self._random_doc(rng, lambda d: d.get('_type') == 'media_element' and case_id(d),
                               "MB-73680")
        low = case_id(doc)
        high = 'C{:04d}'.format(min(9999, int(low[1:]) + rng.randint(10, 80)))
        self._run_prepared_plan_sweep(
            ddl=("CREATE INDEX ix_mb73680 ON {b}(caseId, libraryLabel) "
                 "WHERE _type = 'media_element' USING {t}").format(
                b=self.query_bucket, t=self.index_type),
            index_name='ix_mb73680', stmt_name='mb73680_stmt',
            where="WHERE _type = 'media_element' AND caseId BETWEEN {} AND {}".format(
                self.n1ql_literal(low), self.n1ql_literal(high)),
            matches=lambda d: (d.get('_type') == 'media_element'
                               and isinstance(d.get('caseId'), str)
                               and low <= d['caseId'] <= high),
            index_spec=self._index_spec(
                'ix_mb73680', "_type = 'media_element' AND caseId IS NOT MISSING",
                sum(1 for d in self._kv_dataset().values()
                    if d.get('_type') == 'media_element' and 'caseId' in d)),
            disturbances=['cbq_kill'])

    def test_mb74849_received_seqscan_plan_reprepared(self):
        """MB-74849 (CBSE-23765): a query node that briefly lost the index service
        re-prepared a statement to a sequential scan and distributed that plan to every
        query node (PUT /admin/prepareds/<name>). The receiving nodes verified it against
        their unchanged index metadata and kept the sequential scan until DELETE FROM
        system:prepareds. Repro from the MB: with sequential scans on and no index,
        PREPARE gives a sequential-scan plan; its encoded_plan is saved, the index is
        created (and validated), the prepared statement is deleted, and the saved plan
        is handed to one query node at a time as a distributed plan.
        The fix applies with feature bit 23 (0x800000, metadata-driven reprepare) set:
        EXECUTE there (and on every node that then holds the statement) must run an
        IndexScan3 on the new index, never #sequentialscan. With bit 23 clear (the
        default) neither part of the fix applies and the received plan is kept, as
        before: the default does not reprepare on index metadata changes (confirmed on
        MB-74849), so EXECUTE must still run #sequentialscan. What ran is checked in the
        profile of the run (EXPLAIN would plan again), with exactly the expected rows;
        $v and the data are random (seed logged)."""
        rng, seed = self.seeded_rng("MB-74849")
        scope, name, index = 's_mb74849', 'p_mb74849', 'ix_mb74849_a'
        num = rng.randint(300, 800)
        docs = {'mb74849::{}'.format(i): {'a': rng.randint(0, 20), 'n': i} for i in range(num)}
        self.require_two_nodes()
        nodes = self.get_query_nodes()
        bit23_before = bool(self.get_n1ql_feat_ctrl() & IGNORE_IDXR_META_BIT)
        seqscan_on = False

        def holders():
            return {row['node'].split(':')[0] for row in self.run_cbq_query(
                "SELECT p.node FROM system:prepareds AS p WHERE p.name = {}".format(
                    self.n1ql_literal(name)))['results']}

        def execute(node, what, replanned=True):
            """EXECUTE on node, exact rows. replanned: IndexScan3 on index and no
            sequential scan; otherwise the received #sequentialscan plan still runs."""
            v = rng.randint(0, 20)
            rows, ran = self._execute_prepared(name, node, {'$v': v})
            on_node = "MB-74849: EXECUTE {} on {} {} (seed={})".format(name, node.ip, what, seed)
            self._assert_ran_scan(ran, index, replanned, on_node)
            self.assert_same_rows(rows, [{'k': k, 'a': d['a']} for k, d in
                                         docs.items() if d['a'] == v], on_node)

        try:
            ks = self._create_scope(scope, ['docs'])['docs']
            self._insert_docs(docs, [], keyspace=ks)
            self.assert_item_count(self.buckets[0], num, scope, 'docs')
            # 1. No index: with sequential scans on, PREPARE gives a sequential scan.
            self.set_feat_ctrl_bit(SEQSCAN_OFF_BIT, False)
            seqscan_on = True
            self.run_cbq_query("DELETE FROM system:prepareds WHERE name = {}".format(
                self.n1ql_literal(name)))
            prepared = self.run_cbq_query(
                "PREPARE {} FROM SELECT META(c).id AS k, c.a FROM {} AS c WHERE c.a = $v".format(
                    name, ks), server=nodes[0])['results'][0]
            self.assertTrue(self.find_operators_using_index(prepared, SEQUENTIAL_SCAN),
                            "Without an index, PREPARE did not give a sequential scan:\n{}"
                            .format(self.format_plan(prepared)))
            # The PREPARE response carries an empty encoded_plan (gzip of nothing); the
            # node's cache entry holds the real one, as the MB's repro reads it.
            rest = RestConnection(nodes[0])
            status, content, _ = rest._http_request(
                rest.query_baseUrl + 'admin/prepareds/' + name, 'GET')
            self.assertTrue(status, "GET /admin/prepareds/{} on {} failed: {}".format(
                name, nodes[0].ip, content))
            seqscan_plan = json.loads(content).get('encoded_plan') or ''
            self.assertTrue(seqscan_plan and gzip.decompress(base64.b64decode(seqscan_plan)),
                            "No encoded plan for {} on {}: {}".format(name, nodes[0].ip,
                                                                      content[:500]))
            # 2. The index the received plan ignores.
            self.run_cbq_query("CREATE INDEX {} ON {}(a) USING {}".format(
                index, ks, self.index_type))
            self._wait_for_index_online('docs', index)
            self._validate_indexes([self._collection_index_spec(
                scope, 'docs', ks, index, "a IS NOT MISSING", num)])
            # 3. Each node in turn receives the sequential-scan plan. Bit 23 set: the fix
            # re-prepares it locally. Bit 23 clear (default): the plan is kept, as before.
            for bit23 in (True, False):
                self.set_feat_ctrl_bit(IGNORE_IDXR_META_BIT, bit23)
                for target in nodes:
                    what = "after receiving the sequential-scan plan (bit 23 {})".format(
                        "set" if bit23 else "clear")
                    self.run_cbq_query("DELETE FROM system:prepareds WHERE name = {}".format(
                        self.n1ql_literal(name)))
                    # The body is the encoded plan, as a node distributes it. Not sent as a
                    # form: the query service would parse it as form fields and the handler
                    # would read an empty body (4050 "unexpected end of JSON input").
                    rest = RestConnection(target)
                    status, content, _ = rest._http_request(
                        rest.query_baseUrl + 'admin/prepareds/' + name, 'PUT', seqscan_plan,
                        headers=rest._create_headers_encoded_prepared())
                    self.assertTrue(status, "PUT /admin/prepareds/{} on {} failed: {}".format(
                        name, target.ip, content))
                    self.assertIn(target.ip, holders(), "{} does not hold {} after the PUT"
                                  .format(target.ip, name))
                    execute(target, what, replanned=bit23)
                    # With bit 23 set, any node that now holds the statement (a distributed
                    # index-scan plan) must not run a sequential scan either.
                    if bit23:
                        for node in nodes:
                            if node.ip != target.ip and node.ip in holders():
                                execute(node, "after {} re-prepared it".format(target.ip))
        finally:
            self.run_cbq_query("DELETE FROM system:prepareds WHERE name = {}".format(
                self.n1ql_literal(name)))
            self.set_feat_ctrl_bit(IGNORE_IDXR_META_BIT, bit23_before)
            if seqscan_on:
                self.set_feat_ctrl_bit(SEQSCAN_OFF_BIT, True)
            self._drop_scope(scope)

    def _execute_prepared(self, name, node, query_params):
        """EXECUTE name on node with profile timings; returns (rows, what ran). What ran
        comes from the profile: EXPLAIN would plan the statement again."""
        result = self.run_cbq_query("EXECUTE {}".format(name), server=node,
                                    query_params=dict(query_params, profile='timings'))
        return result['results'], result['profile']['executionTimings']

    def _assert_ran_scan(self, ran, index, use_index, what):
        """ran (an EXECUTE profile) went through index with no sequential or primary scan
        (use_index: assert_index_plan), or is still the #sequentialscan plan."""
        self.log.info("%s: expecting %s", what, index if use_index else SEQUENTIAL_SCAN)
        if use_index:
            self.assert_index_plan(ran, index)
        else:
            self.assertTrue(self.find_operators_using_index(ran, SEQUENTIAL_SCAN),
                            "{}: #sequentialscan was expected (it was replaced; default "
                            "behaviour changed?):\n{}".format(what, self.format_plan(ran)))

    def test_mb74774_seqscan_prepared_reprepared_on_index_online(self):
        """MB-74774: a statement prepared while no index served it got a #sequentialscan
        plan and kept it after the index came online. With bit 23 (0x800000) set,
        Prepared.MetadataCheck() refreshes the plan's indexers and compares the GSI
        metadata version, but the sequential scan indexer's Refresh() was a no-op, so
        the cached version never moved. Steps from the MB's verification, on one query
        node with use_cbo off: PREPARE with no index (#sequentialscan), CREATE INDEX
        deferred, BUILD INDEX, EXECUTE while it builds, wait for Ready through ns_server's
        index status only, then EXECUTE. Nothing queries the collection or system:indexes
        in between: either refreshes the GSI indexer and would hide the bug.
        Bit 23 set: that EXECUTE (and the node's plan in system:prepareds) must use the
        index. Bit 23 clear (default): no reprepare on index metadata changes (as
        confirmed on MB-74849), so #sequentialscan must remain. Each index is validated
        after its pass. What ran is checked in the profile, with exact rows; $v and the
        data are random (seed logged)."""
        rng, seed = self.seeded_rng("MB-74774")
        scope, name = 's_mb74774', 'p_mb74774'
        num = rng.randint(30000, 50000)
        docs = {'mb74774::{}'.format(i): {'f1': rng.randint(0, 50), 'n': i} for i in range(num)}
        node = self.get_query_nodes()[0]
        rest = RestConnection(self.master)
        params = {'use_cbo': False}
        bit23_before = bool(self.get_n1ql_feat_ctrl() & IGNORE_IDXR_META_BIT)
        seqscan_on = False

        def index_status(index):
            # ns_server's view of the indexer; not a query, so it refreshes nothing.
            return rest.get_index_status().get(self.buckets[0].name, {}).get(index, {}).get('status')

        def execute(what):
            v = rng.randint(0, 50)
            rows, ran = self._execute_prepared(name, node, dict(params, **{'$v': v}))
            self.assert_same_rows(rows, [{'k': k, 'f1': d['f1']} for k, d in docs.items()
                                         if d['f1'] == v], what)
            return ran

        try:
            ks = self._create_scope(scope, ['docs'])['docs']
            self._insert_docs(docs, [], keyspace=ks)
            self.assert_item_count(self.buckets[0], num, scope, 'docs')
            self.set_feat_ctrl_bit(SEQSCAN_OFF_BIT, False)
            seqscan_on = True
            for bit23 in (True, False):
                index = 'ix_mb74774_bit23_{}'.format('set' if bit23 else 'clear')
                what = "MB-74774: EXECUTE {} on {} (bit 23 {}, seed={})".format(
                    name, node.ip, 'set' if bit23 else 'clear', seed)
                self.set_feat_ctrl_bit(IGNORE_IDXR_META_BIT, bit23)
                self.run_cbq_query("DELETE FROM system:prepareds WHERE name = {}".format(
                    self.n1ql_literal(name)))
                # 1. No index on f1: PREPARE gives a sequential scan.
                prepared = self.run_cbq_query(
                    "PREPARE {} FROM SELECT META(c).id AS k, c.f1 FROM {} AS c WHERE c.f1 = $v"
                    .format(name, ks), server=node, query_params=params)['results'][0]
                self.assertTrue(self.find_operators_using_index(prepared, SEQUENTIAL_SCAN),
                                "{}: without an index, PREPARE did not give a sequential "
                                "scan:\n{}".format(what, self.format_plan(prepared)))
                self._assert_ran_scan(execute(what + " with no index"), index, False,
                                      what + " with no index")
                # 2. Deferred index, built; EXECUTE while it is still building.
                self.run_cbq_query('CREATE INDEX {} ON {}(f1) USING {} WITH {{"defer_build": true}}'
                                   .format(index, ks, self.index_type), server=node)
                self.run_cbq_query("BUILD INDEX ON {}({})".format(ks, index), server=node)
                self._assert_ran_scan(execute(what + " while the index builds"), index, False,
                                      what + " while {} builds (status now {})".format(
                                          index, index_status(index)))
                # 3. Ready, seen through ns_server only: no query until the EXECUTE.
                deadline = time.time() + 600
                while index_status(index) != 'Ready':
                    self.assertLess(time.time(), deadline, "{}: {} not Ready after 600s ({})"
                                    .format(what, index, index_status(index)))
                    time.sleep(2)
                # 4. Bit 23 set: re-prepared onto the index (allow 30s for the query
                # node's GSI client to see it online). Clear: the sequential scan stays.
                if bit23:
                    deadline = time.time() + 30
                    while True:
                        ran = execute(what + " after {} is Ready".format(index))
                        if self.find_operators_using_index(ran, index) or time.time() > deadline:
                            break
                        time.sleep(2)
                    self._assert_ran_scan(ran, index, True, what + " after {} is Ready".format(index))
                    plans = [r['plan'] for r in self.run_cbq_query(
                        "SELECT p.node, META(p).plan AS plan FROM system:prepareds AS p "
                        "WHERE p.name = {}".format(self.n1ql_literal(name)), server=node)['results']
                        if r['node'].split(':')[0] == node.ip]
                    self.assertEqual(len(plans), 1, "{}: system:prepareds entries for {} on {}: {}"
                                     .format(what, name, node.ip, len(plans)))
                    self._assert_ran_scan(plans[0], index, True,
                                          what + ": plan in system:prepareds")
                else:
                    for attempt in range(3):
                        self._assert_ran_scan(
                            execute(what + " after {} is Ready".format(index)), index, False,
                            what + " after {} is Ready (run {})".format(index, attempt + 1))
                        time.sleep(2)
                # 5. Validate the index (this queries the collection and system:indexes).
                self._validate_indexes([self._collection_index_spec(
                    scope, 'docs', ks, index, "f1 IS NOT MISSING", num)])
                # DROP INDEX returns once the index is gone; the next PREPARE checks it.
                self.run_cbq_query("DROP INDEX {} ON {}".format(index, ks))
        finally:
            self.run_cbq_query("DELETE FROM system:prepareds WHERE name = {}".format(
                self.n1ql_literal(name)))
            self.set_feat_ctrl_bit(IGNORE_IDXR_META_BIT, bit23_before)
            if seqscan_on:
                self.set_feat_ctrl_bit(SEQSCAN_OFF_BIT, True)
            self._drop_scope(scope)

    # ==================================================================
    # DATE / TIME FUNCTIONS
    # ==================================================================

    def test_mb70305_str_to_millis_fraction_rounding(self):
        """MB-70305 (CBSE-21970): STR_TO_MILLIS lost a second when the fraction rounded up
        ('...20.9995Z' gave ...880000, not ...881000), so a later time could convert to
        a smaller number. Timestamps are stored in documents and read back with
        STR_TO_MILLIS: the cases from the MB, plus str_to_millis_docs random ones (seed
        logged; dates 2000-2099, 1-9 fraction digits, half of them on a rounding edge,
        UTC or an offset). Cross-checks: every value equals the one computed here
        (fraction rounded half-up to ms), converts back to itself through MILLIS_TO_UTC,
        and ORDER BY STR_TO_MILLIS(ts) gives the real order of the instants."""
        rng, seed = self.seeded_rng("MB-70305")
        exact = [
            ('2026-01-21T22:01:20.0004Z', 1769032880000),
            ('2026-01-21T22:01:20.0005Z', 1769032880001),
            ('2026-01-21T22:01:20.9994Z', 1769032880999),
            ('2026-01-21T22:01:20.9995Z', 1769032881000),   # the reported case
            ('2026-01-21T22:01:20.9999Z', 1769032881000),
            ('2026-01-21T22:01:59.9996Z', 1769032920000),   # into the next minute
            ('2026-12-31T23:59:59.9995Z', 1798761600000),   # into the next year
            ('2026-01-21T22:01:20.9995+05:30', 1769013081000),  # with a zone offset
        ]
        # In the order they happened: a later one must never convert to a smaller number.
        ordered = ['2026-01-21T22:01:20.9990Z', '2026-01-21T22:01:20.9994Z',
                   '2026-01-21T22:01:20.9995Z', '2026-01-21T22:01:20.9999Z',
                   '2026-01-21T22:01:21.0000Z']

        def random_ts():
            utc = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc) + \
                datetime.timedelta(seconds=rng.randrange(100 * 365 * 86400))
            offset = rng.choice([0, 0, 330, -300, 60, -480, 545])
            if rng.random() < 0.5:      # just past .9995: must round up into the next second
                fraction = '999' + str(rng.randint(5, 9)) + ''.join(
                    rng.choice('0123456789') for _ in range(rng.randint(0, 5)))
            else:
                fraction = ''.join(rng.choice('0123456789') for _ in range(rng.randint(1, 9)))
            local = utc + datetime.timedelta(minutes=offset)
            zone = 'Z' if not offset else '{}{:02d}:{:02d}'.format(
                '+' if offset > 0 else '-', abs(offset) // 60, abs(offset) % 60)
            return local.strftime('%Y-%m-%dT%H:%M:%S') + '.' + fraction + zone

        extra = [random_ts() for _ in range(int(self.input.param("str_to_millis_docs", 30)))]
        expected = dict(exact)
        expected.update((ts, _iso_to_millis(ts)) for ts in ordered + extra)
        # The computation used for the random ones agrees with the MB's values.
        self.assertEqual({ts: _iso_to_millis(ts) for ts, _ in exact}, dict(exact))
        doc_type = 'mb70305_event'
        docs = {'mb70305::{}::{}'.format(grp, i): {'_type': doc_type, 'grp': grp, 'seq': i, 'ts': ts}
                for grp, values in (('exact', [ts for ts, _ in exact]), ('ordered', ordered),
                                    ('random', extra))
                for i, ts in enumerate(values)}
        self.log.info("MB-70305: %d random timestamps, e.g. %s (seed=%s)", len(extra), extra[:3],
                      seed)
        b = self.query_bucket
        try:
            self._insert_docs(docs, [])
            converted = ("SELECT META(d).id AS k, d.ts, STR_TO_MILLIS(d.ts) AS ms, "
                         "STR_TO_MILLIS(MILLIS_TO_UTC(STR_TO_MILLIS(d.ts))) AS back "
                         "FROM {} d WHERE d._type = '{}'".format(b, doc_type))
            by_instant = ("SELECT RAW META(d).id FROM {} d WHERE d._type = '{}' "
                          "ORDER BY STR_TO_MILLIS(d.ts), META(d).id".format(b, doc_type))
            reported = ("SELECT RAW MILLIS_TO_UTC(STR_TO_MILLIS(d.ts)) FROM {} d "
                        "WHERE d._type = '{}' AND d.grp = 'exact' "
                        "AND d.ts = '2026-01-21T22:01:20.9995Z'".format(b, doc_type))
            # All three are served by the _type index.
            for query in (converted, by_instant, reported):
                self.assert_index_plan(self.explain(query), REF_INDEX)
            rows = self.run_cbq_query(converted)['results']
            self.assert_same_rows([row['k'] for row in rows], list(docs),
                                  "Documents read back via {}".format(REF_INDEX), seed)
            wrong = ["{} -> {} (expected {})".format(r['ts'], r['ms'], expected[r['ts']])
                     for r in rows if r['ms'] != expected[r['ts']]]
            self.assertFalse(wrong, "STR_TO_MILLIS is wrong (seed={}):\n{}".format(
                seed, "\n".join(wrong)))
            not_back = ["{} -> {} -> {}".format(r['ts'], r['ms'], r['back'])
                        for r in rows if r['back'] != r['ms']]
            self.assertFalse(not_back, "MILLIS_TO_UTC round trip changed the value (seed={}):"
                             "\n{}".format(seed, "\n".join(not_back)))
            # Sorting by the converted value keeps the real order of the instants.
            sorted_keys = self.run_cbq_query(by_instant)['results']
            self.assertEqual(sorted_keys, sorted(docs, key=lambda k: (expected[docs[k]['ts']], k)),
                             "ORDER BY STR_TO_MILLIS(ts) is not the order of the instants "
                             "(seed={})".format(seed))
            # The reported case converts back to the next whole second.
            utc = self.run_cbq_query(reported)['results']
            self.assertEqual(utc, ['2026-01-21T22:01:21Z'], "Round trip of 20.9995Z gave {}"
                             .format(utc))
        finally:
            # Every key, also after a failed INSERT (e.g. leftovers of a killed run).
            self.run_cbq_query("DELETE FROM {} USE KEYS {}".format(b, json.dumps(list(docs))))

    def test_cbse22544_legacy_timezone_names(self):
        """CBSE-22544 (MB-71315, DOC-14220): CLOCK_TZ('US/Central') returned null while
        CLOCK_TZ('America/Chicago') worked. The query service reads time zones from the
        OS, and Ubuntu 24.04 moved the legacy names (US/*, Asia/Calcutta) into the
        tzdata-legacy package, which the images did not install. Events are stored with
        a legacy or a current zone name and converted with MILLIS_TO_TZ and STR_TO_TZ:
        every result equals the local time and offset computed here (random instants,
        seed logged, in winter and in summer, so with and without daylight saving),
        and a legacy name gives the same result as its current name. CLOCK_TZ of every
        name is not null on every query node; an unknown name gives null."""
        rng, seed = self.seeded_rng("CBSE-22544")
        # legacy name, current name, UTC offset in minutes in winter and in summer
        zones = [('US/Eastern', 'America/New_York', -300, -240),
                 ('US/Central', 'America/Chicago', -360, -300),
                 ('US/Mountain', 'America/Denver', -420, -360),
                 ('US/Pacific', 'America/Los_Angeles', -480, -420),
                 ('US/Alaska', 'America/Anchorage', -540, -480),
                 ('US/Arizona', 'America/Phoenix', -420, -420),
                 ('US/Hawaii', 'Pacific/Honolulu', -600, -600),
                 ('Asia/Calcutta', 'Asia/Kolkata', 330, 330)]

        def instant(summer):
            # Well inside winter (no daylight saving) or summer (daylight saving) in the US.
            year = rng.randint(2025, 2027)
            start = datetime.datetime(year, 4, 15) if summer else datetime.datetime(year, 1, 10)
            days = 160 if summer else 40
            return (start + datetime.timedelta(seconds=rng.randrange(days * 86400))).replace(
                tzinfo=datetime.timezone.utc)

        def local(utc, minutes):
            zone = '{}{:02d}:{:02d}'.format('+' if minutes >= 0 else '-', abs(minutes) // 60,
                                            abs(minutes) % 60)
            return (utc + datetime.timedelta(minutes=minutes)).strftime(
                '%Y-%m-%dT%H:%M:%S') + zone

        doc_type = 'cbse22544_event'
        docs, expected = {}, {}
        for legacy, current, winter, summer in zones:
            for season, offset in (('winter', winter), ('summer', summer)):
                utc = instant(season == 'summer')
                for kind, name in (('legacy', legacy), ('current', current)):
                    key = 'cbse22544::{}::{}::{}'.format(current, season, kind)
                    docs[key] = {'_type': doc_type, 'tz': name,
                                 'ms': int(utc.timestamp()) * 1000,
                                 'iso': utc.strftime('%Y-%m-%dT%H:%M:%SZ')}
                    expected[key] = local(utc, offset)
        self.log.info("CBSE-22544: %d events, e.g. %s -> %s (seed=%s)", len(docs),
                      next(iter(docs.values())), next(iter(expected.values())), seed)
        b = self.query_bucket
        query = ("SELECT META(d).id AS k, d.tz, MILLIS_TO_TZ(d.ms, d.tz) AS m, "
                 "STR_TO_TZ(d.iso, d.tz) AS s FROM {} d WHERE d._type = '{}'".format(
                     b, doc_type))
        names = sorted({name for zone in zones for name in zone[:2]})
        clock = "SELECT {}, CLOCK_TZ('US/Nowhere') AS no_such_zone".format(", ".join(
            "CLOCK_TZ({}) AS `{}`".format(self.n1ql_literal(name), name) for name in names))
        try:
            self._insert_docs(docs, [])
            self.assert_index_plan(self.explain(query), REF_INDEX)
            for node in self.get_query_nodes():
                rows = self.run_cbq_query(query, server=node)['results']
                self.assert_same_rows([r['k'] for r in rows], list(docs),
                                      "Events read back via {} on {}".format(REF_INDEX, node.ip),
                                      seed)
                wrong = ["{} {}: MILLIS_TO_TZ {}, STR_TO_TZ {}, expected {}".format(
                    r['k'], r['tz'], r.get('m'), r.get('s'), expected[r['k']])
                    for r in rows if not r.get('m') == r.get('s') == expected[r['k']]]
                self.assertFalse(wrong, "Time zone conversion is wrong on {} (a null for a "
                                 "legacy name: tzdata-legacy missing, see DOC-14220) (seed={}):"
                                 "\n{}".format(node.ip, seed, "\n".join(wrong)))
                now = self.run_cbq_query(clock, server=node)['results'][0]
                self.assertEqual([name for name in names if now.get(name) is None], [],
                                 "CLOCK_TZ gave null on {} (tzdata-legacy missing, see "
                                 "DOC-14220): {}".format(node.ip, now))
                self.assertIsNone(now.get('no_such_zone'), "CLOCK_TZ('US/Nowhere') on {} is {}, "
                                  "expected null".format(node.ip, now.get('no_such_zone')))
        finally:
            self.run_cbq_query("DELETE FROM {} USE KEYS {}".format(b, json.dumps(list(docs))))

    # ==================================================================
    # USER-DEFINED FUNCTIONS
    # ==================================================================

    def test_mb69973_udf_non_ascii_name(self):
        """MB-69973 (CBSE-21774): global UDFs with non-ASCII names could be created and
        executed, but listing system:functions failed with 10106 "ns_server _metakv
        returned: 500". Related fixes: MB-73478 (query side, non-ASCII UDF names) and
        MB-73647 (metakv2 stored a non-ASCII key under two keys); MB-69973 (metakv returns
        non-UTF-8 bytes) is still open, so this test fails until it is fixed.
        Names are random per run (Latin-1, the bug's range, plus other scripts); rerun
        with udf_seed=<seed> for the same names. Each name is a JavaScript UDF on a
        library that does real work, in turn: arithmetic, string building, a count of one
        _type and the first keys of one _type (the last two run N1QL inside JavaScript,
        served by ix_ref_type). Each is checked against the value computed from the KV
        documents through EXECUTE FUNCTION and SELECT, and two of them are then used per
        document in a query over the bucket, checked row by row."""
        rng, seed = self.seeded_rng("MB-69973", param='udf_seed')
        count = max(4, int(self.input.param("udf_count", 6)))   # at least one of each kind
        names = self._random_non_ascii_names(rng, count)
        self.log.info("MB-69973: UDF names: %s", names)
        fns = ['default:`{}`'.format(name) for name in names]
        kv_docs = self._kv_dataset()
        keys_by_type = {}
        for key, doc in kv_docs.items():
            keys_by_type.setdefault(doc.get('_type'), []).append(key)
        doc_type = rng.choice(sorted(t for t in keys_by_type if isinstance(t, str)))
        type_keys = sorted(keys_by_type[doc_type])
        bucket = '`{}`'.format(self.buckets[0].name)
        library = 'mb69973_lib'
        # One line, no single quotes: create_library posts it through curl -d '...'.
        js = ('function scale(x, k) { return x * k; } '
              'function tag(s, i) { return s.toUpperCase() + "-" + i; } '
              'function countOfType(t, plus) { var q = SELECT RAW COUNT(1) FROM {b} AS d '
              'WHERE d._type = $t; var n = 0; for (var c of q) { n = c; } q.close(); '
              'return n + plus; } '
              'function firstKeys(t, n) { var q = SELECT RAW META(d).id FROM {b} AS d '
              'WHERE d._type = $t ORDER BY META(d).id LIMIT $n; var keys = []; '
              'for (var k of q) { keys.push(k); } q.close(); return keys; }').replace(
                  '{b}', bucket)
        js_functions = ['scale', 'tag', 'countOfType', 'firstKeys']
        # The N1QL inside countOfType and firstKeys, with the same parameters, is served by
        # the _type index.
        for sql in ("SELECT RAW COUNT(1) FROM {} AS d WHERE d._type = $t",
                    "SELECT RAW META(d).id FROM {} AS d WHERE d._type = $t "
                    "ORDER BY META(d).id LIMIT $n"):
            self.assert_index_plan(self.explain(sql.format(bucket), query_params={
                '$t': json.dumps(doc_type), '$n': 3}), REF_INDEX)

        def udf(i):
            """(library function, parameters, arguments, expected result) of the i-th name.
            i is in every argument list, so each name must return its own result."""
            kind = js_functions[i % 4]
            if kind == 'scale':
                x = rng.randint(1, 1000)
                return kind, '(x, k)', [x, i + 2], x * (i + 2)
            if kind == 'tag':
                word = rng.choice(['alpha', 'beta', 'gamma'])
                return kind, '(s, i)', [word, i], '{}-{}'.format(word.upper(), i)
            if kind == 'countOfType':
                return kind, '(t, plus)', [doc_type, i], len(type_keys) + i
            return kind, '(t, n)', [doc_type, i + 1], type_keys[:i + 1]

        udfs = [udf(i) for i in range(len(fns))]
        created = []
        self.assertTrue(self.create_library(library, js, js_functions),
                        "JavaScript library {} was not created".format(library))
        try:
            for fn, (function, params, _, _) in zip(fns, udfs):
                ddl = 'CREATE OR REPLACE FUNCTION {}{} LANGUAGE JAVASCRIPT AS "{}" AT "{}"'.format(
                    fn, params, function, library)
                try:
                    self.run_cbq_query(ddl)
                except CBQError as ex:
                    self._fail_if_mb69973(ex, 'CREATE OR REPLACE FUNCTION ' + fn, names, seed)
                    raise
                created.append(fn)
            for fn, (_, _, args, expected) in zip(fns, udfs):
                call = '{}({})'.format(fn, ', '.join(self.n1ql_literal(a) for a in args))
                self._assert_udf_call(call, expected, "udf_seed={}".format(seed))

            # As an application uses them: per document, in a query over the bucket.
            i_tag = next(i for i, u in enumerate(udfs) if u[0] == 'tag')
            i_scale = next(i for i, u in enumerate(udfs) if u[0] == 'scale')
            doc_query = ("SELECT META(d).id AS k, {tag}(META(d).id, {i}) AS s, "
                         "{scale}(LENGTH(META(d).id), {f}) AS j FROM {b} AS d "
                         "WHERE d._type = {t}".format(tag=fns[i_tag], i=i_tag, scale=fns[i_scale],
                                                      f=i_scale + 2, b=bucket,
                                                      t=self.n1ql_literal(doc_type)))
            self.assert_index_plan(self.explain(doc_query), REF_INDEX)
            expected_rows = [{'k': k, 's': '{}-{}'.format(k.upper(), i_tag),
                              'j': len(k) * (i_scale + 2)} for k in type_keys]
            rows = self.run_cbq_query(doc_query)['results']
            self.assert_same_rows(rows, expected_rows, "JavaScript UDFs per document of _type {}"
                                  .format(doc_type), seed)
            # The bug. Only the exact MB-69973 error is reported as such.
            try:
                rows = self.run_cbq_query('SELECT f.* FROM system:functions AS f')['results']
            except CBQError as ex:
                self._fail_if_mb69973(ex, 'SELECT FROM system:functions', names, seed)
                raise
            listed = [r.get('identity', {}).get('name') for r in rows]
            missing = [name for name in names if name not in listed]
            self.assertFalse(missing, "Not listed (or listed with a different name) in "
                             "system:functions: {} (udf_seed={}); listed: {}".format(
                                 missing, seed, listed))
            for name in names:
                by_name = self.run_cbq_query(
                    'SELECT RAW f.identity.name FROM system:functions AS f '
                    'WHERE f.identity.name = {}'.format(self.n1ql_literal(name)))['results']
                self.assertEqual(by_name, [name], "Lookup of {} returned {} (udf_seed={})"
                                 .format(name, by_name, seed))
        finally:
            for fn in created:
                self.run_cbq_query('DROP FUNCTION {}'.format(fn))
            # Same as the existing UDF tests: its return value is not checked, because the
            # helper's GET after the DELETE can still echo the library name.
            self.delete_library(library)

    def _assert_udf_call(self, call, expected, what, server=None):
        """call (a UDF with its arguments) returns exactly expected through EXECUTE
        FUNCTION and through SELECT RAW."""
        for statement in ('EXECUTE FUNCTION ' + call, 'SELECT RAW ' + call):
            rows = self.run_cbq_query(statement, server=server)['results']
            self.assertEqual(rows, [expected], "{} returned {}, expected {} ({})".format(
                statement, rows, [expected], what))

    def _fail_if_mb69973(self, ex, step, names, seed):
        """Fail with the step, names and seed on the MB-69973 error; return otherwise
        (the caller re-raises)."""
        error = self.process_CBQE(ex)
        if (error.get('code') == MB69973_ERROR_CODE
                and MB69973_ERROR_TEXT in error.get('msg', '')):
            self.fail("MB-69973: {} failed for non-ASCII UDF names {} (udf_seed={}): {}"
                      .format(step, names, seed, error))

    @staticmethod
    def _random_non_ascii_names(rng, count):
        """`count` distinct names: 'udf_', one Latin-1 character and 2-6 random others."""
        sets = list(NON_ASCII_CHARS)
        names = set()
        while len(names) < count:
            chars = [rng.choice(NON_ASCII_CHARS['latin1'])]
            chars += [rng.choice(NON_ASCII_CHARS[rng.choice(sets)])
                      for _ in range(rng.randint(2, 6))]
            rng.shuffle(chars)
            names.add('udf_' + ''.join(chars))
        return sorted(names)

    def test_mb68343_udf_resolves_after_graceful_failover(self):
        """MB-68343 (CBSE-22098): user-defined functions must keep resolving on the query
        nodes across a graceful failover. Three UDFs, as applications use them: a global
        inline one, a global JavaScript one from a library, and a scoped inline one whose
        subquery counts documents of the scope's collection (through an index with a
        replica, so the data stays reachable while a node is out). Each data node in turn
        is gracefully failed over and added back with delta recovery, as for patching.
        Throughout, every query node that stays up calls each UDF through EXECUTE
        FUNCTION and SELECT and must get exactly the expected result; only error 12008
        (KV connection) is allowed, until the node's first success. Afterwards every
        query node, the recovered one included, resolves every UDF with exact results,
        and the subquery's statement keeps its index and the same plan on every node.
        Arguments, constants and data are random (seed logged)."""
        rng, seed = self.seeded_rng("MB-68343")
        self.require_two_nodes()
        bucket, scope, index = self.buckets[0].name, 's_mb68343', 'ix_mb68343_grp'
        library = 'mb68343_lib'
        groups = rng.randint(5, 12)
        num = rng.randint(300, 800)
        docs = {'mb68343::{}'.format(i): {'grp': rng.randrange(groups), 'n': i}
                for i in range(num)}
        counts = Counter(d['grp'] for d in docs.values())
        k, c = rng.randint(2, 97), rng.randint(-500, 500)
        affine = 'mb68343_affine'
        js_fn = 'mb68343_weigh'
        scoped = 'default:`{}`.`{}`.mb68343_grp_count'.format(bucket, scope)
        created = []

        def calls(call_rng):
            """(call, expected result) of each UDF, with fresh random arguments; a group
            id one past the last is absent (count 0)."""
            x = call_rng.randint(-10000, 10000)
            a, b = call_rng.randint(0, 1000), call_rng.randint(0, 1000)
            g = call_rng.randint(0, groups)
            return [('{}({})'.format(affine, x), x * k + c),
                    ('{}({}, {})'.format(js_fn, a, b), a * 7 + b * 3),
                    ('{}({})'.format(scoped, g), counts.get(g, 0))]

        def check(server, what, n=0):
            call_rng = random.Random('{}-{}-{}'.format(seed, server.ip, n))
            for call, expected in calls(call_rng):
                self._assert_udf_call(call, expected, "MB-68343: on {} {} (seed={})".format(
                    server.ip, what, seed), server=server)

        def index_check():
            g = rng.randrange(groups)
            return [("SELECT RAW COUNT(1) FROM {} AS d WHERE d.grp = $g".format(ks), index,
                     [counts.get(g, 0)], {'$g': g})]

        self.log.info("MB-68343: %d docs in %d groups, affine k=%d c=%d (seed=%s)",
                      num, groups, k, c, seed)
        try:
            ks = self._create_scope(scope, ['docs'],
                                    [(index, 'docs', '(grp)', '{"num_replica": 1}')])['docs']
            self._insert_docs(docs, [], keyspace=ks)
            self.assert_item_count(self.buckets[0], num, scope, 'docs')
            self._validate_indexes([self._collection_index_spec(
                scope, 'docs', ks, index, "grp IS NOT MISSING", num, replicas=1)])
            # One line, no single quotes: create_library posts it through curl -d '...'.
            self.assertTrue(self.create_library(
                library, 'function weigh(a, b) { return a * 7 + b * 3; }', ['weigh']),
                "JavaScript library {} was not created".format(library))
            for name, ddl in (
                    (affine, 'CREATE OR REPLACE FUNCTION {}(x) {{ x * {} + {} }}'.format(
                        affine, k, c)),
                    (js_fn, 'CREATE OR REPLACE FUNCTION {}(a, b) LANGUAGE JAVASCRIPT AS '
                            '"weigh" AT "{}"'.format(js_fn, library)),
                    (scoped, 'CREATE OR REPLACE FUNCTION {}(g) {{ (SELECT RAW COUNT(1) FROM '
                             '{} AS d WHERE d.grp = g)[0] }}'.format(scoped, ks))):
                self.run_cbq_query(ddl)
                created.append(name)
            for node in self.get_query_nodes():
                check(node, "before any failover")
            # The scoped UDF's statement is served by the index, the same way on every node.
            self.reassert_queries(index_check(), "before any failover", seed)
            for kv_node in self.get_nodes_from_services_map(service_type="kv",
                                                            get_all_nodes=True):
                label = "graceful failover of {}".format(kv_node.ip)
                stays_up = [n for n in self.get_query_nodes() if n.ip != kv_node.ip]
                self.run_query_workload(
                    functools.partial(self.failover_and_recover, kv_node),
                    lambda server, n, label=label: check(server, "during " + label, n),
                    label, nodes=stays_up, allowed_codes=(KV_CONNECTION_ERROR_CODE,),
                    seed=seed)
                for node in self.get_query_nodes():
                    check(node, "after " + label)
                self.reassert_queries(index_check(), "after " + label, seed)
        finally:
            for name in reversed(created):
                self.run_cbq_query('DROP FUNCTION {}'.format(name))
            # Its return value is not checked, as in test_mb69973_udf_non_ascii_name.
            self.delete_library(library)
            self._drop_scope(scope)

    # ==================================================================
    # IN-LIST / ARRAY / OR PREDICATES: each test inserts its own random documents
    # (seed logged); matching documents of the loaded data are counted too.
    # ==================================================================

    def test_mb70834_or_on_partial_indexes_uses_union_scan(self):
        """MB-70834 (CBSE-22291, CBSE-22613):
        WHERE f = v1 OR f = v2 ... with one partial index per value stopped
        using UnionScan in 8.0.0 ("no index available" without a primary index). The
        customer-shape query must use a UnionScan over exactly those indexes on every
        node and when prepared, and return exactly the matching keys. Number of values,
        values and documents are random."""
        rng, seed = self.seeded_rng("MB-70834")
        b = self.query_bucket
        doc_type = 'mb70834_order'
        statuses = ['ST-NEW', 'ST-PICKED', 'ST-PACKED', 'ST-SHIPPED', 'ST-DELIVERED',
                    'ST-RETURNED']
        docs = {'mb70834::{}'.format(i): {'_type': doc_type, 'orderNo': i,
                                          'status': rng.choice(statuses)}
                for i in range(rng.randint(200, 600))}
        wanted = sorted(rng.sample(statuses, rng.randint(2, 4)))
        indexes = ['ix_mb70834_{}'.format(n) for n in range(len(wanted))]
        inserted_match = {k for k, d in docs.items() if d['status'] in wanted}
        expected = inserted_match | self._kv_keys(lambda d: d.get('status') in wanted)
        self.log.info("MB-70834: %d documents, OR over %s (%d partial indexes), %d expected "
                      "(seed=%s)", len(docs), wanted, len(indexes), len(expected), seed)
        self.assertTrue(inserted_match, "No generated document has one of {} (seed={})".format(
            wanted, seed))
        query = "SELECT RAW META().id FROM {} WHERE {}".format(
            b, " OR ".join("status = {}".format(self.n1ql_literal(v)) for v in wanted))
        reference = ("SELECT RAW META().id FROM {} USE INDEX ({}) WHERE _type = {} "
                     "AND status IN {}".format(b, REF_INDEX, self.n1ql_literal(doc_type),
                                               json.dumps(wanted)))
        stmt = 'mb70834_stmt'
        inserted, created = [], []
        try:
            self._insert_docs(docs, inserted)
            for name, value in zip(indexes, wanted):
                self._create_index("CREATE INDEX {} ON {}(status) WHERE status = {} USING {}"
                                   .format(name, b, self.n1ql_literal(value), self.index_type), name)
                created.append(name)
            kv_docs = list(self._kv_dataset().values()) + list(docs.values())
            self._validate_indexes([
                self._index_spec(name, "status = {}".format(self.n1ql_literal(value)),
                                 sum(1 for d in kv_docs if d.get('status') == value))
                for name, value in zip(indexes, wanted)])
            self.assert_same_rows(self.run_cbq_query(reference)['results'], inserted_match,
                                  "reference query via {}".format(REF_INDEX))
            self.assert_union_scan(self.explain(query), indexes)
            self.assert_same_plan_across_nodes(query)
            self.assert_same_rows(self.run_cbq_query(query)['results'], expected, "OR query")
            plan = self.PreparePlanHelper(self.run_cbq_query(
                "PREPARE {} AS {}".format(stmt, query)))
            self.assert_union_scan(plan, indexes)
            self.assert_same_rows(self.run_cbq_query("EXECUTE {}".format(stmt))['results'],
                                  expected, "EXECUTE {}".format(stmt))
        finally:
            self.run_cbq_query("DELETE FROM system:prepareds WHERE name = '{}'".format(stmt))
            self._delete_keys(inserted)
            for name in created:
                self.drop_index(self.bucket_name, name)

    def test_mb64696_large_in_list_spans(self):
        """MB-64696 / MB-67805 (CBSE-18752, CBSE-20724):
        an IN list over the span limit (8192) is merged into one
        min..max range, so an array index scans far more than needed; bit 0x2000000
        raises the limit to 128K. Customer shape: COUNT(*) ... ANY v IN phoneNumbers
        SATISFIES v IN [...] END over a hash-partitioned array index. Random lists
        (some values absent): under the limit one span per value; over it one span by
        default and one per value with the bit; the count is always exact."""
        rng, seed = self.seeded_rng("MB-64696")
        b = self.query_bucket
        doc_type = 'mb64696_subscriber'
        num = int(self.input.param("inlist_docs", 12000))
        index = 'ix_mb64696_phones'

        def phone(n):
            return 'P{}'.format(1000000 + n)

        # Subscriber d has phone(d) and phone(num + d).
        doc_phones = [(phone(d), phone(num + d)) for d in range(num)]

        def in_list(size):
            """size distinct numbers, shuffled; ~90% exist."""
            hits = rng.sample(range(2 * num), size * 9 // 10)
            misses = rng.sample(range(2 * num, 4 * num), size - len(hits))
            values = [phone(n) for n in hits + misses]
            rng.shuffle(values)
            return values

        def expected_count(values):
            wanted = set(values)
            count = sum(1 for p1, p2 in doc_phones if p1 in wanted or p2 in wanted)
            return count + len(self._kv_keys(
                lambda d: isinstance(d.get('phoneNumbers'), list)
                and any(isinstance(x, str) and x in wanted for x in d['phoneNumbers'])))

        def check(values, expected_spans, label):
            query = ("SELECT COUNT(*) AS n FROM {} AS p WHERE ANY v IN p.phoneNumbers "
                     "SATISFIES v IN {} END".format(b, json.dumps(values)))
            plan = self.explain(query)
            self.assert_index_plan(plan, index)
            spans = self.index_spans(plan, index)
            self.log.info("MB-64696 %s: %d values -> %d spans", label, len(values), len(spans))
            self.assertEqual(len(spans), expected_spans,
                             "{}: {} IN-list values gave {} spans, expected {} (seed={}). "
                             "First spans: {}".format(label, len(values), len(spans),
                                                      expected_spans, seed, spans[:3]))
            expected = expected_count(values)
            rows = self.run_cbq_query(query)['results']
            self.assertEqual(rows, [{'n': expected}], "{}: COUNT returned {}, expected {} "
                             "(seed={})".format(label, rows, expected, seed))

        small = in_list(rng.randint(100, 4000))
        large = in_list(rng.randint(DEFAULT_INLIST_SPAN_LIMIT + 300,
                                    DEFAULT_INLIST_SPAN_LIMIT + 1300))
        self.assertFalse(self.get_n1ql_feat_ctrl() & LARGE_INLIST_SPANS_BIT,
                         "Feature-control bit {:#x} is already set; this test needs the "
                         "default span limit".format(LARGE_INLIST_SPANS_BIT))
        bit_set = False
        try:
            self._insert_generated(doc_type, '"phoneNumbers": ["P" || TO_STR(1000000 + d), '
                                   '"P" || TO_STR({} + d)]'.format(1000000 + num), num)
            self._create_index("CREATE INDEX {} ON {}(DISTINCT ARRAY v FOR v IN phoneNumbers "
                               "END) PARTITION BY HASH(META().id) USING {}".format(
                                   index, b, self.index_type), index, num_partition=8)
            # Each subscriber has two distinct numbers; loaded documents count too.
            arrays = [d['phoneNumbers'] for d in self._kv_dataset().values()
                      if isinstance(d.get('phoneNumbers'), list) and d['phoneNumbers']]
            self._validate_indexes([self._index_spec(
                index, "ANY v IN phoneNumbers SATISFIES v IS NOT MISSING END",
                num + len(arrays), 2 * num + sum(
                    len({json.dumps(self._canonical(v), sort_keys=True) for v in a}) for a in arrays))])
            check(small, len(small), "under the limit, default")
            check(large, 1, "over the limit, default")
            self.set_feat_ctrl_bit(LARGE_INLIST_SPANS_BIT, True)
            bit_set = True
            check(large, len(large), "over the limit, bit {:#x} set".format(
                LARGE_INLIST_SPANS_BIT))
        finally:
            if bit_set:
                self.set_feat_ctrl_bit(LARGE_INLIST_SPANS_BIT, False)
            self.drop_index_safe(self.bucket_name, index)
            self._delete_generated(doc_type)

    def test_inlist_prepared_parameter_dynamic_spans(self):
        """A large IN list passed as a parameter to a prepared statement (IN $keys) gets
        one dynamic_in span, expanded per value at run time (up to 8192), instead of a
        huge inline list (customer: ~43K values, minutes of planning or ~30 s scanning a
        sparse min..max range). Checks with random key lists (some absent): covering
        prepared plan with one dynamic_in span; under the limit exact keys on every
        node and #itemsOut equal to the match count (no range scan); over the limit
        exact keys; the same list inline gives the same rows."""
        rng, seed = self.seeded_rng("IN $keys")
        b = self.query_bucket
        doc_type = 'inlist_account'
        num = int(self.input.param("inlist_docs", 12000))
        index = 'ix_inlist_account_key'
        stmt = 'inlist_param_stmt'

        def account(n):
            return 'ACC{}'.format(1000000 + n)

        # Account d has key account(3 * d): sparse, so most of a min..max range is unused.
        existing = {account(3 * d) for d in range(num)}

        def key_list(size):
            hits = [account(3 * d) for d in rng.sample(range(num), size * 9 // 10)]
            misses = [account(3 * d + 1) for d in rng.sample(range(num), size - len(hits))]
            keys = hits + misses
            rng.shuffle(keys)
            return keys

        def expected_keys(keys):
            wanted = set(keys)
            kv = [d['accountKey'] for d in self._kv_dataset().values()
                  if isinstance(d.get('accountKey'), str) and d['accountKey'] in wanted]
            return sorted([k for k in wanted if k in existing] + kv)

        def execute(keys, server=None, profile=False):
            params = {'$keys': json.dumps(keys)}
            if profile:
                params['profile'] = 'timings'
            return self.run_cbq_query("EXECUTE {}".format(stmt), server=server,
                                      query_params=params)

        small = key_list(rng.randint(200, 4000))
        large = key_list(rng.randint(DEFAULT_INLIST_SPAN_LIMIT + 300,
                                     DEFAULT_INLIST_SPAN_LIMIT + 1300))
        query = "SELECT RAW accountKey FROM {} WHERE accountKey IN $keys".format(b)
        try:
            self._insert_generated(doc_type, '"accountKey": "ACC" || TO_STR(1000000 + 3 * d), '
                                   '"balance": d % 1000', num)
            self._create_index("CREATE INDEX {} ON {}(accountKey) USING {}".format(
                index, b, self.index_type), index)
            self._validate_indexes([self._index_spec(
                index, "accountKey IS NOT MISSING",
                num + sum(1 for d in self._kv_dataset().values() if 'accountKey' in d))])

            plan = self.PreparePlanHelper(self.run_cbq_query(
                "PREPARE {} AS {}".format(stmt, query)))
            self.assert_index_plan(plan, index)
            self.assertFalse(self.find_all_operators(plan, 'Fetch'),
                             "IN $keys is not covered by {}:\n{}".format(
                                 index, self.format_plan(plan)))
            spans = self.index_spans(plan, index)
            dynamic = [r for span in spans for r in span.get('range', [])
                       if r.get('dynamic_in')]
            self.assertTrue(len(spans) == 1 and len(dynamic) == 1,
                            "Prepared IN $keys should have one dynamic_in span, got {}".format(
                                spans))

            # Under the limit: one span per key, so the scan returns exactly the matches.
            expected = expected_keys(small)
            result = execute(small, profile=True)
            self.assert_same_rows(sorted(result['results']), expected,
                                  "EXECUTE with {} keys".format(len(small)), seed)
            items = self.index_items_out(result['profile']['executionTimings'], index)
            self.log.info("IN $keys, %d keys: %d rows, index #itemsOut %s",
                          len(small), len(expected), items)
            self.assertEqual(items, [len(expected)],
                             "Index scan returned {} entries for {} matching keys: the "
                             "list was not expanded into spans (seed={})".format(
                                 items, len(expected), seed))
            for node in self.get_query_nodes():
                self.assert_same_rows(sorted(execute(small, server=node)['results']), expected,
                                      "EXECUTE on {}".format(node.ip), seed)

            # Over the limit: still the right rows (the scan falls back to min..max).
            result = execute(large, profile=True)
            self.assert_same_rows(sorted(result['results']), expected_keys(large),
                                  "EXECUTE with {} keys".format(len(large)), seed)
            self.log.info("IN $keys, %d keys: index #itemsOut %s", len(large),
                          self.index_items_out(result['profile']['executionTimings'], index))

            # The small list inline (adhoc): same index, same rows; planning time is logged.
            inline_sql = "SELECT RAW accountKey FROM {} WHERE accountKey IN {}".format(
                b, json.dumps(small))
            self.assert_index_plan(self.explain(inline_sql), index)
            inline = self.run_cbq_query(inline_sql, query_params={'profile': 'phases'})
            self.assert_same_rows(sorted(inline['results']), expected, "Inline IN list", seed)
            self.log.info("Inline IN list, %d keys: phase times %s", len(small),
                          inline['profile'].get('phaseTimes'))
        finally:
            self.run_cbq_query("DELETE FROM system:prepareds WHERE name = '{}'".format(stmt))
            self.drop_index_safe(self.bucket_name, index)
            self._delete_generated(doc_type)

    def test_cbse21912_composite_index_results_deterministic(self):
        """CBSE-21912 (Won't Fix): the same query through an adaptive index (DISTINCT
        PAIRS over 9 fields) returned different results (1 row, then 0). Adaptive indexes
        have known limitations (no array keys or functions, always an intersect) that will
        not be fixed, and the advice is not to use them. This test runs the customer's
        query (date range + ANY r IN <list> SATISFIES r = region END, GROUP BY region,
        USE INDEX) through the advised replacement, a regular composite index on
        (eventDate, region), partial on this test's documents: one index scan, no
        IntersectScan, and adaptive_runs (default 50) runs across the query nodes, every
        one equal to the regions from the documents. Some events have no eventDate (a
        random 1 in 7, 11 or 13): an INCLUDE MISSING index (eventDate INCLUDE MISSING,
        region) holds every event, and a covering GROUP BY on eventDate IS MISSING
        through it gives, on every query node, the per-region counts of the undated
        events (fails with MB-75052 until it is fixed). Window and regions are random."""
        rng, seed = self.seeded_rng("CBSE-21912")
        b = self.query_bucket
        doc_type = 'cbse21912_event'
        num = int(self.input.param("adaptive_docs", 5000))
        runs = int(self.input.param("adaptive_runs", 50))
        index = 'ix_cbse21912_events'
        regions = ['RG-{:02d}'.format(i) for i in range(10)]
        base = 1735689600000            # 2025-01-01T00:00:00Z
        step = 6307200                  # num=5000 events cover one year

        def event(d):
            e = {'region': regions[(7 * d) % 10]}
            if d % missing_every:
                e['eventDate'] = base + d * step
            return e

        low = base + rng.randint(0, num // 2) * step
        high = low + rng.randint(30, 180) * 86400000
        # One real region and one of the other eleven names (two have no events at all).
        wanted = [rng.choice(regions)]
        wanted.append(rng.choice([r for r in regions + ['RG-98', 'RG-99'] if r != wanted[0]]))
        # Incomplete records: every missing_every-th event has no eventDate.
        missing_every = rng.choice([7, 11, 13])
        all_index = 'ix_cbse21912_events_all'
        events = [event(d) for d in range(num)]
        dated = sum(1 for e in events if 'eventDate' in e)
        undated = dict(Counter(e['region'] for e in events if 'eventDate' not in e))

        def matches(doc):
            return (doc.get('region') in wanted and _is_number(doc.get('eventDate'))
                    and low <= doc['eventDate'] <= high)

        expected = sorted({e['region'] for e in events if matches(e)})
        where = ("WHERE d._type = {} AND d.eventDate BETWEEN {} AND {} AND ANY r IN rf "
                 "SATISFIES r = d.region END".format(self.n1ql_literal(doc_type), low, high))
        query = ("WITH rf AS ({}) SELECT d.region FROM {} AS d USE INDEX ({}) {} "
                 "GROUP BY d.region".format(json.dumps(wanted), b, index, where))
        reference = ("WITH rf AS ({}) SELECT d.region FROM {} AS d USE INDEX ({}) {} "
                     "GROUP BY d.region".format(json.dumps(wanted), b, REF_INDEX, where))
        self.log.info("CBSE-21912: regions %s, eventDate %s..%s, expected %s; %d of %d events "
                      "undated (1 in %d) (seed=%s)", wanted, low, high, expected, num - dated,
                      num, missing_every, seed)
        try:
            self._insert_generated(doc_type, (
                '"region": {regions}[(7 * d) % 10], '
                '"eventDate": CASE WHEN d % {every} = 0 THEN MISSING '
                'ELSE {base} + d * {step} END, '
                '"channel": ["web", "app", "store"][d % 3], '
                '"tier": ["gold", "silver", "bronze", "basic"][d % 4], '
                '"product": "P-" || TO_STR(d % 25), "currency": ["USD", "EUR", "INR"][d % 3], '
                '"segment": d % 7, "source": "src-" || TO_STR(d % 5), "priority": d % 3').format(
                    regions=json.dumps(regions), every=missing_every, base=base,
                    step=step), num)
            # The advised replacement for the customer's adaptive index.
            self._create_index(
                "CREATE INDEX {} ON {}(eventDate, region) WHERE _type = {} USING {}".format(
                    index, b, self.n1ql_literal(doc_type), self.index_type), index)
            # Instead of a primary index: INCLUDE MISSING keeps the undated events too.
            self._create_index(
                "CREATE INDEX {} ON {}(eventDate INCLUDE MISSING, region) WHERE _type = {} "
                "USING {}".format(all_index, b, self.n1ql_literal(doc_type), self.index_type),
                all_index)
            self._validate_indexes([
                self._index_spec(index, "_type = {} AND eventDate IS NOT MISSING".format(
                    self.n1ql_literal(doc_type)), dated),
                self._index_spec(all_index, "_type = {}".format(self.n1ql_literal(doc_type)),
                                 num)])

            # Undated events per region: only the INCLUDE MISSING index holds them, and it
            # covers the query (no Fetch).
            missing_query = ("SELECT d.region, COUNT(1) AS n FROM {} AS d USE INDEX ({}) "
                             "WHERE d._type = {} AND d.eventDate IS MISSING "
                             "GROUP BY d.region".format(b, all_index, self.n1ql_literal(doc_type)))
            plan = self.explain(missing_query)
            self.assert_index_plan(plan, all_index)
            self.assertFalse(self.find_all_operators(plan, 'Fetch'),
                             "IS MISSING query through {} is not covering:\n{}".format(
                                 all_index, self.format_plan(plan)))
            for node in self.get_query_nodes():
                try:
                    result = self.run_cbq_query(missing_query, server=node)
                except CBQError as ex:
                    # MB-75052 (open): the GROUP BY is pushed down asking for full aggregates,
                    # and the indexer refuses them for an IS MISSING span.
                    error = self.process_CBQE(ex)
                    if (error.get('code') == MB75052_ERROR_CODE
                            and MB75052_ERROR_TEXT in error.get('msg', '')):
                        self.fail("MB-75052: GROUP BY on IS MISSING via {} on {} failed: {} "
                                  "(seed={})".format(all_index, node.ip, error, seed))
                    raise
                rows = {r['region']: r['n'] for r in result['results']}
                self.assertEqual(rows, undated,
                                 "Undated events per region via {} on {}: {}, expected {} "
                                 "(seed={})".format(all_index, node.ip, rows, undated, seed))

            ref_rows = sorted(r['region'] for r in self.run_cbq_query(reference)['results'])
            self.assertEqual(ref_rows, expected,
                             "Reference query via {}: {}, expected {} (seed={})".format(
                                 REF_INDEX, ref_rows, expected, seed))
            plan = self.explain(query)
            self.assert_index_plan(plan, index)
            # One scan of the composite index: no IntersectScan (what the adaptive index
            # always needed).
            scans = [op['#operator'] for op in self.find_scans(plan, include_composite=True)
                     if op['#operator'] != 'DistinctScan']
            self.assertEqual(scans, ['IndexScan3'], "Expected one IndexScan3 on {}:\n{}".format(
                index, self.format_plan(plan)))
            nodes = self.get_query_nodes()
            wrong = []
            for run in range(runs):
                node = nodes[run % len(nodes)]
                rows = sorted(r['region'] for r in
                              self.run_cbq_query(query, server=node)['results'])
                if rows != expected:
                    wrong.append((run, node.ip, rows))
            self.assertFalse(wrong, "The composite index returned wrong results in {} of {} runs "
                             "(expected {}, seed={}); first: {}".format(
                                 len(wrong), runs, expected, seed, wrong[:5]))
        finally:
            self.drop_index_safe(self.bucket_name, index)
            self.drop_index_safe(self.bucket_name, all_index)
            self._delete_generated(doc_type)

    def test_not_in_cte_concurrent_results(self):
        """NOT IN <uncorrelated CTE> (WITH dep AS (SELECT RAW i ... UNNEST ...)) in a
        query with a window function returned wrong rows now and then, and crashed
        cbq-engine under concurrent load (MB-66842, MB-67432; CBSE-21906, CBSE-21529,
        CBSE-21457). Customer shape: RANK()
        OVER per store, excluding ids from "dependency" documents, with a named
        parameter. Runs not_in_threads x not_in_iterations (default 8 x 10) across query
        nodes; every result must equal the rows computed from the documents (and the
        query with the ids inlined), and cbq-engine must not restart. Data is random."""
        rng, seed = self.seeded_rng("NOT IN CTE")
        b = self.query_bucket
        threads = int(self.input.param("not_in_threads", 8))
        iterations = int(self.input.param("not_in_iterations", 10))
        stores = ['ST-{:02d}'.format(i) for i in range(12)]
        txns = {'nin_txn::{}'.format(i): {
            '_type': 'nin_txn', 'txnId': 'T-{:05d}'.format(i), 'store': rng.choice(stores),
            'day': rng.randint(0, 120),
            'amounts': [rng.randint(1, 500) for _ in range(rng.randint(1, 5))]}
            for i in range(rng.randint(400, 800))}
        txn_ids = [t['txnId'] for t in txns.values()]
        deps = {'nin_dep::{}'.format(i): {
            '_type': 'nin_dep', 'day': rng.randint(0, 120),
            'depIds': rng.sample(txn_ids, rng.randint(1, 6))}
            for i in range(rng.randint(50, 150))}
        min_day = rng.randint(20, 80)

        excluded = {i for d in deps.values() if d['day'] >= min_day for i in d['depIds']}
        best = {}
        for t in txns.values():
            if t['day'] >= min_day and t['txnId'] not in excluded:
                key = (-sum(t['amounts']), t['txnId'])     # ORDER BY total DESC, txnId
                if t['store'] not in best or key < best[t['store']][0]:
                    best[t['store']] = (key, t)
        expected = [{'txnId': t['txnId'], 'store': s, 'total': sum(t['amounts'])}
                    for s, (_, t) in sorted(best.items())]
        self.log.info("NOT IN CTE: %d transactions, %d dependency docs, minDay=%s, "
                      "%d excluded ids, %d expected rows (seed=%s)", len(txns), len(deps),
                      min_day, len(excluded), len(expected), seed)

        outer = ("SELECT x.txnId, x.store, x.total FROM ("
                 "SELECT st.txnId, st.store, ARRAY_SUM(st.amounts) AS total, "
                 "RANK() OVER (PARTITION BY st.store ORDER BY ARRAY_SUM(st.amounts) DESC, "
                 "st.txnId) AS rnk FROM {b} AS st WHERE st._type = 'nin_txn' "
                 "AND st.day >= $minDay AND st.txnId NOT IN {dep}) AS x "
                 "WHERE x.rnk = 1 ORDER BY x.store, x.txnId")
        query = ("WITH dep AS (SELECT RAW i FROM {b} AS t UNNEST t.depIds AS i "
                 "WHERE t._type = 'nin_dep' AND t.day >= $minDay) ".format(b=b) +
                 outer.format(b=b, dep='dep'))
        inlined = outer.format(b=b, dep=json.dumps(sorted(excluded)))
        indexes = {
            'ix_nin_txn_day': "CREATE INDEX ix_nin_txn_day ON {}(day, store, txnId) "
                              "WHERE _type = 'nin_txn' USING {}".format(b, self.index_type),
            'ix_nin_dep_ids': "CREATE INDEX ix_nin_dep_ids ON {}(ALL depIds, day) "
                              "WHERE _type = 'nin_dep' USING {}".format(b, self.index_type)}

        def run(server):
            # A new params dict per call: run_cbq_query modifies it.
            return self.run_cbq_query(query, server=server,
                                      query_params={'$minDay': min_day})['results']

        inserted, created = [], []
        try:
            self._insert_docs(dict(txns, **deps), inserted)
            for name, ddl in indexes.items():
                self._create_index(ddl, name)
                created.append(name)
            self._validate_indexes([
                self._index_spec('ix_nin_txn_day', "_type = 'nin_txn' AND day IS NOT MISSING",
                                 len(txns)),
                self._index_spec('ix_nin_dep_ids', "_type = 'nin_dep' AND ANY i IN depIds "
                                 "SATISFIES i IS NOT MISSING END", len(deps),
                                 sum(len(d['depIds']) for d in deps.values()))])
            plan = self.explain(query, query_params={'$minDay': min_day})
            self.assert_index_plan(plan, 'ix_nin_txn_day')
            self.assert_index_used(plan, 'ix_nin_dep_ids')   # the dep CTE
            self.assert_index_plan(self.explain(inlined, query_params={'$minDay': min_day}),
                                   'ix_nin_txn_day')
            self.assertEqual(self.run_cbq_query(inlined, query_params={'$minDay': min_day})
                             ['results'], expected,
                             "NOT IN with the ids inlined differs from the expected rows "
                             "(seed={})".format(seed))
            self.assertEqual(run(self.master), expected, "NOT IN <cte> differs from the "
                             "expected rows (seed={})".format(seed))
            nodes = self.get_query_nodes()
            pids = self.cbq_pids()
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
                futures = [pool.submit(run, nodes[i % len(nodes)])
                           for i in range(threads * iterations)]
                # .result() re-raises a failed query here, in the test thread.
                results = [f.result() for f in futures]
            wrong = [(i, len(rows)) for i, rows in enumerate(results) if rows != expected]
            self.assertFalse(wrong, "NOT IN <cte> returned wrong rows in {} of {} concurrent "
                             "runs (expected {} rows, seed={}); (run, rows): {}".format(
                                 len(wrong), len(results), len(expected), seed, wrong[:10]))
            self.assert_cbq_not_restarted(pids, "the concurrent runs")
        finally:
            self._delete_keys(inserted)
            for name in created:
                self.drop_index(self.bucket_name, name)

    # ==================================================================
    # PANICS: a recovered panic reaches the client as error 5001, an unrecovered one
    # restarts cbq-engine. Each test checks both (no 5001, same cbq-engine pid on every
    # query node); new panic lines in query.log fail it through lib/log_scanner.py.
    # ==================================================================

    def test_mb67432_unnest_left_join_cte_no_panic(self):
        """MB-67432 (CBSE-20935): LEFT JOIN of two CTEs, one built with UNNEST, panicked
        with 5001, as a plain query and inside an inline UDF. Random "rule" and "data"
        documents (items with no rule, several rules, empty arrays); the plain query,
        the UDF and the MB's keyspace-free form run on every query node and must return
        the rows computed from the documents."""
        rng, seed = self.seeded_rng("MB-67432")
        b = self.query_bucket
        rule_type, data_type = 'mb67432_rule', 'mb67432_data'
        rules = {'mb67432_rule::{}'.format(i): {
            '_type': rule_type, 'c1': rng.randint(1, 6), 'c2': rng.choice(['abc', 'def', 'ghi'])}
            for i in range(rng.randint(5, 20))}
        data = {'mb67432_data::{}'.format(i): {
            '_type': data_type, 'c10': rng.randint(1, 8),
            'a1': [{'c20': rng.choice(['xyz', 'uvw', 'rst']), 'pos': j}
                   for j in range(rng.randint(0, 3))]}
            for i in range(rng.randint(20, 60))}
        # One row per (data document, array element) per rule with c1 = c10, or one
        # row when no rule matches (LEFT JOIN). n.* has only the items' fields.
        rules_per_c1 = Counter(rule['c1'] for rule in rules.values())
        expected = [{'c10': d['c10'], 'a1': elem}
                    for d in data.values() for elem in d['a1']
                    for _ in range(max(1, rules_per_c1.get(d['c10'], 0)))]
        self.assertTrue(expected, "Generated data gives no rows (seed={})".format(seed))
        body = ("WITH rules AS (SELECT d.c1, d.c2 FROM {b} AS d WHERE d._type = '{rt}'), "
                "items AS (SELECT d.c10, a1 FROM {b} AS d UNNEST d.a1 WHERE d._type = '{dt}') "
                "SELECT n.* FROM items AS n LEFT JOIN rules AS r1 ON r1.c1 = n.c10").format(
                    b=b, rt=rule_type, dt=data_type)
        fn = 'default:f_mb67432'
        udf_query = 'SELECT r.* FROM {}() AS r'.format(fn)
        # The MB's keyspace-free form: 5 rules with c1 = 1, one item -> 5 rows.
        literal_query = ('SELECT r.* FROM (WITH w1 AS (ARRAY {"c1":1} FOR v IN ARRAY_RANGE(0,5) END), '
                         'w2 AS (SELECT d.c10, a1 FROM {"c10":1, "a1":[{"c20":"xyz"}]} AS d '
                         'UNNEST d.a1) SELECT n.* FROM w2 AS n LEFT JOIN w1 AS r1 '
                         'ON r1.c1 = n.c10) AS r')
        literal_expected = [{'a1': {'c20': 'xyz'}, 'c10': 1}] * 5
        self.log.info("MB-67432: %d rules, %d data documents, %d expected rows (seed=%s)",
                      len(rules), len(data), len(expected), seed)
        inserted, fn_created = [], False
        try:
            self._insert_docs(dict(rules, **data), inserted)
            plan = self.explain(body)
            self.assert_index_plan(plan, REF_INDEX)
            # Both CTEs (rules and items) read the bucket through the _type index.
            ctes = [sub for sub in plan.get('~subqueries', [])
                    if REF_INDEX in self.used_index_names(sub.get('plan', {}))]
            self.assertGreaterEqual(len(ctes), 2, "Not both CTEs use {}:\n{}".format(
                REF_INDEX, self.format_plan(plan)))
            self.run_no_panic('CREATE OR REPLACE FUNCTION {}() {{ ({}) }}'.format(fn, body),
                              'CREATE FUNCTION {}'.format(fn))
            fn_created = True
            pids = self.cbq_pids()
            for node in self.get_query_nodes():
                for what, query in (('plain query', body), ('UDF {}'.format(fn), udf_query)):
                    what = '{} on {}'.format(what, node.ip)
                    rows = self.run_no_panic(query, what, server=node)['results']
                    self.assert_same_rows(rows, expected, what, seed)
                rows = self.run_no_panic(literal_query, 'keyspace-free query on {}'.format(
                    node.ip), server=node)['results']
                self.assertEqual(rows, literal_expected,
                                 "Keyspace-free query on {} returned {}".format(node.ip, rows))
            self.assert_cbq_not_restarted(pids, "the MB-67432 queries")
        finally:
            if fn_created:
                self.run_cbq_query('DROP FUNCTION {}'.format(fn))
            self._delete_keys(inserted)

    def test_mb67035_cbo_join_on_document_key_no_panic(self):
        """MB-67035 (CBSE-21579): CBO join enumeration over a chain of joins on META().id
        panicked while planning. Customer shape: a 5-level hierarchy plus "city" in its
        own scope; LEFT OUTER JOIN city, then INNER JOINs down the chain. CBO must plan
        it (bit 0x4000000, optimizer estimates checked): phase 1 on empty collections
        (the MB repro), phase 2 on random data with the rows checked against the
        documents, CBO on and off, on every query node."""
        rng, seed = self.seeded_rng("MB-67035")
        scope = 's_mb67035'
        names = ['tier2', 'tier3', 'tier4', 'tier5', 'tier6', 'city']
        ks = {n: 'default:`{}`.`{}`.`{}`'.format(self.buckets[0].name, scope, n) for n in names}
        indexes = [('ix_mb67035_t2', 'tier2', '(parentKey)'),
                   ('ix_mb67035_t3', 'tier3', '(parentKey, cityKey)'),
                   ('ix_mb67035_t4', 'tier4', '(parentKey)'),
                   ('ix_mb67035_t5', 'tier5', '(parentKey)'),
                   ('ix_mb67035_t6', 'tier6', '(parentKey)'),
                   ('ix_mb67035_city', 'city', '(parentKey)')]
        tops = ['top-a', 'top-b', 'top-c']
        top, field1 = rng.choice(tops), 'yy'

        # Not every tier2 has a city (outer side); some tier3 point to another parent's
        # city or to none (the join drops them); field1 is 'yy' or 'nn'.
        docs = {n: {} for n in names}
        for i in range(rng.randint(20, 40)):
            docs['tier2']['t2::{}'.format(i)] = {'parentKey': rng.choice(tops)}
        t2_keys = sorted(docs['tier2'])
        for i in range(rng.randint(15, 30)):
            docs['city']['city::{}'.format(i)] = {'parentKey': rng.choice(t2_keys)}
        city_keys = sorted(docs['city'])
        for i in range(rng.randint(60, 120)):
            parent = rng.choice(t2_keys)
            own = [k for k in city_keys if docs['city'][k]['parentKey'] == parent]
            doc = {'parentKey': parent, 'field1': rng.choice([field1, 'nn'])}
            pick = rng.random()
            if own and pick < 0.8:
                doc['cityKey'] = rng.choice(own)
            elif pick < 0.9:
                doc['cityKey'] = rng.choice(city_keys)
            docs['tier3']['t3::{}'.format(i)] = doc
        for level, parent_level in (('tier4', 'tier3'), ('tier5', 'tier4'), ('tier6', 'tier5')):
            parents = sorted(docs[parent_level])
            for i in range(rng.randint(80, 150)):
                doc = {'parentKey': rng.choice(parents)}
                if level == 'tier5':
                    doc['field2'] = 'f2-{}'.format(rng.randint(0, 50))
                docs[level]['{}::{}'.format(level, i)] = doc
        # One complete chain under the chosen top, so the result is never empty.
        docs['tier2']['t2::chain'] = {'parentKey': top}
        docs['city']['city::chain'] = {'parentKey': 't2::chain'}
        docs['tier3']['t3::chain'] = {'parentKey': 't2::chain', 'cityKey': 'city::chain',
                                      'field1': field1}
        docs['tier4']['tier4::chain'] = {'parentKey': 't3::chain'}
        docs['tier5']['tier5::chain'] = {'parentKey': 'tier4::chain', 'field2': 'f2-chain'}
        docs['tier6']['tier6::chain'] = {'parentKey': 'tier5::chain'}

        children = {n: {} for n in names}
        for level in names:
            for key, doc in docs[level].items():
                children[level].setdefault(doc['parentKey'], []).append(key)
        expected = []
        for k2, d2 in docs['tier2'].items():
            if d2['parentKey'] != top:
                continue
            # Without a city, META(cty).id is MISSING and the tier3 join never matches.
            for kc in children['city'].get(k2, []):
                for k3 in children['tier3'].get(k2, []):
                    d3 = docs['tier3'][k3]
                    if d3.get('cityKey') != kc or d3['field1'] != field1:
                        continue
                    for k4 in children['tier4'].get(k3, []):
                        for k5 in children['tier5'].get(k4, []):
                            expected.extend([docs['tier5'][k5]['field2']] *
                                            len(children['tier6'].get(k5, [])))
        expected.sort()

        query = ("SELECT t5.field2 FROM {t2} AS t2 "
                 "LEFT OUTER JOIN {city} AS cty ON cty.parentKey = META(t2).id "
                 "INNER JOIN {t3} AS t3 ON META(t2).id = t3.parentKey AND t3.cityKey = META(cty).id "
                 "INNER JOIN {t4} AS t4 ON META(t3).id = t4.parentKey "
                 "INNER JOIN {t5} AS t5 ON META(t4).id = t5.parentKey "
                 "INNER JOIN {t6} AS t6 ON META(t5).id = t6.parentKey "
                 "WHERE t2.parentKey = {top} AND t3.field1 = {f1}").format(
                     t2=ks['tier2'], city=ks['city'], t3=ks['tier3'], t4=ks['tier4'],
                     t5=ks['tier5'], t6=ks['tier6'], top=self.n1ql_literal(top), f1=self.n1ql_literal(field1))
        self.log.info("MB-67035: %s, top=%s, %d expected rows (seed=%s)",
                      {n: len(docs[n]) for n in names}, top, len(expected), seed)

        def check_cbo_plan(step):
            """On every query node: CBO plans it (join enumeration is CBO-only, so the check
            counts only then) through ix_mb67035_t2; rule-based uses the same index."""
            for node in self.get_query_nodes():
                for use_cbo in (True, False):
                    on_node = "{} on {} (use_cbo={})".format(step, node.ip, use_cbo)
                    plan = self.plan_with_subqueries(self.run_no_panic(
                        'EXPLAIN ' + query, on_node, server=node,
                        query_params={'use_cbo': use_cbo}))
                    self.assert_index_plan(plan, 'ix_mb67035_t2')
                    if use_cbo:
                        self.assert_cbo_planned(plan, on_node)

        def specs(counts):
            return [self._collection_index_spec(scope, collection, ks[collection], index,
                                                "parentKey IS NOT MISSING", counts(collection))
                    for index, collection, _ in indexes]

        self.assertFalse(self.get_n1ql_feat_ctrl() & CBO_WITHOUT_STATS_BIT,
                         "Feature-control bit {:#x} is already set".format(CBO_WITHOUT_STATS_BIT))
        bit_set = False
        pids = self.cbq_pids()
        try:
            self._create_scope(scope, names, indexes)
            self._validate_indexes(specs(lambda collection: 0))
            pids = self.cbq_pids()     # the validation restarted the query service
            self.set_feat_ctrl_bit(CBO_WITHOUT_STATS_BIT, True)
            bit_set = True
            # Phase 1, the MB repro: empty collections.
            check_cbo_plan('EXPLAIN on empty collections')
            rows = self.run_no_panic(query, 'query on empty collections')['results']
            self.assertEqual(rows, [], "Empty collections returned {}".format(rows))

            # Phase 2: data, same mode.
            self.assert_cbq_not_restarted(pids, "the MB-67035 queries on empty collections")
            for name in names:
                self._insert_docs(docs[name], [], keyspace=ks[name])
            self._validate_indexes(specs(lambda collection: len(docs[collection])))
            pids = self.cbq_pids()     # the validation restarted the query service
            check_cbo_plan('EXPLAIN with data')
            for node in self.get_query_nodes():
                for use_cbo in (True, False):
                    what = "query on {} (use_cbo={})".format(node.ip, use_cbo)
                    rows = self.run_no_panic(query, what, server=node,
                                             query_params={'use_cbo': use_cbo})['results']
                    self.assert_same_rows(sorted(row['field2'] for row in rows), expected,
                                          what, seed)
                # As the customer ran it: LIMIT 1 gives one of the expected rows.
                rows = self.run_no_panic(query + " LIMIT 1", "LIMIT 1 query on {}".format(
                    node.ip), server=node)['results']
                self.assertTrue(len(rows) == 1 and rows[0]['field2'] in expected,
                                "LIMIT 1 on {} returned {} (seed={})".format(node.ip, rows, seed))
            self.assert_cbq_not_restarted(pids, "the MB-67035 queries")
        finally:
            if bit_set:
                self.set_feat_ctrl_bit(CBO_WITHOUT_STATS_BIT, False)
            self._drop_scope(scope)

    @staticmethod
    def _error_qualifiers(completed):
        """The completed-requests 'error' qualifier as a set (absent, one code or a list)."""
        value = (completed or {}).get('error')
        if value is None:
            return set()
        return set(value) if isinstance(value, list) else {value}

    def test_mb68028_completed_error_qualifiers_no_panic(self):
        """MB-68028 (CBSE-20831): adding a second and third "error" qualifier to the
        completed-requests settings ({"completed": {"+error": N}}) panicked the settings
        handler. 3-5 random codes (always 3000) are added one at a time on every query
        node, each checked in the settings; a syntax-error statement must then be kept
        once in system:completed_requests (only the qualifier keeps it); finally "-error"
        must restore the original settings."""
        rng, seed = self.seeded_rng("MB-68028")
        codes = [SYNTAX_ERROR_CODE] + rng.sample([1080, 4000, 4200, 5000, 12003, 12004],
                                                  rng.randint(2, 4))
        rng.shuffle(codes)
        nodes = self.get_query_nodes()
        original = {n.ip: self.rest.get_query_admin_settings(n).get('completed') for n in nodes}
        self.log.info("MB-68028: error codes %s, original completed settings %s (seed=%s)",
                      codes, original, seed)
        pids = self.cbq_pids()
        added = {n.ip: [] for n in nodes}
        try:
            for code in codes:
                for node in nodes:
                    self.set_query_admin_setting(node, 'completed', {'+error': code})
                    added[node.ip].append(code)
                    completed = self.rest.get_query_admin_settings(node).get('completed')
                    self.assertEqual(self._error_qualifiers(completed),
                                     self._error_qualifiers(original[node.ip]) | set(added[node.ip]),
                                     "After adding error {} on {}: completed settings {} (seed={})"
                                     .format(code, node.ip, completed, seed))
            self.assert_cbq_not_restarted(pids, "the completed-settings updates")

            for node in nodes:
                statement = "SELEC 'mb68028-{}-{}'".format(seed, node.ip)
                with self.assertRaises(CBQError) as failed:
                    self.run_cbq_query(statement, server=node)
                error = self.process_CBQE(failed.exception)
                self.assertEqual(error.get('code'), SYNTAX_ERROR_CODE,
                                 "{} on {} gave {}, expected error {}".format(
                                     statement, node.ip, error, SYNTAX_ERROR_CODE))
                lookup = ("SELECT RAW cr.errorCount FROM system:completed_requests AS cr "
                          "WHERE cr.statement = {}".format(self.n1ql_literal(statement)))
                deadline = time.time() + 30
                while True:
                    kept = self.run_cbq_query(lookup, server=node)['results']
                    if kept or time.time() > deadline:
                        break
                    time.sleep(2)
                self.assertEqual(len(kept), 1, "Failed statement on {} kept {} times in "
                                 "system:completed_requests, expected once".format(node.ip, len(kept)))
                self.assertEqual(kept[0], 1, "Kept request on {} has errorCount {}, expected 1"
                                 .format(node.ip, kept[0]))
        finally:
            for node in nodes:
                for code in added[node.ip]:
                    self.set_query_admin_setting(node, 'completed', {'-error': code})
            for node in nodes:
                completed = self.rest.get_query_admin_settings(node).get('completed')
                self.assertEqual(completed, original[node.ip],
                                 "completed settings on {} not restored: {} (original {})".format(
                                     node.ip, completed, original[node.ip]))

    def test_mb73438_active_requests_plan_concurrent_no_panic(self):
        """MB-73438 (CBSE-23531): SELECT ..., meta().plan FROM system:active_requests
        marshalled the plans of requests finishing at the same time and panicked. Workload
        threads run an index-served query (random _type, results checked against KV)
        while one poller per query node runs the monitoring query, for
        active_requests_secs (default 60). Polls must succeed and catch workload requests
        with their plans; cbq-engine must not restart. A race: affected builds fail when
        the timing hits."""
        rng, seed = self.seeded_rng("MB-73438")
        b = self.query_bucket
        secs = int(self.input.param("active_requests_secs", 60))
        threads = int(self.input.param("active_requests_threads", 6))
        ids_by_type = {}
        for key, doc in self._kv_dataset().items():
            ids_by_type.setdefault(doc.get('_type'), []).append(key)
        ids_by_type = {t: sorted(keys) for t, keys in ids_by_type.items() if isinstance(t, str)}
        types = sorted(ids_by_type)
        query = ("SELECT META(d).id AS mb73438_id FROM {} AS d WHERE d._type = $t "
                 "ORDER BY META(d).id".format(b))
        monitor = "SELECT active_requests.*, meta().plan FROM system:active_requests"
        self.assert_index_plan(self.explain(query, query_params={'$t': json.dumps(types[0])}),
                               REF_INDEX)
        nodes = self.get_query_nodes()
        worker_seeds = [rng.randrange(1 << 30) for _ in range(threads)]
        self.log.info("MB-73438: %d workload threads, %d pollers, %ss, types %s (seed=%s)",
                      threads, len(nodes), secs, types, seed)
        deadline = time.time() + secs

        def workload(worker):
            wrng = random.Random(worker_seeds[worker])
            runs = 0
            while time.time() < deadline:
                doc_type = wrng.choice(types)
                node = nodes[(worker + runs) % len(nodes)]
                rows = self.run_no_panic(query, "workload query on {}".format(node.ip),
                                         server=node, debug_query=False,
                                         query_params={'$t': json.dumps(doc_type)})['results']
                self.assert_same_rows([row['mb73438_id'] for row in rows], ids_by_type[doc_type],
                                      "Workload on {} for _type {}".format(node.ip, doc_type), seed)
                runs += 1
            return runs

        def poll(node):
            polls = caught = 0
            while time.time() < deadline:
                rows = self.run_no_panic(monitor, "active_requests poll on {}".format(node.ip),
                                         server=node, debug_query=False)['results']
                # The poll itself is always an active request.
                self.assertTrue(rows, "active_requests on {} returned no rows".format(node.ip))
                caught += sum(1 for row in rows if 'mb73438_id' in row.get('statement', '')
                              and row.get('plan'))
                polls += 1
            return polls, caught

        pids = self.cbq_pids()
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads + len(nodes)) as pool:
            work = [pool.submit(workload, i) for i in range(threads)]
            polls = [pool.submit(poll, node) for node in nodes]
            # .result() re-raises a failure from a thread here, in the test thread.
            runs = [f.result() for f in work]
            poll_results = [f.result() for f in polls]
        caught = sum(c for _, c in poll_results)
        self.log.info("MB-73438: %d workload queries, %d polls, %d workload requests caught "
                      "with a plan", sum(runs), sum(p for p, _ in poll_results), caught)
        self.assertGreater(caught, 0, "No poll caught a running workload request with its "
                           "plan, so the race was not exercised; raise active_requests_secs "
                           "or active_requests_threads (seed={})".format(seed))
        self.assert_cbq_not_restarted(pids, "the active_requests polling")

    def test_mb67996_meta_id_in_list_seqscan_concurrent(self):
        """MB-67996 (CBSE-20729, CBSE-21909): WHERE META().id IN [...] on a keyspace with
        no index is served by a KV range scan (the sequential-scan path). Under concurrency it panicked
        ("slice bounds out of range" in vbRangeScan) or returned the wrong number of rows
        (duplicates, or fewer than the keys that exist). Sequential scans are turned on for
        this test only and restored after. A prepared IN-list query (random keys, some
        absent, plus four keys that hash to one vBucket with 1024 vBuckets, from the MB)
        and the MB's COUNT form run concurrently on every query node: every run returns
        exactly the existing keys, the plan is a sequential scan, nothing panics and
        cbq-engine does not restart."""
        rng, seed = self.seeded_rng("MB-67996")
        scope = 's_mb67996'
        num = int(self.input.param("seqscan_docs", 10000))
        threads = int(self.input.param("seqscan_threads", 16))
        runs = int(self.input.param("seqscan_runs", 25))
        same_vbucket = ['k696', 'k706', 'k8498', 'k8508']   # one vBucket of 1024 (from the MB)
        stmt = 'mb67996_in_list'
        # Random keys around the data range (keys past num do not exist), plus the MB's.
        keys = sorted(set(['k{}'.format(rng.randrange(num + num // 10)) for _ in range(20)]
                          + same_vbucket))
        expected = sorted(k for k in keys if int(k[1:]) < num)
        count_keys = rng.sample(['k{}'.format(d) for d in range(num)], 5)
        self.log.info("MB-67996: %d docs, IN list of %d keys (%d exist): %s (seed=%s)",
                      num, len(keys), len(expected), keys, seed)
        seqscan_on = False
        try:
            ks = self._create_scope(scope, ['c1'])['c1']
            # The MB's data: key and value from ARRAY_RANGE (no keyspace is read).
            self.assert_mutations(self.run_cbq_query(
                'UPSERT INTO {} (KEY doc.k, VALUE doc) SELECT {{"k": "k" || TO_STR(d)}} AS doc '
                'FROM ARRAY_RANGE(0, {}) AS d'.format(ks, num)), num, "UPSERT")
            self.assert_item_count(self.buckets[0], num, scope, 'c1')
            self.set_feat_ctrl_bit(SEQSCAN_OFF_BIT, False)
            seqscan_on = True
            in_query = "SELECT RAW META(d).id FROM {} AS d WHERE META(d).id IN {}".format(
                ks, json.dumps(keys))
            count_query = ("SELECT RAW COUNT(1) FROM {} AS d WHERE META(d).id IN {} "
                           "AND d.x IS MISSING".format(ks, json.dumps(count_keys)))
            for query in (in_query, count_query):
                plan = self.explain(query)
                self.assertIn(SEQUENTIAL_SCAN, self.used_index_names(plan),
                              "Not a sequential scan, so not the MB-67996 path:\n{}".format(
                                  self.format_plan(plan)))
            self.run_cbq_query("PREPARE {} AS {}".format(stmt, in_query))
            nodes = self.get_query_nodes()
            pids = self.cbq_pids()

            def worker(t):
                node = nodes[t % len(nodes)]
                wrong = []
                for i in range(runs):
                    what = "EXECUTE {} on {} (thread {} run {})".format(stmt, node.ip, t, i)
                    rows = sorted(self.run_no_panic("EXECUTE {}".format(stmt), what, server=node,
                                                    debug_query=False)['results'])
                    if rows != expected:
                        wrong.append((what, rows))
                    count = self.run_no_panic(count_query, "COUNT on {}".format(node.ip),
                                              server=node, debug_query=False)['results']
                    if count != [len(count_keys)]:
                        wrong.append(("COUNT on {} (thread {} run {})".format(node.ip, t, i),
                                      count))
                return wrong

            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
                wrong = [w for future in [pool.submit(worker, t) for t in range(threads)]
                         for w in future.result()]
            self.assertFalse(wrong, "MB-67996: {} of {} runs returned wrong rows (expected {} "
                             "keys {}, COUNT {}; seed={}); first: {}".format(
                                 len(wrong), 2 * threads * runs, len(expected), expected,
                                 len(count_keys), seed, wrong[:5]))
            self.assert_cbq_not_restarted(pids, "the concurrent sequential scans")
        finally:
            self.run_cbq_query("DELETE FROM system:prepareds WHERE name = '{}'".format(stmt))
            if seqscan_on:
                self.set_feat_ctrl_bit(SEQSCAN_OFF_BIT, True)
            self._drop_scope(scope)

    # ==================================================================
    # CBO INDEX SELECTION: each query is checked on every query node for the expected
    # index (and none of the indexes to avoid), a CBO plan (optimizer estimates) and the
    # same plan on all nodes (CBSE-23539, CBSE-23565), then for exact rows with CBO on
    # and off. Each test has its own scope; statistics come from UPDATE STATISTICS.
    # ==================================================================

    def _update_statistics(self, keyspace):
        self.run_cbq_query("UPDATE STATISTICS FOR {} INDEX ALL".format(keyspace))

    def _check_cbo_query(self, what, sql, index, expected, seed, avoid=(), params=None,
                         limit=None, plan_check=None):
        """expected: the rows computed from the documents. With limit, the rows must be
        min(limit, len(expected)) of them. plan_check(plan, node_ip) adds checks. Without
        limit, the CBO run is profiled and the estimate for index must be within
        cbo_estimate_factor (default 10) of the rows the scan returned."""
        params = params or {}
        plans = {}
        for node in self.get_query_nodes():
            plan = self.explain(sql, server=node, use_cbo=True, query_params=params)
            on_node = "{} on {}".format(what, node.ip)
            self.assert_index_plan(plan, index)
            self.assertFalse(self.used_index_names(plan) & set(avoid),
                             "{}: uses {} instead of only {}:\n{}".format(
                                 on_node, sorted(self.used_index_names(plan)), index,
                                 self.format_plan(plan)))
            self.assert_cbo_planned(plan, on_node)
            if plan_check:
                plan_check(plan, on_node)
            plans[node.ip] = plan
            # Rule-based (CBO off) may pick another index, but never a primary or
            # sequential scan.
            rbo_plan = self.explain(sql, server=node, use_cbo=False, query_params=params)
            self.assert_no_primary_scan(rbo_plan)
            self.assert_no_sequential_scan(rbo_plan)
        (base_ip, base_plan), *others = plans.items()
        for ip, plan in others:
            self.assertEqual(self.normalize_plan(plan), self.normalize_plan(base_plan),
                             "{}: plan on {} differs from {}:\n{}\n---\n{}".format(
                                 what, ip, base_ip, self.format_plan(plan),
                                 self.format_plan(base_plan)))
        if limit is None:
            # CBO on and off: exactly the expected rows on every node; the CBO run's
            # estimates against the rows its scan returned.
            for ip, result in self.assert_cbo_result_parity(
                    sql, expected, what, seed, query_params=params, profile_cbo=True).items():
                self.assert_cbo_estimates(result['profile']['executionTimings'], index,
                                          "{} on {} (use_cbo=True)".format(what, ip),
                                          factor=float(self.input.param("cbo_estimate_factor", 10)))
            return
        # With LIMIT the scan stops early (its row count says nothing, so no estimate
        # check), and any min(limit, n) of the expected rows is right.
        for node in self.get_query_nodes():
            for use_cbo in (True, False):
                run = "{} on {} (use_cbo={})".format(what, node.ip, use_cbo)
                rows = self.run_cbq_query(sql, server=node,
                                          query_params=dict(params, use_cbo=use_cbo))['results']
                # No ORDER BY: any min(limit, n) of the expected rows, each once.
                got = [json.dumps(self._canonical(row), sort_keys=True) for row in rows]
                want = {json.dumps(self._canonical(row), sort_keys=True) for row in expected}
                self.assertTrue(len(got) == min(limit, len(expected))
                                and len(set(got)) == len(got) and set(got) <= want,
                                "{}: {} rows (expected {} of the {} matching), unexpected "
                                "{} (seed={})".format(run, len(got), min(limit, len(expected)),
                                                      len(expected),
                                                      sorted(set(got) - want)[:5], seed))

    def test_cbo_meta_id_predicate_vs_secondary_index(self):
        """CBSE-23529 (MB-73449), CBSE-22044 (MB-70519), CBSE-23580: a META().id range or
        LIKE that matches every document must not win over a selective secondary index.
        MB-73449 underestimated a META().id range pair, MB-70519 used a default
        selectivity for LIKE $param, and in CBSE-23580 statistics taken before the data
        grew made the key index look cheap until UPDATE STATISTICS ran again. Statistics
        are taken on a small first load, then refreshed after the full load."""
        rng, seed = self.seeded_rng("CBO META().id")
        scope = 's_cbo_meta'
        num = int(self.input.param("cbo_docs", 6000))
        domains = ['dom-{:02d}'.format(i) for i in range(40)]
        types = ['order', 'invoice', 'ticket', 'note']
        channels = ['ch-{:02d}'.format(i) for i in range(30)]
        docs = {'p/{:06d}'.format(i): {'domainName': rng.choice(domains), 'type': rng.choice(types),
                                       'channels': rng.sample(channels, rng.randint(1, 3))}
                for i in range(num)}
        domain, wanted_types, channel = rng.choice(domains), rng.sample(types, 2), rng.choice(channels)
        by_domain = [k for k, d in docs.items() if d['domainName'] == domain]
        # The customer query had a LIMIT smaller than the matches.
        limit = rng.randint(max(1, len(by_domain) // 2), max(1, len(by_domain) - 1))
        by_type_channel = [k for k, d in docs.items()
                           if d['type'] in wanted_types and channel in d['channels']]
        self.log.info("CBO META().id: %d docs, domain %s (%d), types %s + channel %s (%d) "
                      "(seed=%s)", num, domain, len(by_domain), wanted_types, channel,
                      len(by_type_channel), seed)
        indexes = [('ix_cbo_meta', 'docs', '(META().id)'),
                   ('ix_cbo_domain', 'docs', '(domainName)'),
                   ('ix_cbo_type_ch', 'docs', '(type, DISTINCT ARRAY c FOR c IN channels END)')]
        try:
            ks = self._create_scope(scope, ['docs'], indexes)['docs']
            keys = sorted(docs)
            first = keys[:num // 50]
            self._insert_docs({k: docs[k] for k in first}, [], keyspace=ks)
            self._update_statistics(ks)
            self._insert_docs({k: docs[k] for k in keys[len(first):]}, [], keyspace=ks)
            spec = functools.partial(self._collection_index_spec, scope, 'docs', ks)
            self._validate_indexes([
                spec('ix_cbo_meta', "META().id IS NOT MISSING", num),
                spec('ix_cbo_domain', "domainName IS NOT MISSING", num),
                spec('ix_cbo_type_ch', "type IS NOT MISSING", num,
                     sum(len(set(d['channels'])) for d in docs.values()))])
            self._update_statistics(ks)

            def check(what, sql, index, expected, **kwargs):
                self._check_cbo_query(what, sql, index, expected, seed, avoid=['ix_cbo_meta'],
                                      **kwargs)

            by_domain_sql = "SELECT RAW META().id FROM {} WHERE domainName = {}".format(
                ks, self.n1ql_literal(domain))
            check("META().id range + domainName (MB-73449)",
                  by_domain_sql + " AND META().id > 'p/' AND META().id < 'p/~'",
                  'ix_cbo_domain', by_domain)
            check("META().id LIKE + domainName, LIMIT {} (CBSE-23580)".format(limit),
                  by_domain_sql + " AND META().id LIKE 'p/%' LIMIT {}".format(limit),
                  'ix_cbo_domain', by_domain, limit=limit)
            type_channel_sql = ("SELECT RAW META().id FROM {} WHERE type IN {} AND ANY c IN "
                                "channels SATISFIES c IN {} END AND META().id LIKE {}")
            check("LIKE $p with parameters (MB-70519)",
                  type_channel_sql.format(ks, '$types', '$ch', '$p'), 'ix_cbo_type_ch',
                  by_type_channel, params={'$types': json.dumps(wanted_types),
                                           '$ch': json.dumps([channel]), '$p': json.dumps('p/%')})
            check("LIKE with literals (MB-70519)",
                  type_channel_sql.format(ks, json.dumps(wanted_types), json.dumps([channel]),
                                          self.n1ql_literal('p/%')), 'ix_cbo_type_ch', by_type_channel)
        finally:
            self._drop_scope(scope)

    def test_cbo_array_predicate_selectivity(self):
        """CBSE-21923 (MB-68471), CBSE-21922 (MB-68458), CBSE-23595 (MB-73591): CBO got
        the selectivity of array predicates wrong and picked a worse index: an ANY over
        several fields of the element, an array-construct index key, and two ANY clauses
        on the same array index key (one matching every document), where the plan must
        be one scan on the selective clause, not both and not an IntersectScan."""
        rng, seed = self.seeded_rng("CBO array")
        scope = 's_cbo_array'
        num = int(self.input.param("cbo_docs", 6000))
        groups = ['g1', 'g2', 'g3', 'g4']

        def element():
            return {'nm': rng.choice(['ac0', 'ac1', 'ac2']),
                    'val': rng.choice('vV') + str(rng.randint(0, 999))}

        docs = {}
        for i in range(num):
            doc = {'type': 't', 'grp': rng.choice(groups),
                   # Every document has the common element (the unselective ANY clause).
                   'a1': [{'nm': 'all', 'val': 'x1'}] + [element() for _ in range(rng.randint(1, 3))]}
            if rng.random() < 0.3:
                doc['category'] = 'C1'
            elif rng.random() < 0.5:
                doc['category'] = None
            docs['arr::{}'.format(i)] = doc
        # Search values taken from the data, so every query matches something.
        source = docs[rng.choice(sorted(k for k, d in docs.items() if d.get('category') is None))]
        pick, grp = rng.choice(source['a1'][1:]), source['grp']
        nm, val = pick['nm'], pick['val']

        def has(doc, test):
            return any(test(e) for e in doc['a1'])

        multi_field = [k for k, d in docs.items() if d['grp'] == grp
                       and has(d, lambda e: e['nm'] == nm and e['val'].lower() == val.lower())]
        construct = [k for k, d in docs.items() if d['grp'] == grp
                     and has(d, lambda e: [e['nm'], e['val']] == [nm, val])]
        two_any = [k for k, d in docs.items() if d.get('category') in (None, 'NONE')
                   and has(d, lambda e: e['nm'] == nm and e['val'] == val)]
        self.assertTrue(multi_field and construct and two_any,
                        "A query matches nothing (seed={})".format(seed))
        self.log.info("CBO array: %d docs, grp %s, element %s/%s: %d, %d, %d rows (seed=%s)",
                      num, grp, nm, val, len(multi_field), len(construct), len(two_any), seed)
        indexes = [('ix_cbo_grp', 'docs', '(grp)'),
                   ('ix_cbo_lower', 'docs', '(DISTINCT ARRAY LOWER(v.val) FOR v IN a1 END)'),
                   ('ix_cbo_pair', 'docs', '(DISTINCT ARRAY [v.nm, v.val] FOR v IN a1 END)'),
                   ('ix_cbo_flat', 'docs', "(DISTINCT ARRAY FLATTEN_KEYS(v.nm, v.val) FOR v IN a1 "
                                           "END, IFMISSINGORNULL(category, 'NONE')) WHERE type = 't'")]

        def one_scan_on_selective_clause(plan, what):
            # One index scan (under the DistinctScan an array index always has), no
            # Intersect/UnionScan, spans on the selective element and not on 'all'/'x1'.
            scans = self.find_scans(plan)
            combined = [op['#operator'] for op in self.find_scans(plan, include_composite=True)
                        if op['#operator'] != 'DistinctScan' and op not in scans]
            spans = str(scans[0].get('spans')) if len(scans) == 1 else ''
            self.assertTrue(len(scans) == 1 and not combined and self.n1ql_literal(val) in spans
                            and self.n1ql_literal('x1') not in spans,
                            "{}: expected one scan with spans on {}, not on the clause every "
                            "document matches:\n{}".format(what, val, self.format_plan(plan)))

        try:
            ks = self._create_scope(scope, ['docs'], indexes)['docs']
            self._insert_docs(docs, [], keyspace=ks)
            # DISTINCT array keys: one item per distinct element value in a document.
            pairs = sum(len({(e['nm'], e['val']) for e in d['a1']}) for d in docs.values())
            spec = functools.partial(self._collection_index_spec, scope, 'docs', ks)
            self._validate_indexes([
                spec('ix_cbo_grp', "grp IS NOT MISSING", num),
                spec('ix_cbo_lower', "ANY v IN a1 SATISFIES LOWER(v.val) IS NOT MISSING END", num,
                     sum(len({e['val'].lower() for e in d['a1']}) for d in docs.values())),
                spec('ix_cbo_pair', "ANY v IN a1 SATISFIES [v.nm, v.val] IS NOT MISSING END",
                     num, pairs),
                spec('ix_cbo_flat', "type = 't' AND ANY v IN a1 SATISFIES v.nm IS NOT MISSING END",
                     num, pairs)])
            self._update_statistics(ks)
            base = "SELECT RAW META().id FROM {} WHERE ".format(ks)
            self._check_cbo_query(
                "ANY over two element fields (MB-68471)",
                base + "grp = {} AND ANY v IN a1 SATISFIES v.nm = {} AND LOWER(v.val) = {} END"
                .format(self.n1ql_literal(grp), self.n1ql_literal(nm), self.n1ql_literal(val.lower())),
                'ix_cbo_lower', multi_field, seed, avoid=['ix_cbo_grp'])
            self._check_cbo_query(
                "array-construct key (MB-68458)",
                base + "grp = {} AND ANY v IN a1 SATISFIES [v.nm, v.val] = {} END".format(
                    self.n1ql_literal(grp), json.dumps([nm, val])),
                'ix_cbo_pair', construct, seed, avoid=['ix_cbo_grp'])
            two_any_sql = (base + "type = 't' AND ANY v IN a1 SATISFIES v.nm = 'all' AND "
                           "v.val = 'x1' END AND ANY v IN a1 SATISFIES v.nm = {} AND v.val = {} "
                           "END AND ".format(self.n1ql_literal(nm), self.n1ql_literal(val)))
            for form in ("IFMISSINGORNULL(category, 'NONE') = 'NONE'",
                         "(category IS MISSING OR category IS NULL)"):
                self._check_cbo_query("two ANY clauses, {} (MB-73591)".format(form),
                                      two_any_sql + form, 'ix_cbo_flat', two_any, seed,
                                      plan_check=one_scan_on_selective_clause)
        finally:
            self._drop_scope(scope)

    def test_cbo_unnest_hash_join_results(self):
        """CBSE-23623 (MB-73738): with join enumeration CBO could put a hash join with a
        CTE before the UNNEST and drop the join condition on the unnested alias, so the
        query returned every array element (800 rows instead of 1). MB repro at a smaller
        scale (cbo_join_docs documents x cbo_join_elements elements), random bounds."""
        rng, seed = self.seeded_rng("CBO UNNEST join")
        scope = 's_cbo_join'
        num = int(self.input.param("cbo_join_docs", 5000))
        size = int(self.input.param("cbo_join_elements", 100))
        outer, inner = rng.randint(2, 6), rng.randint(7, 12)    # o.c2 < 10*outer, cte c2 < 10*inner
        nc1_max = rng.randint(3, 10)

        def lpad(n):
            return '{:08d}'.format(n)

        expected = []
        for i in range(min(num, outer)):                         # o.c2 = 10i < 10*outer
            for w in range(min(num, inner)):                     # cte: w.c2 = 10w < 10*inner
                if w != i:                                       # o.c1 = w1.c1
                    continue
                for j in range(size):
                    uid, nc1, nc2 = lpad(i // size * size + j), i, i * 9
                    if uid == lpad(w) and nc1 < nc1_max and nc2 != 10 * w:
                        expected.append({'c1': i, 'c2': 10 * i, 'uid': uid, 'nc1': nc1, 'id': lpad(w)})
        self.assertTrue(expected, "Bounds give no rows (seed={})".format(seed))
        self.log.info("CBO UNNEST join: %d docs x %d, o.c2 < %d, cte c2 < %d, nc1 < %d: %d "
                      "rows (seed=%s)", num, size, 10 * outer, 10 * inner, nc1_max,
                      len(expected), seed)
        indexes = [('ix_cbo_join_c2', 'col1', '(c2)'),
                   ('ix_cbo_join_arr', 'col1', '(ALL ARRAY FLATTEN_KEYS(v.uid, v.nc1, v.nc2) FOR v '
                                               'IN arr1 END, c1, id)')]
        try:
            ks = self._create_scope(scope, ['col1'], indexes)['col1']
            result = self.run_cbq_query(
                'INSERT INTO {ks} (KEY k, VALUE v) SELECT "k" || LPAD(TO_STR(i), 8, "0") AS k, '
                '{{"id": LPAD(TO_STR(i), 8, "0"), "c1": i, "c2": i * 10, "arr1": (SELECT RAW '
                '{{"uid": LPAD(TO_STR(IDIV(i, {size}) * {size} + j), 8, "0"), "nc1": i, '
                '"nc2": i * 9}} FROM ARRAY_RANGE(0, {size}) AS j)}} AS v '
                'FROM ARRAY_RANGE(0, {num}) AS i'.format(ks=ks, size=size, num=num))
            self.assert_mutations(result, num, "INSERT")
            spec = functools.partial(self._collection_index_spec, scope, 'col1', ks)
            self._validate_indexes([
                spec('ix_cbo_join_c2', "c2 IS NOT MISSING", num),
                spec('ix_cbo_join_arr', "ANY v IN arr1 SATISFIES v.uid IS NOT MISSING END",
                     num, num * size)])
            self._update_statistics(ks)
            self._check_cbo_query(
                "UNNEST + hash join with a CTE",
                "WITH cte1 AS (SELECT c1, c2, id FROM {ks} WHERE c2 < {inner}) "
                "SELECT o.c1, o.c2, u.uid, u.nc1, w1.id FROM {ks} AS o USE INDEX (ix_cbo_join_c2) "
                "UNNEST o.arr1 AS u INNER JOIN cte1 AS w1 USE HASH (BUILD) "
                "ON o.c1 = w1.c1 AND w1.id = u.uid "
                "WHERE o.c2 < {outer} AND u.nc1 < {nc1} AND u.nc2 <> w1.c2".format(
                    ks=ks, inner=10 * inner, outer=10 * outer, nc1=nc1_max),
                'ix_cbo_join_c2', expected, seed)
        finally:
            self._drop_scope(scope)

    def test_cbo_function_key_group_aggs_on_every_node(self):
        """CBSE-23565: one query node stopped using the STR_TO_MILLIS index key as a span
        and lost the index GROUP BY pushdown, so the same query ran a different path and
        time per node. Customer shape: array of users, STR_TO_MILLIS(date) range and
        nested fields in one index; the plan must be covered with index_group_aggs on
        every node, and the counts exact."""
        rng, seed = self.seeded_rng("CBO function key")
        scope = 's_cbo_fn'
        num = int(self.input.param("cbo_docs", 6000))
        users = ['user-{:02d}'.format(i) for i in range(20)]
        stages = ['new', 'review', 'approved', 'closed']
        start = 1735689600000                                    # 2025-01-01T00:00:00Z
        docs = {}
        for i in range(num):
            millis = start + rng.randint(0, 364) * 86400000 + rng.randint(0, 86399) * 1000
            docs['fn::{}'.format(i)] = {
                'assignees': rng.sample(users, rng.randint(1, 3)),
                'bookedAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(millis // 1000)),
                'millis': millis,
                'lastReview': {'priority': rng.choice(['P1', 'P2', 'P3', None]),
                               'stage': rng.choice(stages)}}
        user = rng.choice(users)
        low = start + rng.randint(0, 200) * 86400000
        high = low + rng.randint(30, 150) * 86400000
        counts = Counter(d['lastReview']['stage'] for d in docs.values()
                         if user in d['assignees'] and low <= d['millis'] <= high
                         and d['lastReview']['priority'] is not None)
        expected = [{'stage': s, 'n': n} for s, n in counts.items()]
        self.assertTrue(expected, "The query matches nothing (seed={})".format(seed))
        self.log.info("CBO function key: user %s, %s..%s: %s (seed=%s)", user, low, high,
                      dict(counts), seed)
        indexes = [('ix_cbo_fn', 'docs', '(DISTINCT ARRAY u FOR u IN assignees END, '
                                         'STR_TO_MILLIS(bookedAt), lastReview.priority, '
                                         'lastReview.stage)')]

        def covered_with_group_aggs(plan, what):
            scans = [op for op in self.find_operators_using_index(plan, 'ix_cbo_fn')]
            self.assertTrue(scans and all(op.get('covers') and op.get('index_group_aggs')
                                          for op in scans)
                            and not self.find_all_operators(plan, 'Fetch'),
                            "{}: not a covered scan with index_group_aggs:\n{}".format(
                                what, self.format_plan(plan)))

        try:
            ks = self._create_scope(scope, ['docs'], indexes)['docs']
            self._insert_docs(docs, [], keyspace=ks)
            self._validate_indexes([self._collection_index_spec(
                scope, 'docs', ks, 'ix_cbo_fn', "ANY u IN assignees SATISFIES u IS NOT MISSING END",
                num, sum(len(d['assignees']) for d in docs.values()))])
            self._update_statistics(ks)
            self._check_cbo_query(
                "STR_TO_MILLIS key with GROUP BY",
                "SELECT d.lastReview.stage AS stage, COUNT(1) AS n FROM {} AS d "
                "WHERE ANY u IN d.assignees SATISFIES u = $u END "
                "AND STR_TO_MILLIS(d.bookedAt) BETWEEN {} AND {} "
                "AND d.lastReview.priority IS NOT NULL "
                "GROUP BY d.lastReview.stage".format(ks, low, high),
                'ix_cbo_fn', expected, seed, params={'$u': json.dumps(user)},
                plan_check=covered_with_group_aggs)
        finally:
            self._drop_scope(scope)

    # --- memory quota and accounting (CBSE-23272, CBSE-23635, CBSE-23072, CBSE-23450) ---

    def _fail_if_quota_underflow(self, ex, step, seed):
        """Fail naming MB-72964 when a 5500 error reports more memory in use than its limit
        (the request's counter went below zero and wrapped to ~2^64); return otherwise (the
        caller re-raises)."""
        error = self.process_CBQE(ex)
        match = re.search(r'inuse: (\d+) limit: (\d+)', error.get('msg', ''))
        if (error.get('code') == MEMORY_QUOTA_ERROR_CODE and match
                and int(match.group(1)) > int(match.group(2))):
            self.fail("MB-72964: {} failed with in-use memory above the limit (the quota "
                      "counter wrapped below zero) (seed={}): {}".format(step, seed, error))

    def test_mb72964_named_param_upsert_memory_quota(self):
        """MB-72964 (CBSE-23272): UPSERT ... SELECT ... FROM $docs (an ExpressionScan over a
        named parameter) tracked the parameter at its JSON size but released the full value
        size, so the request's in-use memory went below zero, wrapped to ~2^64 and a
        request memory_quota failed the statement with 5500. Random batches are upserted
        under a quota: each must succeed, write exactly its documents (read back by key)
        and report a usedMemory below the quota."""
        rng, seed = self.seeded_rng("MB-72964")
        scope = 's_mem_param'
        quota_mb = int(self.input.param("memory_quota", 500))
        rounds = int(self.input.param("rounds", 5))
        upsert = "UPSERT INTO {} (KEY k, VALUE v) SELECT d.id AS k, d.doc AS v FROM $docs AS d"
        written = 0
        try:
            ks = self._create_scope(scope, ['c1'])['c1']
            for r in range(rounds):
                docs = [{'id': 'user::{}::{}'.format(r, i), 'doc': {
                    'name': 'user{}'.format(rng.randint(0, 99999)),
                    'role': rng.choice(['admin', 'editor', 'viewer']),
                    'status': rng.choice(['active', 'locked']),
                    'visits': rng.randint(0, 10 ** 6),
                    'tags': rng.sample(['a', 'b', 'c', 'd', 'e', 'f'], rng.randint(0, 6))}}
                    for i in range(rng.randint(1, 200))]
                step = "round {} ({} documents, memory_quota {} MB)".format(r, len(docs), quota_mb)
                try:
                    self.log.info("%s: %s", step, upsert.format(ks))
                    result = self.run_cbq_query(upsert.format(ks), debug_query=False, query_params={
                        'memory_quota': quota_mb, '$docs': json.dumps(docs)})
                except CBQError as ex:
                    self._fail_if_quota_underflow(ex, step, seed)
                    raise
                self.assert_mutations(result, len(docs), "UPSERT FROM $docs, " + step)
                self.assert_used_memory(result, quota_mb, step)
                written += len(docs)
                self.assert_item_count(self.buckets[0], written, scope, 'c1')
                rows = self.run_cbq_query(
                    "SELECT META(d).id AS k, d AS v FROM {} AS d USE KEYS $keys".format(ks),
                    debug_query=False,
                    query_params={'$keys': json.dumps([d['id'] for d in docs])})['results']
                self.assert_same_rows(rows, [{'k': d['id'], 'v': d['doc']} for d in docs],
                                      "Documents read back by key, " + step, seed)
        finally:
            self._drop_scope(scope)

    def _node_memory_baseline(self, node):
        """MB-73749: with no request running, node.memory.usage is (servicers +
        plus-servicers) x 1 MiB."""
        settings = self.rest.get_query_admin_settings(node)
        return (settings['servicers'] + settings['plus-servicers']) * MIB

    def _wait_node_memory_baseline(self, baselines, after, seed, timeout=180):
        """Every query node's node.memory.usage gets back to its baseline (an async
        statement, such as the UPDATE STATISTICS of a CREATE INDEX, may still be running)."""
        deadline = time.time() + timeout
        for node in self.get_query_nodes():
            while True:
                vitals = self.query_vitals(node)
                usage = vitals.get('node.memory.usage')
                if usage == baselines[node.ip]:
                    break
                if time.time() > deadline:
                    self.fail("MB-73749: after {}, node.memory.usage on {} is {} (active requests "
                              "{}), not the baseline {} = (servicers + plus-servicers) x 1 MiB "
                              "(seed={})".format(after, node.ip, usage,
                                                 vitals.get('request.active.count'),
                                                 baselines[node.ip], seed))
                time.sleep(3)

    def test_mb73749_node_memory_back_to_baseline(self):
        """MB-73749 (CBSE-23635, CBSE-22562): the asynchronous UPDATE STATISTICS that a
        CREATE INDEX starts never gave its document
        memory back to the node pool, so node.memory.usage grew with every index build
        until plain queries failed with 5600 (node quota). With a node-quota set, the
        usage must be back at the baseline after each index build and after queries
        through the index, which return exact rows."""
        rng, seed = self.seeded_rng("MB-73749")
        scope = 's_mem_node'
        num = int(self.input.param("mem_docs", 100000))
        builds = int(self.input.param("index_builds", 3))
        names = rng.randint(50, 500)
        index = 'ix_mem_node'
        before = {}
        try:
            ks = self._create_scope(scope, ['docs'])['docs']
            self.assert_mutations(self.run_cbq_query(
                'UPSERT INTO {} (KEY k, VALUE v) SELECT "k" || TO_STR(d) AS k, '
                '{{"cluid": "k" || TO_STR(d), "client_name": "n" || TO_STR(d % {})}} AS v '
                'FROM ARRAY_RANGE(0, {}) AS d'.format(ks, names, num)), num, "UPSERT")
            self.assert_item_count(self.buckets[0], num, scope, 'docs')
            spec = self._collection_index_spec(scope, 'docs', ks, index,
                                               "client_name IS NOT MISSING", num)
            before = self.set_query_setting('node-quota', int(self.input.param("node_quota", 2048)))
            baselines = {n.ip: self._node_memory_baseline(n) for n in self.get_query_nodes()}
            self._wait_node_memory_baseline(baselines, "setting node-quota (before any index "
                                            "build of this test)", seed)
            for b in range(builds):
                self.run_cbq_query("CREATE INDEX {} ON {}(client_name, cluid)".format(index, ks))
                self._wait_for_index_online('docs', index)
                if b == 0:
                    # The build started UPDATE STATISTICS: the index shows up in the dictionary.
                    deadline = time.time() + 120
                    while not any(index in str(row) and scope in str(row) for row in
                                  self.run_cbq_query("SELECT RAW d FROM system:dictionary AS d")
                                  ['results']):
                        self.assertLess(time.time(), deadline, "CREATE INDEX {} did not collect "
                                        "statistics (not in system:dictionary)".format(index))
                        time.sleep(3)
                self._wait_node_memory_baseline(baselines, "CREATE INDEX #{}".format(b + 1), seed)
                # Item count, scan and explain of each build. The query-service restart of
                # the full validation would reset node.memory.usage and hide the leak, so it
                # runs once, after the last build.
                self._check_indexes([spec], "build #{}".format(b + 1))
                if b < builds - 1:
                    self.run_cbq_query("DROP INDEX {} ON {}".format(index, ks))
            # Queries a customer runs through the index: exact rows, then back at the baseline.
            query = "SELECT RAW d.cluid FROM {} AS d WHERE d.client_name = $c".format(ks)
            self.assert_index_plan(self.explain(query, query_params={'$c': '"n0"'}), index)
            for value in rng.sample(range(names), min(names, 50)):
                rows = self.run_cbq_query(query, query_params={'$c': json.dumps(
                    'n{}'.format(value))})['results']
                self.assertEqual(sorted(rows), sorted('k{}'.format(d) for d in
                                                      range(value, num, names)),
                                 "client_name n{} (seed={})".format(value, seed))
            self._wait_node_memory_baseline(baselines, "the queries", seed)
            self._validate_indexes([spec])
        finally:
            self.restore_query_setting('node-quota', before)
            self._drop_scope(scope)

    def test_mb72654_js_udf_transaction_memory_bounded(self):
        """MB-72654 (CBSE-23072), with MB-72964 (CBSE-23272): a JavaScript UDF that upserts
        a batch passed as a parameter inside BEGIN WORK / COMMIT WORK left its execution
        context referenced by the transaction, so cbq-engine grew by GBs over thousands of
        calls (and the same $docs path wrapped the request quota). Workers call the UDF
        under a request memory_quota: every call must return exactly its keys, the final
        documents must be the last batch of each worker, cbq-engine must not restart and
        its RSS must grow by less than rss_growth_mb (the bug: about 1.8 GB per 8000 calls)."""
        rng, seed = self.seeded_rng("MB-72654")
        scope, library, fn = 's_mem_txn', 'mb72654_lib', 'mb72654_upsert'
        workers = int(self.input.param("udf_workers", 8))
        calls = int(self.input.param("udf_calls", 8000)) // workers
        batch = int(self.input.param("udf_batch", 25))
        quota_mb = int(self.input.param("memory_quota", 500))
        growth_mb = int(self.input.param("rss_growth_mb", 512))
        salts = [rng.randint(0, 10 ** 6) for _ in range(workers)]
        created = False

        def docs(w, i):
            return [{'id': 'txn::w{}::{}'.format(w, j), 'doc': {
                'worker': w, 'call': i, 'quantity': (salts[w] + 31 * i + j) % 1000,
                'uuid': '{:08x}-{:04x}'.format(salts[w], j)}} for j in range(batch)]

        try:
            ks = self._create_scope(scope, ['docs'])['docs']
            # One line, no single quotes: create_library posts it through curl -d '...'.
            js = ('function upsertBatch(docs) { BEGIN WORK; var q = N1QL("UPSERT INTO {ks} '
                  '(KEY k, VALUE v) SELECT d.id AS k, d.doc AS v FROM $docs AS d RETURNING RAW '
                  'META().id", {docs: docs}); var ids = []; for (const id of q) { ids.push(id); } '
                  'q.close(); COMMIT WORK; return ids; }').replace('{ks}', ks)
            self.assertTrue(self.create_library(library, js, ['upsertBatch']),
                            "JavaScript library {} was not created".format(library))
            self.run_cbq_query('CREATE OR REPLACE FUNCTION {}(docs) LANGUAGE JAVASCRIPT AS '
                               '"upsertBatch" AT "{}"'.format(fn, library))
            created = True

            def call(w, i):
                batch_docs = docs(w, i)
                step = "worker {} call {}".format(w, i)
                try:
                    result = self.run_cbq_query('EXECUTE FUNCTION {}($docs)'.format(fn),
                                                debug_query=False,
                                                query_params={'memory_quota': quota_mb,
                                                              '$docs': json.dumps(batch_docs)})
                except CBQError as ex:
                    self._fail_if_quota_underflow(ex, step, seed)
                    raise
                self.assertEqual(sorted(result['results'][0]), sorted(d['id'] for d in batch_docs),
                                 "{} returned other keys (seed={})".format(step, seed))

            def worker(w):
                for i in range(calls):
                    call(w, i)

            call(0, 0)  # warm-up: the library is loaded before the first RSS sample
            pids = self.cbq_pids()
            rss_before = self.query_rss_mb(self.master)
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                for future in [pool.submit(worker, w) for w in range(workers)]:
                    future.result()
            rss_after = self.query_rss_mb(self.master)
            self.log.info("MB-72654: %d UDF calls, cbq-engine RSS %.0f MB -> %.0f MB (seed=%s)",
                          workers * calls, rss_before, rss_after, seed)
            self.assert_cbq_not_restarted(pids, "the UDF calls")
            self.assert_item_count(self.buckets[0], workers * batch, scope, 'docs')
            self.assertLess(rss_after - rss_before, growth_mb,
                            "MB-72654: cbq-engine RSS grew {:.0f} MB (from {:.0f} MB) over {} "
                            "transactional UDF calls, limit {} MB (seed={})".format(
                                rss_after - rss_before, rss_before, workers * calls, growth_mb,
                                seed))
            expected = {d['id']: d['doc'] for w in range(workers) for d in docs(w, calls - 1)}
            rows = self.run_cbq_query("SELECT META(d).id AS k, d AS v FROM {} AS d USE KEYS $keys"
                                      .format(ks), debug_query=False,
                                      query_params={'$keys': json.dumps(sorted(expected))})['results']
            self.assert_same_rows(rows, [{'k': k, 'v': v} for k, v in expected.items()],
                                  "Last batch of every worker", seed)
        finally:
            if created:
                self.run_cbq_query('DROP FUNCTION {}'.format(fn))
            self.delete_library(library)
            self._drop_scope(scope)

    def test_mb73258_group_by_distinct_aggs_memory_bounded(self):
        """MB-73258 (CBSE-23450): every DISTINCT aggregate of every group allocated a set of
        about 24 KB that the memory quota counted as 8 bytes, so a GROUP BY with many groups
        and DISTINCT aggregates grew cbq-engine by GBs. The query must return exactly the
        expected groups while cbq-engine RSS grows by less than rss_growth_mb (the bug grew
        it by about 1.3 GB at 20k groups)."""
        rng, seed = self.seeded_rng("MB-73258")
        scope, index = 's_mem_group', 'ix_mem_group'
        num = int(self.input.param("group_docs", 20000))
        growth_mb = int(self.input.param("rss_growth_mb", 768))
        names = rng.randint(20, 200)
        limit = 1000
        try:
            ks = self._create_scope(scope, ['docs'], [(index, 'docs', '(client_name, cluid)')])['docs']
            self.assert_mutations(self.run_cbq_query(
                'UPSERT INTO {} (KEY k, VALUE v) SELECT "k" || TO_STR(d) AS k, '
                '{{"cluid": "K" || TO_STR(d), "client_name": "n" || TO_STR(d % {})}} AS v '
                'FROM ARRAY_RANGE(0, {}) AS d'.format(ks, names, num)), num, "UPSERT")
            self.assert_item_count(self.buckets[0], num, scope, 'docs')
            # Restarts cbq-engine last, so the RSS baseline below is a fresh process.
            self._validate_indexes([self._collection_index_spec(
                scope, 'docs', ks, index, "client_name IS NOT MISSING", num)])
            # The ticket's query, plus ORDER BY so the LIMIT keeps known rows.
            query = ("SELECT LOWER(d.cluid) AS k, ARRAY_AGG(DISTINCT d.cluid) AS v, "
                     "ARRAY_AGG(DISTINCT d.client_name) AS c, COUNT(1) AS docs FROM {} AS d "
                     "WHERE d.client_name IS NOT NULL GROUP BY LOWER(d.cluid) "
                     "HAVING COUNT(DISTINCT d.cluid) > 0 ORDER BY k LIMIT {}".format(ks, limit))
            self.assert_index_plan(self.explain(query), index)
            expected = sorted(({'k': 'k{}'.format(d), 'v': ['K{}'.format(d)],
                                'c': ['n{}'.format(d % names)], 'docs': 1} for d in range(num)),
                              key=lambda row: row['k'])[:limit]
            rss_before = self.query_rss_mb(self.master)
            peak = rss_before
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self.run_cbq_query, query)
                while not future.done():
                    peak = max(peak, self.query_rss_mb(self.master))
                    time.sleep(0.25)
                rows = future.result()['results']
            self.log.info("MB-73258: %d groups, cbq-engine RSS %.0f MB, peak %.0f MB (seed=%s)",
                          num, rss_before, peak, seed)
            self.assert_same_rows(rows, expected, "GROUP BY with DISTINCT aggregates", seed,
                                  ordered=True)
            self.assertLess(peak - rss_before, growth_mb,
                            "MB-73258: cbq-engine RSS grew {:.0f} MB (from {:.0f} MB) during a "
                            "GROUP BY over {} groups with DISTINCT aggregates, limit {} MB "
                            "(seed={})".format(peak - rss_before, rss_before, num, growth_mb, seed))
        finally:
            self._drop_scope(scope)

    # --- KV connection resilience (CBSE-22897, CBSE-23370, CBSE-22033) ---

    def _wait_queries_recover(self, run_round, after, seed, timeout):
        """run_round() until it succeeds. While KV is coming back only error 12008 (the
        query service lost its connection to KV) is allowed, for at most `timeout`
        seconds; any other error is raised. Returns the 12008 errors seen."""
        deadline = time.time() + timeout
        errors = []
        while True:
            try:
                run_round()
                return errors
            except CBQError as ex:
                error = self.process_CBQE(ex)
                if error.get('code') != KV_CONNECTION_ERROR_CODE:
                    raise
                errors.append(error)
                if time.time() > deadline:
                    self.fail("Queries still fail {}s after {} (the query service did not "
                              "reconnect to KV by itself): {} errors {}, last {} (seed={})".format(
                                  timeout, after, KV_CONNECTION_ERROR_CODE, len(errors),
                                  error, seed))
                time.sleep(2)

    def _kv_workload(self, seed, ks_groups, groups_docs, ks_writes):
        """A round a customer application runs, on one query node: documents of a random
        _type (index scan + KV fetch), of a random group in a collection (index scan +
        fetch), then (writes=True) an UPSERT batch read back by key. Every result is
        exact. Each round's inputs come from (seed, label, node), so a retried round
        repeats its inputs and the run replays with the same seed."""
        kv_docs = self._kv_dataset()
        types = sorted({d['_type'] for d in kv_docs.values() if isinstance(d.get('_type'), str)})
        counter = itertools.count(1)

        def run_round(server, label, writes=True):
            rng = random.Random("{}|{}|{}".format(seed, label, server.ip))
            doc_type = rng.choice(types)
            rows = self.run_cbq_query(
                TYPE_FETCH_SQL.format(self.query_bucket), server=server,
                query_params={'$t': json.dumps(doc_type)})['results']
            self.assert_same_rows(rows, [{'k': k, 'v': d} for k, d in kv_docs.items()
                                         if d.get('_type') == doc_type],
                                  "{} on {}: _type {} documents".format(label, server.ip, doc_type),
                                  seed)
            group = rng.choice(sorted({d['grp'] for d in groups_docs.values()}))
            rows = self.run_cbq_query(
                GROUP_SCAN_SQL.format(ks_groups),
                server=server, query_params={'$g': json.dumps(group)})['results']
            self.assert_same_rows(rows, [{'k': k, 'v': d} for k, d in groups_docs.items()
                                         if d['grp'] == group],
                                  "{} on {}: group {} documents".format(label, server.ip, group),
                                  seed)
            if not writes:
                return
            batch_no = next(counter)    # thread-safe: workload threads share the counter
            batch = {'w::{}::{}'.format(batch_no, i): {'round': batch_no, 'i': i,
                                                         'v': rng.randint(0, 10 ** 6)}
                     for i in range(rng.randint(1, 50))}
            self.assert_mutations(self.run_cbq_query(
                "UPSERT INTO {} (KEY, VALUE) VALUES {}".format(ks_writes, ", ".join(
                    "({}, {})".format(json.dumps(k), json.dumps(v)) for k, v in batch.items())),
                server=server, debug_query=False), len(batch), "{} on {}: UPSERT".format(
                    label, server.ip))
            rows = self.run_cbq_query(
                "SELECT META(w).id AS k, w AS v FROM {} AS w USE KEYS $keys".format(ks_writes),
                server=server, debug_query=False,
                query_params={'$keys': json.dumps(sorted(batch))})['results']
            self.assert_same_rows(rows, [{'k': k, 'v': v} for k, v in batch.items()],
                                  "{} on {}: UPSERT batch read back by key".format(
                                  label, server.ip), seed)
        return run_round

    def _conn_checks(self, rng, ks_groups, groups_docs):
        """reassert_queries checks for the two reads of _kv_workload: documents of a random
        _type (through REF_INDEX) and of a random group (through ix_conn_grp)."""
        kv_docs = self._kv_dataset()
        doc_type = rng.choice(sorted({d['_type'] for d in kv_docs.values()
                                      if isinstance(d.get('_type'), str)}))
        group = rng.choice(sorted({d['grp'] for d in groups_docs.values()}))
        return [(TYPE_FETCH_SQL.format(self.query_bucket), REF_INDEX,
                 [{'k': k, 'v': d} for k, d in kv_docs.items() if d.get('_type') == doc_type],
                 {'$t': json.dumps(doc_type)}),
                (GROUP_SCAN_SQL.format(ks_groups), 'ix_conn_grp',
                 [{'k': k, 'v': d} for k, d in groups_docs.items() if d['grp'] == group],
                 {'$g': json.dumps(group)})]

    def _conn_setup(self, rng, scope):
        """Scope with a grouped collection (index on grp, validated) and a write
        collection; returns (keyspaces, grouped documents)."""
        num = int(self.input.param("conn_docs", 2000))
        groups = rng.randint(5, 50)
        docs = {'c::{}'.format(i): {'grp': 'g{}'.format(rng.randrange(groups)), 'n': i,
                                    'pad': 'p' * rng.randint(10, 500)} for i in range(num)}
        # One replica, as customers run it: the index survives a node failover.
        ks = self._create_scope(scope, ['conn_docs', 'conn_writes'],
                                [('ix_conn_grp', 'conn_docs', '(grp)', '{"num_replica": 1}')])
        self._insert_docs(docs, [], keyspace=ks['conn_docs'])
        self.assert_item_count(self.buckets[0], num, scope, 'conn_docs')
        self._validate_indexes([self._collection_index_spec(
            scope, 'conn_docs', ks['conn_docs'], 'ix_conn_grp', "grp IS NOT MISSING", num,
            replicas=1)])
        # The workload's queries, with the same parameters.
        self.assert_index_plan(self.explain(
            "SELECT META(c).id AS k, c AS v FROM {} AS c WHERE c.grp = $g".format(
                ks['conn_docs']), query_params={'$g': '"g0"'}), 'ix_conn_grp')
        self.assert_index_plan(self.explain(
            "SELECT META(d).id AS k, d AS v FROM {} AS d WHERE d._type = $t".format(
                self.query_bucket), query_params={'$t': '"x"'}), REF_INDEX)
        return ks, docs

    def _new_collection_round(self, scope, name, rng, seed, label):
        """MB-70453: a collection created after the disturbance (a new manifest) is
        written and read back by key on every query node."""
        bucket = self.buckets[0].name
        self.assertTrue(CollectionsN1QL(self.master).create_collection(
            bucket_name=bucket, scope_name=scope, collection_name=name),
            "Collection {}.{} was not created".format(scope, name))
        ks = 'default:`{}`.`{}`.`{}`'.format(bucket, scope, name)
        for node in self.get_query_nodes():
            deadline = time.time() + 60
            while not self.run_cbq_query(
                    "SELECT RAW k.name FROM system:keyspaces AS k WHERE k.`bucket` = {} AND "
                    "k.`scope` = {} AND k.name = {}".format(self.n1ql_literal(bucket), self.n1ql_literal(scope),
                                                            self.n1ql_literal(name)),
                    server=node)['results']:
                self.assertLess(time.time(), deadline, "{} not visible on {}".format(ks, node.ip))
                time.sleep(2)
            docs = {'{}::{}::{}'.format(name, node.ip, i): {'i': i, 'v': rng.randint(0, 999)}
                    for i in range(rng.randint(1, 20))}
            self._insert_docs(docs, [], keyspace=ks)
            rows = self.run_cbq_query("SELECT META(n).id AS k, n AS v FROM {} AS n USE KEYS $keys"
                                      .format(ks), server=node, debug_query=False,
                                      query_params={'$keys': json.dumps(sorted(docs))})['results']
            self.assert_same_rows(rows, [{'k': k, 'v': v} for k, v in docs.items()],
                                  "{}: new collection {} on {}".format(label, name, node.ip), seed)

    def test_mb72084_kv_restart_query_connections_recover(self):
        """MB-72084 (CBSE-22897), MB-73071 (CBSE-23370), MB-70453 (CBSE-22033): after KV on
        a node restarted, the query service kept using its old connections to memcached
        (12008 "connection timed out" / "i/o timeout" for hours, until cbq-engine was
        restarted) and could fail to get the collections manifest ("Failed to get
        collection ID for scan"). memcached is killed, couchbase-server restarted, and the
        node gracefully failed over and added back with delta recovery (as for patching),
        on each data node in turn. Throughout each one, a background workload (fetches
        and collection scans) runs on every query node that stays up and
        must give exact results; only error 12008 is allowed, and queries must recover by
        themselves within recovery_timeout. Then fetches, collection scans, writes and a
        new collection give exact results on every query node, the reads keep their
        index and the same plan on every node; cbq-engine is not
        restarted by a memcached kill (only on that node by a restart or failover)."""
        rng, seed = self.seeded_rng("MB-72084")
        scope = 's_conn'
        timeout = int(self.input.param("recovery_timeout", 120))
        disturbances = self.input.param(
            "kv_disturbances", "memcached_kill,server_restart,failover_delta").split(',')
        try:
            ks, docs = self._conn_setup(rng, scope)
            run_round = self._kv_workload(seed, ks['conn_docs'], docs, ks['conn_writes'])
            for node in self.get_query_nodes():
                run_round(node, "before any restart")
            step = 0
            for disturbance in disturbances:
                for kv_node in self.get_nodes_from_services_map(service_type="kv",
                                                                get_all_nodes=True):
                    step += 1
                    label = "{} on {}".format(disturbance, kv_node.ip)
                    pids = self.cbq_pids()
                    # The workload runs on the query nodes that stay up: a memcached kill
                    # leaves every query service up, a restart or failover not the one on
                    # kv_node.
                    stays_up = [n for n in self.get_query_nodes()
                                if disturbance == 'memcached_kill' or n.ip != kv_node.ip]
                    if disturbance == 'memcached_kill':
                        def action(node=kv_node, label=label):
                            self.kill_server_memcached(node)
                            self.assertTrue(ClusterOperationHelper._wait_warmup_completed(
                                self, [node], self.buckets[0].name),
                                "Bucket on {} did not warm up after {}".format(node.ip, label))
                    elif disturbance == 'server_restart':
                        action = functools.partial(self.restart_couchbase_server, kv_node)
                    elif disturbance == 'failover_delta':
                        action = functools.partial(self.failover_and_recover, kv_node)
                    else:
                        self.fail("Unknown kv_disturbances entry: {}".format(disturbance))
                    self.run_query_workload(
                        # Reads only while the node is out: writes are checked once it is back.
                        action, lambda server, n, label=label: run_round(
                            server, "during {} #{}".format(label, n), writes=False),
                        label, nodes=stays_up, allowed_codes=(KV_CONNECTION_ERROR_CODE,),
                        recover_timeout=timeout, seed=seed)
                    errors = self._wait_queries_recover(
                        lambda: run_round(self.master, label, writes=False), label, seed, timeout)
                    self.log.info("%s: queries recovered after %d errors %d (seed=%s)", label,
                                  len(errors), KV_CONNECTION_ERROR_CODE, seed)
                    for node in self.get_query_nodes():
                        run_round(node, "after " + label)
                    # Same index, same plan on every node, exact rows.
                    self.reassert_queries(self._conn_checks(rng, ks['conn_docs'], docs),
                                          "after " + label, seed)
                    self._new_collection_round(scope, 'new{}'.format(step), rng, seed, label)
                    # A memcached kill must not restart cbq-engine anywhere; a server restart
                    # or a failover may restart only the one on that node.
                    if disturbance in ('server_restart', 'failover_delta'):
                        pids.pop(kv_node.ip, None)
                    after = self.cbq_pids()
                    self.assertEqual({ip: after[ip] for ip in pids}, pids,
                                     "cbq-engine restarted during {}: {} -> {}".format(
                                         label, pids, after))
        finally:
            self._drop_scope(scope)

    def test_mb70478_encryption_level_change_query_kv_over_tls(self):
        """MB-70478 / MB-73105 (CBSE-22033), MB-73090 (CBSE-23370): when the cluster
        encryption level changed, or the query service rebuilt its bucket connections
        after an error, it could keep or go back to the plain KV port 11210, which the
        cluster refuses under 'strict': queries failed with 12008 "dial tcp ...:11210".
        The level goes control -> all -> strict on the fly, then memcached is killed on
        a data node under strict: queries keep giving exact results on every query node
        without restarting cbq-engine, and no query.log line dials port 11210 after the
        switch to strict."""
        rng, seed = self.seeded_rng("MB-70478")
        scope = 's_tls'
        timeout = int(self.input.param("recovery_timeout", 120))
        nton = ntonencryptionBase()
        failover = self.rest.get_autofailover_settings()
        encrypted = False
        try:
            ks, docs = self._conn_setup(rng, scope)
            run_round = self._kv_workload(seed, ks['conn_docs'], docs, ks['conn_writes'])
            pids = self.cbq_pids()
            encrypted = True
            nton.setup_nton_cluster([self.master], clusterEncryptionLevel='all')
            self.assertEqual(nton.get_encryption_level_cli(self.master), 'all')
            for node in self.get_query_nodes():
                run_round(node, "encryption level all")
            marks = {n.ip: self.query_log_line_count(n) for n in self.get_query_nodes()}
            nton.change_cluster_encryption_cli([self.master], 'strict')
            # Under strict only TLS ports take connections from other hosts.
            self.use_https = True
            CbServer.use_https = True
            self.assertEqual(nton.get_encryption_level_cli(self.master), 'strict')
            errors = self._wait_queries_recover(
                lambda: run_round(self.master, "strict", writes=False), "the switch to strict",
                seed, timeout)
            self.log.info("strict: queries ran after %d errors %d", len(errors),
                          KV_CONNECTION_ERROR_CODE)
            for node in self.get_query_nodes():
                run_round(node, "encryption level strict")
            kv_node = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True)[-1]
            label = "memcached kill on {} under strict".format(kv_node.ip)
            self.kill_server_memcached(kv_node)
            self.wait_cluster_healthy(label)
            errors = self._wait_queries_recover(
                lambda: run_round(self.master, label, writes=False), label, seed, timeout)
            self.log.info("%s: queries recovered after %d errors %d (seed=%s)", label,
                          len(errors), KV_CONNECTION_ERROR_CODE, seed)
            for node in self.get_query_nodes():
                run_round(node, "after " + label)
            self.assert_cbq_not_restarted(pids, "the encryption level changes")
            for node in self.get_query_nodes():
                dials = self.query_log_grep(node, marks[node.ip], r'dial tcp [^ ]*:11210')
                self.assertFalse(dials, "MB-70478/MB-73090: under strict, query.log on {} shows "
                                 "connections to the plain KV port 11210: {}".format(
                                     node.ip, dials))
        finally:
            if encrypted:
                nton.disable_nton_cluster([self.master])
                self.use_https = False
                CbServer.use_https = False
                self.rest.update_autofailover_settings(failover.enabled, failover.timeout)
                self.wait_cluster_healthy("restoring encryption level control")
            self._drop_scope(scope)
