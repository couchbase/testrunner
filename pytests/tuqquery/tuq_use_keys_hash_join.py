"""
Tests for MB-68273: JOIN with USE KEYS on both sides should support Hash Join.

Background
----------
When the right-hand JOIN term carries USE KEYS, the optimizer previously only
considered Nested Loop (NL), which re-fetches the right side once per left row.
The fix makes the optimizer also consider HashJoin in this case (RBO path).

Test strategy
-------------
* Correctness tests: verify JOIN with USE KEYS on both sides returns correct rows
  AND assert the plan operator.  With small key sets the cost model picks NL, so
  these tests assert NestedLoopJoin in the plan.
* Explicit HashJoin tests: use `USE HASH(BUILD) KEYS [...]` hint on the right side
  to force the hash path and assert HashJoin appears in the plan.  This confirms the
  hash-join code path is reachable with USE KEYS (pre-fix it was not).
"""

from .tuq import QueryTests


class UseKeysHashJoinTests(QueryTests):

    def setUp(self):
        super(UseKeysHashJoinTests, self).setUp()
        self.log.info("==============  UseKeysHashJoinTests setup has started ==============")
        # suite_setUp sets skip_buckets_handle=True on the shared input, so the default
        # bucket may not be created for subsequent test instances.  Create it via REST
        # if it is absent so each test is self-sufficient.
        existing = [b.name for b in self.buckets]
        if 'default' not in existing:
            self.log.info("default bucket missing — creating via REST")
            self.rest.create_bucket(bucket='default', ramQuotaMB=256,
                                    replicaNumber=0, proxyPort=11211)
            self.sleep(5, "wait for default bucket to be ready")
        self.query_buckets = self.get_query_buckets(check_all_buckets=True)
        self.bucket = self.query_buckets[0]
        self._insert_test_docs()
        self.log.info("==============  UseKeysHashJoinTests setup has completed ==============")
        self.log_config_info()

    def suite_setUp(self):
        super(UseKeysHashJoinTests, self).suite_setUp()
        self.log.info("==============  UseKeysHashJoinTests suite_setUp has started ==============")
        self.log.info("==============  UseKeysHashJoinTests suite_setUp has completed ==============")
        self.log_config_info()

    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  UseKeysHashJoinTests tearDown has started ==============")
        self._delete_test_docs()
        self.log.info("==============  UseKeysHashJoinTests tearDown has completed ==============")
        super(UseKeysHashJoinTests, self).tearDown()

    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  UseKeysHashJoinTests suite_tearDown has started ==============")
        self.log.info("==============  UseKeysHashJoinTests suite_tearDown has completed ==============")
        super(UseKeysHashJoinTests, self).suite_tearDown()

    # ------------------------------------------------------------------ helpers

    def _insert_test_docs(self):
        inserts = [
            ('order::1',    '{"type":"order","order_id":"o1","customer_ref":"c1","amount":100}'),
            ('order::2',    '{"type":"order","order_id":"o2","customer_ref":"c2","amount":200}'),
            ('order::3',    '{"type":"order","order_id":"o3","customer_ref":"c1","amount":300}'),
            ('customer::1', '{"type":"customer","customer_id":"c1","name":"Alice","city":"NYC"}'),
            ('customer::2', '{"type":"customer","customer_id":"c2","name":"Bob","city":"SFO"}'),
            ('customer::3', '{"type":"customer","customer_id":"c3","name":"Carol","city":"LAX"}'),
        ]
        for key, val in inserts:
            self.run_cbq_query(
                'UPSERT INTO {0} (KEY, VALUE) VALUES ("{1}", {2})'.format(self.bucket, key, val)
            )

    def _delete_test_docs(self):
        if not getattr(self, 'bucket', None):
            return
        keys = ['order::1', 'order::2', 'order::3',
                'customer::1', 'customer::2', 'customer::3']
        keys_str = ','.join('"{0}"'.format(k) for k in keys)
        self.run_cbq_query('DELETE FROM {0} USE KEYS [{1}]'.format(self.bucket, keys_str))

    def _get_plan(self, query):
        result = self.run_cbq_query('EXPLAIN ' + query)
        return self.ExplainPlanHelper(result)

    # ------------------------------------------------------------------ correctness tests
    # Plain USE KEYS lets the optimizer choose; with tiny key sets it always picks NL.
    # Each test asserts both the result rows AND that NestedLoopJoin appears in the plan.

    def test_use_keys_both_sides_correct_results(self):
        """MB-68273: JOIN with USE KEYS on both sides + equality ON → correct 2 rows, NL plan."""
        query = (
            'SELECT o.amount, c.name '
            'FROM {0} o USE KEYS ["order::1","order::2"] '
            'JOIN {0} c USE KEYS ["customer::1","customer::2"] '
            'ON o.customer_ref = c.customer_id '
            'ORDER BY o.amount'.format(self.bucket)
        )
        plan = self._get_plan(query)
        self.assertIn("NestedLoopJoin", str(plan),
                      "Expected NestedLoopJoin in plan, got: {0}".format(plan))
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')
        rows = result['results']
        self.assertEqual(len(rows), 2, "Expected 2 rows, got {0}".format(rows))
        self.assertEqual(rows[0], {'amount': 100, 'name': 'Alice'})
        self.assertEqual(rows[1], {'amount': 200, 'name': 'Bob'})

    def test_use_keys_both_sides_multiple_keys_correct_results(self):
        """MB-68273: Multiple keys each side → 3 rows (Alice twice, Bob once), NL plan."""
        query = (
            'SELECT o.amount, c.name '
            'FROM {0} o USE KEYS ["order::1","order::2","order::3"] '
            'JOIN {0} c USE KEYS ["customer::1","customer::2","customer::3"] '
            'ON o.customer_ref = c.customer_id '
            'ORDER BY o.amount'.format(self.bucket)
        )
        plan = self._get_plan(query)
        self.assertIn("NestedLoopJoin", str(plan),
                      "Expected NestedLoopJoin in plan, got: {0}".format(plan))
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')
        rows = result['results']
        self.assertEqual(len(rows), 3, "Expected 3 rows, got {0}".format(rows))
        names = [r['name'] for r in rows]
        self.assertEqual(names.count('Alice'), 2)
        self.assertEqual(names.count('Bob'), 1)

    def test_use_keys_both_sides_no_match(self):
        """MB-68273: Keys don't satisfy ON condition → empty result, no error, NL plan."""
        query = (
            'SELECT o.amount, c.name '
            'FROM {0} o USE KEYS ["order::1"] '
            'JOIN {0} c USE KEYS ["customer::2"] '
            'ON o.customer_ref = c.customer_id'.format(self.bucket)
        )
        plan = self._get_plan(query)
        self.assertIn("NestedLoopJoin", str(plan),
                      "Expected NestedLoopJoin in plan, got: {0}".format(plan))
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')
        self.assertEqual(len(result['results']), 0,
                         "Expected 0 rows (no matching ON), got {0}".format(result['results']))

    def test_use_keys_both_sides_single_key_each(self):
        """MB-68273: Single key per side → 1 matching row, NL plan."""
        query = (
            'SELECT o.amount, c.name '
            'FROM {0} o USE KEYS ["order::1"] '
            'JOIN {0} c USE KEYS ["customer::1"] '
            'ON o.customer_ref = c.customer_id'.format(self.bucket)
        )
        plan = self._get_plan(query)
        self.assertIn("NestedLoopJoin", str(plan),
                      "Expected NestedLoopJoin in plan, got: {0}".format(plan))
        result = self.run_cbq_query(query)
        self.assertEqual(result['status'], 'success')
        rows = result['results']
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], {'amount': 100, 'name': 'Alice'})

    # ------------------------------------------------------------------ forced HashJoin tests
    # USE HASH(BUILD) KEYS [...] forces the hash path.  Pre-fix, this combination
    # (hash hint + USE KEYS on the join term) was not considered by the optimizer.

    def test_forced_hash_join_with_use_keys_plan(self):
        """MB-68273: USE HASH(BUILD) KEYS [...] on right side → HashJoin in plan."""
        query = (
            'SELECT o.amount, c.name '
            'FROM {0} o USE KEYS ["order::1","order::2"] '
            'JOIN {0} c USE HASH(BUILD) KEYS ["customer::1","customer::2"] '
            'ON o.customer_ref = c.customer_id '
            'ORDER BY o.amount'.format(self.bucket)
        )
        plan = self._get_plan(query)
        self.assertTrue(
            "HashJoin" in str(plan),
            "Expected HashJoin with USE HASH(BUILD) KEYS hint, got: {0}".format(plan)
        )

    def test_forced_hash_join_with_use_keys_correct_results(self):
        """MB-68273: Forced HashJoin with USE KEYS → HashJoin in plan, same results as NL."""
        query_hash = (
            'SELECT o.amount, c.name '
            'FROM {0} o USE KEYS ["order::1","order::2"] '
            'JOIN {0} c USE HASH(BUILD) KEYS ["customer::1","customer::2"] '
            'ON o.customer_ref = c.customer_id '
            'ORDER BY o.amount'.format(self.bucket)
        )
        query_nl = (
            'SELECT o.amount, c.name '
            'FROM {0} o USE KEYS ["order::1","order::2"] '
            'JOIN {0} c USE NL KEYS ["customer::1","customer::2"] '
            'ON o.customer_ref = c.customer_id '
            'ORDER BY o.amount'.format(self.bucket)
        )
        plan_hash = self._get_plan(query_hash)
        self.assertIn("HashJoin", str(plan_hash),
                      "Expected HashJoin in plan for USE HASH(BUILD) query, got: {0}".format(plan_hash))
        plan_nl = self._get_plan(query_nl)
        self.assertIn("NestedLoopJoin", str(plan_nl),
                      "Expected NestedLoopJoin in plan for USE NL query, got: {0}".format(plan_nl))
        result_hash = self.run_cbq_query(query_hash)
        result_nl = self.run_cbq_query(query_nl)
        self.assertEqual(result_hash['status'], 'success')
        self.assertEqual(result_nl['status'], 'success')
        self.assertEqual(
            result_hash['results'], result_nl['results'],
            "HashJoin and NL results differ: hash={0}, nl={1}".format(
                result_hash['results'], result_nl['results'])
        )
