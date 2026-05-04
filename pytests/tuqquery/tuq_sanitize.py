from .tuq import QueryTests


class QuerySanitizeTests(QueryTests):
    def setUp(self):
        super(QuerySanitizeTests, self).setUp()
        self.bucket = "default"

    def suite_setUp(self):
        super(QuerySanitizeTests, self).suite_setUp()

    def tearDown(self):
        super(QuerySanitizeTests, self).tearDown()

    def suite_tearDown(self):
        super(QuerySanitizeTests, self).suite_tearDown()

    def test_sanitize_basic(self):
        """
        MB-68699: Test SANITIZE function with basic SELECT statements.
        """
        result = self.run_cbq_query('SELECT SANITIZE("SELECT * FROM default WHERE name = \\"xyz\\" AND age > 10") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized output: {sanitized}")
        self.assertIn('statement', sanitized)
        self.assertIn('parametersMap', sanitized)
        self.assertTrue(len(sanitized['parametersMap']) >= 2,
                        f"Expected at least 2 parameters, got {sanitized['parametersMap']}")
        self.assertNotIn('"xyz"', sanitized['statement'])
        self.assertIn('$_n', sanitized['statement'])
        param_values = list(sanitized['parametersMap'].values())
        self.assertIn('xyz', param_values)
        self.assertIn(10, param_values)

        # SELECT with no literals
        result = self.run_cbq_query('SELECT SANITIZE("SELECT * FROM default") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized (no literals): {sanitized}")
        self.assertIn('statement', sanitized)
        params = sanitized.get('parametersMap') or {}
        self.assertEqual(len(params), 0,
                         f"Expected empty/null parametersMap, got {sanitized.get('parametersMap')}")

    def test_sanitize_dml_statements(self):
        """
        MB-68699: Test SANITIZE function with DML statements (UPDATE, DELETE).
        """
        result = self.run_cbq_query('SELECT SANITIZE("UPDATE default SET name = \\"new_name\\" WHERE id = 5") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized UPDATE: {sanitized}")
        param_values = list(sanitized['parametersMap'].values())
        self.assertIn('new_name', param_values)
        self.assertIn(5, param_values)
        self.assertNotIn('"new_name"', sanitized['statement'])

        result = self.run_cbq_query('SELECT SANITIZE("DELETE FROM default WHERE status = \\"inactive\\"") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized DELETE: {sanitized}")
        param_values = list(sanitized['parametersMap'].values())
        self.assertIn('inactive', param_values)

    def test_sanitize_complex(self):
        """
        MB-68699: Test SANITIZE with subqueries, nested expressions, and multiple same values.
        """
        result = self.run_cbq_query('SELECT SANITIZE("SELECT * FROM default d1 WHERE id IN (SELECT RAW id FROM default d2 WHERE x = 100)") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized subquery: {sanitized}")
        param_values = list(sanitized['parametersMap'].values())
        self.assertIn(100, param_values)

        result = self.run_cbq_query('SELECT SANITIZE("SELECT c1, {c1, \\"o1\\":{c2, \\"c3\\": \\"abc\\", \\"c4\\": abs(c2)}} AS o FROM default AS d WHERE c1 = 10") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized array/object: {sanitized}")
        param_values = list(sanitized['parametersMap'].values())
        self.assertIn('abc', param_values)
        self.assertIn(10, param_values)

        result = self.run_cbq_query('SELECT SANITIZE("SELECT * FROM default WHERE a = \\"x\\" AND b = \\"x\\"") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized duplicate values: {sanitized}")
        self.assertTrue(len(sanitized['parametersMap']) >= 2,
                        f"Each literal should get its own parameter, got {sanitized['parametersMap']}")

    def test_sanitize_insert_upsert(self):
        """
        MB-68699: Test SANITIZE with INSERT and UPSERT statements.
        """
        result = self.run_cbq_query('SELECT SANITIZE("INSERT INTO default VALUES (\\"k1\\", {\\"name\\": \\"abc\\"})") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized INSERT: {sanitized}")
        self.assertIn('statement', sanitized)
        self.assertIn('parametersMap', sanitized)
        param_values = list(sanitized['parametersMap'].values())
        self.assertIn('k1', param_values)
        self.assertTrue(any(isinstance(v, dict) and v.get('name') == 'abc' for v in param_values),
                        f"Expected object with name=abc in params, got {param_values}")

        result = self.run_cbq_query('SELECT SANITIZE("UPSERT INTO default VALUES (\\"k2\\", {\\"x\\": 1})") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized UPSERT: {sanitized}")
        param_values = list(sanitized['parametersMap'].values())
        self.assertIn('k2', param_values)
        self.assertTrue(any(isinstance(v, dict) and v.get('x') == 1 for v in param_values),
                        f"Expected object with x=1 in params, got {param_values}")

    def test_sanitize_merge(self):
        """
        MB-68699: Test SANITIZE with MERGE statement.
        """
        result = self.run_cbq_query('SELECT SANITIZE("MERGE INTO default t1 USING default t2 ON t1.id = t2.id WHEN MATCHED THEN UPDATE SET t1.v = \\"val\\"") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized MERGE: {sanitized}")
        self.assertIn('statement', sanitized)
        self.assertIn('parametersMap', sanitized)
        param_values = list(sanitized['parametersMap'].values())
        self.assertIn('val', param_values)

    def test_sanitize_null_boolean(self):
        """
        MB-68699: Test SANITIZE with NULL, true, false literals.
        """
        result = self.run_cbq_query('SELECT SANITIZE("SELECT * FROM default WHERE a IS NULL AND b = true AND c = false") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized NULL/boolean: {sanitized}")
        self.assertIn('statement', sanitized)
        self.assertIn('parametersMap', sanitized)

    def test_sanitize_nested_functions(self):
        """
        MB-68699: Test SANITIZE with literals inside nested functions.
        """
        result = self.run_cbq_query('SELECT SANITIZE("SELECT * FROM default WHERE LOWER(name) = \\"test\\" AND ABS(val) > 5.5") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized nested functions: {sanitized}")
        self.assertIn('statement', sanitized)
        param_values = list(sanitized['parametersMap'].values())
        self.assertIn('test', param_values) 
        self.assertIn(5.5, param_values)

    def test_sanitize_ddl_noop(self):
        """
        MB-68699: Verify DDL statements (CREATE INDEX, CREATE FUNCTION) are not sanitized.
        """
        result = self.run_cbq_query('SELECT SANITIZE("CREATE INDEX idx1 ON default(name)") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized CREATE INDEX: {sanitized}")
        self.assertIn('statement', sanitized)
        params = sanitized.get('parametersMap') or {}
        self.assertEqual(len(params), 0,
                         f"DDL CREATE INDEX should not be sanitized, got {params}")

        result = self.run_cbq_query('SELECT SANITIZE("DROP INDEX idx1 ON default") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized DROP INDEX: {sanitized}")
        self.assertIn('statement', sanitized)
        params = sanitized.get('parametersMap') or {}
        self.assertEqual(len(params), 0,
                         f"DDL DROP INDEX should not be sanitized, got {params}")

    def test_sanitize_dcl_noop(self):
        """
        MB-68699: Verify DCL statements (GRANT, REVOKE) are not sanitized.
        """
        result = self.run_cbq_query('SELECT SANITIZE("GRANT query_select ON default TO user1") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized GRANT: {sanitized}")
        self.assertIn('statement', sanitized)
        params = sanitized.get('parametersMap') or {}
        self.assertEqual(len(params), 0,
                         f"DCL GRANT should not be sanitized, got {params}")

        result = self.run_cbq_query('SELECT SANITIZE("REVOKE query_select ON default FROM user1") AS sanitized')
        sanitized = result['results'][0]['sanitized']
        self.log.info(f"Sanitized REVOKE: {sanitized}")
        self.assertIn('statement', sanitized)
        params = sanitized.get('parametersMap') or {}
        self.assertEqual(len(params), 0,
                         f"DCL REVOKE should not be sanitized, got {params}")
