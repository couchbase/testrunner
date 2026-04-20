from .tuq import QueryTests

class QueryDDLTests(QueryTests):
    def setUp(self):
        super(QueryDDLTests, self).setUp()
        self.bucket = "default"

    def suite_setUp(self):
        super(QueryDDLTests, self).suite_setUp()

    def tearDown(self):
        super(QueryDDLTests, self).tearDown()

    def suite_tearDown(self):
        super(QueryDDLTests, self).suite_tearDown()

    def test_drop_primary_index(self):
        # Drop primary (implicit name)
        self.run_cbq_query(f"DROP PRIMARY INDEX IF EXISTS ON {self.bucket}")

        self.run_cbq_query(f"CREATE PRIMARY INDEX ON {self.bucket}")
        self.run_cbq_query(f"DROP PRIMARY INDEX ON {self.bucket}")

        # Drop primary (explicit name)
        self.run_cbq_query(f"DROP PRIMARY INDEX idx1 IF EXISTS ON {self.bucket}")

        self.run_cbq_query(f"CREATE PRIMARY INDEX idx1 ON {self.bucket}")
        self.run_cbq_query(f"DROP PRIMARY INDEX idx1 ON {self.bucket}")

    def test_drop_index(self):
        self.run_cbq_query(f"DROP INDEX idx1 IF EXISTS ON {self.bucket}")

        self.run_cbq_query(f"CREATE INDEX idx1 ON {self.bucket}(a)")
        self.run_cbq_query(f"DROP INDEX idx1 ON {self.bucket}")

    def test_create_index_if_not_exists(self):
        self.run_cbq_query(f"DROP INDEX idx1 IF EXISTS ON {self.bucket}")

        self.run_cbq_query(f"CREATE INDEX idx1 ON {self.bucket}(a)")
        self.run_cbq_query(f"CREATE INDEX idx1 IF NOT EXISTS ON {self.bucket}(a)")

    def test_drop_index_if_exists(self):
        self.run_cbq_query(f"DROP INDEX idx1 IF EXISTS ON {self.bucket}")
        self.run_cbq_query(f"CREATE INDEX idx1 ON {self.bucket}(a)")

        self.run_cbq_query(f"DROP INDEX idx1 ON {self.bucket}")
        self.run_cbq_query(f"DROP INDEX idx1 IF EXISTS ON {self.bucket}")
        self.run_cbq_query(f"DROP INDEX {self.bucket}.idx1 IF EXISTS")

    def test_create_scope_collection_if_not_exists(self):
        self.run_cbq_query(f"DROP SCOPE {self.bucket}.scope1 IF EXISTS")
        self.sleep(3,"Wait for scope to be dropped")

        self.run_cbq_query(f"CREATE SCOPE {self.bucket}.scope1")
        self.sleep(3,"Wait for scope to be created")
        self.run_cbq_query(f"CREATE SCOPE {self.bucket}.scope1 IF NOT EXISTS")

        self.run_cbq_query(f"CREATE COLLECTION {self.bucket}.scope1.collection1")
        self.sleep(3,"Wait for collection to be created")
        self.run_cbq_query(f"CREATE COLLECTION {self.bucket}.scope1.collection1 IF NOT EXISTS")

    def test_drop_scope_collection_if_exists(self):
        self.run_cbq_query(f"DROP SCOPE {self.bucket}.scope1 IF EXISTS")
        self.run_cbq_query(f"CREATE SCOPE {self.bucket}.scope1")
        self.sleep(5)
        self.run_cbq_query(f"CREATE COLLECTION {self.bucket}.scope1.collection1")
        self.sleep(5)
        self.run_cbq_query(f"DROP COLLECTION {self.bucket}.scope1.collection1")
        self.run_cbq_query(f"DROP COLLECTION {self.bucket}.scope1.collection1 IF EXISTS")

        self.run_cbq_query(f"DROP SCOPE {self.bucket}.scope1")
        self.run_cbq_query(f"DROP SCOPE {self.bucket}.scope1 IF EXISTS")

    def test_create_function_if_not_exists(self):
        self.run_cbq_query("DROP FUNCTION fun1 IF EXISTS")

        self.run_cbq_query("CREATE FUNCTION fun1(a) { a * 1000}")
        self.run_cbq_query("CREATE FUNCTION fun1(a) IF NOT EXISTS { a * 1000}")

    def test_drop_function_if_exists(self):
        self.run_cbq_query("DROP FUNCTION fun1 IF EXISTS")
        self.run_cbq_query("CREATE FUNCTION fun1(a) { a * 1000}")

        self.run_cbq_query("DROP FUNCTION fun1")
        self.run_cbq_query("DROP FUNCTION fun1 IF EXISTS")
        self.run_cbq_query("DROP FUNCTION fun1 IF EXISTS")

    def test_extractddl_function_flag(self):
        """
        MB-67884: Test EXTRACTDDL function with function flag.
        MB-70691: Verify EXTRACTDDL filters functions by bucket.
        """
        try:
            # Create a global function and a scoped function
            self.run_cbq_query("CREATE OR REPLACE FUNCTION global_func1(a) { a + 1 }")
            self.run_cbq_query("CREATE OR REPLACE FUNCTION default._default.inline_func1(a) { a * 1000 }")

            # MB-67884: Verify scoped function appears when filtering by bucket
            ddl_result = self.run_cbq_query("SELECT EXTRACTDDL('default', {'flags': ['function']}) AS ddl_statements")
            ddl_text = str(ddl_result['results'])
            self.assertIn('inline_func1', ddl_text, "Scoped function should be in EXTRACTDDL output")

            # MB-70691: Verify global function is NOT returned when filtering by bucket
            self.assertNotIn('global_func1', ddl_text,
                             "Global function should NOT appear when filtering by specific bucket")

            # MB-70691: Verify no filter (empty bucket) returns ALL functions
            ddl_all = self.run_cbq_query("SELECT EXTRACTDDL('', {'flags': ['function']}) AS ddl_statements")
            ddl_all_text = str(ddl_all['results'])
            self.assertIn('global_func1', ddl_all_text, "Global function should appear with no bucket filter")
            self.assertIn('inline_func1', ddl_all_text, "Scoped function should appear with no bucket filter")

            # MB-70691: Verify no duplicate function entries
            ddl_statements = ddl_all['results'][0]['ddl_statements']
            func_names = [s for s in ddl_statements if 'inline_func1' in s]
            self.assertEqual(len(func_names), 1, "Scoped function should not be duplicated")

        finally:
            self.run_cbq_query("DROP FUNCTION IF EXISTS global_func1")
            self.run_cbq_query("DROP FUNCTION IF EXISTS default._default.inline_func1")

    def test_extractddl_prepared_flag(self):
        """
        MB-67884: Test EXTRACTDDL function with prepared flag.
        MB-70690: Verify no duplicate prepared statements and query context is preserved.
        """
        try:
            self.run_cbq_query("DELETE FROM system:prepareds")

            # Create prepared statements - one with default bucket, one global
            self.run_cbq_query("PREPARE prepared_stmt1 FROM SELECT * FROM default LIMIT 10")
            self.run_cbq_query("PREPARE prepared_stmt2 FROM SELECT name FROM default WHERE type = 'test'")

            # MB-67884: Verify prepared statements appear in output
            ddl_result = self.run_cbq_query("SELECT EXTRACTDDL('default', {'flags': ['prepared']}) AS ddl_statements")
            ddl_text = str(ddl_result['results'])
            self.assertIn('prepared_stmt1', ddl_text, "prepared_stmt1 should be in EXTRACTDDL output")
            self.assertIn('prepared_stmt2', ddl_text, "prepared_stmt2 should be in EXTRACTDDL output")

            # MB-70690: Verify no duplicate prepared statements
            ddl_statements = ddl_result['results'][0]['ddl_statements']
            stmt1_count = [s for s in ddl_statements if 'prepared_stmt1' in s]
            stmt2_count = [s for s in ddl_statements if 'prepared_stmt2' in s]
            self.assertEqual(len(stmt1_count), 1, "prepared_stmt1 should not be duplicated")
            self.assertEqual(len(stmt2_count), 1, "prepared_stmt2 should not be duplicated")

            # MB-70690: Verify no filter (empty bucket) also has no duplicates
            ddl_all = self.run_cbq_query("SELECT EXTRACTDDL('', {'flags': ['prepared']}) AS ddl_statements")
            ddl_all_stmts = ddl_all['results'][0]['ddl_statements']
            all_stmt1 = [s for s in ddl_all_stmts if 'prepared_stmt1' in s]
            self.assertEqual(len(all_stmt1), 1, "prepared_stmt1 should not be duplicated with empty filter")

        finally:
            self.run_cbq_query("DELETE FROM system:prepareds")

