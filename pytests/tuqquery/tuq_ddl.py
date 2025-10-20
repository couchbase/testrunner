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

