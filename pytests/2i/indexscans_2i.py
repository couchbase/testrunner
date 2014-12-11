from base_2i import BaseSecondaryIndexingTests

class SecondaryIndexingScanTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingScanTests, self).setUp()

    def suite_setUp(self):
        super(SecondaryIndexingScanTests, self).suite_setUp()

    def tearDown(self):
        super(SecondaryIndexingScanTests, self).tearDown()

    def suite_tearDown(self):
        super(SecondaryIndexingScanTests, self).suite_tearDown()

    def test_multi_create_query_explain_drop_index(self):
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = True, drop_index = True,
            query_with_explain = True, query = True)
