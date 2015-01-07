from base_2i import BaseSecondaryIndexingTests

class SecondaryIndexingCreateDropTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingCreateDropTests, self).setUp()

    def suite_setUp(self):
        super(SecondaryIndexingCreateDropTests, self).suite_setUp()

    def tearDown(self):
        super(SecondaryIndexingCreateDropTests, self).tearDown()

    def suite_tearDown(self):
        super(SecondaryIndexingCreateDropTests, self).suite_tearDown()

    def test_multi_create_drop_index(self):
        self.log.info("test_multi_create_drop_index")
        self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = True, drop_index = True)