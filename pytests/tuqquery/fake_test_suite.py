from tuq import QueryTests

class FakeTests(QueryTests):

    def suite_setUp(self):
        super(FakeTests, self).suite_setUp()

    def test_negative_1(self):
        self.assertEquals(True, False, "Negative test 1 is failed")

    def test_negative_2(self):
        self.assertEquals(True, False, "Negative test 2 is failed")

    def test_positive_1(self):
        self.assertEquals(True, True, "Positive test 1 is failed")

    def test_positive_2(self):
        self.assertEquals(True, False, "Positive test 2 is failed")


