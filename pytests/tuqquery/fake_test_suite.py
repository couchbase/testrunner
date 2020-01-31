import random
from tuq import QueryTests

class FakeTests(QueryTests):

    def suite_setUp(self):
        super(FakeTests, self).suite_setUp()

    def test_1(self):
        i = random.randint(0, 1)
        self.assertEquals(i, 1, "Test 1 is failed")

    def test_2(self):
        i = random.randint(0, 1)
        self.assertEquals(i, 1, "Test 1 is failed")

    def test_3(self):
        i = random.randint(0, 1)
        self.assertEquals(i, 1, "Test 1 is failed")

    def test_4(self):
        i = random.randint(0, 1)
        self.assertEquals(i, 1, "Test 1 is failed")

    def test_5(self):
        i = random.randint(0, 1)
        self.assertEquals(i, 1, "Test 1 is failed")

    def test_6(self):
        i = random.randint(0, 1)
        self.assertEquals(i, 1, "Test 1 is failed")

    def test_7(self):
        i = random.randint(0, 1)
        self.assertEquals(i, 1, "Test 1 is failed")

    def test_8(self):
        i = random.randint(0, 1)
        self.assertEquals(i, 1, "Test 1 is failed")

    def test_9(self):
        i = random.randint(0, 1)
        self.assertEquals(i, 1, "Test 1 is failed")

    def test_10(self):
        i = random.randint(0, 1)
        self.assertEquals(i, 1, "Test 1 is failed")


