import time
from .tuq import QueryTests


class FakeTests(QueryTests):

    def suite_setUp(self):
        super(FakeTests, self).suite_setUp()

    def test_1(self):
        t = int(time.time())
        self.assertEqual(t % 2, 0, "Test 1 is failed")

    def test_2(self):
        t = int(time.time())
        self.assertEqual(t % 2, 0, "Test 2 is failed")

    def test_3(self):
        t = int(time.time())
        self.assertEqual(t % 2, 0, "Test 3 is failed")

    def test_4(self):
        t = int(time.time())
        self.assertEqual(t % 2, 0, "Test 4 is failed")

    def test_5(self):
        t = int(time.time())
        self.assertEqual(t % 2, 0, "Test 5 is failed")

    def test_6(self):
        t = int(time.time())
        self.assertEqual(t % 2, 0, "Test 6 is failed")

    def test_7(self):
        t = int(time.time())
        self.assertEqual(t % 2, 0, "Test 7 is failed")

    def test_8(self):
        t = int(time.time())
        self.assertEqual(t % 2, 0, "Test 8 is failed")

    def test_9(self):
        t = int(time.time())
        self.assertEqual(t % 2, 0, "Test 9 is failed")

    def test_10(self):
        t = int(time.time())
        self.assertEqual(t % 2, 0, "Test 10 is failed")
