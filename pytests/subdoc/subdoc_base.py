from basetestcase import BaseTestCase


class SubdocBaseTest(BaseTestCase):
    def setUp(self):
        super(SubdocBaseTest, self).setUp()
        self.server = self.input.servers[0]

    def tearDown(self):
        super(SubdocBaseTest, self).tearDown()
