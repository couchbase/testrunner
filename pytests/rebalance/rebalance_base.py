from basetestcase import BaseTestCase

class RebalanceBaseTest(BaseTestCase):

    def setUp(self):
        super(RebalanceBaseTest, self).setUp()
        self.value_size = self.input.param("value_size", 256)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        self.doc_ops=self.input.param("doc_ops", None)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")

    def tearDown(self):
        super(RebalanceBaseTest, self).tearDown()

