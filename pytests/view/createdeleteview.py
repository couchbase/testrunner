from view.view_base import ViewBaseTest
from couchbase.documentgenerator import BlobGenerator
from couchbase.document import View

class CreateDeleteViewTests(ViewBaseTest):

    def setUp(self):
        super(CreateDeleteViewTests, self).setUp()

    def tearDown(self):
        super(CreateDeleteViewTests, self).tearDown()

    """Create single view in single design doc"""
    def test_create_view(self):
        design_doc_name = "ddoc1"
        server = self.servers[0]
        task = self.cluster.async_create_view(server, design_doc_name, self.default_view)
        task.result()

    """Add views to design doc design doc"""
    def test_add_views(self):
        design_doc_name = "ddoc1"
        views = self.make_default_views("test_add_views", 10)
        server = self.servers[0]
        self.create_views(server, design_doc_name, views)

    """Multiple design docs multiple views"""
    def test_multi_ddoc_multi_views(self):
        design_docs = ["ddoc1","ddoc2","ddoc3"]
        views = self.make_default_views("test_multi_ddoc_multi_views", 2)
        server = self.servers[0]

        for design_doc in design_docs:
            self.create_views(server, design_doc, views)

