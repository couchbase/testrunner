from basetestcase import BaseTestCase
from couchbase.document import View

class ViewBaseTest(BaseTestCase):

    def setUp(self):
        super(ViewBaseTest, self).setUp()

        map_func = 'function (doc) { emit(null, doc);}'
        self.default_view = View("default_view", map_func, None)

    def tearDown(self):
        super(ViewBaseTest, self).tearDown()


    def async_create_views(self, server, design_doc_name, views, bucket = "default"):
        tasks = []
        for view in views:
            t_ = self.cluster.async_create_view(server, design_doc_name, view, bucket)
            tasks.append(t_)
        return tasks

    def create_views(self, server, design_doc_name, views, bucket = "default", timeout=None):
        for view in views:
            self.cluster.create_view(server, design_doc_name, view, bucket, timeout)

    def make_default_views(self, prefix, count):
        ref_view = self.default_view
        return [View(ref_view.name+str(i), ref_view.map_func, None, False) \
                for i in range(0,count)]

