from basetestcase import BaseTestCase
from couchbase.document import View

class ViewBaseTest(BaseTestCase):

    def setUp(self):
        super(ViewBaseTest, self).setUp()

        map_func = 'function (doc) { emit(null, doc);}'
        self.default_view = View("default_view", map_func, None)


    def tearDown(self):
        super(ViewBaseTest, self).tearDown()
