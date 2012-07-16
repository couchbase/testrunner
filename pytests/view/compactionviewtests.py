from view.view_base import ViewBaseTest
from couchbase.documentgenerator import BlobGenerator
from couchbase.document import View
import time
from tasks.future import TimeoutError

class CompactionViewTests(ViewBaseTest):

    def setUp(self):
        super(CompactionViewTests, self).setUp()
        self.value_size = self.input.param("value_size", 256)
        self.fragmentation_value = self.input.param("fragmentation_value", 80)

    def tearDown(self):
        super(CompactionViewTests, self).tearDown()

    """Trigger Compaction When specified Fragmentation is reached"""
    def test_start_compaction(self):

        # disable auto compaction
        self.disable_compaction()

        # create ddoc and add views
        ddoc_name = "ddoc1"
        views = self.make_default_views("test_add_views", 3)
        server = self.servers[0]
        self.create_views(server, ddoc_name, views)

        # load initial documents
        gen_load = BlobGenerator('test_view_compaction',
                                 'test_view_compaction-',
                                 self.value_size,
                                 end=self.num_items)

        self._load_all_buckets(server, gen_load, "create", 0)

        # start fragmentation monitor
        fragmentation_monitor = \
            self.cluster.async_monitor_view_fragmentation(server,
                                                          ddoc_name,
                                                          self.fragmentation_value,
                                                          timeout = 20)

        # generate load until fragmentation reached
        while fragmentation_monitor.state != "FINISHED":

            # update docs to create fragmentation
            self._load_all_buckets(server, gen_load, "update", 0)
            for view in views:

                # run queries to create indexes
                self.cluster.query_view(server, ddoc_name, view.name, {})
        fragmentation_monitor.result()


        # compact ddoc and make sure fragmentation is less than high_mark
        # will throw exception if failed
        result = self.cluster.compact_view(server, ddoc_name)

        self.assertTrue(result)
