import time
from tasks.future import TimeoutError
from basetestcase import BaseTestCase
from couchbase.documentgenerator import DocumentGenerator
from couchbase.documentgenerator import BlobGenerator
from couchbase.document import DesignDocument, View
from remote.remote_util import RemoteMachineShellConnection

class CompactionViewTests(BaseTestCase):

    def setUp(self):
        super(CompactionViewTests, self).setUp()
        self.value_size = self.input.param("value_size", 256)
        self.fragmentation_value = self.input.param("fragmentation_value", 80)
        self.ddocs_num = self.input.param("ddocs_num", 1)
        self.view_per_ddoc = self.input.param("view_per_ddoc", 2)
        self.use_dev_views = self.input.param("use_dev_views", False)
        self.default_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view_name = "default_view"
        self.default_view = View(self.default_view_name, self.default_map_func, None)
        self.ddocs = []
        # disable auto compaction
        self.disable_compaction()

    def tearDown(self):
        super(CompactionViewTests, self).tearDown()


        """Trigger Compaction When specified Fragmentation is reached"""
    def test_multiply_compaction(self):

        cycles_num = self.input.param("cycles-num", 3)

        # create ddoc and add views
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()

        # load initial documents
        gen_load = BlobGenerator('test_view_compaction',
                                 'test_view_compaction-',
                                 self.value_size,
                                 end=self.num_items)

        self._load_all_buckets(self.master, gen_load, "create", 0)

        for i in xrange(cycles_num):

            for ddoc in self.ddocs:
                # start fragmentation monitor
                fragmentation_monitor = \
                    self.cluster.async_monitor_view_fragmentation(self.master,
                                                                  ddoc.name,
                                                                  self.fragmentation_value)

                # generate load until fragmentation reached
                while fragmentation_monitor.state != "FINISHED":
                    # update docs to create fragmentation
                    self._load_all_buckets(self.master, gen_load, "update", 0)
                    for view in ddoc.views:
                        # run queries to create indexes
                        self.cluster.query_view(self.master, ddoc.name, view.name, {})
                fragmentation_monitor.result()

            for ddoc in self.ddocs:
                result = self.cluster.compact_view(self.master, ddoc.name)
                self.assertTrue(result)

    def make_ddocs(self, ddocs_num, views_per_ddoc):
        ddoc_name = "compaction_ddoc"
        view_name = "compaction_view"
        for i in xrange(ddocs_num):
            views = self.make_default_views(view_name, views_per_ddoc, different_map=True)
            self.ddocs.append(DesignDocument(ddoc_name + str(i), views))

    def create_ddocs(self, ddocs=None, bucket=None):
        bucket_views = bucket or self.buckets[0]
        ddocs_to_create = ddocs or self.ddocs
        for ddoc in ddocs_to_create:
            if not ddoc.views:
                self.cluster.create_view(self.master, ddoc.name, [], bucket=bucket_views)
            for view in ddoc.views:
                self.cluster.create_view(self.master, ddoc.name, view, bucket=bucket_views)

    '''
    test changes ram quota during index.
    http://www.couchbase.com/issues/browse/CBQE-1649
    '''
    def test_compaction_with_cluster_ramquota_change(self):
        self.make_ddocs(self.ddocs_num, self.view_per_ddoc)
        self.create_ddocs()
        gen_load = BlobGenerator('test_view_compaction',
                                 'test_view_compaction-',
                                 self.value_size,
                                 end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)
        for ddoc in self.ddocs:
            fragmentation_monitor = \
                self.cluster.async_monitor_view_fragmentation(self.master,
                                                              ddoc.name,
                                                              self.fragmentation_value)
            while fragmentation_monitor.state != "FINISHED":
                self._load_all_buckets(self.master, gen_load, "update", 0)
                for view in ddoc.views:
                    self.cluster.query_view(self.master, ddoc.name, view.name, {})
            fragmentation_monitor.result()

        compaction_tasks = []
        for ddoc in self.ddocs:
            compaction_tasks.append(self.cluster.async_compact_view(self.master, ddoc.name))
        
        remote = RemoteMachineShellConnection(self.master)
        cli_command = "cluster-init"
        options = "--cluster-init-ramsize=%s" % (self.quota + 10)
        output, error = remote.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost",
                                                     user=self.master.rest_username, password=self.master.rest_password)
        self.assertTrue('\n'.join(output).find('SUCCESS') != -1, 'ram wasn\'t changed')
        self.log.info('Quota was changed')
        for task in compaction_tasks:
            task.result()
