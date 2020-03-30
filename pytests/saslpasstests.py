import logger

from basetestcase import BaseTestCase
from remote.remote_util import RemoteUtilHelper
from couchbase_helper.document import View

class SaslPassTests(BaseTestCase):

    def setUp(self):
        super(SaslPassTests, self).setUp()

    def tearDown(self):
        super(SaslPassTests, self).tearDown()

    def pass_encrypted_in_logs_test(self):
        self.bucket_size = self._get_bucket_size(self.quota, 1)
        self._create_sasl_buckets(self.master, 1, password='mysuperpass')
        bucket = self.buckets[-1]

        if self.input.param("load", 0):
            self.num_items = self.input.param("load", 0)
            self._load_doc_data_all_buckets()
        if self.input.param("views", 0):
            views = []
            for i in range(self.input.param("views", 0)):
                views.append(View("view_sasl" + str(i),
                                  'function (doc, meta) {'
                                  'emit(meta.id, "emitted_value%s");}' % str(i),
                                  None, False))
            self.create_views(self.master, "ddoc", views, bucket)
        if self.input.param("rebalance", 0):
            self.cluster.rebalance(self.servers[:self.nodes_init],
                                   self.servers[self.nodes_init:self.nodes_init + self.input.param("rebalance", 0)],
                                   [])

        for server in self.servers[:self.nodes_init]:
            for log_file in ['debug', 'info', 'views', 'xdcr']:
                self.assertFalse(RemoteUtilHelper.is_text_present_in_logs(server, bucket.saslPassword, logs_to_check=log_file),
                                 "%s logs contains password in plain text" % log_file)
