from basetestcase import BaseTestCase
from memcached.helper.data_helper import MemcachedClientHelper


class CasBaseTest(BaseTestCase):
    def setUp(self):
        super(CasBaseTest, self).setUp()
        self.value_size = self.input.param("value_size", 256)
        self.doc_ops = self.input.param("doc_ops", None)
        self.mutate_times = self.input.param("mutate_times", 10)
        self.expire_time = self.input.param("expire_time", 5)
        self.item_flag = self.input.param("item_flag", 0)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.clients = {}
        for bucket in self.buckets:
            client = MemcachedClientHelper.direct_client(self.master, bucket.name)
            self.clients[bucket.name]=client

    def tearDown(self):
        super(CasBaseTest, self).tearDown()
        for bucket in self.buckets:
            self.clients[bucket.name].close()
