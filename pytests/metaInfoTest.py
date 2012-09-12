from basetestcase import BaseTestCase
from memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached
from membase.api.rest_client import RestConnection
from mc_bin_client import MemcachedError
import crc32
import time

class MetaInfoTest(BaseTestCase):

    def setUp(self):
        super(MetaInfoTest, self).setUp()
        self.expiration = 60
        self.new_expiration = 100
        self.num_docs = self.input.param("num_test_docs", 100)
        self._load_doc_data_all_buckets()

        self.rest = RestConnection(self.master)
        self.awareness = VBucketAwareMemcached(self.rest, "default")
        self.mc = MemcachedClientHelper.direct_client(self.master, "default")
        self.num_vbuckets = len(self.rest.get_vbuckets("default"))
        self.docs = {}
        for i in xrange(self.num_docs):
            pref = str(i)
            self.docs["id" + pref] = "value"*100 + pref
        for doc in self.docs:
            self.awareness.set(doc, int(self.expiration), 0, self.docs[doc])

    def tearDown(self):
        super(MetaInfoTest, self).tearDown()

    def testExpiration(self):
        time.sleep(self.expiration)
        t = time.time() + self.expiration * 5

        while  t - time.time() > 0:
            if len(self.docs) == 0:
                self.log.info("all items are expired")
                break
            for doc in self.docs.keys():
                try:
                    self.getMetadatastats(self.mc, doc, self.num_vbuckets)
                except MemcachedError, e:
                    if "Not found for vbucket" in e.message:
                        self.docs.pop(doc)
            time.sleep(1)
        self.assertTrue(len(self.docs) == 0, "not all items were expired:" + str(self.docs))

    def testMetaAfterMutation(self):
        for doc in self.docs:
            metadatastats = self.getMetadatastats(self.mc, doc, self.num_vbuckets)
            self.awareness.set(doc, int(self.new_expiration), 0, self.docs[doc])
            metadatastats_after = self.getMetadatastats(self.mc, doc, self.num_vbuckets)
            self.assertTrue(int(metadatastats_after["key_exptime"]) - int(metadatastats["key_exptime"]) >= self.new_expiration - self.expiration)
            self.assertTrue(int(metadatastats_after["key_cas"]) > int(metadatastats["key_cas"]), metadatastats_after["key_cas"] + " !> " + metadatastats["key_cas"])



    def testGat(self):
        for doc in self.docs:
            metadatastats = self.getMetadatastats(self.mc, doc, self.num_vbuckets)
            self.mc.gat(doc, self.new_expiration)
            metadatastats_after = self.getMetadatastats(self.mc, doc, self.num_vbuckets)
            self.assertTrue(int(metadatastats_after["key_exptime"]) - int(metadatastats["key_exptime"]) >= self.new_expiration - self.expiration)
            self.assertTrue(int(metadatastats_after["key_cas"]) > int(metadatastats["key_cas"]), metadatastats_after["key_cas"] + " !> " + metadatastats["key_cas"])


    def getMetadatastats(self, client, key, num_vbuckets):
        vid = crc32.crc32_hash(key) & (num_vbuckets - 1)
        i = 0
        while True:
            try:
                metadatastats = client.stats("vkey {0} {1}".format(key, vid))
                break
            except MemcachedError, e:
                if i < 5:
                    i += 1
                else:
                    raise e
        return metadatastats

