import time
import logger

from basetestcase import BaseTestCase
from memcacheConstants import ERR_NOT_FOUND
from castest.cas_base import CasBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError

from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from ep_mc_bin_client import MemcachedClient, MemcachedError
import json
from lib.couchbase_helper.tuq_generators import JsonGenerator

from remote.remote_util import RemoteMachineShellConnection


"""

Capture basic get, set operations, also the meta operations. This is based on some 4.1.1 test which had separate
bugs with incr and delete with meta and I didn't see an obvious home for them.

This is small now but we will reactively add things

These may be parameterized by:
   - full and value eviction
   - DGM and non-DGM



"""



class basic_ops(BaseTestCase):

    def setUp(self):
        super(basic_ops, self).setUp()
        self.src_bucket = RestConnection(self.master).get_buckets()

    def tearDown(self):
        super(basic_ops, self).tearDown()

    def do_basic_ops(self):

        KEY_NAME = 'key1'
        KEY_NAME2 = 'key2'
        CAS = 1234
        self.log.info('Starting basic ops')


        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, 'default')
        mcd = client.memcached(KEY_NAME)

        # MB-17231 - incr with full eviction
        rc = mcd.incr(KEY_NAME, 1)
        print 'rc for incr', rc



        # MB-17289 del with meta


        rc = mcd.set(KEY_NAME, 0,0, json.dumps({'value':'value2'}))
        print 'set is', rc
        cas = rc[1]


        # wait for it to persist
        persisted = 0
        while persisted == 0:
                opaque, rep_time, persist_time, persisted, cas = client.observe(KEY_NAME)


        try:
            rc = mcd.evict_key(KEY_NAME)

        except MemcachedError as exp:
            self.fail("Exception with evict meta - {0}".format(exp) )

        CAS = 0xabcd
        # key, value, exp, flags, seqno, remote_cas

        try:
            #key, exp, flags, seqno, cas
            rc = mcd.del_with_meta(KEY_NAME2, 0, 0, 2, CAS)



        except MemcachedError as exp:
            self.fail("Exception with del_with meta - {0}".format(exp) )

    # Reproduce test case for MB-28078
    def do_setWithMeta_twice(self):

        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('default')

        try:
            mc.setWithMeta('1', '{"Hello":"World"}', 3600, 0, 1, 0x1512a3186faa0000)
        except MemcachedError as error:
            self.log.info("<MemcachedError #%d ``%s''>" % (error.status, error.message))
            self.fail("Error on First setWithMeta()")

        stats = mc.stats()
        self.log.info('curr_items: {} and curr_temp_items:{}'.format(stats['curr_items'], stats['curr_temp_items']))
        self.log.info("Sleeping for 5 and checking stats again")
        time.sleep(5)
        stats = mc.stats()
        self.log.info('curr_items: {} and curr_temp_items:{}'.format(stats['curr_items'], stats['curr_temp_items']))

        try:
            mc.setWithMeta('1', '{"Hello":"World"}', 3600, 0, 1, 0x1512a3186faa0000)
        except MemcachedError as error:
            stats = mc.stats()
            self.log.info('After 2nd setWithMeta(), curr_items: {} and curr_temp_items:{}'.format(stats['curr_items'], stats['curr_temp_items']))
            if int(stats['curr_temp_items']) == 1:
                self.fail("Error on second setWithMeta(), expected curr_temp_items to be 0")
            else:
                self.log.info("<MemcachedError #%d ``%s''>" % (error.status, error.message))

    def generate_docs_bigdata(self, docs_per_day, start=0, document_size=1024000):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(end=docs_per_day, start=start, value_size=document_size)

    def test_large_doc_size_1MB(self):
        # bucket size =256MB, when Bucket gets filled 236MB then the test starts failing
        # document size =2MB, No of docs = 221 , load 250 docs
        # epengine.basic_ops.basic_ops.test_large_doc_size_1MB,skip_cleanup=True,document_size=1024000,dgm_run=True
        docs_per_day = 10
        document_size = self.input.param('document_size')
        # generate docs with size >=  1MB , See MB-29333
        gens_load = self.generate_docs_bigdata(docs_per_day=(25 * docs_per_day), document_size=document_size)
        self.load(gens_load, buckets=self.src_bucket, verify_data=False, batch_size=10)

        # check if all the documents(250) are loaded else the test has failed with "Memcached Error 134"
        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('default')
        stats = mc.stats()
        self.assertEquals(int(stats['curr_items']), 250)


    def test_large_doc_size_2MB(self):
        # bucket size =256MB, when Bucket gets filled 236MB then the test starts failing
        # document size =2MB, No of docs = 108 , load 150 docs
        # epengine.basic_ops.basic_ops.test_large_doc_size_2MB,skip_cleanup = True,document_size=2048000,dgm_run=True
        docs_per_day = 5
        document_size = self.input.param('document_size')
        # generate docs with size >=  1MB , See MB-29333
        gens_load = self.generate_docs_bigdata(docs_per_day=(25 *docs_per_day), document_size=document_size)
        self.load(gens_load, buckets=self.src_bucket, verify_data=False, batch_size=10)

        # check if all the documents(125) are loaded else the test has failed with "Memcached Error 134"
        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('default')
        stats = mc.stats()
        self.assertEquals(int(stats['curr_items']), 125)

    def test_large_doc_20MB(self):
        # test reproducer for MB-29258,
        # Load a doc which is greater than 20 MB with compression enabled and check if it fails
        # check with compression_mode as active, passive and off
        document_size= self.input.param('document_size', 20)
        gens_load = self.generate_docs_bigdata(docs_per_day=1, document_size=(document_size * 1024000))
        self.load(gens_load, buckets=self.src_bucket, verify_data=False, batch_size=10)

        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
        mc.bucket_select('default')
        stats = mc.stats()
        if (document_size > 20):
            self.assertEquals(int(stats['curr_items']), 0) # failed with error "Data Too Big" when document size > 20MB
        else:
            self.assertEquals(int(stats['curr_items']), 1)
            gens_update = self.generate_docs_bigdata(docs_per_day=1, document_size=(21 * 1024000))
            self.load(gens_update, buckets=self.src_bucket, verify_data=False, batch_size=10)
            stats = mc.stats()
            self.assertEquals(int(stats['curr_items']), 1)