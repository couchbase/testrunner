import time
import logger

from basetestcase import BaseTestCase
from memcacheConstants import ERR_NOT_FOUND
from castest.cas_base import CasBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError

from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
import json


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

