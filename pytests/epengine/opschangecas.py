import time
import logger
from memcacheConstants import ERR_NOT_FOUND
from castest.cas_base import CasBaseTest
from epengine.bucket_config import BucketConfig
from couchbase_helper.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError

from membase.api.rest_client import RestConnection, RestHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
import json


from remote.remote_util import RemoteMachineShellConnection

class OpsChangeCasTests(BucketConfig):

    def setUp(self):
        super(OpsChangeCasTests, self).setUp()
        self.expire_time = self.input.param("expire_time", 5)
        self.item_flag = self.input.param("item_flag", 0)

    def tearDown(self):
        super(OpsChangeCasTests, self).tearDown()

    def test_cas_set(self):
        KEY_NAME = 'key1'

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, 'default')

        for i in range(10):
            # set a key
            value = 'value' + str(i)
            client.memcached(KEY_NAME).set(KEY_NAME, 0, 0,json.dumps({'value':value}))
            mc_active = client.memcached(KEY_NAME)
            cas = mc_active.getMeta(KEY_NAME)[4]

        max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(KEY_NAME)) + ':max_cas'] )

        self.assertTrue(cas == max_cas, '[ERROR]Max cas  is not 0 it is {0}'.format(cas))

    def test_cas_delete(self):
        KEY_NAME = 'key2'

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, 'default')

        for i in range(10):
            # set a key
            value = 'value' + str(i)
            client.memcached(KEY_NAME).set(KEY_NAME, 0, 0,json.dumps({'value':value}))
            mc_active = client.memcached(KEY_NAME)

            cas = mc_active.getMeta(KEY_NAME)[4]

        rc  = client.memcached(KEY_NAME).delete(KEY_NAME)
        mc_active = client.memcached(KEY_NAME)

        cas = mc_active.getMeta(KEY_NAME)[4]

        max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(KEY_NAME)) + ':max_cas'] )

        self.assertTrue(cas == max_cas, '[ERROR]Max cas  is not 0 it is {0}'.format(max_cas))

    def test_cas_expiry(self):
        KEY_NAME = 'key2'

        rest = RestConnection(self.master)
        client = VBucketAwareMemcached(rest, 'default')

        for i in range(10):
            # set a key
            value = 'value' + str(i)
            client.memcached(KEY_NAME).set(KEY_NAME, 0, 0,json.dumps({'value':value}))
            mc_active = client.memcached(KEY_NAME)

            cas = mc_active.getMeta(KEY_NAME)[4]

        rc = client.memcached(KEY_NAME).set(KEY_NAME, self.expire_time, 0, 'new_value')
        mc_active = client.memcached(KEY_NAME)

        cas = mc_active.getMeta(KEY_NAME)[4]

        max_cas = int( mc_active.stats('vbucket-details')['vb_' + str(client._get_vBucket_id(KEY_NAME)) + ':max_cas'] )

        self.assertTrue(cas == max_cas, 'max cas  is not 0 it is {0}'.format(max_cas))

        time.sleep(self.expire_time+1)
        self.log.info("Try to mutate an expired item with its previous cas {0}".format(cas))
        try:
            client.memcached(KEY_NAME).cas(KEY_NAME, 0, self.item_flag, cas, 'new')
            raise Exception("The item should already be expired. We can't mutate it anymore")
        except MemcachedError as error:
        #It is expected to raise MemcachedError becasue the key is expired.
            if error.status == ERR_NOT_FOUND:
                self.log.info("<MemcachedError #%d ``%s''>" % (error.status, error.msg))
                pass
            else:
                raise Exception(error)

