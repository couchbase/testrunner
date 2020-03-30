import copy, json, os, random
import string, re, time, sys

from ep_mc_bin_client import MemcachedClient, MemcachedError
from TestInput import TestInputSingleton
from clitest.cli_base import CliBaseTest
from membase.api.rest_client import RestConnection



class CouchbaseCliTestWithMeta(CliBaseTest):

    def setUp(self):
        TestInputSingleton.input.test_params["default_bucket"] = False
        super(CouchbaseCliTestWithMeta, self).setUp()


    def tearDown(self):
        super(CouchbaseCliTestWithMeta, self).tearDown()

    def test_epengine_save_meta_to_48bits_in_ram(self):
        """
            As in MB-29119, ep-engine must save rev-seqno in 48 bits
            This fix went into version 5.1+
            params: eviction_policy=fullEviction,sasl_buckets=1
        """
        if 5.1 > float(self.cb_version[:3]):
            self.log.info("This test only work for version 5.1+")
            return
        if len(self.buckets) >= 2:
            self.fail("This test only runs in one bucket")

        rest = RestConnection(self.master)
        success_set_exist_item = False
        mc = MemcachedClient(self.master.ip, 11210)
        mc.sasl_auth_plain('Administrator', 'password')
        mc.bucket_select('bucket0')
        mc.vbucketId = 903

        self.log.info("Start to test")
        mc.setWithMeta('test_with_meta', 'value', 0, 0, 0x1234000000000001, 1)
        self.sleep(10)
        item_count = rest.get_active_key_count("bucket0")

        if int(item_count) != 1:
            self.fail("Fail to set 1 key to bucket0")

        mc.evict_key('test_with_meta')
        try:
            mc.setWithMeta('test_with_meta', 'value', 0, 0, 0x1234000000000001, 1)
            success_set_exist_item = True
        except MemcachedError as e:
            print("\nMemcached exception: ", e)
            if "#2" not in str(e):
                self.fail("ep engine failed to check existed key")
                """error #2 is ErrorKeyEexists"""
        if success_set_exist_item:
            self.fail("ep engine could set an existed key")
