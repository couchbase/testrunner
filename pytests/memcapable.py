import unittest
from TestInput import TestInputSingleton
import mc_bin_client
import time
import uuid
import logger
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper

class MemcapableTestBase(object):
    log = None
    keys = None
    servers = None
    input = None
    test = None
    bucket_port = None
    bucket_name = None

    def setUp_bucket(self, bucket_name, port, bucket_type, unittest):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        unittest.assertTrue(self.input, msg="input parameters missing...")
        self.test = unittest
        self.servers = self.input.servers
        self.bucket_port = port
        self.bucket_name = bucket_name
        ClusterOperationHelper.cleanup_cluster(self.servers)
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self.test)

        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            info = rest.get_nodes_self()
            rest.init_cluster(username=serverInfo.rest_username,
                              password=serverInfo.rest_password)
            rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
            bucket_ram = info.mcdMemoryReserved * 2 / 3
            if bucket_name != 'default' and self.bucket_port == 11211:
                rest.create_bucket(bucket=bucket_name,
                                   bucketType=bucket_type,
                                   ramQuotaMB=bucket_ram,
                                   proxyPort=self.bucket_port,
                                   authType='sasl',
                                   saslPassword='password')
                msg = 'create_bucket succeeded but bucket "default" does not exist'
                self.test.assertTrue(BucketOperationHelper.wait_for_bucket_creation(bucket_name, rest), msg=msg)
                ready = BucketOperationHelper.wait_for_memcached(serverInfo, self.bucket_name)
                unittest.assertTrue(ready, "wait_for_memcached failed")

            else:
                rest.create_bucket(bucket=bucket_name,
                                   bucketType=bucket_type,
                                   ramQuotaMB=bucket_ram,
                                   proxyPort=self.bucket_port)
                msg = 'create_bucket succeeded but bucket "default" does not exist'
                self.test.assertTrue(BucketOperationHelper.wait_for_bucket_creation(bucket_name, rest), msg=msg)
                ready = BucketOperationHelper.wait_for_memcached(serverInfo, self.bucket_name)
                unittest.assertTrue(ready, "wait_for_memcached failed")

    def set_test(self, key, exp, flags, values):
        for serverInfo in self.servers:
            client = MemcachedClientHelper.proxy_client(serverInfo,self.bucket_name,)
            #            self.log.info('Waitting 15 seconds for memcached started')
            #            time.sleep(15)
            for v in values:
                for f in flags:
                    client.set(key, exp, f, v)
                    flags_v, cas_v, get_v = client.get(key)
                    if get_v == v:
                        if flags_v == f:
                            self.log.info('Flags is set to {0}; and when run get {1}'.format(f, flags_v))
                        else:
                            self.test.fail('FAILED.  Flags is set to {0};  and when run get {1}'.format(f, flags_v))
                        self.log.info('Value is set {0};  and when run get {1}'.format(v, get_v))
                    else:
                        self.test.fail('FAILED.  Value is set to {0};  and when run get {1}'.format(v, get_v))

                        #    def tearDown_bucket(self):
                        #        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self.test)

    # Instead of checking the value before incrementing, 
    # you can simply ADD it instead before incrementing each time.
    # If it's already there, your ADD is ignored, and if it's not there, it's set.
    def incr_test(self, key, exp, flags, value, incr_amt, decr_amt, incr_time):
        global update_value
        for serverInfo in self.servers:
            client = MemcachedClientHelper.proxy_client(serverInfo,self.bucket_name)
            #            self.log.info('Waitting 15 seconds for memcached started')
            #            time.sleep(15)
            if key != 'no_key':
                client.set(key, exp, flags, value)
            if exp:
                self.log.info('Wait {0} seconds for the key expired' .format(exp + 2))
                time.sleep(exp + 2)
            if decr_amt:
                c, d = client.decr(key, decr_amt)
                self.log.info('decr amt {0}' .format(c))
            try:
                i = 0
                while i < incr_time:
                    update_value, cas = client.incr(key, incr_amt)
                    i += 1
                self.log.info('incr {0} times with value {1}'.format(incr_time, incr_amt))
                return update_value
            except mc_bin_client.MemcachedError as error:
                self.log.info('memcachedError : {0}'.format(error.status))
                self.test.fail("unable to increment value: {0}".format(incr_amt))


    def decr_test(self, key, exp, flags, value, incr_amt, decr_amt, decr_time):
        global update_value
        for serverInfo in self.servers:
            client = MemcachedClientHelper.proxy_client(serverInfo,self.bucket_name)
            if key != 'no_key':
                client.set(key, exp, flags, value)
            if exp:
                self.log.info('Wait {0} seconds for the key expired' .format(exp + 2))
                time.sleep(exp + 2)
            if incr_amt:
                c, d = client.incr(key, incr_amt)
                self.log.info('incr amt {0}' .format(c))
            i = 0
            while i < decr_time:
                update_value, cas = client.decr(key, decr_amt)
                i += 1
            self.log.info('decr {0} times with value {1}'.format(decr_time, decr_amt))
        return update_value


class SimpleSetMembaseBucketDefaultPort(unittest.TestCase):
    memcapableTestBase = None
    log = logger.Logger.get_logger()

    def setUp(self):
        self.memcapableTestBase = MemcapableTestBase()
        self.memcapableTestBase.setUp_bucket('default', 11211, 'membase', self)

    def test_set_pos_int_value_pos_flag_key_never_expired(self):
        key_test = 'has_key'
        valuesList = ['0', '000', '4', '678', '6560987', '32456754', '0000000000', '00001000']
        exp_time = 0
        flagsList = [0, 0000, 00001, 34532, 453456, 0001000, 1100111100, 4294967295]
        self.memcapableTestBase.set_test(key_test, exp_time, flagsList, valuesList)

    def test_set_neg_int_value_pos_flag_key_never_expired(self):
        key_test = 'has_key'
        valuesList = ['-0', '-000', '-4', '-678', '-6560987', '-32456754', '-0000000000', '-00001000']
        exp_time = 0
        flagsList = [0, 0000, 00001, 34532, 453456, 0001000, 1100111100, 4294967295]
        self.memcapableTestBase.set_test(key_test, exp_time, flagsList, valuesList)

    def test_set_pos_float_value_pos_flag_key_never_expired(self):
        key_test = 'has_key'
        valuesList = ['0.00', '000.0', '4.6545', '678.87967', '6560987.0', '32456754.090987', '0000000000.0000001',
                      '00001000.008']
        exp_time = 0
        flagsList = [0, 0000, 00001, 34532, 453456, 0001000, 1100111100, 4294967295]
        self.memcapableTestBase.set_test(key_test, exp_time, flagsList, valuesList)

    def test_set_neg_float_value_pos_flag_key_never_expired(self):
        key_test = 'has_key'
        valuesList = ['-0.00', '-000.0', '-4.6545', '-678.87967', '-6560987.0', '-32456754.090987',
                      '-0000000000.0000001', '-00001000.008']
        exp_time = 0
        flagsList = [0, 0000, 00001, 34532, 453456, 0001000, 1100111100, 4294967295]
        self.memcapableTestBase.set_test(key_test, exp_time, flagsList, valuesList)


class SimpleIncrMembaseBucketDefaultPort(unittest.TestCase):
    memcapableTestBase = None
    log = logger.Logger.get_logger()

    def setUp(self):
        self.memcapableTestBase = MemcapableTestBase()
        self.memcapableTestBase.setUp_bucket('default', 11211, 'membase', self)

    def test_incr_an_exist_key_never_exp(self):
        key_test = 'has_key'
        value = '10'
        decr_amt = 0
        incr_amt = 5
        incr_time = 10
        update_v = self.memcapableTestBase.incr_test(key_test, 0, 0, value, incr_amt, decr_amt, incr_time)
        if update_v == (int(value) + incr_amt * incr_time):
            self.log.info('Value update correctly.  Expected value {0}.  Tested value {1}'\
            .format((int(value) + incr_amt * incr_time), update_v))
        else:
            self.fail("FAILED test_incr_an_exist_key_never_exp. Original value %s. \
                            Expected value %d" % (value, int(value) + incr_amt * incr_time))

    def test_incr_non_exist_key(self):
        key_test = 'no_key'
        value = '10'
        decr_amt = 0
        incr_amt = 5
        incr_time = 10
        update_v = self.memcapableTestBase.incr_test(key_test, 0, 0, value, incr_amt, decr_amt, incr_time)
        if update_v == incr_amt * (incr_time - 1):
            self.log.info('Value update correctly.  Expected value {0}.  Tested value {1}'\
            .format(incr_amt * (incr_time - 1), update_v))
        else:
            self.fail("FAILED test_incr_non_exist_key")

    def test_incr_with_exist_key_and_expired(self):
        key_test = 'expire_key'
        value = '10'
        exp_time = 5
        decr_amt = 0
        incr_amt = 5
        incr_time = 10
        update_v = self.memcapableTestBase.incr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, incr_time)
        if update_v == incr_amt * (incr_time - 1):
            self.log.info('Value update correctly.  Expected value {0}.  Tested value {1}'\
            .format(incr_amt * (incr_time - 1), update_v))
        else:
            self.fail("FAILED test_incr_with_exist_key_and_expired")

    def test_incr_with_exist_key_decr_then_incr_never_expired(self):
        key_test = 'has_key'
        value = '101'
        exp_time = 0
        decr_amt = 10
        incr_amt = 5
        incr_time = 10
        update_v = self.memcapableTestBase.incr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, incr_time)
        if update_v == (int(value) - decr_amt + incr_amt * incr_time):
            self.log.info('Value update correctly.  Expected value {0}.  Tested value {1}'\
            .format(int(value) - decr_amt + incr_amt * incr_time, update_v))
        else:
            self.fail("FAILED test_incr_with_exist_key_and_expired")

## this test will fail as expected
#    def test_incr_with_non_int_key(self):
#        key_test = 'has_key'
#        value = 'abcd'
#        exp_time = 0
#        decr_amt = 0
#        incr_amt = 5
#        incr_time = 10
#        self.assertRaises(self.memcapableTestBase.incr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, incr_time))
##        self.assertRaises('Expected FAILED.  Can not incr with string value')

class GetlTests(unittest.TestCase):
    memcapableTestBase = None
    log = logger.Logger.get_logger()

    def setUp(self):
        self.memcapableTestBase = MemcapableTestBase()
        self.memcapableTestBase.setUp_bucket('default', 11211, 'membase', self)

    #set an item for 5 seconds
    #getl for 15 seconds and verify that setting the item again
    #throes Data exists
    def setUp(self):
        self.memcapableTestBase = MemcapableTestBase()
        self.memcapableTestBase.setUp_bucket('default', 11211, 'membase', self)


    def _getl_body(self, prefix, getl_timeout, expiration):
        node = self.memcapableTestBase.servers[0]
        mc = MemcachedClientHelper.direct_client(node,"default")
        key = "{0}_{1}".format(prefix, str(uuid.uuid4()))
        self.log.info("setting key {0} with expiration {1}".format(key, expiration))
        mc.set(key, expiration, 0, key)
        self.log.info("getl key {0} timeout {1}".format(key, getl_timeout))
        mc.getl(key, getl_timeout)
        self.log.info("get key {0} which is locked now".format(key))
        flags_v, cas_v, get_v = mc.get(key)
        self.assertEquals(get_v, key)
        i = 0
        while i < 40:
            self.log.info("setting key {0} with new value {1}".format(key, '*'))
            try:
                mc.set(key, 0, 0, '*')
                break
            except Exception as ex:
                print ex
            time.sleep(1)
            print i
            i += 1
        if getl_timeout > 30:
            self.log.info("sleep for {0} seconds".format(30))
            time.sleep(30)
        elif getl_timeout > 0:
            self.log.info("sleep for {0} seconds".format(15 - getl_timeout))
            self.log.info("sleep for {0} seconds".format(15))
            time.sleep(getl_timeout)
        else:
            self.log.info("sleep for {0} seconds".format(15))
            time.sleep(15)
        self.log.info("lock should have timed out by now . try to set the item again")
        new_value = "*"
        self.log.info("setting key {0} with new value {1}".format(key, new_value))
        mc.set(key, 0, 0, new_value)
        self.log.info("get key {0}".format(key))
        flags_v, cas_v, get_v = mc.get(key)
        self.assertEquals(get_v, "*")

    def getl_minus_one(self):
        self._getl_body("getl_-1", -1, 0)

    def getl_zero(self):
        self._getl_body("getl_0", 0, 0)

    def getl_five(self):
        self._getl_body("getl_5", 15, 0)

    def getl_ten(self):
        self._getl_body("getl_10", 10, 0)

    def getl_fifteen(self):
        self._getl_body("getl_15", 15, 0)

    def getl_thirty(self):
        self._getl_body("getl_30", 30, 0)

    def getl_sixty(self):
        self._getl_body("getl_60", 60, 0)

    def getl_expired_item(self):
        prefix = "getl_expired_item"
        expiration = 5
        getl_timeout = 15
        node = self.memcapableTestBase.servers[0]
        mc = MemcachedClientHelper.direct_client(node,"default")
        key = "{0}_{1}".format(prefix, str(uuid.uuid4()))
        self.log.info("setting key {0} with expiration {1}".format(key, expiration))
        mc.set(key, expiration, 0, key)
        self.log.info("getl key {0} timeout {1}".format(key, getl_timeout))
        mc.getl(key, getl_timeout)
        self.log.info("get key {0} which is locked now".format(key))
        flags_v, cas_v, get_v = mc.get(key)
        self.assertEquals(get_v, key)
        if getl_timeout > 30:
            self.log.info("sleep for {0} seconds".format(30))
            time.sleep(30)
        elif getl_timeout > 0:
            self.log.info("sleep for {0} seconds".format(getl_timeout))
            time.sleep(getl_timeout)
        else:
            self.log.info("sleep for {0} seconds".format(15))
            time.sleep(15)
        self.log.info("get key {0} which was locked when it expired. should fail".format(key))
        try:
            mc.get(key)
            self.fail("get {0} should have raised not_found error".format(key))
        except mc_bin_client.MemcachedError as error:
            self.log.info("raised exception as expected : {0}".format(error))
        self.log.info("item expired and lock should have timed out by now . try to set the item again")
        new_value = "*"
        self.log.info("setting key {0} with new value {1}".format(key, new_value))
        mc.set(key, 0, 0, new_value)
        self.log.info("get key {0}".format(key))
        flags_v, cas_v, get_v = mc.get(key)
        self.assertEquals(get_v, "*")


class SimpleDecrMembaseBucketDefaultPort(unittest.TestCase):
    memcapableTestBase = None
    log = logger.Logger.get_logger()

    def setUp(self):
        self.memcapableTestBase = MemcapableTestBase()
        self.memcapableTestBase.setUp_bucket('default', 11211, 'membase', self)

    def test_decr_an_exist_key_never_exp(self):
        key_test = 'has_key'
        value = '100'
        exp_time = 0
        decr_amt = 5
        incr_amt = 0
        decr_time = 10
        update_v = self.memcapableTestBase.decr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, decr_time)
        if update_v == (int(value) - decr_amt * decr_time):
            self.log.info('Value update correctly.  Expected value {0}.  Tested value {1}'\
            .format((int(value) - decr_amt * decr_time), update_v))
        else:
            self.fail("FAILED test_decr_an_exist_key_never_exp. Original value %s. \
                            Expected value %d" % (value, int(value) - decr_amt * decr_time))

    def test_decr_non_exist_key(self):
        key_test = 'no_key'
        value = '100'
        exp_time = 0
        decr_amt = 5
        incr_amt = 0
        decr_time = 10
        update_v = self.memcapableTestBase.decr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, decr_time)
        if not update_v:
            self.log.info('Value update correctly.  Expected value 0.  Tested value {0}'\
            .format(update_v))
        else:
            self.fail('FAILED test_decr_non_exist_key. Expected value 0')

    def test_decr_with_exist_key_and_expired(self):
        key_test = 'has_key'
        value = '100'
        exp_time = 5
        decr_amt = 5
        incr_amt = 0
        decr_time = 10
        update_v = self.memcapableTestBase.decr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, decr_time)
        if not update_v:
            self.log.info('Value update correctly.  Expected value 0.  Tested value {0}'\
            .format(update_v))
        else:
            self.fail('FAILED test_decr_with_exist_key_and_expired.  Expected value 0')

    def test_decr_with_exist_key_incr_then_decr_never_expired(self):
        key_test = 'has_key'
        value = '100'
        exp_time = 0
        decr_amt = 5
        incr_amt = 50
        decr_time = 10
        update_v = self.memcapableTestBase.decr_test(key_test, exp_time, 0, value, incr_amt, decr_amt, decr_time)
        if update_v == (int(value) + incr_amt - decr_amt * decr_time):
            self.log.info('Value update correctly.  Expected value {0}.  Tested value {1}'\
            .format(int(value) + incr_amt - decr_amt * decr_time, update_v))
        else:
            self.fail(
                "Expected value %d.  Test result %d" % (int(value) + incr_amt - decr_amt * decr_time, update_v))