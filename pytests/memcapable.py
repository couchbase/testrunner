import os
import random
import unittest
from TestInput import TestInputSingleton
import mc_bin_client
import time
import uuid
import logger
import logging
import datetime
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.rebalance_helper import RebalanceHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper, VBucketAwareMemcached
from remote.remote_util import RemoteMachineShellConnection, RemoteMachineHelper
from basetestcase import BaseTestCase
from lib.membase.api.exception import BucketCreationException
from security.rbac_base import RbacBase


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
        self.master = self.input.servers[0]
        self.bucket_port = port
        self.bucket_name = bucket_name
        self.bucket_storage = self.input.param("bucket_storage", 'magma')
        ClusterOperationHelper.cleanup_cluster([self.master])
        BucketOperationHelper.delete_all_buckets_or_assert([self.master], self.test)

        # Add built-in user
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', self.master)

        # Assign user to role
        role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(self.master), 'builtin')

        self._create_default_bucket(unittest)

    def _create_default_bucket(self, unittest):
        name = "default"
        master = self.master
        rest = RestConnection(master)
        helper = RestHelper(RestConnection(master))
        num_vbs = 128 if self.bucket_storage == "magma" else None
        if not helper.bucket_exists(name):
            # MB-65261: If Magma+numVb=None, create bucket should fail
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(TestInputSingleton.input.servers)
            info = rest.get_nodes_self()
            available_ram = info.memoryQuota * node_ram_ratio
            rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram),
                               storageBackend=self.bucket_storage,
                               numVBuckets=num_vbs)
            BucketOperationHelper.wait_for_vbuckets_ready_state(master, name)
        unittest.assertTrue(helper.bucket_exists(name),
                            msg="unable to create {0} bucket".format(name))


    def set_test(self, key, exp, flags, values):
        serverInfo = self.master
        client = MemcachedClientHelper.proxy_client(serverInfo, self.bucket_name,)
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

    # Instead of checking the value before incrementing,
    # you can simply ADD it instead before incrementing each time.
    # If it's already there, your ADD is ignored, and if it's not there, it's set.
    def incr_test(self, key, exp, flags, value, incr_amt, decr_amt, incr_time):
        global update_value

        serverInfo = self.master
        client = MemcachedClientHelper.proxy_client(serverInfo, self.bucket_name)
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
        serverInfo = self.master
        client = MemcachedClientHelper.proxy_client(serverInfo, self.bucket_name)
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

    def tearDown(self):
        ClusterOperationHelper.cleanup_cluster([self.master])
        BucketOperationHelper.delete_all_buckets_or_assert([self.master], self)


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
        flagsList = [0, 0000, 0o0001, 34532, 453456, 0o001000, 1100111100, 4294967295]
        self.memcapableTestBase.set_test(key_test, exp_time, flagsList, valuesList)

    def test_set_neg_int_value_pos_flag_key_never_expired(self):
        key_test = 'has_key'
        valuesList = ['-0', '-000', '-4', '-678', '-6560987', '-32456754', '-0000000000', '-00001000']
        exp_time = 0
        flagsList = [0, 0000, 0o0001, 34532, 453456, 0o001000, 1100111100, 4294967295]
        self.memcapableTestBase.set_test(key_test, exp_time, flagsList, valuesList)

    def test_set_pos_float_value_pos_flag_key_never_expired(self):
        key_test = 'has_key'
        valuesList = ['0.00', '000.0', '4.6545', '678.87967', '6560987.0', '32456754.090987', '0000000000.0000001',
                      '00001000.008']
        exp_time = 0
        flagsList = [0, 0000, 0o0001, 34532, 453456, 0o001000, 1100111100, 4294967295]
        self.memcapableTestBase.set_test(key_test, exp_time, flagsList, valuesList)

    def test_set_neg_float_value_pos_flag_key_never_expired(self):
        key_test = 'has_key'
        valuesList = ['-0.00', '-000.0', '-4.6545', '-678.87967', '-6560987.0', '-32456754.090987',
                      '-0000000000.0000001', '-00001000.008']
        exp_time = 0
        flagsList = [0, 0000, 0o0001, 34532, 453456, 0o001000, 1100111100, 4294967295]
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
    def _getl_body(self, prefix, getl_timeout, expiration):
        node = self.memcapableTestBase.master
        mc = MemcachedClientHelper.direct_client(node, "default")
        key = "{0}_{1}".format(prefix, str(uuid.uuid4()))
        self.log.info("setting key {0} with expiration {1}".format(key, expiration))
        mc.set(key, expiration, 0, key)
        self.log.info("getl key {0} timeout {1}".format(key, getl_timeout))
        try:
            mc.getl(key, getl_timeout)
        except Exception as ex:
            if getl_timeout < 0:
                print(ex)
            else:
                raise
        self.log.info("get key {0} which is locked now".format(key))
        flags_v, cas_v, get_v = mc.get(key)
        self.assertEqual(get_v.decode(), key)
        i = 0
        while i < 40:
            self.log.info("setting key {0} with new value {1}".format(key, '*'))
            try:
                mc.set(key, 0, 0, '*')
                break
            except Exception as ex:
                print(ex)
            time.sleep(1)
            print(i)
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
        self.assertEqual(get_v.decode(), "*")

    def test_getl_minus_one(self):
        self._getl_body("getl_-1", -1, 0)

    def test_getl_zero(self):
        self._getl_body("getl_0", 0, 0)

    def test_getl_five(self):
        self._getl_body("getl_5", 15, 0)

    def test_getl_ten(self):
        self._getl_body("getl_10", 10, 0)

    def test_getl_fifteen(self):
        self._getl_body("getl_15", 15, 0)

    def test_getl_thirty(self):
        self._getl_body("getl_30", 30, 0)

    def test_getl_sixty(self):
        self._getl_body("getl_60", 60, 0)

    def test_getl_expired_item(self):
        prefix = "getl_expired_item"
        expiration = 5
        getl_timeout = 15
        node = self.memcapableTestBase.master

        # Add built-in user
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', node)
        time.sleep(10)

        # Assign user to role
        role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(node), 'builtin')
        time.sleep(10)

        mc = MemcachedClientHelper.direct_client(node, "default")
        key = "{0}_{1}".format(prefix, str(uuid.uuid4()))
        self.log.info("setting key {0} with expiration {1}".format(key, expiration))
        mc.set(key, expiration, 0, key)
        self.log.info("getl key {0} timeout {1}".format(key, getl_timeout))
        mc.getl(key, getl_timeout)
        self.log.info("get key {0} which is locked now".format(key))
        flags_v, cas_v, get_v = mc.get(key)
        self.assertEqual(get_v.decode(), key)
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
        self.assertEqual(get_v.decode(), "*")

    def tearDown(self):
        self.memcapableTestBase.tearDown()

class GetrTests(BaseTestCase):
    NO_REBALANCE = 0
    DURING_REBALANCE = 1
    AFTER_REBALANCE = 2

    def setUp(self):
        super(GetrTests, self).setUp()
        self.memcapableTestBase = MemcapableTestBase()
        self.rest = RestConnection(self.master)
        descr = self.input.param("descr", "")
        if descr:
            self.log.info("Test:{0}".format(descr))

    def tearDown(self):
        super(GetrTests, self).tearDown()

    def test_getr(self):
        item_count = self.input.param("item_count", 10000)
        replica_count = self.input.param("replica_count", 1)
        expiration = self.input.param("expiration", 0)
        delay = float(self.input.param("delay", 0))
        eject = self.input.param("eject", 0)
        delete = self.input.param("delete", 0)
        mutate = self.input.param("mutate", 0)
        warmup = self.input.param("warmup", 0)
        skipload = self.input.param("skipload", 0)
        rebalance = self.input.param("rebalance", 0)

        negative_test = False
        if delay > expiration:
            negative_test = True
        if delete and not mutate:
            negative_test = True
        if skipload and not mutate:
            negative_test = True

        prefix = str(uuid.uuid4())[:7]

        BucketOperationHelper.delete_all_buckets_or_assert([self.master], self)
        BucketOperationHelper.create_bucket(
            self.master, name=self.default_bucket_name,
            replica=replica_count, port=11210, test_case=self,
            bucket_ram=-1, password="")

        if rebalance == GetrTests.DURING_REBALANCE or rebalance == GetrTests.AFTER_REBALANCE:
            # leave 1 node unclustered for rebalance in
            ClusterOperationHelper.begin_rebalance_out(self.master, self.servers[-1:])
            ClusterOperationHelper.end_rebalance(self.master)
            ClusterOperationHelper.begin_rebalance_in(self.master, self.servers[:-1])
            ClusterOperationHelper.end_rebalance(self.master)
        else:
            ClusterOperationHelper.begin_rebalance_in(self.master, self.servers)
            ClusterOperationHelper.end_rebalance(self.master)

        vprefix = ""
        if not skipload:
            self._load_items(item_count=item_count, expiration=expiration, prefix=prefix, vprefix=vprefix)
            if not expiration:
                RebalanceHelper.wait_for_stats_int_value(self.master, self.default_bucket_name, "curr_items_tot", item_count * (replica_count + 1), "<=", 600, True)

        if delete:
            self._delete_items(item_count=item_count, prefix=prefix)

        if mutate:
            vprefix = "mutated"
            self._load_items(item_count=item_count, expiration=expiration, prefix=prefix, vprefix=vprefix)

        self.assertTrue(RebalanceHelper.wait_for_replication(self.rest.get_nodes(), timeout=180),
                            msg="replication did not complete")

        if eject:
            self._eject_items(item_count=item_count, prefix=prefix)

        if delay:
            self.sleep(delay)

        if rebalance == GetrTests.DURING_REBALANCE:
            ClusterOperationHelper.begin_rebalance_in(self.master, self.servers)
        if rebalance == GetrTests.AFTER_REBALANCE:
            ClusterOperationHelper.end_rebalance(self.master)
        if warmup:
            self.log.info("restarting memcached")
            command = "rpc:multicall(erlang, apply, [fun () -> try ns_server_testrunner_api:restart_memcached(20000) catch _:_ -> ns_port_sup:restart_port_by_name(memcached) end end, []], 20000)."
            memcached_restarted, content = self.rest.diag_eval(command)
            #wait until memcached starts
            self.assertTrue(memcached_restarted, "unable to restart memcached process through diag/eval")
            RebalanceHelper.wait_for_stats(self.master, self.default_bucket_name, "curr_items_tot", item_count * (replica_count + 1), 600)

        count = self._getr_items(item_count=item_count, replica_count=replica_count, prefix=prefix, vprefix=vprefix)

        if negative_test:
            self.assertTrue(count == 0, "found {0} items, expected none".format(count))
        else:
            self.assertTrue(count == replica_count * item_count, "expected {0} items, got {1} items".format(replica_count * item_count, count))
        if rebalance == GetrTests.DURING_REBALANCE:
            ClusterOperationHelper.end_rebalance(self.master)

    def _load_items(self, item_count, expiration, prefix, vprefix=""):
        flags = 0
        client = MemcachedClientHelper.proxy_client(self.master, self.default_bucket_name)
        time_start = time.time()
        for i in range(item_count):
            timeout_end = time.time() + 10
            passed = False
            while time.time() < timeout_end and not passed:
                try:
                    client.set(prefix + "_key_" + str(i), expiration, flags, vprefix + "_value_" + str(i))
                    passed = True
                except Exception as e:
                    self.log.error("failed to set key {0}, error: {1}".format(prefix + "_key_" + str(i), e))
                    time.sleep(2)
            self.assertTrue(passed, "exit due to set errors after {0} seconds".format(time.time() - time_start))
        self.log.info("loaded {0} items in {1} seconds".format(item_count, time.time() - time_start))

    def _get_items(self, item_count, prefix, vprefix=""):
        client = MemcachedClientHelper.proxy_client(self.master, self.default_bucket_name)
        time_start = time.time()
        get_count = 0
        last_error = ""
        error_count = 0
        for i in range(item_count):
            try:
                value = client.get(prefix + "_key_" + str(i))[2]
                assert(value == vprefix + "_value_" + str(i))
                get_count += 1
            except Exception as e:
                last_error = "failed to getr key {0}, error: {1}".format(prefix + "_key_" + str(i), e)
                error_count += 1
        if error_count > 0:
            self.log.error("got {0} errors, last error: {1}".format(error_count, last_error))
        self.log.info("got {0} replica items in {1} seconds".format(get_count, time.time() - time_start))
        return get_count

    def _getr_items(self, item_count, replica_count, prefix, vprefix=""):
        time_start = time.time()
        get_count = 0
        last_error = ""
        error_count = 0
        awareness = VBucketAwareMemcached(self.rest, self.default_bucket_name)
        for r in range(replica_count):
            for i in range(item_count):
                retry = True

                key = prefix + "_key_" + str(i)
                while retry:
                    client = awareness.memcached(key, r)
                    try:
                        value = client.getr(prefix + "_key_" + str(i))[2]
                        assert(value == vprefix + "_value_" + str(i))
                        get_count += 1
                        retry = False
                    except mc_bin_client.MemcachedError as e:
                        last_error = "failed to getr key {0}, error: {1}".format(prefix + "_key_" + str(i), e)
                        error_count += 1
                        if e.status == 7:
                            self.log.info("getting new vbucket map {0}")
                            awareness.reset(self.rest)
                        else:
                            retry = False
                    except Exception as e:
                        last_error = "failed to getr key {0}, error: {1}".format(prefix + "_key_" + str(i), e)
                        error_count += 1
                        retry = False
        if error_count > 0:
            self.log.error("got {0} errors, last error: {1}".format(error_count, last_error))
        self.log.info("got {0} replica items in {1} seconds".format(get_count, time.time() - time_start))
        awareness.done()
        return get_count

    def _delete_items(self, item_count, prefix):
        client = MemcachedClientHelper.proxy_client(self.master, self.default_bucket_name)
        time_start = time.time()
        for i in range(item_count):
            timeout_end = time.time() + 10
            passed = False
            while time.time() < timeout_end and not passed:
                try:
                    client.delete(prefix + "_key_" + str(i))
                    passed = True
                except Exception as e:
                    self.log.error("failed to delete key {0}, error: {1}".format(prefix + "_key_" + str(i), e))
                    time.sleep(2)
            self.assertTrue(passed, "exit due to delete errors after {0} seconds".format(time.time() - time_start))
        self.log.info("deleted {0} items in {1} seconds".format(item_count, time.time() - time_start))

    def _eject_items(self, item_count, prefix):
        client = MemcachedClientHelper.proxy_client(self.master, self.default_bucket_name)
        time_start = time.time()
        for i in range(item_count):
            timeout_end = time.time() + 10
            passed = False
            while time.time() < timeout_end and not passed:
                try:
                    client.evict_key(prefix + "_key_" + str(i))
                    passed = True
                except Exception as e:
                    self.log.error("failed to eject key {0}, error: {1}".format(prefix + "_key_" + str(i), e))
                    time.sleep(2)
            self.assertTrue(passed, "exit due to eject errors after {0} seconds".format(time.time() - time_start))
        self.log.info("ejected {0} items in {1} seconds".format(item_count, time.time() - time_start))

    def _next_parameter(self, d, di):
        carry = True
        for k in d:
            if carry:
                di[k] += 1
            if di[k] >= len(d[k]):
                carry = True
                di[k] = 0
            else:
                carry = False
        return not carry


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


class AppendTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.params = TestInputSingleton.input.test_params
        self.master = TestInputSingleton.input.servers[0]
        rest = RestConnection(self.master)
        rest.init_cluster(self.master.rest_username, self.master.rest_password)
        info = rest.get_nodes_self()
        rest.init_cluster_memoryQuota(self.master.rest_username, self.master.rest_password,
                                      memoryQuota=info.mcdMemoryReserved)
        ClusterOperationHelper.cleanup_cluster([self.master])
        ClusterOperationHelper.wait_for_ns_servers_or_assert([self.master], self)
        self._create_default_bucket()
        self.onenodemc = MemcachedClientHelper.direct_client(self.master, "default")

    def _create_default_bucket(self):
        name = "default"
        master = self.master
        rest = RestConnection(master)
        helper = RestHelper(RestConnection(master))
        if not helper.bucket_exists(name):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(TestInputSingleton.input.servers)
            info = rest.get_nodes_self()
            available_ram = info.memoryQuota * node_ram_ratio
            rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram),
                               numVBuckets=128)
            ready = BucketOperationHelper.wait_for_memcached(master, name)
            self.assertTrue(ready, msg="wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(name),
                        msg="unable to create {0} bucket".format(name))
        self.load_thread = None
        self.shutdown_load_data = False

    def test_append_wrong_cas(self):
        #monitor the memory usage , it should not go beyond
        #doing append 20,000 times ( each 5k) mem_used should not increase more than
        #10 percent
        #
        stats = self.onenodemc.stats()
        initial_mem_used = -1
        if "mem_used" in stats:
            initial_mem_used = int(stats["mem_used"])
            self.assertTrue(initial_mem_used > 0)
        key = str(uuid.uuid4())
        size = 5 * 1024
        value = MemcachedClientHelper.create_value("*", size)
        self.onenodemc.set(key, 0, 0, value)
        flags_v, cas_v, get_v = self.onenodemc.get(key)
        self.onenodemc.append(key, value, cas_v)
        iteration = 50000
        for i in range(0, iteration):
            try:
                self.onenodemc.append(key, value, random.randint(0, 1000))
            except:
                #ignoring the error here
                pass
        stats = self.onenodemc.stats()
        if "mem_used" in stats:
            delta = int(stats["mem_used"]) - initial_mem_used
            self.log.info("initial mem_used {0}, current mem_used {1}".format(initial_mem_used, stats["mem_used"]))
            self.log.info(delta)

    def test_append_with_delete(self):
        #monitor the memory usage , it should not go beyond
        #doing append 20,000 times ( each 5k) mem_used should not increase more than
        #10 percent
        #
        if "iteration" in self.params:
            iteration = int(self.params["iteration"])
        else:
            iteration = 50000
        if "items" in self.params:
            items = int(self.params["items"])
        else:
            items = 10000
        if "append_size" in self.params:
            append_size = int(self.params["append_size"])
        else:
            append_size = 5 * 1024
        append_iteration_before_delete = 100
        keys = [str(uuid.uuid4()) for i in range(0, items)]
        size = 5 * 1024
        stats = self.onenodemc.stats()
        initial_mem_used = -1
        self.log.info("items : {0} , iteration : {1} ".format(items, iteration))
        if "mem_used" in stats:
            initial_mem_used = int(stats["mem_used"])
            self.assertTrue(initial_mem_used > 0)
        for i in range(0, iteration):
            for key in keys:
                self.onenodemc.set(key, 0, 0, os.urandom(size))
            try:
                for append_iteration in range(0, append_iteration_before_delete):
                    appened_value = MemcachedClientHelper.create_value("*", append_size)
                    for key in keys:
                        self.onenodemc.append(key, appened_value)
                        self.onenodemc.get(key)
            except:
                #ignoring the error here
                pass
            stats = None
            for t in range(0, 10):
                try:
                    stats = self.onenodemc.stats()
                    break
                except:
                    pass

            if stats and "mem_used" in stats:
                delta = int(stats["mem_used"]) - initial_mem_used
                #only print out if delta is more than 20% than it should be
                # delta ahould be #items * size + #items * append
                expected_delta = items * (size + append_iteration_before_delete * append_size * 1.0)
                msg = "initial mem_used {0}, current mem_used {1} , delta : {2} , expected delta : {3} , increase percentage {4}"
                self.log.info(
                    msg.format(initial_mem_used, stats["mem_used"], delta, expected_delta, delta // expected_delta))
                if delta > (1.2 * expected_delta):
                    self.fail("too much memory..")
                for key in keys:
                    self.onenodemc.delete(key)
            self.log.info   ("iteration #{0} completed".format(i))

    def tearDown(self):
        self.shutdown_load_data = True
        if self.load_thread:
            self.load_thread.join()

#        BucketOperationHelper.delete_all_buckets_or_assert([self.master], self)

class WarmUpMemcachedTest(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.params = TestInputSingleton.input.test_params
        if TestInputSingleton.input.param("log_level", None):
            self.log.setLevel(level=0)
            for hd in self.log.handlers:
                if str(hd.__class__).find('FileHandler') != -1:
                    hd.setLevel(level=logging.DEBUG)
                else:
                    log_level = str(TestInputSingleton.input.param("log_level", "INFO")).upper()
                    hd.setLevel(level=getattr(logging, log_level))
        self.master = TestInputSingleton.input.servers[0]
        rest = RestConnection(self.master)
        rest.init_cluster(self.master.rest_username, self.master.rest_password)
        info = rest.get_nodes_self()
        rest.init_cluster_memoryQuota(self.master.rest_username, self.master.rest_password,
                                      memoryQuota=info.mcdMemoryReserved)
        ClusterOperationHelper.cleanup_cluster([self.master])
        ClusterOperationHelper.wait_for_ns_servers_or_assert([self.master], self)
        BucketOperationHelper.delete_all_buckets_or_assert([self.master], self)
        # recreate bucket instead of flush
        self._create_default_bucket()
        self.onenodemc = MemcachedClientHelper.direct_client(self.master, "default")
        self._log_start()

    def tearDown(self):
        ClusterOperationHelper.cleanup_cluster([self.master])
        BucketOperationHelper.delete_all_buckets_or_assert([self.master], self)
        self._log_finish()

    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass


    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    def _create_default_bucket(self):
        name = "default"
        master = self.master
        rest = RestConnection(master)
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(TestInputSingleton.input.servers)
        info = rest.get_nodes_self()
        available_ram = info.memoryQuota * node_ram_ratio
        rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram),
                           numVBuckets=128)
        ready = BucketOperationHelper.wait_for_memcached(master, name)
        self.assertTrue(ready, msg="wait_for_memcached failed")
        self.load_thread = None
        self.shutdown_load_data = False

    def _insert_data(self, howmany):
    #        prefix = str(uuid.uuid4())
        items = ["{0}-{1}".format(str(uuid.uuid4()), i) for i in range(0, howmany)]
        for item in items:
            self.onenodemc.set(item, 0, 0, item)
        self.log.info("inserted {0} items".format(howmany))

    def _do_warmup(self, howmany, timeout_in_seconds=1800):
        # max_time is in micro seconds
        self._insert_data(howmany)
        if int(howmany) < 50:
            self.log.info("sleep 10 seconds for small number items insert correctly into bucket")
            time.sleep(10)
        curr_items = int(self.onenodemc.stats()["curr_items"])
        uptime = int(self.onenodemc.stats()["uptime"])
        RebalanceHelper.wait_for_persistence(self.master, "default")

        rest = RestConnection(self.master)
        command = "try ns_server_testrunner_api:kill_memcached(20000) catch _:_ -> [erlang:exit(element(2, X), kill) || X <- supervisor:which_children(ns_port_sup)] end."
        memcached_restarted, content = rest.diag_eval(command)
        self.assertTrue(memcached_restarted, "unable to restart memcached process through diag/eval")

        #wait until memcached starts
        start = time.time()
        memcached_restarted = False
        while time.time() - start < 60:
            try:
                self.onenodemc = MemcachedClientHelper.direct_client(self.master, "default")
                value = int(self.onenodemc.stats()["uptime"])
                if value < uptime:
                    self.log.info("memcached restarted...")
                    memcached_restarted = True
                    break
                self.onenodemc.close()
                # The uptime stat have a 1 sec resolution so there is no point of
                # retrying more often
                time.sleep(1)
            except Exception:
                time.sleep(1)

        self.assertTrue(memcached_restarted, "memcached restarted and uptime is now reset")

        # There is a race condition from the server started until
        # ns_server loaded ep_engine and the curr_items stat is being
        # reported... until that trying to call int(stats["curr_items"])
        # may cause the test to fail.
        # As of Watson you'll get associated with the bucket
        # during the initial connect, so if "default" isn't
        # loaded when you connect you won't be "magically" bound
        # to it when it is loaded... disconnect and try again
        # at a later time..
        self.log.info("Waiting for bucket to be loaded again")
        for _ in range(5):
            self.log.info("Waiting for bucket to be loaded again.")
            try:
                ready = BucketOperationHelper.wait_for_memcached(self.master, "default")
                self.assertTrue(ready, "wait_for_memcached failed")
            except:
                continue
            break

        while True:
            self.onenodemc = MemcachedClientHelper.direct_client(self.master, "default")
            stats = self.onenodemc.stats()
            if 'curr_items' in stats:
                break
            self.onenodemc.close()
            time.sleep(0.025)

        self.log.info("Bucket loaded, wait for warmup to complete")

        # Warmup till curr_items match
        present_count = int(stats["curr_items"])
        ep_warmup_thread = stats["ep_warmup_thread"]
        self.log.info("ep curr_items : {0}, inserted_items {1} directly after kill_memcached ".format(present_count, curr_items))
        self.log.info("ep_warmup_thread directly after kill_memcached: {0}".format(ep_warmup_thread))
        start = time.time()
        while ep_warmup_thread != "complete":
            if (time.time() - start) <= timeout_in_seconds:
                stats = self.onenodemc.stats()
                present_count = int(stats["curr_items"])
                ep_warmup_thread = stats["ep_warmup_thread"]
                self.log.warning("curr_items {0}, ep_warmup_thread {1}".format(present_count, ep_warmup_thread))
                time.sleep(1)
            else:
                self.fail("Timed out waiting for warmup")

        stats = self.onenodemc.stats()
        present_count = int(stats["curr_items"])
        if present_count < curr_items:
            self.log.error("Warmup failed. Got {0} and expected {1} items".format(present_count, curr_items))
            self.fail("Warmup failed. Incomplete number of messages after killing memcached")

        if "ep_warmup_time" not in stats:
            self.log.error("'ep_warmup_time' was not found in stats:{0}".format(stats))
        warmup_time = int(stats["ep_warmup_time"])
        self.log.info("ep_warmup_time is {0}".format(warmup_time))

    def do_warmup_2(self):
        self._do_warmup(2)

    def do_warmup_10k(self):
        self._do_warmup(10000)

    def do_warmup_100k(self):
        self._do_warmup(100000)

    def do_warmup_1M(self):
        self._do_warmup(1000000)


class MultiGetNegativeTest(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.params = TestInputSingleton.input.test_params
        self.master = TestInputSingleton.input.servers[0]
        rest = RestConnection(self.master)
        rest.init_cluster(self.master.rest_username, self.master.rest_password)
        info = rest.get_nodes_self()
        rest.init_cluster_memoryQuota(self.master.rest_username, self.master.rest_password,
                                      memoryQuota=info.mcdMemoryReserved)
        ClusterOperationHelper.cleanup_cluster([self.master])
        ClusterOperationHelper.wait_for_ns_servers_or_assert([self.master], self)
        self._create_default_bucket()
        self.keys_cleanup = []
        self.onenodemc = MemcachedClientHelper.direct_client(self.master, "default", timeout=600)


    def tearDown(self):
        if self.onenodemc:
            #delete the keys
            for key in self.keys_cleanup:
                try:
                    self.onenodemc.delete(key)
                except Exception:
                    pass

    def _create_default_bucket(self):
        name = "default"
        master = self.master
        rest = RestConnection(master)
        helper = RestHelper(RestConnection(master))
        if not helper.bucket_exists(name):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(TestInputSingleton.input.servers)
            info = rest.get_nodes_self()
            available_ram = info.memoryQuota * node_ram_ratio
            rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram),
                               numVBuckets=128)
            ready = BucketOperationHelper.wait_for_memcached(master, name)
            self.assertTrue(ready, msg="wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(name),
                        msg="unable to create {0} bucket".format(name))

    def _insert_data(self, client, howmany):
        prefix = str(uuid.uuid4())
        keys = ["{0}-{1}".format(prefix, i) for i in range(0, howmany)]
        value = MemcachedClientHelper.create_value("*", 1024)
        for key in keys:
            client.set(key, 0, 0, value)
        self.log.info("inserted {0} items".format(howmany))
        return keys


    def test_mc_multi_get(self):
        self._test_multi_get(self.onenodemc, 10)
        self._test_multi_get(self.onenodemc, 100)
        self._test_multi_get(self.onenodemc, 1000)
        self._test_multi_get(self.onenodemc, 10 * 1000)
        self._test_multi_get(self.onenodemc, 20 * 1000)
        self._test_multi_get(self.onenodemc, 30 * 1000)
        self._test_multi_get(self.onenodemc, 40 * 1000)

    def _test_multi_get(self, client, howmany):
        mc_message = "memcached virt memory size {0} : resident memory size : {1}"
        shell = RemoteMachineShellConnection(self.master)
        mc_pid = RemoteMachineHelper(shell).is_process_running("memcached").pid
        keys = self._insert_data(client, howmany)
        self.keys_cleanup.extend(keys)
        self.log.info("printing memcached stats before running multi-get")
        memcached_sys_stats = self._extract_proc_info(shell, mc_pid)
        self.log.info(mc_message.format(int(memcached_sys_stats["rss"]) * 4096,
                                        memcached_sys_stats["vsize"]))

        self.log.info("running multiget to get {0} keys".format(howmany))
        gets = client.getMulti(keys)
        self.log.info("received {0} keys".format(len(gets)))

        self.log.info(gets)
        self.assertEqual(len(gets), len(keys))

        self.log.info("printing memcached stats after running multi-get")
        memcached_sys_stats = self._extract_proc_info(shell, mc_pid)
        self.log.info(mc_message.format(int(memcached_sys_stats["rss"]) * 4096,
                                        memcached_sys_stats["vsize"]))

    def _extract_proc_info(self, shell, pid):
        o, r = shell.execute_command("cat /proc/{0}/stat".format(pid))
        fields = ('pid comm state ppid pgrp session tty_nr tpgid flags minflt '
                  'cminflt majflt cmajflt utime stime cutime cstime priority '
                  'nice num_threads itrealvalue starttime vsize rss rsslim '
                  'startcode endcode startstack kstkesp kstkeip signal blocked '
                  'sigignore sigcatch wchan nswap cnswap exit_signal '
                  'processor rt_priority policy delayacct_blkio_ticks '
                  'guest_time cguest_time ').split(' ')
        d = dict(list(zip(fields, o[0].split(' '))))
        return d


class MemcachedValueSizeLimitTest(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.params = TestInputSingleton.input.test_params
        self.master = TestInputSingleton.input.servers[0]
        rest = RestConnection(self.master)
        rest.init_cluster(self.master.rest_username, self.master.rest_password)
        info = rest.get_nodes_self()
        rest.init_cluster_memoryQuota(self.master.rest_username, self.master.rest_password,
                                      memoryQuota=info.mcdMemoryReserved)
        ClusterOperationHelper.cleanup_cluster([self.master])
        ClusterOperationHelper.wait_for_ns_servers_or_assert([self.master], self)
        self._create_default_bucket()
        self.keys_cleanup = []
        self.onenodemc = MemcachedClientHelper.direct_client(self.master, "default")


    def tearDown(self):
        if self.onenodemc:
            #delete the keys
            for key in self.keys_cleanup:
                try:
                    self.onenodemc.delete(key)
                except Exception:
                    pass

    def _create_default_bucket(self):
        name = "default"
        master = self.master
        rest = RestConnection(master)
        helper = RestHelper(RestConnection(master))
        if not helper.bucket_exists(name):
            node_ram_ratio = BucketOperationHelper.base_bucket_ratio(TestInputSingleton.input.servers)
            info = rest.get_nodes_self()
            available_ram = info.memoryQuota * node_ram_ratio
            rest.create_bucket(bucket=name, ramQuotaMB=int(available_ram),
                               numVBuckets=128)
            ready = BucketOperationHelper.wait_for_memcached(master, name)
            self.assertTrue(ready, msg="wait_for_memcached failed")
        self.assertTrue(helper.bucket_exists(name),
                        msg="unable to create {0} bucket".format(name))


    def test_append_till_20_mb(self):
        initial_value = "12345678"
        key = str(uuid.uuid4())
        self.keys_cleanup.append(key)
        self.onenodemc.set(key, 0, 0, initial_value)
        #for 20 * 1024 times append 1024 chars each time
        value = MemcachedClientHelper.create_value("*", 1024 * 20)
        for i in range(0, (1024 - 1)):
            self.onenodemc.append(key, value)
        try:
            self.onenodemc.append(key, value)
            self.fail("memcached did not raise an error")
        except mc_bin_client.MemcachedError as err:
            self.assertEqual(err.status, 3)


    def test_prepend_till_20_mb(self):
        initial_value = "12345678"
        key = str(uuid.uuid4())
        self.keys_cleanup.append(key)
        self.onenodemc.set(key, 0, 0, initial_value)
        #for 20 * 1024 times append 1024 chars each time
        value = MemcachedClientHelper.create_value("*", 1024 * 20)
        for i in range(0, (1024 - 1)):
            self.onenodemc.prepend(key, value)
        try:
            self.onenodemc.prepend(key, value)
            self.fail("memcached did not raise an error")
        except mc_bin_client.MemcachedError as err:
            self.assertEqual(err.status, 3)

#    def test_incr_till_max(self):
#        initial_value = '0'
#        max_value = pow(2, 64)
#        step = max_value // 1024
#        self.log.info("step : {0}")
#        key = str(uuid.uuid4())
#        self.keys_cleanup.append(key)
#        self.onenodemc.set(key, 0, 0, initial_value)
#        for i in range(0, (2 * 1024 - 1)):
#            self.onenodemc.incr(key, amt=step)
#            a, b, c = self.onenodemc.get(key)
#            delta = long(c) - max_value
#            self.log.info("delta = value - pow(2,64) = {0}".format(delta))
#        a, b, c = self.onenodemc.get(key)
#        self.log.info("key : {0} value {1}".format(key, c))
#        try:
#            self.onenodemc.incr(key, step)
#            self.fail("memcached did not raise an error")
#        except mc_bin_client.MemcachedError as err:
#            self.assertEqual(err.status, 5)
#
#    def test_decr_till_max(self):
#        initial_value = '1'
#        max_value = pow(2, 64)
#        step = max_value // 1024
#        key = str(uuid.uuid4())
#        self.keys_cleanup.append(key)
#        self.onenodemc.set(key, 0, 0, initial_value)
#        for i in range(0, (1024 - 1)):
#            self.onenodemc.decr(key, amt=step)
#        a, b, c = self.onenodemc.get(key)
#        self.log.info("key : {0} value {1}".format(key, c))
#        try:
#            self.onenodemc.incr(key, step)
#            self.fail("memcached did not raise an error")
#        except mc_bin_client.MemcachedError as err:
#            self.assertEqual(err.status, 5)
