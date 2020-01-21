from queue import Empty
from multiprocessing import Queue
from threading import Thread
import unittest
import os
import testconstants
from TestInput import TestInputSingleton
import mc_bin_client
import uuid
import logger
import time
import datetime
from membase.api.rest_client import RestConnection
from membase.api.tap import TapConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
import memcacheConstants
from memcached.helper.data_helper import MemcachedClientHelper
from sdk_client import SDKSmartClient
from security.rbac_base import RbacBase

class ExpiryTests(unittest.TestCase):
    log = None
    _servers = None
    _bucket_port = None
    _bucket_name = None

    def suite_setUp(self):
        self.log.info("suite_setUp...")

    def suite_tearDown(self):
        self.log.info("suite_tearDown...")

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.master = TestInputSingleton.input.servers[0]
        ClusterOperationHelper.cleanup_cluster([self.master])
        BucketOperationHelper.delete_all_buckets_or_assert([self.master], self)

        self._bucket_name = 'default'

        serverInfo = self.master

        rest = RestConnection(serverInfo)
        info = rest.get_nodes_self()
        self._bucket_port = info.moxi
        rest.init_cluster(username=serverInfo.rest_username,
                          password=serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_ram = info.memoryQuota * 2 // 3

        # Add built-in user
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', self.master)
        
        # Assign user to role
        role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(self.master), 'builtin')
        
        self.log.info("-->create_bucket: {},{},{}".format(self._bucket_name,bucket_ram,info.memcached))
        rest.create_bucket(bucket=self._bucket_name,
                           ramQuotaMB=bucket_ram,
                           proxyPort=info.memcached)
        
        msg = 'create_bucket succeeded but bucket "default" does not exist'
        
        if (testconstants.TESTRUNNER_CLIENT in list(os.environ.keys())) and os.environ[testconstants.TESTRUNNER_CLIENT] == testconstants.PYTHON_SDK:
            self.client = SDKSmartClient(serverInfo, self._bucket_name, compression=TestInputSingleton.input.param(
                "sdk_compression", True))
        else:
            self.client = MemcachedClientHelper.direct_client(serverInfo, self._bucket_name)
            
        self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(self._bucket_name, rest), msg=msg)
        ready = BucketOperationHelper.wait_for_memcached(serverInfo, self._bucket_name)
        self.assertTrue(ready, "wait_for_memcached failed")
        self._log_start()

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

    # test case to set 1000 keys and verify that those keys are stored
    #e1
    def test_expired_keys(self):
        self.log.info("--> in test_expired_keys...")
        serverInfo = self.master
        client = self.client
        expirations = [2, 5, 10]
        for expiry in expirations:
            testuuid = uuid.uuid4()
            keys = ["key_%s_%d" % (testuuid, i) for i in range(500)]
            self.log.info("pushing keys with expiry set to {0}".format(expiry))
            for key in keys:
                try:
                    client.set(key, expiry, 0, key)
                except mc_bin_client.MemcachedError as error:
                    msg = "unable to push key : {0} to bucket : {1} error : {2}"
                    self.log.error(msg.format(key, client.vbucketId, error.status))
                    self.fail(msg.format(key, client.vbucketId, error.status))
            self.log.info("inserted {0} keys with expiry set to {1}".format(len(keys), expiry))
            delay = expiry + 5
            msg = "sleeping for {0} seconds to wait for those items with expiry set to {1} to expire"
            self.log.info(msg.format(delay, expiry))
            time.sleep(delay)
            self.log.info('verifying that all those keys have expired...')
            for key in keys:
                try:
                    client.get(key=key)
                    msg = "expiry was set to {0} but key: {1} did not expire after waiting for {2}+ seconds"
                    self.fail(msg.format(expiry, key, delay))
                except mc_bin_client.MemcachedError as error:
                    self.assertEqual(error.status, 1,
                                      msg="expected error code {0} but saw error code {1}".format(1, error.status))
            self.log.info("verified that those keys inserted with expiry set to {0} have expired".format(expiry))


    # MB-3964
    # 1) set 1000 keys with 15s expiry
    # 2) verify curr_items
    # 3) wait 30 seconds
    # 4) do a dump with tap.py and count number of deletes sent (should be 0)
    def test_expired_keys_tap(self):

        server = self.master
        # callback to track deletes
        queue = Queue(maxsize=10000)
        listener = TapListener(queue, server, "CMD_TAP_DELETE")

        client = self.client
        expirations = [15]
        for expiry in expirations:
            testuuid = uuid.uuid4()
            keys = ["key_%s_%d" % (testuuid, i) for i in range(1000)]
            self.log.info("pushing keys with expiry set to {0}".format(expiry))
            for key in keys:
                try:
                    client.set(key, expiry, 0, key)
                except mc_bin_client.MemcachedError as error:
                    msg = "unable to push key : {0} to bucket : {1} error : {2}"
                    self.log.error(msg.format(key, client.vbucketId, error.status))
                    self.fail(msg.format(key, client.vbucketId, error.status))
            self.log.info("inserted {0} keys with expiry set to {1}".format(len(keys), expiry))
            delay = expiry + 15
            msg = "sleeping for {0} seconds to wait for those items with expiry set to {1} to expire"
            self.log.info(msg.format(delay, expiry))
            time.sleep(delay)
            self.log.info('verifying that all those keys have expired...')
            for key in keys:
                try:
                    client.get(key=key)
                    msg = "expiry was set to {0} but key: {1} did not expire after waiting for {2}+ seconds"
                    self.fail(msg.format(expiry, key, delay))
                except mc_bin_client.MemcachedError as error:
                    self.assertEqual(error.status, 1,
                                      msg="expected error code {0} but saw error code {1}".format(1, error.status))
            self.log.info("verified that those keys inserted with expiry set to {0} have expired".format(expiry))
            listener.start()
            try:
                was_empty = 0
                deletes_seen = 0
                while was_empty < 2:
                    try:
                        queue.get(False, 5)
                        deletes_seen += 1
                    except Empty:
                        print("exception thrown")
                        print("how many deletes_seen ? {0}".format(deletes_seen))
                        was_empty += 1
                self.assertEqual(deletes_seen, 0, msg="some some deletes")
                self.log.info("seen {0} CMD_TAP_DELETE".format(deletes_seen))
            finally:
                listener.aborted = True

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(servers=[self.master], test_case=self)
        # Remove rbac user in teardown
        role_del = ['cbadminbucket']
        RbacBase().remove_user_role(role_del, RestConnection(self.master))
        self._log_finish()


class TapListener(Thread):
    def __init__(self, queue, server, filter):
        Thread.__init__(self)
        self.queue = queue
        self.server = server
        self.stats = []
        self.aborted = False
        self.filter = filter

    def run(self):
        self.tap()

    def callback(self, identifier, cmd, extra, key, vb, val, cas):
        command_names = memcacheConstants.COMMAND_NAMES[cmd]
        if self.filter and command_names == self.filter:
#            print "%s: ``%s'' (vb:%d) -> (%d bytes from %s)" % (
#            memcacheConstants.COMMAND_NAMES[cmd],
#            key, vb, len(val), identifier)
#            print extra, cas
            self.queue.put(key)


    def tap(self):
        print("starting tap process")
        t = TapConnection(self.server, 11210, callback=self.callback, clientId=str(uuid.uuid4()),
                          opts={memcacheConstants.TAP_FLAG_BACKFILL: 0xffffffff})
        while True and not self.aborted:
            t.receive()
