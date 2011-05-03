import random
import unittest
from TestInput import TestInputSingleton
import mc_bin_client
import socket
import zlib
import ctypes
import uuid
import logger
import time
import crc32
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection

class SimpleSetGetTestBase(object):
    log = None
    keys = None
    clients = None
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
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self.test)
        for serverInfo in self.servers:
            remote = RemoteMachineShellConnection(serverInfo)
            info = remote.extract_remote_info()
            remote.terminate_process(info, 'memcached')
            remote.terminate_process(info, 'moxi')

        #let's kill moxi and memcached

        #let's first install membase server

        #lets create 'default' bucket
        #        bucket_name = 'SimpleSetGetTest-{0}'.format(uuid.uuid4())
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            if bucket_name != 'default' and self.bucket_port == 11211:
                rest.create_bucket(bucket=bucket_name,
                                   bucketType=bucket_type,
                                   ramQuotaMB=200,
                                   replicaNumber=1,
                                   proxyPort=self.bucket_port,
                                   authType='sasl',
                                   saslPassword='password')
                msg = 'create_bucket succeeded but bucket "default" does not exist'
                self.test.assertTrue(BucketOperationHelper.wait_for_bucket_creation(bucket_name, rest), msg=msg)
                BucketOperationHelper.wait_till_memcached_is_ready_or_assert(self.servers,
                                                                             self.bucket_port,
                                                                             test=unittest,
                                                                             bucket_name=self.bucket_name,
                                                                             bucket_password='password')
            else:
                rest.create_bucket(bucket=bucket_name,
                                   bucketType=bucket_type,
                                   ramQuotaMB=200,
                                   replicaNumber=1,
                                   proxyPort=self.bucket_port)
            msg = 'create_bucket succeeded but bucket "default" does not exist'
            self.test.assertTrue(BucketOperationHelper.wait_for_bucket_creation(bucket_name, rest), msg=msg)
            BucketOperationHelper.wait_till_memcached_is_ready_or_assert(self.servers,
                                                                         self.bucket_port,
                                                                         test=unittest,
                                                                         bucket_name=self.bucket_name)

        #if its a sasl enabled bucket then wait_till should know about it

        self.log.info('10 seconds sleep...')
        self.clients = self.create_mc_bin_clients_for_servers(self.servers)
        #populate key
        testuuid = uuid.uuid4()
        self.keys = ["key_%s_%d" % (testuuid, i) for i in range(500)]


    # test case to set 500 keys and verify that those keys are stored
    def set_get_small_keys(self):
        for serverInfo in self.servers:
            self.log.info(
                'pushing 500 keys (sizes between 100 bytes to 1KB to different vbuckets in membase @ {0}'.format(serverInfo.ip))
            for key in self.keys:
                vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                self.clients[serverInfo.ip].vbucketId = vbucketId
                #                self.log.info("vbucket : {0}".format(vbucketId))
                self.clients[serverInfo.ip].vbucketId = vbucketId
                payload = self.generate_payload(key + '\0\r\n\0\0\n\r\0',
                                                random.randint(100, 1024))
                flag = socket.htonl(ctypes.c_uint32(zlib.adler32(payload)).value)
                try:
                    self.clients[serverInfo.ip].set(key, 0, flag, payload)
                    self.log.info("inserted key {0} to vBucket {1}".format(key, vbucketId))
                except mc_bin_client.MemcachedError as error:
                    self.log.info('memcachedError : {0}'.format(error.status))
                    self.test.fail("unable to push key : {0} to bucket : {1}".format(key, self.clients[serverInfo.ip].vbucketId))

            for key in self.keys:
                try:
                    vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                    self.clients[serverInfo.ip].vbucketId = vbucketId
                    flag, keyx, value = self.clients[serverInfo.ip].get(key=key)
                    actual_flag = socket.ntohl(flag)
                    expected_flag = ctypes.c_uint32(zlib.adler32(value)).value
                    self.test.assertEquals(actual_flag, expected_flag, msg='flags dont match')
                except mc_bin_client.MemcachedError as error:
                    self.log.info('memcachedError : {0}'.format(error.status))
                    self.test.fail("unable to get a pre-inserted key : {0}".format(key))

    def set_get_large_keys(self):
        for serverInfo in self.servers:
            self.log.info(
                'pushing 500 keys (sizes between 500KB and 1 MB to different vbuckets in membase @ {0}'.format(
                    serverInfo.ip))
            error_codes = []
            inserted_keys = []
            for key in self.keys:
                vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                self.clients[serverInfo.ip].vbucketId = vbucketId
                #                self.log.info("vbucket : {0}".format(vbucketId))
                self.clients[serverInfo.ip].vbucketId = vbucketId
                payload = self.generate_payload(key + '\0\r\n\0\0\n\r\0',
                                                random.randint(500 * 1024, 1024 * 1024))
                rest = RestConnection(serverInfo)
                info = rest.get_bucket(self.bucket_name)
                emptySpace = info.stats.ram - info.stats.memUsed
                #break if there isn't at least 30 mg free space
                if emptySpace < ( 30 * 1024 * 1024):
                    self.log.info('emptySpace : {0}'.format(emptySpace))
                    break
                flag = socket.htonl(ctypes.c_uint32(zlib.adler32(payload)).value)
                try:
                    self.clients[serverInfo.ip].set(key, 0, flag, payload)
#                    self.log.info("inserted key {0} to vBucket {1}".format(key, vbucketId))
                    inserted_keys.append(key)
                except mc_bin_client.MemcachedError as error:
                    self.log.info('memcachedError : {0}'.format(error.status))
                    error_codes.append(error.status)
                    self.log.error("unable to push key : {0} to bucket : {1}".format(key, self.clients[serverInfo.ip].vbucketId))

            #fail if we were not able to insert even 1 key
            self.log.info('inserted {0} keys '.format(len(inserted_keys)))
            if error_codes:
                self.log.info('printing error codes seen during key insertions')
                for code in error_codes:
                    self.log.info('error code {0} while inserting keys'.format(code))
                self.test.fail('unable to insert {0} keys'.format(len(error_codes)))
            for key in inserted_keys:
                try:
                    vbucketId = crc32.crc32_hash(key) & 1023 # or & 0x3FF
                    self.clients[serverInfo.ip].vbucketId = vbucketId
                    flag, keyx, value = self.clients[serverInfo.ip].get(key=key)
                    actual_flag = socket.ntohl(flag)
                    expected_flag = ctypes.c_uint32(zlib.adler32(value)).value
                    self.test.assertEquals(actual_flag, expected_flag, msg='flags dont match')
                except mc_bin_client.MemcachedError as error:
                    self.log.info('memcachedError : {0}'.format(error.status))
                    self.test.fail("unable to get a pre-inserted key : {0}".format(key))


    def tearDown_bucket(self):
        #let's clean up the memcached
        for serverInfo in self.servers:
            for key in self.keys:
                try:
                    self.clients[serverInfo.ip].delete(key=key)
                except mc_bin_client.MemcachedError:
                #                    self.log.info('unable to delete key : {0} from memcached @ {1}'.format(key, ip))
                    pass
            self.clients[serverInfo.ip].close()
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self.test)


    def generate_payload(self, pattern, size):
        return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]

    def create_mc_bin_clients_for_servers(self, servers):
        clients = {}
        for serverInfo in servers:
            clients[serverInfo.ip] = MemcachedClientHelper.create_memcached_client(serverInfo.ip,
                                                                                   self.bucket_name,
                                                                                   self.bucket_port,
                                                                                   'password')
        return clients


class SimpleSetGetMembaseBucketDefaultPort11211(unittest.TestCase):
    simpleSetGetTestBase = None

    def setUp(self):
        self.simpleSetGetTestBase = SimpleSetGetTestBase()
        self.simpleSetGetTestBase.setUp_bucket('default', 11211, 'membase', self)

    def test_set_get_small_keys(self):
        self.simpleSetGetTestBase.set_get_small_keys()

    def test_set_get_large_keys(self):
        self.simpleSetGetTestBase.set_get_large_keys()

    def tearDown(self):
        if self.simpleSetGetTestBase:
            self.simpleSetGetTestBase.tearDown_bucket()


class SimpleSetGetMembaseBucketNonDefaultDedicatedPort(unittest.TestCase):
    simpleSetGetTestBase = None

    def setUp(self):
        self.simpleSetGetTestBase = SimpleSetGetTestBase()
        self.simpleSetGetTestBase.setUp_bucket('setget-{0}'.format(uuid.uuid4()), 11220, 'membase', self)

    def test_set_get_small_keys(self):
        self.simpleSetGetTestBase.set_get_small_keys()

    def test_set_get_large_keys(self):
        self.simpleSetGetTestBase.set_get_large_keys()

    def tearDown(self):
        if self.simpleSetGetTestBase:
            self.simpleSetGetTestBase.tearDown_bucket()

class SimpleSetGetMembaseBucketNonDefaultPost11211(unittest.TestCase):

    simpleSetGetTestBase = None

    def setUp(self):
        self.simpleSetGetTestBase = SimpleSetGetTestBase()
        self.simpleSetGetTestBase.setUp_bucket('setget-{0}'.format(uuid.uuid4()), 11211, 'membase', self)

    def test_set_get_small_keys(self):
        self.simpleSetGetTestBase.set_get_small_keys()

    def test_set_get_large_keys(self):
        self.simpleSetGetTestBase.set_get_large_keys()

    def tearDown(self):
        if self.simpleSetGetTestBase:
            self.simpleSetGetTestBase.tearDown_bucket()