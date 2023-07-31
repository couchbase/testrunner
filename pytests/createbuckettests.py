import unittest
import uuid
from TestInput import TestInput, TestInputSingleton
import logger
import datetime
import time
from membase.api.exception import BucketCreationException
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from security.rbac_base import RbacBase
from membase.helper.bucket_helper import BucketOperationHelper

class CreateMembaseBucketsTests(unittest.TestCase):
    version = None
    servers = None
    input = TestInput
    log = None

    def suite_tearDown(self):
        pass

    def suite_setUp(self):
        pass

    #as part of the setup let's delete all the existing buckets
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.servers = self.input.servers
        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
        self.master = self.servers[0]
        self.bucket_storage = self.input.param("bucket_storage", 'couchstore')
        self._log_start()

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
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

    def test_default_case_sensitive_dedicated(self):
        name = 'Default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.create_bucket(bucket=name,
                               ramQuotaMB=256)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def test_less_than_minimum_memory_quota(self):
        postfix = uuid.uuid4()
        name = 'minmemquota_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=99)
                self.fail('create-bucket did not throw exception while creating a new bucket with 99 MB quota')
            #make sure it raises bucketcreateexception
            except BucketCreationException as ex:
                self.log.error(ex)

            try:
                rest.create_bucket(bucket=name, ramQuotaMB=0)

                self.fail('create-bucket did not throw exception while creating a new bucket with 0 MB quota')
            #make sure it raises bucketcreateexception
            except BucketCreationException as ex:
                self.log.info(ex)

    def test_max_memory_quota(self):
        postfix = uuid.uuid4()
        name = 'maxquota_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            info = rest.get_nodes_self()
            bucket_ram = info.memoryQuota
            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=bucket_ram)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('failed to create bucket with max ram per node')

            msg = 'failed to start up bucket with max ram per node'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def test_negative_replica(self):
        postfix = uuid.uuid4()
        name = '-1replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=256,
                                   replicaNumber=-1)
                self.fail('bucket create succeded even with a negative replica count')
            except BucketCreationException as ex:
                self.log.info(ex)

    def test_zero_replica(self):
        postfix = uuid.uuid4()
        name = 'replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=256,
                                   replicaNumber=0)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('failed to create bucket with 0 replicas')

            msg = 'failed to start up bucket with 0 replicas'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def test_one_replica(self):
        postfix = uuid.uuid4()
        name = 'replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=256)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('failed to create bucket with 1 replicas')

            msg = 'failed to start up bucket with 1 replicas'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def test_two_replica(self):
        postfix = uuid.uuid4()
        name = '2replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=256, replicaNumber=2,
                                   storageBackend=self.bucket_storage)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('failed to create bucket with 2 replicas')

            msg = 'failed to start up bucket with 2 replicas'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def test_three_replica(self):
        postfix = uuid.uuid4()
        name = '3replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=256, replicaNumber=3,
                                   storageBackend=self.bucket_storage)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('failed to create bucket with 3 replicas')

            msg = 'failed to start up bucket with 3 replicas'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def test_four_replica(self):
        postfix = uuid.uuid4()
        name = '4replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=256, replicaNumber=4)
                self.fail('created bucket with 4 replicas')
            except BucketCreationException as ex:
                self.log.info(ex)

    # Bucket name can only contain characters in range A-Z, a-z, 0-9 as well as underscore, period, dash & percent. Consult the documentation.
    def test_valid_chars(self):
        name = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.%'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=256)
            except BucketCreationException as ex:
                self.log.error(ex)
                self.fail('could not create bucket with all valid characters')

            msg = 'failed to start up bucket with all valid characters'
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    # Bucket name can only contain characters in range A-Z, a-z, 0-9 as well as underscore, period, dash & percent. Consult the documentation.
    # only done on the first server
    def test_invalid_chars(self):
        postfix = uuid.uuid4()
        for char in ['~', '!', '@', '#', '$', '^', '&', '*', '(', ')', ':', ',', ';', '"', '\'', '<', '>', '?', '/']:
            name = '{0}invalid_{1}'.format(postfix, char)
            for serverInfo in [self.servers[0]]:
                rest = RestConnection(serverInfo)
                try:
                    rest.create_bucket(bucket=name, ramQuotaMB=256, replicaNumber=2)
                    self.fail('created a bucket with invalid characters')
                except BucketCreationException as ex:
                    self.log.info(ex)

    # create maximum number of buckets (server memory // 100MB)
    # only done on the first server
    def test_max_buckets(self):
        log = logger.Logger.get_logger()
        serverInfo = self.servers[0]
        log.info('picking server : {0} as the master'.format(serverInfo))
        rest = RestConnection(serverInfo)
        info = rest.get_nodes_self()
        rest.init_cluster(username=serverInfo.rest_username,
                          password=serverInfo.rest_password)
        bucket_num = rest.get_internalSettings("maxBucketCount")
        log.info("max # buckets allow in cluster: {0}".format(bucket_num))
        bucket_ram = 100
        cluster_ram = info.memoryQuota
        max_buckets = cluster_ram / bucket_ram
        log.info("RAM setting for this cluster: {0}".format(cluster_ram))
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                                                'password': 'password'}]
        rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                                                      'roles': 'admin'}]
        RbacBase().create_user_source(testuser, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')



        for i in range(max_buckets):
            bucket_name = 'max_buckets-{0}'.format(uuid.uuid4())
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram)
            ready = BucketOperationHelper.wait_for_memcached(serverInfo, bucket_name)
            log.info("kv RAM left in cluster: {0}".format(cluster_ram - 100))
            cluster_ram -= bucket_ram
            self.assertTrue(ready, "wait_for_memcached failed")

        buckets = rest.get_buckets()
        if len(buckets) != max_buckets:
            msg = 'tried to create {0} buckets, only created {1}'\
                               .format(bucket_count, len(buckets))
            self.fail(msg)
        try:
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram)
            msg = 'bucket creation did not fail even though system was overcommited'
            self.fail(msg)
        except BucketCreationException as ex:
            log.info('\n******\nBucketCreationException was thrown as expected when\
                           we try to create {0} buckets'.format(max_buckets + 1))
        buckets = rest.get_buckets()
        if len(buckets) != max_buckets:
            msg = 'tried to create {0} buckets, only created {1}'\
                                           .format(max_buckets + 1, len(buckets))
            self.fail(msg)


    def test_valid_length(self):
        max_len = 100
        name_len = self.input.param('name_length', 100)
        name = 'a' * name_len
        master = self.servers[0]
        rest = RestConnection(master)
        try:
            rest.create_bucket(bucket=name, ramQuotaMB=256,
                               storageBackend=self.bucket_storage)
            if name_len <= max_len:
                msg = 'failed to start up bucket with valid length'
                self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)
            else:
                self.fail('Bucket with invalid length created')
        except BucketCreationException as ex:
            self.log.error(ex)
            if name_len <= max_len:
                self.fail('could not create bucket with valid length')
            else:
                self.log.info('bucket with invalid length not created as expected')
