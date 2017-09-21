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

    #as part of the setup let's delete all the existing buckets
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.servers = self.input.servers
        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
        self.master = self.servers[0]
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

    # read each server's version number and compare it to self.version
    def test_default_moxi(self):
        name = 'default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi + 1000
            rest.create_bucket(bucket=name, ramQuotaMB=200, proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def test_default_case_sensitive_dedicated(self):
        name = 'Default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               authType='sasl',
                               saslPassword='test_non_default',
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

            name = 'default'

            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=200,
                                   proxyPort=11221,
                                   authType='sasl',
                                   saslPassword='test_non_default')
                msg = "create_bucket created two buckets in different case : {0},{1}".format('default', 'Default')
                self.fail(msg)
            except BucketCreationException as ex:
            #check if 'default' and 'Default' buckets exist
                self.log.info('BucketCreationException was thrown as expected')
                self.log.info(ex.message)

    def test_default_on_non_default_port(self):
        name = 'default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi + 1000
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               proxyPort=proxyPort,
                               authType='sasl',
                               saslPassword='test_non_default')
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)


    def test_non_default_moxi(self):
        name = 'test_non_default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi + 400
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def test_default_case_sensitive_different_ports(self):
        name = 'default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi + 500
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

            try:
                name = 'DEFAULT'
                rest.create_bucket(bucket=name, ramQuotaMB=200, proxyPort=proxyPort + 1000)
                msg = "create_bucket created two buckets in different case : {0},{1}".format('default', 'DEFAULT')
                self.fail(msg)
            except BucketCreationException as ex:
                #check if 'default' and 'Default' buckets exist
                self.log.info('BucketCreationException was thrown as expected')
                self.log.info(ex.message)


    def test_non_default_case_sensitive_different_port(self):
        postfix = uuid.uuid4()
        lowercase_name = 'uppercase_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi + 500
            rest.create_bucket(bucket=lowercase_name,
                               ramQuotaMB=200,
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(lowercase_name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(lowercase_name, rest), msg=msg)

            uppercase_name = 'UPPERCASE_{0}'.format(postfix)
            try:
                rest.create_bucket(bucket=uppercase_name,
                                   ramQuotaMB=200,
                                   proxyPort=proxyPort + 1000)
                msg = "create_bucket created two buckets in different case : {0},{1}".format(lowercase_name,
                                                                                             uppercase_name)
                self.fail(msg)
            except BucketCreationException as ex:
                #check if 'default' and 'Default' buckets exist
                self.log.info('BucketCreationException was thrown as expected')
                self.log.info(ex.message)

    def test_non_default_case_sensitive_same_port(self):
        postfix = uuid.uuid4()
        name = 'uppercase_{0}'.format(postfix)
        master = self.servers[0]
        rest = RestConnection(master)
        proxyPort = rest.get_nodes_self().moxi + 100
        shell = RemoteMachineShellConnection(master)
        url = "http://%s:8091/pools/default/buckets" % master.ip
        params = "name=%s&ramQuotaMB=200&authType=none&replicaNumber=1&proxyPort=%s" \
                                                                   % (name, proxyPort)
        cmd = "curl -X POST -u Administrator:password  -d '%s' %s" % (params, url)
        output, error = shell.execute_command(cmd)
        if output and "error" in output[0]:
            self.fail("Fail to create bucket %s" % name)

        msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
        self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

        name = 'UPPERCASE{0}'.format(postfix)
        params = "name=%s&ramQuotaMB=200&authType=none&replicaNumber=1&proxyPort=%s" \
                                                                   % (name, proxyPort)
        cmd = "curl -X POST -u Administrator:password  -d '%s' %s" % (params, url)
        output, error = shell.execute_command(cmd)
        if output and 'port is already in use' not in output[0]:
            self.log.error(output)
            self.fail('create-bucket on same port failed as expected.')

    def test_less_than_minimum_memory_quota(self):
        postfix = uuid.uuid4()
        name = 'minmemquota_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=99, authType='sasl', proxyPort=proxyPort)
                self.fail('create-bucket did not throw exception while creating a new bucket with 99 MB quota')
            #make sure it raises bucketcreateexception
            except BucketCreationException as ex:
                self.log.error(ex)

            try:
                rest.create_bucket(bucket=name, ramQuotaMB=0,
                                   authType='sasl', proxyPort=proxyPort)

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
            proxyPort = rest.get_nodes_self().moxi
            bucket_ram = info.memoryQuota
            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=bucket_ram,
                                   authType='sasl',
                                   proxyPort=proxyPort)
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
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=200,
                                   replicaNumber=-1,
                                   authType='sasl',
                                   proxyPort=proxyPort)
                self.fail('bucket create succeded even with a negative replica count')
            except BucketCreationException as ex:
                self.log.info(ex)

    def test_zero_replica(self):
        postfix = uuid.uuid4()
        name = 'replica_{0}'.format(postfix)
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=200,
                                   replicaNumber=0,
                                   authType='sasl', proxyPort=proxyPort)
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
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=200, authType='sasl', proxyPort=proxyPort)
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
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=200, replicaNumber=2,
                                   authType='sasl', proxyPort=proxyPort)
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
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=200, replicaNumber=3,
                                   authType='sasl', proxyPort=proxyPort)
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
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=200, replicaNumber=4,
                                   authType='sasl', proxyPort=proxyPort)
                self.fail('created bucket with 4 replicas')
            except BucketCreationException as ex:
                self.log.info(ex)

    # Bucket name can only contain characters in range A-Z, a-z, 0-9 as well as underscore, period, dash & percent. Consult the documentation.
    def test_valid_chars(self):
        name = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.%'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            proxyPort = rest.get_nodes_self().moxi
            try:
                rest.create_bucket(bucket=name, ramQuotaMB=200,
                                   authType='sasl', proxyPort=proxyPort)
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
                proxyPort = rest.get_nodes_self().moxi
                try:
                    rest.create_bucket(bucket=name, ramQuotaMB=200, replicaNumber=2,
                                       authType='sasl', proxyPort=proxyPort)
                    self.fail('created a bucket with invalid characters')
                except BucketCreationException as ex:
                    self.log.info(ex)

    # create maximum number of buckets (server memory / 100MB)
    # only done on the first server
    def test_max_buckets(self):
        log = logger.Logger.get_logger()
        serverInfo = self.servers[0]
        log.info('picking server : {0} as the master'.format(serverInfo))
        rest = RestConnection(serverInfo)
        proxyPort = rest.get_nodes_self().moxi
        info = rest.get_nodes_self()
        rest.init_cluster(username=serverInfo.rest_username,
                          password=serverInfo.rest_password)
        rest.init_cluster_memoryQuota(memoryQuota=info.mcdMemoryReserved)
        bucket_num = rest.get_internalSettings("maxBucketCount")
        bucket_ram = 100
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                                                'password': 'password'}]
        rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                                                      'roles': 'admin'}]
        RbacBase().create_user_source(testuser, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')



        for i in range(bucket_num):
            bucket_name = 'max_buckets-{0}'.format(uuid.uuid4())
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram,
                               authType='sasl', proxyPort=proxyPort)
            ready = BucketOperationHelper.wait_for_memcached(serverInfo, bucket_name)
            self.assertTrue(ready, "wait_for_memcached failed")

        buckets = rest.get_buckets()
        if len(buckets) != bucket_num:
            msg = 'tried to create {0} buckets, only created {1}'.format(bucket_count, len(buckets))
            self.fail(msg)
        try:
            rest.create_bucket(bucket=bucket_name,
                               ramQuotaMB=bucket_ram,
                               authType='sasl', proxyPort=proxyPort)
            msg = 'bucket creation did not fail even though system was overcommited'
            self.fail(msg)
        except BucketCreationException as ex:
            self.log.info('BucketCreationException was thrown as expected when we try to create {0} buckets'.
                          format(bucket_num + 1))
        buckets = rest.get_buckets()
        if len(buckets) != bucket_num:
            msg = 'tried to create {0} buckets, only created {1}'.format(bucket_num + 1, len(buckets))
            self.fail(msg)


    def test_valid_length(self):
        max_len = 100
        name_len = self.input.param('name_length', 100)
        name = 'a' * name_len
        master = self.servers[0]
        rest = RestConnection(master)
        proxyPort = rest.get_nodes_self().moxi
        try:
            rest.create_bucket(bucket=name, ramQuotaMB=200,
                                authType='sasl', proxyPort=proxyPort)
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
