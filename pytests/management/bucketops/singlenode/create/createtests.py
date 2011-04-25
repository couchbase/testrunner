import unittest
import uuid
from TestInput import TestInput, TestInputSingleton
import logger
from membase.api.exception import BucketCreationException
from membase.api.rest_client import RestConnection
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

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(servers=self.servers, test_case=self)
        pass

    # read each server's version number and compare it to self.version
    def test_default_on_11211(self):
        name = 'default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=11211)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)


    def test_default_case_sensitive_on_11211(self):
        name = 'Default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=11211,
                               authType='sasl',
                               saslPassword='test_non_default')
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

            name = 'default'

            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=200,
                                   replicaNumber=1,
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
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=11217,
                               authType='sasl',
                               saslPassword='test_non_default')
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)


    def test_non_default(self):
        name = 'test_non_default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=11217)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

    def test_default_case_sensitive_different_port(self):
        name = 'default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=11220)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

            try:
                name = 'DEFAULT'
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=200,
                                   replicaNumber=1,
                                   proxyPort=11221)
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
            rest.create_bucket(bucket=lowercase_name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=11220)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(lowercase_name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(lowercase_name, rest), msg=msg)

            uppercase_name = 'UPPERCASE_{0}'.format(postfix)
            try:
                rest.create_bucket(bucket=uppercase_name,
                                   ramQuotaMB=200,
                                   replicaNumber=1,
                                   proxyPort=11221)
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
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=11218)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)

            self.log.info("user should not be able to create a new bucket on a an already used port")
            name = 'UPPERCASE{0}'.format(postfix)
            try:
                rest.create_bucket(bucket=name,
                                   ramQuotaMB=200,
                                   replicaNumber=1,
                                   proxyPort=11218)
                self.fail('create-bucket did not throw exception while creating a new bucket on an already used port')
            #make sure it raises bucketcreateexception
            except BucketCreationException as ex:
                self.log.error(ex)