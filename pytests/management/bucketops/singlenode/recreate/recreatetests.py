import unittest
import uuid
import TestInput
import logger
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper


class RecreateMembaseBuckets(unittest.TestCase):
    version = None
    servers = None
    log  = None
    input = TestInput.TestInput

    #as part of the setup let's delete all the existing buckets
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInput.TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.servers = self.input.servers
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers,test_case=self)

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers,test_case=self)

    #create bucket-load some keys-delete bucket-recreate bucket
    def test_recreate_default_on_11211(self):
        name = 'default'
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=11211)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)
            BucketOperationHelper.wait_till_memcached_is_ready_or_assert([serverInfo],11211,self)
            inserted_keys = BucketOperationHelper.load_data_or_assert(serverInfo,1,name,11211,self)
            self.assertTrue(inserted_keys,'unable to insert any key to memcached')
            self.assertTrue(BucketOperationHelper.verify_data(serverInfo.ip,inserted_keys,True,False,11211,self),
            msg='verified all the keys stored')
            #verify keys
            rest.delete_bucket(name)
            msg = 'bucket "{0}" was not deleted even after waiting for two minutes'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_deletion(name, rest, timeout_in_seconds=60), msg=msg)

            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=11211)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)
            BucketOperationHelper.wait_till_memcached_is_ready_or_assert([serverInfo],11211,self)
            #now let's recreate the bucket
            self.log.info('recreated the default bucket...')
            #loop over the keys make sure they dont exist
            self.assertTrue(BucketOperationHelper.keys_dont_exist(inserted_keys,serverInfo.ip,11211,self),
                            msg='at least one key found in the bucket')

    def test_recreate_non_default_on_11220(self):
        name = 'recreate-non-default-{0}'.format(uuid.uuid4())
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=11220)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)
            BucketOperationHelper.wait_till_memcached_is_ready_or_assert([serverInfo],11220,self)
            inserted_keys = BucketOperationHelper.load_data_or_assert(serverInfo,1,name,11220,self)
            self.assertTrue(inserted_keys,'unable to insert any key to memcached')
            self.assertTrue(BucketOperationHelper.verify_data(serverInfo.ip,inserted_keys,True,False,11220,self),
            msg='verified all the keys stored')
            #verify keys
            rest.delete_bucket(name)
            msg = 'bucket "{0}" was not deleted even after waiting for two minutes'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_deletion(name, rest, timeout_in_seconds=60), msg=msg)

            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               proxyPort=11220)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)
            BucketOperationHelper.wait_till_memcached_is_ready_or_assert([serverInfo],11220,self)
            #now let's recreate the bucket
            self.log.info('recreated the default bucket...')
            #loop over the keys make sure they dont exist
            self.assertTrue(BucketOperationHelper.keys_dont_exist(inserted_keys,serverInfo.ip,11220,self),
                            msg='at least one key found in the bucket')


    def test_recreate_non_default(self):
        name = 'new-bucket-{0}'.format(uuid.uuid4())
        for serverInfo in self.servers:
            rest = RestConnection(serverInfo)
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=1,
                               saslPassword='password',
                               proxyPort=11211)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)
            rest.delete_bucket(name)
            msg = 'bucket "{0}" was not deleted even after waiting for two minutes'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_deletion(name, rest, timeout_in_seconds=30), msg=msg)