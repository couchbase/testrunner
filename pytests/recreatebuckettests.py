import unittest
import uuid
import TestInput
import logger
import datetime
import time
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from security.rbac_base import RbacBase



class RecreateMembaseBuckets(unittest.TestCase):
    version = None
    servers = None
    log = None
    input = TestInput.TestInput
    
    def suite_setUp(self):
       pass

    def suite_tearDown(self):
       pass

    #as part of the setup let's delete all the existing buckets
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInput.TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        self.servers = self.input.servers
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, test_case=self)
        # Add built-in user
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', self.servers[0])
        
        # Assign user to role
        role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(self.servers[0]), 'builtin')
        
        self._log_start()

    def tearDown(self):
        BucketOperationHelper.delete_all_buckets_or_assert(self.servers, test_case=self)
        rest = RestConnection(self.servers[0])
        # Remove rbac user in teardown
        role_del = ['cbadminbucket']
        temp = RbacBase().remove_user_role(role_del, rest)

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

    #create bucket-load some keys-delete bucket-recreate bucket
    def test_default_moxi(self):
        name = 'default'
        serverInfo = self.servers[0]
        if serverInfo.ip != "":
            rest = RestConnection(serverInfo)
            replicaNumber = 1
            proxyPort = rest.get_nodes_self().moxi
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=replicaNumber,
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)
            ready = BucketOperationHelper.wait_for_memcached(serverInfo, name)
            self.assertTrue(ready, "wait_for_memcached failed")
            inserted_keys = BucketOperationHelper.load_some_data(serverInfo, 1, name)
            self.assertTrue(inserted_keys, 'unable to insert any key to memcached')
            verified = BucketOperationHelper.verify_data(serverInfo, inserted_keys, True, False, self, bucket=name)
            self.assertTrue(verified, msg='verified all the keys stored')
            #verify keys
            rest.delete_bucket(name)
            msg = 'bucket "{0}" was not deleted even after waiting for two minutes'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_deletion(name, rest, timeout_in_seconds=60), msg=msg)

            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=replicaNumber,
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)
            BucketOperationHelper.wait_for_memcached(serverInfo, name)
            #now let's recreate the bucket
            self.log.info('recreated the default bucket...')
            #loop over the keys make sure they dont exist
            self.assertTrue(BucketOperationHelper.keys_dont_exist(serverInfo, inserted_keys, name),
                            msg='at least one key found in the bucket')

    def test_default_dedicated(self):
        name = 'recreate-non-default-{0}'.format(uuid.uuid4())
        serverInfo = self.servers[0]
        if serverInfo.ip != "":
            rest = RestConnection(serverInfo)
            replicaNumber = 1
            proxyPort = rest.get_nodes_self().memcached + 2000
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=replicaNumber,
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)
            ready = BucketOperationHelper.wait_for_memcached(serverInfo, name)
            self.assertTrue(ready, "wait_for_memcached failed")
            inserted_keys = BucketOperationHelper.load_some_data(serverInfo, 1, name)
            self.assertTrue(inserted_keys, 'unable to insert any key to memcached')
            verified = BucketOperationHelper.verify_data(serverInfo, inserted_keys, True, False, self, bucket=name)
            self.assertTrue(verified, msg='verified all the keys stored')
            #verify keys
            rest.delete_bucket(name)
            msg = 'bucket "{0}" was not deleted even after waiting for two minutes'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_deletion(name, rest, timeout_in_seconds=60), msg=msg)

            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=replicaNumber,
                               proxyPort=proxyPort)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)
            ready = BucketOperationHelper.wait_for_memcached(serverInfo, name)
            self.assertTrue(ready, "wait_for_memcached failed")
            #now let's recreate the bucket
            self.log.info('recreated the default bucket...')
            #loop over the keys make sure they dont exist
            self.assertTrue(BucketOperationHelper.keys_dont_exist(serverInfo, inserted_keys, name),
                            msg='at least one key found in the bucket')


    def test_default_moxi_sasl(self):
        name = 'new-bucket-{0}'.format(uuid.uuid4())
        serverInfo = self.servers[0]
        if serverInfo.ip != "":
            rest = RestConnection(serverInfo)
            replicaNumber = 1
            proxyPort = rest.get_nodes_self().moxi
            rest.create_bucket(bucket=name,
                               ramQuotaMB=200,
                               replicaNumber=replicaNumber,
                               proxyPort=proxyPort,
                               authType="sasl",
                               saslPassword='password')
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(name, rest), msg=msg)
            ready = BucketOperationHelper.wait_for_memcached(serverInfo, name)
            self.assertTrue(ready, "wait_for_memcached failed")
            rest.delete_bucket(name)
            msg = 'bucket "{0}" was not deleted even after waiting for two minutes'.format(name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_deletion(name, rest, timeout_in_seconds=30), msg=msg)
