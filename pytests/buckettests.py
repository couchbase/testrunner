from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection, Bucket
from membase.api.exception import BucketCreationException
from membase.helper.bucket_helper import BucketOperationHelper
from couchbase.documentgenerator import BlobGenerator
from testconstants import STANDARD_BUCKET_PORT

class CreateBucketTests(BaseTestCase):
    def setUp(self):
        super(CreateBucketTests, self).setUp()
        self._init_parameters()

    def _init_parameters(self):
        self.bucket_name = self.input.param("bucket_name", 'default')
        self.bucket_type = self.input.param("bucket_type", 'sasl')
        self.bucket_size = self.quota
        self.password = 'password'
        self.server = self.master
        self.rest = RestConnection(self.server)

    def tearDown(self):
        super(CreateBucketTests, self).tearDown()

    # Bucket creation with names as mentioned in MB-5844(.delete, _replicator.couch.1, _users.couch.1)
    def test_banned_bucket_name(self, password='password'):
        try:
            if self.bucket_type == 'sasl':
                self.rest.create_bucket(self.bucket_name, authType='sasl', saslPassword=password, ramQuotaMB=200)
            elif self.bucket_type == 'standard':
                self.rest.create_bucket(self.bucket_name, ramQuotaMB=200, proxyPort=STANDARD_BUCKET_PORT + 1)
            elif self.bucket_type == 'memcached':
                self.rest.create_bucket(self.bucket_name, ramQuotaMB=200, proxyPort=STANDARD_BUCKET_PORT + 1, bucketType='memcached')
            else:
                self.log.error('Bucket type not specified')
                return
            self.fail('created a bucket with invalid name {0}'.format(self.bucket_name))
        except BucketCreationException as ex:
            self.log.info(ex)

    # Bucket creation with names as mentioned in MB-5844(isasl.pw, ns_log)
    def test_valid_bucket_name(self, password='password'):
            tasks = []
            if self.bucket_type == 'sasl':
                self.cluster.create_sasl_bucket(self.server, self.bucket_name, password, self.num_replicas, self.bucket_size)
                self.buckets.append(Bucket(name=self.bucket_name, authType="sasl", saslPassword=password, num_replicas=self.num_replicas,
                                           bucket_size=self.bucket_size, master_id=self.server))
            elif self.bucket_type == 'standard':
                self.cluster.create_standard_bucket(self.server, self.bucket_name, STANDARD_BUCKET_PORT + 1, self.bucket_size, self.num_replicas)
                self.buckets.append(Bucket(name=self.bucket_name, authType=None, saslPassword=None, num_replicas=self.num_replicas,
                                           bucket_size=self.bucket_size, port=STANDARD_BUCKET_PORT + 1, master_id=self.server))
            elif self.bucket_type == "memcached":
                tasks.append(self.cluster.async_create_memcached_bucket(self.server, self.bucket_name, STANDARD_BUCKET_PORT + 1,
                                                                        self.bucket_size, self.num_replicas))
                self.buckets.append(Bucket(name=self.bucket_name, authType=None, saslPassword=None, num_replicas=self.num_replicas,
                                           bucket_size=self.bucket_size, port=STANDARD_BUCKET_PORT + 1 , master_id=self.server, type='memcached'))
                for task in tasks:
                    task.result()
            else:
                self.log.error('Bucket type not specified')
                return
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(self.bucket_name, self.rest),
                            msg='failed to start up bucket with name "{0}'.format(self.bucket_name))
            gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
            self._load_all_buckets(self.server, gen_load, "create", 0)
            self.cluster.bucket_delete(self.server, self.bucket_name)
            self.assertTrue(BucketOperationHelper.wait_for_bucket_deletion(self.bucket_name, self.rest, timeout_in_seconds=60),
                            msg='bucket "{0}" was not deleted even after waiting for 30 seconds'.format(self.bucket_name))
