import time
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from remote.remote_util import RemoteMachineShellConnection
from couchbase.documentgenerator import BlobGenerator
from mc_bin_client import MemcachedError


class NewUpgradeBaseTest(BaseTestCase):

    def setUp(self):
        super(NewUpgradeBaseTest, self).setUp()
        self.ops = []
        self.product = 'couchbase-server-enterprise'
        self.remote = RemoteMachineShellConnection(self.master)
        self.rest = RestConnection(self.master)
        self.info = self.remote.extract_remote_info()
        self.rest_settings = self.input.membase_settings
        self.rest_helper = RestHelper(self.rest)
        self.bucket_ddoc_map = {}
        self.bucketnames = []
        self.sleep_time = 60
        self.data_size = 1024
        self.op_types = self.input.param("op_types", 'bucket')
        if self.default_bucket:
            self.bucketnames.append(self.default_bucket_name)
        if self.sasl_buckets > 0:
            for i in range(self.sasl_buckets):
                self.bucketnames.append('bucket' + str(i))
        if self.standard_buckets > 0:
            for i in range(self.standard_buckets):
                self.bucketnames.append('standard_bucket' + str(i))

    def tearDown(self):
        super(NewUpgradeBaseTest, self).tearDown()

    def operations(self):
        self.quota = self._initialize_nodes(self.cluster, self.servers, self.disabled_consistent_view)
        if self.total_buckets > 0:
            self.bucket_size = self._get_bucket_size(self.quota, self.total_buckets)

        if self.default_bucket:
            self.cluster.create_default_bucket(self.master, self.bucket_size, self.num_replicas)
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="",
                                       num_replicas=self.num_replicas, bucket_size=self.bucket_size))

        self._create_sasl_buckets(self.master, self.sasl_buckets)
        self._create_standard_buckets(self.master, self.standard_buckets)
        time.sleep(self.sleep_time)
        if self.op_types == "data":
            self._load_doc_data_all_buckets("create")

    def _load_doc_data_all_buckets(self, op_type='create', start=0, expiry=0):
        loaded = False
        count = 0
        gen_load = BlobGenerator('upgrade-', 'upgrade-', self.data_size, start=start, end=self.num_items)
        while not loaded and count < 60:
            try :
                self._load_all_buckets(self.servers[0], gen_load, op_type, expiry)
                loaded = True
            except MemcachedError as error:
                if error.status == 134:
                    loaded = False
                    self.log.error("Memcached error 134, wait for 5 seconds and then try again")
                    count += 1
                    time.sleep(self.sleep_time)

    def verfication(self):
        for bucket in self.buckets:
            bucketname = bucket.name
            if self.op_types == "data":
                bucketinfo = self.rest.get_bucket(bucketname)
                self.log.info("bucket info :- %s" % bucketinfo)
            if self.rest_helper.bucket_exists(bucketname):
                continue
            else:
                raise Exception("bucket:- %s not found" % bucketname)
            if self.op_types == "data":
                bucketinfo = self.rest.get_bucket(bucketname)
                self.log.info("bucket info :- %s" % bucketinfo)



