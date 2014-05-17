import time
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from membase.api.exception import BucketCreationException
from membase.helper.bucket_helper import BucketOperationHelper
from couchbase.documentgenerator import BlobGenerator
from testconstants import STANDARD_BUCKET_PORT
from scripts.install import InstallerJob

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

    def test_win_specific_names(self):
        version = self._get_cb_version()
        if self._get_cb_os() != 'windows':
            self.log.warn('This test is windows specific')
            return
        try:
            self.test_banned_bucket_name()
        finally:
            try:
                self.log.info('Will check if ns_server is running')
                RestConnection(self.master)
                self.assertTrue(RestHelper(rest).is_ns_server_running(timeout_in_seconds=60))
            except:
                self._reinstall(version)
                self.fail("ns_server is not running after bucket '%s' creation" %(
                                           self.bucket_name))

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

    """ put param like -p log_message="Created bucket". If test need a cluster,
        put nodes_init=x in param to create cluster """
    def test_log_message_in_log_page(self):
        if self.log_message is not None:
            self._load_doc_data_all_buckets(data_op="create", batch_size=5000)
            serverInfo = self.servers[0]
            shell = RemoteMachineShellConnection(serverInfo)
            time.sleep(5)
            output, error = shell.execute_command("curl -v -u Administrator:password \
                            http://{0}:8091/logs | grep '{1}'".format(serverInfo.ip, self.log_message))
            if not output:
                self.log.info("message {0} is not in log".format(self.log_message))
            elif output:
                raise Exception("The message %s is in log." % self.log_message)
        else:
            raise Exception("No thing to test.  You need to put log_message=something_to_test")

    """ check for if there is a curly bracket { in the first 10 lines in diag log.
        If there is a {, possibly it is from erlang dump """
    def test_log_output_on_top(self):
        self._load_doc_data_all_buckets(data_op="create", batch_size=5000)
        serverInfo = self.servers[0]
        shell = RemoteMachineShellConnection(serverInfo)
        self.log.info("download diag into local server {0}".format(serverInfo.ip))
        ot = shell.execute_command("curl -v -u Administrator:password \
                            http://{0}:8091/diag > diag".format(serverInfo.ip))
        r_path = shell.execute_command("pwd")
        file_path = r_path[0][0] + "/diag"
        sftp = shell._ssh_client.open_sftp()
        f = sftp.open(file_path, 'r')
        # check first 10 lines in diag file, looking for this '{'
        self.log.info("check if there is any curly bracket in first 10 lines")
        count = 0
        for line in f:
            print line
            if "{" in line:
                raise Exception("Curly bracket '{' is in first 10 lines at %s diag log" % serverInfo.ip)
            count += 1
            if count == 10:
                break
        """ delete diag file after create """
        sftp.remove(file_path)
        sftp.close()

    def _get_cb_version(self):
        rest = RestConnection(self.master)
        version = rest.get_nodes_self().version
        return version[:version.rfind('-')]

    def _get_cb_os(self):
        rest = RestConnection(self.master)
        return rest.get_nodes_self().os

    def _reinstall(self, version):
        servs = self.servers[:self.nodes_init]
        params = {}
        params['num_nodes'] = len(servs)
        params['product'] = 'cb'
        params['version'] = version
        params['vbuckets'] = [self.input.param('vbuckets', 1024)]
        self.log.info("will install {0} on {1}".format(version, [s.ip for s in servs]))
        InstallerJob().parallel_install(servs, params)
        if params['product'] in ["couchbase", "couchbase-server", "cb"]:
            success = True
            for server in servs:
                success &= RemoteMachineShellConnection(server).is_couchbase_installed()
                if not success:
                    self.input.test_params["stop-on-failure"] = True
                    self.log.error("Couchbase wasn't recovered. All downstream tests will be skipped")
                    self.fail("some nodes were not install successfully!")
