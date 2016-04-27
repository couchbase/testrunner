import time
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, Bucket, RestHelper
from membase.api.exception import BucketCreationException
from membase.helper.bucket_helper import BucketOperationHelper
from couchbase_helper.documentgenerator import BlobGenerator
from testconstants import STANDARD_BUCKET_PORT
from testconstants import LINUX_COUCHBASE_BIN_PATH
from testconstants import LINUX_COUCHBASE_SAMPLE_PATH
from testconstants import WIN_COUCHBASE_BIN_PATH
from testconstants import WIN_COUCHBASE_SAMPLE_PATH
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
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        self.sample_path = LINUX_COUCHBASE_SAMPLE_PATH
        self.bin_path = LINUX_COUCHBASE_BIN_PATH
        if type.lower() == 'windows':
            self.sample_path = WIN_COUCHBASE_SAMPLE_PATH
            self.bin_path = WIN_COUCHBASE_BIN_PATH
        elif type.lower() == "mac":
            self.sample_path = MAC_COUCHBASE_SAMPLE_PATH
            self.bin_path = MAC_COUCHBASE_BIN_PATH

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
                rest = RestConnection(self.master)
                self.assertTrue(RestHelper(rest).is_ns_server_running(timeout_in_seconds=60))
            except:
                self._reinstall(version)
                self.fail("ns_server is not running after bucket '%s' creation" %(
                                           self.bucket_name))

    def test_create_bucket_used_port(self):
        ports = [25, 68, 80, 135, 139, 143, 500]
        for port in ports:
            try:
                self.cluster.create_standard_bucket(self.server, self.bucket_name + str(port), port, self.bucket_size, self.num_replicas)
            except:
                self.log.info('Error appears as expected')
                rest = RestConnection(self.master)
                self.assertTrue(RestHelper(rest).is_ns_server_running(timeout_in_seconds=60))
            else:
                raise Exception('User has to be unable to create a bucket using port %s' % port)

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

    def test_travel_sample_bucket(self):
        sample = "travel-sample"
        num_expected = self.input.param('num_items', 31569)
        """ reset node to set services correctly: index,kv,n1ql """
        self.rest.force_eject_node()
        status = False

        try:
            status = self.rest.init_node_services(hostname=self.master.ip,
                                        services= ["index,kv,n1ql"])
            init_node = self.cluster.async_init_node(self.master,
                                            services = ["index,kv,n1ql"])
        except Exception, e:
            if e:
                print e
        if status:
            shell = RemoteMachineShellConnection(self.master)
            shell.execute_command("""curl -v -u Administrator:password \
                         -X POST http://{0}:8091/sampleBuckets/install \
                      -d  '["travel-sample"]'""".format(self.master.ip))
            shell.disconnect()

        buckets = RestConnection(self.master).get_buckets()
        for bucket in buckets:
            if bucket.name != "travel-sample":
                self.fail("travel-sample bucket did not create")

        """ check for load data into travel-sample bucket """
        end_time = time.time() + 120
        while time.time() < end_time:
            self.sleep(10)
            num_actual = self.get_item_count(self.master,"travel-sample")
            if num_actual == num_expected:
                break
        self.assertTrue(num_actual == num_expected,
                        "Items number expected %s, actual %s" % (
                                                    num_expected, num_actual))

        """ check all indexes are completed """
        index_name = []
        result = self.rest.index_tool_stats()
        end_time_i = time.time() + 60
        while time.time() < end_time_i and len(index_name) < 8:
            for map in result:
                if result["indexes"]:
                    for x in result["indexes"]:
                        if x["bucket"] == "travel-sample":
                            if x["progress"] < 100:
                                self.sleep(7, "waiting for indexing {0} complete"
                                           .format(x["index"]))
                                result = self.rest.index_tool_stats()
                            elif x["progress"] == 100:
                                if x["index"] not in index_name:
                                    index_name.append(x["index"])
                                    self.sleep(7, "waiting for other indexing complete")
                                    result = self.rest.index_tool_stats()
                else:
                    self.sleep(7, "waiting for indexing start")
                    result = self.rest.index_tool_stats()
        if time.time() >= end_time_i and len(index_name) < 8:
            self.log.info("index list {0}".format(index_name))
            self.fail("some indexing may not complete")
        elif len(index_name) == 8:
            self.log.info("travel-sample bucket is created and complete indexing")
            self.log.info("index list in travel-sample bucket: {0}"
                                       .format(index_name))

    def test_cli_travel_sample_bucket(self):
        sample = "travel-sample"
        num_expected = self.input.param('num_items', 31569)
        """ couchbase-cli does not have option to reset the node yet
            use rest to reset node to set services correctly: index,kv,n1ql """
        self.rest.force_eject_node()

        shell = RemoteMachineShellConnection(self.master)
        options = '--cluster-init-username="Administrator" \
                        --cluster-init-password="password" \
                        --cluster-init-port=8091 \
                        --cluster-ramsize=300 \
                        --cluster-index-ramsize=300 \
                        --services=data,index,query'
        o, e = shell.execute_couchbase_cli(cli_command="cluster-init", options=options)
        self.assertEqual(o[0], "SUCCESS: init/edit localhost")

        shell = RemoteMachineShellConnection(self.master)
        shell.execute_command("{0}cbdocloader -u Administrator -p password \
                      -n {1} -b travel-sample -s 100 {2}travel-sample.zip" \
                   .format(self.bin_path, self.master.ip, self.sample_path))
        shell.disconnect()

        buckets = RestConnection(self.master).get_buckets()
        for bucket in buckets:
            if bucket.name != "travel-sample":
                self.fail("travel-sample bucket did not create")

        """ check for load data into travel-sample bucket """
        end_time = time.time() + 120
        while time.time() < end_time:
            self.sleep(10)
            num_actual = self.get_item_count(self.master,"travel-sample")
            if num_actual == num_expected:
                break
        self.assertTrue(num_actual == num_expected,
                        "Items number expected %s, actual %s" % (
                                                    num_expected, num_actual))

        """ check all indexes are completed """
        index_name = []
        result = self.rest.index_tool_stats()
        end_time_i = time.time() + 60
        for map in result:
            if result["indexes"]:
                for x in result["indexes"]:
                    if x["bucket"] == "travel-sample":
                        if x["progress"] == 100 and x["index"] not in index_name:
                            index_name.append(x["index"])
            else:
                self.fail("indexing failed to build")
        if len(index_name) < 8:
            self.log.info("index list {0}".format(index_name))
            self.fail("some indexing may not complete")
        elif len(index_name) == 8:
            self.log.info("travel-sample bucket is created and complete indexing")
            self.log.info("index list in travel-sample bucket: {0}"
                                       .format(index_name))

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
