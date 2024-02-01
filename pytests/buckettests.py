import time
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, Bucket
from membase.api.exception import BucketCreationException
from membase.helper.bucket_helper import BucketOperationHelper
from couchbase_helper.documentgenerator import BlobGenerator
from testconstants import STANDARD_BUCKET_PORT
from testconstants import LINUX_COUCHBASE_BIN_PATH
from testconstants import LINUX_COUCHBASE_SAMPLE_PATH
from testconstants import WIN_COUCHBASE_BIN_PATH
from testconstants import WIN_COUCHBASE_SAMPLE_PATH_C

from scripts.install import InstallerJob
from ep_mc_bin_client import MemcachedClient, MemcachedError


class CreateBucketTests(BaseTestCase):
    def setUp(self):
        super(CreateBucketTests, self).setUp()
        self._init_parameters()

    def _init_parameters(self):
        self.bucket_name = self.input.param("bucket_name", 'default')
        self.bucket_type = self.input.param("bucket_type", 'sasl')
        self.reset_node_services = self.input.param("reset_node_services", True)
        self.bucket_size = self.quota
        self.password = 'password'
        self.server = self.master
        self.rest = RestConnection(self.server)
        self.node_version = self.rest.get_nodes_version()
        self.total_items_travel_sample = 63288
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        shell.disconnect()
        self.sample_path = LINUX_COUCHBASE_SAMPLE_PATH
        self.bin_path = LINUX_COUCHBASE_BIN_PATH
        if self.nonroot:
            self.sample_path = "/home/%s%s" % (self.master.ssh_username,
                                               LINUX_COUCHBASE_SAMPLE_PATH)
            self.bin_path = "/home/%s%s" % (self.master.ssh_username,
                                            LINUX_COUCHBASE_BIN_PATH)
        if type.lower() == 'windows':
            self.sample_path = WIN_COUCHBASE_SAMPLE_PATH_C
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
                self.rest.create_bucket(self.bucket_name, ramQuotaMB=200)
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
            shared_params = self._create_bucket_params(server=self.server, size=self.bucket_size,
                                                              replicas=self.num_replicas)
            if self.bucket_type == 'sasl':
                self.cluster.create_sasl_bucket(name=self.bucket_name, password=password, bucket_params=shared_params)
                self.buckets.append(Bucket(name=self.bucket_name, num_replicas=self.num_replicas,
                                           bucket_size=self.bucket_size, master_id=self.server))
            elif self.bucket_type == 'standard':
                self.cluster.create_standard_bucket(name=self.bucket_name, port=STANDARD_BUCKET_PORT+1,
                                                    bucket_params=shared_params)
                self.buckets.append(Bucket(name=self.bucket_name, num_replicas=self.num_replicas,
                                           bucket_size=self.bucket_size, port=STANDARD_BUCKET_PORT + 1, master_id=self.server))
            elif self.bucket_type == "memcached":
                tasks.append(self.cluster.async_create_memcached_bucket(name=self.bucket_name,
                                                                        port=STANDARD_BUCKET_PORT+1,
                                                                        bucket_params=shared_params))

                self.buckets.append(Bucket(name=self.bucket_name,
                                           num_replicas=self.num_replicas, bucket_size=self.bucket_size,
                                           port=STANDARD_BUCKET_PORT + 1, master_id=self.server, type='memcached'))
                for task in tasks:
                    task.result()
            else:
                self.log.error('Bucket type not specified')
                return
            self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(self.bucket_name, self.rest),
                            msg='failed to start up bucket with name "{0}'.format(self.bucket_name))
            if self.bucket_type == "memcached":
                mc = MemcachedClient(self.master.ip, 11210)
                mc.sasl_auth_plain(self.master.rest_username, self.master.rest_password)
                mc.bucket_select(self.bucket_name)
                for i in range(self.num_items):
                    Key = "key" + str(i)
                    try:
                        mc.set(Key, 0, 0, "value1")
                    except MemcachedError as error:
                        self.fail("Error on creating a doc")
            else:
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
            output, error = shell.execute_command("curl -g -v -u Administrator:password \
                            http://{0}:8091/logs | grep '{1}'".format(serverInfo.ip,
                                                                      self.log_message))
            if not output:
                self.log.info("message {0} is not in log".format(self.log_message))
            elif output:
                raise Exception("The message %s is in log." % self.log_message)
        else:
            raise Exception("No thing to test.  You need to put log_message=something_to_test")

    def test_travel_sample_bucket(self):
        sample = "travel-sample"
        if self.reset_node_services:
            """ reset node to set services correctly: index,kv,n1ql """
            self.rest.force_eject_node()
            if self.input.param('enable_ipv6', False):
                self.reset_and_enable_ipv6(self.master)
            status = False

            try:
                status = self.rest.init_node_services(hostname=self.master.ip,
                                            services= ["index,kv,n1ql,fts"])
                init_node = self.cluster.async_init_node(self.master,
                                                services = ["index,kv,n1ql,fts"])
            except Exception as e:
                if e:
                    print(e)
            self.sleep(10)
            self.log.info("Add new user after reset node! ")
            self.add_built_in_server_user(node=self.master)
            if status:
                self.rest.set_indexer_storage_mode(storageMode="memory_optimized")
        shell = RemoteMachineShellConnection(self.master)
        shell.execute_command("""curl -g -v -u Administrator:password \
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
            num_actual = self.get_item_count(self.master, "travel-sample")
            if int(num_actual) == self.total_items_travel_sample:
                break
        self.assertTrue(int(num_actual) == self.total_items_travel_sample,
                        "Items number expected %s, actual %s" % (
                                    self.total_items_travel_sample, num_actual))

        """ check all indexes are completed """
        index_name = []
        index_count = 10
        result = self.rest.index_tool_stats(False)

        self.log.info("check if all %s indexes built." % index_count)
        end_time_i = time.time() + 60
        while time.time() < end_time_i and len(index_name) < index_count:
            for map in result:
                if result["indexes"]:
                    for x in result["indexes"]:
                        if x["bucket"] == "travel-sample":
                            if x["progress"] < 100:
                                self.sleep(7, "waiting for indexing {0} complete"
                                           .format(x["index"]))
                                result = self.rest.index_tool_stats(False)
                            elif x["progress"] == 100:
                                if x["index"] not in index_name:
                                    index_name.append(x["index"])
                                    self.sleep(7, "waiting for other indexing complete")
                                    result = self.rest.index_tool_stats(False)
                else:
                    self.sleep(7, "waiting for indexing start")
                    result = self.rest.index_tool_stats()
        if time.time() >= end_time_i and len(index_name) < index_count:
            self.log.info("index list {0}".format(index_name))
            self.fail("some indexing may not complete")
        elif len(index_name) == index_count:
            self.log.info("travel-sample bucket is created and complete indexing")
            self.log.info("index list in travel-sample bucket: {0}"
                                       .format(index_name))
        else:
            self.log.info("There is extra index %s" % index_name)

    def test_cli_travel_sample_bucket(self):
        """ couchbase-cli does not have option to reset the node yet
            use rest to reset node to set services correctly: index,kv,n1ql """
        self.rest.force_eject_node()

        shell = RemoteMachineShellConnection(self.master)
        if self.input.param('enable_ipv6', False):
            self.reset_and_enable_ipv6(self.master)
        set_index_storage_type = " --index-storage-setting=memopt "
        options = ' --cluster-port=8091 \
                    --cluster-ramsize=1000 \
                    --cluster-index-ramsize=300 \
                    --services=data,index,query,fts %s ' % set_index_storage_type
        o, e = shell.execute_couchbase_cli(cli_command="cluster-init", options=options)
        self.assertTrue(self._check_output("SUCCESS: Cluster initialized", o),
                                           "Failed to initialize cluster")
        self.sleep(7, "wait for services up completely")

        self.log.info("Add new user after reset node! ")
        self.add_built_in_server_user(node=self.master)
        shell = RemoteMachineShellConnection(self.master)
        shell.execute_command(
            "{0}cbimport json -f sample -u Administrator -p password "
            "-c {1}:{2} -b travel-sample -m 200 -d {3}travel-sample.zip"
            .format(self.bin_path, self.master.ip, self.master.port,
                    self.sample_path))
        shell.disconnect()

        buckets = RestConnection(self.master).get_buckets()
        for bucket in buckets:
            if bucket.name != "travel-sample":
                self.fail("travel-sample bucket did not create")

        """ check for load data into travel-sample bucket """
        end_time = time.time() + 120
        while time.time() < end_time:
            self.sleep(10)
            num_actual = self.get_item_count(self.master, "travel-sample")
            if int(num_actual) == self.total_items_travel_sample:
                break
        self.assertTrue(int(num_actual) == self.total_items_travel_sample,
                        "Items number expected %s, actual %s" % (
                                self.total_items_travel_sample, num_actual))
        self.log.info("Total items %s " % num_actual)

        """ check all indexes are completed """
        index_name = []
        index_count = 10
        result = self.rest.index_tool_stats(False)
        """ check all indexes are completed """

        self.log.info("check if all %s indexes built." % index_count)
        end_time_i = time.time() + 180
        while time.time() < end_time_i and len(index_name) < index_count:
            if result["indexes"]:
                for x in result["indexes"]:
                    if x["bucket"] == "travel-sample":
                        if x["progress"] == 100 and \
                           x["index"] not in index_name:
                                index_name.append(x["index"])
                self.sleep(7, "waiting for indexing complete")
                result = self.rest.index_tool_stats(False)
            else:
                self.sleep(2, "waiting for indexing start")
                result = self.rest.index_tool_stats(False)
        if time.time() >= end_time_i and len(index_name) < index_count:
            self.log.info("index list {0}".format(index_name))
            self.fail("some indexing may not complete")
        elif len(index_name) == index_count:
            self.log.info("travel-sample bucket is created and complete indexing")
            self.log.info("index list in travel-sample bucket: {0}"
                          .format(index_name))
        else:
            self.log.info("There is extra index %s" % index_name)

    def test_cli_bucket_maxttl_setting(self):
        """ couchbase-cli does not have option to reset the node yet
            use rest to reset node to set services correctly: index,kv,n1ql """
        self.rest.force_eject_node()

        shell = RemoteMachineShellConnection(self.master)
        if self.input.param('enable_ipv6', False):
            self.reset_and_enable_ipv6(self.master)
        set_index_storage_type = " --index-storage-setting=memopt "
        options = ' --cluster-port=8091 \
                    --cluster-ramsize=300 \
                    --cluster-index-ramsize=300 \
                    --services=data,index %s ' \
                  % set_index_storage_type
        o, e = shell.execute_couchbase_cli(cli_command="cluster-init",
                                           options=options)
        self.assertEqual(o[0], 'SUCCESS: Cluster initialized')

        self.log.info("Add new user after reset node! ")
        self.add_built_in_server_user(node=self.master)
        bucket_type = self.input.param("bucket_type", "couchbase")
        options = ' --bucket=default \
                    --bucket-type={0} \
                    --bucket-ramsize=200 \
                    --max-ttl=400 \
                    --wait '.format(bucket_type)
        o, e = shell.execute_couchbase_cli(cli_command="bucket-create",
                                           options=options)
        self.assertEqual(o[0], 'SUCCESS: Bucket created')

        self.sleep(30, "Sleep before loading doc using cbimport")
        for _ in range(2):
            shell.execute_command(
                "{0}cbimport json -u Administrator -p password -f sample "
                "-c {1}:8091 -b default -d {2}travel-sample.zip"
                .format(self.bin_path, self.master.ip, self.sample_path))
            self.sleep(10, "Sleep before checking")
        shell.disconnect()

        for bucket in RestConnection(self.master).get_buckets():
            if bucket.name != "default":
                self.fail("default bucket did not get created")

        """ check for load data into travel-sample bucket """
        end_time = time.time() + 120
        num_actual = 0
        while time.time() < end_time:
            num_actual = self.get_item_count(self.master, "default")
            if int(num_actual) == self.total_items_travel_sample:
                break
            self.sleep(10, "Sleep before checking")
        else:
            self.fail("Items number expected %s, actual %s"
                      % (self.total_items_travel_sample, num_actual))
        self.sleep(400, "Waiting for docs to expire as per maxttl")
        self.expire_pager([self.master])
        self.sleep(20, "Wait for expiry_purger to run")
        num_actual = self.get_item_count(self.master, "default")
        if int(num_actual) != 0:
            self.fail("Item count is not 0 after maxttl has elapsed")
        else:
            self.log.info("SUCCESS: Item count is 0 after maxttl has elapsed")

    # Start of tests for ephemeral buckets
    #
    def test_ephemeral_buckets(self):
        eviction_policy = self.input.param("eviction_policy", 'noEviction')
        shared_params = self._create_bucket_params(server=self.server, size=100,
                                                   replicas=self.num_replicas, bucket_type='ephemeral',
                                                   eviction_policy=eviction_policy)
        # just do sasl for now, pending decision on support of non-sasl buckets in 5.0
        self.cluster.create_sasl_bucket(name=self.bucket_name, password=self.sasl_password, bucket_params=shared_params)
        self.buckets.append(Bucket(name=self.bucket_name,
                                           num_replicas=self.num_replicas,
                                           bucket_size=self.bucket_size, master_id=self.server))

        self.assertTrue(BucketOperationHelper.wait_for_bucket_creation(self.bucket_name, self.rest),
                            msg='failed to start up bucket with name "{0}'.format(self.bucket_name))
        gen_load = BlobGenerator('buckettest', 'buckettest-', self.value_size, start=0, end=self.num_items)
        self._load_all_buckets(self.server, gen_load, "create", 0)
        self.cluster.bucket_delete(self.server, self.bucket_name)
        self.assertTrue(BucketOperationHelper.wait_for_bucket_deletion(self.bucket_name, self.rest, timeout_in_seconds=60),
                            msg='bucket "{0}" was not deleted even after waiting for 30 seconds'.format(self.bucket_name))

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

    def _check_output(self, word_check, output):
        found = False
        if len(output) >= 1:
            if isinstance(word_check, list):
                for ele in word_check:
                    for x in output:
                        if ele.lower() in str(x.lower()):
                            self.log.info("Found '{0} in CLI output".format(ele))
                            found = True
                            break
            elif isinstance(word_check, str):
                for x in output:
                    if word_check.lower() in str(x.lower()):
                        self.log.info("Found '{0}' in CLI output".format(word_check))
                        found = True
                        break
            else:
                self.log.error("invalid {0}".format(word_check))
        return found
