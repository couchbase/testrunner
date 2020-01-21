import zlib

from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from .xdcrnewbasetests import XDCRNewBaseTest, FloatingServers
from .xdcrnewbasetests import NodeHelper
from membase.api.rest_client import RestConnection
from testconstants import STANDARD_BUCKET_PORT
from remote.remote_util import RemoteMachineShellConnection
from couchbase.exceptions import NotFoundError
from couchbase.bucket import Bucket
from couchbase_helper.cluster import Cluster
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.api.exception import XDCRCheckpointException
from memcached.helper.data_helper import VBucketAwareMemcached
from security.rbac_base import RbacBase


class Lww(XDCRNewBaseTest):

    def setUp(self):
        super(Lww, self).setUp()
        self.cluster = Cluster()
        self.c1_cluster = self.get_cb_cluster_by_name('C1')
        self.c2_cluster = self.get_cb_cluster_by_name('C2')

        self.skip_ntp = self._input.param("skip_ntp", False)
        self.clean_backup = self._input.param("clean_backup", False)
        self.bucketType = self._input.param("bucket_type", "membase")
        self.evictionPolicy = self._input.param("eviction_policy", "valueOnly")

        if not self.skip_ntp:
            self._enable_ntp_and_sync()

    def tearDown(self):
        super(Lww, self).tearDown()
        if self.clean_backup:
            remote_client = RemoteMachineShellConnection(self._input.servers[6])
            command = "rm -rf /data/lww-backup"
            output, error = remote_client.execute_command(command)
            remote_client.log_command_output(output, error)
        if not self.skip_ntp:
            self._disable_ntp()

    def suite_setUp(self):
        self.log.info("*** Lww: suite_setUp() ***")

    def suite_tearDown(self):
        self.log.info("*** Lww: suite_tearDown() ***")

    def _enable_ntp_and_sync(self, nodes=[], ntp_server="0.north-america.pool.ntp.org"):
        if not nodes:
            nodes = self._input.servers
        for node in nodes:
            conn = RemoteMachineShellConnection(node)
            output, error = conn.execute_command("chkconfig ntpd on")
            conn.log_command_output(output, error)
            output, error = conn.execute_command("/etc/init.d/ntpd start")
            conn.log_command_output(output, error)
            output, error = conn.execute_command("systemctl start ntpd")
            conn.log_command_output(output, error)            
            output, error = conn.execute_command("ntpdate -q " + ntp_server)
            conn.log_command_output(output, error)

    def _disable_ntp(self):
        for node in self._input.servers:
            conn = RemoteMachineShellConnection(node)
            output, error = conn.execute_command("chkconfig ntpd off")
            conn.log_command_output(output, error)
            output, error = conn.execute_command("/etc/init.d/ntpd stop")
            conn.log_command_output(output, error)
            output, error = conn.execute_command("systemctl stop ntpd")
            conn.log_command_output(output, error)

    def _offset_wall_clock(self, cluster=None, offset_secs=0, inc=True, offset_drift=-1):
        counter = 1
        for node in cluster.get_nodes():
            conn = RemoteMachineShellConnection(node)
            output, error = conn.execute_command("date +%s")
            conn.log_command_output(output, error)
            curr_time = int(output[-1])
            if inc:
                new_time = curr_time + (offset_secs * counter)
            else:
                new_time = curr_time - (offset_secs * counter)
            output, error = conn.execute_command("date --date @" + str(new_time))
            conn.log_command_output(output, error)
            output, error = conn.execute_command("date --set='" + output[-1] + "'")
            conn.log_command_output(output, error)
            if offset_drift > 0 and counter < offset_drift:
                counter = counter + 1

    def _change_time_zone(self, cluster=None, time_zone="America/Los_Angeles"):
        for node in cluster.get_nodes():
            conn = RemoteMachineShellConnection(node)
            output, error = conn.execute_command("timedatectl set-timezone " + time_zone)
            conn.log_command_output(output, error)

    def _create_buckets(self, bucket='',
                       ramQuotaMB=1,
                       authType='none',
                       saslPassword='',
                       replicaNumber=1,
                       proxyPort=11211,
                       replica_index=1,
                       threadsNumber=3,
                       flushEnabled=1,
                       src_lww=True,
                       dst_lww=True,
                       skip_src=False,
                       skip_dst=False):
        if not skip_src:
            src_rest = RestConnection(self.c1_cluster.get_master_node())
            if src_lww:
                src_rest.create_bucket(bucket=bucket, ramQuotaMB=ramQuotaMB, authType=authType, saslPassword=saslPassword,
                                       replicaNumber=replicaNumber, proxyPort=proxyPort, bucketType=self.bucketType,
                                       replica_index=replica_index, flushEnabled=flushEnabled, evictionPolicy=self.evictionPolicy,
                                       lww=True)
            else:
                src_rest.create_bucket(bucket=bucket, ramQuotaMB=ramQuotaMB, authType=authType, saslPassword=saslPassword,
                                       replicaNumber=replicaNumber, proxyPort=proxyPort, bucketType=self.bucketType,
                                       replica_index=replica_index, flushEnabled=flushEnabled, evictionPolicy=self.evictionPolicy)
            self.c1_cluster.add_bucket(ramQuotaMB=ramQuotaMB, bucket=bucket, authType=authType,
                                       saslPassword=saslPassword, replicaNumber=replicaNumber,
                                       proxyPort=proxyPort, bucketType=self.bucketType, evictionPolicy=self.evictionPolicy)
        if not skip_dst:
            dst_rest = RestConnection(self.c2_cluster.get_master_node())
            if dst_lww:
                dst_rest.create_bucket(bucket=bucket, ramQuotaMB=ramQuotaMB, authType=authType, saslPassword=saslPassword,
                                       replicaNumber=replicaNumber, proxyPort=proxyPort, bucketType=self.bucketType,
                                       replica_index=replica_index, flushEnabled=flushEnabled, evictionPolicy=self.evictionPolicy,
                                       lww=True)
            else:
                dst_rest.create_bucket(bucket=bucket, ramQuotaMB=ramQuotaMB, authType=authType, saslPassword=saslPassword,
                                       replicaNumber=replicaNumber, proxyPort=proxyPort, bucketType=self.bucketType,
                                       replica_index=replica_index, flushEnabled=flushEnabled, evictionPolicy=self.evictionPolicy)
            self.c2_cluster.add_bucket(ramQuotaMB=ramQuotaMB, bucket=bucket, authType=authType,
                                       saslPassword=saslPassword, replicaNumber=replicaNumber,
                                       proxyPort=proxyPort, bucketType=self.bucketType, evictionPolicy=self.evictionPolicy)

    def _get_python_sdk_client(self, ip, bucket, cluster):
        try:
            role_del = [bucket]
            RbacBase().remove_user_role(role_del, RestConnection(cluster.get_master_node()))
        except Exception as ex:
            self.log.info(str(ex))
            self.assertTrue(str(ex) == str(b'"User was not found."'), str(ex))

        testuser = [{'id': bucket, 'name': bucket, 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', cluster.get_master_node())
        self.sleep(10)

        role_list = [{'id': bucket, 'name': bucket, 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(cluster.get_master_node()), 'builtin')
        self.sleep(10)

        try:
            cb = Bucket('couchbase://' + ip + '/' + bucket, password='password')
            if cb is not None:
                self.log.info("Established connection to bucket " + bucket + " on " + ip + " using python SDK")
            else:
                self.fail("Failed to connect to bucket " + bucket + " on " + ip + " using python SDK")
            return cb
        except Exception as ex:
            self.fail(str(ex))

    def _upsert(self, conn, doc_id, old_key, new_key, new_val):
        obj = conn.get(key=doc_id)
        value = obj.value
        value[new_key] = value.pop(old_key)
        value[new_key] = new_val
        conn.upsert(key=doc_id, value=value)

    def _kill_processes(self, crashed_nodes=[]):
        try:
            NodeHelper.kill_erlang(node)
        except:
            self.log.info('Could not kill erlang process on node, continuing..')

    def _start_cb_server(self, node):
        shell = RemoteMachineShellConnection(node)
        shell.start_couchbase()
        shell.disconnect()

    def _get_max_cas(self, node, bucket, vbucket_id=0):
        max_cas = 0
        conn = RemoteMachineShellConnection(node)
        command = "/opt/couchbase/bin/cbstats -u cbadminbucket -p password " + node.ip + ":11210 vbucket-details " + str(vbucket_id) + " -b " + bucket
        output, error = conn.execute_command(command)
        conn.log_command_output(output, error)
        for line in output:
            if "max_cas" in line:
                max_cas = line.split()[1]
                break
        return int(max_cas)

    def _get_vbucket_id(self, key, num_vbuckets=1024):
        vbucket_id = ((zlib.crc32(key.encode()) >> 16) & 0x7FFF) % num_vbuckets
        return vbucket_id

    def test_lww_enable(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=False, dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        src_conn.delete_bucket()
        dest_conn.delete_bucket()

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

    def test_replication_with_lww_default(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.verify_results()

    def test_replication_with_lww_sasl(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='sasl_bucket', ramQuotaMB=100, authType='sasl', saslPassword='password')
        self.assertTrue(src_conn.is_lww_enabled('sasl_bucket'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled('sasl_bucket'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.verify_results()

    def test_replication_with_lww_standard(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='standard_bucket', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled('standard_bucket'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled('standard_bucket'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.verify_results()

    def test_replication_with_lww_and_no_lww(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")


        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                            dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.verify_results()

    def test_seq_upd_on_uni_with_src_wins(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                            dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww', self.c2_cluster)
        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()
        self.sleep(10)

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)
        self.sleep(10)
        self._upsert(conn=dest_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.sleep(10)
        self._upsert(conn=dest_nolww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.sleep(10)
        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self.sleep(10)

        self.c1_cluster.resume_all_replications_by_id()
        self.sleep(10)
        self._wait_for_replication_to_catchup()

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Src doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        obj = src_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Src doc did not win using LWW")
        obj = dest_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using Rev Id")
        self.log.info("Target doc won using Rev Id as expected")

        self.verify_results(skip_verify_data=['nolww'], skip_verify_revid=['nolww'])

    def test_seq_upd_on_uni_with_dest_wins(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                            dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww', self.c2_cluster)
        self.sleep(10)

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self.sleep(10)
        self._upsert(conn=src_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.sleep(10)
        self._upsert(conn=src_nolww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.sleep(10)
        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)
        self.sleep(10)

        self.c1_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        obj = src_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Src doc did not win using Rev Id")
        obj = dest_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Src doc did not win using Rev Id")
        self.log.info("Src doc won using Rev Id as expected")

        self.verify_results(skip_verify_data=['lww', 'nolww'], skip_verify_revid=['lww'])

    def test_seq_upd_on_bi_with_src_wins(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                            dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww', self.c2_cluster)
        self.sleep(10)

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)
        self.sleep(10)
        self._upsert(conn=dest_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.sleep(10)
        self._upsert(conn=dest_nolww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.sleep(10)
        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self.sleep(10)

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Src doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        obj = dest_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using Rev Id")
        obj = src_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using Rev Id")
        self.log.info("Target doc won using Rev Id as expected")

        self.verify_results(skip_verify_data=['nolww'])

    def test_seq_upd_on_bi_with_dest_wins(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                            dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww', self.c2_cluster)
        self.sleep(10)

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self.sleep(10)
        self._upsert(conn=src_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.sleep(10)
        self._upsert(conn=src_nolww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.sleep(10)
        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)
        self.sleep(10)

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Target doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        obj = src_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Src doc did not win using Rev Id")
        obj = dest_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Src doc did not win using Rev Id")
        self.log.info("Src doc won using Rev Id as expected")

        self.verify_results(skip_verify_data=['lww', 'nolww'])

    def test_seq_add_del_on_bi_with_src_wins(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup()

        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)

        dest_lww.remove(key='lww-0')
        self.sleep(10)
        self._upsert(conn=src_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Source doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Source doc did not win using LWW")
        self.log.info("Source doc won using LWW as expected")

        self.verify_results(skip_verify_data=['lww'])

    def test_seq_add_del_on_bi_with_dest_wins(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup()

        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)

        self._upsert(conn=src_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.sleep(10)
        dest_lww.remove(key='lww-0')

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        try:
            obj = src_lww.get(key='lww-0')
            if obj:
                self.fail("Doc not deleted in src cluster using LWW")
        except NotFoundError:
            self.log.info("Doc deleted in src cluster using LWW as expected")

        try:
            obj = dest_lww.get(key='lww-0')
            if obj:
                self.fail("Doc not deleted in target cluster using LWW")
        except NotFoundError:
            self.log.info("Doc deleted in target cluster using LWW as expected")

        # TODO - figure out how to verify results in this case
        # self.verify_results(skip_verify_data=['lww'])

    def test_seq_upd_on_uni_with_lww_disabled_target_and_src_wins(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=True, dst_lww=False)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self.sleep(10)

        try:
            self.setup_xdcr()
        except Exception as e:
            self.assertTrue("Replication between buckets with different ConflictResolutionType setting is not allowed" in str(e),
                            "ConflictResolutionType mismatch message not thrown as expected")
            self.log.info("ConflictResolutionType mismatch message thrown as expected")

    def test_seq_upd_on_uni_with_lww_disabled_source_and_target_wins(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=False, dst_lww=True)
        self.assertFalse(src_conn.is_lww_enabled(), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")
        self.sleep(10)

        try:
            self.setup_xdcr()
        except Exception as e:
            self.assertTrue("Replication between buckets with different ConflictResolutionType setting is not allowed" in str(e),
                            "ConflictResolutionType mismatch message not thrown as expected")
            self.log.info("ConflictResolutionType mismatch message thrown as expected")

    def test_seq_upd_on_bi_with_lww_disabled_on_both_clusters(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=False, dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_def = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'default', self.c1_cluster)
        self.sleep(10)
        dst_def = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default', self.c2_cluster)
        self.sleep(10)

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self.sleep(10)
        self._upsert(conn=src_def, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.sleep(10)
        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)
        self.sleep(10)
        self._upsert(conn=dst_def, doc_id='lww-0', old_key='key', new_key='key2', new_val='value2')
        self.sleep(10)

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        obj = src_def.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Doc with greater rev id did not win")
        obj = dst_def.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Doc with greater rev id did not win")
        self.log.info("Doc with greater rev id won as expected")

        self.verify_results(skip_verify_data=['default'])

    def test_seq_upd_on_uni_with_src_failover(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                            dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.pause_all_replications_by_id()

        self.sleep(30)

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww', self.c2_cluster)
        self.sleep(10)

        self.c1_cluster.failover_and_rebalance_master(graceful=True, rebalance=True)

        self._upsert(conn=dest_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self._upsert(conn=dest_nolww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')

        gen = DocumentGenerator('lww', '{{"key3":"value3"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww', self.c1_cluster)

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key3':'value3'}, obj.value, "Src doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key3':'value3'}, obj.value, "Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        obj = src_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key3':'value3'}, obj.value, "Target doc did not win using Rev Id")
        obj = dest_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using Rev Id")
        self.log.info("Target doc won using Rev Id as expected")

        self.verify_results(skip_verify_data=['nolww'], skip_verify_revid=['nolww'])

    def test_seq_upd_on_uni_with_src_rebalance(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                            dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.pause_all_replications_by_id()

        self.sleep(30)

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww', self.c2_cluster)
        self.sleep(10)

        self.c1_cluster.rebalance_out_master()

        self._upsert(conn=dest_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self._upsert(conn=dest_nolww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')

        gen = DocumentGenerator('lww', '{{"key3":"value3"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww', self.c1_cluster)
        self.sleep(10)

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key3':'value3'}, obj.value, "Src doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key3':'value3'}, obj.value, "Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        obj = src_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key3':'value3'}, obj.value, "Target doc did not win using Rev Id")
        obj = dest_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using Rev Id")
        self.log.info("Target doc won using Rev Id as expected")

        self.verify_results(skip_verify_data=['nolww'], skip_verify_revid=['nolww'])

    def test_seq_add_del_on_bi_with_rebalance(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup()

        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)

        dest_lww.remove(key='lww-0')
        self.c2_cluster.rebalance_out_master()
        self._upsert(conn=src_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.c1_cluster.rebalance_out_master()

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Source doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Source doc did not win using LWW")
        self.log.info("Source doc won using LWW as expected")

        self.verify_results(skip_verify_data=['lww'])

    def test_seq_add_del_on_bi_with_failover(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup()

        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)

        dest_lww.remove(key='lww-0')
        self.c2_cluster.failover_and_rebalance_master(graceful=True, rebalance=True)
        self._upsert(conn=src_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self.c1_cluster.failover_and_rebalance_master(graceful=True, rebalance=True)

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Source doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Source doc did not win using LWW")
        self.log.info("Source doc won using LWW as expected")

        self.verify_results(skip_verify_data=['lww'])

    def test_simult_upd_on_bi(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                            dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww', self.c2_cluster)
        self.sleep(10)

        tasks = []

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        tasks += self.c1_cluster.async_load_all_buckets_from_generator(gen)

        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        tasks += self.c2_cluster.async_load_all_buckets_from_generator(gen)

        for task in tasks:
            task.result()

        #update doc at C1 thrice
        self._upsert(conn=src_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self._upsert(conn=src_lww, doc_id='lww-0', old_key='key1', new_key='key2', new_val='value2')
        self._upsert(conn=src_lww, doc_id='lww-0', old_key='key2', new_key='key3', new_val='value3')

        self._upsert(conn=src_nolww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self._upsert(conn=src_nolww, doc_id='lww-0', old_key='key1', new_key='key2', new_val='value2')
        self._upsert(conn=src_nolww, doc_id='lww-0', old_key='key2', new_key='key3', new_val='value3')

        #update doc at C2 twice
        self._upsert(conn=dest_lww, doc_id='lww-0', old_key='key2', new_key='key3', new_val='value3')
        self._upsert(conn=dest_lww, doc_id='lww-0', old_key='key3', new_key='key4', new_val='value4')

        self._upsert(conn=dest_nolww, doc_id='lww-0', old_key='key2', new_key='key3', new_val='value3')
        self._upsert(conn=dest_nolww, doc_id='lww-0', old_key='key3', new_key='key4', new_val='value4')

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key4':'value4'}, obj.value, "Target doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key4':'value4'}, obj.value, "Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        obj = dest_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key3':'value3'}, obj.value, "Src doc did not win using Rev Id")
        obj = src_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key3':'value3'}, obj.value, "Src doc did not win using Rev Id")
        self.log.info("Src doc won using Rev Id as expected")

        self.verify_results(skip_verify_data=['lww', 'nolww'])

    def test_lww_with_optimistic_threshold_change(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        src_conn.set_xdcr_param('default', 'default', 'optimisticReplicationThreshold', self._optimistic_threshold)

        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.verify_results()

    def test_lww_with_master_warmup(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        self.sleep(self._wait_timeout)

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.async_load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.async_load_all_buckets_from_generator(gen2)

        self.sleep(self._wait_timeout // 2)

        NodeHelper.wait_warmup_completed([self.c1_cluster.warmup_node(master=True)])

        self.verify_results()

    def test_lww_with_cb_restart_at_master(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        self.sleep(self._wait_timeout)

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.async_load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.async_load_all_buckets_from_generator(gen2)

        self.sleep(self._wait_timeout // 2)

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()
        self.sleep(5)
        conn.start_couchbase()
        self.wait_service_started(self.c1_cluster.get_master_node())
        self.sleep(600, "Slepping so that vBuckets are ready and to avoid \
        MemcachedError: Memcached error #1 'Not found':   for vbucket :0")
        self.verify_results()

    def test_lww_with_erlang_restart_at_master(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        self.sleep(self._wait_timeout)

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.async_load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.async_load_all_buckets_from_generator(gen2)

        self.sleep(self._wait_timeout // 2)

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.kill_erlang()
        conn.start_couchbase()
        self.wait_service_started(self.c1_cluster.get_master_node())
        self.sleep(600, "Slepping so that vBuckets are ready and to avoid \
        MemcachedError: Memcached error #1 'Not found':   for vbucket :0")
        self.verify_results()

    def test_lww_with_memcached_restart_at_master(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        self.sleep(self._wait_timeout)

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.async_load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.async_load_all_buckets_from_generator(gen2)

        self.sleep(self._wait_timeout // 2)

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.pause_memcached()
        conn.unpause_memcached()
        self.sleep(600, "Wait such that any replication happening should get completed after memcached restart.")
        self.verify_results()

    def test_seq_upd_on_bi_with_target_clock_faster(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                            dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self._offset_wall_clock(self.c2_cluster, offset_secs=3600)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww', self.c2_cluster)
        self.sleep(10)

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)
        self._upsert(conn=dest_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self._upsert(conn=dest_nolww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        obj = dest_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using Rev Id")
        obj = src_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using Rev Id")
        self.log.info("Target doc won using Rev Id as expected")

        self.verify_results(skip_verify_data=['lww', 'nolww'])

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_seq_upd_on_bi_with_src_clock_faster(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                            dst_lww=False)
        self.assertFalse(src_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on source bucket")
        self.log.info("LWW not enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(bucket='nolww'), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")

        self._offset_wall_clock(self.c1_cluster, offset_secs=3600)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww', self.c2_cluster)
        self.sleep(10)

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self._upsert(conn=src_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self._upsert(conn=src_nolww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Src doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        obj = dest_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Src doc did not win using Rev Id")
        obj = src_nolww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Src doc did not win using Rev Id")
        self.log.info("Src doc won using Rev Id as expected")

        self.verify_results(skip_verify_data=['lww', 'nolww'])

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_seq_add_del_on_bi_with_target_clock_faster(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._offset_wall_clock(self.c2_cluster, offset_secs=3600)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup()

        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)

        dest_lww.remove(key='lww-0')
        self._upsert(conn=src_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        try:
            obj = src_lww.get(key='lww-0')
            if obj:
                self.fail("Doc not deleted in src cluster using LWW")
        except NotFoundError:
            self.log.info("Doc deleted in src cluster using LWW as expected")

        try:
            obj = dest_lww.get(key='lww-0')
            if obj:
                self.fail("Doc not deleted in target cluster using LWW")
        except NotFoundError:
            self.log.info("Doc deleted in target cluster using LWW as expected")

        # TODO - figure out how to verify results in this case
        # self.verify_results(skip_verify_data=['lww'])

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_seq_del_add_on_bi_with_target_clock_faster(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.assertTrue(src_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(bucket='lww'), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._offset_wall_clock(self.c2_cluster, offset_secs=3600)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup()

        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww', self.c1_cluster)
        self.sleep(10)
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww', self.c2_cluster)
        self.sleep(10)

        self._upsert(conn=dest_lww, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        src_lww.remove(key='lww-0')

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        obj = src_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using LWW")
        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        self.verify_results(skip_verify_data=['lww'])

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_lww_with_bucket_recreate(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.c1_cluster.delete_bucket(bucket_name='default')
        self._create_buckets(bucket='default', ramQuotaMB=100, skip_dst=True)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.verify_results()

    def test_lww_while_rebalancing_node_at_src(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.async_perform_update_delete()

        task = self.c1_cluster.async_rebalance_out()
        task.result()

        FloatingServers._serverlist.append(self._input.servers[1])

        task = self.c1_cluster.async_rebalance_in()
        task.result()
        self.sleep(300)
        self.verify_results()

    def test_lww_while_failover_node_at_src(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.async_perform_update_delete()

        graceful = self._input.param("graceful", False)
        self.recoveryType = self._input.param("recoveryType", None)
        task = self.c1_cluster.async_failover(graceful=graceful)
        task.result()

        self.sleep(30)

        if self.recoveryType:
            server_nodes = src_conn.node_statuses()
            for node in server_nodes:
                if node.ip == self._input.servers[1].ip:
                    src_conn.set_recovery_type(otpNode=node.id, recoveryType=self.recoveryType)
                    self.sleep(30)
                    src_conn.add_back_node(otpNode=node.id)
            rebalance = self.cluster.async_rebalance(self.c1_cluster.get_nodes(), [], [])
            rebalance.result()
        self.sleep(300)
        self.verify_results()

    def test_lww_with_rebalance_in_and_simult_upd_del(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        task = self.c1_cluster.async_rebalance_in(num_nodes=1)

        self.async_perform_update_delete()

        task.result()

        self._wait_for_replication_to_catchup(timeout=600)

        self.verify_results()

    def test_lww_with_rebalance_out_and_simult_upd_del(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        task = self.c1_cluster.async_rebalance_out()

        self.async_perform_update_delete()

        task.result()

        FloatingServers._serverlist.append(self._input.servers[1])

        task = self.c1_cluster.async_rebalance_in()
        task.result()

        self._wait_for_replication_to_catchup(timeout=1200)

        self.verify_results()

    def test_lww_with_failover_and_simult_upd_del(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        graceful = self._input.param("graceful", False)
        self.recoveryType = self._input.param("recoveryType", None)
        task = self.c1_cluster.async_failover(graceful=graceful)

        self.async_perform_update_delete()

        task.result()

        self.sleep(30)

        if self.recoveryType:
            server_nodes = src_conn.node_statuses()
            for node in server_nodes:
                if node.ip == self._input.servers[1].ip:
                    src_conn.set_recovery_type(otpNode=node.id, recoveryType=self.recoveryType)
                    self.sleep(30)
                    src_conn.add_back_node(otpNode=node.id)
            rebalance = self.cluster.async_rebalance(self.c1_cluster.get_nodes(), [], [])
            rebalance.result()

        self._wait_for_replication_to_catchup(timeout=1200)

        self.verify_results()

    def test_mixed_mode(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=True, dst_lww=False)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(), "LWW enabled on dest bucket")
        self.log.info("LWW not enabled on dest bucket as expected")
        self.sleep(10)

        try:
            self.setup_xdcr()
        except Exception as e:
            self.assertTrue("Replication between buckets with different ConflictResolutionType setting is not allowed" in str(e),
                            "ConflictResolutionType mismatch message not thrown as expected")
            self.log.info("ConflictResolutionType mismatch message thrown as expected")

    def test_lww_with_nodes_reshuffle(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        zones = list(src_conn.get_zone_names().keys())
        source_zone = zones[0]
        target_zone = "test_lww"

        try:
            self.log.info("Current nodes in group {0} : {1}".format(source_zone,
                                                                    str(list(src_conn.get_nodes_in_zone(source_zone).keys()))))
            self.log.info("Creating new zone " + target_zone)
            src_conn.add_zone(target_zone)
            self.log.info("Moving {0} to new zone {1}".format(str(list(src_conn.get_nodes_in_zone(source_zone).keys())),
                                                              target_zone))
            src_conn.shuffle_nodes_in_zones(["{0}".format(str(list(src_conn.get_nodes_in_zone(source_zone).keys())))],
                                            source_zone, target_zone)

            gen = DocumentGenerator('lww-', '{{"age": {0}}}', range(100), start=0, end=self._num_items)
            self.c2_cluster.load_all_buckets_from_generator(gen)
            gen = DocumentGenerator('lww-', '{{"age": {0}}}', range(100), start=0, end=self._num_items)
            self.c1_cluster.load_all_buckets_from_generator(gen)

            self.c1_cluster.resume_all_replications_by_id()

            self._wait_for_replication_to_catchup(timeout=600)
        except Exception as e:
            self.log.info(e)
        finally:
            self.log.info("Moving {0} back to old zone {1}".format(str(list(src_conn.get_nodes_in_zone(source_zone).keys())),
                                                                   source_zone))
            src_conn.shuffle_nodes_in_zones(["{0}".format(str(list(src_conn.get_nodes_in_zone(source_zone).keys())))],
                                            target_zone, source_zone)
            self.log.info("Deleting new zone " + target_zone)
            src_conn.delete_zone(target_zone)

    def test_lww_with_dst_failover_and_rebalance(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.async_load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.async_load_all_buckets_from_generator(gen2)

        graceful = self._input.param("graceful", False)
        self.recoveryType = self._input.param("recoveryType", None)
        task = self.c2_cluster.async_failover(graceful=graceful)

        task.result()

        if self.recoveryType:
            server_nodes = src_conn.node_statuses()
            for node in server_nodes:
                if node.ip == self._input.servers[3].ip:
                    dest_conn.set_recovery_type(otpNode=node.id, recoveryType=self.recoveryType)
                    self.sleep(30)
                    dest_conn.add_back_node(otpNode=node.id)
            rebalance = self.cluster.async_rebalance(self.c2_cluster.get_nodes(), [], [])
            rebalance.result()

        self.sleep(60)

        self._wait_for_replication_to_catchup(timeout=600)

        self.verify_results()

    def test_lww_with_rebooting_non_master_node(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.async_load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.async_load_all_buckets_from_generator(gen2)

        rebooted_node_src = self.c1_cluster.reboot_one_node(self)
        NodeHelper.wait_node_restarted(rebooted_node_src, self, wait_time=self._wait_timeout * 4, wait_if_warmup=True)

        rebooted_node_dst = self.c2_cluster.reboot_one_node(self)
        NodeHelper.wait_node_restarted(rebooted_node_dst, self, wait_time=self._wait_timeout * 4, wait_if_warmup=True)

        self.sleep(120)

        ClusterOperationHelper.wait_for_ns_servers_or_assert([rebooted_node_dst], self, wait_if_warmup=True)
        ClusterOperationHelper.wait_for_ns_servers_or_assert([rebooted_node_src], self, wait_if_warmup=True)

        self.verify_results()

    def test_lww_with_firewall(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.async_load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.async_load_all_buckets_from_generator(gen2)

        NodeHelper.enable_firewall(self.c2_cluster.get_master_node())
        self.sleep(30)
        NodeHelper.disable_firewall(self.c2_cluster.get_master_node())

        self.sleep(30)

        self.verify_results()

    def test_lww_with_node_crash_cluster(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.async_load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.async_load_all_buckets_from_generator(gen2)

        crashed_nodes = []
        crash = self._input.param("crash", "").split('-')
        if "C1" in crash:
            crashed_nodes += self.c1_cluster.get_nodes()
            self._kill_processes(crashed_nodes)
            self.sleep(30)
        if "C2" in crash:
            crashed_nodes += self.c2_cluster.get_nodes()
            self._kill_processes(crashed_nodes)

        for crashed_node in crashed_nodes:
            self._start_cb_server(crashed_node)

        if "C1" in crash:
            NodeHelper.wait_warmup_completed(self.c1_cluster.get_nodes())
            gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
            self.c1_cluster.load_all_buckets_from_generator(gen1)

        self.async_perform_update_delete()

        if "C2" in crash:
            NodeHelper.wait_warmup_completed(self.c2_cluster.get_nodes())

        self.verify_results()

    def test_lww_with_auto_failover(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.log.info("Enabling auto failover on " + str(self.c1_cluster.get_master_node()))
        src_conn.update_autofailover_settings(enabled=True, timeout=30)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.verify_results()

    def test_lww_with_mixed_buckets(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self._create_buckets(bucket='sasl_bucket_1', ramQuotaMB=100, authType='sasl', saslPassword='password')
        self._create_buckets(bucket='sasl_bucket_2', ramQuotaMB=100, authType='sasl', saslPassword='password')
        self._create_buckets(bucket='standard_bucket_1', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='standard_bucket_2', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1)

        for bucket in self.c1_cluster.get_buckets():
            self.assertTrue(src_conn.is_lww_enabled(bucket=bucket.name), "LWW not enabled on source bucket " + str(bucket.name))
            self.log.info("LWW enabled on source bucket " + str(bucket.name) + " as expected")
            self.assertTrue(dest_conn.is_lww_enabled(bucket=bucket.name), "LWW not enabled on source bucket " + str(bucket.name))
            self.log.info("LWW enabled on source bucket " + str(bucket.name) + " as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.verify_results()

    def test_lww_with_diff_time_zones(self):
        self.c3_cluster = self.get_cb_cluster_by_name('C3')

        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())
        c3_conn = RestConnection(self.c3_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        c3_conn.create_bucket(bucket='default', ramQuotaMB=100, authType='none', saslPassword='', replicaNumber=1,
                                proxyPort=11211, replica_index=1, threadsNumber=3,
                                flushEnabled=1, lww=True)
        self.c3_cluster.add_bucket(ramQuotaMB=100, bucket='default', authType='none',
                                   saslPassword='', replicaNumber=1, proxyPort=11211,
                                   )
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")
        self.assertTrue(c3_conn.is_lww_enabled(), "LWW not enabled on c3 bucket")
        self.log.info("LWW enabled on c3 bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        self._change_time_zone(self.c2_cluster, time_zone="America/Chicago")
        self._change_time_zone(self.c3_cluster, time_zone="America/New_York")

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c3_cluster.load_all_buckets_from_generator(gen1)
        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.verify_results()

    def test_lww_with_dest_shutdown(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.async_load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.async_load_all_buckets_from_generator(gen2)

        crashed_nodes = self.c2_cluster.get_nodes()

        self._kill_processes(crashed_nodes=crashed_nodes)

        self.sleep(timeout=180)

        for crashed_node in crashed_nodes:
            self._start_cb_server(crashed_node)

        self.async_perform_update_delete()

        NodeHelper.wait_warmup_completed(crashed_nodes)

        self.verify_results()

    def test_disk_full(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self._wait_for_replication_to_catchup()

        self.verify_results()

        self.sleep(self._wait_timeout)

        zip_file = "%s.zip" % (self._input.param("file_name", "collectInfo"))
        try:
            for node in [self.src_master, self.dest_master]:
                self.shell = RemoteMachineShellConnection(node)
                self.shell.execute_cbcollect_info(zip_file)
                if self.shell.extract_remote_info().type.lower() != "windows":
                    command = "unzip %s" % (zip_file)
                    output, error = self.shell.execute_command(command)
                    self.shell.log_command_output(output, error)
                    if len(error) > 0:
                        raise Exception("unable to unzip the files. Check unzip command output for help")
                    cmd = 'grep -R "Approaching full disk warning." cbcollect_info*/'
                    output, _ = self.shell.execute_command(cmd)
                else:
                    cmd = "curl -0 http://{1}:{2}@{0}:8091/diag 2>/dev/null | grep 'Approaching full disk warning.'".format(
                                                        self.src_master.ip,
                                                        self.src_master.rest_username,
                                                        self.src_master.rest_password)
                    output, _ = self.shell.execute_command(cmd)
                self.assertNotEqual(len(output), 0, "Full disk warning not generated as expected in %s" % node.ip)
                self.log.info("Full disk warning generated as expected in %s" % node.ip)

                self.shell.delete_files(zip_file)
                self.shell.delete_files("cbcollect_info*")
        except Exception as e:
            self.log.info(e)

    def test_lww_with_checkpoint_validation(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self._wait_for_replication_to_catchup()

        self.sleep(60)

        vb0_node = None
        nodes = self.c1_cluster.get_nodes()
        ip = VBucketAwareMemcached(src_conn, 'default').vBucketMap[0].split(':')[0]
        for node in nodes:
            if ip == node.ip:
                vb0_node = node
        if not vb0_node:
            raise XDCRCheckpointException("Error determining the node containing active vb0")
        rest_con = RestConnection(vb0_node)
        repl = rest_con.get_replication_for_buckets('default', 'default')
        try:
            checkpoint_record = rest_con.get_recent_xdcr_vb_ckpt(repl['id'])
            self.log.info("Checkpoint record : {0}".format(checkpoint_record))
        except Exception as e:
            raise XDCRCheckpointException("Error retrieving last checkpoint document - {0}".format(e))

        self.verify_results()

    def test_lww_with_backup_and_restore(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")

        backup_host_conn = RemoteMachineShellConnection(self._input.servers[6])
        output, error = backup_host_conn.execute_command("cbbackupmgr config --archive /data/lww-backup --repo lww")
        backup_host_conn.log_command_output(output, error)
        output, error = backup_host_conn.execute_command("cbbackupmgr backup --archive /data/lww-backup --repo lww "
                                                         "--host couchbase://{0} --username Administrator "
                                                         "--password password".format(self._input.servers[0].ip))
        backup_host_conn.log_command_output(output, error)
        output, error = backup_host_conn.execute_command("cbbackupmgr restore --archive /data/lww-backup --repo lww "
                                                         "--host couchbase://{0} --username Administrator "
                                                         "--password password".format(self._input.servers[2].ip))
        backup_host_conn.log_command_output(output, error)

        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_lww_with_time_diff_in_src_nodes(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._offset_wall_clock(cluster=self.c1_cluster, offset_secs=300, offset_drift=3)

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self._wait_for_replication_to_catchup()

        self.verify_results()

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_lww_with_nfs(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        #test will fail if there is a problem with this permanently mounted nfs folder
        src_conn.set_data_path(data_path='/mnt/nfs/var/nfsshare/test_lww')
        dest_conn.set_data_path(data_path='/mnt/nfs/var/nfsshare/test_lww')

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_lww_enabled_with_diff_topology_and_clocks_out_of_sync(self):
        self.c3_cluster = self.get_cb_cluster_by_name('C3')

        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())
        c3_conn = RestConnection(self.c3_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        c3_conn.create_bucket(bucket='default', ramQuotaMB=100, authType='none', saslPassword='', replicaNumber=1,
                              proxyPort=11211, replica_index=1, threadsNumber=3,
                              flushEnabled=1, lww=True)
        self.c3_cluster.add_bucket(ramQuotaMB=100, bucket='default', authType='none',
                                   saslPassword='', replicaNumber=1, proxyPort=11211)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on C1 bucket")
        self.log.info("LWW enabled on C1 bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on C2 bucket")
        self.log.info("LWW enabled on C2 bucket as expected")
        self.assertTrue(c3_conn.is_lww_enabled(), "LWW not enabled on C3 bucket")
        self.log.info("LWW enabled on C3 bucket as expected")

        self._offset_wall_clock(self.c1_cluster, offset_secs=3600)
        self._offset_wall_clock(self.c2_cluster, offset_secs=7200)
        self._offset_wall_clock(self.c3_cluster, offset_secs=10800)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup()

        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()
        self.c3_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_def = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'default', self.c1_cluster)
        self.sleep(10)
        dest_def = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default', self.c2_cluster)
        self.sleep(10)
        c3_def = self._get_python_sdk_client(self.c3_cluster.get_master_node().ip, 'default', self.c3_cluster)
        self.sleep(10)

        self._upsert(conn=dest_def, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self._upsert(conn=c3_def, doc_id='lww-0', old_key='key', new_key='key2', new_val='value2')
        src_def.remove(key='lww-0')

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self.c3_cluster.resume_all_replications_by_id()

        self._wait_for_replication_to_catchup()

        obj = src_def.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "C3 doc did not win using LWW")
        obj = dest_def.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "C3 doc did not win using LWW")
        obj = c3_def.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "C3 doc did not win using LWW")
        self.log.info("C3 doc won using LWW as expected")

        conn1 = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn1.stop_couchbase()
        conn2 = RemoteMachineShellConnection(self.c2_cluster.get_master_node())
        conn2.stop_couchbase()
        conn3 = RemoteMachineShellConnection(self.c3_cluster.get_master_node())
        conn3.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn1.start_couchbase()
        conn2.start_couchbase()
        conn3.start_couchbase()
        self._wait_for_replication_to_catchup()

    def test_lww_mixed_with_diff_topology_and_clocks_out_of_sync(self):
        self.c3_cluster = self.get_cb_cluster_by_name('C3')

        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())
        c3_conn = RestConnection(self.c3_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=True, dst_lww=False)
        c3_conn.create_bucket(bucket='default', ramQuotaMB=100, authType='none', saslPassword='', replicaNumber=1,
                              proxyPort=11211, replica_index=1, threadsNumber=3,
                              flushEnabled=1, lww=True)
        self.c3_cluster.add_bucket(ramQuotaMB=100, bucket='default', authType='none',
                                   saslPassword='', replicaNumber=1, proxyPort=11211)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on C1 bucket")
        self.log.info("LWW enabled on C1 bucket as expected")
        self.assertFalse(dest_conn.is_lww_enabled(), "LWW enabled on C2 bucket")
        self.log.info("LWW not enabled on C2 bucket as expected")
        self.assertTrue(c3_conn.is_lww_enabled(), "LWW not enabled on C3 bucket")
        self.log.info("LWW enabled on C3 bucket as expected")

        self.sleep(10)

        try:
            self.setup_xdcr()
        except Exception as e:
            self.assertTrue("Replication between buckets with different ConflictResolutionType setting is not allowed" in str(e),
                            "ConflictResolutionType mismatch message not thrown as expected")
            self.log.info("ConflictResolutionType mismatch message thrown as expected")

    def test_v_topology_with_clocks_out_of_sync(self):
        self.c3_cluster = self.get_cb_cluster_by_name('C3')

        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())
        c3_conn = RestConnection(self.c3_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=True, dst_lww=True)
        c3_conn.create_bucket(bucket='default', ramQuotaMB=100, authType='none', saslPassword='', replicaNumber=1,
                              proxyPort=11211, replica_index=1, threadsNumber=3,
                              flushEnabled=1, lww=True)
        self.c3_cluster.add_bucket(ramQuotaMB=100, bucket='default', authType='none',
                                   saslPassword='', replicaNumber=1, proxyPort=11211)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on C1 bucket")
        self.log.info("LWW enabled on C1 bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on C2 bucket")
        self.log.info("LWW enabled on C2 bucket as expected")
        self.assertTrue(c3_conn.is_lww_enabled(), "LWW not enabled on C3 bucket")
        self.log.info("LWW enabled on C3 bucket as expected")

        self._offset_wall_clock(self.c1_cluster, offset_secs=3600)
        self._offset_wall_clock(self.c2_cluster, offset_secs=7200)
        self._offset_wall_clock(self.c3_cluster, offset_secs=10800)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        gen1 = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen1)
        self._wait_for_replication_to_catchup()

        gen2 = DocumentGenerator('lww', '{{"key":"value"}}', range(100), start=0, end=1)
        self.c3_cluster.load_all_buckets_from_generator(gen2)
        self._wait_for_replication_to_catchup()

        self.c1_cluster.pause_all_replications_by_id()
        self.c3_cluster.pause_all_replications_by_id()

        self.sleep(30)

        src_def = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'default', self.c1_cluster)
        self.sleep(10)
        dest_def = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default', self.c2_cluster)
        self.sleep(10)
        c3_def = self._get_python_sdk_client(self.c3_cluster.get_master_node().ip, 'default', self.c3_cluster)
        self.sleep(10)

        self._upsert(conn=c3_def, doc_id='lww-0', old_key='key', new_key='key1', new_val='value1')
        self._upsert(conn=src_def, doc_id='lww-0', old_key='key', new_key='key2', new_val='value2')

        self.c1_cluster.resume_all_replications_by_id()
        self.c3_cluster.resume_all_replications_by_id()

        self._wait_for_replication_to_catchup()

        obj = dest_def.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "C3 doc did not win using LWW")

        conn1 = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn1.stop_couchbase()
        conn2 = RemoteMachineShellConnection(self.c2_cluster.get_master_node())
        conn2.stop_couchbase()
        conn3 = RemoteMachineShellConnection(self.c3_cluster.get_master_node())
        conn3.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn1.start_couchbase()
        conn2.start_couchbase()
        conn3.start_couchbase()

    def test_hlc_active_and_replica(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, skip_dst=True)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")

        self.sleep(10)

        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_active = self._get_max_cas(node=self.c1_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_replica = self._get_max_cas(node=self._input.servers[1], bucket='default', vbucket_id=vbucket_id)

        self.log.info("max_cas_active: " + str(max_cas_active))
        self.log.info("max_cas_replica: " + str(max_cas_replica))
        self.assertTrue(not (max_cas_active ^ max_cas_replica), "HLC of active is not equal to replica")
        self.log.info("HLC of active is equal to replica")

    def test_hlc(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        max_cas_c1 = self._get_max_cas(node=self.c1_cluster.get_master_node(), bucket='default')
        max_cas_c2 = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default')
        self.log.info("max_cas C1: " + str(max_cas_c1))
        self.log.info("max_cas C2: " + str(max_cas_c2))
        self.assertTrue(not (max_cas_c1 ^ max_cas_c2), "HLC of C1 is not equal to C2")
        self.log.info("HLC of C1 is equal to C2")

    def test_hlc_target_faster(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self._offset_wall_clock(self.c2_cluster, offset_secs=900)

        self.setup_xdcr()
        self.merge_all_buckets()

        self.c1_cluster.pause_all_replications_by_id()

        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_c2_before = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()
        self.sleep(300)
        self._wait_for_replication_to_catchup(fetch_bucket_stats_by="hour")

        max_cas_c2_after = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default', self.c2_cluster)
        self.sleep(10)

        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        self.log.info("max_cas_c2_before: " + str(max_cas_c2_before))
        self.log.info("max_cas_c2_after: " + str(max_cas_c2_after))
        self.assertTrue(not (max_cas_c2_before ^ max_cas_c2_after), "HLC of C2 changed after replication")
        self.log.info("HLC of C2 did not change after replication as expected")

        conn = RemoteMachineShellConnection(self.c2_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_hlc_source_faster(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._offset_wall_clock(self.c1_cluster, offset_secs=900)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        self.c1_cluster.pause_all_replications_by_id()

        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_c2_before = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()

        max_cas_c2_after = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default', self.c2_cluster)
        self.sleep(10)

        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        self.log.info("max_cas_c2_before: " + str(max_cas_c2_before))
        self.log.info("max_cas_c2_after: " + str(max_cas_c2_after))
        self.assertTrue(not ((max_cas_c2_after + (~max_cas_c2_before +1)) >> 63 & 1), "HLC of C2 is not greater than before replication")
        self.log.info("HLC of C2 is greater than before replication as expected")

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_hlc_within_cluster_target_faster(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._offset_wall_clock(self.c2_cluster, offset_secs=900)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        self.c1_cluster.pause_all_replications_by_id()

        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_c2_before = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()
        self.sleep(300)
        self._wait_for_replication_to_catchup(fetch_bucket_stats_by="hour")

        max_cas_c2_after = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default', self.c2_cluster)
        self.sleep(10)

        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key1':'value1'}, obj.value, "Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        self.log.info("max_cas_c2_before: " + str(max_cas_c2_before))
        self.log.info("max_cas_c2_after: " + str(max_cas_c2_after))
        self.assertTrue(not (max_cas_c2_before ^ max_cas_c2_after), "HLC of C2 changed after replication")
        self.log.info("HLC of C2 did not change after replication as expected")

        self._upsert(conn=dest_lww, doc_id='lww-0', old_key='key1', new_key='key3', new_val='key3')
        max_cas_c2_after_new_mutation = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)
        self.log.info("max_cas_c2_after_new_mutation: " + str(max_cas_c2_after_new_mutation))
        self.assertTrue(not ((max_cas_c2_after_new_mutation + (~max_cas_c2_after +1)) >> 63 & 1), "HLC of C2 is not greater after new mutation")
        self.log.info("HLC of C2 is greater after new mutation as expected")

        conn = RemoteMachineShellConnection(self.c2_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_hlc_within_cluster_source_faster(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._offset_wall_clock(self.c1_cluster, offset_secs=900)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        self.c1_cluster.pause_all_replications_by_id()

        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_c2_before = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()

        max_cas_c2_after = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default', self.c2_cluster)
        self.sleep(10)

        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        self.log.info("max_cas_c2_before: " + str(max_cas_c2_before))
        self.log.info("max_cas_c2_after: " + str(max_cas_c2_after))
        self.assertTrue(not ((max_cas_c2_after + (~max_cas_c2_before +1)) >> 63 & 1), "HLC of C2 is not greater than before replication")
        self.log.info("HLC of C2 is greater than before replication as expected")

        self._upsert(conn=dest_lww, doc_id='lww-0', old_key='key2', new_key='key3', new_val='key3')
        max_cas_c2_after_new_mutation = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)
        self.log.info("max_cas_c2_after_new_mutation: " + str(max_cas_c2_after_new_mutation))
        self.assertTrue(not ((max_cas_c2_after_new_mutation + (~max_cas_c2_after +1)) >> 63 & 1), "HLC of C2 is not greater after new mutation")
        self.log.info("HLC of C2 is greater after new mutation as expected")

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_hlc_ordering_with_delay_source_faster(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self._offset_wall_clock(self.c1_cluster, offset_secs=900)

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()

        self.c1_cluster.pause_all_replications_by_id()

        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', range(100), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        hlc_c1 = self._get_max_cas(node=self.c1_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        self.sleep(timeout=1200)

        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', range(100), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        hlc_c2_1 = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        self.c1_cluster.resume_all_replications_by_id()

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default', self.c2_cluster)
        self.sleep(10)

        obj = dest_lww.get(key='lww-0')
        self.assertDictContainsSubset({'key2':'value2'}, obj.value, "Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        hlc_c2_2 = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        self.log.info("hlc_c1: " + str(hlc_c1))
        self.log.info("hlc_c2_1: " + str(hlc_c2_1))
        self.log.info("hlc_c2_2: " + str(hlc_c2_2))
        self.assertTrue(not (hlc_c2_1 ^ hlc_c2_2), "HLC of C2 changed after replication")
        self.log.info("HLC of C2 did not change after replication as expected")

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_lww_with_two_ntp_pools(self):
        self._enable_ntp_and_sync(nodes=self.c1_cluster.get_nodes(), ntp_server="0.north-america.pool.ntp.org")
        self._enable_ntp_and_sync(nodes=self.c2_cluster.get_nodes(), ntp_server="3.north-america.pool.ntp.org")

        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")
        self.assertTrue(dest_conn.is_lww_enabled(), "LWW not enabled on dest bucket")
        self.log.info("LWW enabled on dest bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()

        self.verify_results()

    def test_conflict_resolution_after_warmup(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, skip_dst=True)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")

        NodeHelper.wait_warmup_completed([self.c1_cluster.warmup_node(master=True)])

        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket after warmup")
        self.log.info("LWW enabled on source bucket after warmup as expected")

    def test_conflict_resolution_mode_with_bucket_delete_and_recreate(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, skip_dst=True)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")

        self.sleep(10)

        self.c1_cluster.delete_bucket(bucket_name='default')
        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=False, skip_dst=True)

        self.assertFalse(src_conn.is_lww_enabled(), "LWW enabled on source bucket after recreate")
        self.log.info("LWW not enabled on source bucket after recreation as expected")

    def test_conflict_resolution_mode_edit(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, skip_dst=True)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")

        self.sleep(10)

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        command = "curl -X POST -u Administrator:password " + self.c1_cluster.get_master_node().ip + \
                  ":8091/pools/default/buckets/default -d name=default -d conflictResolutionType=seqno " + \
                  "-d authType=none -d proxyPort=11212 -d ramQuotaMB=100"
        output, error = conn.execute_command(command)
        conn.log_command_output(output, error)
        self.assertTrue("Conflict resolution type not allowed in update bucket" in str(output),
                        "Expected error message not found on editing conflict resolution type")
        self.log.info("Expected error message found on editing conflict resolution type")

    def test_conflict_resolution_mode_after_swap_rebalance(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, skip_dst=True)
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket")
        self.log.info("LWW enabled on source bucket as expected")

        self.sleep(10)

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen1)

        self.c1_cluster.swap_rebalance_master()

        src_conn = RestConnection(self.c1_cluster.get_master_node())

        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket after swap rebalance")
        self.log.info("LWW enabled on source bucket after swap rebalance as expected")
