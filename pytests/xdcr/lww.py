import zlib

from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator, SDKDataLoader
from .xdcrnewbasetests import XDCRNewBaseTest, FloatingServers
from .xdcrnewbasetests import NodeHelper
from membase.api.rest_client import RestConnection
from testconstants import STANDARD_BUCKET_PORT
from remote.remote_util import RemoteMachineShellConnection
try:
    from sdk_client3 import SDKClient
except:
    from lib.sdk_client import SDKClient

try:
    from couchbase.exceptions import DocumentNotFoundException
except:
    from couchbase.exceptions import NotFoundError as DocumentNotFoundException

# from couchbase.bucket import Bucket
from couchbase_helper.cluster import Cluster
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.api.exception import XDCRCheckpointException
from memcached.helper.data_helper import VBucketAwareMemcached
from security.rbac_base import RbacBase
from collection.collections_rest_client import CollectionsRest


class Lww(XDCRNewBaseTest):

    def setUp(self):
        super(Lww, self).setUp()
        self.c1_cluster = self.get_cb_cluster_by_name('C1')
        self.c2_cluster = self.get_cb_cluster_by_name('C2')
        self.clean_backup = self._input.param("clean_backup", False)
        self.bucketType = self._input.param("bucket_type", "membase")
        self.evictionPolicy = self._input.param("eviction_policy", "valueOnly")
        self._scope_num = self._input.param("scope_num", 2)
        self._collection_num = self._input.param("collection_num", 2)
        self.skip_ntp = self._input.param("skip_ntp", False)
        if not self.skip_ntp:
            self._enable_ntp_and_sync()

    def tearDown(self):
        super(Lww, self).tearDown()
        if self.clean_backup:
            remote_client = RemoteMachineShellConnection(self._input.servers[6])
            command = "rm -rf /data/lww-backup"
            remote_client.execute_command(command, debug=False)
        if not self.skip_ntp:
            self._disable_ntp()

    def _enable_ntp_and_sync(self, nodes=[], ntp_server="0.north-america.pool.ntp.org"):
        if not nodes:
            nodes = self._input.servers
        for node in nodes:
            conn = RemoteMachineShellConnection(node)
            os = conn.extract_remote_info().distribution_version.lower()
            if os == "oel 8":
                conn.execute_command("systemctl start chronyd", debug=False)
                conn.execute_command("systemctl enable chronyd", debug=False)
            else:
                conn.execute_command("chkconfig ntpd on", debug=False)
                conn.execute_command("/etc/init.d/ntpd start", debug=False)
                conn.execute_command("sudo systemctl start ntpd", debug=False)
                conn.execute_command("ntpdate -q " + ntp_server, debug=False)

    def _disable_ntp(self):
        for node in self._input.servers:
            conn = RemoteMachineShellConnection(node)
            os = conn.extract_remote_info().distribution_version.lower()
            if os == "oel 8":
                conn.execute_command("systemctl stop chronyd", debug=False)
                conn.execute_command("systemctl disable chronyd", debug=False)
            else:
                conn.execute_command("chkconfig ntpd off", debug=False)
                conn.execute_command("/etc/init.d/ntpd stop", debug=False)
                conn.execute_command("sudo systemctl stop ntpd", debug=False)

    def _offset_wall_clock(self, cluster=None, offset_secs=0, inc=True, offset_drift=-1):
        counter = 1
        for node in cluster.get_nodes():
            conn = RemoteMachineShellConnection(node)
            output, error = conn.execute_command("date +%s", debug=False)
            curr_time = int(output[-1])
            if inc:
                new_time = curr_time + (offset_secs * counter)
            else:
                new_time = curr_time - (offset_secs * counter)
            conn.execute_command("date --date @" + str(new_time), debug=False)
            conn.execute_command("date --set='" + output[-1] + "'", debug=False)
            if offset_drift > 0 and counter < offset_drift:
                counter = counter + 1

    def _change_time_zone(self, cluster=None, time_zone="America/Los_Angeles"):
        for node in cluster.get_nodes():
            conn = RemoteMachineShellConnection(node)
            conn.execute_command("timedatectl set-timezone " + time_zone, debug=False)

    def _create_buckets(self, bucket='',
                        ramQuotaMB=1,
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
                src_rest.create_bucket(bucket=bucket, ramQuotaMB=ramQuotaMB,
                                       replicaNumber=replicaNumber, proxyPort=proxyPort, bucketType=self.bucketType,
                                       replica_index=replica_index, flushEnabled=flushEnabled,
                                       evictionPolicy=self.evictionPolicy,
                                       lww=True)
                self.assertTrue(src_rest.is_lww_enabled(bucket), "LWW not enabled on source bucket")
                self.log.info("LWW enabled on source bucket as expected")
            else:
                src_rest.create_bucket(bucket=bucket, ramQuotaMB=ramQuotaMB,
                                       replicaNumber=replicaNumber, proxyPort=proxyPort, bucketType=self.bucketType,
                                       replica_index=replica_index, flushEnabled=flushEnabled,
                                       evictionPolicy=self.evictionPolicy)
                self.assertFalse(src_rest.is_lww_enabled(bucket), "LWW enabled on source bucket")
                self.log.info("LWW not enabled on source bucket as expected")
            self.c1_cluster.add_bucket(ramQuotaMB=ramQuotaMB, bucket=bucket,
                                       replicaNumber=replicaNumber,
                                       proxyPort=proxyPort, bucketType=self.bucketType,
                                       evictionPolicy=self.evictionPolicy)
            self._create_collections(bucket, self.c1_cluster.get_master_node())
        if not skip_dst:
            dst_rest = RestConnection(self.c2_cluster.get_master_node())
            if dst_lww:
                dst_rest.create_bucket(bucket=bucket, ramQuotaMB=ramQuotaMB,
                                       replicaNumber=replicaNumber, proxyPort=proxyPort, bucketType=self.bucketType,
                                       replica_index=replica_index, flushEnabled=flushEnabled,
                                       evictionPolicy=self.evictionPolicy,
                                       lww=True)
                self.assertTrue(dst_rest.is_lww_enabled(bucket), "LWW not enabled on dest bucket")
                self.log.info("LWW enabled on dest bucket as expected")
            else:
                dst_rest.create_bucket(bucket=bucket, ramQuotaMB=ramQuotaMB,
                                       replicaNumber=replicaNumber, proxyPort=proxyPort, bucketType=self.bucketType,
                                       replica_index=replica_index, flushEnabled=flushEnabled,
                                       evictionPolicy=self.evictionPolicy)
                self.assertFalse(dst_rest.is_lww_enabled(bucket), "LWW enabled on dest bucket")
                self.log.info("LWW not enabled on dest bucket as expected")
            self.c2_cluster.add_bucket(ramQuotaMB=ramQuotaMB, bucket=bucket,
                                       replicaNumber=replicaNumber,
                                       proxyPort=proxyPort, bucketType=self.bucketType,
                                       evictionPolicy=self.evictionPolicy)
            self._create_collections(bucket, self.c2_cluster.get_master_node())

    def _create_collections(self, bucket, node):
        if self._scope_num or self._collection_num:
            self.sleep(timeout=10, message="Waiting for bucket creation to complete")
            CollectionsRest(node).async_create_scope_collection(
                self._scope_num, self._collection_num, bucket)

    def load_cluster_buckets(self, cluster=None):
        if self._use_java_sdk:
            gen = SDKDataLoader(self._num_items, percent_create=100, key_prefix="lww-")
        else:
            gen = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        if cluster:
            cluster.load_all_buckets_from_generator(gen)
        else:
            # Load buckets on both src and target clusters
            self.c2_cluster.load_all_buckets_from_generator(gen)
            self.c1_cluster.load_all_buckets_from_generator(gen)

    def _get_python_sdk_client(self, ip, bucket):
        # try:
        #     role_del = [bucket]
        #     RbacBase().remove_user_role(role_del, RestConnection(cluster.get_master_node()))
        # except Exception as ex:
        #     self.log.info(str(ex))
        #     self.assertTrue(str(ex) == str(b'"User was not found."'), str(ex))
        #
        # testuser = [{'id': bucket, 'name': bucket, 'password': 'password'}]
        # RbacBase().create_user_source(testuser, 'builtin', cluster.get_master_node())
        # self.sleep(10)
        #
        # role_list = [{'id': bucket, 'name': bucket, 'roles': 'admin'}]
        # RbacBase().add_user_role(role_list, RestConnection(cluster.get_master_node()), 'builtin')
        # self.sleep(10)

        try:
            cb = SDKClient(scheme="couchbase", hosts=[ip], bucket=bucket)
            return cb
        except Exception as ex:
            self.fail(str(ex))

    def _upsert(self, conn, doc_id, key, val, scope="scope_1", collection="collection_1"):
        conn.upsert(doc_id, {key: val}, scope=scope, collection=collection)

    def _kill_processes(self, crashed_nodes=[]):
        try:
            for node in crashed_nodes:
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
        command = "/opt/couchbase/bin/cbstats -u cbadminbucket -p password " + node.ip + ":11210 vbucket-details " + str(
            vbucket_id) + " -b " + bucket
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

    def check_kv_exists_in_doc(self, kv, conn, doc_id="lww-0", scope="scope_1", collection="collection_1"):
        try:
            flag, cas, kvs = conn.get(doc_id, scope=scope, collection=collection)
            if not kvs:
                self.log.info("Cannot retrieve doc {0} from scope {1}, collection {2}"
                              .format(doc_id, scope, collection))
                return False
            for item in kvs.items():
                if kv == item:
                    return True
        except Exception as ex:
            self.log.error("Exception thrown while retrieving doc {0} from scope {1}, collection {2} \n {3}"
                           .format(doc_id, scope, collection, ex))
        return False

    def test_lww_enable(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())
        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=False, dst_lww=False)
        src_conn.delete_bucket()
        dest_conn.delete_bucket()
        self._create_buckets(bucket='default', ramQuotaMB=100)

    def test_replication_with_lww_default(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()
        self.load_cluster_buckets()
        self.c1_cluster.resume_all_replications_by_id()
        self.verify_results()

    def test_replication_with_lww_sasl(self):
        self._create_buckets(bucket='sasl_bucket', ramQuotaMB=100)
        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()
        self.load_cluster_buckets()
        self.c1_cluster.resume_all_replications_by_id()
        self.verify_results()

    def test_replication_with_lww_standard(self):
        self._create_buckets(bucket='standard_bucket', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()
        self.load_cluster_buckets()
        self.c1_cluster.resume_all_replications_by_id()
        self.verify_results()

    def test_replication_with_lww_and_no_lww(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                             dst_lww=False)
        self.setup_xdcr()
        self.merge_all_buckets()
        self.c1_cluster.pause_all_replications_by_id()
        self.load_cluster_buckets()
        self.c1_cluster.resume_all_replications_by_id()
        self.verify_results()

    def test_seq_upd_on_uni_with_src_wins(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                             dst_lww=False)
        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww')
        self.setup_xdcr()
        self._upsert(conn=dest_lww, doc_id='lww-0', key="key", val="value")
        self._upsert(conn=dest_nolww, doc_id='lww-0', key="key", val="value")
        self.sleep(5)
        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value1')
        self._upsert(conn=dest_nolww, doc_id='lww-0', key='key', val='value1')
        self.sleep(5)
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value2')
        self._upsert(conn=src_nolww, doc_id='lww-0', key='key', val='value2')
        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_lww):
            self.fail("Src doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key': 'value2'}, dest_lww):
            self.log.info("Src doc won using LWW as expected")
        if self.check_kv_exists_in_doc({'key': 'value2'}, dest_nolww):
            self.fail("Target doc did not win using Rev Id")
        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_nolww):
            self.log.info("Target doc won using Rev Id as expected")

    def test_seq_upd_on_uni_with_dest_wins(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                             dst_lww=False)
        self.setup_xdcr()
        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww')

        self._upsert(conn=src_lww, doc_id='lww-0', key="key", val="value")
        self._upsert(conn=src_nolww, doc_id='lww-0', key="key", val="value")
        self.sleep(5)
        self._upsert(conn=src_lww, doc_id='lww-0', key="key", val="value1")
        self._upsert(conn=src_nolww, doc_id='lww-0', key="key", val="value1")
        self.sleep(5)
        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value2')
        self._upsert(conn=dest_nolww, doc_id='lww-0', key='key', val='value2')
        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_lww):
            self.fail("Target doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key': 'value2'}, dest_lww):
            self.log.info("Target doc won using LWW as expected")
        if self.check_kv_exists_in_doc({'key': 'value2'}, dest_nolww):
            self.fail("Target doc did not win using Rev Id")
        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_nolww):
            self.log.info("Target doc won using Rev Id as expected")

    def test_seq_upd_on_bi_with_src_wins(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                             dst_lww=False)
        self.setup_xdcr()
        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww')

        self._upsert(conn=dest_lww, doc_id='lww-0', key="key", val="value")
        self._upsert(conn=dest_nolww, doc_id='lww-0', key="key", val="value")
        self.sleep(5)
        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value1')
        self._upsert(conn=dest_nolww, doc_id='lww-0', key='key', val='value1')
        self.sleep(5)
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value2')
        self._upsert(conn=src_nolww, doc_id='lww-0', key='key', val='value2')
        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key': 'value2'}, src_lww):
            self.fail("Src doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key': 'value2'}, dest_lww):
            self.log.info("Src doc won using LWW as expected")

        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_nolww):
            self.fail("Target doc did not win using Rev Id")
        if self.check_kv_exists_in_doc({'key': 'value1'}, src_nolww):
            self.fail("Target doc did not win using Rev Id")
        self.log.info("Target doc won using Rev Id as expected")

    def test_seq_upd_on_bi_with_dest_wins(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                             dst_lww=False)
        self.setup_xdcr()
        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww')

        self._upsert(conn=src_lww, doc_id='lww-0', key="key", val="value")
        self._upsert(conn=src_nolww, doc_id='lww-0', key="key", val="value")
        self.sleep(5)
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value1')
        self._upsert(conn=src_nolww, doc_id='lww-0', key='key', val='value1')
        self.sleep(5)
        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value2')
        self._upsert(conn=dest_nolww, doc_id='lww-0', key='key', val='value2')
        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key': 'value2'}, src_lww):
            self.fail("Target doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key': 'value2'}, dest_lww):
            self.fail("Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        if self.check_kv_exists_in_doc({'key': 'value1'}, src_nolww):
            self.fail("Src doc did not win using Rev Id")
        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_nolww):
            self.fail("Src doc did not win using Rev Id")
        self.log.info("Src doc won using Rev Id as expected")

    def test_seq_add_del_on_bi_with_src_wins(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.setup_xdcr()

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        self.sleep(5)
        dest_lww.remove(key='lww-0', scope='scope_1', collection='collection_1')
        self.sleep(5)
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value1')
        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key': 'value1'}, src_lww):
            self.fail("Source doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_lww):
            self.fail("Source doc did not win using LWW")
        self.log.info("Source doc won using LWW as expected")

    def test_seq_add_del_on_bi_with_dest_wins(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.setup_xdcr()

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        self._upsert(conn=src_lww, doc_id='lww-0', key="key", val="value",
                     scope='_default', collection='_default')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value1',
                     scope='_default', collection='_default')
        self._wait_for_replication_to_catchup()
        dest_lww.remove(key='lww-0')

        flag, cas, val = src_lww.get('lww-0')
        if val:
            self.fail("Doc not deleted in src cluster using LWW")
        flag, cas, val = dest_lww.get('lww-0')
        if val:
            self.fail("Doc not deleted in target cluster using LWW")

    def test_seq_upd_on_uni_with_lww_disabled_target_and_src_wins(self):
        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=True, dst_lww=False)
        try:
            self.setup_xdcr()
        except Exception as e:
            self.assertTrue(
                "Replication between buckets with different ConflictResolutionType setting is not allowed" in str(e),
                "ConflictResolutionType mismatch message not thrown as expected")
            self.log.info("ConflictResolutionType mismatch message thrown as expected")

    def test_seq_upd_on_uni_with_lww_disabled_source_and_target_wins(self):
        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=False, dst_lww=True)
        try:
            self.setup_xdcr()
        except Exception as e:
            self.assertTrue(
                "Replication between buckets with different ConflictResolutionType setting is not allowed" in str(e),
                "ConflictResolutionType mismatch message not thrown as expected")
            self.log.info("ConflictResolutionType mismatch message thrown as expected")

    def test_seq_upd_on_bi_with_lww_disabled_on_both_clusters(self):
        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=False, dst_lww=False)
        self.setup_xdcr()

        src_def = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'default')
        dst_def = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default')

        self._upsert(conn=src_def, doc_id='lww-0', key='key', val='value')
        self._upsert(conn=src_def, doc_id='lww-0', key='key', val='value1')
        self._upsert(conn=dst_def, doc_id='lww-0', key='key', val='value')
        self._upsert(conn=dst_def, doc_id='lww-0', key='key', val='value2')
        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key': 'value2'}, src_def):
            self.fail("Doc with greater rev id did not win")
        if self.check_kv_exists_in_doc({'key': 'value2'}, dst_def):
            self.fail("Doc with greater rev id did not win")
        self.log.info("Doc with greater rev id won as expected")

    def test_seq_upd_on_uni_with_src_failover(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                             dst_lww=False)
        self.setup_xdcr()

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww')
        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value')
        self._upsert(conn=dest_nolww, doc_id='lww-0', key='key', val='value')

        self.c1_cluster.failover_and_rebalance_master(graceful=True, rebalance=True)
        self._upsert(conn=dest_lww, doc_id='lww-0', key='key1', val='value1')
        self._upsert(conn=dest_nolww, doc_id='lww-0', key='key1', val='value1')

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww')
        self._upsert(conn=src_lww, doc_id='lww-0', key='key3', val='value3')
        self._upsert(conn=src_nolww, doc_id='lww-0', key='key3', val='value3')

        self.c1_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key3': 'value3'}, src_lww):
            self.fail("Src doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key3': 'value3'}, dest_lww):
            self.fail("Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        if self.check_kv_exists_in_doc({'key3': 'value3'}, src_nolww):
            self.fail("Target doc did not win using Rev Id")
        if self.check_kv_exists_in_doc({'key1': 'value1'}, dest_nolww):
            self.fail("Target doc did not win using Rev Id")
        self.log.info("Target doc won using Rev Id as expected")

    def test_seq_upd_on_uni_with_src_rebalance(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                             dst_lww=False)
        self.setup_xdcr()

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww')

        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value')
        self._upsert(conn=dest_nolww, doc_id='lww-0', key='key', val='value')

        self.c1_cluster.pause_all_replications_by_id()
        self.sleep(30)
        self.c1_cluster.rebalance_out_master()
        self.sleep(30)
        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value1')
        self._upsert(conn=dest_nolww, doc_id='lww-0', key='key', val='value1')

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww')
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value3')
        self._upsert(conn=src_nolww, doc_id='lww-0', key='key', val='value3')

        self.c1_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key': 'value3'}, dest_lww):
            self.fail("Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        if self.check_kv_exists_in_doc({'key': 'value3'}, src_nolww):
            self.fail("Target doc did not win using Rev Id")

        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_nolww):
            self.fail("Target doc did not win using Rev Id")
        self.log.info("Target doc won using Rev Id as expected")

    def test_seq_add_del_on_bi_with_rebalance(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.setup_xdcr()

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value')

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_lww.remove(key='lww-0', scope='scope_1', collection='collection1')
        self.c2_cluster.rebalance_out_master()
        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value1')
        self.c1_cluster.rebalance_out_master()
        self._wait_for_replication_to_catchup()

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')

        if self.check_kv_exists_in_doc({'key': 'value1'}, src_lww):
            self.fail("Source doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key1': 'value1'}, dest_lww):
            self.fail("Source doc did not win using LWW")
        self.log.info("Source doc won using LWW as expected")

        self.verify_results(skip_verify_data=['lww'])

    def test_seq_add_del_on_bi_with_failover(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self.setup_xdcr()

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value')

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_lww.remove(key='lww-0', scope='scope_1', collection='collection_1')
        self.c2_cluster.failover_and_rebalance_master(graceful=True, rebalance=True)
        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value1')
        self.c1_cluster.failover_and_rebalance_master(graceful=True, rebalance=True)
        self._wait_for_replication_to_catchup()
        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')

        if self.check_kv_exists_in_doc({'key': 'value1'}, src_lww):
            self.fail("Source doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_lww):
            self.fail("Source doc did not win using LWW")
        self.log.info("Source doc won using LWW as expected")

        self.verify_results(skip_verify_data=['lww'])

    def test_simult_upd_on_bi(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                             dst_lww=False)
        self.setup_xdcr()

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww')

        tasks = []
        gen = DocumentGenerator('lww', '{{"key":"value"}}', list(range(100)), start=0, end=1)
        tasks += self.c1_cluster.async_load_all_buckets_from_generator(gen)
        gen = DocumentGenerator('lww', '{{"key":"value"}}', list(range(100)), start=0, end=1)
        tasks += self.c2_cluster.async_load_all_buckets_from_generator(gen)
        for task in tasks:
            task.result()

        # update doc at C1 thrice
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value1')
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value2')
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value3')

        self._upsert(conn=src_nolww, doc_id='lww-0', key='key', val='value1')
        self._upsert(conn=src_nolww, doc_id='lww-0', key='key', val='value2')
        self._upsert(conn=src_nolww, doc_id='lww-0', key='key', val='value3')

        # update doc at C2 twice
        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value4')
        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value5')

        self._upsert(conn=dest_nolww, doc_id='lww-0', key='key', val='value4')
        self._upsert(conn=dest_nolww, doc_id='lww-0', key='key', val='value5')

        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key': 'value4'}, src_lww):
            self.fail("Target doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key': 'value4'}, dest_lww):
            self.fail("Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        if self.check_kv_exists_in_doc({'key': 'value3'}, dest_nolww):
            self.fail("Src doc did not win using Rev Id")
        if self.check_kv_exists_in_doc({'key3': 'value3'}, src_nolww):
            self.fail("Src doc did not win using Rev Id")
        self.log.info("Src doc won using Rev Id as expected")

        self.verify_results(skip_verify_data=['lww', 'nolww'])

    def test_lww_with_optimistic_threshold_change(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()
        src_conn.set_xdcr_param('default', 'default', 'optimisticReplicationThreshold', self._optimistic_threshold)
        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)
        self.verify_results()

    def test_lww_with_master_warmup(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()
        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.async_load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.async_load_all_buckets_from_generator(gen2)
        self.sleep(self._wait_timeout // 2)
        NodeHelper.wait_warmup_completed([self.c1_cluster.warmup_node(master=True)])
        self.verify_results()

    def test_lww_with_cb_restart_at_master(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

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
        self.sleep(600, "Sleeping so that vBuckets are ready and to avoid \
        MemcachedError: Memcached error #1 'Not found':   for vbucket :0")
        self.verify_results()

    def test_lww_with_erlang_restart_at_master(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.async_load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.async_load_all_buckets_from_generator(gen2)

        self.sleep(self._wait_timeout // 2)

        self._kill_processes([self.c1_cluster.get_master_node()])
        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.start_couchbase()
        self.wait_service_started(self.c1_cluster.get_master_node())
        self.sleep(600, "Sleeping so that vBuckets are ready and to avoid \
        MemcachedError: Memcached error #1 'Not found':   for vbucket :0")
        self.verify_results()

    def test_lww_with_memcached_restart_at_master(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()
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
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                             dst_lww=False)
        self._offset_wall_clock(self.c2_cluster, offset_secs=3600)
        self.sleep(10)
        self.setup_xdcr()

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww')

        gen = DocumentGenerator('lww', '{{"key":"value"}}', list(range(100)), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)
        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value1')
        self._upsert(conn=dest_nolww, doc_id='lww-0', key='key', val='value1')
        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', list(range(100)), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key': 'value1'}, src_lww):
            self.fail("Target doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_lww):
            self.fail("Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_nolww):
            self.fail("Target doc did not win using Rev Id")
        if self.check_kv_exists_in_doc({'key': 'value1'}, src_nolww):
            self.fail("Target doc did not win using Rev Id")
        self.log.info("Target doc won using Rev Id as expected")

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_seq_upd_on_bi_with_src_clock_faster(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='nolww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1, src_lww=False,
                             dst_lww=False)

        self._offset_wall_clock(self.c1_cluster, offset_secs=3600)
        self.sleep(10)
        self.setup_xdcr()

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        src_nolww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'nolww')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_nolww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'nolww')

        gen = DocumentGenerator('lww', '{{"key":"value"}}', list(range(100)), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)
        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value1')
        self._upsert(conn=src_nolww, doc_id='lww-0', key='key', val='value1')
        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', list(range(100)), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)
        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key': 'value1'}, src_lww):
            self.fail("Src doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_lww):
            self.fail("Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        if self.check_kv_exists_in_doc({'key': 'value1'}, dest_nolww):
            self.fail("Src doc did not win using Rev Id")
        if self.check_kv_exists_in_doc({'key': 'value1'}, src_nolww):
            self.fail("Src doc did not win using Rev Id")
        self.log.info("Src doc won using Rev Id as expected")

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_seq_add_del_on_bi_with_target_clock_faster(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._offset_wall_clock(self.c2_cluster, offset_secs=3600)
        self.sleep(10)
        self.setup_xdcr()

        gen = DocumentGenerator('lww', '{{"key":"value"}}', list(range(100)), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.pause_all_replications_by_id()
        self.c2_cluster.pause_all_replications_by_id()
        self.sleep(30)

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')
        dest_lww.remove(key='lww-0')
        self._upsert(conn=src_lww, doc_id='lww-0', key='key1', val='value1',
                     scope='_default', collection='_default')

        self.c1_cluster.resume_all_replications_by_id()
        self.c2_cluster.resume_all_replications_by_id()
        self._wait_for_replication_to_catchup()

        flag, cas, val = src_lww.get('lww-0')
        if val:
            self.fail("Doc not deleted in src cluster using LWW")
        flag, cas, val = dest_lww.get('lww-0')
        if val:
            self.fail("Doc not deleted in target cluster using LWW")

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_seq_del_add_on_bi_with_target_clock_faster(self):
        self._create_buckets(bucket='lww', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._offset_wall_clock(self.c2_cluster, offset_secs=3600)
        self.sleep(10)
        self.setup_xdcr()

        src_lww = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'lww')
        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'lww')

        self._upsert(conn=src_lww, doc_id='lww-0', key='key', val='value')
        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value1')
        src_lww.remove(key='lww-0', scope='scope_1', collection='collection_1')
        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key1': 'value1'}, src_lww):
            self.fail("Target doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key1': 'value1'}, dest_lww):
            self.fail("Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_lww_with_bucket_recreate(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.c1_cluster.delete_bucket(bucket_name='default')
        self._create_buckets(bucket='default', ramQuotaMB=100, skip_dst=True)
        self.setup_xdcr()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.verify_results()

    def test_lww_while_rebalancing_node_at_src(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.async_perform_update_delete()

        task = self.c1_cluster.async_rebalance_out()
        task.result()

        FloatingServers._serverlist.append(self._input.servers[1])

        task = self.c1_cluster.async_rebalance_in()
        task.result()
        self.sleep(300)
        self.verify_results()

    def test_lww_while_failover_node_at_src(self):
        self.cluster = Cluster()
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)
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
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        task = self.c1_cluster.async_rebalance_in(num_nodes=1)
        self.async_perform_update_delete()
        task.result()

        self._wait_for_replication_to_catchup(timeout=600)
        self.verify_results()

    def test_lww_with_rebalance_out_and_simult_upd_del(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        task = self.c1_cluster.async_rebalance_out()

        self.async_perform_update_delete()

        task.result()

        FloatingServers._serverlist.append(self._input.servers[1])

        task = self.c1_cluster.async_rebalance_in()
        task.result()

        self._wait_for_replication_to_catchup(timeout=1200)

        self.verify_results()

    def test_lww_with_failover_and_simult_upd_del(self):
        self.cluster = Cluster()
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

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
        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=True, dst_lww=False)
        try:
            self.setup_xdcr()
        except Exception as e:
            self.assertTrue(
                "Replication between buckets with different ConflictResolutionType setting is not allowed" in str(e),
                "ConflictResolutionType mismatch message not thrown as expected")
            self.log.info("ConflictResolutionType mismatch message thrown as expected")

    def test_lww_with_nodes_reshuffle(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()
        self.c1_cluster.pause_all_replications_by_id()

        zones = list(src_conn.get_zone_names().keys())
        source_zone = zones[0]
        target_zone = "test_lww"

        try:
            self.log.info("Current nodes in group {0} : {1}".format(source_zone,
                                                                    str(list(src_conn.get_nodes_in_zone(
                                                                        source_zone).keys()))))
            self.log.info("Creating new zone " + target_zone)
            src_conn.add_zone(target_zone)
            self.log.info("Moving {0} to new zone {1}".format(str(list(src_conn.get_nodes_in_zone(source_zone).keys())),
                                                              target_zone))
            src_conn.shuffle_nodes_in_zones(["{0}".format(str(list(src_conn.get_nodes_in_zone(source_zone).keys())))],
                                            source_zone, target_zone)

            gen = DocumentGenerator('lww-', '{{"age": {0}}}', list(range(100)), start=0, end=self._num_items)
            self.c2_cluster.load_all_buckets_from_generator(gen)
            gen = DocumentGenerator('lww-', '{{"age": {0}}}', list(range(100)), start=0, end=self._num_items)
            self.c1_cluster.load_all_buckets_from_generator(gen)

            self.c1_cluster.resume_all_replications_by_id()

            self._wait_for_replication_to_catchup(timeout=600)
        except Exception as e:
            self.log.info(e)
        finally:
            self.log.info(
                "Moving {0} back to old zone {1}".format(str(list(src_conn.get_nodes_in_zone(source_zone).keys())),
                                                         source_zone))
            src_conn.shuffle_nodes_in_zones(["{0}".format(str(list(src_conn.get_nodes_in_zone(source_zone).keys())))],
                                            target_zone, source_zone)
            self.log.info("Deleting new zone " + target_zone)
            src_conn.delete_zone(target_zone)

    def test_lww_with_dst_failover_and_rebalance(self):
        self.cluster = Cluster()
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        dest_conn = RestConnection(self.c2_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

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
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

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
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

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
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

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
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.log.info("Enabling auto failover on " + str(self.c1_cluster.get_master_node()))
        src_conn.update_autofailover_settings(enabled=True, timeout=30)
        self.sleep(10)
        self.setup_xdcr()
        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)
        self.verify_results()

    def test_lww_with_mixed_buckets(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self._create_buckets(bucket='sasl_bucket_1', ramQuotaMB=100)
        self._create_buckets(bucket='sasl_bucket_2', ramQuotaMB=100)
        self._create_buckets(bucket='standard_bucket_1', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT)
        self._create_buckets(bucket='standard_bucket_2', ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + 1)
        self.sleep(10)
        self.setup_xdcr()
        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)
        self.verify_results()

    def test_lww_with_diff_time_zones(self):
        self.c3_cluster = self.get_cb_cluster_by_name('C3')

        c3_conn = RestConnection(self.c3_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        c3_conn.create_bucket(bucket='default', ramQuotaMB=100, replicaNumber=1,
                              proxyPort=11211, replica_index=1, threadsNumber=3,
                              flushEnabled=1, lww=True)
        self.c3_cluster.add_bucket(ramQuotaMB=100, bucket='default',
                                   replicaNumber=1, proxyPort=11211,
                                   )
        self.assertTrue(c3_conn.is_lww_enabled(), "LWW not enabled on c3 bucket")
        self.log.info("LWW enabled on c3 bucket as expected")

        self.sleep(10)

        self.setup_xdcr()
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
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

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
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()
        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)
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
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()
        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)
        self._wait_for_replication_to_catchup()
        self.sleep(60)

        vb0_node = None
        nodes = self.c1_cluster.get_nodes()
        src_conn = RestConnection(self.c1_cluster.get_master_node())
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
        self._create_buckets(bucket='default', ramQuotaMB=100)

        backup_host_conn = RemoteMachineShellConnection(self._input.servers[6])
        backup_host_conn.execute_command("cbbackupmgr config --archive /data/lww-backup --repo lww", debug=False)
        backup_host_conn.execute_command("cbbackupmgr backup --archive /data/lww-backup --repo lww "
                                         "--host couchbase://{0} --username Administrator "
                                         "--password password".format(self._input.servers[0].ip), debug=False)
        backup_host_conn.execute_command("cbbackupmgr restore --archive /data/lww-backup --repo lww "
                                         "--host couchbase://{0} --username Administrator "
                                         "--password password".format(self._input.servers[2].ip), debug=False)
        self.sleep(10)
        self.setup_xdcr()
        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)
        self._wait_for_replication_to_catchup()
        self.verify_results()

    def test_lww_with_time_diff_in_src_nodes(self):
        self._offset_wall_clock(cluster=self.c1_cluster, offset_secs=300, offset_drift=3)
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()
        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)
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

        # test will fail if there is a problem with this permanently mounted nfs folder
        src_conn.set_data_path(data_path='/mnt/nfs/var/nfsshare/test_lww')
        dest_conn.set_data_path(data_path='/mnt/nfs/var/nfsshare/test_lww')

        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)
        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self._wait_for_replication_to_catchup()

        self.verify_results()

    def test_lww_enabled_with_diff_topology_and_clocks_out_of_sync(self):
        self.c3_cluster = self.get_cb_cluster_by_name('C3')
        c3_conn = RestConnection(self.c3_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100)
        c3_conn.create_bucket(bucket='default', ramQuotaMB=100, replicaNumber=1,
                              proxyPort=11211, replica_index=1, threadsNumber=3,
                              flushEnabled=1, lww=True)
        self.c3_cluster.add_bucket(ramQuotaMB=100, bucket='default',
                                   replicaNumber=1, proxyPort=11211)
        self.assertTrue(c3_conn.is_lww_enabled(), "LWW not enabled on C3 bucket")
        self.log.info("LWW enabled on C3 bucket as expected")
        self._create_collections("default", self.c3_cluster.get_master_node())

        self._offset_wall_clock(self.c1_cluster, offset_secs=3600)
        self._offset_wall_clock(self.c2_cluster, offset_secs=7200)
        self._offset_wall_clock(self.c3_cluster, offset_secs=10800)
        self.sleep(10)

        self.setup_xdcr()
        src_def = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'default')
        dest_def = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default')
        c3_def = self._get_python_sdk_client(self.c3_cluster.get_master_node().ip, 'default')

        self._upsert(conn=dest_def, doc_id='lww-0', key='key', val='value')
        self._upsert(conn=dest_def, doc_id='lww-0', key='key1', val='value1')
        self._upsert(conn=c3_def, doc_id='lww-0', key='key2', val='value2')
        #src_def.remove(key='lww-0', scope='scope_1', collection='collection_1')

        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key2': 'value2'}, src_def):
            self.fail("C3 doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key2': 'value2'}, dest_def):
            self.fail("C3 doc did not win using LWW")
        if self.check_kv_exists_in_doc({'key2': 'value2'}, c3_def):
            self.fail("C3 doc did not win using LWW")
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

    def test_lww_mixed_with_diff_topology_and_clocks_out_of_sync(self):
        self.c3_cluster = self.get_cb_cluster_by_name('C3')
        c3_conn = RestConnection(self.c3_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=True, dst_lww=False)
        c3_conn.create_bucket(bucket='default', ramQuotaMB=100, replicaNumber=1,
                              proxyPort=11211, replica_index=1, threadsNumber=3,
                              flushEnabled=1, lww=True)
        self.c3_cluster.add_bucket(ramQuotaMB=100, bucket='default',
                                   replicaNumber=1, proxyPort=11211)
        self.assertTrue(c3_conn.is_lww_enabled(), "LWW not enabled on C3 bucket")
        self.log.info("LWW enabled on C3 bucket as expected")
        try:
            self.setup_xdcr()
        except Exception as e:
            self.assertTrue(
                "Replication between buckets with different ConflictResolutionType setting is not allowed" in str(e),
                "ConflictResolutionType mismatch message not thrown as expected")
            self.log.info("ConflictResolutionType mismatch message thrown as expected")

    def test_v_topology_with_clocks_out_of_sync(self):
        self.c3_cluster = self.get_cb_cluster_by_name('C3')
        c3_conn = RestConnection(self.c3_cluster.get_master_node())

        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=True, dst_lww=True)
        c3_conn.create_bucket(bucket='default', ramQuotaMB=100, replicaNumber=1,
                              proxyPort=11211, replica_index=1, threadsNumber=3,
                              flushEnabled=1, lww=True)
        self.c3_cluster.add_bucket(ramQuotaMB=100, bucket='default',
                                   replicaNumber=1, proxyPort=11211)
        self.assertTrue(c3_conn.is_lww_enabled(), "LWW not enabled on C3 bucket")
        self.log.info("LWW enabled on C3 bucket as expected")
        self._create_collections("default", self.c3_cluster.get_master_node())

        self._offset_wall_clock(self.c1_cluster, offset_secs=3600)
        self._offset_wall_clock(self.c2_cluster, offset_secs=7200)
        self._offset_wall_clock(self.c3_cluster, offset_secs=10800)

        self.sleep(10)

        self.setup_xdcr()
        gen1 = DocumentGenerator('lww', '{{"key":"value"}}', list(range(100)), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen1)

        gen2 = DocumentGenerator('lww', '{{"key":"value"}}', list(range(100)), start=0, end=1)
        self.c3_cluster.load_all_buckets_from_generator(gen2)

        src_def = self._get_python_sdk_client(self.c1_cluster.get_master_node().ip, 'default')
        dest_def = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default')
        c3_def = self._get_python_sdk_client(self.c3_cluster.get_master_node().ip, 'default')

        self._upsert(conn=c3_def, doc_id='lww-0', key='key1', val='value1')
        self._upsert(conn=src_def, doc_id='lww-0', key='key2', val='value2')

        self._wait_for_replication_to_catchup()

        if self.check_kv_exists_in_doc({'key1': 'value1'}, dest_def):
            self.fail("C3 doc did not win using LWW")

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
        self._create_buckets(bucket='default', ramQuotaMB=100, skip_dst=True)
        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', list(range(100)), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_active = self._get_max_cas(node=self.c1_cluster.get_master_node(), bucket='default',
                                           vbucket_id=vbucket_id)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_replica = self._get_max_cas(node=self._input.servers[1], bucket='default', vbucket_id=vbucket_id)

        self.log.info("max_cas_active: " + str(max_cas_active))
        self.log.info("max_cas_replica: " + str(max_cas_replica))
        self.assertTrue(not (max_cas_active ^ max_cas_replica), "HLC of active is not equal to replica")
        self.log.info("HLC of active is equal to replica")

    def test_hlc(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()
        self.c1_cluster.pause_all_replications_by_id()

        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c2_cluster.load_all_buckets_from_generator(gen1)

        gen2 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen2)

        self.c1_cluster.resume_all_replications_by_id()
        self.sleep(300)
        self._wait_for_replication_to_catchup()

        max_cas_c1 = self._get_max_cas(node=self.c1_cluster.get_master_node(), bucket='default')
        max_cas_c2 = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default')
        self.log.info("max_cas C1: " + str(max_cas_c1))
        self.log.info("max_cas C2: " + str(max_cas_c2))
        self.assertTrue(not (max_cas_c1 ^ max_cas_c2), "HLC of C1 is not equal to C2")
        self.log.info("HLC of C1 is equal to C2")

    def test_hlc_target_faster(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self._offset_wall_clock(self.c2_cluster, offset_secs=900)

        self.setup_xdcr()

        self.c1_cluster.pause_all_replications_by_id()

        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', list(range(100)), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_c2_before = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default',
                                              vbucket_id=vbucket_id)

        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', list(range(100)), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()
        self.sleep(300)
        self._wait_for_replication_to_catchup(fetch_bucket_stats_by="hour")

        max_cas_c2_after = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default',
                                             vbucket_id=vbucket_id)

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default')
        if self.check_kv_exists_in_doc({'key1': 'value1'}, dest_lww):
            self.fail("Target doc did not win using LWW")
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
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self._offset_wall_clock(self.c1_cluster, offset_secs=900)

        self.setup_xdcr()
        self.merge_all_buckets()

        self.c1_cluster.pause_all_replications_by_id()

        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', list(range(100)), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_c2_before = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default',
                                              vbucket_id=vbucket_id)

        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', list(range(100)), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()

        max_cas_c2_after = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default',
                                             vbucket_id=vbucket_id)

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default')
        self.sleep(10)

        if self.check_kv_exists_in_doc({'key2': 'value2'}, dest_lww):
            self.fail("Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        self.log.info("max_cas_c2_before: " + str(max_cas_c2_before))
        self.log.info("max_cas_c2_after: " + str(max_cas_c2_after))
        self.assertTrue(not ((max_cas_c2_after + (~max_cas_c2_before + 1)) >> 63 & 1),
                        "HLC of C2 is not greater than before replication")
        self.log.info("HLC of C2 is greater than before replication as expected")

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_hlc_within_cluster_target_faster(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self._offset_wall_clock(self.c2_cluster, offset_secs=900)
        self.setup_xdcr()
        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', list(range(100)), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_c2_before = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default',
                                              vbucket_id=vbucket_id)

        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', list(range(100)), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        self.c1_cluster.resume_all_replications_by_id()
        self.sleep(300)
        self._wait_for_replication_to_catchup(fetch_bucket_stats_by="hour")

        max_cas_c2_after = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default',
                                             vbucket_id=vbucket_id)

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default')
        self.sleep(10)

        if self.check_kv_exists_in_doc({'key1': 'value1'}, dest_lww):
            self.fail("Target doc did not win using LWW")
        self.log.info("Target doc won using LWW as expected")

        self.log.info("max_cas_c2_before: " + str(max_cas_c2_before))
        self.log.info("max_cas_c2_after: " + str(max_cas_c2_after))
        self.assertTrue(not (max_cas_c2_before ^ max_cas_c2_after), "HLC of C2 changed after replication")
        self.log.info("HLC of C2 did not change after replication as expected")

        self._upsert(conn=dest_lww, doc_id='lww-0', key='key', val='value3')
        max_cas_c2_after_new_mutation = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default',
                                                          vbucket_id=vbucket_id)
        self.log.info("max_cas_c2_after_new_mutation: " + str(max_cas_c2_after_new_mutation))
        self.assertTrue(not ((max_cas_c2_after_new_mutation + (~max_cas_c2_after + 1)) >> 63 & 1),
                        "HLC of C2 is not greater after new mutation")
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
        self._offset_wall_clock(self.c1_cluster, offset_secs=900)
        self.setup_xdcr()

        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', list(range(100)), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        max_cas_c2_before = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default',
                                              vbucket_id=vbucket_id)

        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', list(range(100)), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        max_cas_c2_after = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default',
                                             vbucket_id=vbucket_id)

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default')
        if self.check_kv_exists_in_doc({'key2': 'value2'}, dest_lww):
            self.fail("Src doc did not win using LWW")
        self.log.info("Src doc won using LWW as expected")

        self.log.info("max_cas_c2_before: " + str(max_cas_c2_before))
        self.log.info("max_cas_c2_after: " + str(max_cas_c2_after))
        self.assertTrue(not ((max_cas_c2_after + (~max_cas_c2_before + 1)) >> 63 & 1),
                        "HLC of C2 is not greater than before replication")
        self.log.info("HLC of C2 is greater than before replication as expected")

        self._upsert(conn=dest_lww, doc_id='lww-0', key='key3', val='value3')
        max_cas_c2_after_new_mutation = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default',
                                                          vbucket_id=vbucket_id)
        self.log.info("max_cas_c2_after_new_mutation: " + str(max_cas_c2_after_new_mutation))
        self.assertTrue(not ((max_cas_c2_after_new_mutation + (~max_cas_c2_after + 1)) >> 63 & 1),
                        "HLC of C2 is not greater after new mutation")
        self.log.info("HLC of C2 is greater after new mutation as expected")

        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        conn.stop_couchbase()

        self._enable_ntp_and_sync()
        self._disable_ntp()

        conn.start_couchbase()

    def test_hlc_ordering_with_delay_source_faster(self):
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self._offset_wall_clock(self.c1_cluster, offset_secs=900)
        self.sleep(10)
        self.setup_xdcr()
        gen = DocumentGenerator('lww', '{{"key1":"value1"}}', list(range(100)), start=0, end=1)
        self.c1_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        hlc_c1 = self._get_max_cas(node=self.c1_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        self.sleep(timeout=1200)

        gen = DocumentGenerator('lww', '{{"key2":"value2"}}', list(range(100)), start=0, end=1)
        self.c2_cluster.load_all_buckets_from_generator(gen)

        vbucket_id = self._get_vbucket_id(key='lww-0')
        hlc_c2_1 = self._get_max_cas(node=self.c2_cluster.get_master_node(), bucket='default', vbucket_id=vbucket_id)

        self.c1_cluster.resume_all_replications_by_id()

        dest_lww = self._get_python_sdk_client(self.c2_cluster.get_master_node().ip, 'default')
        self.sleep(10)

        obj = dest_lww.get('lww-0')
        self.check_kv_exists_in_doc({'key2': 'value2'}, obj.value, "Target doc did not win using LWW")
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
        self._create_buckets(bucket='default', ramQuotaMB=100)
        self.setup_xdcr()
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
        NodeHelper.wait_warmup_completed([self.c1_cluster.warmup_node(master=True)])
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket after warmup")
        self.log.info("LWW enabled on source bucket after warmup as expected")

    def test_conflict_resolution_mode_with_bucket_delete_and_recreate(self):
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        self._create_buckets(bucket='default', ramQuotaMB=100, skip_dst=True)
        self.sleep(10)
        self.c1_cluster.delete_bucket(bucket_name='default')
        self._create_buckets(bucket='default', ramQuotaMB=100, src_lww=False, skip_dst=True)
        self.assertFalse(src_conn.is_lww_enabled(), "LWW enabled on source bucket after recreate")
        self.log.info("LWW not enabled on source bucket after recreation as expected")

    def test_conflict_resolution_mode_edit(self):
        self._create_buckets(bucket='default', ramQuotaMB=100, skip_dst=True)
        conn = RemoteMachineShellConnection(self.c1_cluster.get_master_node())
        command = "curl -X POST -u Administrator:password " + self.c1_cluster.get_master_node().ip + \
                  ":8091/pools/default/buckets/default -d name=default -d conflictResolutionType=seqno " + \
                  "-d proxyPort=11212 -d ramQuotaMB=100"
        output, error = conn.execute_command(command)
        conn.log_command_output(output, error)
        self.assertTrue("Conflict resolution type not allowed in update bucket" in str(output),
                        "Expected error message not found on editing conflict resolution type")
        self.log.info("Expected error message found on editing conflict resolution type")

    def test_conflict_resolution_mode_after_swap_rebalance(self):
        self._create_buckets(bucket='default', ramQuotaMB=100, skip_dst=True)
        gen1 = BlobGenerator("lww-", "lww-", self._value_size, end=self._num_items)
        self.c1_cluster.load_all_buckets_from_generator(gen1)
        self.c1_cluster.swap_rebalance_master()
        src_conn = RestConnection(self.c1_cluster.get_master_node())
        self.assertTrue(src_conn.is_lww_enabled(), "LWW not enabled on source bucket after swap rebalance")
        self.log.info("LWW enabled on source bucket after swap rebalance as expected")
