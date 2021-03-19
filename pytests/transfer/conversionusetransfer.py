from transfer.transfer_base import TransferBaseTest
from membase.api.rest_client import RestConnection, Bucket
from scripts.install import InstallerJob
from testconstants import COUCHBASE_DATA_PATH, WIN_COUCHBASE_DATA_PATH
from remote.remote_util import RemoteMachineShellConnection
import time
import sys

class ConversionUseTransfer(TransferBaseTest):

    def setUp(self):
        self.times_teardown_called = 1
        super(ConversionUseTransfer, self).setUp()
        self.command_options = self.input.param("command_options", '-x rehash=1')
        self.latest_version = self.input.param('latest_version', None)
        if self.latest_version is None:
            self.fail("for the test you need to specify 'latest_version'")
        self.openssl = self.input.param('openssl', '')

    def tearDown(self):
        if not self.input.param("skip_cleanup", True):
            if self.times_teardown_called > 1 :
                self.shell.disconnect()
                del self.buckets
        if self.input.param("skip_cleanup", True):
            if self.case_number > 1 or self.times_teardown_called > 1:
                self.shell.disconnect()
                del self.buckets
        self.times_teardown_called += 1
        super(ConversionUseTransfer, self).tearDown()


    def _install(self, servers, version='1.8.1-937-rel', vbuckets=1024):
        params = {}
        params['num_nodes'] = len(servers)
        params['product'] = 'couchbase-server'
        params['version'] = version
        params['vbuckets'] = [vbuckets]
        params['openssl'] = self.openssl
        InstallerJob().parallel_install(servers, params)
        success = True
        for server in servers:
            success &= RemoteMachineShellConnection(server).is_couchbase_installed()
            if not success:
                sys.exit("some nodes were not install successfully!")


    def convert_sqlite_to_couchstore(self):
        """Convert data with 181 sqlite files to a 2.0+ online server

        We load a number of items to one 181 node first and then do some mutation on these items.
        Later we use cbtranfer to transfer the sqlite files we have on this
        node to a new node. We verify the data by comparison between the items in KVStore
        and items in the new node."""

        self._install([self.server_origin])

        if self.default_bucket:
            bucket_params = self._create_bucket_params(server=self.server_origin, size=self.bucket_size,
                                                              replicas=self.num_replicas)
            self.cluster.create_default_bucket(bucket_params)
            self.buckets.append(Bucket(name="default", num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        self._create_sasl_buckets(self.server_origin, self.sasl_buckets)
        self._create_standard_buckets(self.server_origin, self.standard_buckets)

        self.load_data()

        if self.os == 'windows':
            output, error = self.shell.execute_command("taskkill /F /T /IM cbtransfer.exe")
            self.shell.log_command_output(output, error)
            self.shell.delete_files("/cygdrive/c%s" % self.win_data_location)
            self.shell.execute_command("mkdir /cygdrive/c%s" % self.win_data_location)
            self.shell.execute_command("cp -rf %s /cygdrive/c/tmp/" % (WIN_COUCHBASE_DATA_PATH))
        else:
            self.shell.delete_files(self.backup_location)
            self.shell.execute_command("mkdir %s" % self.backup_location)
            self.shell.execute_command("cp -rf %s %s" % (COUCHBASE_DATA_PATH, self.backup_location))


        self._install([self.server_origin], version=self.latest_version)
        self._install([self.server_recovery], version=self.latest_version)

        kvs_before = {}
        bucket_names = []
        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
            bucket_names.append(bucket.name)

        del self.buckets
        self.buckets = []
        if self.default_bucket:
            bucket_params = self._create_bucket_params(server=self.server_recovery, size=self.bucket_size,
                                                              replicas=self.num_replicas)
            self.cluster.create_default_bucket(bucket_params)
            self.buckets.append(Bucket(name="default", num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        self._create_sasl_buckets(self.server_recovery, self.sasl_buckets)
        self._create_standard_buckets(self.server_recovery, self.standard_buckets)

        for bucket in self.buckets:
            bucket.kvs[1] = kvs_before[bucket.name]
            transfer_source = "%s/data/%s-data/%s" % (self.backup_location, bucket.name, bucket.name)
            if self.os == 'windows':
                transfer_source = "C:%s/%s-data/%s" % (self.win_data_location, bucket.name, bucket.name)
            transfer_destination = "http://%s@%s:%s" % (self.couchbase_login_info,
                                                        self.server_recovery.ip,
                                                        self.server_recovery.port)
            self.shell.execute_cbtransfer(transfer_source, transfer_destination, "-b %s -B %s %s" % (bucket.name, bucket.name, self.command_options))
        del kvs_before

        time.sleep(self.expire_time + 1)
        shell_server_recovery = RemoteMachineShellConnection(self.server_recovery)
        for bucket in self.buckets:
            shell_server_recovery.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        shell_server_recovery.disconnect()
        time.sleep(self.wait_timeout)

        self._wait_for_stats_all_buckets([self.server_recovery])
        self._verify_stats_all_buckets([self.server_recovery])
        self._verify_all_buckets(self.server_recovery, 1, self.wait_timeout * 50, self.max_verify, True, 1)
