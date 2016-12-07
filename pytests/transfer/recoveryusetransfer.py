from testconstants import COUCHBASE_DATA_PATH, WIN_COUCHBASE_DATA_PATH
from transfer.transfer_base import TransferBaseTest
from membase.api.rest_client import RestConnection, Bucket
from remote.remote_util import RemoteMachineShellConnection
import time

class RecoveryUseTransferTests(TransferBaseTest):

    def setUp(self):
        self.times_teardown_called = 1
        super(RecoveryUseTransferTests, self).setUp()

    def tearDown(self):
        if not self.input.param("skip_cleanup", True):
            if self.times_teardown_called > 1 :
                if self.os == 'windows':
                    self.shell.delete_files("/cygdrive/c%s" % (self.backup_location))
                else:
                    self.shell.delete_files(self.backup_location)
                self.shell.disconnect()
                del self.buckets
        if self.input.param("skip_cleanup", True):
            if self.case_number > 1 or self.times_teardown_called > 1:
                if self.os == 'windows':
                    self.shell.delete_files("/cygdrive/c%s" % (self.backup_location))
                else:
                    self.shell.delete_files(self.backup_location)
                self.shell.disconnect()
                del self.buckets
        self.times_teardown_called += 1
        super(RecoveryUseTransferTests, self).tearDown()

    def recover_to_cbserver(self):
        """Recover data with 2.0 couchstore files to a 2.0 online server

        We load a number of items to one node first and then do some mutation on these items.
        Later we use cbtranfer to transfer the couchstore files we have on this
        node to a new node. We verify the data by comparison between the items in KVStore
        and items in the new node."""

        self.load_data()

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
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="", num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        self._create_sasl_buckets(self.server_recovery, self.sasl_buckets)
        self._create_standard_buckets(self.server_recovery, self.standard_buckets)

        transfer_source = "couchstore-files://%s" % (COUCHBASE_DATA_PATH)
        if self.os == 'windows':
            output, error = self.shell.execute_command("taskkill /F /T /IM cbtransfer.exe")
            self.shell.log_command_output(output, error)
            self.shell.delete_files("/cygdrive/c%s" % self.win_data_location)
            self.shell.execute_command("mkdir /cygdrive/c%s" % self.win_data_location)
            self.shell.execute_command("cp -rf %s /cygdrive/c/tmp/" % (WIN_COUCHBASE_DATA_PATH))
            transfer_source = "couchstore-files://C:%s" % (self.win_data_location)
        for bucket in self.buckets:
            bucket.kvs[1] = kvs_before[bucket.name]
            transfer_destination = "http://%s@%s:%s" % (self.couchbase_login_info,
                                                        self.server_recovery.ip,
                                                        self.server_recovery.port)
            self.shell.execute_cbtransfer(transfer_source, transfer_destination, "-b %s -B %s" % (bucket.name, bucket.name))
        del kvs_before

        time.sleep(self.expire_time + 1)
        shell_server_recovery = RemoteMachineShellConnection(self.server_recovery)
        for bucket in self.buckets:
            shell_server_recovery.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        shell_server_recovery.disconnect()
        time.sleep(30)

        self._wait_for_stats_all_buckets([self.server_recovery])
        self._verify_all_buckets(self.server_recovery, 1, self.wait_timeout * 50, self.max_verify, True, 1)
        self._verify_stats_all_buckets([self.server_recovery])

    def recover_to_backupdir(self):
        """Recover data with 2.0 couchstore files to a 2.0 backup diretory

        We load a number of items to a node first and then do some mutataion on these items.
        Later we use cbtransfer to transfer the couchstore files we have on this node to
        a backup directory. We use cbrestore to restore these backup files to the same node
        for verification."""

        self.load_data()

        kvs_before = {}
        bucket_names = []

        if self.os == 'windows':
            output, error = self.shell.execute_command("taskkill /F /T /IM cbtransfer.exe")
            self.shell.log_command_output(output, error)
            self.shell.delete_files("/cygdrive/c%s" % self.backup_location)
            self.shell.create_directory("/cygdrive/c%s" % self.backup_location)
        else:
            self.shell.delete_files(self.backup_location)
            self.shell.create_directory(self.backup_location)

        transfer_source = "couchstore-files://%s" % (COUCHBASE_DATA_PATH)
        transfer_destination = self.backup_location
        if self.os == 'windows':
            self.shell.delete_files("/cygdrive/c%s" % self.win_data_location)
            self.shell.execute_command("mkdir /cygdrive/c%s" % self.win_data_location)
            self.shell.execute_command("cp -rf %s /cygdrive/c/tmp/" % (WIN_COUCHBASE_DATA_PATH))
            transfer_source = "couchstore-files://C:%s" % (self.win_data_location)
            transfer_destination = "C:%s" % self.backup_location

        for bucket in self.buckets:
            kvs_before[bucket.name] = bucket.kvs[1]
            bucket_names.append(bucket.name)

        self.shell.execute_cbtransfer(transfer_source, transfer_destination)

        self._all_buckets_delete(self.server_origin)
        if self.default_bucket:
            bucket_params = self._create_bucket_params(server=self.server_origin, size=self.bucket_size,
                                                              replicas=self.num_replicas)
            self.cluster.create_default_bucket(bucket_params)
            self.buckets.append(Bucket(name="default", authType="sasl", saslPassword="", num_replicas=self.num_replicas, bucket_size=self.bucket_size))
        self._create_sasl_buckets(self.server_origin, self.sasl_buckets)
        self._create_standard_buckets(self.server_origin, self.standard_buckets)

        for bucket in self.buckets:
            bucket.kvs[1] = kvs_before[bucket.name]
        del kvs_before
        self.shell.restore_backupFile(self.couchbase_login_info, self.backup_location, bucket_names)
        time.sleep(self.expire_time + 1)
        for bucket in self.buckets:
            self.shell.execute_cbepctl(bucket, "", "set flush_param", "exp_pager_stime", 5)
        time.sleep(30)

        self._wait_for_stats_all_buckets([self.server_origin])
        self._verify_all_buckets(self.server_origin, 1, self.wait_timeout * 50, self.max_verify, True, 1)
        self._verify_stats_all_buckets([self.server_origin])
