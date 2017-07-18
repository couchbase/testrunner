import copy
import os
import shutil
import urllib

from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator,DocumentGenerator
from ent_backup_restore.validation_helpers.backup_restore_validations import BackupRestoreValidations
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper, Bucket as \
    RestBucket
from testconstants import COUCHBASE_FROM_4DOT6, LINUX_COUCHBASE_BIN_PATH,\
                          COUCHBASE_DATA_PATH, WIN_COUCHBASE_DATA_PATH,\
                          WIN_COUCHBASE_BIN_PATH_RAW, WIN_TMP_PATH_RAW,\
                          MAC_COUCHBASE_BIN_PATH, LINUX_ROOT_PATH, WIN_ROOT_PATH,\
                          WIN_TMP_PATH, STANDARD_BUCKET_PORT
from membase.api.rest_client import RestConnection
from couchbase.bucket import Bucket
from lib.memcached.helper.data_helper import MemcachedClientHelper


class EnterpriseBackupRestoreBase(BaseTestCase):
    def setUp(self):
        super(EnterpriseBackupRestoreBase, self).setUp()
        """ from version 4.6.0 and later, --host flag is deprecated """
        self.cluster_flag = "--host"
        if self.cb_version[:5] in COUCHBASE_FROM_4DOT6:
            self.cluster_flag = "--cluster"
        self.backupset = Backupset()
        self.cmd_ext = ""
        self.database_path = COUCHBASE_DATA_PATH
        self.cli_command_location = LINUX_COUCHBASE_BIN_PATH
        self.debug_logs = self.input.param("debug_logs", False)
        self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        shell = RemoteMachineShellConnection(self.servers[0])
        info = shell.extract_remote_info().type.lower()
        self.root_path = LINUX_ROOT_PATH
        self.os_name = "linux"
        self.tmp_path = "/tmp/"
        if info == 'linux':
            self.cli_command_location = LINUX_COUCHBASE_BIN_PATH
            self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        elif info == 'windows':
            self.cmd_ext = ".exe"
            self.cli_command_location = WIN_COUCHBASE_BIN_PATH_RAW
            self.backupset.directory = self.input.param("dir", WIN_TMP_PATH_RAW + "entbackup")
        elif info == 'mac':
            self.cli_command_location = MAC_COUCHBASE_BIN_PATH
            self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        else:
            raise Exception("OS not supported.")
        self.backup_validation_files_location = "/tmp/backuprestore"
        self.backupset.backup_host = self.input.clusters[1][0]
        self.backupset.name = self.input.param("name", "backup")
        self.non_master_host = self.input.param("non-master", False)
        self.compact_backup = self.input.param("compact-backup", False)
        self.merged = self.input.param("merged", None)
        self.do_restore = self.input.param("do-restore", False)
        self.do_verify = self.input.param("do-verify", False)
        self.create_views = self.input.param("create-views", False)
        self.create_fts_index = self.input.param("create-fts-index", False)
        self.cluster_new_user = self.input.param("new_user", None)
        self.cluster_new_role = self.input.param("new_role", None)
        self.restore_only = self.input.param("restore-only", False)
        if self.non_master_host:
            self.backupset.cluster_host = self.servers[1]
            self.backupset.cluster_host_username = self.servers[1].rest_username
            self.backupset.cluster_host_password = self.servers[1].rest_password
        else:
            self.backupset.cluster_host = self.servers[0]
            self.backupset.cluster_host_username = self.servers[0].rest_username
            self.backupset.cluster_host_password = self.servers[0].rest_password
        self.same_cluster = self.input.param("same-cluster", False)
        self.reset_restore_cluster = self.input.param("reset-restore-cluster", True)
        self.no_progress_bar = self.input.param("no-progress-bar", True)
        self.multi_threads = self.input.param("multi_threads", False)
        self.threads_count = self.input.param("threads_count", 1)
        self.bucket_delete = self.input.param("bucket_delete", False)
        self.bucket_flush = self.input.param("bucket_flush", False)
        if self.same_cluster:
            self.backupset.restore_cluster_host = self.servers[0]
            self.backupset.restore_cluster_host_username = self.servers[0].rest_username
            self.backupset.restore_cluster_host_password = self.servers[0].rest_password
        else:
            self.backupset.restore_cluster_host = self.input.clusters[0][0]
            self.backupset.restore_cluster_host_username = self.input.clusters[0][0].rest_username
            self.backupset.restore_cluster_host_password = self.input.clusters[0][0].rest_password
        """ new user to test RBAC """
        if self.cluster_new_user:
            self.backupset.cluster_host_username = self.cluster_new_user
            self.backupset.restore_cluster_host_username = self.cluster_new_user
        include_buckets = self.input.param("include-buckets", "")
        include_buckets = include_buckets.split(",") if include_buckets else []
        exclude_buckets = self.input.param("exclude-buckets", "")
        exclude_buckets = exclude_buckets.split(",") if exclude_buckets else []
        self.backupset.exclude_buckets = exclude_buckets
        self.backupset.include_buckets = include_buckets
        self.backupset.disable_bucket_config = self.input.param("disable-bucket-config", False)
        self.backupset.disable_views = self.input.param("disable-views", False)
        self.backupset.disable_gsi_indexes = self.input.param("disable-gsi-indexes", False)
        self.backupset.disable_ft_indexes = self.input.param("disable-ft-indexes", False)
        self.backupset.disable_data = self.input.param("disable-data", False)
        self.backupset.disable_conf_res_restriction = self.input.param("disable-conf-res-restriction", None)
        self.backupset.force_updates = self.input.param("force-updates", False)
        self.backupset.resume = self.input.param("resume", False)
        self.backupset.purge = self.input.param("purge", False)
        self.backupset.threads = self.input.param("threads", self.number_of_processors())
        self.backupset.start = self.input.param("start", 1)
        self.backupset.end = self.input.param("stop", 1)
        self.backupset.number_of_backups = self.input.param("number_of_backups", 1)
        self.backupset.filter_keys = self.input.param("filter-keys", "")
        self.backupset.random_keys = self.input.param("random_keys", False)
        self.backupset.filter_values = self.input.param("filter-values", "")
        self.backupset.backup_list_name = self.input.param("list-names", None)
        self.backupset.backup_incr_backup = self.input.param("incr-backup", None)
        self.backupset.bucket_backup = self.input.param("bucket-backup", None)
        self.backupset.backup_to_compact = self.input.param("backup-to-compact", 0)
        self.backupset.map_buckets = self.input.param("map-buckets", None)
        self.backups = []
        self.validation_helper = BackupRestoreValidations(self.backupset, self.cluster_to_backup, self.cluster_to_restore,
                                                          self.buckets, self.backup_validation_files_location, self.backups,
                                                          self.num_items)
        self.number_of_backups_taken = 0
        self.vbucket_seqno = []
        self.expires = self.input.param("expires", 0)
        self.auto_failover = self.input.param("enable-autofailover", False)
        self.auto_failover_timeout = self.input.param("autofailover-timeout", 30)
        self.graceful = self.input.param("graceful",False)
        self.recoveryType = self.input.param("recoveryType", "full")
        self.skip_buckets = self.input.param("skip_buckets", False)
        self.lww_new = self.input.param("lww_new", False)
        self.skip_consistency = self.input.param("skip_consistency", False)
        self.per_node = self.input.param("per_node", True)
        if not os.path.exists(self.backup_validation_files_location):
            os.mkdir(self.backup_validation_files_location)

    def tearDown(self):
        super(EnterpriseBackupRestoreBase, self).tearDown()
        if not self.input.param("skip_cleanup", False):
            remote_client = RemoteMachineShellConnection(self.input.clusters[1][0])
            info = remote_client.extract_remote_info().type.lower()
            if info == 'linux' or info == 'mac':
                backup_directory = "/tmp/entbackup"
            elif info == 'windows':
                backup_directory = WIN_TMP_PATH_RAW + "entbackup"
            else:
                raise Exception("OS not supported.")
            validation_files_location = "/tmp/backuprestore"
            if info == 'linux':
                command = "rm -rf {0}".format(backup_directory)
                output, error = remote_client.execute_command(command)
                remote_client.log_command_output(output, error)
            elif info == 'windows':
                remote_client.remove_directory_recursive(backup_directory)
            if info == 'linux':
                command = "rm -rf /cbqe3043/entbackup".format(backup_directory)
                output, error = remote_client.execute_command(command)
                remote_client.log_command_output(output, error)
            if self.input.clusters:
                for key in self.input.clusters.keys():
                    servers = self.input.clusters[key]
                    self.backup_reset_clusters(servers)
            if os.path.exists(validation_files_location):
                shutil.rmtree(validation_files_location)

    @property
    def cluster_to_backup(self):
        return self.get_nodes_in_cluster(self.backupset.cluster_host)

    @property
    def cluster_to_restore(self):
        return self.get_nodes_in_cluster(self.backupset.restore_cluster_host)

    def number_of_processors(self):
        remote_client = RemoteMachineShellConnection(self.input.clusters[1][0])
        info = remote_client.extract_remote_info().type.lower()
        if info == 'linux' or info == 'mac':
            command = "nproc"
            output, error = remote_client.execute_command(command)
            if output:
                return output[0]
            else:
                return error[0]
        elif info == 'windows':
            sysinfo = remote_client.get_windows_system_info()
            numprocs = sysinfo['Processor(s)'].split(' ')
            return numprocs[0]

    def backup_reset_clusters(self, servers):
        BucketOperationHelper.delete_all_buckets_or_assert(servers, self)
        ClusterOperationHelper.cleanup_cluster(servers, master=servers[0])
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, self)

    def store_vbucket_seqno(self):
        vseqno = self.get_vbucket_seqnos(self.cluster_to_backup, self.buckets, self.skip_consistency, self.per_node)
        self.vbucket_seqno.append(vseqno)

    def backup_create(self, del_old_backup=True):
        args = "config --archive {0} --repo {1}".format(self.backupset.directory, self.backupset.name)
        if self.backupset.exclude_buckets:
            args += " --exclude-buckets \"{0}\"".format(",".join(self.backupset.exclude_buckets))
        if self.backupset.include_buckets:
            args += " --include-buckets \"{0}\"".format(",".join(self.backupset.include_buckets))
        if self.backupset.disable_bucket_config:
            args += " --disable-bucket-config"
        if self.backupset.disable_views:
            args += " --disable-views"
        if self.backupset.disable_gsi_indexes:
            args += " --disable-gsi-indexes"
        if self.backupset.disable_ft_indexes:
            args += " --disable-ft-indexes"
        if self.backupset.disable_data:
            args += " --disable-data"
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        if del_old_backup:
            self.log.info("Remove any old dir before create new one")
            remote_client.execute_command("rm -rf %s" % self.backupset.directory)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        return output, error

    def backup_create_validate(self):
        output, error = self.backup_create()
        if error or "Backup repository `{0}` created successfully".format(self.backupset.name) not in output[0]:
            self.fail("Creating backupset failed.")
        status, msg = self.validation_helper.validate_backup_create()
        if not status:
            self.fail(msg)
        self.log.info(msg)

    def backup_cluster(self):
        args = "backup --archive {0} --repo {1} {6} http://{2}:{3} --username {4} --password {5}". \
            format(self.backupset.directory, self.backupset.name, self.backupset.cluster_host.ip,
                   self.backupset.cluster_host.port, self.backupset.cluster_host_username,
                   self.backupset.cluster_host_password, self.cluster_flag)
        if self.backupset.resume:
            args += " --resume"
        if self.backupset.purge:
            args += " --purge"
        if self.no_progress_bar:
            args += " --no-progress-bar"
        if self.multi_threads:
            args += " --threads %s " % threads_count
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)

        if error or (output and "Backup successfully completed" not in output[0]):
            return output, error
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = remote_client.execute_command(command)
        if o:
            self.backups.append(o[0])
        self.number_of_backups_taken += 1
        self.log.info("Finished taking backup  with args: {0}".format(args))
        return output, error

    def backup_cluster_validate(self, skip_backup=False, repeats=0):
        if not skip_backup:
            output, error = self.backup_cluster()
            if error or "Backup successfully completed" not in output[-1]:
                self.fail("Taking cluster backup failed.")
        if not repeats:
            status, msg = self.validation_helper.validate_backup()
            if not status:
                self.fail(msg)
            self.log.info(msg)
        self.store_vbucket_seqno()
        self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                          self.backup_validation_files_location)
        self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                            self.backup_validation_files_location)
        self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)

    def backup_restore(self):
        try:
            backup_start = self.backups[int(self.backupset.start) - 1]
        except IndexError:
            backup_start = "{0}{1}".format(self.backups[-1], self.backupset.start)
        try:
            backup_end = self.backups[int(self.backupset.end) - 1]
        except IndexError:
            backup_end = "{0}{1}".format(self.backups[-1], self.backupset.end)
        args = "restore --archive {0} --repo {1} {2} http://{3}:{4} --username {5} "\
               "--password {6} --start {7} --end {8}" \
                               .format(self.backupset.directory, self.backupset.name,
                                       self.cluster_flag, self.backupset.restore_cluster_host.ip,
                                       self.backupset.restore_cluster_host.port,
                                       self.backupset.restore_cluster_host_username,
                                       self.backupset.restore_cluster_host_password,
                                       backup_start, backup_end)
        if self.backupset.exclude_buckets:
            args += " --exclude-buckets {0}".format(self.backupset.exclude_buckets)
        if self.backupset.include_buckets:
            args += " --include-buckets {0}".format(self.backupset.include_buckets)
        if self.backupset.disable_bucket_config:
            args += " --disable-bucket-config {0}".format(self.backupset.disable_bucket_config)
        if self.backupset.disable_views:
            args += " --disable-views {0}".format(self.backupset.disable_views)
        if self.backupset.disable_gsi_indexes:
            args += " --disable-gsi-indexes {0}".format(self.backupset.disable_gsi_indexes)
        if self.backupset.disable_ft_indexes:
            args += " --disable-ft-indexes {0}".format(self.backupset.disable_ft_indexes)
        if self.backupset.disable_data:
            args += " --disable-data {0}".format(self.backupset.disable_data)
        if self.backupset.disable_conf_res_restriction is not None:
            args += " --disable-conf-res-restriction {0}".format(
                self.backupset.disable_conf_res_restriction)
        if self.backupset.filter_keys:
            args += " --filter-keys '{0}'".format(self.backupset.filter_keys)
        if self.backupset.filter_values:
            args += " --filter-values '{0}'".format(self.backupset.filter_values)
        if self.backupset.force_updates:
            args += " --force-updates"
        if self.no_progress_bar:
            args += " --no-progress-bar"
        if not self.skip_buckets:
            rest_conn = RestConnection(self.backupset.restore_cluster_host)
            rest_helper = RestHelper(rest_conn)
            ram_size = rest_conn.get_nodes_self().memoryQuota
            bucket_size = self._get_bucket_size(ram_size, self.total_buckets)
            count = 0
            buckets = []
            for bucket in self.buckets:
                bucket_name = bucket.name
                if not rest_helper.bucket_exists(bucket_name):
                    if self.backupset.map_buckets is None:
                        self.log.info("Creating bucket {0} in restore host {1}"
                                                .format(bucket_name,
                                                 self.backupset.restore_cluster_host.ip))
                    elif self.backupset.map_buckets:
                        self.log.info("Create new bucket name to restore to this bucket")
                        bucket_maps = ""
                        bucket_name = bucket.name + "_" + str(count)
                    self.log.info("Creating bucket {0} in restore host {1}"
                                                .format(bucket_name,
                                                 self.backupset.restore_cluster_host.ip))
                    rest_conn.create_bucket(bucket=bucket_name,
                                    ramQuotaMB=bucket_size,
                                    authType=bucket.authType if bucket.authType else 'none',
                                    proxyPort=bucket.port,
                                    saslPassword=bucket.saslPassword,
                                    lww=self.lww_new)
                    bucket_ready = rest_helper.vbucket_map_ready(bucket_name)
                    if not bucket_ready:
                        self.fail("Bucket %s not created after 120 seconds." % bucket_name)
                buckets.append("%s=%s" % (bucket.name, bucket_name))
                count +=1
            bucket_maps = ",".join(buckets)
        if self.backupset.map_buckets:
            args += " --map-buckets %s " % bucket_maps
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        res = output
        res.extend(error)
        error_str = "Error restoring cluster: Transfer failed. Check the logs for more information."
        if error_str in res:
            command = "cat " + self.backupset.directory + "/logs/backup.log | grep '" + error_str + "' -A 10 -B 100"
            output, error = remote_client.execute_command(command)
            remote_client.log_command_output(output, error)
        if 'Required Flags:' in res:
            self.fail("Command line failed. Please check test params.")
        return output, error

    def backup_restore_validate(self, compare_uuid=False, seqno_compare_function="==",
                                replicas=False, mode="memory", expected_error=None):
        output, error =self.backup_restore()
        if expected_error:
            output.extend(error)
            error_found = False
            if expected_error:
                for line in output:
                    if line.find(expected_error) != -1:
                        error_found = True
                        break
            self.assertTrue(error_found, "Expected error not found: %s" % expected_error)
            return
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "grep 'Transfer plan finished successfully' " + self.backupset.directory + "/logs/backup.log"
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if not output:
            self.fail("Restoring backup failed.")
        command = "grep 'Transfer failed' " + self.backupset.directory + "/logs/backup.log"
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if output:
            self.fail("Restoring backup failed.")

        self.log.info("Finished restoring backup")
        self.log.info("Get current vseqno on node %s " % self.cluster_to_restore[0].ip )

        current_vseqno = self.get_vbucket_seqnos(self.cluster_to_restore, self.buckets, self.skip_consistency, self.per_node)
        self.log.info("*** Start to validate the restore ")
        status, msg = self.validation_helper.validate_restore(self.backupset.end, self.vbucket_seqno, current_vseqno,
                                                              compare_uuid=compare_uuid, compare=seqno_compare_function,
                                                              get_replica=replicas, mode=mode)

        if not status:
            self.fail(msg)
        self.log.info(msg)

    def backup_list(self):
        args = "list --archive {0}".format(self.backupset.directory)
        if self.backupset.backup_list_name:
            args += " --repo {0}".format(self.backupset.backup_list_name)
        if self.backupset.backup_incr_backup:
            args += " --backup {0}".format(self.backupset.backup_incr_backup)
        if self.backupset.bucket_backup:
            args += " --bucket {0}".format(self.backupset.bucket_backup)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error:
            return False, error, "Getting backup list failed."
        else:
            return True, output, "Backup list obtained"

    def backup_compact(self):
        args = "compact --archive {0} --repo {1} --backup {2}".format(self.backupset.directory, self.backupset.name,
                                                                  self.backups[self.backupset.backup_to_compact])
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if "Compaction succeeded," not in output[0]:
            return False, output, "Compacting backup failed."
        else:
            return True, output, "Compaction of backup success"

    def backup_remove(self):
        args = "remove --archive {0} --repo {1}".format(self.backupset.directory, self.backupset.name)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        self.verify_cluster_stats()
        if error:
            return False, error, "Removing backup failed."
        else:
            return True, output, "Removing of backup success"

    def backup_list_validate(self):
        status, output, message = self.backup_list()
        if not status:
            self.fail(message)
        status, message = self.validation_helper.validate_backup_list(output)
        if not status:
            self.fail(message)
        self.log.info(message)

    def backup_compact_validate(self):
        self.log.info("Listing backup details before compact")
        status, output_before_compact, message = self.backup_list()
        if not status:
            self.fail(message)
        status, output, message = self.backup_compact()
        if not status:
            self.fail(message)
        self.log.info("Listing backup details after compact")
        status, output_after_compact, message = self.backup_list()
        if not status:
            self.fail(message)
        status, message = self.validation_helper.validate_compact_lists(output_before_compact, output_after_compact)
        if not status:
            self.fail(message)
        self.log.info(message)

    def backup_compact_deleted_keys_validation(self, delete_keys):
        self.log.info("Check deleted keys status in file after compact")
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        output, error = conn.execute_command("ls %s/backup/201*/default*/data "\
                                                     % self.backupset.directory)
        deleted_key_status = {}
        if "shard_0.fdb" in output:
            cmd = "%sforestdb_dump%s --plain-meta --no-body "\
                  "%s/backup/201*/default*/data/shard_0.fdb | grep -A 6 ent-backup "\
                                         % (self.cli_command_location, self.cmd_ext,\
                                         self.backupset.directory)
            dump_output, error = conn.execute_command(cmd)
            if dump_output:
                key_ids = [x.split(":")[1].strip(' ') for x in dump_output[0::8]]
                miss_keys = [x for x in delete_keys if x not in key_ids]
                if miss_keys:
                    raise Exception("Lost some keys %s ", miss_keys)
                partition_ids =  [x.split(":")[1].strip(' ') for x in dump_output[1::8]]
                status_ids =     [x.split(" ")[-3].strip(' ') for x in dump_output[6::8]]
                for idx, key in enumerate(key_ids):
                    deleted_key_status[key] = \
                           {"KV store name":partition_ids[idx], "Status":status_ids[idx]}
                    if status_ids[idx] != "deleted":
                        raise Exception("key %s status was not deleted. " % key)
            else:
                raise Exception("backup compaction failed to keep delete docs in file")
        else:
            raise Exception("file shard_0.fdb did not created ")
        return deleted_key_status

    def backup_merge(self):
        self.log.info("backups before merge: " + str(self.backups))
        self.log.info("number_of_backups_taken before merge: " + str(self.number_of_backups_taken))
        try:
            backup_start = self.backups[int(self.backupset.start) - 1]
        except IndexError:
            backup_start = "{0}{1}".format(self.backups[-1], self.backupset.start)
        try:
            backup_end = self.backups[int(self.backupset.end) - 1]
        except IndexError:
            backup_end = "{0}{1}".format(self.backups[-1], self.backupset.end)
        args = "merge --archive {0} --repo {1} --start {2} --end {3}".format(self.backupset.directory, self.backupset.name,
                                                                         backup_start, backup_end)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error:
            return False, error, "Merging backup failed"
        elif output and "Merge completed successfully" not in output[0]:
            return False, output, "Merging backup failed"
        elif not output:
            self.log.info("process cbbackupmge may be killed")
            return False, [] , "cbbackupmgr may be killed"
        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = remote_client.execute_command(command)
        if o:
            self.backups.insert(self.backupset.start - 1, o[0])
        self.number_of_backups_taken -= (self.backupset.end - self.backupset.start + 1)
        self.number_of_backups_taken += 1
        self.log.info("backups after merge: " + str(self.backups))
        self.log.info("number_of_backups_taken after merge: " + str(self.number_of_backups_taken))
        return True, output, "Merging backup succeeded"

    def backup_merge_validate(self, repeats=0):
        status, output, message = self.backup_merge()
        if not status:
            self.fail(message)
        if not repeats:
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location, merge=True)

            self.validation_helper.validate_merge(self.backup_validation_files_location)


class Backupset:
    def __init__(self):
        self.backup_host = None
        self.directory = ''
        self.name = ''
        self.cluster_host = None
        self.cluster_host_username = ''
        self.cluster_host_password = ''
        self.restore_cluster_host = None
        self.restore_cluster_host_username = ''
        self.restore_cluster_host_password = ''
        self.threads = ''
        self.exclude_buckets = []
        self.include_buckets = []
        self.disable_bucket_config = False
        self.disable_views = False
        self.disable_gsi_indexes = False
        self.disable_ft_indexes = False
        self.disable_data = False
        self.disable_conf_res_restriction = False
        self.force_updates = False
        self.resume = False
        self.purge = False
        self.start = 1
        self.end = 1
        self.number_of_backups = 1
        self.filter_keys = ''
        self.filter_values = ''
        self.backup_list_name = ''
        self.backup_incr_backup = ''
        self.bucket_backup = ''
        self.backup_to_compact = ''
        self.map_buckets = None
        self.deleted_buckets = []
        self.new_buckets = []
        self.flushed_buckets = []


class EnterpriseBackupMergeBase(EnterpriseBackupRestoreBase):
    def setUp(self):
        super(EnterpriseBackupMergeBase, self).setUp()
        self.actions = self.input.param("actions", None)
        self.document_type = self.input.param("document_type", "json")
        if self.document_type == "binary":
            self.initial_load_gen = BlobGenerator("ent-backup", "ent-backup-",
                                                  self.value_size,
                                                  start=0,
                                                  end=self.num_items)
            self.create_gen = BlobGenerator("ent-backup", "ent-backup-",
                                            self.value_size, start=self.num_items,
                                            end=self.num_items + self.num_items
                                                                 * 0.5)
            self.update_gen = BlobGenerator("ent-backup", "ent-backup-",
                                            self.value_size,
                                            end=self.num_items * 0.9)

            self.delete_gen = BlobGenerator("ent-backup", "ent-backup-",
                                            self.value_size,
                                            start=self.num_items / 10,
                                            end=self.num_items)
        elif self.document_type == "json":
            age = range(5)
            first = ['james', 'sharon']
            template = '{{ "age": {0}, "first_name": "{1}" }}'
            gen = DocumentGenerator('test_docs', template, age, first, start=0,
                                    end=5)
            self.initial_load_gen = DocumentGenerator('test_docs', template,
                                                      age, first, start=0,
                                                      end=self.num_items)
            self.create_gen = DocumentGenerator('test_docs', template,
                                                age, first,
                                                start=self.num_items,
                                                end=self.num_items +
                                                    self.num_items * 0.5)
            self.update_gen = DocumentGenerator('test_docs', template,
                                                age, first,
                                                end=self.num_items * 0.9)

            self.delete_gen = DocumentGenerator('test_docs', template,
                                                age, first,
                                                start=self.num_items / 10,
                                                end=self.num_items)
        self.new_buckets = 1
        self.bucket_to_flush = 1
        self.ephemeral = False
        self.recreate_bucket = False
        self.graceful = True
        self.recoveryType = 'full'
        self.nodes_in = self.input.param("nodes_in", 0)
        self.nodes_out = self.input.param("nodes_out", 0)
        self.number_of_repeats = self.input.param("repeats", 1)

    def tearDown(self):
        super(EnterpriseBackupMergeBase, self).tearDown()

    def _get_python_sdk_client(self, ip, bucket):
        try:
            cb = Bucket('couchbase://' + ip + '/' + bucket.name)
            if cb is not None:
                self.log.info("Established connection to bucket " + bucket.name + " on " + ip + " using python SDK")
            else:
                self.fail("Failed to connect to bucket " + bucket.name + " on " + ip + " using python SDK")
            return cb
        except Exception, ex:
            self.fail(str(ex))

    def async_ops_on_buckets(self):
        """
        Performs async operations on all the buckets in the cluster.
        The operations are: creates, updates and deletes
        :return: List of tasks running the load operations.
        """
        tasks = []
        create_tasks = self._async_load_all_buckets(self.master,
                                                    self.create_gen,
                                                    "create", self.expires)
        update_tasks = self._async_load_all_buckets(self.master,
                                                    self.update_gen,
                                                    "update", self.expires)
        delete_tasks = self._async_load_all_buckets(self.master,
                                                    self.delete_gen,
                                                    "delete", self.expires)
        tasks.extend(create_tasks)
        tasks.extend(update_tasks)
        tasks.extend(delete_tasks)
        return tasks

    def ops_on_buckets(self):
        ops_tasks = self.async_ops_on_buckets()
        for task in ops_tasks:
            task.result()

    def backup(self):
        """
        Backup the cluster and validate the backupset.
        :return: Nothing
        """
        self.backup_cluster_validate(repeats=self.number_of_repeats)

    def backup_with_expiry(self):
        """
        Backup the cluster and validate the backupset with expiry items before they expire.
        :return: Nothing
        """
        for bucket in self.buckets:
            cb = self._get_python_sdk_client(self.master.ip, bucket)
            for i in range(int(self.num_items * 0.7) + 1, self.num_items + 1):
                cb.upsert("doc" + str(i), {"key":"value"}, ttl=self.expires)
        self.backup_cluster_validate()
        self.sleep(self.expires)

    def backup_after_expiry(self):
        """
        Backup the cluster and validate the backupset with expiry items after they expire.
        :return: Nothing
        """
        for bucket in self.buckets:
            cb = self._get_python_sdk_client(self.master.ip, bucket)
            for i in range(int(self.num_items * 0.7) + 1, self.num_items + 1):
                cb.upsert("doc" + str(i), {"key":"value"}, ttl=self.expires)
        self.sleep(self.expires)
        self.backup_cluster_validate()

    def backup_with_ops(self):
        """
        Backup the data while loading the buckets in parallel.
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.backup()
        self.log.info(ops_tasks)
        for task in ops_tasks:
            task.result()

    def merge(self):
        """
        Merge all the existing backups in the backupset.
        :return: Nothing
        """
        self.backup_merge_validate(repeats=self.number_of_repeats)

    def merge_with_ops(self):
        """
        Merge all the existing backups in the backupset while loading the
        buckets in parallel
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.merge()
        for task in ops_tasks:
            task.result()

    def compact_backup(self):
        """
        Compact the given backup and validate that the compaction resulted
        in reduction of backup size.
        :return:
        """
        self.backup_compact_validate()

    def async_rebalance(self):
        """
        Asynchronously rebalance the cluster.
        :return: The task that is rebalancing the nodes.
        """
        rest = RestConnection(self.master)
        # Get the nodes already in the backup cluster
        nodes_in_cluster = rest.node_statuses()
        # Get the nodes available for the backup_custer
        backup_cluster = [server for server in self.servers if (server not in
                          self.input.clusters[0] and server not in
                          self.input.clusters[1])]
        # Get the potential servers that can be added to the cluster
        serv_in = [server for server in backup_cluster if server.ip not in [
            node.ip for node in nodes_in_cluster]]
        # Get the potential servers that can be removed from the cluster
        serv_out = [server for server in backup_cluster if server.ip in [
            node.ip for node in nodes_in_cluster]]
        serv_in = serv_in[:self.nodes_in]
        serv_out = serv_out[serv_out.__len__() - self.nodes_out:]
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup,
                                                 serv_in, serv_out)
        return rebalance

    def rebalance(self):
        """
        Rebalance the cluster
        :return: Nothing
        """
        rebalance = self.async_rebalance()
        rebalance.result()

    def rebalance_with_ops(self):
        """
        Rebalance the cluster while running bucket operations in parallel.
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.rebalance()
        for task in ops_tasks:
            task.result()

    def async_failover(self, ip=None):
        """
        Asynchronously failover a node
        :return: Nothing
        """
        rest = RestConnection(self.backupset.cluster_host)
        nodes_all = rest.node_statuses()
        if ip:
            ip_to_failover = ip
        else:
            ip_to_failover = self.servers[1].ip
        for node in nodes_all:
            if node.ip == ip_to_failover:
                rest.fail_over(otpNode=node.id, graceful=self.graceful)

    def async_failover_and_recover(self):
        """
            Asynchronously failover a node and add back the node after 30 sec
            :return: Task that is running the rebalance at end.
        """
        rest = RestConnection(self.backupset.cluster_host)
        nodes_all = rest.node_statuses()
        for node in nodes_all:
            if node.ip == self.servers[1].ip:
                rest.fail_over(otpNode=node.id, graceful=self.graceful)
                self.sleep(60)
                rest.set_recovery_type(otpNode=node.id,
                                       recoveryType=self.recoveryType)
                rest.add_back_node(otpNode=node.id)
        rebalance = self.cluster.async_rebalance(self.servers, [], [])
        return rebalance

    def async_recover_node(self):
        """
            Asynchronously add back the node after failover.
            :return: Task that is running the rebalance at end.
        """
        rest = RestConnection(self.backupset.cluster_host)
        nodes_all = rest.node_statuses()
        for node in nodes_all:
            if node.ip == self.servers[1].ip:
                rest.set_recovery_type(otpNode=node.id,
                                       recoveryType=self.recoveryType)
                rest.add_back_node(otpNode=node.id)
        rebalance = self.cluster.async_rebalance(self.servers, [], [])
        return rebalance

    def async_remove_failed_node(self):
        """
            Asynchronously remove a failed over node by performing a
            rebalance of cluster
        :return: Task that is running the rebalance.
        """
        rebalance = self.cluster.async_rebalance(self.servers, [], [])
        return rebalance

    def failover(self):
        """
        Failover a node and add back the node.
        :return: Nothing
        """
        self.async_failover()

    def failover_with_ops(self):
        """
        Failover a node while bucket operations are running in parallel.
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.failover()
        for task in ops_tasks:
            task.result()

    def failover_and_recover(self):
        """
        Failover a node and add back the node after 30 sec.
        :return: Nothing
        """
        task = self.async_failover_and_recover()
        task.result()

    def failover_and_recover_with_ops(self):
        """
        Failover a node and add back the node after 30 sec while bucket
        operations are running in parallel
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.failover_and_recover()
        for task in ops_tasks:
            task.result()

    def recover_node(self):
        """
        Recover a failedover node.
        :return: Nothing
        """
        task = self.async_recover_node()
        task.result()

    def recover_node_with_ops(self):
        """
        Recover a failedover node while bucket operations are running in
        parallel.
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.recover_node()
        for task in ops_tasks:
            task.result()

    def remove_failed_node(self):
        """
        Remove a failed over node from the cluster
        :return: Nothing
        """
        task = self.async_remove_failed_node()
        task.result()

    def _reconfigure_bucket_memory(self, num_new_buckets):
        """
        Reconfigure bucket memories in the cluster to accommodate adding new
        buckets
        :param num_new_buckets:
        :return: The new bucket size.
        """
        rest = RestConnection(self.master)
        ram_size = rest.get_nodes_self().memoryQuota
        bucket_size = self._get_bucket_size(ram_size, self.bucket_size +
                                            num_new_buckets)
        self.log.info("Changing the existing buckets size to accomodate new "
                      "buckets")
        for bucket in self.buckets:
            rest.change_bucket_props(bucket, ramQuotaMB=bucket_size)
        return bucket_size

    def create_new_bucket_and_populate(self):
        """
        Create new buckets and populate the bucket with items.
        :return: Nothing
        """
        bucket_size = self._reconfigure_bucket_memory(self.new_buckets)
        rest = RestConnection(self.master)
        server_id = rest.get_nodes_self().id
        bucket_tasks = []
        standard_buckets = []
        for i in range(0, self.new_buckets):
            if self.recreate_bucket:
                name = self.backupset.deleted_buckets[i]
            else:
                name = 'bucket' + str(i)
            port = STANDARD_BUCKET_PORT + i + 1
            bucket_priority = None
            if self.standard_bucket_priority is not None:
                bucket_priority = self.get_bucket_priority(
                    self.standard_bucket_priority[i])
            bucket_tasks.append(
                self.cluster.async_create_standard_bucket(self.master, name,
                                                          port,
                                                          bucket_size,
                                                          self.num_replicas,
                                                          enable_replica_index=self.enable_replica_index,
                                                          eviction_policy=self.eviction_policy,
                                                          bucket_priority=bucket_priority,
                                                          lww=self.lww))
            bucket = RestBucket(name=name, authType=None, saslPassword=None,
                                num_replicas=self.num_replicas,
                                bucket_size=self.bucket_size,
                                port=port, master_id=server_id,
                                eviction_policy='noEviction', lww=self.lww)
            self.buckets.append(bucket)
            standard_buckets.append(bucket)
            if not self.recreate_bucket:
                self.backupset.new_buckets.append(bucket.name)
        for task in bucket_tasks:
            task.result(self.wait_timeout * 10)
        if self.enable_time_sync:
            self._set_time_sync_on_buckets(
                ['bucket' + str(i) for i in range(
                    self.new_buckets)])
        for bucket in standard_buckets:
            self._load_bucket(bucket, self.master, self.initial_load_gen,
                              "create", self.expires)

    def delete_bucket(self):
        """
        Delete a bucket from the cluster
        :return: Nothing
        """
        self.log.info("Deleting a bucket")
        bucket_to_delete = self.buckets[-1].name
        self.backupset.deleted_buckets.append(bucket_to_delete)
        BucketOperationHelper.delete_bucket_or_assert(self.master,
                                                      bucket_to_delete,
                                                      self)
        self.buckets.pop()

    def _flush_bucket(self, bucket_to_flush):
        """
        Flush a bucket from the cluster.
        :param bucket_to_flush:
        :return: Nothing
        """
        self.log.info("Flushing {} buckets")
        rest = RestConnection(self.master)
        bucket_name = self.buckets[bucket_to_flush].name
        rest.flush_bucket(bucket_name)
        self.backupset.flushed_buckets.append(bucket_name)

    def flush_buckets(self):
        """
        Flush buckets from the cluster
        :return: Nothing
        """
        self.log.info("Flushing {} buckets".format(self.bucket_to_flush))
        for i in range(self.buckets.__len__() - self.bucket_to_flush,
                       self.buckets.__len__()):
            self._flush_bucket(i)

    def load_flushed_buckets(self):
        """
        Reload the flushed buckets in the cluster.
        :return: Nothing
        """
        self.log.info("Loading data into buckets that were flushed")
        tasks = []
        for i in range(self.buckets.__len__() - self.bucket_to_flush,
                       self.buckets.__len__()):
            self._load_bucket(self.buckets[i], self.master,
                              self.initial_load_gen, "create", self.expires)

    def compact_buckets(self):
        """
        Compact all the buckets in the cluster
        :return: Nothing
        """
        self._run_compaction(number_of_times=1)

    def rollback(self):
        buckets = self.buckets
        rest = RestConnection(self.backupset.cluster_host)
        nodes = rest.get_nodes()

        for bucket in buckets:
            # Stop Persistence on Node A & Node B
            for node in nodes:
                mem_client = MemcachedClientHelper.direct_client(node, bucket)
                mem_client.stop_persistence()

            # Perform mutations on the bucket
            self.ops_on_buckets()

            # Kill memcached on Node A so that Node B becomes master
            shell = RemoteMachineShellConnection(self.backupset.cluster_host)
            shell.kill_memcached()

            # Start persistence on Node B
            mem_client = MemcachedClientHelper.direct_client(nodes[1], bucket)
            mem_client.start_persistence()

            # Failover Node B
            self.async_failover(nodes[1].ip)

            # Wait for Failover & rollback to complete
            self.sleep(60)

            shell.stop_couchbase()
            shell.start_couchbase()

            self.sleep(60)

    def do_backup_merge_actions(self):
        """
        Perform the actions mentioned the self.actions param.
        :return: Nothing
        """
        self.backupset.number_of_backups = 0
        # running testrunner with conf file takes in the quotes " too into
        # the parameter. Below step removes the quotes from the parameter.
        self.actions = self.actions.replace('"', '')
        actions = self.actions.split(",")
        for action in actions:
            iterations = 1
            if ":" in action:
                action = action.split(':')
                params = action[1].split('&')
                action = action[0]
                if "backup" in action:
                    iterations = params[0]
                    self.backupset.number_of_backups += int(iterations)
                elif "backup_with_expiry" in action:
                    iterations = params[0]
                    self.backupset.number_of_backups += int(iterations)
                elif "backup_after_expiry" in action:
                    iterations = params[0]
                    self.backupset.number_of_backups += int(iterations)
                elif "failover" in action or "recover" in action:
                    if 'hard' in params:
                        self.graceful = False
                    else:
                        self.graceful = True
                    if 'delta' in params:
                        self.recoveryType = 'delta'
                    else:
                        self.recoveryType = 'full'
                elif "merge" in action:
                    if params.__len__() == 0:
                        iterations = 1
                        self.backupset.start = 1
                        self.backupset.end = len(self.backups)
                    if params.__len__() == 2:
                        iterations = 1
                        self.backupset.start = int(params[0])
                        self.backupset.end = int(params[1])
                    if params.__len__() == 3:
                        iterations = params[0]
                        self.backupset.start = int(params[1])
                        self.backupset.end = int(params[2])
                elif "compact_backup" in action:
                    self.backupset.backup_to_compact = int(params[0])
                elif "flush" in action:
                    self.bucket_to_flush = int(params[0])
                elif "create_buckets" in action:
                    self.new_buckets = int(params[0])
                    if params.__len__() == 2:
                        if params[1] == "ephemeral":
                            self.ephemeral = True
                elif "recreate_buckets" in action:
                    self.new_buckets = int(params[0])
                    self.recreate_bucket = True
                    if params.__len__() == 2:
                        if params[1] == "ephemeral":
                            self.ephemeral = True
                else:
                    if params[0].isdigit():
                        iterations = int(params[0])
            else:
                iterations = 1
            for i in range(0, int(iterations)):
                self.log.info("Performing {} action for {}th time".format(
                    action, i + 1))
                self.backup_merge_actions[action](self)
                self.log.info("Finished {} action for {}th time.".format(
                    action, i + 1))

    def set_meta_purge_interval(self):
        rest = RestConnection(self.master)
        params = {}
        api = rest.baseUrl + "controller/setAutoCompaction"
        params["purgeInterval"] = 0.003
        params = urllib.urlencode(params)
        return rest._http_request(api, "POST", params)

    backup_merge_actions = {
        "bucket_ops": ops_on_buckets,
        "backup": backup,
        "backup_with_expiry": backup_with_expiry,
        "backup_after_expiry": backup_after_expiry,
        "merge": merge,
        "compact_backup": compact_backup,
        "rebalance": rebalance,
        "failover": failover,
        "recover": recover_node,
        "remove_node": remove_failed_node,
        "failover_and_recover": failover_and_recover,
        "backup_with_ops": backup_with_ops,
        "merge_with_ops": merge_with_ops,
        "rebalance_with_ops": rebalance_with_ops,
        "failover_with_ops": failover_with_ops,
        "recover_with_ops": recover_node_with_ops,
        "failover_and_recover_with_ops": failover_and_recover_with_ops,
        "create_buckets": create_new_bucket_and_populate,
        "recreate_buckets": create_new_bucket_and_populate,
        "flush_buckets": flush_buckets,
        "load_flushed_buckets": load_flushed_buckets,
        "delete_buckets": delete_bucket,
        "compact_buckets": compact_buckets,
        "rollback": rollback
    }
