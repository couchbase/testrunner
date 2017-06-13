import os
import re
import shutil

from basetestcase import BaseTestCase
from couchbase_helper.data_analysis_helper import DataCollector
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import BlobGenerator
from ent_backup_restore.validation_helpers.backup_restore_validations import BackupRestoreValidations
from membase.api.rest_client import RestConnection, RestHelper
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from testconstants import COUCHBASE_FROM_4DOT6, LINUX_COUCHBASE_BIN_PATH,\
                          COUCHBASE_DATA_PATH, WIN_COUCHBASE_DATA_PATH,\
                          WIN_COUCHBASE_BIN_PATH_RAW, WIN_TMP_PATH_RAW,\
                          MAC_COUCHBASE_BIN_PATH, LINUX_ROOT_PATH, WIN_ROOT_PATH,\
                          WIN_TMP_PATH


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
            if self.nonroot:
                base_path = "/home/%s" % self.master.ssh_username
                self.cli_command_location = "%s%s" % (base_path,
                                                      LINUX_COUCHBASE_BIN_PATH)
                self.database_path = "%s%s" % (base_path, COUCHBASE_DATA_PATH)
                self.root_path = "/home/%s/" % self.master.ssh_username
        elif info == 'windows':
            self.os_name = "windows"
            self.cmd_ext = ".exe"
            self.database_path = WIN_COUCHBASE_DATA_PATH
            self.cli_command_location = WIN_COUCHBASE_BIN_PATH_RAW
            self.root_path = WIN_ROOT_PATH
            self.tmp_path = WIN_TMP_PATH
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
        self.backupset.exclude_buckets = self.input.param("exclude-buckets", "")
        self.backupset.include_buckets = self.input.param("include-buckets", "")
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
        self.backupset.no_ssl_verify = self.input.param("no-ssl-verify", False)
        self.backupset.secure_conn = self.input.param("secure-conn", False)
        self.backupset.bk_no_cert = self.input.param("bk-no-cert", False)
        self.backupset.rt_no_cert = self.input.param("rt-no-cert", False)
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

    def backup_cluster(self, threads_count=1):
        url_format = ""
        secure_port = ""
        if self.backupset.secure_conn:
            cacert = self.get_cluster_certificate_info(self.backupset.backup_host,
                                                       self.backupset.cluster_host)
            secure_port = "1"
            url_format = "s"
        args = "backup -a {0} -r {1} {6} http{7}://{2}:{8}{3} -u {4} -p {5}". \
            format(self.backupset.directory, self.backupset.name,
                   self.backupset.cluster_host.ip,
                   self.backupset.cluster_host.port, self.backupset.cluster_host_username,
                   self.backupset.cluster_host_password, self.cluster_flag, url_format,
                   secure_port)
        if self.backupset.no_ssl_verify:
            args += " --no-ssl-verify"
        if self.backupset.secure_conn:
            if not self.backupset.bk_no_cert:
                args += " --cacert %s" % cacert
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

    def backup_cluster_validate(self, skip_backup=False):
        if not skip_backup:
            output, error = self.backup_cluster()
            if error or "Backup successfully completed" not in output[-1]:
                self.fail("Taking cluster backup failed.")
        status, msg = self.validation_helper.validate_backup()
        if not status:
            self.fail(msg)
        self.log.info(msg)
        self.store_vbucket_seqno()
        self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                          self.backup_validation_files_location)
        self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                            self.backup_validation_files_location)

    def backup_restore(self):
        if self.restore_only:
            if self.create_fts_index:
                self.backups.append("2017-05-18T13_40_30.842368123-07_00")
            else:
                self.backups.append("2017-05-18T11_55_22.009680763-07_00")
        try:
            backup_start = self.backups[int(self.backupset.start) - 1]
        except IndexError:
            backup_start = "{0}{1}".format(self.backups[-1], self.backupset.start)
        try:
            backup_end = self.backups[int(self.backupset.end) - 1]
        except IndexError:
            backup_end = "{0}{1}".format(self.backups[-1], self.backupset.end)
        url_format = ""
        secure_port = ""
        if self.backupset.secure_conn:
            cacert = self.get_cluster_certificate_info(self.backupset.backup_host,
                                                       self.backupset.restore_cluster_host)
            url_format = "s"
            secure_port = "1"
        args = "restore --archive {0} --repo {1} {2} http{9}://{3}:{10}{4} -u {5} "\
               "--password {6} --start {7} --end {8}" \
                               .format(self.backupset.directory,
                                       self.backupset.name,
                                       self.cluster_flag,
                                       self.backupset.restore_cluster_host.ip,
                                       self.backupset.restore_cluster_host.port,
                                       self.backupset.restore_cluster_host_username,
                                       self.backupset.restore_cluster_host_password,
                                       backup_start, backup_end, url_format, secure_port)
        if self.backupset.no_ssl_verify:
            args += " --no-ssl-verify"
        if self.backupset.secure_conn:
            if not self.backupset.rt_no_cert:
                args += " --cacert %s" % cacert
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
                                    ramQuotaMB=512,
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

        """ Add built-in user cbadminbucket to second cluster """
        self.add_built_in_server_user(node=self.input.clusters[0][:self.nodes_init][0])
        current_vseqno = self.get_vbucket_seqnos(self.cluster_to_restore, self.buckets,
                                                 self.skip_consistency, self.per_node)
        self.log.info("*** Start to validate the restore ")
        status, msg = self.validation_helper.validate_restore(self.backupset.end,
                                              self.vbucket_seqno, current_vseqno,
                                              compare_uuid=compare_uuid,
                                              compare=seqno_compare_function,
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

    def validate_backup_data(self, server_host, server_bucket, master_key,
                                   perNode, getReplica, mode, items, key_check,
                                   validate_keys=False,
                                   regex_pattern=None):
        """
            Compare data in backup file with data in bucket
        """
        data_matched = True
        data_collector = DataCollector()
        bk_file_data, _ = data_collector.get_kv_dump_from_backup_file(server_host,
                                      self.cli_command_location, self.cmd_ext,
                                      self.backupset.directory, master_key,
                                      self.buckets)
        restore_file_data = bk_file_data
        regex_backup_data = {}
        if regex_pattern is not None:
            pattern = re.compile("%s" % regex_pattern)
            for bucket in self.buckets:
                regex_backup_data[bucket.name] = {}
                self.log.info("Extract keys with regex pattern '%s' either in key or body"
                                                                          % regex_pattern)
                for key in restore_file_data[bucket.name]:
                    if self.debug_logs: print "key in backup file of bucket %s:  %s" \
                                                               % (bucket.name, key)
                    if validate_keys:
                        if pattern.search(key):
                            regex_backup_data[bucket.name][key] = \
                                     restore_file_data[bucket.name][key]
                        if self.debug_logs:
                            print "\nKeys in backup file of bucket %s that matches "\
                                        "pattern '%s'" % (bucket.name, regex_pattern)
                            for x in regex_backup_data[bucket.name]:
                                print x
                    else:
                        print "value of key in backup file  ", restore_file_data[bucket.name][key]
                        if pattern.search(restore_file_data[bucket.name][key]["Value"]):
                            regex_backup_data[bucket.name][key] = \
                                         restore_file_data[bucket.name][key]
                        if self.debug_logs:
                            print "\nKeys and value in backup file of bucket %s "\
                                  " that matches pattern '%s'" \
                                    % (bucket.name, regex_pattern)
                            for x in regex_backup_data[bucket.name]:
                                print "key: ", x
                                print "value: ", regex_backup_data[bucket.name][x]["Value"]
                restore_file_data = regex_backup_data

        buckets_data = {}
        for bucket in self.buckets:
            headerInfo, bucket_data = data_collector.collect_data(server_bucket, [bucket],
                                                              perNode=False,
                                                              getReplica=getReplica,
                                                              mode=mode)
            buckets_data[bucket.name] = bucket_data[bucket.name]
            for key in buckets_data[bucket.name]:
                value = buckets_data[bucket.name][key]
                if self.backupset.random_keys:
                    value = ",".join(value.split(',')[4:7])
                    value = value[1:-1]
                    value = value.replace('""', '"')
                else:
                    value = ",".join(value.split(',')[4:5])
                buckets_data[bucket.name][key] = value
            self.log.info("*** Compare data in bucket and in backup file of bucket %s ***"
                                                                            % bucket.name)
            count = 0
            key_count = 0
            for key in buckets_data[bucket.name]:
                if restore_file_data[bucket.name]:
                    if buckets_data[bucket.name][key] != restore_file_data[bucket.name][key]["Value"]:
                        if count < 20:
                            self.log.error("Data does not match at key %s. bucket: %s != %s file"
                                           % (key, buckets_data[bucket.name][key],
                                              restore_file_data[bucket.name][key]["Value"]))
                            data_matched = False
                            count += 1
                        else:
                            raise Exception ("Data not match in backup bucket %s" % bucket.name)
                    key_count += 1
                else:
                    raise Exception("Database file is empty")
            if len(restore_file_data[bucket.name]) != key_count:
                raise Exception ("Total key counts do not match.  Backup %s != %s bucket"
                                  % (restore_file_data[bucket.name], key_count))
            self.log.info("******** Data macth in backup file and bucket %s ******** "
                                                                        % bucket.name)
            print "Bucket: ", bucket.name
            print "Total items in backup file:   ", len(bk_file_data[bucket.name])
            if regex_pattern is not None:
                print "Total items to be restored with regex pattern '%s' is %s "\
                                         % (regex_pattern,len(restore_file_data[bucket.name]))
            print "Total items in bucket should be:   ", key_count
            rest = RestConnection(server_bucket[0])
            actual_keys = rest.get_active_key_count(bucket.name)
            print "Total items actual in bucket:      ", actual_keys
            if actual_keys != key_count:
                self.fail("Total keys matched: %s != Total keys in bucket %s: %s" \
                          "at node %s " % (key_count, bucket.name, actual_keys,
                                           server_bucket[0]))
            if self.merged:
                if key_check:
                    self.log.info("Check if deleted keys still in backup after merged" )
                    for key in restore_file_data[bucket.name]:
                        if key == key_check:
                            raise Exception ("There is an old key after delete bucket,"
                                         " backup and merged ")
                    self.log.info("No deleted keys in backup after merged")
        return data_matched

    def get_info_in_database(self, server, bucket, text_search):
        """
            Get info from database file like couch.1
        """
        shell = RemoteMachineShellConnection(server)
        cmd = "%s/couch_dbdump " % self.cli_command_location
        """ since there is no load, it should have only one file per vbucket """
        output, error = shell.execute_command("%s %s/%s/0.couch.* | grep deleted"
                                               % (cmd, self.database_path,
                                                  bucket.name))
        found = False
        if output:
            self.log.info("Search for '%s' in database info" % text_search)
            for x in output:
                if text_search in x:
                    self.log.info("Found %s in database" % text_search)
                    found = True
                    break
        else:
            self.log.info("output is empty")
        shell.disconnect()
        return found

    def validate_help_content(self, output, source):
        """
            Compare content of of the list with sample output
        """
        for x in range(0, len(source)):
            if ' '.join(output[x].split()) != ' '.join(source[x].split()):
                self.log.error("Element %s in output did not match " % x)
                self.log.error("Output => %s != %s <= Source" % (output[x], source[x]))
                raise Exception("Content does not match "
                                "Output => %s != %s <= Source" % (output[x], source[x]))

    def get_cluster_certificate_info(self, server_host, server_cert):
        """
            This will get certificate info from cluster
        """
        cert_file_location = self.root_path + "cert.pem"
        shell = RemoteMachineShellConnection(server_host)
        cmd = "%s/couchbase-cli ssl-manage -c %s:8091 -u Administrator -p password "\
              " --cluster-cert-info > %s" % (self.cli_command_location,
                                                     server_cert.ip,
                                                     cert_file_location)
        output, _ = shell.execute_command(cmd)
        if output and "Error" in output[0]:
            self.fail("Failed to get CA certificate from cluster.")
        shell.disconnect()
        return cert_file_location

    def _create_views(self):
        default_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        default_view_name = "test"
        default_ddoc_name = "ddoc_test"
        prefix = "dev_"
        query = {"full_set": "true", "stale": "false", "connection_timeout": 60000}
        view = View(default_view_name, default_map_func)
        task = self.cluster.async_create_view(self.backupset.cluster_host,
                                              default_ddoc_name, view, "default")
        task.result()

    def validate_backup_views(self, server_host):
        """
            Verify view is backup
        """
        data_collector = DataCollector()
        bk_views_def = data_collector.get_views_definition_from_backup_file(server_host,
                                                               self.backupset.directory,
                                                               self.buckets)
        def_check = ['"id": "_design/dev_ddoc_test"',
            '"json": { "views": { "test": { "map": "function (doc) {\\n  emit(doc._id, doc);\\n}" } } }']
        if bk_views_def:
            self.log.info("Validate views function")
            for x in def_check:
                if x not in bk_views_def:
                    return False, "Missing %s in views definition" % x
            return True, "Views function validated"


class Backupset:
    def __init__(self):
        self.backup_host = None
        self.directory = ''
        self.name = ''
        self.cluster_host = None
        self.cluster_host_username = ''
        self.cluster_host_password = ''
        self.cluster_new_user = None
        self.cluster_new_role = None
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
        self.no_ssl_verify = False
        self.random_keys = False
        self.secure_conn = False
        self.bk_no_cert = False
        self.rt_no_cert = False
        self.backup_list_name = ''
        self.backup_incr_backup = ''
        self.bucket_backup = ''
        self.backup_to_compact = ''
        self.map_buckets = None

class EnterpriseBackupMergeBase(EnterpriseBackupRestoreBase):
    def setUp(self):
        super(EnterpriseBackupMergeBase, self).setUp()
        self.actions = self.input.param("actions", None)
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

    def tearDown(self):
        super(EnterpriseBackupMergeBase, self).tearDown()

    def async_ops_on_buckets(self):
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

    def backup(self):
        self.backup_cluster_validate()

    def backup_with_ops(self):
        ops_tasks = self.async_ops_on_buckets()
        self.backup()
        self.log.info(ops_tasks)
        for task in ops_tasks:
            task.result()

    def merge(self):
        self.backupset.start = 1
        self.backupset.end = len(self.backups)
        self.backup_merge()

    def merge_with_ops(self):
        update_tasks = self._async_load_all_buckets(self.master,
                                                    self.update_gen,
                                                    "update", self.expires)
        delete_tasks = self._async_load_all_buckets(self.master,
                                                    self.delete_gen,
                                                    "delete", self.expires)
        self.merge()
        for task in update_tasks:
            task.result()
        for task in delete_tasks:
            task.result()

    def async_rebalance(self):
        serv_in = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        serv_out = self.servers[
                   self.nodes_init - self.nodes_out:self.nodes_init]
        rebalance = self.cluster.async_rebalance(self.cluster_to_backup,
                                                 serv_in, serv_out)
        return rebalance

    def rebalance(self):
        rebalance = self.async_rebalance()
        rebalance.result()

    def rebalance_with_ops(self):
        update_tasks = self._async_load_all_buckets(self.master,
                                                    self.update_gen,
                                                    "update", self.expires)
        delete_tasks = self._async_load_all_buckets(self.master,
                                                    self.delete_gen,
                                                    "delete", self.expires)
        self.rebalance()
        for task in update_tasks:
            task.result()
        for task in delete_tasks:
            task.result()

    def async_failover(self):
        rest = RestConnection(self.backupset.cluster_host)
        nodes_all = rest.node_statuses()
        for node in nodes_all:
            if node.ip == self.servers[1].ip:
                rest.fail_over(otpNode=node.id, graceful=self.graceful)
                self.sleep(30)
                rest.set_recovery_type(otpNode=node.id,
                                       recoveryType=self.recoveryType)
                rest.add_back_node(otpNode=node.id)
        rebalance = self.cluster.async_rebalance(self.servers, [], [])
        return rebalance

    def failover(self):
        task = self.async_failover()
        task.result()

    def failover_with_ops(self):
        update_tasks = self._async_load_all_buckets(self.master,
                                                    self.update_gen,
                                                    "update", self.expires)

        delete_tasks = self._async_load_all_buckets(self.master,
                                                    self.delete_gen,
                                                    "delete", self.expires)
        self.failover()
        for task in update_tasks:
            task.result()
        for task in delete_tasks:
            task.result()

    def do_backup_merge_actions(self):
        self.backupset.number_of_backups = 0
        actions = self.actions.split(",")
        for action in actions:
            if ":" in action:
                action = action.split(':')
                if action[1].isdigit():
                    iterations = int(action[1])
                    self.backupset.number_of_backups += iterations
                    action = action[0]
                else:
                    iterations = 1
                    params = action[1].split('&')
                    action = action[0]
                    if "failover" in action:
                        if 'hard' in params:
                            self.graceful = False
                        else:
                            self.graceful = True
                        if 'delta' in params:
                            self.recoveryType = 'delta'
                        else:
                            self.recoveryType = 'full'
            else:
                iterations = 1
            for i in range(0, iterations):
                self.backup_merge_actions[action](self)

    backup_merge_actions = {
        "backup": backup,
        "merge": merge,
        "rebalance": rebalance,
        "backup_with_ops": backup_with_ops,
        "merge_with_ops": merge_with_ops,
        "failover": failover
    }
