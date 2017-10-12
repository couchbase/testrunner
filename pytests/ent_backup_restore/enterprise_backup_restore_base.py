import copy
import os, re
import shutil
import urllib

from basetestcase import BaseTestCase
from couchbase_helper.data_analysis_helper import DataCollector
from couchbase_helper.documentgenerator import BlobGenerator,DocumentGenerator
from ent_backup_restore.validation_helpers.backup_restore_validations \
                                                 import BackupRestoreValidations
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection, RestHelper, Bucket as \
    RestBucket
from couchbase_helper.document import View
from testconstants import LINUX_COUCHBASE_BIN_PATH,\
                          COUCHBASE_DATA_PATH, WIN_COUCHBASE_DATA_PATH,\
                          WIN_COUCHBASE_BIN_PATH_RAW, WIN_TMP_PATH_RAW,\
                          MAC_COUCHBASE_BIN_PATH, LINUX_ROOT_PATH, WIN_ROOT_PATH,\
                          WIN_TMP_PATH, STANDARD_BUCKET_PORT
from membase.api.rest_client import RestConnection
from couchbase.bucket import Bucket
from lib.memcached.helper.data_helper import MemcachedClientHelper

SOURCE_CB_PARAMS = {
                      "authUser": "default",
                      "authPassword": "",
                      "authSaslUser": "",
                      "authSaslPassword": "",
                      "clusterManagerBackoffFactor": 0,
                      "clusterManagerSleepInitMS": 0,
                      "clusterManagerSleepMaxMS": 20000,
                      "dataManagerBackoffFactor": 0,
                      "dataManagerSleepInitMS": 0,
                      "dataManagerSleepMaxMS": 20000,
                      "feedBufferSizeBytes": 0,
                      "feedBufferAckThreshold": 0
                    }
INDEX_DEFINITION = {
                          "type": "fulltext-index",
                          "name": "",
                          "uuid": "",
                          "params": {},
                          "sourceType": "couchbase",
                          "sourceName": "default",
                          "sourceUUID": "",
                          "sourceParams": SOURCE_CB_PARAMS,
                          "planParams": {}
                        }


class EnterpriseBackupRestoreBase(BaseTestCase):
    def setUp(self):
        super(EnterpriseBackupRestoreBase, self).setUp()
        """ from version 4.6.0 and later, --host flag is deprecated """
        self.cluster_flag = "--cluster"
        self.backupset = Backupset()
        self.cmd_ext = ""
        self.should_fail = self.input.param("should-fail", False)
        self.database_path = COUCHBASE_DATA_PATH
        self.cli_command_location = LINUX_COUCHBASE_BIN_PATH
        self.debug_logs = self.input.param("debug_logs", False)
        self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        self.backupset.passwd_env = self.input.param("passwd-env", False)
        self.backupset.passwd_env_with_prompt = \
            self.input.param("passwd-env-with-prompt", False)
        shell = RemoteMachineShellConnection(self.servers[0])
        info = shell.extract_remote_info().type.lower()
        self.root_path = LINUX_ROOT_PATH
        self.os_name = "linux"
        self.tmp_path = "/tmp/"
        self.long_help_flag = "--help"
        self.short_help_flag = "-h"
        if info == 'linux':
            self.cli_command_location = LINUX_COUCHBASE_BIN_PATH
            self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
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

        self.backup_validation_files_location = "/tmp/backuprestore" + self.master.ip
        self.backupset.backup_host = self.input.clusters[1][0]
        self.backupset.name = self.input.param("name", "backup")
        self.non_master_host = self.input.param("non-master", False)
        self.compact_backup = self.input.param("compact-backup", False)
        self.merged = self.input.param("merged", None)
        self.after_upgrade_merged = self.input.param("after-upgrade-merged", False)
        self.create_gsi = self.input.param("create-gsi", False)
        self.do_restore = self.input.param("do-restore", False)
        self.do_verify = self.input.param("do-verify", False)
        self.create_views = self.input.param("create-views", False)
        self.create_fts_index = self.input.param("create-fts-index", False)
        self.cluster_new_user = self.input.param("new_user", None)
        self.cluster_new_role = self.input.param("new_role", None)
        self.restore_only = self.input.param("restore-only", False)
        self.force_version_upgrade = self.input.param("force-version-upgrade", None)
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
        self.backupset.number_of_backups_after_upgrade = \
            self.input.param("number_of_backups_after_upgrade", 0)
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
        self.add_node_services = self.input.param("add-node-services", "kv")
        self.backupset.backup_compressed = \
            self.input.param("backup-conpressed", False)
        self.backups = []
        self.validation_helper = BackupRestoreValidations(self.backupset,
                                                          self.cluster_to_backup,
                                                          self.cluster_to_restore,
                                                          self.buckets,
                                                          self.backup_validation_files_location,
                                                          self.backups,
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
            self.tmp_path = "/tmp/"
            if info == 'linux' or info == 'mac':
                backup_directory = "/tmp/entbackup"
            elif info == 'windows':
                backup_directory = WIN_TMP_PATH_RAW + "entbackup"
            else:
                raise Exception("OS not supported.")
            validation_files_location = "%sbackuprestore" % self.tmp_path + self.master.ip
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
                    try:
                        self.backup_reset_clusters(servers)
                    except:
                        self.log.error("was not able to cleanup cluster the first time")
                        self.backup_reset_clusters(servers)
            if os.path.exists(validation_files_location):
                self.log.info("delete dir %s" % validation_files_location)
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
        cacert = ""
        if self.backupset.secure_conn:
            cacert = self.get_cluster_certificate_info(self.backupset.backup_host,
                                                       self.backupset.cluster_host)
            secure_port = "1"
            url_format = "s"
        password_input = "--password %s " % self.backupset.cluster_host_password
        if self.backupset.passwd_env:
            password_input = ""
        elif self.backupset.passwd_env_with_prompt:
            password_input = "-p "
        if "4.6" <= RestConnection(self.backupset.cluster_host).get_nodes_version():
            self.cluster_flag = "--cluster"

        args = "backup --archive {0} --repo {1} {6} http{7}://{2}:{8}{3} --username" \
               " {4} {5}".format(self.backupset.directory, self.backupset.name,
                                 self.backupset.cluster_host.ip,
                                 self.backupset.cluster_host.port,
                                 self.backupset.cluster_host_username,
                                 password_input,
                                 self.cluster_flag, url_format,
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
        if self.backupset.backup_compressed:
            args += " --value-compression compressed"
        password_env = ""
        if self.backupset.passwd_env:
            self.log.info("set password env to password")
            password_env = "export CB_PASSWORD=password; "
        if self.backupset.passwd_env_with_prompt:
            self.log.info("set password env to prompt")
            password_env = "unset CB_PASSWORD; export CB_PASSWORD;"
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{2} {0}/cbbackupmgr {1}".format(self.cli_command_location, args,
                                                   password_env)

        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)

        if error or (output and "Backup successfully completed" not in output):
            return output, error
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = remote_client.execute_command(command)
        if o:
            self.backups.append(o[0])
        self.number_of_backups_taken += 1
        self.log.info("Finished taking backup  with args: {0}".format(args))
        return output, error

    def backup_cluster_validate(self, skip_backup=False, repeats=1):
        if not skip_backup:
            output, error = self.backup_cluster()
            if error or "Backup successfully completed" not in output[-1]:
                self.fail("Taking cluster backup failed.")
        self.backup_list()
        if repeats < 2:
            status, msg = self.validation_helper.validate_backup()
            if not status:
                self.fail(msg)
            self.log.info(msg)
        if not self.backupset.deleted_buckets:
            if not self.backupset.force_updates:
                self.store_vbucket_seqno()
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location)

    def backup_restore(self, expected_error=None):
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
        password_input = "--password %s " % self.backupset.restore_cluster_host_password
        if self.backupset.passwd_env:
            password_input = ""
        elif self.backupset.passwd_env_with_prompt:
            password_input = "-p "
        if "4.6" <= RestConnection(self.backupset.restore_cluster_host).get_nodes_version():
            self.cluster_flag = "--cluster"

        args = "restore --archive {0} --repo {1} {2} http{9}://{3}:{10}{4}" \
               " --username {5} {6} --start {7} --end {8}" \
            .format(self.backupset.directory,
                    self.backupset.name,
                    self.cluster_flag,
                    self.backupset.restore_cluster_host.ip,
                    self.backupset.restore_cluster_host.port,
                    self.backupset.restore_cluster_host_username,
                    password_input,
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
                        self.fail("Bucket %s not created after 120 seconds."
                                  % bucket_name)
                buckets.append("%s=%s" % (bucket.name, bucket_name))
                count +=1
            bucket_maps = ",".join(buckets)
        if self.backupset.map_buckets:
            args += " --map-buckets %s " % bucket_maps
        password_env = ""
        if self.backupset.passwd_env:
            self.log.info("set password env to password")
            password_env = "export CB_PASSWORD=password; "
        if self.backupset.passwd_env_with_prompt:
            self.log.info("set password env to prompt")
            password_env = "unset CB_PASSWORD; export CB_PASSWORD;"
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{2} {0}/cbbackupmgr {1}".format(self.cli_command_location, args,
                                                   password_env)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if "Error restoring cluster" in output[0]:
            if not self.should_fail:
                self.fail("Failed to restore cluster")
            else:
                self.log.info("This test is for negative test")
        res = output
        res.extend(error)
        error_str = "Error restoring cluster: Transfer failed. " \
                    "Check the logs for more information."
        if error_str in res:
            command = "cat " + self.backupset.directory + \
                      "/logs/backup.log | grep '" + error_str + "' -A 10 -B 100"
            output, error = remote_client.execute_command(command)
            remote_client.log_command_output(output, error)
        if 'Required Flags:' in res:
            self.fail("Command line failed. Please check test params.")
        return output, error

    def backup_restore_validate(self, compare_uuid=False,
                                seqno_compare_function="==",
                                replicas=False,
                                mode="memory",
                                expected_error=None):
        output, error =self.backup_restore(expected_error=expected_error)
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

        current_vseqno = {}
        if not self.backupset.force_updates:
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
                                                                  self.backups[int(self.backupset.backup_to_compact)])
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
        if self.backupset.deleted_backups:
            self.backupset.end -= len(self.backupset.deleted_backups)
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

    def backup_merge_validate(self, repeats=1):
        status, output, message = self.backup_merge()
        if not status:
            self.fail(message)
        if repeats < 2:
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location, merge=True)

            self.validation_helper.validate_merge(self.backup_validation_files_location)

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
                            print "\nKeys in backup file of bucket %s that matches " \
                                  "pattern '%s'" % (bucket.name, regex_pattern)
                            for x in regex_backup_data[bucket.name]:
                                print x
                    else:
                        print "value of key in backup file  ", restore_file_data[bucket.name][key]
                        if pattern.search(restore_file_data[bucket.name][key]["Value"]):
                            regex_backup_data[bucket.name][key] = \
                                restore_file_data[bucket.name][key]
                        if self.debug_logs:
                            print "\nKeys and value in backup file of bucket %s " \
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
                            raise Exception("Data not match in backup bucket %s" % bucket.name)
                    key_count += 1
                else:
                    raise Exception("Database file is empty")
            if len(restore_file_data[bucket.name]) != key_count:
                raise Exception("Total key counts do not match.  Backup %s != %s bucket"
                                % (restore_file_data[bucket.name], key_count))
            self.log.info("******** Data macth in backup file and bucket %s ******** "
                          % bucket.name)
            print "Bucket: ", bucket.name
            print "Total items in backup file:   ", len(bk_file_data[bucket.name])
            if regex_pattern is not None:
                print "Total items to be restored with regex pattern '%s' is %s " \
                      % (regex_pattern, len(restore_file_data[bucket.name]))
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
                    self.log.info("Check if deleted keys still in backup after merged")
                    for key in restore_file_data[bucket.name]:
                        if key == key_check:
                            raise Exception("There is an old key after delete bucket,"
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
            output[x] = output[x].strip()
            source[x] = source[x].strip()
            if not isinstance(output[x], str):
                continue
            if " ".join(output[x].split()) != " ".join(source[x].split()):
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
        cmd = "%s/couchbase-cli ssl-manage -c %s:8091 -u Administrator -p password " \
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

    def get_database_file_info(self):
        """
           Extract database file size and items from backup repo
           :return: Datebase file information
        """
        status, output, message = self.backup_list()
        file_info = {}
        unit_size = ["MB", "GB"]
        if status:
            if output:
                for x in output:
                    if "shard_0.fdb" in x:
                        if x.strip().split()[0][-2:] in unit_size:
                            file_info["file_size"] = \
                                int(x.strip().split()[0][:-2].split(".")[0])

                            file_info["items"] = int(x.strip().split()[1])
                        print "output content   ", file_info
            return file_info
        else:
            print message

    def validate_backup_compressed_file(self, no_compression, with_compression):
        """
            Compare before and after using flag --value-compression compressed
        :return: None
        """
        compressed = False
        if no_compression["items"] == with_compression["items"]:
            if no_compression["file_size"] > with_compression["file_size"]:
                compressed = True
                self.log.info("no compressed: %d, with compressed: %d"
                              % (no_compression["file_size"],
                                 with_compression["file_size"]))
            else:
                self.log.info("no compressed: %d, with compressed: %d"
                              % (no_compression["file_size"],
                                 with_compression["file_size"]))
                self.fail("cbbackupmgr failed to compress database")

        else:
            self.fail("Item miss match on 2 backup files")

    def create_indexes(self):
        gsi_type = "memory_optimized"
        self.gsi_names = ["num1", "num2"]
        rest = RestConnection(self.backupset.cluster_host)
        if "5" <= rest.get_nodes_version()[:1]:
            gsi_type = "plasma"
        cmd = "cbindex -type create -bucket default -using %s -index " \
              "%s -fields=Num1" % (gsi_type, self.gsi_names[0])
        shell = RemoteMachineShellConnection(self.backupset.cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        self.log.info("Create gsi indexes")
        output, error = shell.execute_command(command)
        if self.debug_logs:
            self.log.info("\noutput gsi:   %s" % output)
        cmd = "cbindex -type create -bucket default -using %s -index " \
              "%s -fields=Num2" % (gsi_type, self.gsi_names[1])
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        shell.execute_command(command)
        shell.disconnect()

    def verify_gsi(self):
        cmd = "cbindex -type list"
        shell = RemoteMachineShellConnection(self.backupset.restore_cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        index_found = 0
        if len(output) > 1:
            for name in self.gsi_names:
                for x in output:
                    if "Index:default/%s" % name in x:
                        index_found += 1
                        self.log.info("GSI index name %s created in restore cluster "
                                      "as expected" % name)
        if index_found < len(self.gsi_names):
            self.fail("Some GSI index is not created in restore cluster.")


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
        self.number_of_backups_after_upgrade = 1
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
        self.backup_compressed = False
        self.passwd_env = False
        self.passwd_env_with_prompt = False
        self.deleted_buckets = []
        self.new_buckets = []
        self.flushed_buckets = []
        self.deleted_backups = []


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
        self.backup_to_corrupt = 0
        self.backup_to_delete = 0
        self.skip_restore_indexes = self.input.param("skip_restore_indexe", False)
        self.overwrite_indexes = self.input.param("overwrite_indexes", False)

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

    def backup_with_memcached_crash_and_restart(self):
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                                          backup_host=self.backupset.backup_host,
                                                          directory=self.backupset.directory, name=self.backupset.name,
                                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                                          no_progress_bar=self.no_progress_bar,
                                                          cli_command_location=self.cli_command_location,
                                                          cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.pause_memcached()
        conn.unpause_memcached()
        output = backup_result.result(timeout=200)
        self.assertTrue("Backup successfully completed" in output[0],
                        "Backup failed with memcached crash and restart within 180 seconds")
        self.log.info("Backup succeeded with memcached crash and restart within 180 seconds")
        self.sleep(30)
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = conn.execute_command(command)
        if o:
            self.backups.append(o[0])
        conn.log_command_output(o, e)
        self.number_of_backups_taken += 1
        self.store_vbucket_seqno()
        self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                          self.backup_validation_files_location)
        self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                            self.backup_validation_files_location)
        self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)

    def backup_with_erlang_crash_and_restart(self):
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                                          backup_host=self.backupset.backup_host,
                                                          directory=self.backupset.directory, name=self.backupset.name,
                                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                                          no_progress_bar=self.no_progress_bar,
                                                          cli_command_location=self.cli_command_location,
                                                          cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.kill_erlang()
        conn.start_couchbase()
        output = backup_result.result(timeout=200)
        self.assertTrue("Backup successfully completed" in output[0],
                        "Backup failed with erlang crash and restart within 180 seconds")
        self.log.info("Backup succeeded with erlang crash and restart within 180 seconds")
        self.sleep(30)
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = conn.execute_command(command)
        if o:
            self.backups.append(o[0])
        conn.log_command_output(o, e)
        self.number_of_backups_taken += 1
        self.store_vbucket_seqno()
        self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                          self.backup_validation_files_location)
        self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                            self.backup_validation_files_location)
        self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)

    def backup_with_cb_server_stop_and_restart(self):
        backup_result = self.cluster.async_backup_cluster(cluster_host=self.backupset.cluster_host,
                                                          backup_host=self.backupset.backup_host,
                                                          directory=self.backupset.directory, name=self.backupset.name,
                                                          resume=self.backupset.resume, purge=self.backupset.purge,
                                                          no_progress_bar=self.no_progress_bar,
                                                          cli_command_location=self.cli_command_location,
                                                          cb_version=self.cb_version)
        self.sleep(10)
        conn = RemoteMachineShellConnection(self.backupset.cluster_host)
        conn.stop_couchbase()
        conn.start_couchbase()
        output = backup_result.result(timeout=200)
        self.assertTrue("Backup successfully completed" in output[0],
                        "Backup failed with couchbase stop and start within 180 seconds")
        self.log.info("Backup succeeded with couchbase stop and start within 180 seconds")
        self.sleep(30)
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = conn.execute_command(command)
        if o:
            self.backups.append(o[0])
        conn.log_command_output(o, e)
        self.number_of_backups_taken += 1
        self.store_vbucket_seqno()
        self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                          self.backup_validation_files_location)
        self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                            self.backup_validation_files_location)
        self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)

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

    def merge_with_memcached_crash_and_restart(self):
        self.log.info("backups before merge: " + str(self.backups))
        self.log.info("number_of_backups_taken before merge: " + str(self.number_of_backups_taken))
        merge_result = self.cluster.async_merge_cluster(backup_host=self.backupset.backup_host,
                                                        backups=self.backups,
                                                        directory=self.backupset.directory, name=self.backupset.name,
                                                        cli_command_location=self.cli_command_location,
                                                        start=int(self.backupset.start),
                                                        end=int(self.backupset.end)
                                                        )
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        conn.pause_memcached()
        conn.unpause_memcached()
        output = merge_result.result(timeout=200)
        self.assertTrue("Merge completed successfully" in output[0],
                        "Merge failed with memcached crash and restart within 180 seconds")
        self.log.info("Merge succeeded with memcached crash and restart within 180 seconds")
        self.sleep(30)
        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = conn.execute_command(command)
        if o:
            self.backups.insert(self.backupset.start - 1, o[0])
        self.number_of_backups_taken -= (self.backupset.end - self.backupset.start + 1)
        self.number_of_backups_taken += 1
        self.log.info("backups after merge: " + str(self.backups))
        self.log.info("number_of_backups_taken after merge: " + str(self.number_of_backups_taken))
        if self.number_of_repeats < 2:
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location)
            self.validation_helper.validate_merge(self.backup_validation_files_location)

    def merge_with_erlang_crash_and_restart(self):
        self.log.info("backups before merge: " + str(self.backups))
        self.log.info("number_of_backups_taken before merge: " + str(self.number_of_backups_taken))
        merge_result = self.cluster.async_merge_cluster(backup_host=self.backupset.backup_host,
                                                        backups=self.backups,
                                                        directory=self.backupset.directory, name=self.backupset.name,
                                                        cli_command_location=self.cli_command_location,
                                                        start=int(self.backupset.start),
                                                        end=int(self.backupset.end)
                                                        )
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        conn.kill_erlang()
        conn.start_couchbase()
        output = merge_result.result(timeout=200)
        self.assertTrue("Merge completed successfully" in output[0],
                        "Merge failed with erlang crash and restart within 180 seconds")
        self.log.info("Merge succeeded with erlang crash and restart within 180 seconds")
        self.sleep(30)
        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = conn.execute_command(command)
        if o:
            self.backups.insert(self.backupset.start - 1, o[0])
        self.number_of_backups_taken -= (self.backupset.end - self.backupset.start + 1)
        self.number_of_backups_taken += 1
        self.log.info("backups after merge: " + str(self.backups))
        self.log.info("number_of_backups_taken after merge: " + str(self.number_of_backups_taken))
        if self.number_of_repeats < 2:
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location)
            self.validation_helper.validate_merge(self.backup_validation_files_location)

    def merge_with_cb_server_stop_and_restart(self):
        self.log.info("backups before merge: " + str(self.backups))
        self.log.info("number_of_backups_taken before merge: " + str(self.number_of_backups_taken))
        merge_result = self.cluster.async_merge_cluster(backup_host=self.backupset.backup_host,
                                                        backups=self.backups,
                                                        directory=self.backupset.directory, name=self.backupset.name,
                                                        cli_command_location=self.cli_command_location,
                                                        start=int(self.backupset.start),
                                                        end=int(self.backupset.end)
                                                        )
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        conn.stop_couchbase()
        conn.start_couchbase()
        output = merge_result.result(timeout=200)
        self.assertTrue("Merge completed successfully" in output[0],
                        "Merge failed with couchbase stop and start within 180 seconds")
        self.log.info("Merge succeeded with couchbase stop and start within 180 seconds")
        self.sleep(30)
        del self.backups[self.backupset.start - 1:self.backupset.end]
        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = conn.execute_command(command)
        if o:
            self.backups.insert(self.backupset.start - 1, o[0])
        self.number_of_backups_taken -= (self.backupset.end - self.backupset.start + 1)
        self.number_of_backups_taken += 1
        self.log.info("backups after merge: " + str(self.backups))
        self.log.info("number_of_backups_taken after merge: " + str(self.number_of_backups_taken))
        if self.number_of_repeats < 2:
            self.validation_helper.store_keys(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                              self.backup_validation_files_location)
            self.validation_helper.store_latest(self.cluster_to_backup, self.buckets, self.number_of_backups_taken,
                                                self.backup_validation_files_location)
            self.validation_helper.store_range_json(self.buckets, self.number_of_backups_taken,
                                                    self.backup_validation_files_location)
            self.validation_helper.validate_merge(self.backup_validation_files_location)

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
        failovered = False
        for node in nodes_all:
            if node.ip == ip_to_failover:
                status = rest.fail_over(otpNode=node.id, graceful=self.graceful)
                if status:
                    failovered = True
        if failovered and self.graceful:
            rest.monitorRebalance()

    def async_failover_and_recover(self):
        """
            Asynchronously failover a node and add back the node after 30 sec
            :return: Task that is running the rebalance at end.
        """
        rest = RestConnection(self.backupset.cluster_host)
        nodes_all = rest.node_statuses()
        for node in nodes_all:
            if node.ip == self.servers[1].ip:
                failovered = False
                status = rest.fail_over(otpNode=node.id, graceful=self.graceful)
                if status:
                    failovered = True
                if failovered and self.graceful:
                    rest.monitorRebalance()
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

    def create_bucket_with_ops(self):
        """
        Create new buckets and populate the bucket with items while ops are on
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        bucket_size = self._reconfigure_bucket_memory(self.new_buckets)
        rest = RestConnection(self.master)
        server_id = rest.get_nodes_self().id
        bucket_tasks = []
        bucket_params = copy.deepcopy(
            self.bucket_base_params['membase']['non_ephemeral'])
        bucket_params['size'] = bucket_size
        if self.ephemeral:
            bucket_params['bucket_type'] = 'ephemeral'
            bucket_params['eviction_policy'] = 'noEviction'
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

            bucket_params['bucket_priority'] = bucket_priority
            bucket_tasks.append(
                self.cluster.async_create_standard_bucket(name=name, port=port,
                                                          bucket_params=bucket_params))
            bucket = Bucket(name=name, authType=None, saslPassword=None,
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
        for task in ops_tasks:
            task.result()
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

    def delete_bucket_with_ops(self):
        """
        Delete a bucket from the cluster while bucket operations are on
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.log.info("Deleting a bucket")
        bucket_to_delete = self.buckets[-1].name
        self.backupset.deleted_buckets.append(bucket_to_delete)
        BucketOperationHelper.delete_bucket_or_assert(self.master,
                                                      bucket_to_delete,
                                                      self)
        self.buckets.pop()
        for task in ops_tasks:
            task.result()

    def delete_backup(self):
        """
        Delete an incr backup
        :return: Nothing
        """
        backup_to_delete = self.backups.pop(self.backup_to_delete - 1)
        self.log.info("Deleting backup " + backup_to_delete)
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        o, e = conn.execute_command(
            "rm -rf " + self.backupset.directory + "/" + self.backupset.name + "/" + backup_to_delete)
        conn.log_command_output(o, e)
        self.backupset.deleted_backups.append(backup_to_delete)
        self.number_of_backups_taken -= 1

    def corrupt_backup(self):
        """
        Corrupt an incr backup
        :return: Nothing
        """
        backup_to_corrupt = self.backups[self.backup_to_corrupt - 1]
        self.log.info("Corrupting backup " + backup_to_corrupt)
        conn = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "ls -tr {0}/{1}/{2} | tail".format(self.backupset.directory, self.backupset.name,
                                                     backup_to_corrupt)
        o, e = conn.execute_command(command)
        conn.log_command_output(o, e)
        data_dir = o[0]
        o, e = conn.execute_command("dd if=/dev/zero of=" + self.backupset.directory + "/" + self.backupset.name + "/" +
                                    backup_to_corrupt + "/" + data_dir + "/data/shard_0.fdb" +
                                    " bs=1024 count=100 seek=10 conv=notrunc")
        conn.log_command_output(o, e)

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

    def flush_buckets_with_ops(self):
        """
        Flush buckets from the cluster while ops are on
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self.log.info("Flushing {} buckets".format(self.bucket_to_flush))
        for i in range(self.buckets.__len__() - self.bucket_to_flush,
                       self.buckets.__len__()):
            self._flush_bucket(i)
        for task in ops_tasks:
            task.result()

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

    def compact_buckets_with_ops(self):
        """
        Compact all the buckets in the cluster while ops are on
        :return: Nothing
        """
        ops_tasks = self.async_ops_on_buckets()
        self._run_compaction(number_of_times=1)
        for task in ops_tasks:
            task.result()

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

    def create_indexes(self):
        rest_src = RestConnection(self.backupset.cluster_host)

        rest_src.set_indexer_storage_mode(storageMode="memory_optimized")

        cmd = "cbindex -type create -bucket default -using memory_optimized -index " \
              "age_idx -fields=age"
        remote_client = RemoteMachineShellConnection(
            self.backupset.cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if "Index created" not in output[-1]:
            self.fail("GSI index cannot be created")
        cmd = "cbindex -type create -bucket default -using memory_optimized -index " \
              "name_idx -fields=name"
        remote_client = RemoteMachineShellConnection(
            self.backupset.cluster_host)
        command = "{0}/{1}".format(self.cli_command_location, cmd)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if "Index created" not in output[-1]:
            self.fail("GSI index cannot be created")

        index_definition = INDEX_DEFINITION
        index_name = index_definition['name'] = "age"
        try:
            self.log.info("Create fts index")
            rest_src.create_fts_index(index_name, index_definition)
        except Exception, ex:
            self.fail(ex)

        if not self.skip_restore_indexes:
            rest_src = RestConnection(self.backupset.restore_cluster_host)

            if self.overwrite_indexes:
                cmd = "cbindex -type create -bucket default -using memory_optimized -index " \
                      "age_idx1 -fields=age"
                remote_client = RemoteMachineShellConnection(
                    self.backupset.restore_cluster_host)
                command = "{0}/{1}".format(self.cli_command_location, cmd)
                output, error = remote_client.execute_command(command)
                remote_client.log_command_output(output, error)
                if error or "Index created" not in output[-1]:
                    self.fail("GSI index cannot be created")

                index_definition = INDEX_DEFINITION
                index_name = index_definition['name'] = "age1"
                try:
                    self.log.info("Create fts index")
                    rest_src.create_fts_index(index_name, index_definition)
                except Exception, ex:
                    self.fail(ex)

    def update_indexes(self):
        index_definition = INDEX_DEFINITION
        index_definition['name'] = "age1"
        rest_fts = RestConnection(self.backupset.cluster_host)
        try:
            self.log.info("Update fts index")
            rest_fts.update_fts_index("age1", index_definition)
        except Exception, ex:
            self.fail(ex)

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
                elif "delete_bkup" in action:
                    iterations = 1
                    self.backup_to_delete = int(params[0])
                elif "corrupt_bkup" in action:
                    iterations = 1
                    self.backup_to_corrupt = int(params[0])
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
        "backup_with_memcached_crash_and_restart": backup_with_memcached_crash_and_restart,
        "backup_with_erlang_crash_and_restart": backup_with_erlang_crash_and_restart,
        "backup_with_cb_server_stop_and_restart": backup_with_cb_server_stop_and_restart,
        "merge": merge,
        "merge_with_memcached_crash_and_restart": merge_with_memcached_crash_and_restart,
        "merge_with_erlang_crash_and_restart": merge_with_erlang_crash_and_restart,
        "merge_with_cb_server_stop_and_restart": merge_with_cb_server_stop_and_restart,
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
        "create_buckets_with_ops": create_bucket_with_ops,
        "recreate_buckets": create_new_bucket_and_populate,
        "flush_buckets": flush_buckets,
        "flush_buckets_with_ops": flush_buckets_with_ops,
        "load_flushed_buckets": load_flushed_buckets,
        "delete_buckets": delete_bucket,
        "delete_buckets_with_ops": delete_bucket_with_ops,
        "compact_buckets": compact_buckets,
        "compact_buckets_with_ops": compact_buckets_with_ops,
        "rollback": rollback,
        "delete_bkup": delete_backup,
        "corrupt_bkup": corrupt_backup,
        "create_indexes": create_indexes,
        "update_indexes": update_indexes
    }
