from basetestcase import BaseTestCase
from enterprise_backup_restore.validation_helpers.valdation_base import ValidationBase
from remote.remote_util import RemoteMachineShellConnection


class EnterpriseBackupRestoreBase(BaseTestCase):
    def setUp(self):
        super(EnterpriseBackupRestoreBase, self).setUp()
        backup_host = self.input.param("backup-host", self.servers[self.nodes_init])
        directory = self.input.param("dir", "/tmp/entbackup")
        backup_name = self.input.param("name", "backup")
        backup_cluster_host = self.input.param("host", self.servers[0])
        cluster_host_username = self.input.param("username", self.servers[0].rest_username)
        cluster_host_password = self.input.param("password", self.servers[0].rest_password)
        restore_cluster_host = self.input.param("restore-host", self.servers[0])
        restore_cluster_host_username = self.input.param("restore-host-username",self.servers[0].rest_username)
        restore_cluster_host_password = self.input.param("restore-host-password", self.servers[0].rest_password)
        exclude_buckets = self.input.param("exclude-buckets", "")
        include_buckets = self.input.param("include-buckets", "")
        disable_bucket_config = self.input.param("disable-bucket-config", False)
        disable_views = self.input.param("disable-views", False)
        disable_gsi_indexes = self.input.param("disable-gsi-indexes", False)
        disable_ft_indexes = self.input.param("disable-ft-indexes", False)
        disable_data = self.input.param("disable-data", False)
        resume = self.input.param("resume", False)
        purge = self.input.param("purge", False)
        threads = self.input.param("threads", self.number_of_processors())
        start = self.input.param("start", 1)
        end = self.input.param("stop", 1)
        backup_number_times = self.input.param("number_of_backups", 1)
        filter_keys = self.input.param("filter-keys", "")
        filter_values = self.input.param("filter-values", "")
        backup_list_name = self.input.param("list-names", None)
        backup_incr_backup = self.input.param("incr-backup", None)
        bucket_backup = self.input.param("bucket-backup", None)
        backup_to_compact = self.input.param("backup-to-compact", 0)
        self.backupset = BackupSet(backup_host, backup_cluster_host, cluster_host_username,
                                   cluster_host_password, restore_cluster_host, restore_cluster_host_username,
                                   restore_cluster_host_password, threads, directory, backup_name, exclude_buckets,
                                   include_buckets, disable_bucket_config, disable_views, disable_gsi_indexes,
                                   disable_ft_indexes, disable_data, resume, purge, start, end, backup_number_times,
                                   filter_keys, filter_values, backup_list_name, backup_incr_backup, bucket_backup,
                                   backup_to_compact)
        self.cli_command_location = "/opt/couchbase/bin"
        self.backups = []
        self.validation_helper = ValidationBase(self.backupset)

    def tearDown(self):
        super(EnterpriseBackupRestoreBase, self).tearDown()
        remote_client = RemoteMachineShellConnection(self.servers[self.nodes_init])
        command = "rm -rf {0}".format(self.input.param("dir", "/tmp/entbackup"))
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)

    def number_of_processors(self):
        remote_client = RemoteMachineShellConnection(self.servers[self.nodes_init])
        command = "nproc"
        output, error = remote_client.execute_command(command)
        if output:
            return output[0]
        else:
            return error[0]


    def backup_create(self):
        args = "create --dir {0} --name {1}".format(self.backupset.dir, self.backupset.name)
        if self.backupset.exclude_buckets:
            args += " --exclude-buckets \"{0}\"".format(",".join(self.backupset.exclude_buckets))
        if self.backupset.include_buckets:
            args += " --include-buckets=\"{0}\"".format(",".join(self.backupset.include_buckets))
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
        command = "{0}/backup {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error or "Backup `{0}` created successfully".format(self.backupset.name) not in output:
            return False, error, "Creating backupset failed."
        else:
            return True, output, "Created backupset with args: {0}".format(args)

    def backup_cluster(self):
        args = "cluster --dir {0} --name {1} --host http://{2}:{3} --username {4} --password {5}". \
            format(self.backupset.dir, self.backupset.name, self.backupset.cluster_host.ip,
                   self.backupset.cluster_host.port, self.backupset.cluster_host_username,
                   self.backupset.cluster_host_password)
        if self.backupset.resume:
            args += "--resume"
        if self.backupset.purge:
            args += "--purge"
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/backup {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if output:
            return False, output, "Taking cluster backup failed."
        else:
            command = "ls -tr {0}/{1} | tail -1".format(self.backupset.dir, self.backupset.name)
            output, error = remote_client.execute_command(command)
            if output:
                self.backups.append(output[0])
            elif error:
                self.backups.append(output[0])
            return True, error, "Finished taking backup  with args: {0}".format(args)

    def backup_restore(self):
        try:
            backup_start = self.backups[int(self.backupset.start) - 1]
        except IndexError:
            backup_start = "{0}{1}".format(self.backups[-1], self.backupset.start)
        try:
            backup_end = self.backups[int(self.backupset.end) - 1]
        except IndexError:
            backup_end = "{0}{1}".format(self.backups[-1], self.backupset.end)
        args = "restore --dir {0} --name {1} --host http://{2}:{3} --username {4} --password {5} --start {6} --end {7} --force-updates". \
            format(self.backupset.dir, self.backupset.name, self.backupset.restore_host.ip,
                   self.backupset.restore_host.port, self.backupset.restore_host_username,
                   self.backupset.restore_host_password, backup_start, backup_end)
        if self.backupset.exclude_buckets:
            args += "--exclude-buckets".format(self.backupset.exclude_buckets)
        if self.backupset.include_buckets:
            args += "--include-buckets {0}".format(self.backupset.include_buckets)
        if self.backupset.disable_bucket_config:
            args += "--disable-bucket-config {0}".format(self.backupset.disable_bucket_config)
        if self.backupset.disable_views:
            args += "--disable-views {0}".format(self.backupset.disable_views)
        if self.backupset.disable_gsi_indexes:
            args += "--disable-gsi-indexes {0}".format(self.backupset.disable_gsi_indexes)
        if self.backupset.disable_ft_indexes:
            args += "--disable-ft-indexes {0}".format(self.backupset.disable_ft_indexes)
        if self.backupset.disable_data:
            args += "--disable-data {0}".format(self.backupset.disable_data)
        if self.backupset.filter_keys:
            args += "--filter_keys {0}".format(self.backupset.filter_keys)
        if self.backupset.filter_values:
            args += "--filter_values {0}".format(self.backupset.filter_values)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/backup {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if output or "Transfer plan finished successfully" not in error[-1]:
            return False, output, "Restoring backup failed."
        else:
            return True, error, "Restored backup with args: {0}".format(args)

    def backup_list(self):
        args = "list --dir {0}".format(self.backupset.dir)
        if self.backupset.backup_list_name:
            args += "--name {0}".format(self.backupset.backup_list_name)
        if self.backupset.backup_incr_backup:
            args += "--incr-backup {0}".format(self.backupset.backup_incr_backup)
        if self.backupset.bucket_backup:
            args += "--bucket-backup {0}".format(self.backupset.bucket_backup)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/backup {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error:
            return False, error, "Getting backup list failed."
        else:
            return True, output, "Backup list obtained"

    def backup_compact(self):
        try:
            backup_to_compact = self.buckets[int(self.backupset.backup_to_compact)]
        except IndexError:
            backup_to_compact = "{0}{1}".format(self.buckets[-1], self.backupset.backup_to_compact)
        args = "compact --dir {0} --name {1} --backup {2}".format(self.backupset.dir, self.backupset.name, backup_to_compact)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/backup {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error:
            return False, error, "Compacting backup failed."
        else:
            return True, output, "Compaction of backup success"

    def backup_remove(self):
        args = "remove --dir {0} --name {1}".format(self.backupset.dir, self.backupset.name)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/backup {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        if error:
            return False, error, "Removing backup failed."
        else:
            return True, output, "Removing of backup success"


class BackupSet(object):
    def __init__(self, backup_host, cluster_host, cluster_host_username,
                 cluster_host_password, restore_host, restore_host_username, restore_host_password, threads, directory="/tmp/entbackup", name="backup",
                 exclude_buckets="", include_buckets="", disable_bucket_config=False,
                 disable_views=False, disable_gsi_indexes=False, disable_ft_indexes=False,
                 disable_data=False, resume=False, purge=False, start=1, end=1, number_of_backups=1,
                 filter_keys="", filter_values="", backup_list_name="", backup_incr_backup="", bucket_backup="",
                 backup_to_compact=""):
        self.backup_host = backup_host
        self.dir = directory
        self.name = name
        self.cluster_host = cluster_host
        self.cluster_host_username = cluster_host_username
        self.cluster_host_password = cluster_host_password
        self.restore_host = restore_host
        self.restore_host_username = restore_host_username
        self.restore_host_password = restore_host_password
        self.threads = threads
        self.exclude_buckets = exclude_buckets.split(',') if exclude_buckets else []
        self.include_buckets = include_buckets.split(',') if include_buckets else []
        self.disable_bucket_config = disable_bucket_config
        self.disable_views = disable_views
        self.disable_gsi_indexes = disable_gsi_indexes
        self.disable_ft_indexes = disable_ft_indexes
        self.disable_data = disable_data
        self.resume = resume
        self.purge = purge
        self.start = start
        self.end = end
        self.number_of_backups = number_of_backups
        self.filter_keys = filter_keys
        self.filter_values = filter_values
        self.backup_list_name = backup_list_name
        self.backup_incr_backup = backup_incr_backup
        self.bucket_backup = bucket_backup
        self.backup_to_compact = backup_to_compact
        self.backups = []
        for i in range(1, self.number_of_backups + 1):
            self.backups.append("backup{0}".format(i))

    def buckets_list(self, buckets):
        self.buckets = []
        for bucket in buckets:
            self.buckets.append(bucket)

