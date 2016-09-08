
from remote.remote_util import RemoteMachineShellConnection

class CouchbaseCLI():

    def __init__(self, server, username, password):
        self.server = server
        self.hostname = "%s:%s" % (server.ip, server.port)
        self.username = username
        self.password = password

    def bucket_create(self, name, password, type, quota, eviction_policy, replica_count, enable_replica_indexes,
                      priority, enable_flush, wait):
        options = self._get_default_options()
        if name is not None:
            options += " --bucket " + name
        if password is not None:
            options += " --bucket-password " + password
        if type is not None:
            options += " --bucket-type " + type
        if quota is not None:
            options += " --bucket-ramsize " + str(quota)
        if eviction_policy is not None:
            options += " --bucket-eviction-policy " + eviction_policy
        if replica_count is not None:
            options += " --bucket-replica " + str(replica_count)
        if enable_replica_indexes is not None:
            options += " --enable-index-replica " + str(enable_replica_indexes)
        if priority is not None:
            options += " --bucket-priority " + priority
        if enable_flush is not None:
            options += " --enable-flush " + str(enable_flush)
        if wait:
            options += " --wait"

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-create", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket created")

    def bucket_compact(self, bucket_name, data_only, views_only):
        options = self._get_default_options()
        if bucket_name is not None:
            options += " --bucket " + bucket_name
        if data_only:
            options += " --data-only"
        if views_only:
            options += " --view-only"

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-compact", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket compaction started")

    def bucket_delete(self, bucket_name):
        options = self._get_default_options()
        if bucket_name is not None:
            options += " --bucket " + bucket_name

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-delete", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket deleted")

    def bucket_edit(self, name, password, quota, eviction_policy, replica_count, priority, enable_flush):
        options = self._get_default_options()
        if name is not None:
            options += " --bucket " + name
        if password is not None:
            options += " --bucket-password " + password
        if quota is not None:
            options += " --bucket-ramsize " + str(quota)
        if eviction_policy is not None:
            options += " --bucket-eviction-policy " + eviction_policy
        if replica_count is not None:
            options += " --bucket-replica " + str(replica_count)
        if priority is not None:
            options += " --bucket-priority " + priority
        if enable_flush is not None:
            options += " --enable-flush " + str(enable_flush)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-edit", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket edited")

    def bucket_flush(self, name, force):
        options = self._get_default_options()
        if name is not None:
            options += " --bucket " + name
        if force:
            options += " --force"

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-flush", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket flushed")

    def cluster_edit(self, data_ramsize, index_ramsize, fts_ramsize, cluster_name, cluster_username,
                            cluster_password, cluster_port):
        return self._setting_cluster("cluster-edit", data_ramsize, index_ramsize, fts_ramsize, cluster_name,
                                     cluster_username, cluster_password, cluster_port)

    def cluster_init(self, data_ramsize, index_ramsize, fts_ramsize, services, index_storage_mode, cluster_name,
                     cluster_username, cluster_password, cluster_port):
        options = ""
        if cluster_username:
            options += " --cluster-username " + str(cluster_username)
        if cluster_password:
            options += " --cluster-password " + str(cluster_password)
        if data_ramsize:
            options += " --cluster-ramsize " + str(data_ramsize)
        if index_ramsize:
            options += " --cluster-index-ramsize " + str(index_ramsize)
        if fts_ramsize:
            options += " --cluster-fts-ramsize " + str(fts_ramsize)
        if cluster_name:
            options += " --cluster-name " + str(cluster_name)
        if index_storage_mode:
            options += " --index-storage-setting " + str(index_storage_mode)
        if cluster_port:
            options += " --cluster-port " + str(cluster_port)
        if services:
            options += " --services " + str(services)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("cluster-init", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Cluster initialized")

    def failover(self, failover_servers, force):
        options = self._get_default_options()
        if failover_servers:
            options += " --server-failover " + str(failover_servers)
        if force:
            options += " --force"

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("failover", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Server failed over")

    def rebalance(self, remove_servers):
        options = self._get_default_options()
        if remove_servers:
            options += " --server-remove " + str(remove_servers)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("rebalance", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Rebalance complete")

    def rebalance_stop(self):
        options = self._get_default_options()
        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("rebalance-stop", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Rebalance stopped")

    def server_add(self, server, server_username, server_password, group_name, services, index_storage_mode):
        options = self._get_default_options()
        if server:
            options += " --server-add " + str(server)
        if server_username:
            options += " --server-add-username " + str(server_username)
        if server_password:
            options += " --server-add-password " + str(server_password)
        if group_name:
            options += " --group-name " + str(group_name)
        if services:
            options += " --services " + str(services)
        if index_storage_mode:
            options += " --index-storage-setting " + str(index_storage_mode)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("server-add", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Server added")

    def setting_audit(self, enabled, log_path, rotate_interval):
        options = self._get_default_options()
        if enabled is not None:
            options += " --audit-enabled " + str(enabled)
        if log_path is not None:
            options += " --audit-log-path " + str(log_path)
        if rotate_interval is not None:
            options += " --audit-log-rotate-interval " + str(rotate_interval)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-audit", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Audit settings modified")

    def setting_autofailover(self, enabled, timeout):
        options = self._get_default_options()
        if enabled is not None:
            options += " --enable-auto-failover " + str(enabled)
        if timeout is not None:
            options += " --auto-failover-timeout " + str(timeout)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-autofailover", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Auto-failover settings modified")

    def setting_cluster(self, data_ramsize, index_ramsize, fts_ramsize, cluster_name, cluster_username,
                         cluster_password, cluster_port):
        return self._setting_cluster("setting-cluster", data_ramsize, index_ramsize, fts_ramsize, cluster_name,
                                     cluster_username, cluster_password, cluster_port)

    def setting_index(self, max_rollbacks, stable_snap_interval, mem_snap_interval, storage_mode, threads,
                      log_level):
        options = self._get_default_options()
        if max_rollbacks:
            options += " --index-max-rollback-points " + str(max_rollbacks)
        if stable_snap_interval:
            options += " --index-stable-snapshot-interval " + str(stable_snap_interval)
        if mem_snap_interval:
            options += " --index-memory-snapshot-interval " + str(mem_snap_interval)
        if storage_mode:
            options += " --index-storage-setting " + str(storage_mode)
        if threads:
            options += " --index-threads " + str(threads)
        if log_level:
            options += " --index-log-level " + str(log_level)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-index", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Indexer settings modified")

    def setting_ldap(self, admins, ro_admins, default, enabled):
        options = self._get_default_options()
        if admins:
            options += " --ldap-admins " + str(admins)
        if ro_admins:
            options += " --ldap-roadmins " + str(ro_admins)
        if default:
            options += " --ldap-default " + str(default)
        if enabled is not None:
            options += " --ldap-enabled " + str(enabled)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-ldap", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "LDAP settings modified")

    def setting_notification(self, enable):
        options = self._get_default_options()
        if enable is not None:
            options += " --enable-notification " + str(enable)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-notification", self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Notification settings updated")

    def user_manage(self, delete, list, set, ro_username, ro_password):
        options = self._get_default_options()
        if delete:
            options += " --delete "
        if list:
            options += " --list "
        if set:
            options += " --set "
        if ro_username is not None:
            options += " --ro-username " + str(ro_username)
        if ro_password:
            options += " --ro-password " + str(ro_password)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("user-manage", self.hostname, options)
        remote_client.disconnect()

        if delete:
            return stdout, stderr, self._was_success(stdout, "Local read-only user deleted")
        elif set:
            return stdout, stderr, self._was_success(stdout, "Local read-only user created")
        else:
            return stdout, stderr, self._no_error_in_output(stdout)

    def _setting_cluster(self, cmd, data_ramsize, index_ramsize, fts_ramsize, cluster_name, cluster_username,
                         cluster_password, cluster_port):
        options = self._get_default_options()
        if cluster_username is not None:
            options += " --cluster-username " + str(cluster_username)
        if cluster_password is not None:
            options += " --cluster-password " + str(cluster_password)
        if data_ramsize:
            options += " --cluster-ramsize " + str(data_ramsize)
        if index_ramsize:
            options += " --cluster-index-ramsize " + str(index_ramsize)
        if fts_ramsize:
            options += " --cluster-fts-ramsize " + str(fts_ramsize)
        if cluster_name:
            options += " --cluster-name " + str(cluster_name)
        if cluster_port:
            options += " --cluster-port " + str(cluster_port)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli(cmd, self.hostname, options)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Cluster settings modified")

    def _get_default_options(self):
        options = ""
        if self.username is not None:
            options += " -u " + str(self.username)
        if self.password is not None:
            options += " -p " + str(self.password)
        return options

    def _no_error_in_output(self, stdout):
        """Inspects each line of the command output and checks to see if the command errored

        This check is used for API's that get data and do not simply report a success message.

        Options:
        stdout - A list of output lines from stdout

        Returns true if not error was found, false otherwise
        """

        for line in stdout:
            if line.startswith("ERROR:"):
                return False
        return True

    def _was_success(self, stdout, message):
        """Inspects each line of the command output and checks to see if the command succeeded

        Options:
        stdout - A list of output lines from stdout
        message - The success message

        Returns a boolean indicating whether or not the success message was found in the output
        """

        for line in stdout:
            if line == "SUCCESS: " + message:
                return True
        return False