from remote.remote_util import RemoteMachineShellConnection


class CouchbaseCLI:
    def __init__(self, server, username=None, password=None, cb_version=None):
        self.server = server
        # Use the internal ip if it's defined when running a command on the remote server
        self.hostname = "%s:%s" % (server.cluster_ip, server.port)
        self.username = username
        self.password = password
        self.cb_version = cb_version

    def bucket_create(self, name, bucket_type, quota,
                      eviction_policy, replica_count, enable_replica_indexes,
                      priority, enable_flush, wait, admin_tools_package=False):
        options = self._get_default_options()
        if name is not None:
            options += " --bucket " + name
        if bucket_type is not None:
            options += " --bucket-type " + bucket_type
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
        stdout, stderr = remote_client.couchbase_cli("bucket-create",
                                                     self.hostname.split(":")[0], options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket created")

    def bucket_compact(self, bucket_name, data_only, views_only, admin_tools_package=False):
        options = self._get_default_options()
        if bucket_name is not None:
            options += " --bucket " + bucket_name
        if data_only:
            options += " --data-only"
        if views_only:
            options += " --view-only"

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-compact",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout,
                                                 "Bucket compaction started")

    def bucket_delete(self, bucket_name, admin_tools_package=False):
        options = self._get_default_options()
        if bucket_name is not None:
            options += " --bucket " + bucket_name

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-delete",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket deleted")

    def bucket_edit(self, name, quota, eviction_policy,
                    replica_count, priority, enable_flush, admin_tools_package=False):
        options = self._get_default_options()
        if name is not None:
            options += " --bucket " + name
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
        stdout, stderr = remote_client.couchbase_cli("bucket-edit",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket edited")

    def bucket_flush(self, name, force, admin_tools_package=False):
        options = self._get_default_options()
        if name is not None:
            options += " --bucket " + name
        if force:
            options += " --force"

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("bucket-flush",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Bucket flushed")

    def cluster_edit(self, data_ramsize, index_ramsize, fts_ramsize,
                     cluster_name, cluster_username,
                     cluster_password, cluster_port, admin_tools_package=False):
        return self._setting_cluster("cluster-edit", data_ramsize,
                                     index_ramsize, fts_ramsize, cluster_name,
                                     cluster_username, cluster_password,
                                     cluster_port, admin_tools_package=admin_tools_package)

    def cluster_init(self, data_ramsize, index_ramsize, fts_ramsize, services,
                     index_storage_mode, cluster_name,
                     cluster_username, cluster_password, cluster_port, admin_tools_package=False):
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
        stdout, stderr = remote_client.couchbase_cli("cluster-init",
                                                     self.hostname.split(":")[0], options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        print_msg = "Cluster initialized"
        if self.cb_version is not None and \
                        self.cb_version[:3] == "4.6":
            print_msg = "init/edit %s" % self.server.ip
        return stdout, stderr, self._was_success(stdout, print_msg)

    def collect_logs_start(self, all_nodes, nodes, upload, upload_host,
                           upload_customer, upload_ticket, admin_tools_package=False):
        options = self._get_default_options()
        if all_nodes is True:
            options += " --all-nodes "
        if nodes is not None:
            options += " --nodes " + str(nodes)
        if upload is True:
            options += " --upload "
        if upload_host is not None:
            options += " --upload-host " + str(upload_host)
        if upload_customer is not None:
            options += " --customer " + str(upload_customer)
        if upload_ticket is not None:
            options += " --ticket " + str(upload_ticket)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("collect-logs-start",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout,
                                                 "Log collection started")

    def collect_logs_stop(self, admin_tools_package=False):
        options = self._get_default_options()
        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("collect-logs-stop",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout,
                                                 "Log collection stopped")

    def create_scope(self, bucket="default", scope="scope0", admin_tools_package=False):
        remote_client = RemoteMachineShellConnection(self.server)
        options = f" --bucket {str(bucket)} --create-scope {str(scope)}"
        stdout, stderr = remote_client.execute_couchbase_cli("collection-manage", self.hostname,
                                                             options,
                                                             admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout)

    def create_collection(self, bucket="default", scope="scope0", collection="mycollection0", max_ttl=None, admin_tools_package=False):
        remote_client = RemoteMachineShellConnection(self.server)
        options = f" --bucket {str(bucket)} --create-collection {str(scope)}.{str(collection)}"
        if max_ttl is not None:
            options += f" --max-ttl {str(max_ttl)}"
        stdout, stderr = remote_client.execute_couchbase_cli("collection-manage", self.hostname,
                                                             options,
                                                             admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout)

    def delete_collection(self, bucket="default", scope='_default', collection='_default', admin_tools_package=False):
        remote_client = RemoteMachineShellConnection(self.server)
        options = f" --bucket {str(bucket)} --drop-collection {str(scope)}.{str(collection)}"
        stdout, stderr = remote_client.execute_couchbase_cli("collection-manage", self.hostname,
                                                             options,
                                                             admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout)

    # Custom scope should be passed as default scope can not be deleted
    def delete_scope(self, scope, bucket="default", admin_tools_package=False):
        remote_client = RemoteMachineShellConnection(self.server)
        options = f" --bucket {str(bucket)} --drop-scope {str(scope)}"
        stdout, stderr = remote_client.execute_couchbase_cli("collection-manage", self.hostname,
                                                             options,
                                                             admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout)

    def get_bucket_scopes(self, bucket, admin_tools_package=False):
        remote_client = RemoteMachineShellConnection(self.server)
        options = f" --bucket {str(bucket)} --list-scopes"
        stdout, stderr = remote_client.execute_couchbase_cli("collection-manage", self.hostname,
                                                             options,
                                                             admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout)

    def get_bucket_collections(self, bucket, admin_tools_package=False):
        remote_client = RemoteMachineShellConnection(self.server)
        options = " --bucket " + str(bucket)
        options += " --list-collections"
        stdout, stderr = remote_client.execute_couchbase_cli("collection-manage", self.hostname,
                                                             options,
                                                             admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout)

    def get_scope_collections(self, bucket, scope, admin_tools_package=False):
        remote_client = RemoteMachineShellConnection(self.server)
        options = " --bucket " + str(bucket)
        options += " --list-collections " + str(scope)
        stdout, stderr = remote_client.execute_couchbase_cli("collection-manage", self.hostname,
                                                             options,
                                                             admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout)

    #Temporarily need to enable DP mode for collections
    def enable_dp(self, admin_tools_package=False):
        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.execute_couchbase_cli("enable-developer-preview", self.hostname,
                                                             "--enable", _stdin="y",
                                                             admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Developer mode enabled")

    def failover(self, failover_servers, force, admin_tools_package=False):
        options = self._get_default_options()
        if failover_servers:
            options += " --server-failover " + str(failover_servers)
        if force:
            options += " --hard "

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("failover", self.hostname,
                                                     options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Server failed over")

    def group_manage(self, create, delete, list, move_servers, rename, name,
                     to_group, from_group, admin_tools_package=False):
        options = self._get_default_options()
        if create:
            options += " --create "
        if delete:
            options += " --delete "
        if list:
            options += " --list "
        if rename is not None:
            options += " --rename " + str(rename)
        if move_servers is not None:
            options += " --move-servers " + str(move_servers)
        if name:
            options += " --group-name " + str(name)
        if to_group:
            options += " --to-group " + str(to_group)
        if from_group:
            options += " --from-group " + str(from_group)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("group-manage",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()

        success = False
        if create:
            success = self._was_success(stdout, "Server group created")
        elif delete:
            success = self._was_success(stdout, "Server group deleted")
        elif list:
            success = self._no_error_in_output(stdout)
        elif move_servers:
            success = self._was_success(stdout, "Servers moved between groups")
        elif rename:
            success = self._was_success(stdout, "Server group renamed")

        return stdout, stderr, success

    def node_init(self, data_path, index_path, hostname, admin_tools_package=False):
        options = self._get_default_options()
        if data_path:
            options += " --node-init-data-path " + str(data_path)
        if index_path:
            options += " --node-init-index-path " + str(index_path)
        if hostname:
            options += " --node-init-hostname " + str(hostname)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("node-init",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Node initialized")

    def rebalance(self, remove_servers, admin_tools_package=False):
        options = self._get_default_options()
        if remove_servers:
            options += " --server-remove " + str(remove_servers)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("rebalance",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Rebalance complete")

    def rebalance_stop(self, admin_tools_package=False):
        options = self._get_default_options()
        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("rebalance-stop",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Rebalance stopped")

    def recovery(self, servers, recovery_type, admin_tools_package=False):
        options = self._get_default_options()
        if servers:
            options += " --server-recovery " + str(servers)
        if recovery_type:
            options += " --recovery-type " + str(recovery_type)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("recovery", self.hostname,
                                                     options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Servers recovered")

    def server_add(self, server, server_username, server_password, group_name,
                   services, index_storage_mode, admin_tools_package=False):
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
        stdout, stderr = remote_client.couchbase_cli("server-add",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Server added")

    def server_readd(self, servers, admin_tools_package=False):
        options = self._get_default_options()
        if servers:
            options += " --server-add " + str(servers)

        hostname = "https://{0}:1{1} --no-ssl-verify ".format(self.server.cluster_ip, self.server.port)
        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("server-readd",
                                                     hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Servers recovered")

    def setting_audit(self, enabled, log_path, rotate_interval, admin_tools_package=False):
        options = self._get_default_options()
        if enabled is not None:
            options += " --set --audit-enabled " + str(enabled)
        if log_path is not None:
            options += " --audit-log-path " + str(log_path)
        if rotate_interval is not None:
            options += " --audit-log-rotate-interval " + str(rotate_interval)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-audit",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout,
                                                 "Audit settings modified")

    def setting_alert(self, enabled, email_recipients, email_sender,
                      email_username, email_password, email_host,
                      email_port, encrypted, alert_af_node,
                      alert_af_max_reached, alert_af_node_down, alert_af_small,
                      alert_af_disable, alert_ip_changed, alert_disk_space,
                      alert_meta_overhead, alert_meta_oom,
                      alert_write_failed, alert_audit_dropped, admin_tools_package=False):
        options = self._get_default_options()

        if enabled is not None:
            options += " --enable-email-alert " + str(enabled)
        if email_recipients is not None:
            options += " --email-recipients " + str(email_recipients)
        if email_sender is not None:
            options += " --email-sender " + str(email_sender)
        if email_username is not None:
            options += " --email-user " + str(email_username)
        if email_password is not None:
            options += " --email-password " + str(email_password)
        if email_host is not None:
            options += " --email-host " + str(email_host)
        if email_port is not None:
            options += " --email-port " + str(email_port)
        if encrypted is not None:
            options += "--enable-email-encrypt" + str(encrypted)
        if alert_af_node:
            options += " --alert-auto-failover-node "
        if alert_af_max_reached:
            options += " --alert-auto-failover-max-reached "
        if alert_af_node_down:
            options += " --alert-auto-failover-node-down "
        if alert_af_small:
            options += " --alert-auto-failover-cluster-small "
        if alert_af_disable:
            options += " --alert-auto-failover-disable "
        if alert_ip_changed:
            options += " --alert-ip-changed "
        if alert_disk_space:
            options += " --alert-disk-space "
        if alert_meta_overhead:
            options += " --alert-meta-overhead "
        if alert_meta_oom:
            options += " --alert-meta-oom "
        if alert_write_failed:
            options += " --alert-write-failed "
        if alert_audit_dropped:
            options += " --alert-audit-msg-dropped "

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-alert",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Email alert "
                                                         "settings modified")

    def setting_autofailover(self, enabled, timeout, admin_tools_package=False):
        options = self._get_default_options()
        if enabled is not None:
            options += " --enable-auto-failover " + str(enabled)
        if timeout is not None:
            options += " --auto-failover-timeout " + str(timeout)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-autofailover",
                                                     self.server.ip, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Auto-failover "
                                                         "settings modified")

    def setting_autoreprovision(self, enabled, max_nodes, admin_tools_package=False):
        options = self._get_default_options()
        if enabled is not None:
            options += " --enabled " + str(enabled)
        if max_nodes is not None:
            options += " --max-nodes " + str(max_nodes)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-autoreprovision",
                                                     self.server.ip, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Auto-reprovision "
                                                         "settings modified")

    def setting_cluster(self, data_ramsize, index_ramsize, fts_ramsize,
                        cluster_name, cluster_username,
                        cluster_password, cluster_port, admin_tools_package=False):
        return self._setting_cluster("setting-cluster", data_ramsize,
                                     index_ramsize, fts_ramsize, cluster_name,
                                     cluster_username, cluster_password,
                                     cluster_port, admin_tools_package=admin_tools_package)

    def setting_compaction(self, db_frag_perc, db_frag_size, view_frag_perc,
                           view_frag_size, from_period, to_period,
                           abort_outside, parallel_compact, purgeint, admin_tools_package=False):
        options = self._get_default_options()
        if db_frag_perc is not None:
            options += " --compaction-db-percentage " + str(db_frag_perc)
        if db_frag_size is not None:
            options += " --compaction-db-size " + str(db_frag_size)
        if view_frag_perc is not None:
            options += " --compaction-view-percentage " + str(view_frag_perc)
        if view_frag_size is not None:
            options += " --compaction-view-size " + str(view_frag_size)
        if from_period is not None:
            options += " --compaction-period-from " + str(from_period)
        if to_period is not None:
            options += " --compaction-period-to " + str(to_period)
        if abort_outside is not None:
            options += " --enable-compaction-abort " + str(abort_outside)
        if parallel_compact is not None:
            options += " --enable-compaction-parallel " + str(parallel_compact)
        if purgeint is not None:
            options += " --metadata-purge-interval " + str(purgeint)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-compaction",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Compaction "
                                                         "settings modified")

    def setting_gsi_compaction(self, compact_mode, compact_percent, compact_interval,
                                               from_period, to_period, enable_abort, admin_tools_package=False):
        options = self._get_default_options()
        if compact_mode is not None:
            options += " --gsi-compaction-mode %s" % compact_mode
            if compact_mode == "append":
                if compact_percent is not None:
                    options += " --compaction-gsi-percentage=" + str(compact_percent)
            elif compact_mode == "circular":
                if compact_interval is not None:
                    options += " --compaction-gsi-interval " + str(compact_interval)
                if from_period is not None:
                    options += " --compaction-gsi-period-from=" + str(from_period)
                if to_period is not None:
                    options += " --compaction-gsi-period-to=" + str(to_period)
                if enable_abort:
                    options += " --enable-gsi-compaction-abort=" + str(enable_abort)
            else:
                raise Exception("need compact mode to run!")

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-compaction",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Compaction settings modified")

    def setting_index(self, max_rollbacks, stable_snap_interval,
                      mem_snap_interval, storage_mode, threads,
                      log_level, admin_tools_package=False):
        options = self._get_default_options()
        if max_rollbacks:
            options += " --index-max-rollback-points " + str(max_rollbacks)
        if stable_snap_interval:
            options += " --index-stable-snapshot-interval " + str(
                stable_snap_interval)
        if mem_snap_interval:
            options += " --index-memory-snapshot-interval " + str(
                mem_snap_interval)
        if storage_mode:
            options += " --index-storage-setting " + str(storage_mode)
        if threads:
            options += " --index-threads " + str(threads)
        if log_level:
            options += " --index-log-level " + str(log_level)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-index",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout,
                                                 "Indexer settings modified")

    def setting_ldap(self, admins, ro_admins, default, enabled, admin_tools_package=False):
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
        stdout, stderr = remote_client.couchbase_cli("setting-ldap",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout,
                                                 "LDAP settings modified")

    def setting_notification(self, enable, admin_tools_package=False):
        options = self._get_default_options()
        if enable is not None:
            options += " --enable-notifications " + str(enable)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("setting-notification",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Notification "
                                                         "settings updated")

    def user_manage(self, delete, list, set, rbac_username, rbac_password, roles,
                    auth_domain, admin_tools_package=False):
        options = self._get_default_options()
        if delete:
            options += " --delete "
        if list:
            options += " --list "
        if set:
            options += " --set "
        if rbac_username is not None:
            options += " --rbac-username " + str(rbac_username)
        if rbac_password:
            options += " --rbac-password " + str(rbac_password)
        if roles:
            options += " --roles " + str(roles)
        if auth_domain:
            options += " --auth-domain " + str(auth_domain)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("user-manage",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()

        if delete:
            return stdout, stderr, self._was_success(stdout, "Local read-only"
                                                             "user deleted")
        elif set:
            return stdout, stderr, self._was_success(stdout, "RBAC user set")
        else:
            return stdout, stderr, self._no_error_in_output(stdout)

    def set_ip_family(self, ip_family, admin_tools_package=False):
        options = self._get_default_options()
        options += " --set "
        options += " --{0} ".format(ip_family)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("ip-family",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "SUCCESS: "
                                                         "Switched IP family of the cluster")

    def get_ip_family(self, admin_tools_package=False):
        options = self._get_default_options()
        options += " --get "

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("ip-family",
                                                     self.hostname, options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout, "Cluster using ")

    def _setting_cluster(self, cmd, data_ramsize, index_ramsize, fts_ramsize,
                         cluster_name, cluster_username,
                         cluster_password, cluster_port, admin_tools_package=False):
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
            if cluster_name == "empty":
                cluster_name = " "
            options += " --cluster-name " + str(cluster_name)
        if cluster_port:
            options += " --cluster-port " + str(cluster_port)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli(cmd, self.hostname,
                                                     options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout,
                                                 "Cluster settings modified")

    def analytics_link_setup(self, list_links=False, create=False, edit=False, delete=False,
                             name=None, link_type=None, scope=None,
                             # S3 link parameters
                             access_key_id=None, secret_access_key=None, region=None,
                             service_endpoint=None, session_token=None,
                             # Couchbase link parameters
                             remote_hostname=None, link_username=None, link_password=None,
                             encryption=None, user_certificate=None, user_key=None,
                             user_key_passphrase=None, certificate=None,
                             # GCS link parameters
                             application_default_credentials=False, json_credentials=None,
                             # Azure link parameters
                             account_name=None, account_key=None, shared_access_signature=None,
                             managed_identity_id=None, client_id=None, client_secret=None,
                             client_certificate=None, client_certificate_password=None,
                             tenant_id=None, endpoint=None,
                             admin_tools_package=False):
        options = self._get_default_options()
        if list_links:
            options += " --list"
        if create:
            options += " --create"
        if edit:
            options += " --edit"
        if delete:
            options += " --delete"
        if name is not None:
            options += " --name " + str(name)
        if link_type is not None:
            options += " --type " + str(link_type)
        if scope is not None:
            options += " --scope " + str(scope)

        # S3 link parameters
        if access_key_id is not None:
            options += " --access-key-id " + str(access_key_id)
        if secret_access_key is not None:
            options += " --secret-access-key " + str(secret_access_key)
        if region is not None:
            options += " --region " + str(region)
        if service_endpoint is not None:
            options += " --service-endpoint " + str(service_endpoint)
        if session_token is not None:
            options += " --session-token " + str(session_token)

        # Couchbase link parameters
        if remote_hostname is not None:
            options += " --hostname " + str(remote_hostname)
        if link_username is not None:
            options += " --link-username " + str(link_username)
        if link_password is not None:
            options += " --link-password " + str(link_password)
        if encryption is not None:
            options += " --encryption " + str(encryption)
        if user_certificate is not None:
            options += " --user-certificate " + str(user_certificate)
        if user_key is not None:
            options += " --user-key " + str(user_key)
        if user_key_passphrase is not None:
            options += " --user-key-passphrase " + str(user_key_passphrase)
        if certificate is not None:
            options += " --certificate " + str(certificate)

        # GCS link parameters
        if application_default_credentials:
            options += " --application-default-credentials"
        if json_credentials is not None:
            options += " --json-credentials " + str(json_credentials)

        # Azure link parameters
        if account_name is not None:
            options += " --account-name " + str(account_name)
        if account_key is not None:
            options += " --account-key " + str(account_key)
        if shared_access_signature is not None:
            options += " --shared-access-signature " + str(shared_access_signature)
        if managed_identity_id is not None:
            options += " --managed-identity-id " + str(managed_identity_id)
        if client_id is not None:
            options += " --client-id " + str(client_id)
        if client_secret is not None:
            options += " --client-secret " + str(client_secret)
        if client_certificate is not None:
            options += " --client-certificate " + str(client_certificate)
        if client_certificate_password is not None:
            options += " --client-certificate-password " + str(client_certificate_password)
        if tenant_id is not None:
            options += " --tenant-id " + str(tenant_id)
        if endpoint is not None:
            options += " --endpoint " + str(endpoint)

        remote_client = RemoteMachineShellConnection(self.server)
        stdout, stderr = remote_client.couchbase_cli("analytics-link-setup",
                                                     self.hostname.split(":")[0], options,
                                                     admin_tools_package=admin_tools_package)
        remote_client.disconnect()
        return stdout, stderr, self._was_success(stdout)

    def _get_default_options(self):
        options = ""
        if self.username is not None:
            options += " -u " + str(self.username)
        if self.password is not None:
            options += " -p " + str(self.password)
        return options

    def _no_error_in_output(self, stdout):
        """Inspects each line of the command output and checks to see if
        the command errored. This check is used for API's that get data and
        do not simply report a success message.

        Options:
        stdout - A list of output lines from stdout
        Returns true if not error was found, false otherwise
        """

        for line in stdout:
            if line.startswith("ERROR:"):
                return False
        return True

    def _was_success(self, stdout, message=None):
        """Inspects each line of the command output and checks to see if
        the command succeeded
        Options:
        stdout - A list of output lines from stdout
        message - The success message
        Returns a boolean indicating whether or not the success message was
        found in the output
        """
        for line in stdout:
            if message:
                if line == "SUCCESS: " + message:
                    return True
            else:
                if line.startswith("SUCCESS:"):
                    return True
        if stdout:
            return True
        return False
