from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest, NodeHelper
from lib.membase.api.on_prem_rest_client import RestConnection
from lib.remote.remote_util import RemoteMachineShellConnection

class TargetAwarenessXDCR(XDCRNewBaseTest):
    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_master = self.dest_cluster.get_master_node()
        self.src_master_rest = RestConnection(self.src_master)
        self.dest_master_rest = RestConnection(self.dest_master)

    def verify_source_to_dest_replication(self, src_outgoing_repls, dest_incoming_repls):
        src_target_cluster_uuid_list = set()  # list of target cluster UUIDs for src cluster
        for outgoing_repl in src_outgoing_repls:
            src_target_cluster_uuid_list.add(outgoing_repl["uuid"])
        for repl in dest_incoming_repls:
            uuid = repl["SourceClusterReplSpecs"][0]["targetClusterUUID"]
            if uuid not in src_target_cluster_uuid_list:
                self.fail(f"Target cluster UUID {uuid} not found in source cluster's outgoing replications")

    def verify_dest_to_source_replication(self, dest_outgoing_repls, src_incoming_repls):
        dest_target_cluster_uuid_list = set()  # list of target cluster UUIDs for dest cluster
        for outgoing_repl in dest_outgoing_repls:
            dest_target_cluster_uuid_list.add(outgoing_repl["uuid"])
        for repl in src_incoming_repls:
            uuid = repl["SourceClusterReplSpecs"][0]["targetClusterUUID"]
            if uuid not in dest_target_cluster_uuid_list:
                self.fail(f"Target cluster UUID {uuid} not found in dest cluster's outgoing replications")
                
    def stop_couchbase(self, server):
        remote_shell_conn = RemoteMachineShellConnection(server)
        try:
            o, err = remote_shell_conn.execute_command("systemctl stop couchbase-server.service", use_channel=True, timeout=10)
            if err:
                self.fail(f"Error setting firewall rules: {err}")
        except Exception as e:
            self.fail(f"Exception while setting firewall rules: {e}")

    def restart_couchbase(self, server):
        remote_shell_conn = RemoteMachineShellConnection(server)
        try:
            o, err = remote_shell_conn.execute_command("systemctl start couchbase-server.service && systemctl restart couchbase-server.service", use_channel=True, timeout=10)
            if err:
                self.fail(f"Error cleaning firewall rules: {err}")      
        except Exception as e:
            self.fail(f"Exception while cleaning firewall rules: {e}")

    def set_internal_xdcr_settings(self, server, param, value):
        server_shell = RemoteMachineShellConnection(server)
        cmd = f"curl -X POST -u Administrator:password http://localhost:9998/xdcr/internalSettings -d '{param}={value}'"
        out, err = server_shell.execute_command(cmd, timeout=5, use_channel=True)
        server_shell.disconnect()
        if err:
            self.log.info(f"Error setting internal XDCR settings: {err}")

    def block_network_nftables(self, src_cluster, dest_cluster):
        src_nodes_str = ", ".join([node.ip for node in src_cluster.get_nodes()])
        for dest_node in dest_cluster.get_nodes():
            remote_shell_conn = RemoteMachineShellConnection(dest_node)
            try:
                cmd = "nft add table inet filter && "
                cmd += "nft add chain inet filter forward { type filter hook forward priority 0 \; } && " 
                cmd += "nft add chain inet filter input { type filter hook input priority 0 \; policy accept \; } && "
                cmd += "nft add chain inet filter output { type filter hook output priority 0 \; policy accept \; } && "
                cmd += f"nft add rule inet filter input ip saddr {{ {src_nodes_str} }} drop"
                o, err = remote_shell_conn.execute_command(cmd, use_channel=True, timeout=5)
                if err:
                    self.fail(f"Error blocking network with nftables: {err}")

            except Exception as e:
                self.fail(f"Exception while blocking network with nftables: {e}")
            finally:
                remote_shell_conn.disconnect()

    def unblock_network_nftables(self, src_cluster, dest_cluster):
        for dest_node in dest_cluster.get_nodes():
            remote_shell_conn = RemoteMachineShellConnection(dest_node)
            try:
                cmd = "nft flush ruleset"
                o, err = remote_shell_conn.execute_command(cmd, use_channel=True, timeout=5)
                if err:
                    self.fail(f"Error unblocking network with nftables: {err}")

            except Exception as e:
                self.fail(f"Exception while unblocking network with nftables: {e}")
            finally:
                remote_shell_conn.disconnect()

    def test_target_awareness(self):
        self.setup_xdcr_and_load()
        self.wait_interval(30, "Waiting for heartbeats to be sent and updated in target cluster")

        src_outgoing_repls = self.get_outgoing_replications(self.src_master_rest)
        dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
        retries = 5
        count = 0
        while dest_incoming_repls is None and count < retries:
            dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
            count += 1
            self.wait_interval(2, f"Trying to get incoming replications from dest cluster for {count}/{retries} times")
        if dest_incoming_repls is None:
            self.fail("No incoming replications reported by dest cluster")
        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls)
        if self._rdirection == "bidirection":
            src_incoming_repls = self.get_incoming_replications(self.src_master_rest)
            dest_outgoing_repls = self.get_outgoing_replications(self.dest_master_rest)
            retries = 5
            count = 0
            while src_incoming_repls is None and count < retries:
                src_incoming_repls = self.get_incoming_replications(self.src_master_rest)
                count += 1
                self.wait_interval(2,
                                   f"Trying to get incoming replications from src cluster for {count}/{retries} times")
            if src_incoming_repls is None:
                self.fail("No incoming replications reported by src cluster")
            self.verify_dest_to_source_replication(dest_outgoing_repls, src_incoming_repls)

    def test_heartbeat(self):
        # Check heartbeat appearing in logs

        self.setup_xdcr_and_load()
        self.wait_interval(40, "Waiting for heartbeats to be sent and updated in target cluster")
        src_outgoing_repls = self.get_outgoing_replications(self.src_master_rest)
        dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
        
        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls)

        src_target_cluster_uuid_list = set() 
        for outgoing_repl in src_outgoing_repls:
            src_target_cluster_uuid_list.add(outgoing_repl["uuid"])
        
        for replications in dest_incoming_repls:
            src_cluster_uuid_on_dest = replications["SourceClusterUUID"]
            for specs in replications["SourceClusterReplSpecs"]:
                uuid = specs["id"].split('/')[0]
                if uuid in src_target_cluster_uuid_list:
                    search_str = f"GOXDCR.P2PManager: Heartbeats heard from - SrcUUID: {src_cluster_uuid_on_dest}"
                    matches, count = NodeHelper.check_goxdcr_log(self.dest_master, search_str, timeout=30)
                    if count == 0:
                        self.fail(f"No heartbeat heard in dest cluster for source cluster UUID {src_cluster_uuid_on_dest}")
                    self.log.info(f"Found logs for heartbeat in dest cluster with source cluster UUID {src_cluster_uuid_on_dest}")
    
    
    def test_replication_delete(self):
        self.setup_xdcr_and_load()
        self.wait_interval(40, "Waiting for heartbeats to be sent and updated in target cluster")
        src_outgoing_repls = self.get_outgoing_replications(self.src_master_rest)
        dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)

        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls)

        self.set_internal_xdcr_settings(self.src_master, "SrcHeartbeatMinInterval", 10)
        self.set_internal_xdcr_settings(self.dest_master, "SrcHeartbeatMinInterval", 10)
        self.src_master_rest.remove_all_replications()
        self.src_master_rest.remove_all_remote_clusters()
        self.wait_interval(30, "Waiting for replications to be deleted from source cluster")
        retries = 5 
        count = 1
        dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
        while dest_incoming_repls is not None and count<=retries:
            dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
            self.wait_interval(10, f"Trying to get incoming replications from dest cluster for {count}/{retries} times")
            count += 1

        if dest_incoming_repls is not None:
            self.fail("Replications not deleted from dest cluster")

    def test_node_crash(self):
        self.setup_xdcr_and_load()
        self.wait_interval(40, "Waiting for heartbeats to be sent and updated in target cluster")
        src_outgoing_repls = self.get_outgoing_replications(self.src_master_rest)
        dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
        
        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls)

        self.set_internal_xdcr_settings(self.src_master, "SrcHeartbeatMinInterval", 10)
        self.set_internal_xdcr_settings(self.dest_master, "SrcHeartbeatMinInterval", 10)
        self.stop_couchbase(self.src_master)
        self.wait_interval(20, "Waiting for source cluster to be down")
        dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
        retries = 5
        count = 1
        while dest_incoming_repls is not None and count<=retries:
            dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
            self.wait_interval(20, f"Trying to get incoming replications from dest cluster for {count}/{retries} times")
            count += 1
        if dest_incoming_repls is not None:
            self.fail("Replications not deleted from dest cluster")
        self.restart_couchbase(self.src_master)
        self.wait_interval(20, "Waiting for source cluster to be up")
        dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
        retries = 5
        count = 1
        while dest_incoming_repls is None and count <= retries:
            dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)
            self.wait_interval(20, f"Trying to get incoming replications from dest cluster for {count}/{retries} times")
            count += 1
        if dest_incoming_repls is None:
            self.fail("Replications not added back in dest cluster")
        self.sleep(10, "Delay for server to be ready to recieve requests")
    
    def test_network_failure(self):
        self.setup_xdcr_and_load()
        self.wait_interval(40, "Waiting for heartbeats to be sent and updated in target cluster")
        src_outgoing_repls = self.get_outgoing_replications(self.src_master_rest)
        dest_incoming_repls = self.get_incoming_replications(self.dest_master_rest)

        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls)

        self.set_internal_xdcr_settings(self.src_master, "SrcHeartbeatMinInterval", 10)
        self.set_internal_xdcr_settings(self.dest_master, "SrcHeartbeatMinInterval", 10)

        # Block network between source and destination clusters
        self.block_network_nftables(self.src_cluster, self.dest_cluster)
        self.wait_interval(100, "Waiting for network block to take effect")

        # Verify replication stops or errors occur
        dest_incoming_repls_after_block = self.get_incoming_replications(self.dest_master_rest)
        if dest_incoming_repls_after_block is not None:
           self.fail(f"Replications exists despite network failure.")

        # Unblock network
        self.unblock_network_nftables(self.src_cluster, self.dest_cluster)
        self.wait_interval(90, "Waiting for network to be restored")

        # Verify replication resumes
        dest_incoming_repls_after_unblock = self.get_incoming_replications(self.dest_master_rest)
        retries = 5
        count = 0
        while dest_incoming_repls_after_unblock is None and count < retries:
            dest_incoming_repls_after_unblock = self.get_incoming_replications(self.dest_master_rest)
            count += 1
            self.wait_interval(10, f"Trying to get incoming replications from dest cluster for {count}/{retries} times")

        if dest_incoming_repls_after_unblock is None:
            self.fail("No incoming replications reported by dest cluster after unblocking network")

        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls_after_unblock)

        self.log.info("Network failure test completed successfully.")