from pytests.xdcr.xdcrnewbasetests import XDCRNewBaseTest
from lib.membase.api.on_prem_rest_client import RestConnection

class TargetAwarenessXDCR(XDCRNewBaseTest):
    def setUp(self):
        super().setUp()
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.src_master = RestConnection(self.src_cluster.get_master_node())
        self.dest_master = RestConnection(self.dest_cluster.get_master_node())

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

    def test_target_awareness(self):
        self.setup_xdcr_and_load()
        self.wait_interval(30, "Waiting for heartbeats to be sent and updated in target cluster")

        src_outgoing_repls = self.get_outgoing_replications(self.src_master)
        dest_incoming_repls = self.get_incoming_replications(self.dest_master)
        retries = 5
        count = 0
        while dest_incoming_repls is None and count < retries:
            dest_incoming_repls = self.get_incoming_replications(self.dest_master)
            count += 1
            self.wait_interval(2, f"Trying to get incoming replications from dest cluster for {count}/{retries} times")
        if dest_incoming_repls is None:
            self.fail("No incoming replications reported by dest cluster")
        self.verify_source_to_dest_replication(src_outgoing_repls, dest_incoming_repls)
        if self._rdirection == "bidirection":
            src_incoming_repls = self.get_incoming_replications(self.src_master)
            dest_outgoing_repls = self.get_outgoing_replications(self.dest_master)
            retries = 5
            count = 0
            while src_incoming_repls is None and count < retries:
                src_incoming_repls = self.get_incoming_replications(self.src_master)
                count += 1
                self.wait_interval(2,
                                   f"Trying to get incoming replications from src cluster for {count}/{retries} times")
            if src_incoming_repls is None:
                self.fail("No incoming replications reported by src cluster")
            self.verify_dest_to_source_replication(dest_outgoing_repls, src_incoming_repls)
