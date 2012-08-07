from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from xdcrbasetests import XDCRReplicationBaseTest
from remote.remote_util import RemoteMachineShellConnection
from random import randrange
import time

"""Testing Unidirectional xdcr replication while performing different rebalance operations on the source/destination clusters."""
class unidirectional(XDCRReplicationBaseTest):
    def setUp(self):
        super(unidirectional, self).setUp()

    def tearDown(self):
        super(unidirectional, self).tearDown()

    def _async_modify_data(self):
        tasks = []
        """Setting up creates/updates/deletes at source nodes"""
        if self._doc_ops is not None:
            # allows multiple of them but one by one
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "create" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_create, "create", 0))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
        for task in tasks:
            task.result()

    """Loading at only source cluster. Rebalance-In at either source/destination or both.
    Verifying whether XDCR replication is successful on subsequent destination clusters."""

    def rebalance_in(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        for num_rebalance in range(self._num_rebalance):
            for node in self._floating_servers_set:
                self._log.info("Adding node {0}".format(node.ip))
                if "source" in self._rebalance:
                    self._log.info(" Rebalance In node {0} at Source{1}".format(node.ip, self.src_master.ip))
                    self._cluster_helper.rebalance(self.src_nodes, [node], [])

                if "destination" in self._rebalance:
                    self._log.info(" Rebalance In node {0} at Destination".format(node.ip, self.dest_master.ip))
                    self._cluster_helper.rebalance(self.dest_nodes, [node], [])

                time.sleep(self._timeout/6)

                self._async_modify_data()

                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

                self.verify_results()

    """Loading only at source cluster.Rebalance-Out Non-master node at either source/destination.
Verifying whether XDCR replication is successful on subsequent destination clusters."""

    def rebalance_out(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        for num_rebalance in range(self._num_rebalance):
            if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                i = randrange(1, len(self.src_nodes))
                self._log.info(" Rebalance Out node {0} at Source{1}".format(self.src_nodes[i].ip, self.src_master.ip))
                self._cluster_helper.rebalance(self.src_nodes, [], [self.src_nodes[i]])
                self.src_nodes.pop(i)

            if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                i = randrange(1, len(self.dest_nodes))
                self._log.info(
                    " Rebalance Out node {0} at Destination{1}".format(self.dest_nodes[i].ip, self.dest_master.ip))
                self._cluster_helper.rebalance(self.dest_nodes, [], [self.dest_nodes[i]])
                self.dest_nodes.pop(i)

            time.sleep(self._timeout/6)

            self._async_modify_data()

            self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

            self.verify_results()

    """Loading only at source cluster. Async Rebalance-In node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_in(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        for num_rebalance in range(self._num_rebalance):
            for node in self._floating_servers_set:
                tasks = []
                if "source" in self._rebalance:
                    tasks.extend(self._async_rebalance(self.src_nodes, [node], []))
                    self._log.info(
                        " Starting rebalance-in node {0} at Source cluster {1}".format(node.ip, self.src_master.ip))

                    self.src_nodes.extend([node])

                if "destination" in self._rebalance:
                    tasks.extend(self._async_rebalance(self.dest_nodes, [node], []))
                    self._log.info(" Starting rebalance-in node{0} at Destination cluster {1}".format(node.ip,
                        self.dest_master.ip))
                    self.dest_nodes.extend([node])

                self._async_modify_data()

                time.sleep(self._timeout/6)
                for task in tasks:
                    task.result()

                self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

                self.verify_results()

    """Loading only at source cluster. Async Rebalance-Out Non-master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        for num_rebalance in range(self._num_rebalance):
            tasks = []
            if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                i = randrange(1, len(self.src_nodes))
                tasks.extend(self._async_rebalance(self.src_nodes, [], [self.src_nodes[i]]))
                self._log.info(" Starting rebalance-out node {0} at Source cluster {1}".format(self.src_nodes[i].ip,
                    self.src_master.ip))
                self.src_nodes.pop(i)
            if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                i = randrange(1, len(self.dest_nodes))
                tasks.extend(self._async_rebalance(self.dest_nodes, [], [self.dest_nodes[i]]))
                self._log.info(" Starting rebalance-out node{0} at Destination cluster {1}".format(self.src_nodes[i].ip,
                    self.dest_master.ip))
                self.dest_nodes.pop(i)

            self._async_modify_data()

            time.sleep(self._timeout/6)
            for task in tasks:
                task.result()

            self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

            self.verify_results()

    """Loading only at source cluster. Async Rebalance-Out Master node at Source/Destination while
Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out_master(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        tasks = []
        if "source" in self._rebalance:
            tasks.extend(self._async_rebalance(self.src_nodes, [], [self.src_master]))
            self._log.info(" Starting rebalance-out Master node {0} at Source cluster {1}".format(self.src_master.ip,
                self.src_master.ip))
            self.src_nodes.remove(self.src_master)
            self.src_master = self.src_nodes[0]
        if "destination" in self._rebalance:
            tasks.extend(self._async_rebalance(self.dest_nodes, [], [self.dest_master]))
            self._log.info(
                " Starting rebalance-out Master node {0} at Destination cluster {1}".format(self.dest_master.ip,
                    self.dest_master.ip))
            self.dest_nodes.remove(self.dest_master)
            self.dest_master = self.dest_nodes[0]

        self._async_modify_data()

        time.sleep(self._timeout/6)
        for task in tasks:
            task.result()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=False)

        self.verify_results()
        # TODO :  0. Swap Rebalance,

"""Testing bidirectional xdcr replication while performing differernt rebalance operations on the clusters."""

class bidirectional(XDCRReplicationBaseTest):
    def setUp(self):
        super(bidirectional, self).setUp()
        self.gen_create2 = BlobGenerator('LoadTwo', 'LoadTwo', self._value_size, end=self._num_items)
        self.gen_delete2 = BlobGenerator('LoadTwo', 'LoadTwo-', self._value_size,
            start=int((self._num_items) * (float)(100 - self._percent_delete) / 100), end=self._num_items)
        self.gen_update2 = BlobGenerator('LoadTwo', 'LoadTwo-', self._value_size, start=0,
            end=int(self._num_items * (float)(self._percent_update) / 100))

    def tearDown(self):
        super(bidirectional, self).tearDown()

    def _async_update_delete_data(self):
        self._log.info("The tasks:-")
        tasks = []
        """Setting up doc-ops at source nodes"""
        if self._doc_ops is not None or self._doc_ops_dest is not None:
            # allows multiple of them but one by one on either of the clusters
            if "update" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "update" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_update2, "update", 0))
            if "delete" in self._doc_ops:
                tasks.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", self._expires))
            if "delete" in self._doc_ops_dest:
                tasks.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0))
            time.sleep(5)
            for task in tasks:
                task.result()

    """Loading on both clusters. Async Rebalance-In node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_in(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        for num_rebalance in range(self._num_rebalance):
            for node in self._floating_servers_set:
                tasks = []
                if "source" in self._rebalance:
                    tasks.extend(self._async_rebalance(self.src_nodes, [node], []))
                    self._log.info(
                        " Starting rebalance-in node {0} at Source cluster {1}".format(node.ip, self.src_master.ip))

                    self.src_nodes.extend([node])

                if "destination" in self._rebalance:
                    tasks.extend(self._async_rebalance(self.dest_nodes, [node], []))
                    self._log.info(" Starting rebalance-in node{0} at Destination cluster {1}".format(node.ip,
                        self.dest_master.ip))
                    self.dest_nodes.extend([node])

                self._async_update_delete_data()

                time.sleep(self._timeout/6)
                for task in tasks:
                    task.result()

                self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

                self.verify_results(verify_src=True)

    """Bidirectional load. Async Rebalance-Out non-master node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        for num_rebalance in range(self._num_rebalance):
            tasks = []
            if "source" in self._rebalance and self._num_rebalance < len(self.src_nodes):
                i = randrange(1, len(self.src_nodes))
                tasks.extend(self._async_rebalance(self.src_nodes, [], [self.src_nodes[i]]))
                self._log.info(" Starting rebalance-out node {0} at Source cluster {1}".format(self.src_nodes[i].ip,
                    self.src_master.ip))
                self.src_nodes.pop(i)
            if "destination" in self._rebalance and self._num_rebalance < len(self.dest_nodes):
                i = randrange(1, len(self.dest_nodes))
                tasks.extend(self._async_rebalance(self.dest_nodes, [], [self.dest_nodes[i]]))
                self._log.info(" Starting rebalance-out node{0} at Destination cluster {1}".format(self.src_nodes[i].ip,
                    self.dest_master.ip))
                self.dest_nodes.pop(i)

            self._async_update_delete_data()

            time.sleep(self._timeout/6)
            for task in tasks:
                task.result()

            self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

            self.verify_results(verify_src=True)

    """Bidirectional load. Async Rebalance-Out Master node at Source/Destination while
Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def async_rebalance_out_master(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)

        tasks = []
        if "source" in self._rebalance:
            tasks.extend(self._async_rebalance(self.src_nodes, [], [self.src_master]))
            self._log.info(" Starting rebalance-out Master node {0} at Source cluster {1}".format(self.src_master.ip,
                self.src_master.ip))
            self.src_nodes.remove(self.src_master)
            self.src_master = self.src_nodes[0]
        if "destination" in self._rebalance:
            tasks.extend(self._async_rebalance(self.dest_nodes, [], [self.dest_master]))
            self._log.info(
                " Starting rebalance-out Master node {0} at Destination cluster {1}".format(self.dest_master.ip,
                    self.dest_master.ip))
            self.dest_nodes.remove(self.dest_master)
            self.dest_master = self.dest_nodes[0]

        time.sleep(self._timeout/6)

        self._async_update_delete_data()

        time.sleep(self._timeout/6)
        for task in tasks:
            task.result()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.verify_results(verify_src=True)

        #TODO : 1. Swap Rebalance