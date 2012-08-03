from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from xdcrbasetests import XDCRReplicationBaseTest
from membase.helper.rebalance_helper import RebalanceHelper
from random import randrange

import time

#Assumption that at least 2 nodes on every cluster
#TODO fail the tests if this condition is not met
class bidirectional(XDCRReplicationBaseTest):
    def setUp(self):
        super(bidirectional, self).setUp()

        self.gen_create2 = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self._num_items)
        self.gen_delete2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size,
            start=int((self._num_items) * (float)(100 - self._percent_delete) / 100), end=self._num_items)
        self.gen_update2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, start=0,
            end=int(self._num_items * (float)(self._percent_update) / 100))

    def tearDown(self):
        super(bidirectional, self).tearDown()

        """Verify the stats at the destination cluster
        1. Data Validity check - using kvstore-node key-value check
        2. Item count check on source versus destination
        3. For deleted items, check the CAS/SeqNo/Expiry/Flags for same key on source/destination
        * Make sure to call expiry_pager function to flush out temp items(deleted/expired items)"""

    def _async_update_delete_data(self):
        print "The tasks:-"
        tasks = []
        #Setting up doc-ops at source nodes
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


    """Bidirectional replication between two clusters(currently), create-updates-deletes on DISJOINT sets on same bucket."""
    # TODO fix exit condition on mismatch error, to check for a range instead of exiting on 1st mismatch
    def load_with_ops(self):
        self._modify_src_data()

        # Setting up doc_ops_dest at destination nodes
        if self._doc_ops_dest is not None:
        # allows multiple of them but one by one
            if "create" in self._doc_ops_dest:
                self._load_all_buckets(self.dest_master, self.gen_create2, "create", self._expires)
            if "update" in self._doc_ops_dest:
                self._load_all_buckets(self.dest_master, self.gen_update2, "update", self._expires)
            if "delete" in self._doc_ops_dest:
                self._load_all_buckets(self.dest_master, self.gen_delete2, "delete", self._expires)
            self._wait_for_stats_all_buckets(self.dest_nodes)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.verify_results(verify_src=True)

    """Bidirectional replication between two clusters(currently), create-updates-deletes on DISJOINT sets on same bucket.
    Here running incremental load on both cluster1 and cluster2 as specified by the user/conf file"""

    def load_with_async_ops(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", self._expires)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", self._expires)

        self._async_update_delete_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.verify_results(verify_src=True)

    """Testing Bidirectional load( Loading at source/destination). Failover node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """
    def load_with_async_ops_and_joint_sets(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        time.sleep(60)

        self._async_update_delete_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.verify_results(verify_src=True)

    def load_with_async_ops_with_warmup(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", self._expires)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", self._expires)

        time.sleep(30)
        #warmup
        warmupnodes = []
        dest_warm_flag = 0
        if self._warmup == "source":
            warmupnode = self.src_nodes
        elif self._warmup == "destination":
            warmupnode = self.dest_nodes
        elif self._warmup == "all":
            warmupnode = self.src_nodes
            dest_warm_flag = 1
        warmupnodes.append(warmupnode[randrange(1, len(warmupnode))])
        self.do_a_warm_up(warmupnodes[0])
        if dest_warm_flag == 1:
            warmupnodes.append(self.dest_nodes[randrange(1, len(self.dest_nodes))])
            self.do_a_warm_up(warmupnodes[1])
        time.sleep(30)

        self._async_update_delete_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        time.sleep(60)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results(verify_src=True)

    def load_with_async_ops_with_warmup_master(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", self._expires)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", self._expires)

        time.sleep(30)
        #warmup
        warmupnodes = []
        dest_warm_flag = 0
        if self._warmup == "source":
            warmupnodes.append(self.src_master)
        elif self._warmup == "destination":
            warmupnodes.append(self.dest_master)
        elif self._warmup == "all":
            warmupnodes.append(self.src_master)
            dest_warm_flag = 1
        self.do_a_warm_up(warmupnodes[0])
        if dest_warm_flag == 1:
            warmupnodes.append(self.dest_master)
            self.do_a_warm_up(warmupnodes[1])
        time.sleep(30)

        self._async_update_delete_data()

        time.sleep(30)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results(verify_src=True)

    def load_with_async_ops_and_joint_sets_with_warmup(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        time.sleep(30)
        #warmup
        warmupnodes = []
        dest_warm_flag = 0
        if self._warmup == "source":
            warmupnode = self.src_nodes
        elif self._warmup == "destination":
            warmupnode = self.dest_nodes
        elif self._warmup == "all":
            warmupnode = self.src_nodes
            dest_warm_flag = 1
        warmupnodes.append(warmupnode[randrange(1, len(warmupnode))])
        self.do_a_warm_up(warmupnodes[0])
        if dest_warm_flag == 1:
            warmupnodes.append(self.dest_nodes[randrange(1, len(self.dest_nodes))])
            self.do_a_warm_up(warmupnodes[1])
        time.sleep(30)

        self._async_update_delete_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        time.sleep(30)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results(verify_src=True)

    def load_with_async_ops_and_joint_sets_with_warmup_master(self):
        if "create" in self._doc_ops:
            self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        time.sleep(30)
        #warmup
        warmupnodes = []
        dest_warm_flag = 0
        if self._warmup == "source":
            warmupnodes.append(self.src_master)
        elif self._warmup == "destination":
            warmupnodes.append(self.dest_master)
        elif self._warmup == "all":
            warmupnodes.append(self.src_master)
            dest_warm_flag = 1
        self.do_a_warm_up(warmupnodes[0])
        if dest_warm_flag == 1:
            warmupnodes.append(self.dest_master)
            self.do_a_warm_up(warmupnodes[1])
        time.sleep(30)

        self._async_update_delete_data()

        time.sleep(30)

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results(verify_src=True)

    def load_with_failover(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", self._expires)
        self._load_all_buckets(self.dest_master, self.gen_create2, "create", self._expires)

        if "source" in self._failover:
            i = randrange(1, len(self.src_nodes))
            self._cluster_helper.failover(self.src_nodes, [self.src_nodes[i]])
            self._log.info(" Failing over Source Non-Master Node {0}".format(self.src_nodes[i].ip))
        if "destination" in self._failover:
            i = randrange(1, len(self.dest_nodes))
            self._cluster_helper.failover(self.dest_nodes, [self.dest_nodes[i]])
            self._log.info(" Failing over Destination Non-Master Node {0}".format(self.dest_nodes[i].ip))

        self._async_update_delete_data()

        self.merge_buckets(self.src_master, self.dest_master, bidirection=True)

        if "source" in self._failover or "src_master" in self._failover:
            self._log.info(" Rebalance out Source Non-Master Node {0}".format(self.src_nodes[i].ip))
            self._cluster_helper.rebalance(self.src_nodes, [], [self.src_nodes[i]])
            self.src_nodes.pop(i)

        if "destination" in self._failover:
            self._log.info(" Rebalance out Destination Non-Master Node {0}".format(self.dest_nodes[i].ip))
            self._cluster_helper.rebalance(self.dest_nodes, [], [self.dest_nodes[i]])
            self.dest_nodes.pop(i)

        self.verify_xdcr_stats(self.src_nodes, self.dest_nodes)