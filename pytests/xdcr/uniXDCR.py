from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from membase.helper.rebalance_helper import RebalanceHelper
from xdcrbasetests import XDCRReplicationBaseTest
from remote.remote_util import RemoteMachineShellConnection
from random import randrange

import time

#Assumption that at least 2 nodes on every cluster
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

    """Verify the stats at the destination cluster
    1. Data Validity check - using kvstore-node key-value check
    2. Item count check on source versus destination
    3. For deleted items, check the CAS/SeqNo/Expiry/Flags for same key on source/destination
    * Make sure to call expiry_pager function to flush out temp items(deleted/expired items)"""

    """Testing Unidirectional load( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters.Create/Update/Delete operations are performed based on doc-ops specified by the user. """
    def load_with_ops(self):
        self._modify_src_data()

        time.sleep(60)

        self.merge_buckets(self.src_master, self.dest_master)

        self.verify_results()

    """Testing Unidirectional load( Loading only at source) Verifying whether XDCR replication is successful on
    subsequent destination clusters. Create/Update/Delete are performed in parallel- doc-ops specified by the user. """

    def load_with_async_ops(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        self._async_modify_data()

        self.merge_buckets(self.src_master, self.dest_master)

        self._wait_for_stats_all_buckets(self.src_nodes)

        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed after based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """
    def load_with_ops_with_warmup(self):
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

        self._modify_src_data()

        time.sleep(60)

        self._wait_for_stats_all_buckets(self.src_nodes)

        self.merge_buckets(self.src_master, self.dest_master)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_ops_with_warmup_master(self):
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

        self._modify_src_data()

        time.sleep(60)

        self.merge_buckets(self.src_master, self.dest_master)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_async_ops_with_warmup(self):
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

        self._async_modify_data()

        time.sleep(30)

        self.merge_buckets(self.src_master, self.dest_master)

        self._wait_for_stats_all_buckets(self.src_nodes)
        self.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_async_ops_with_warmup_master(self):
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

        self._async_modify_data()

        time.sleep(30)

        self.merge_buckets(self.src_master, self.dest_master)

        self._wait_for_stats_all_buckets(self.src_nodes)

        self.wait_warmup_completed(warmupnodes)

        self.verify_results()

    def load_with_failover(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        time.sleep(60)

        if "source" in self._failover:
            i = randrange(1, len(self.src_nodes))
            self._log.info(" Failing over Source Non-Master Node {0}".format(self.src_nodes[i].ip))
            self._cluster_helper.failover(self.src_nodes, [self.src_nodes[i]])
            self._log.info(" Rebalance out Source Non-Master Node {0}".format(self.src_nodes[i].ip))
            self._cluster_helper.rebalance(self.src_nodes, [], [self.src_nodes[i]])
            self.src_nodes.pop(i)
        if "destination" in self._failover:
            i = randrange(1, len(self.dest_nodes))
            self._log.info(" Failing over Destination Non-Master Node {0}".format(self.dest_nodes[i].ip))
            self._cluster_helper.failover(self.dest_nodes, [self.dest_nodes[i]])
            self._log.info(" Rebalance out Destination Non-Master Node {0}".format(self.dest_nodes[i].ip))
            self._cluster_helper.rebalance(self.dest_nodes, [], [self.dest_nodes[i]])
            self.dest_nodes.pop(i)

        time.sleep(5)

        self._async_modify_data()

        self.merge_buckets(self.src_master, self.dest_master)

        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """

    def load_with_failover_master(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        time.sleep(60)

        if "source" in self._failover:
            i = randrange(1, len(self.src_nodes))
            self._log.info(" Failing over Source Master Node {0}".format(self.src_master.ip))
            self._cluster_helper.failover(self.src_nodes, [self.src_master])
            self._log.info(" Rebalance out Source Master Node {0}".format(self.src_master.ip))
            self._cluster_helper.rebalance(self.src_nodes, [], [self.src_master])
            self.src_nodes.remove(self.src_master)
            self.src_master=self.src_nodes[0]

        if "destination" in self._failover:
            self._log.info(" Failing over Destination Master Node {0}".format(self.dest_master.ip))
            self._cluster_helper.failover(self.dest_nodes, [self.dest_master])
            self._log.info(" Rebalance out Destination Master Node {0}".format(self.dest_master.ip))
            self._cluster_helper.rebalance(self.dest_nodes, [], [self.dest_master])
            self.dest_nodes.remove(self.dest_master)
            self.dest_master=self.dest_nodes[0]

        time.sleep(10)

        self._async_modify_data()

        self.merge_buckets(self.src_master, self.dest_master)

        self.verify_results()

    """Testing Unidirectional load( Loading only at source). Failover node at Source/Destination while
    Create/Update/Delete are performed in parallel based on doc-ops specified by the user.
    Verifying whether XDCR replication is successful on subsequent destination clusters. """
    def load_with_async_failover(self):
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)

        time.sleep(60)

        tasks = []
        """Setting up failover while creates/updates/deletes at source nodes"""
        if "source" in self._failover:
            i = randrange(1, len(self.src_nodes))
            tasks.extend(self._async_failover(self.src_nodes, [self.src_nodes[i]]))
            self._log.info(" Failing over Source Node {0}".format(self.src_nodes[i].ip))
        if "destination" in self._failover:
            i = randrange(1, len(self.dest_nodes))
            tasks.extend(self._async_failover(self.dest_nodes, [self.dest_nodes[i]]))
            self._log.info(" Failing over Destination Node {0}".format(self.dest_nodes[i].ip))

        self._async_modify_data()

        time.sleep(15)
        for task in tasks:
            task.result()

        self.merge_buckets(self.src_master, self.dest_master)

        if "source" in self._failover:
            self._log.info(" Rebalance out Source Node {0}".format(self.src_nodes[i].ip))
            self._cluster_helper.rebalance(self.src_nodes, [], [self.src_nodes[i]])
            self.src_nodes.pop(i)

        if "destination" in self._failover:
            self._log.info(" Rebalance out Destination Node {0}".format(self.dest_nodes[i].ip))
            self._cluster_helper.rebalance(self.dest_nodes, [], [self.dest_nodes[i]])
            self.dest_nodes.pop(i)

            self.verify_results()
            #ToDO - Failover and ADD BACK NODE