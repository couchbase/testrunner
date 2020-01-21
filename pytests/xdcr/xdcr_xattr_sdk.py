import json
import re
from threading import Thread
import httplib2

import couchbase.subdocument as SD
import crc32
from sdk_client import SDKClient

from lib.membase.api.exception import XDCRException
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from .xdcrnewbasetests import XDCRNewBaseTest, REPLICATION_DIRECTION, TOPOLOGY


# Assumption that at least 2 nodes on every cluster
class XDCRXattr(XDCRNewBaseTest):
    def setUp(self):
        super(XDCRXattr, self).setUp()
        self.__topology = self._input.param("ctopology", TOPOLOGY.CHAIN)
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()
        self.only_store_hash = False

        for _, cluster in enumerate(self.get_cb_clusters()):
            for bucket in cluster.get_buckets():
                testuser = [{'id': bucket.name, 'name': bucket.name, 'password': 'password'}]
                rolelist = [{'id': bucket.name, 'name': bucket.name, 'roles': 'admin'}]
                self.add_built_in_server_user(testuser=testuser, rolelist=rolelist, node=cluster.get_master_node())
        for cluster in self.get_cb_clusters():
            for node in cluster.get_nodes():
                shell = RemoteMachineShellConnection(node)
                # TODO: works for Centos6 only now
                shell.execute_command('initctl stop sync_gateway')

    def tearDown(self):
        super(XDCRXattr, self).tearDown()

    def __load_chain(self, start_num=0):
        for i, cluster in enumerate(self.get_cb_clusters()):
            if self._rdirection == REPLICATION_DIRECTION.BIDIRECTION:
                if i > len(self.get_cb_clusters()) - 1:
                    break
            else:
                if i >= len(self.get_cb_clusters()) - 1:
                    break
            if not self._dgm_run:
                for bucket in cluster.get_buckets():
                    client = SDKClient(scheme="couchbase", hosts=[cluster.get_master_node().ip],
                                            bucket=bucket.name).cb
                    for i in range(start_num, start_num + self._num_items):
                        key = 'k_%s_%s' % (i, str(cluster).replace(' ', '_').
                                           replace('.', '_').replace(',', '_').replace(':', '_'))
                        value = {'xattr_%s' % i:'value%s' % i}
                        client.upsert(key, value)
                        client.mutate_in(key, SD.upsert('xattr_%s' % i, 'value%s' % i,
                                                             xattr=True,
                                                             create_parents=True))
                        partition = bucket.kvs[1].acquire_partition(key)#["partition"]
                        if self.only_store_hash:
                            value = str(crc32.crc32_hash(value))
                        res = client.get(key)
                        partition.set(key, json.dumps(value), 0, res.flags)
                        bucket.kvs[1].release_partition(key)

            else:
                cluster.load_all_buckets_till_dgm(
                    active_resident_threshold=self._active_resident_threshold,
                    items=self._num_items)

    def load_data_topology(self, start_num = 0):
        """load data as per ctopology test parameter
        """
        if self.__topology == TOPOLOGY.CHAIN:
            self.__load_chain(start_num)
        elif self.__topology == TOPOLOGY.STAR:
            self.__load_star()
        elif self.__topology == TOPOLOGY.RING:
            self.__load_ring()
        elif self._input.param(TOPOLOGY.HYBRID, 0):
            self.__load_star()
        else:
            raise XDCRException(
                'Unknown topology set: {0}'.format(
                    self.__topology))

    def __merge_keys(
            self, kv_src_bucket, kv_dest_bucket, kvs_num=1, filter_exp=None):
        """ Will merge kv_src_bucket keys that match the filter_expression
            if any into kv_dest_bucket.
        """
        valid_keys_src, deleted_keys_src = kv_src_bucket[
            kvs_num].key_set()
        valid_keys_dest, deleted_keys_dest = kv_dest_bucket[
            kvs_num].key_set()

        self.log.info("src_kvstore has %s valid and %s deleted keys"
                      % (len(valid_keys_src), len(deleted_keys_src)))
        self.log.info("dest kvstore has %s valid and %s deleted keys"
                      % (len(valid_keys_dest), len(deleted_keys_dest)))

        if filter_exp:
            filtered_src_keys = [key for key in valid_keys_src if re.search(str(filter_exp), key) is not None]
            valid_keys_src = filtered_src_keys
            self.log.info(
                "{0} keys matched the filter expression {1}".format(
                    len(valid_keys_src),
                    filter_exp))

        for key in valid_keys_src:
            # replace/add the values for each key in src kvs
            if key not in deleted_keys_dest:
                partition1 = kv_src_bucket[kvs_num].acquire_partition(key)
                partition2 = kv_dest_bucket[kvs_num].acquire_partition(key)
                # In case of lww, if source's key timestamp is lower than
                # destination than no need to set.
                # with Synchronized(dict(part1)) as partition1, Synchronized(dict(part2)) as partition2:
                if self.get_lww() and partition1.get_timestamp(
                    key) < partition2.get_timestamp(key):
                    continue
                key_add = partition1.get_key(key)
                partition2.set(
                        key,
                        key_add["value"],
                        key_add["expires"],
                        key_add["flag"])
                kv_src_bucket[kvs_num].release_partition(key)
                kv_dest_bucket[kvs_num].release_partition(key)

        for key in deleted_keys_src:
            if key not in deleted_keys_dest:
                partition1 = kv_src_bucket[kvs_num].acquire_partition(key)
                partition2 = kv_dest_bucket[kvs_num].acquire_partition(key)
                # In case of lww, if source's key timestamp is lower than
                # destination than no need to delete.
                # with Synchronized(dict(part1)) as partition1, Synchronized(dict(part2)) as partition2:
                if self.__lww and partition1.get_timestamp(
                    key) < partition2.get_timestamp(key):
                    continue
                partition2.delete(key)
                kv_src_bucket[kvs_num].release_partition(key)
                kv_dest_bucket[kvs_num].release_partition(key)


        valid_keys_dest, deleted_keys_dest = kv_dest_bucket[
            kvs_num].key_set()
        self.log.info("After merging: destination bucket's kv_store now has {0}"
                      " valid keys and {1} deleted keys".
                      format(len(valid_keys_dest), len(deleted_keys_dest)))

    def __merge_all_buckets(self):
        """Merge bucket data between source and destination bucket
        for data verification. This method should be called after replication started.
        """
        # TODO need to be tested for Hybrid Topology
        for cb_cluster in self.get_cb_clusters():
            for remote_cluster_ref in cb_cluster.get_remote_clusters():
                for repl in remote_cluster_ref.get_replications():
                    self.log.info("Merging keys for replication {0}".format(repl))
                    self.__merge_keys(
                        repl.get_src_bucket().kvs,
                        repl.get_dest_bucket().kvs,
                        kvs_num=1,
                        filter_exp=repl.get_filter_exp())

    def verify_results(self, skip_verify_data=[], skip_verify_revid=[], sg_run=False):
        """Verify data between each couchbase and remote clusters.
        Run below steps for each source and destination cluster..
            1. Run expiry pager.
            2. Wait for disk queue size to 0 on each nodes.
            3. Wait for Outbound mutations to 0.
            4. Wait for Items counts equal to kv_store size of buckets.
            5. Verify items value on each bucket.
            6. Verify Revision id of each item.
        """
        skip_key_validation = self._input.param("skip_key_validation", False)
        self.__merge_all_buckets()
        for cb_cluster in self.get_cb_clusters():
            for remote_cluster_ref in cb_cluster.get_remote_clusters():
                try:
                    src_cluster = remote_cluster_ref.get_src_cluster()
                    dest_cluster = remote_cluster_ref.get_dest_cluster()

                    if self._evict_with_compactor:
                        for b in src_cluster.get_buckets():
                            # only need to do compaction on the source cluster, evictions are propagated to the remote
                            # cluster
                            src_cluster.get_cluster().compact_bucket(src_cluster.get_master_node(), b)

                    else:
                        src_cluster.run_expiry_pager()
                        dest_cluster.run_expiry_pager()

                    src_cluster.wait_for_flusher_empty()
                    dest_cluster.wait_for_flusher_empty()

                    src_dcp_queue_drained = src_cluster.wait_for_dcp_queue_drain()
                    dest_dcp_queue_drained = dest_cluster.wait_for_dcp_queue_drain()

                    src_cluster.wait_for_outbound_mutations()
                    dest_cluster.wait_for_outbound_mutations()
                except Exception as e:
                    # just log any exception thrown, do not fail test
                    self.log.error(e)
                if not skip_key_validation:
                    try:
                        if not sg_run:
                            src_active_passed, src_replica_passed = \
                                src_cluster.verify_items_count(timeout=self._item_count_timeout)
                            dest_active_passed, dest_replica_passed = \
                                dest_cluster.verify_items_count(timeout=self._item_count_timeout)

                        src_cluster.verify_data(max_verify=self._max_verify, skip=skip_verify_data,
                                                only_store_hash=self.only_store_hash)
                        dest_cluster.verify_data(max_verify=self._max_verify, skip=skip_verify_data,
                                                 only_store_hash=self.only_store_hash)
                        for _, cluster in enumerate(self.get_cb_clusters()):
                            for bucket in cluster.get_buckets():
                                h = httplib2.Http(".cache")
                                resp, content = h.request(
                                    "http://{0}:4984/db/_all_docs".format(cluster.get_master_node().ip))
                                self.assertEqual(json.loads(content)['total_rows'], self._num_items)
                                client = SDKClient(scheme="couchbase", hosts=[cluster.get_master_node().ip],
                                                        bucket=bucket.name).cb
                                for i in range(self._num_items):
                                    key = 'k_%s_%s' % (i, str(cluster).replace(' ', '_').
                                                      replace('.', '_').replace(',', '_').replace(':', '_'))
                                    res = client.get(key)
                                    for xk, xv in res.value.items():
                                        rv = client.mutate_in(key, SD.get(xk, xattr=True))
                                        self.assertTrue(rv.exists(xk))
                                        self.assertEqual(xv, rv[xk])
                                    if sg_run:
                                        resp, content = h.request("http://{0}:4984/db/{1}".format(cluster.get_master_node().ip, key))
                                        self.assertEqual(json.loads(content)['_id'], key)
                                        self.assertEqual(json.loads(content)[xk], xv)
                                        self.assertTrue('2-' in json.loads(content)['_rev'])
                    except Exception as e:
                        self.log.error(e)
                    finally:
                        if not sg_run:
                            rev_err_count = self.verify_rev_ids(remote_cluster_ref.get_replications(),
                                                            skip=skip_verify_revid)
                            # we're done with the test, now report specific errors
                            if (not (src_active_passed and dest_active_passed)) and \
                                    (not (src_dcp_queue_drained and dest_dcp_queue_drained)):
                                self.fail("Incomplete replication: Keys stuck in dcp queue")
                            if not (src_active_passed and dest_active_passed):
                                self.fail("Incomplete replication: Active key count is incorrect")
                            if not (src_replica_passed and dest_replica_passed):
                                self.fail("Incomplete intra-cluster replication: "
                                          "replica count did not match active count")
                            if rev_err_count > 0:
                                self.fail("RevID verification failed for remote-cluster: {0}".
                                          format(remote_cluster_ref))

        # treat errors in self.__report_error_list as failures
        if len(self.get_report_error_list()) > 0:
            error_logger = self.check_errors_in_goxdcr_logs()
            if error_logger:
                self.fail("Errors found in logs : {0}".format(error_logger))

    def load_with_ops(self):
        self.setup_xdcr()
        self.load_data_topology()
        # self.perform_update_delete()
        self.verify_results()

    def load_with_ops_rebalance_out_in(self):
        self.setup_xdcr()

        thread_load = Thread(target=self.load_data_topology, args=())
        thread_load.start()

        rebalance_src = self.get_cluster_op().async_rebalance(
            self.src_cluster.get_nodes(), [], self.src_cluster.get_nodes()[1:])
        rebalance_dest = self.get_cluster_op().async_rebalance(self.dest_cluster.get_nodes(), [],
                                                          self.dest_cluster.get_nodes()[1:])
        rebalance_src.result()
        rebalance_dest.result()

        rebalance_src = self.get_cluster_op().async_rebalance(
            self.src_cluster.get_nodes()[:1], self.src_cluster.get_nodes()[1:], [])
        rebalance_dest = self.get_cluster_op().async_rebalance(
            self.dest_cluster.get_nodes()[:1], self.dest_cluster.get_nodes()[1:], [])
        rebalance_src.result()
        rebalance_dest.result()

        thread_load.join()
        self.verify_results()

    def load_with_ops_rebalance_out_in_with_updates(self):
        self.setup_xdcr()

        thread_load = Thread(target=self.load_data_topology, args=())
        thread_load.start()

        rebalance_src = self.get_cluster_op().async_rebalance(
        self.src_cluster.get_nodes(), [], self.src_cluster.get_nodes()[1:])
        rebalance_dest = self.get_cluster_op().async_rebalance(self.dest_cluster.get_nodes(), [],
                                                           self.dest_cluster.get_nodes()[1:])
        rebalance_src.result()
        rebalance_dest.result()

        thread_load2 = Thread(target=self.load_data_topology, args=(self._num_items/2,))
        thread_load2.start()

        rebalance_src = self.get_cluster_op().async_rebalance(
            self.src_cluster.get_nodes()[:1], self.src_cluster.get_nodes()[1:], [])
        rebalance_dest = self.get_cluster_op().async_rebalance(
        self.dest_cluster.get_nodes()[:1], self.dest_cluster.get_nodes()[1:], [])
        rebalance_src.result()
        rebalance_dest.result()

        thread_load.join()
        thread_load2.join()
        self.verify_results()


class XDCRXattrSG(XDCRXattr):
    def setUp(self):
        super(XDCRXattrSG, self).setUp()

    def tearDown(self):
        super(XDCRXattr, self).tearDown()

    def load_with_ops(self):
        self.setup_xdcr()
        for cluster in self.get_cb_clusters():
            for node in cluster.get_nodes():
                shell = RemoteMachineShellConnection(node)
                shell.copy_file_local_to_remote("pytests/sg/resources/sg_localhost_default_xattrs.conf",
                                                "/home/sync_gateway/sync_gateway.json")
                # TODO: works for Centos6 only now
                shell.execute_command('initctl stop sync_gateway')
                shell.execute_command('initctl start sync_gateway')
        self.load_data_topology()
        self.verify_results(sg_run=True)
