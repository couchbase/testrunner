import threading

from lib import global_vars
from lib.SystemEventLogLib.fts_service_events import SearchServiceEvents
from .fts_base import FTSBaseTest, FTSException
from .fts_base import NodeHelper
from TestInput import TestInputSingleton
from threading import Thread
from lib.remote.remote_util import RemoteMachineShellConnection
from lib.memcached.helper.data_helper import MemcachedClientHelper
from lib.membase.api.rest_client import RestConnection, RestHelper
import json
import time

class MovingTopFTS(FTSBaseTest):

    def setUp(self):
        self.num_rebalance = TestInputSingleton.input.param("num_rebalance", 1)
        self.retry_time = TestInputSingleton.input.param("retry_time", 300)
        self.num_retries = TestInputSingleton.input.param("num_retries", 1)
        self.rebalance_in = TestInputSingleton.input.param("rebalance_in", False)
        self.rebalance_out = TestInputSingleton.input.param("rebalance_out", False)
        self.disable_file_transfer_rebalance = TestInputSingleton.input.param("disableFileTransferRebalance", False)
        self.max_concurrent_partition_moves_per_node = TestInputSingleton.input.param(
            "maxConcurrentPartitionMovesPerNode", 1)
        self.default_concurrent_partition_moves_per_node = 1
        self.query = {"match": "emp", "field": "type"}
        super(MovingTopFTS, self).setUp()
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_disableFileTransferRebalance(self.disable_file_transfer_rebalance)
        if not self.capella_run:
            self.index_path = rest.get_index_path()
            if self.index_path == "/data":
                self.reset_data_mount_point(self._cb_cluster.get_fts_nodes())

    def tearDown(self):
        super(MovingTopFTS, self).tearDown()
        if not self.capella_run and self.index_path == "/data":
            try:
                self.reset_data_mount_point(self._cb_cluster.get_fts_nodes())
            except Exception as err:
                self.log.info(str(err))

    # def suite_setUp(self):
    #     self.log.info("*** MovingTopFTS: suite_setUp() ***")

    # def suite_tearDown(self):
    #     self.log.info("*** MovingTopFTS: suite_tearDown() ***")

    """ Topology change during indexing"""

    def kill_fts_service(self, timeout=0, retries=1):
        fts_node = self._cb_cluster.get_random_fts_node()
        self.sleep(timeout)
        for i in range(retries):
            NodeHelper.kill_cbft_process(fts_node)
        return fts_node

    @staticmethod
    def reset_data_mount_point(nodes):
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            shell.execute_command('umount -l /data; '
                                  'mkdir -p /usr/disk-img; dd if=/dev/zero '
                                  'of=/usr/disk-img/disk-quota.ext4 count=10485760; '
                                  '/sbin/mkfs -t ext4 -q /usr/disk-img/disk-quota.ext4 -F; '
                                  'mount -o loop,rw,usrquota,grpquota /usr/disk-img/disk-quota.ext4 /data; '
                                  'umount -l /data; fsck.ext4 /usr/disk-img/disk-quota.ext4 -y; '
                                  'chattr +i /data; mount -o loop,rw,usrquota,grpquota /usr/disk-img/disk-quota.ext4 '
                                  '/data; rm -rf /data/*; chmod -R 777 /data')

    def stop_cb_service(self, nodes, timeout=0):
        self.sleep(timeout)
        for n in nodes:
            remote = RemoteMachineShellConnection(n)
            remote.stop_couchbase()

    def start_cb_service(self, nodes):
        for n in nodes:
            remote = RemoteMachineShellConnection(n)
            remote.start_couchbase()

    def kill_erlang_service(self, timeout):
        fts_node = self._cb_cluster.get_random_fts_node()
        self.sleep(timeout)
        NodeHelper.kill_erlang(fts_node)

    def swap_rebalance_parallel_partitions_move(self):
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_maxConcurrentPartitionMovesPerNode(self.default_concurrent_partition_moves_per_node)
        rest.set_maxFeedsPerDCPAgent(1)
        rest.set_maxDCPAgents(3)

        self.load_data()
        self.create_fts_indexes_all_buckets()
        for index in self._cb_cluster.get_indexes():
            index.update_index_partitions(self.num_index_partitions)

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        if rest.is_enterprise_edition():
            services = ["fts"]
        else:
            services = ["fts,kv,index,n1ql"]

        start_rebalance_time = time.time()
        self._cb_cluster.swap_rebalance(services=services)
        end_rebalance_time = time.time()
        simple_rebalance_time = end_rebalance_time - start_rebalance_time


        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_maxConcurrentPartitionMovesPerNode(self.max_concurrent_partition_moves_per_node)

        self.sleep(30, "Addtional sleep between 2 rebalance sessions.")

        start_parallel_rebalance_time = time.time()
        self._cb_cluster.swap_rebalance(services=services)
        end_parallel_rebalance_time = time.time()
        parallel_rebalance_time = end_parallel_rebalance_time - start_parallel_rebalance_time

        self.log.info("Simple rebalance took {0} sec.".
                      format(simple_rebalance_time))
        self.log.info("Concurrent partitions move rebalance took {0} sec.".
                      format(parallel_rebalance_time))
        self.log.info("Delta between simple and concurrent partitions move rebalance is {0} sec.".
                      format(simple_rebalance_time - parallel_rebalance_time))

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.validate_index_count(equal_bucket_doc_count=True)
        self.assertTrue(parallel_rebalance_time < simple_rebalance_time,
                        "Swap rebalance in with maxConcurrentPartitionMovesPerNode={0} takes longer time than "
                        "swap rebalance in with maxConcurrentPartitionMovesPerNode={1}"
                        .format(self.max_concurrent_partition_moves_per_node,
                                self.default_concurrent_partition_moves_per_node)
                        )

    def rebalance_in_parallel_partitions_move_add_node(self):
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_maxConcurrentPartitionMovesPerNode(self.default_concurrent_partition_moves_per_node)
        rest.set_maxFeedsPerDCPAgent(1)
        rest.set_maxDCPAgents(3)

        self.load_data()
        self.create_fts_indexes_all_buckets()
        for index in self._cb_cluster.get_indexes():
            index.update_index_partitions(self.num_index_partitions)
        self.sleep(10)
        self.log.info("Index building has begun...")

        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        start_rebalance_time = time.time()
        self._cb_cluster.rebalance_in(num_nodes=1, services=["fts"])
        end_rebalance_time = time.time()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        simple_rebalance_time = end_rebalance_time - start_rebalance_time

        self._cb_cluster.rebalance_out(num_nodes=1)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_maxConcurrentPartitionMovesPerNode(self.max_concurrent_partition_moves_per_node)

        start_parallel_rebalance_time = time.time()
        self._cb_cluster.rebalance_in(num_nodes=1, services=["fts"])
        end_parallel_rebalance_time = time.time()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        parallel_rebalance_time = end_parallel_rebalance_time - start_parallel_rebalance_time
        self.log.info("Delta between simple and concurrent partitions move rebalance is {0} sec.".
                      format(simple_rebalance_time - parallel_rebalance_time))
        self.validate_index_count(equal_bucket_doc_count=True)
        self.assertTrue(simple_rebalance_time > parallel_rebalance_time,
                        "Rebalance in with maxConcurrentPartitionMovesPerNode={0} takes longer time than "
                        "rebalance in with maxConcurrentPartitionMovesPerNode={1}".
                        format(self.max_concurrent_partition_moves_per_node,
                               self.default_concurrent_partition_moves_per_node))

    def rebalance_out_parallel_partitions_move(self):
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_maxConcurrentPartitionMovesPerNode(self.default_concurrent_partition_moves_per_node)
        rest.set_maxFeedsPerDCPAgent(1)
        rest.set_maxDCPAgents(3)

        self.load_data()
        self.create_fts_indexes_all_buckets()
        for index in self._cb_cluster.get_indexes():
            index.update_index_partitions(self.num_index_partitions)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        simple_rebalance_start_time = time.time()
        self._cb_cluster.rebalance_out(num_nodes=self.num_rebalance)
        simple_rebalance_finish_time = time.time()
        simple_rebalance_time = simple_rebalance_finish_time - simple_rebalance_start_time

        self._cb_cluster.rebalance_in(num_nodes=1)

        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_maxConcurrentPartitionMovesPerNode(self.max_concurrent_partition_moves_per_node)

        parallel_rebalance_start_time = time.time()
        self._cb_cluster.rebalance_out(num_nodes=self.num_rebalance)
        parallel_rebalance_finish_time = time.time()
        parallel_rebalance_time = parallel_rebalance_finish_time - parallel_rebalance_start_time

        self.log.info("Delta between simple and concurrent partitions move rebalance is {0} sec.".
                      format(simple_rebalance_time - parallel_rebalance_time))

        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                                expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)

        self.assertTrue(simple_rebalance_time > parallel_rebalance_time,
                        "Rebalance out with maxConcurrentPartitionMovesPerNode={0} takes longer time than "
                        "rebalance out with maxConcurrentPartitionMovesPerNode={1}".
                        format(self.max_concurrent_partition_moves_per_node,
                               self.default_concurrent_partition_moves_per_node))

    def failover_non_master_parallel_partitions_move(self):
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_maxConcurrentPartitionMovesPerNode(self.default_concurrent_partition_moves_per_node)
        rest.set_maxFeedsPerDCPAgent(1)

        self.load_data()
        self.create_fts_indexes_all_buckets()
        for index in self._cb_cluster.get_indexes():
            index.update_index_partitions(self.num_index_partitions)
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        simple_rebalance_start_time = time.time()
        self._cb_cluster.failover_and_rebalance_nodes()
        simple_rebalance_finish_time = time.time()
        simple_rebalance_time = simple_rebalance_finish_time - simple_rebalance_start_time

        self._cb_cluster.rebalance_in(num_nodes=1)

        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_maxConcurrentPartitionMovesPerNode(self.max_concurrent_partition_moves_per_node)

        parallel_rebalance_start_time = time.time()
        self._cb_cluster.failover_and_rebalance_nodes()
        parallel_rebalance_finish_time = time.time()
        parallel_rebalance_time = parallel_rebalance_finish_time - parallel_rebalance_start_time

        self.log.info("Delta between simple and concurrent partitions move rebalance is {0} sec.".
                      format(simple_rebalance_time - parallel_rebalance_time))

        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
        except Exception as e:
            if self._cb_cluster.get_num_fts_nodes() == 0:
                self.log.info("Expected exception: %s" % e)
            else:
                raise e
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.assertTrue(simple_rebalance_time > parallel_rebalance_time,
                        "Failover and rebalance with maxConcurrentPartitionMovesPerNode={0} takes longer time than "
                        "failover and rebalance out with maxConcurrentPartitionMovesPerNode={1}".
                        format(self.max_concurrent_partition_moves_per_node,
                               self.default_concurrent_partition_moves_per_node))

    def rebalance_in_during_index_building(self):
        if self._input.param("must_fail", False):
            self.fail("Temporal fail to let all the other tests to be passed")
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_in(num_nodes=1, services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def retry_rebalance_in_during_index_building(self):
        self._cb_cluster.enable_retry_rebalance(self.retry_time, self.num_retries)
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(1)
        self.log.info("Index building has begun...")
        self.sleep(1)
        node_to_rebalance_in = None
        for s in self._input.servers:
            node_in_use = False
            for s1 in self._cb_cluster.get_nodes():
                if s.ip == s1.ip:
                    node_in_use = True
                    break
            if not node_in_use:
                node_to_rebalance_in = s
                break
        if node_to_rebalance_in is None:
            self.fail("Cannot find free node to rebalance-in.")

        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name,index.get_indexed_doc_count()))

        stop_CB_thread = Thread(
            target=self.stop_cb_service,
            args=[[node_to_rebalance_in], 45])

        rebalance_was_successful = True
        try:
            stop_CB_thread.start()
            rebalance_task = self._cb_cluster.async_rebalance_in_node(nodes_in=node_to_rebalance_in, services=["kv"], sleep_before_rebalance=150)
            rebalance_task.result()

        except Exception as e:
            rebalance_was_successful = False
            self.log.error(str(e))
            self.start_cb_service([node_to_rebalance_in])
            self.sleep(5)
            self._check_retry_rebalance_succeeded()
        finally:
            self.start_cb_service([node_to_rebalance_in])
            if rebalance_was_successful:
                self.fail("Rebalance was successful")
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self._cb_cluster.disable_retry_rebalance()

    def rebalance_out_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(30)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_out()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def rebalance_2_nodes_during_index_building(self):
        index = self.create_index_generate_queries(wait_idx=False)
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        if self.rebalance_out:
            self._cb_cluster.rebalance_out(2)
        if self.rebalance_in:
            self._cb_cluster.rebalance_in(2, services=["fts", "fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.validate_index_count(equal_bucket_doc_count=True)
        tasks = []
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        if not self.disable_file_transfer_rebalance and not index.is_upside_down():
            file_copy_transfer_found, file_transfer_success, failed_file_transfer = \
                self.validate_file_copy_rebalance_stats()
            if not file_copy_transfer_found:
                self.log.info("no file based transfer found during rebalance")
            if not file_transfer_success:
                self.fail(f'Found file transfer failed for these partitions: {failed_file_transfer}')

    def rebalance_kill_fts_existing_fts_node(self):
        index = self.create_index_generate_queries(wait_idx=False)
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        fts_nodes = self._cb_cluster.get_fts_nodes()
        if self.rebalance_out:
            rebalance_task = self._cb_cluster.async_rebalance_out_node(node=fts_nodes[0])
            self.sleep(5)
            NodeHelper.kill_cbft_process(fts_nodes[1])
            rebalance_task.result()
        if self.rebalance_in:
            rebalance_task = self._cb_cluster.async_rebalance_in(1, services=["fts"])
            self.sleep(5)
            NodeHelper.kill_cbft_process(fts_nodes[0])
            rebalance_task.result()
        #Add stat validation
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.validate_index_count(equal_bucket_doc_count=True)
        tasks = []
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        if not self.disable_file_transfer_rebalance and not index.is_upside_down():
            file_copy_transfer_found, file_transfer_success, failed_file_transfer = \
                self.validate_file_copy_rebalance_stats()
            if not file_copy_transfer_found:
                self.log.info("no file based transfer found during rebalance")
            if not file_transfer_success:
                self.fail(f'Found file transfer failed for these partitions: {failed_file_transfer}')

    def rebalance_during_kv_mutations(self):
        index = self.create_index_generate_queries(wait_idx=False)
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        load_tasks = self.async_perform_update_delete(self.upd_del_fields, async_run=True)
        self.sleep(10)
        rebalance_task = None
        if self.rebalance_out:
            node_to_rebalance_out = self._cb_cluster.get_fts_nodes()[0]
            rebalance_task = self._cb_cluster.async_rebalance_out_node(node=node_to_rebalance_out)
        if self.rebalance_in:
            rebalance_task = self._cb_cluster.async_rebalance_in(1, services=["fts"])
        [task.result() for task in load_tasks]
        rebalance_task.result()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        tasks = []
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        # mutate some docs and run query again to test if mutated documents are indexing

        self.async_perform_update_delete(self.upd_del_fields)
        self.sleep(5)
        self.wait_for_indexing_complete()

        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        if not self.disable_file_transfer_rebalance and not index.is_upside_down():
            file_copy_transfer_found, file_transfer_success, failed_file_transfer = \
                self.validate_file_copy_rebalance_stats()
            if not file_copy_transfer_found:
                self.log.info("no file based transfer found during rebalance")
            if not file_transfer_success:
                self.fail(f'Found file transfer failed for these partitions: {failed_file_transfer}')

    def retry_rebalance_out_during_index_building(self):
        self._cb_cluster.enable_retry_rebalance(self.retry_time, self.num_retries)
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name,index.get_indexed_doc_count()))

        node_to_rebalance_out = self._cb_cluster.get_fts_nodes()[0]

        stop_CB_thread = Thread(
            target=self.stop_cb_service,
            args=[[node_to_rebalance_out], 45])

        rebalance_was_successful = True
        try:
            stop_CB_thread.start()
            rebalance_task = self._cb_cluster.rebalance_out_node(node=node_to_rebalance_out, sleep_before_rebalance=150)
            rebalance_task.result()

        except Exception as e:
            rebalance_was_successful = False
            self.log.error(str(e))
            self.start_cb_service([node_to_rebalance_out])
            self.sleep(5)
            self._check_retry_rebalance_succeeded()
        finally:
            self.start_cb_service([node_to_rebalance_out])
            if rebalance_was_successful:
                frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
                err = self.validate_partition_distribution(frest)
                if len(err) > 0:
                    self.fail(err)
                self.fail("Rebalance was successful")

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        for index in self._cb_cluster.get_indexes():
            if not self.disable_file_transfer_rebalance and not index.is_upside_down():
                file_copy_transfer_found, file_transfer_success, failed_file_transfer = \
                    self.validate_file_copy_rebalance_stats()
                if not file_copy_transfer_found:
                    self.log.info("no file based transfer found during rebalance")
                if not file_transfer_success:
                    self.fail(f'Found file transfer failed for these partitions: {failed_file_transfer}')
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def rebalance_out_master_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_out_master()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def swap_rebalance_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        rest = RestConnection(self._cb_cluster.get_master_node())
        if rest.is_enterprise_edition():
            services = ["fts"]
        else:
            services = ["fts,kv,index,n1ql"]
        self._cb_cluster.swap_rebalance(services=services)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def retry_swap_rebalance_during_index_building(self):
        self._cb_cluster.enable_retry_rebalance(self.retry_time, self.num_retries)
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        self.sleep(10)
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))

        try:
            reb_thread = Thread(
                target=self.kill_fts_service,
                args=[5])
            #reb_thread = Thread(
            #    target=self._cb_cluster.reboot_after_timeout,
            #    args=[5])
            reb_thread.start()
            self._cb_cluster.swap_rebalance(services=["fts"])
        except Exception as e:
            self.log.error(str(e))
            self.sleep(5)
            self._check_retry_rebalance_succeeded()

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def swap_rebalance_kv_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.swap_rebalance_master(services=["kv"])
        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
            self.wait_for_indexing_complete()
            self.validate_index_count(equal_bucket_doc_count=True)
        except Exception as e:
            # to work around fts node becoming master and
            # unable to rebalance out kv node
            self._cb_cluster.swap_rebalance_master(services=["fts"])
            self.validate_index_count(equal_bucket_doc_count=True)
            raise e
        self._cb_cluster.swap_rebalance_master(services=["fts"])
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def failover_non_master_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.failover_and_rebalance_nodes()
        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
        except Exception as e:
            if self._cb_cluster.get_num_fts_nodes() == 0:
                self.log.info("Expected exception: %s" % e)
            else:
                raise e
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def failover_no_rebalance_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.async_failover().result()
        self.sleep(60)
        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
        except Exception as e:
            self.log.info("Expected exception: %s" % e)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def failover_master_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_in(num_nodes=1, services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self._cb_cluster.failover_and_rebalance_master()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def failover_only_kv_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        try:
            self._cb_cluster.failover_and_rebalance_master()
        except Exception as e:
            self.log.info("Expected exception caught: %s" % e)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def hard_failover_and_delta_recovery_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        task = self._cb_cluster.async_failover()
        task.result()
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def hard_failover_and_full_recovery_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        task = self._cb_cluster.async_failover()
        task.result()
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def graceful_failover_and_delta_recovery_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        task = self._cb_cluster.async_failover(graceful=True)
        task.result()
        self.sleep(60)
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def graceful_failover_and_full_recovery_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        task = self._cb_cluster.async_failover(graceful=True)
        task.result()
        self.sleep(60)
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv, fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def warmup_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.warmup_node()
        self.sleep(60, "waiting for fts to start...")
        self.wait_for_indexing_complete()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def warmup_master_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.warmup_node(master=True)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def node_reboot_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        #self._cb_cluster.reboot_one_node(test_case=self)
        self.kill_fts_service(120)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def node_reboot_only_kv_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        #self._cb_cluster.reboot_one_node(test_case=self, master=True)
        self.kill_fts_service(120)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def memc_crash_on_kv_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        NodeHelper.kill_memcached(self._cb_cluster.get_master_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.sleep(30, "Sleep additional 30 seconds to refresh actual bucket and index docs counts.")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def erl_crash_on_kv_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        NodeHelper.kill_erlang(self._cb_cluster.get_master_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def fts_node_crash_during_index_building(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        NodeHelper.kill_cbft_process(self._cb_cluster.get_random_fts_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    """ Topology change between indexing and querying"""

    def rebalance_in_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        rest = RestConnection(self._cb_cluster.get_master_node())
        if rest.is_enterprise_edition():
            services = "kv,fts"
        else:
            services = "fts,kv,index,n1ql"
        self._cb_cluster.rebalance_in(num_nodes=self.num_rebalance,
                                      services=[services])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)

        self.wait_for_indexing_complete()

        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                                expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def rebalance_out_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        self._cb_cluster.rebalance_out(num_nodes=self.num_rebalance)

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def rebalance_out_master_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_out(master=True)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def swap_rebalance_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        self._cb_cluster.swap_rebalance(services=services,
                                        num_nodes=self.num_rebalance)

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def hard_failover_and_remove_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        self._cb_cluster.failover_and_rebalance_nodes()
        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
        except Exception as e:
            if self._cb_cluster.get_num_fts_nodes() == 0:
                self.log.info("Expected exception: %s" % e)
            else:
                raise e
        self.wait_for_indexing_complete()

        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def hard_failover_no_rebalance_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self._cb_cluster.async_failover().result()
        self.sleep(30)

        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query)
            if hits < self._find_expected_indexed_items_number():
                self.log.info("SUCCESS: Fewer docs ({0}) returned after "
                              "hard-failover".format(hits))
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def hard_failover_master_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self._cb_cluster.failover_and_rebalance_master()

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def hard_failover_and_delta_recovery_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        task = self._cb_cluster.async_failover()
        task.result()
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()

        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def hard_failover_and_full_recovery_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        task = self._cb_cluster.async_failover()
        task.result()
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()

        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def failover_no_rebalance_with_replicas_between_indexing_and_querying(self):
        index = self.create_index_generate_queries()
        self._cb_cluster.async_failover(
            graceful=self._input.param("graceful", False)).result()
        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
        except Exception as e:
            if self._cb_cluster.get_num_fts_nodes() == 0:
                self.log.info("Expected exception: %s" % e)
            else:
                raise e
        self.wait_for_indexing_complete()
        self.sleep(30, "Sleep for additional time to make sure, fts index is ready.")
        self.run_query_and_compare(index)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def graceful_failover_and_delta_recovery_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        task = self._cb_cluster.async_failover(graceful=True)
        task.result()
        self.sleep(30)
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()

        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def graceful_failover_and_full_recovery_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        task = self._cb_cluster.async_failover(graceful=True)
        task.result()
        self.sleep(30)
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv, fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()

        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def warmup_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        self._cb_cluster.warmup_node()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.sleep(30, "waiting for fts process to start")

        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def warmup_master_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        self._cb_cluster.warmup_node(master=True)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)

        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def node_reboot_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        #self._cb_cluster.reboot_one_node(test_case=self)
        self.kill_fts_service(120)
        self.sleep(5)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.validate_index_count(equal_bucket_doc_count=True)

        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def memc_crash_between_indexing_and_querying(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        NodeHelper.kill_memcached(self._cb_cluster.get_random_fts_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)

        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def erl_crash_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        NodeHelper.kill_erlang(self._cb_cluster.get_random_fts_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def fts_node_crash_between_indexing_and_querying(self):
        #TESTED
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        NodeHelper.kill_cbft_process(self._cb_cluster.get_random_fts_node())
        self.sleep(60)

        for index in self._cb_cluster.get_indexes():
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
            self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
    """ Topology change during querying"""

    def create_index_generate_queries(self, wait_idx=True):
        collection_index, _type, index_scope, index_collections =  self.define_index_parameters_collection_related()
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index",
            collection_index=collection_index,
            _type=_type,
            scope=index_scope,
            collections=index_collections
        )
        self.load_data()
        if wait_idx:
            self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.generate_random_queries(index, self.num_queries, self.query_types)
        return index

    def run_tasks_and_report(self, tasks, num_queries):
        fail_count = 0
        failed_queries = []
        for task in tasks:
            task.result()
            if hasattr(task, 'passed'):
                if not task.passed:
                    fail_count += 1
                    failed_queries.append(task.query_index+1)

        if fail_count:
            self.fail("%s out of %s queries failed! - %s" % (fail_count,
                                                             num_queries,
                                                             failed_queries))
        else:
            self.log.info("SUCCESS: %s out of %s queries passed"
                          %(num_queries-fail_count, num_queries))

    def rebalance_in_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        tasks = []
        tasks.append(self._cb_cluster.async_rebalance_in(
            num_nodes=self.num_rebalance,
            services=services))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.is_index_partitioned_balanced(index)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
        self.log.info("SUCCESS! Hits: %s" % hits)

    def retry_rebalance_in_during_querying(self):
        #TESTED
        self._cb_cluster.enable_retry_rebalance(self.retry_time, self.num_retries)
        index = self.create_index_generate_queries()
        services = []

        node_to_rebalance_in = []
        for s in self._input.servers:
            node_in_use = False
            for s1 in self._cb_cluster.get_nodes():
                if s.ip == s1.ip:
                    node_in_use = True
                    break
            if not node_in_use:
                node_to_rebalance_in.append(s)
            if len(node_to_rebalance_in) == self.num_rebalance:
                break
        if len(node_to_rebalance_in) < self.num_rebalance:
            self.fail("Cannot find free node to rebalance-in.")

        for _ in range(self.num_rebalance):
            services.append("fts")
        tasks = []

        stop_CB_thread = Thread(
            target=self.stop_cb_service,
            args=[node_to_rebalance_in, 45])

        stop_CB_thread.start()
        self.retry_rebalance(services=services, nodes_in=node_to_rebalance_in, sleep_before_rebalance=150)

        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.sleep(30)
        self.is_index_partitioned_balanced(index)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        hits, _, _, _ = index.execute_query(query=self.query,
                                            expected_hits=self._find_expected_indexed_items_number())
        self.log.info("SUCCESS! Hits: %s" % hits)
        self._cb_cluster.disable_retry_rebalance()

    def rebalance_out_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        #self.run_query_and_compare(index)
        tasks = []
        tasks.append(self._cb_cluster.async_rebalance_out(
            num_nodes=self.num_rebalance))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.is_index_partitioned_balanced(index)
        self.run_query_and_compare(index)
        hits, _, _, _ = index.execute_query(query=self.query,
                                         expected_hits=self._find_expected_indexed_items_number())
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def retry_rebalance(self, services=None, nodes_in=[], sleep_before_rebalance=0):
        rebalance_was_successful = True
        try:
            if services:
                self._cb_cluster.rebalance_in_node(
                    nodes_in = nodes_in,
                    services=services,
                    sleep_before_rebalance=sleep_before_rebalance
                )
            else:
                self._cb_cluster.rebalance_out()
            rebalance_was_successful = True
        except Exception as e:
            rebalance_was_successful = False
            self.log.error(str(e))
            self.start_cb_service(nodes_in)
            self.sleep(5)
            self._check_retry_rebalance_succeeded()
        finally:
            self.start_cb_service(nodes_in)
            if rebalance_was_successful:
                frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
                err = self.validate_partition_distribution(frest)
                if len(err) > 0:
                    self.fail(err)
                self.fail("Rebalance was successful")

    def retry_rebalance_out_during_querying(self):
        #TESTED
        self._cb_cluster.enable_retry_rebalance(self.retry_time, self.num_retries)
        index = self.create_index_generate_queries()
        #self.run_query_and_compare(index)
        tasks = []
        retry_reb_thread = Thread(
            target=self.retry_rebalance,
            args=[])
        retry_reb_thread.start()
        #reb_thread = Thread(
        #    target=self._cb_cluster.reboot_after_timeout,
        #    args=[2])
        reb_thread = Thread(
            target=self.kill_fts_service,
            args=[2])
        reb_thread.start()
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.is_index_partitioned_balanced(index)
        self.run_query_and_compare(index)
        hits, _, _, _ = index.execute_query(query=self.query,
                                            expected_hits=self._find_expected_indexed_items_number())
        self.log.info("SUCCESS! Hits: %s" % hits)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self._cb_cluster.disable_retry_rebalance()


    def swap_rebalance_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        services = []
        tasks = []
        rest = RestConnection(self._cb_cluster.get_master_node())
        for _ in range(self.num_rebalance):
            if rest.is_enterprise_edition():
                services.append("fts")
            else:
                services.append("fts,kv,index,n1ql")
        reb_thread = Thread(
            target=self._cb_cluster.async_swap_rebalance,
            args=[self.num_rebalance, services])
        reb_thread.start()
        # wait for a bit before querying
        self.sleep(90)
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.sleep(5)
        self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.sleep(30, "Sleep for additional time to make sure, fts index is ready.")
        hits, _, _, _ = index.execute_query(query=self.query,
                                         expected_hits=self._find_expected_indexed_items_number())
        self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_no_rebalance_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        tasks = []
        tasks.append(self._cb_cluster.async_failover(
            num_nodes=1,
            graceful=False))
        self.sleep(60)
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.sleep(60, "Sleep for additional time to make sure, fts index is ready")
        hits, _, _, _ = index.execute_query(query=self.query)
        if hits < self._find_expected_indexed_items_number():
            self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_rebalance_out_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        tasks = []
        tasks.append(self._cb_cluster.async_failover_and_rebalance(
            num_nodes=1,
            graceful=False))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.sleep(30, "Sleep for additional time to make sure, fts index is ready.")
        hits, _, _, _ = index.execute_query(query=self.query,
                                         expected_hits=self._find_expected_indexed_items_number())
        self.log.info("SUCCESS! Hits: %s" % hits)

    def failover_and_addback_during_querying(self):
        #TESTED
        recovery = self._input.param("recovery", None)
        graceful = self._input.param("graceful", False)
        index = self.create_index_generate_queries()
        if graceful:
            services = ['kv,fts']
        else:
            services = ['fts']
        tasks = []
        tasks.append(self._cb_cluster.async_failover_add_back_node(
            num_nodes=1,
            graceful=graceful,
            recovery_type=recovery,
            services=services))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        hits, _, _, _ = index.execute_query(query=self.query,
                                         expected_hits=self._find_expected_indexed_items_number())
        self.log.info("SUCCESS! Hits: %s" % hits)

    def graceful_failover_during_querying(self):
        index = self.create_index_generate_queries()
        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        tasks = []
        tasks.append(self._cb_cluster.__async_failover_and_rebalance(
            graceful=False))
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        self.is_index_partitioned_balanced(index)
        hits, _, _, _ = index.execute_query(query=self.query,
                                         expected_hits=self._find_expected_indexed_items_number())
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.log.info("SUCCESS! Hits: %s" % hits)

    def fts_node_down_with_replicas_during_querying(self):
        index = self.create_index_generate_queries()
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.stop_couchbase(node)
        try:
            hits, _, _, _ = index.execute_query(query=self.query,
                                             expected_hits=self._find_expected_indexed_items_number())
        except Exception as e:
            self.log.info("Expected exception : %s" % e)
        NodeHelper.start_couchbase(node)
        NodeHelper.wait_warmup_completed([node])
        self.run_query_and_compare(index)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def warmup_master_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        self._cb_cluster.warmup_node(master=True)
        self.is_index_partitioned_balanced(index)
        self.run_query_and_compare(index)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def node_reboot_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        #node = self._cb_cluster.reboot_one_node(test_case=self)
        node = self.kill_fts_service(120)
        self._cb_cluster.set_bypass_fts_node(node)
        self.is_index_partitioned_balanced(index)
        self.run_query_and_compare(index)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def memc_crash_during_indexing_and_querying(self):
        self.load_data()
        index = self.create_index(
            bucket=self._cb_cluster.get_bucket_by_name('default'),
            index_name="default_index")
        self.generate_random_queries(index, self.num_queries, self.query_types)
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_memcached(node)
        self._cb_cluster.set_bypass_fts_node(node)
        self.run_query_and_compare(index)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def erl_crash_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_erlang(node)
        self.is_index_partitioned_balanced(index)
        self.run_query_and_compare(index)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def fts_crash_during_querying(self):
        #TESTED
        index = self.create_index_generate_queries()
        self.run_query_and_compare(index)
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_cbft_process(node)
        self._cb_cluster.set_bypass_fts_node(node)
        self.run_query_and_compare(index)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def fts_diskfull_scenario(self):
        #TESTED
        index = self.create_index_generate_queries()
        node = self._cb_cluster.get_random_fts_node()
        shell = RemoteMachineShellConnection(node)
        process_id = shell.get_process_id("cbft")
        self.log.debug("Pid of '%s'=%s" % ("cbft", process_id))
        shell.fill_disk_space(self.index_path)
        NodeHelper.kill_cbft_process(node)
        process_id = None
        for i in range(100):
            if not process_id:
                try:
                    process_id = shell.get_process_id("cbft")
                except Exception as e:
                    self.log.info(str(e))
        if process_id:
            global_vars.system_event_logs.add_event(SearchServiceEvents.fts_crash(node.ip, process_id))
        else:
            self.log.info("Verifying without process id")
            global_vars.system_event_logs.add_event(SearchServiceEvents.fts_crash_no_processid(node.ip))
        shell._recover_disk_full_failure(self.index_path)
        self.sleep(2)
        self.run_query_and_compare(index)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def update_index_during_rebalance(self):
        """
         Perform indexing + rebalance + index defn change in parallel
        """
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        # wait till indexing is midway...
        self.wait_for_indexing_complete(self._find_expected_indexed_items_number()//2)
        reb_thread = Thread(target=self._cb_cluster.rebalance_out_master,
                                   name="rebalance",
                                   args=())
        reb_thread.start()
        self.sleep(15)
        index = self._cb_cluster.get_indexes()[0]
        new_plan_param = {"maxPartitionsPerPIndex": 64}
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        update_index_thread = Thread(target=index.update,
                                   name="update_index",
                                   args=())
        update_index_thread.start()
        _, defn = index.get_index_defn()
        self.log.info(defn['indexDef'])
        reb_thread.join()
        update_index_thread.join()
        hits, _, _, _ = index.execute_query(self.query,
                                            zero_results_ok=True)
        self.log.info("Hits: %s" % hits)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.validate_index_count(equal_bucket_doc_count=True)
        hits, _, _, _ = index.execute_query(self.query,
                                            zero_results_ok=False)
        self.log.info("Hits: %s" % hits)

    def delete_index_during_rebalance(self):
        """
         Perform indexing + rebalance + index delete in parallel
        """
        import copy
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        indexes = copy.copy(self._cb_cluster.get_indexes())
        for index in indexes:
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        # wait till indexing is midway...
        self.wait_for_indexing_complete(self._find_expected_indexed_items_number()//2)
        reb_thread = Thread(target=self._cb_cluster.rebalance_out,
                                   name="rebalance",
                                   args=())
        reb_thread.start()
        # the first part of the rebalance is kv, wait for fts rebalance
        self.sleep(50)

        for index in indexes:
            index.delete()

        self.sleep(5)

        for index in indexes:
            try:
                _, defn = index.get_index_defn()
                self.log.info(defn['indexDef'])
            except KeyError as e:
                self.log.info("Expected exception: {0}".format(e))
                deleted = self._cb_cluster.are_index_files_deleted_from_disk(index.name)
                if deleted:
                    self.log.info("Confirmed: index files deleted from disk")
                else:
                    self.fail("ERROR: Index files still present on disk")
            else:
                self.fail("ERROR: Index definition still exists after deletion! "
                          "%s" %defn['indexDef'])


    def delete_buckets_during_rebalance(self):
        """
            Perform indexing + rebalance + bucket delete in parallel
        """
        from lib.membase.api.rest_client import RestConnection
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        # wait till indexing is midway...
        self.wait_for_indexing_complete(self._find_expected_indexed_items_number() // 2)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        reb_thread = Thread(target=self._cb_cluster.rebalance_out_master,
                            name="rebalance",
                            args=())
        reb_thread.start()
        self.sleep(10)
        for bucket in self._cb_cluster.get_buckets():
            self.log.info("Deleting bucket {0}".format(bucket.name))
            if not RestConnection(self._cb_cluster.get_master_node()).delete_bucket(bucket.name):
                self.log.info("Expected error - cannot delete buckets during rebalance!")
            else:
                self.fail("Able to delete buckets during rebalance!")


    def update_index_during_failover(self):
        """
         Perform indexing + failover + index defn change in parallel
         for D,D+F,F cluster
        """
        self.load_data()
        plan_params = None
        index_partitions = TestInputSingleton.input.param("index_partitions", 0)
        if index_partitions > 0:
            plan_params = {"indexPartitions": index_partitions}

        self.create_fts_indexes_all_buckets(plan_params=plan_params)
        self.sleep(10)
        self.log.info("Index building has begun...")
        idx_name = None
        for index in self._cb_cluster.get_indexes():
            if not idx_name:
                idx_name = index.name
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        # wait till indexing is midway...
        self.wait_for_indexing_complete(self._find_expected_indexed_items_number()//2)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        fail_thread = Thread(target=self._cb_cluster.failover,
                                   name="failover",
                                   args=())
        fail_thread.start()
        index = self._cb_cluster.get_fts_index_by_name(idx_name)
        new_plan_param = {"maxPartitionsPerPIndex": 64}
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['planParams']['indexPartitions'] = 15
        index.index_definition['uuid'] = index.get_uuid()
        update_index_thread = Thread(target=index.update,
                                   name="update_index",
                                   args=())
        update_index_thread.start()
        _, defn = index.get_index_defn()
        self.log.info(defn['indexDef'])
        fail_thread.join()
        update_index_thread.join()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        hits, _, _, _ = index.execute_query(self.query)
        self.log.info("Hits: %s" % hits)
        #for index in self._cb_cluster.get_indexes():
        #    self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        hits, _, _, _ = index.execute_query(
            self.query,
            expected_hits=index.get_src_bucket_doc_count())
        self.log.info("Hits: %s" % hits)

    def update_index_during_failover_and_rebalance(self):
        """
         Perform indexing + failover + index defn change in parallel
        """
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        idx_name = None
        for index in self._cb_cluster.get_indexes():
            if not idx_name:
                idx_name = index.name
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        # wait till indexing is midway...
        self.wait_for_indexing_complete(self._find_expected_indexed_items_number()//2)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        fail_thread = Thread(
            target=self._cb_cluster.failover_and_rebalance_nodes,
            name="failover",
            args=())

        index = self._cb_cluster.get_fts_index_by_name(idx_name)
        new_plan_param = {"maxPartitionsPerPIndex": 128}
        index.index_definition['planParams'] = \
            index.build_custom_plan_params(new_plan_param)
        index.index_definition['uuid'] = index.get_uuid()
        update_index_thread = Thread(target=index.update,
                                   name="update_index",
                                   args=())
        fail_thread.start()
        update_index_thread.start()
        _, defn = index.get_index_defn()
        self.log.info(defn['indexDef'])
        fail_thread.join()
        update_index_thread.join()
        hits, _, _, _ = index.execute_query(self.query)
        self.log.info("Hits: %s" % hits)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        self.validate_index_count(equal_bucket_doc_count=True)
        hits, _, _, _ = index.execute_query(
            self.query,
            expected_hits=index.get_src_bucket_doc_count())
        self.log.info("Hits: %s" % hits)


    def partial_rollback(self):
        bucket = self._cb_cluster.get_bucket_by_name("default")

        self._cb_cluster.flush_buckets([bucket])
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
        index = self.create_index(bucket, "default_index", collection_index=collection_index, _type=_type,
                                  scope=index_scope, collections=index_collections)
        self.load_data()
        self.wait_for_indexing_complete()

        # Stop Persistence on Node A & Node B
        self.log.info("Stopping persistence on {0}".
                      format(self._input.servers[:2]))
        mem_client = MemcachedClientHelper.direct_client(self._input.servers[0],
                                                         bucket)
        mem_client.stop_persistence()
        mem_client = MemcachedClientHelper.direct_client(self._input.servers[1],
                                                         bucket)
        mem_client.stop_persistence()
        self.sleep(20)

        # Perform mutations on the bucket
        self.async_perform_update_delete(self.upd_del_fields)
        if self._update:
            self.sleep(60, "Waiting for updates to get indexed...")
        self.wait_for_indexing_complete()

        # Run FTS Query to fetch the initial count of mutated items
        query = "{\"query\": \"mutated:>0\"}"
        query = json.loads(query)
        for index in self._cb_cluster.get_indexes():
            hits1, _, _, _ = index.execute_query(query)
            self.log.info("Hits before rollback: %s" % hits1)

        # Fetch count of docs in index and bucket
        before_index_doc_count = index.get_indexed_doc_count()
        before_bucket_doc_count = index.get_src_bucket_doc_count()

        self.log.info("Docs in Bucket : %s, Docs in Index : %s" % (
            before_bucket_doc_count, before_index_doc_count))

        # Kill memcached on Node A
        self.log.info("Killing memcached on {0}".format(self._master.ip))
        shell = RemoteMachineShellConnection(self._master)
        shell.kill_memcached()
        self.sleep(20)

        # Start persistence on Node B
        self.log.info("Starting persistence on {0}".
                      format(self._input.servers[1].ip))
        mem_client = MemcachedClientHelper.direct_client(self._input.servers[1],
                                                         bucket)
        mem_client.start_persistence()
        self.sleep(20)

        # Failover Node B
        failover_task = self._cb_cluster.async_failover(
            node=self._input.servers[1])
        failover_task.result()

        # Wait for Failover & FTS index rollback to complete
        self.wait_for_indexing_complete()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        # Run FTS query to fetch count of mutated items post rollback.
        for index in self._cb_cluster.get_indexes():
            hits2, _, _, _ = index.execute_query(query)
            self.log.info("Hits after rollback: %s" % hits2)
        # Fetch count of docs in index and bucket
        after_index_doc_count = index.get_indexed_doc_count()
        after_bucket_doc_count = index.get_src_bucket_doc_count()

        self.log.info("Docs in Bucket : %s, Docs in Index : %s"
                      % (after_bucket_doc_count, after_index_doc_count))

        # Validation : If there are deletes, validate the #docs in index goes
        #  up post rollback
        if self._input.param("delete", False):
            self.assertGreater(after_index_doc_count, before_index_doc_count,
                               "Deletes : Index count after rollback not "
                               "greater than before rollback")
        else:
            # For Updates, validate that #hits goes down in the query output
            # post rollback
            self.assertGreater(hits1, hits2,
                               "Mutated items before rollback are not more "
                               "than after rollback")

        # Failover FTS node
        failover_fts_node = self._input.param("failover_fts_node", False)

        if failover_fts_node:
            failover_task = self._cb_cluster.async_failover(
                node=self._input.servers[2])
            failover_task.result()
            self.sleep(10)
            frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
            err = self.validate_partition_distribution(frest)
            if len(err) > 0:
                self.fail(err)
            # Run FTS query to fetch count of mutated items post FTS node failover.
            for index in self._cb_cluster.get_indexes():
                hits3, _, _, _ = index.execute_query(query)
                self.log.info(
                    "Hits after rollback and failover of primary FTS node: %s" % hits3)
                self.assertEqual(hits2, hits3,
                                 "Mutated items after FTS node failover are not equal to that after rollback")


    def test_cancel_node_removal_rebalance(self):
        """
            1. start rebalance out
            2. stop rebalance
            3. cancel node removal
            4. start rebalance
        """
        from lib.membase.api.rest_client import RestConnection, RestHelper
        rest = RestConnection(self._cb_cluster.get_master_node())
        nodes = rest.node_statuses()
        ejected_nodes = []

        for node in nodes:
            if node.ip != self._master.ip:
                ejected_nodes.append(node.id)
                break

        self.log.info(
                "removing node {0} from cluster".format(ejected_nodes))
        rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=ejected_nodes)
        self.sleep(3)
        stopped = rest.stop_rebalance()
        self.assertTrue(stopped, msg="unable to stop rebalance")

        #for cases if rebalance ran fast
        if RestHelper(rest).is_cluster_rebalanced():
            self.log.info("Rebalance is finished already.")
        else:
            rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(), msg="rebalance operation "
                                                     "failed after restarting")

        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def test_stop_restart_rebalance_in_loop(self):
        """
         Kick off rebalance-out. Stop and stop rebalance in a loop
         continuously till rebalance finishes.
        :return:
        """
        count = 0
        self.create_fts_indexes_all_buckets()
        index = self.create_index_generate_queries()
        self.sleep(10)
        self.wait_for_indexing_complete()

        from lib.membase.api.rest_client import RestConnection, RestHelper
        rest = RestConnection(self._cb_cluster.get_master_node())
        nodes = rest.node_statuses()
        ejected_nodes = []

        for node in nodes:
            if node.ip != self._master.ip:
                ejected_nodes.append(node.id)
                break
        self.log.info(
            "removing node(s) {0} from cluster".format(ejected_nodes))

        while count<5:
            rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=ejected_nodes)
            self.sleep(5)
            stopped = rest.stop_rebalance()

            self.assertTrue(stopped, msg="unable to stop rebalance")
            count += 1

        rest.rebalance(otpNodes=[node.id for node in nodes],
                       ejectedNodes=ejected_nodes)
        self.assertTrue(rest.monitorRebalance(), msg="rebalance operation "
                                                     "failed after restarting")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        tasks = []
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=None,
                query_index=count))
        self.run_tasks_and_report(tasks, len(index.fts_queries))
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)
        if not self.disable_file_transfer_rebalance and not index.is_upside_down():
            file_copy_transfer_found, file_transfer_success, failed_file_transfer = \
                self.validate_file_copy_rebalance_stats()
            if not file_copy_transfer_found:
                self.log.info("no file based transfer found during rebalance")
            if not file_transfer_success:
                self.fail(f'Found file transfer failed for these partitions: {failed_file_transfer}')

    def test_rebalance_cancel_new_rebalance(self):
        """
            Load bucket, do not create index
            From a 3 kv+fts node cluster, rebalance out master + one node
            Immediately (after few secs), stop rebalance
            Rebalance out other nodes than master.
            After rebalance completes, create an index
        :return:
        """
        self.load_data()

        non_master_nodes = list(set(self._cb_cluster.get_nodes())-
                           {self._master})

        from lib.membase.api.rest_client import RestConnection, RestHelper
        rest = RestConnection(self._master)
        nodes = rest.node_statuses()
        ejected_nodes = []

        # first eject a master + non-master node
        eject_nodes = [self._master] + [non_master_nodes[0]]

        for eject in eject_nodes:
            for node in nodes:
                if eject.ip == node.ip:
                    ejected_nodes.append(node.id)
                    break

        rest.rebalance(otpNodes=[node.id for node in nodes],
                       ejectedNodes=ejected_nodes)
        self.sleep(3)
        rest._rebalance_progress()
        stopped = rest.stop_rebalance()
        self.assertTrue(stopped, msg="unable to stop rebalance")

        eject_nodes = non_master_nodes[:2]
        ejected_nodes = []

        for eject in eject_nodes:
            for node in nodes:
                if eject.ip == node.ip:
                    ejected_nodes.append(node.id)
                    break

        rest.rebalance(otpNodes=[node.id for node in nodes],
                       ejectedNodes=ejected_nodes)
        rest.monitorRebalance()

        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

    def test_kv_and_fts_rebalance_with_high_ops(self):
        from lib.membase.api.rest_client import RestConnection
        rest = RestConnection(self._cb_cluster.get_master_node())

        # start loading
        default_bucket = self._cb_cluster.get_bucket_by_name("default")
        load_thread = Thread(target=self._cb_cluster.load_from_high_ops_loader,
                                       name="gen_high_ops_load",
                                       args=[default_bucket])
        load_thread.start()

        # create index and query
        index = self.create_index(
            self._cb_cluster.get_bucket_by_name('default'),
            "default_index")
        load_thread.join()
        self.wait_for_indexing_complete()
        
        # check for dataloss
        errors = self._cb_cluster.check_dataloss_with_high_ops_loader(default_bucket)
        if errors:
            self.log.info("Printing missing keys:")
            for error in errors:
                self.log.info(error)

        if self._num_items != rest.get_active_key_count(default_bucket):
            self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                      format(self._num_items, rest.get_active_key_count(default_bucket)))
        else:
            self.log.info("SUCCESS: No traces of data loss upon verification")

        # load again
        load_thread = Thread(target=self._cb_cluster.load_from_high_ops_loader,
                                       name="gen_high_ops_load",
                                       args=[default_bucket])
        load_thread.start()


        # do a rebalance-out of kv+fts node
        self._cb_cluster.rebalance_out_master()
        load_thread.join()

        # check for dataloss
        errors = self._cb_cluster.check_dataloss_with_high_ops_loader(default_bucket)
        if errors:
            self.log.info("Printing missing keys:")
            for error in errors:
                self.log.info(error)
        if self._num_items != rest.get_active_key_count(default_bucket):
            self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                      format(self._num_items, rest.get_active_key_count(default_bucket)))
        else:
            self.log.info("SUCCESS: No traces of data loss upon verification")
            frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
            err = self.validate_partition_distribution(frest)
            if len(err) > 0:
                self.fail(err)

        # load again
        load_thread = Thread(target=self._cb_cluster.load_from_high_ops_loader,
                                       name="gen_high_ops_load",
                                       args=[default_bucket])
        load_thread.start()

        # do a rebalance-out of fts node
        self._cb_cluster.rebalance_out()
        load_thread.join()

        # check for dataloss
        errors = self._cb_cluster.check_dataloss_with_high_ops_loader(default_bucket)
        if errors:
            self.log.info("Printing missing keys:")
            for error in errors:
                self.log.info(error)
        if self._num_items != rest.get_active_key_count(default_bucket):
            self.fail("FATAL: Data loss detected!! Docs loaded : {0}, docs present: {1}".
                      format(self._num_items, rest.get_active_key_count(default_bucket)))
        else:
            self.log.info("SUCCESS: No traces of data loss upon verification")
            frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
            err = self.validate_partition_distribution(frest)
            if len(err) > 0:
                self.fail(err)

    def _find_expected_indexed_items_number(self):
        if self.container_type == 'bucket':
            return self._num_items
        else:
            if isinstance(self.collection, list):
                return self._num_items * len(self.collection)
            else:
                return self._num_items

    def test_cleanup_after_failover_rebalance_addback(self):
        self.load_data()
        self.create_fts_indexes_all_buckets()
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))

        # failover the fts node on which index recides
        nodeIds = []
        rest = RestConnection(self._master)

        index = self._cb_cluster.get_indexes()[0]
        _, indexdef = rest.get_fts_index_definition(index.name, index._source_name, index.scope)
        _, cfg = rest.get_cfg_stats()

        for plans in indexdef['planPIndexes']:
            if "nodes" in plans:
                for nodeId, val in plans["nodes"].items():
                    nodeIds.append(nodeId)
        hostnames = []
        for nodeId in nodeIds:
            hostnames.append(cfg['nodeDefsWanted']['nodeDefs'][nodeId]['hostPort'][:-5])

        node_obj = None
        for node in self._cb_cluster.get_nodes():
            if node.ip == hostnames[0]:
                node_obj = node
        self._cb_cluster.failover(node=node_obj)
        time.sleep(30)
        self._cb_cluster.rebalance_failover_nodes()
        frest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(frest)
        if len(err) > 0:
            self.fail(err)

        self._cb_cluster.delete_bucket(bucket_name="default")

        try:
            _, indexdef = rest.get_fts_index_definition(index.name, index._source_name, index.scope)
            self.fail("Index partitions still reside on the node - node not cleaned up properly")
        except Exception as e:
            self.log.info(f"Success - : {str(e)}")

    def partition_validation_sanity(self):
        partitions = [18, 3, 3]
        self.create_fts_indexes_all_buckets()
        for count, index in enumerate(self._cb_cluster.get_indexes()):
            index.update_index_partitions(partitions[count])
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        err = self.validate_partition_distribution(rest)
        if len(err) > 0:
            self.fail(err)

    def partition_validate_failover_and_rebalance_in(self):
        partitions = [18, 3, 3]
        self.create_fts_indexes_all_buckets()
        for count, index in enumerate(self._cb_cluster.get_indexes()):
            index.update_index_partitions(partitions[count])
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))

        # get all fts node on which index recides
        nodeIds = []
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        index = self._cb_cluster.get_indexes()[0]
        _, indexdef = rest.get_fts_index_definition(index.name, index._source_name, index.scope)
        _, cfg = rest.get_cfg_stats()

        for plans in indexdef['planPIndexes']:
            if "nodes" in plans:
                for nodeId, val in plans["nodes"].items():
                    nodeIds.append(nodeId)
        hostnames = []
        for nodeId in nodeIds:
            hostnames.append(cfg['nodeDefsWanted']['nodeDefs'][nodeId]['hostPort'][:-5])

        # failover all nodes 1 by 1 and validate index parition count each time
        for i in range(len(hostnames)):
            node_obj = None
            for node in self._cb_cluster.get_nodes():
                if node.ip == hostnames[i]:
                    node_obj = node

            err = self.validate_partition_distribution(rest)
            if len(err) > 0:
                self.fail(err)

            self._cb_cluster.failover(node=node_obj)

            self._cb_cluster.rebalance_in(num_nodes=1, services=["fts"])

            err = self.validate_partition_distribution(rest)
            if len(err) > 0:
                self.fail(err)

    def partition_validate_failover_remove_and_rebalance_in_same_node(self):
        partitions = [18, 3, 3]
        self.create_fts_indexes_all_buckets()
        for count, index in enumerate(self._cb_cluster.get_indexes()):
            index.update_index_partitions(partitions[count])
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))

        # fts node on which index recides
        nodeIds = []
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        index = self._cb_cluster.get_indexes()[0]
        _, indexdef = rest.get_fts_index_definition(index.name, index._source_name, index.scope)
        _, cfg = rest.get_cfg_stats()
        for plans in indexdef['planPIndexes']:
            if "nodes" in plans:
                for nodeId, val in plans["nodes"].items():
                    nodeIds.append(nodeId)
        hostnames = []
        for nodeId in nodeIds:
            hostnames.append(cfg['nodeDefsWanted']['nodeDefs'][nodeId]['hostPort'][:-5])

        # failover all nodes 1 by 1 and validate index parition count each time
        for i in range(len(hostnames)):
            node_obj = None
            node_obj2 = None
            for node in self._cb_cluster.get_nodes():
                if node.ip == hostnames[i]:
                    node_obj = node
                else:
                    node_obj2 = node

            err = self.validate_partition_distribution(rest)
            if len(err) > 0:
                self.fail(err)

            self._cb_cluster.failover(node=node_obj)

            self._cb_cluster.add_back_specific_node(node=node_obj, master_node=node_obj2)
            err = self.validate_partition_distribution(rest)
            if len(err) > 0:
                self.fail(err)

    def partition_validate_failover_remove_cancel_rebalance_in_same_node(self):
        partitions = [18, 3, 3]
        self.create_fts_indexes_all_buckets()
        for count, index in enumerate(self._cb_cluster.get_indexes()):
            index.update_index_partitions(partitions[count])
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))

        # fts node on which index recides
        nodeIds = []
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        index = self._cb_cluster.get_indexes()[0]
        _, indexdef = rest.get_fts_index_definition(index.name, index._source_name, index.scope)
        _, cfg = rest.get_cfg_stats()
        for plans in indexdef['planPIndexes']:
            if "nodes" in plans:
                for nodeId, val in plans["nodes"].items():
                    nodeIds.append(nodeId)
        hostnames = []
        for nodeId in nodeIds:
            hostnames.append(cfg['nodeDefsWanted']['nodeDefs'][nodeId]['hostPort'][:-5])

        # failover all nodes 1 by 1 and validate index parition count each time
        for i in range(len(hostnames)):
            node_obj = None
            node_obj2 = None
            for node in self._cb_cluster.get_nodes():
                if node.ip == hostnames[i]:
                    node_obj = node
                else:
                    node_obj2 = node

            err = self.validate_partition_distribution(rest)
            if len(err) > 0:
                self.fail(err)

            self._cb_cluster.failover(node=node_obj)

            self._cb_cluster.add_back_specific_node(node=node_obj, master_node=node_obj2, rebalance=False)

            thread = threading.Thread(target=self._cb_cluster.rebalance,
                                      kwargs={'servers': self._cb_cluster.get_nodes(), 'to_add': [], 'to_remove': [],
                                              'services': None})
            thread.start()
            self._cb_cluster.__stop_rebalance()
            self._cb_cluster.add_back_specific_node(node=node_obj, master_node=node_obj2, rebalance=True)

            err = self.validate_partition_distribution(rest)
            if len(err) > 0:
                self.fail(err)
