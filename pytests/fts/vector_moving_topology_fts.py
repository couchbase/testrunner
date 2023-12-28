import random

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


class VectorSearchMovingTopFTS(FTSBaseTest):

    def setUp(self):
        self.num_vect_buckets = TestInputSingleton.input.param("num_vector_buckets", 1)
        self.index_per_vect_bucket = TestInputSingleton.input.param("index_per_vector_bucket", 1)
        self.vector_dataset = TestInputSingleton.input.param("vector_dataset", ["siftsmall"])
        self.dimension = TestInputSingleton.input.param("dimension", 128)
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
        super(VectorSearchMovingTopFTS, self).setUp()
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_disableFileTransferRebalance(self.disable_file_transfer_rebalance)
        if not self.capella_run:
            self.index_path = rest.get_index_path()
            if self.index_path == "/data":
                self.reset_data_mount_point(self._cb_cluster.get_fts_nodes())
        self.type_of_load = TestInputSingleton.input.param("type_of_load", "separate")

    def tearDown(self):
        super(VectorSearchMovingTopFTS, self).tearDown()
        if not self.capella_run and self.index_path == "/data":
            try:
                self.reset_data_mount_point(self._cb_cluster.get_fts_nodes())
            except Exception as err:
                self.log.info(str(err))

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

    def create_vect_bucket_containers(self, num_buckets):

        containers = []
        for i in range(num_buckets):
            bucket_name = f"vector-bucket-{i}"
            scope = "s1"
            collection = "c1"
            bucket_name_space = bucket_name + "." + scope + "." + collection
            containers.append(bucket_name_space)

        return containers

    def create_vect_index_containers(self, bucket_container, num_index):

        index_container = []
        for b in bucket_container:
            decoded_container = self._cb_cluster._decode_containers([b])
            bucket_name = decoded_container["buckets"][0]["name"]
            for i in range(num_index):
                index_name = bucket_name + "_index_" + str(i)
                index_bucket_tuple = (index_name, b)
                index_container.append(index_bucket_tuple)

        return index_container

    def setup_seperate_load_for_test(self):
        """
            Generates seperate buckets for vector data(SIFT, GIST etc) and normal dataset(emp, Person etc)
        """
        self.load_data()

        vect_bucket_containers = self.create_vect_bucket_containers(self.num_vect_buckets)
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client,
                                                              containers=vect_bucket_containers)

        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        vect_index_container = self.create_vect_index_containers(vect_bucket_containers,
                                                                 self.index_per_vect_bucket)

        exempt_buckets = [bucket["name"] for bucket in containers["buckets"]]
        # create normal fts indexes
        self.create_fts_indexes_some_buckets(exempt_bucket=exempt_buckets)

        # create vector indexes
        similarity = random.choice(["l2_norm", "dot_product"])
        vector_fields = {"dims": self.dimension, "similarity": similarity}

        index = self._create_fts_index_parameterized(field_name="vector_data", field_type="vector",
                                                     test_indexes=vect_index_container,
                                                     vector_fields=vector_fields,
                                                     create_vector_index=True,
                                                     wait_for_index_complete=False)

    def setup_common_load_for_test(self):
        """
            All documents have vector field in them, there are no seperate buckets containing only vector datasets
        """
        self.load_data()
        # create 5 indexes in bucket 1 and bucket 2, bucket 1 will only have normal indexes
        self.create_fts_indexes_some_buckets(exempt_bucket=["standard_3"])

        from sentence_transformers import SentenceTransformer
        encoder = SentenceTransformer(self.llm_model)
        dimension = encoder.get_sentence_embedding_dimension()

        # create 5 more vector indexes in bucket 2 -> this now contains 5 normal and 5 vector indexes
        for i in range(5):
            similarity = random.choice(["l2_norm", "dot_product"])
            idx = [(f"i{i}", "standard_bucket_2._default._default")]
            vector_fields = {"dims": dimension, "similarity": similarity}
            self._create_fts_index_parameterized(field_name="l_vector", field_type="vector", test_indexes=idx,
                                                 vector_fields=vector_fields,
                                                 create_vector_index=True)
        # create 5 indexes in bucket 3 -> this now contains 5 vector indexes only
        for i in range(5):
            similarity = random.choice(["l2_norm", "dot_product"])
            idx = [(f"i{i}", "standard_bucket_3._default._default")]
            vector_fields = {"dims": dimension, "similarity": similarity}
            self._create_fts_index_parameterized(field_name="l_vector", field_type="vector", test_indexes=idx,
                                                 vector_fields=vector_fields,
                                                 create_vector_index=True)

    def start_data_loading(self):
        if self.type_of_load == "common":
            if not self.vector_search:
                self.fail("Pass `vector_search=True` in order to have vector fields within the documents")
            self.setup_common_load_for_test()
        else:
            self.setup_seperate_load_for_test()

    def rebalance_in_during_index_building(self):
        if self._input.param("must_fail", False):
            self.fail("Temporal fail to let all the other tests to be passed")

        self.start_data_loading()

        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_in(num_nodes=1, services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def rebalance_out_during_index_building(self):
        self.start_data_loading()

        self.sleep(20)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_out()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def rebalance_out_master_during_index_building(self):
        self.start_data_loading()

        self.sleep(5)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_out_master()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def swap_rebalance_during_index_building(self):
        self.start_data_loading()

        self.sleep(5)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
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

    def swap_rebalance_kv_during_index_building(self):
        self.start_data_loading()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
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

    def failover_non_master_during_index_building(self):
        self.start_data_loading()

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

    def failover_no_rebalance_during_index_building(self):
        self.start_data_loading()

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

    def failover_master_during_index_building(self):
        self.start_data_loading()

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

    def failover_only_kv_during_index_building(self):
        self.start_data_loading()

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

    def graceful_failover_and_delta_recovery_during_index_building(self):
        self.start_data_loading()

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

    def graceful_failover_and_full_recovery_during_index_building(self):
        self.start_data_loading()

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

    def hard_failover_and_delta_recovery_during_index_building(self):
        self.start_data_loading()

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

    def hard_failover_and_full_recovery_during_index_building(self):
        self.start_data_loading()

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

    def warmup_during_index_building(self):
        self.start_data_loading()

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

    def warmup_master_during_index_building(self):
        self.start_data_loading()

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

    def node_reboot_during_index_building(self):
        self.start_data_loading()

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

    def node_reboot_only_kv_during_index_building(self):
        self.start_data_loading()

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

    def memc_crash_on_kv_during_index_building(self):
        self.start_data_loading()

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

    def fts_node_crash_during_index_building(self):
        self.start_data_loading()

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

    def erl_crash_on_kv_during_index_building(self):
        self.start_data_loading()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          %(index.name, index.get_indexed_doc_count()))
        bucket_names = []
        for bucket in self._cb_cluster.get_buckets():
            bucket_names.append(bucket.name)
        NodeHelper.kill_erlang(self._cb_cluster.get_master_node(), bucket_names)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def create_index_generate_queries(self, wait_idx=True):
        collection_index, _type, index_scope, index_collections = self.define_index_parameters_collection_related()
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