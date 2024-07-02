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
import threading


class VectorSearchMovingTopFTS(FTSBaseTest):

    def setUp(self):
        self.num_vect_buckets = TestInputSingleton.input.param("num_vector_buckets", 1)
        self.index_per_vect_bucket = TestInputSingleton.input.param("index_per_vector_bucket", 1)
        self.vector_dataset = TestInputSingleton.input.param("vector_dataset", "siftsmall")
        self.dimension = TestInputSingleton.input.param("dimension", 128)
        self.k = TestInputSingleton.input.param("k", 100)
        self.num_vector_queries = TestInputSingleton.input.param("num_vect_queries", 100)
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
        self.validate_memory_leak = TestInputSingleton.input.param("validate_memory_leak", False)
        if self.validate_memory_leak:
            self.memory_validator_thread = threading.Thread(target=self.start_memory_stat_collector_and_validator, kwargs={
                'fts_nodes': self._cb_cluster.get_fts_nodes()
            })
            self.memory_validator_thread.start()

        self.goloader_toggle = TestInputSingleton.input.param("goloader_toggle", False)
        self.log.info("Modifying quotas for each services in the cluster")
        try:
            RestConnection(self._cb_cluster.get_master_node()).modify_memory_quota(512, 400, 2000, 1024, 256)
        except Exception as e:
            print(e)

    def tearDown(self):
        if self.validate_memory_leak:
            self.stop_memory_collector_and_validator = True
            self.memory_validator_thread.join()
        super(VectorSearchMovingTopFTS, self).tearDown()
        if not self.capella_run and self.index_path == "/data":
            try:
                self.reset_data_mount_point(self._cb_cluster.get_fts_nodes())
            except Exception as err:
                self.log.info(str(err))

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

    def _find_expected_indexed_items_number(self):
        if self.container_type == 'bucket':
            return self._num_items
        else:
            if isinstance(self.collection, list):
                return self._num_items * len(self.collection)
            else:
                return self._num_items

    def wait_for_rebalance_to_start(self, timeout=1800):

        rest = RestConnection(self._cb_cluster.get_master_node())
        status, _ = rest._rebalance_status_and_progress()
        end_time = time.time() + timeout
        current_time = time.time()
        self.log.info("Waiting for rebalance to start")
        while status != 'running' and current_time < end_time:
            time.sleep(1)
            status, _ = rest._rebalance_status_and_progress()
            current_time = time.time()

        if current_time < end_time:
            self.log.info("Rebalance has started running.")
        else:
            self.fail("Rebalance failed to start")

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
                index_name = "vector_" + bucket_name + "_index_" + str(i)
                index_bucket_tuple = (index_name, b)
                index_container.append(index_bucket_tuple)

        return index_container

    def setup_seperate_load_for_test(self, wait_for_index_complete=False, generate_queries=False):
        """
            Generates seperate buckets for vector data(SIFT, GIST etc) and normal dataset(emp, Person etc)
        """

        # load data in standard buckets
        self.load_data()

        # create vector buckets
        vect_bucket_containers = self.create_vect_bucket_containers(self.num_vect_buckets)
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client,
                                                              containers=vect_bucket_containers)
        self.load_vector_data(containers, dataset=self.vector_dataset, goloader_toggle= self.goloader_toggle)
        vect_index_containers = self.create_vect_index_containers(vect_bucket_containers,
                                                                  self.index_per_vect_bucket)

        exempt_buckets = [bucket["name"] for bucket in containers["buckets"]]

        # create normal fts indexes in buckets except vector buckets
        self.create_fts_indexes_some_buckets(exempt_bucket=exempt_buckets)

        # create vector indexes in vector buckets
        similarity = random.choice(["l2_norm", "dot_product"])
        vector_fields = {"dims": self.dimension, "similarity": similarity}
        self._create_fts_index_parameterized(field_name="vector_data", field_type="vector",
                                             test_indexes=vect_index_containers,
                                             vector_fields=vector_fields,
                                             create_vector_index=True,
                                             wait_for_index_complete=wait_for_index_complete,
                                             extra_fields=[{"sno": "number"}])

        exempt_indexes = []
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                exempt_indexes.append(index.name)
        if generate_queries:
            self.generate_queries_some_indexes(exempt_index=exempt_indexes)

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

    def load_data_and_create_indexes(self, wait_for_index_complete=False, generate_queries=False):
        if self.type_of_load == "common":
            if not self.vector_search:
                self.fail("Pass `vector_search=True` in order to have vector fields within the documents")
            self.setup_common_load_for_test()
        else:
            self.setup_seperate_load_for_test(wait_for_index_complete=wait_for_index_complete,
                                              generate_queries=generate_queries)

    def rebalance_in_during_index_building(self):
        if self._input.param("must_fail", False):
            self.fail("Temporal fail to let all the other tests to be passed")

        self.load_data_and_create_indexes()

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
        self.load_data_and_create_indexes()

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
        self.load_data_and_create_indexes()

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
        self.load_data_and_create_indexes()

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
        self.load_data_and_create_indexes()

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
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
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
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        self._cb_cluster.async_failover().result()
        self.sleep(60)
        try:
            for index in self._cb_cluster.get_indexes():
                self.is_index_partitioned_balanced(index)
        except Exception as e:
            self.log.info("Expected exception: %s" % e)

    def failover_master_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        self._cb_cluster.rebalance_in(num_nodes=1, services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self._cb_cluster.failover_and_rebalance_master()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def failover_only_kv_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        try:
            self._cb_cluster.failover_and_rebalance_master()
        except Exception as e:
            self.log.info("Expected exception caught: %s" % e)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def graceful_failover_and_delta_recovery_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))

        failover_node = self._cb_cluster.get_nodes()[-1:]
        for node in self._cb_cluster.get_nodes():
            if node.ip == self._cb_cluster.get_master_node().ip:
                continue
            node_services = node.services.split(",")
            if "kv" in node_services and "fts" in node_services:
                failover_node = node
                break

        task = self._cb_cluster.async_failover(graceful=True, node=failover_node)
        task.result()
        self.sleep(60)
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def graceful_failover_and_full_recovery_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))

        failover_node = self._cb_cluster.get_nodes()[-1:]
        for node in self._cb_cluster.get_nodes():
            if node.ip == self._cb_cluster.get_master_node().ip:
                continue
            node_services = node.services.split(",")
            if "kv" in node_services and "fts" in node_services:
                failover_node = node
                break

        task = self._cb_cluster.async_failover(graceful=True, node=failover_node)
        task.result()
        self.sleep(60)
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv, fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def hard_failover_and_delta_recovery_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))

        failover_node = self._cb_cluster.get_nodes()[-1:]
        for node in self._cb_cluster.get_nodes():
            if node.ip == self._cb_cluster.get_master_node().ip:
                continue
            node_services = node.services.split(",")
            if "kv" in node_services and "fts" in node_services:
                failover_node = node
                break

        task = self._cb_cluster.async_failover(node=failover_node)
        task.result()
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def hard_failover_and_full_recovery_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        task = self._cb_cluster.async_failover()
        task.result()
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def warmup_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        self._cb_cluster.warmup_node()
        self.sleep(60, "waiting for fts to start...")
        self.wait_for_indexing_complete()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.validate_index_count(equal_bucket_doc_count=True)

    def warmup_master_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        self._cb_cluster.warmup_node(master=True)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def node_reboot_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        # self._cb_cluster.reboot_one_node(test_case=self)
        self.kill_fts_service(120)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def node_reboot_only_kv_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        # self._cb_cluster.reboot_one_node(test_case=self, master=True)
        self.kill_fts_service(120)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def memc_crash_on_kv_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        NodeHelper.kill_memcached(self._cb_cluster.get_master_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.sleep(30, "Sleep additional 30 seconds to refresh actual bucket and index docs counts.")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def fts_node_crash_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        NodeHelper.kill_cbft_process(self._cb_cluster.get_random_fts_node())
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def erl_crash_on_kv_during_index_building(self):
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        bucket_names = []
        for bucket in self._cb_cluster.get_buckets():
            bucket_names.append(bucket.name)
        NodeHelper.kill_erlang(self._cb_cluster.get_master_node(), bucket_names)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def rebalance_in_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

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
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def rebalance_out_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        self._cb_cluster.rebalance_out(num_nodes=self.num_rebalance)

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def swap_rebalance_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        self._cb_cluster.swap_rebalance(services=services,
                                        num_nodes=self.num_rebalance)

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_and_remove_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

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
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")

            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_no_rebalance_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        self._cb_cluster.async_failover().result()
        self.sleep(30)

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")

            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_master_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.wait_for_indexing_complete()

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        self._cb_cluster.failover_and_rebalance_master()

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")

            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_and_delta_recovery_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.wait_for_indexing_complete()

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        failover_node = self._cb_cluster.get_nodes()[-1:]
        for node in self._cb_cluster.get_nodes():
            if node.ip == self._cb_cluster.get_master_node().ip:
                continue
            node_services = node.services.split(",")
            if "kv" in node_services and "fts" in node_services:
                failover_node = node
                break

        task = self._cb_cluster.async_failover(node=failover_node)
        task.result()
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_and_full_recovery_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.wait_for_indexing_complete()

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        task = self._cb_cluster.async_failover()
        task.result()
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")

            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def graceful_failover_and_full_recovery_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.wait_for_indexing_complete()

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        failover_node = self._cb_cluster.get_nodes()[-1:]
        for node in self._cb_cluster.get_nodes():
            if node.ip == self._cb_cluster.get_master_node().ip:
                continue
            node_services = node.services.split(",")
            if "kv" in node_services and "fts" in node_services:
                failover_node = node
                break

        task = self._cb_cluster.async_failover(graceful=True, node=failover_node)
        task.result()
        self.sleep(30)
        self._cb_cluster.add_back_node(recovery_type='full', services=["kv, fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")

            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def graceful_failover_and_delta_recovery_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.wait_for_indexing_complete()

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        failover_node = self._cb_cluster.get_nodes()[-1:]
        for node in self._cb_cluster.get_nodes():
            if node.ip == self._cb_cluster.get_master_node().ip:
                continue
            node_services = node.services.split(",")
            if "kv" in node_services and "fts" in node_services:
                failover_node = node
                break

        task = self._cb_cluster.async_failover(graceful=True, node=failover_node)
        task.result()
        self.sleep(30)
        self._cb_cluster.add_back_node(recovery_type='delta', services=["kv,fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def warmup_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.wait_for_indexing_complete()

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        self.validate_index_count(equal_bucket_doc_count=True)
        self._cb_cluster.warmup_node()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.sleep(30, "waiting for fts process to start")

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def warmup_master_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        self._cb_cluster.warmup_node(master=True)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def node_reboot_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        # self._cb_cluster.reboot_one_node(test_case=self)
        self.kill_fts_service(120)
        self.sleep(5)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.validate_index_count(equal_bucket_doc_count=True)

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def fts_node_crash_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        NodeHelper.kill_cbft_process(self._cb_cluster.get_random_fts_node())
        self.sleep(60)

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def erl_crash_between_indexing_and_querying(self):
        # TESTED
        self.load_data_and_create_indexes()

        self.sleep(10)
        self.log.info("Index building has begun...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        index_query_matches_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_before = self.run_vector_queries_and_report(index,
                                                                                                num_queries=1)
                index_query_matches_map[index.name] = query_matches_before
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

        bucket_names = []
        for bucket in self._cb_cluster.get_buckets():
            bucket_names.append(bucket.name)
        NodeHelper.kill_erlang(self._cb_cluster.get_random_fts_node(), bucket_names)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def rebalance_in_during_querying(self):
        # TESTED
        # index = self.create_index_generate_queries()
        self.load_data_and_create_indexes(generate_queries=True)

        self.wait_for_indexing_complete()

        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        rebalance_tasks = []
        rebalance_tasks.append(self._cb_cluster.async_rebalance_in(
            num_nodes=self.num_rebalance,
            services=services))

        self.wait_for_rebalance_to_start()

        index_query_task_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.log.info("\n\nRunning queries for vector index: {}\n\n".format(index.name))
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index, self.num_vector_queries)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure} for index {index.name}")
            else:
                tasks = []
                self.log.info("Running queries for fts index: {}".format(index.name))
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                index_query_task_map[index.name] = tasks

        for rebalance_task in rebalance_tasks:
            print("\n\nGETTING RESULT FOR REBALANCE TASK\n\n")
            rebalance_task.result()

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.is_index_partitioned_balanced(index)
            else:
                query_failure = self.run_tasks_and_report(index_query_task_map[index.name], len(index.fts_queries))
                if query_failure:
                    self.fail(f"queries failed -> {query_failure} for index {index.name}")
                self.is_index_partitioned_balanced(index)
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def rebalance_out_during_querying(self):

        self.load_data_and_create_indexes(generate_queries=True)

        self.wait_for_indexing_complete()

        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        rebalance_tasks = []
        rebalance_tasks.append(self._cb_cluster.async_rebalance_out(
             num_nodes=self.num_rebalance))

        self.wait_for_rebalance_to_start()

        index_query_task_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.log.info("\n\nRunning queries for vector index: {}\n\n".format(index.name))
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index, self.num_vector_queries)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure} for index {index.name}")
            else:
                tasks = []
                self.log.info("Running queries for fts index: {}".format(index.name))
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                index_query_task_map[index.name] = tasks

        for rebalance_task in rebalance_tasks:
            rebalance_task.result()

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.is_index_partitioned_balanced(index)
            else:
                query_failure = self.run_tasks_and_report(index_query_task_map[index.name], len(index.fts_queries))
                if query_failure:
                    self.fail(f"queries failed -> {query_failure} for index {index.name}")
                self.is_index_partitioned_balanced(index)
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def swap_rebalance_during_querying(self):
        #TESTED
        self.load_data_and_create_indexes(generate_queries=True)

        self.wait_for_indexing_complete()

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

        index_query_task_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.log.info("\n\nRunning queries for vector index: {}\n\n".format(index.name))
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index, self.num_vector_queries)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure} for index {index.name}")
            else:
                tasks = []
                self.log.info("Running queries for fts index: {}".format(index.name))
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                index_query_task_map[index.name] = tasks

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.is_index_partitioned_balanced(index)
            else:
                query_failure = self.run_tasks_and_report(index_query_task_map[index.name], len(index.fts_queries))
                if query_failure:
                    self.fail(f"queries failed -> {query_failure} for index {index.name}")
                self.is_index_partitioned_balanced(index)
                self.wait_for_indexing_complete()
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
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
                self.fail("Rebalance was successful")

    def retry_rebalance_in_during_querying(self):
        #TESTED
        self._cb_cluster.enable_retry_rebalance(self.retry_time, self.num_retries)
        self.load_data_and_create_indexes(generate_queries=True)

        self.wait_for_indexing_complete()
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

        index_query_task_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.log.info("\n\nRunning queries for vector index: {}\n\n".format(index.name))
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index, self.num_vector_queries)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure} for index {index.name}")
            else:
                tasks = []
                self.log.info("Running queries for fts index: {}".format(index.name))
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                index_query_task_map[index.name] = tasks

        self.sleep(30)

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.is_index_partitioned_balanced(index)
            else:
                query_failure = self.run_tasks_and_report(index_query_task_map[index.name], len(index.fts_queries))
                if query_failure:
                    self.fail(f"queries failed -> {query_failure} for index {index.name}")
                self.is_index_partitioned_balanced(index)
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)
        self._cb_cluster.disable_retry_rebalance()

    def retry_rebalance_out_during_querying(self):
        #TESTED
        self._cb_cluster.enable_retry_rebalance(self.retry_time, self.num_retries)
        self.load_data_and_create_indexes(generate_queries=True)

        self.wait_for_indexing_complete()
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

        index_query_task_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.log.info("\n\nRunning queries for vector index: {}\n\n".format(index.name))
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index, self.num_vector_queries)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure} for index {index.name}")
            else:
                tasks = []
                self.log.info("Running queries for fts index: {}".format(index.name))
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                index_query_task_map[index.name] = tasks

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.is_index_partitioned_balanced(index)
            else:
                query_failure = self.run_tasks_and_report(index_query_task_map[index.name], len(index.fts_queries))
                if query_failure:
                    self.fail(f"queries failed -> {query_failure} for index {index.name}")
                self.is_index_partitioned_balanced(index)
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)
        self._cb_cluster.disable_retry_rebalance()

    def hard_failover_no_rebalance_during_querying(self):
        #TESTED
        self.load_data_and_create_indexes(generate_queries=True)

        self.wait_for_indexing_complete()

        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        failover_tasks = []
        failover_tasks.append(self._cb_cluster.async_failover(
            num_nodes=1,
            graceful=False))

        self.sleep(60)

        index_query_task_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.log.info("\n\nRunning queries for vector index: {}\n\n".format(index.name))
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index, self.num_vector_queries)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure} for index {index.name}")
            else:
                tasks = []
                self.log.info("Running queries for fts index: {}".format(index.name))
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                index_query_task_map[index.name] = tasks

        for failover_task in failover_tasks:
            failover_task.result()

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.is_index_partitioned_balanced(index)
            else:
                query_failure = self.run_tasks_and_report(index_query_task_map[index.name], len(index.fts_queries))
                if query_failure:
                    self.fail(f"queries failed -> {query_failure} for index {index.name}")
                self.is_index_partitioned_balanced(index)
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def hard_failover_rebalance_out_during_querying(self):
        #TESTED
        self.load_data_and_create_indexes(generate_queries=True)

        self.wait_for_indexing_complete()

        services = []
        for _ in range(self.num_rebalance):
            services.append("fts")
        failover_tasks = []
        failover_tasks.append(self._cb_cluster.async_failover_and_rebalance(
            num_nodes=1,
            graceful=False))

        index_query_task_map = {}
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.log.info("\n\nRunning queries for vector index: {}\n\n".format(index.name))
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index, self.num_vector_queries)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure} for index {index.name}")
            else:
                tasks = []
                self.log.info("Running queries for fts index: {}".format(index.name))
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                index_query_task_map[index.name] = tasks

        for failover_task in failover_tasks:
            failover_task.result()

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                self.is_index_partitioned_balanced(index)
            else:
                query_failure = self.run_tasks_and_report(index_query_task_map[index.name], len(index.fts_queries))
                if query_failure:
                    self.fail(f"queries failed -> {query_failure} for index {index.name}")
                self.is_index_partitioned_balanced(index)
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

    def update_index_during_rebalance(self):
        """
         Perform indexing + rebalance + index defn change in parallel
        """
        self.load_data_and_create_indexes()
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
        for index_obj in self._cb_cluster.get_indexes():
            index = self._cb_cluster.get_fts_index_by_name(index_obj.name)
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
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    zero_results_ok=True)
                self.log.info("SUCCESS! Hits: %s" % hits)

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches_after = self.run_vector_queries_and_report(index,
                                                                                               num_queries=1)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    zero_results_ok=True)
                self.log.info("SUCCESS! Hits: %s" % hits)

    def delete_index_during_rebalance(self):
        """
         Perform indexing + rebalance + index delete in parallel
        """
        import copy
        self.load_data_and_create_indexes()

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
                    failed_queries.append(task.query_index + 1)

        if fail_count:
            return ("%s out of %s queries failed! - %s" % (fail_count,
                                                           num_queries,
                                                           failed_queries))
        else:
            self.log.info("SUCCESS: %s out of %s queries passed"
                          % (num_queries - fail_count, num_queries))
            return None

    def validate_vector_query_matches(self, query_matches_returned,
                                      query_matches_expected):

        for q_idx in range(len(query_matches_returned)):
            returned_matches = query_matches_returned[q_idx]
            expected_matches = query_matches_expected[q_idx]

            fts_doc_ids_returned = [returned_matches[i]['fields']['sno'] for i in range(self.k)]
            fts_doc_ids_expected = [expected_matches[i]['fields']['sno'] for i in range(self.k)]

            fts_doc_returned_score = [(returned_matches[i]['fields']['sno'], returned_matches[i]['score']) for i in range(self.k)]
            fts_doc_expected_score = [(expected_matches[i]['fields']['sno'], expected_matches[i]['score']) for i in range(self.k)]

            self.log.info("Doc id's expected: {}".format(fts_doc_expected_score))
            self.log.info("Doc id's returned: {}".format(fts_doc_returned_score))

            if fts_doc_ids_returned == fts_doc_ids_expected:
                self.log.info("SUCCESS: Query matches returned {} are the same as" \
                              " the query matches expected {}".
                              format(fts_doc_ids_returned, fts_doc_ids_expected))
            else:
                for i in range(len(expected_matches)):
                    doc_expected = expected_matches[i]['fields']['sno']
                    doc_returned = returned_matches[i]['fields']['sno']

                    if doc_expected != doc_returned:
                        pos_returned = fts_doc_ids_returned.index(doc_expected)
                        print("Doc id: {}, Pos/score before rebalance: ({},{}), Pos/score after rebalance: ({},{})".
                              format(doc_expected, i, expected_matches[i]['score'], pos_returned, returned_matches[pos_returned]['score']))

                self.fail("FAIL: Query matches returned do not equal the expected" \
                          " matches for query #{}. Expected: {}, Returned: {}".
                          format(q_idx, fts_doc_ids_expected, fts_doc_ids_returned))

    def run_vector_queries_and_report(self, index, num_queries=None):
        queries = self.get_query_vectors(self.vector_dataset)[:num_queries]
        queries_to_run = []
        query_matches = []
        failed_queries = []
        fail_count = 0
        for count, q in enumerate(queries):
            query = {"query": {"match_none": {}}, "explain": True, "fields": ["*"],
                     "knn": [{"field": "vector_data", "k": self.k,
                              "vector": []}]}
            query['knn'][0]['vector'] = q.tolist()
            queries_to_run.append(query)

        for count, query in enumerate(queries_to_run):
            print("*" * 10 + f" Running Vector Query # {count} - on index {index.name} " + "*" * 10)
            if isinstance(query, str):
                query = json.loads(query)

            # Run fts query
            print(f" Running FTS Query - {query}")
            try:
                hits, matches, time_taken, status = index.execute_query(query=query['query'], knn=query['knn'],
                                                                        explain=query['explain'], return_raw_hits=True,
                                                                        fields=query['fields'])

                if hits == 0:
                    hits = -1
                self.log.info("FTS Hits for Search query: %s" % hits)
                # self.log.info("Matches returned: {}".format(matches))
                # self.log.info("Matches: {}".format(matches))
                query_matches.append(matches)

                # validate no of results are k only
                if len(matches) != self.k:
                    fail_count += 1
                    self.log.info(
                        f"No of results are not same as k=({self.k} \n || FTS hits = {hits}")
                    failed_queries.append(count)
            except Exception:
                fail_count += 1
                failed_queries.append(count)

        if fail_count:
            return ("%s out of %s queries failed! - %s" % (fail_count,
                                                           len(queries),
                                                           failed_queries)), query_matches
        else:
            self.log.info("SUCCESS: %s out of %s queries passed"
                          % (len(queries) - fail_count, len(queries)))
            return None, query_matches

    def failover_and_addback_during_querying(self):
        recovery = self._input.param("recovery", None)
        graceful = self._input.param("graceful", False)
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)
        if graceful:
            services = ['kv,fts']
        else:
            services = ['fts']

        failover_node = self._cb_cluster.get_nodes()[-1:]
        for node in self._cb_cluster.get_nodes():
            if node.ip == self._cb_cluster.get_master_node().ip:
                continue
            node_services = node.services.split(",")
            if "kv" in node_services and "fts" in node_services:
                failover_node = node
                break

        tasks = []
        tasks.append(self._cb_cluster.async_failover_add_back_node(
            num_nodes=1,
            graceful=graceful,
            recovery_type=recovery,
            services=services,
            node=failover_node))

        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

    def fts_node_down_with_replicas_during_querying(self):
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.stop_couchbase(node)
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                continue
            try:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())

                vector_query_failure = None
                for index in self._cb_cluster.get_indexes():
                    if self.type_of_load == "separate" and "vector_" in index.name:
                        vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
                if vector_query_failure:
                    self.fail(f"Vector queries failed -> {vector_query_failure}")
            except Exception as e:
                self.log.info("Expected exception : %s" % e)
        NodeHelper.start_couchbase(node)
        NodeHelper.wait_warmup_completed([node])

        vector_query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                self.run_query_and_compare(index)
        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

    def warmup_master_during_querying(self):
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

        self._cb_cluster.warmup_node(master=True)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

    def node_reboot_during_querying(self):
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)
        # node = self._cb_cluster.reboot_one_node(test_case=self)
        node = self.kill_fts_service(120)
        self._cb_cluster.set_bypass_fts_node(node)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

    def memc_crash_during_indexing_and_querying(self):
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_memcached(node)
        self._cb_cluster.set_bypass_fts_node(node)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

    def erl_crash_during_querying(self):
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_erlang(node)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

    def fts_crash_during_querying(self):
        # TESTED
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_cbft_process(node)
        self._cb_cluster.set_bypass_fts_node(node)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

    def memc_crash_during_indexing_and_querying(self):
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_memcached(node)
        self._cb_cluster.set_bypass_fts_node(node)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

    def erl_crash_during_querying(self):
        # TESTED
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_erlang(node)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

    def fts_crash_during_querying(self):
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)
        for index in self._cb_cluster.get_indexes():
            self.run_query_and_compare(index)
        node = self._cb_cluster.get_random_fts_node()
        NodeHelper.kill_cbft_process(node)
        self._cb_cluster.set_bypass_fts_node(node)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

    def update_index_during_failover_and_rebalance(self):
        """
         Perform indexing + failover + index defn change in parallel
        """
        self.load_data_and_create_indexes()
        self.sleep(10)
        self.log.info("Index building has begun...")
        idx_name = None
        for index in self._cb_cluster.get_indexes():
            if not idx_name:
                idx_name = index.name
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        # wait till indexing is midway...
        self.wait_for_indexing_complete(self._find_expected_indexed_items_number() // 2)
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
        vector_query_failure = []
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        hits, _, _, _ = index.execute_query(
            self.query,
            expected_hits=index.get_src_bucket_doc_count())
        self.log.info("Hits: %s" % hits)
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index, num_queries=2)
        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

    def test_stop_restart_rebalance_in_loop(self):
        """
         Kick off rebalance-out. Stop and stop rebalance in a loop
         continuously till rebalance finishes.
        :return:
        """
        count = 0
        self.load_data_and_create_indexes(wait_for_index_complete=True, generate_queries=True)

        from lib.membase.api.rest_client import RestConnection
        rest = RestConnection(self._cb_cluster.get_master_node())
        nodes = rest.node_statuses()
        ejected_nodes = []

        for node in nodes:
            if node.ip != self._master.ip:
                ejected_nodes.append(node.id)
                break
        self.log.info(
            "removing node(s) {0} from cluster".format(ejected_nodes))

        while count < 5:
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
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

    def test_rebalance_cancel_new_rebalance(self):
        """
            Load bucket, do not create index
            From a 3 kv+fts node cluster, rebalance out master + one node
            Immediately (after few secs), stop rebalance
            Rebalance out other nodes than master.
            After rebalance completes, create an index
        :return:
        """
        # create vector buckets
        vect_bucket_containers = self.create_vect_bucket_containers(self.num_vect_buckets)
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client,
                                                              containers=vect_bucket_containers)

        self.load_data(exempt_bucket_prefix="vector-data")
        self.load_vector_data(containers, dataset=self.vector_dataset, goloader_toggle= self.goloader_toggle)
        vect_index_containers = self.create_vect_index_containers(vect_bucket_containers,
                                                                  self.index_per_vect_bucket)

        non_master_nodes = list(set(self._cb_cluster.get_nodes()) -
                                {self._master})

        from lib.membase.api.rest_client import RestConnection
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

        self.log.info("Cluster nodes: {}".format(self._cb_cluster.get_nodes()))
        for remove_node in eject_nodes:
            self._cb_cluster.get_nodes().remove(remove_node)
            node_services = remove_node.services.split(",")

            if "kv" in node_services:
                self._cb_cluster.get_kv_nodes().remove(remove_node)

        self.create_fts_indexes_some_buckets(exempt_bucket_prefix="vector-bucket")
        similarity = random.choice(["l2_norm", "dot_product"])
        vector_fields = {"dims": self.dimension, "similarity": similarity}
        self._create_fts_index_parameterized(field_name="vector_data", field_type="vector",
                                             test_indexes=vect_index_containers,
                                             vector_fields=vector_fields,
                                             create_vector_index=True,
                                             wait_for_index_complete=False)
        self.sleep(10)

        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)

    def rebalance_in_parallel_partitions_move_add_node(self):
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_maxConcurrentPartitionMovesPerNode(self.default_concurrent_partition_moves_per_node)
        rest.set_maxFeedsPerDCPAgent(1)
        rest.set_maxDCPAgents(3)

        self.load_data_and_create_indexes(wait_for_index_complete=False)
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
        simple_rebalance_time = end_rebalance_time - start_rebalance_time

        self._cb_cluster.rebalance_out(num_nodes=1)
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()

        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        rest.set_maxConcurrentPartitionMovesPerNode(self.max_concurrent_partition_moves_per_node)

        start_parallel_rebalance_time = time.time()
        self._cb_cluster.rebalance_in(num_nodes=1, services=["fts"])
        end_parallel_rebalance_time = time.time()
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
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

        self.load_data_and_create_indexes(wait_for_index_complete=False)

        for index in self._cb_cluster.get_indexes():
            index.update_index_partitions(self.num_index_partitions)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
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

        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        vector_query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index, num_queries=2)
            else:
                hits, _, _, _ = index.execute_query(query=self.query,
                                                    expected_hits=self._find_expected_indexed_items_number())
                self.log.info("SUCCESS! Hits: %s" % hits)

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        self.assertTrue(simple_rebalance_time > parallel_rebalance_time,
                        "Rebalance out with maxConcurrentPartitionMovesPerNode={0} takes longer time than "
                        "rebalance out with maxConcurrentPartitionMovesPerNode={1}".
                        format(self.max_concurrent_partition_moves_per_node,
                               self.default_concurrent_partition_moves_per_node))

    def rebalance_2_nodes_during_index_building(self):
        self.load_data_and_create_indexes(wait_for_index_complete=False, generate_queries=True)
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
        if self.rebalance_out:
            self._cb_cluster.rebalance_out(2)
        if self.rebalance_in:
            self._cb_cluster.rebalance_in(2, services=["fts", "fts"])
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")

    def rebalance_kill_fts_existing_fts_node(self):
        self.load_data_and_create_indexes(wait_for_index_complete=False, generate_queries=True)
        self.sleep(10)
        self.log.info("Index building has begun...")
        for index in self._cb_cluster.get_indexes():
            self.log.info("Index count for %s: %s"
                          % (index.name, index.get_indexed_doc_count()))
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
        for index in self._cb_cluster.get_indexes():
            self.is_index_partitioned_balanced(index)
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)
        tasks = []
        vector_query_failure = None
        query_failure = None
        for index in self._cb_cluster.get_indexes():
            if self.type_of_load == "separate" and "vector_" in index.name:
                vector_query_failure, query_matches = self.run_vector_queries_and_report(index)
            else:
                for count in range(0, len(index.fts_queries)):
                    tasks.append(self._cb_cluster.async_run_fts_query_compare(
                        fts_index=index,
                        es=self.es,
                        es_index_name=None,
                        query_index=count))
                query_failure = self.run_tasks_and_report(tasks, len(index.fts_queries))

        if vector_query_failure and query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure} \n Normal queries failed -> {query_failure}")

        if vector_query_failure:
            self.fail(f"Vector queries failed -> {vector_query_failure}")

        if query_failure:
            self.fail(f"queries failed -> {query_failure}")
