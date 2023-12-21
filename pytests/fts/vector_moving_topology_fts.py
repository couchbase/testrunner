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
        # 2 BUCKETS
        # 1 BUCKET-> NORMAL DATA LOAD -> Normal indexes
        # 2 bucket-> vector load -> vector indexes
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
        # 3 BUCKETS
        # 1 BUCKET-> NORMAL DATA LOAD -> Normal indexes
        # 2 bucket-> vector load -> vector indexes
        # 3 bucket-> load_data with vector_search = True -> this loads vector within normal doc
        # -> create both normal and vector indexes on 3 bucket
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

    def rebalance_in_during_index_building(self):
        if self._input.param("must_fail", False):
            self.fail("Temporal fail to let all the other tests to be passed")

        self.setup_seperate_load_for_test()

        # self.setup_common_load_for_test()

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
        self.setup_seperate_load_for_test()

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
        self.setup_seperate_load_for_test()

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
        self.setup_seperate_load_for_test()

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
        self.setup_seperate_load_for_test()

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
