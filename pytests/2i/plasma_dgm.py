import copy
import logging

from couchbase_helper.query_definitions import QueryDefinition
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.bucket_helper import BucketOperationHelper
from remote.remote_util import RemoteMachineShellConnection
from base_2i import BaseSecondaryIndexingTests

log = logging.getLogger(__name__)

class SecondaryIndexDGMTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexDGMTests, self).setUp()
        self.indexMemQuota = self.input.param("indexMemQuota", 256)
        self.doc_ops = self.input.param("doc_ops", True)
        self.dgmServer = self.get_nodes_from_services_map(service_type="index")
        self.rest = RestConnection(self.dgmServer)
        if self.indexMemQuota > 256:
            log.info("Setting indexer memory quota to {0} MB...".format(self.indexMemQuota))
            self.rest.set_indexer_memoryQuota(indexMemoryQuota=self.indexMemQuota)
            self.sleep(30)
        self.deploy_node_info = ["{0}:{1}".format(self.dgmServer.ip, self.dgmServer.port)]
        self.multi_create_index(
            buckets=self.buckets, query_definitions=self.query_definitions,
            deploy_node_info=self.deploy_node_info)
        self.index_map = self.rest.get_index_status()
        self.sleep(30)

    def tearDown(self):
        super(SecondaryIndexDGMTests, self).tearDown()

    def test_dgm_increase_mem_quota(self):
        """
        1. Get DGM
        2. Drop Already existing indexes
        3. Verify if state of indexes is changed to ready
        :return:
        """
        self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        indexer_memQuota = self.get_indexer_mem_quota()
        log.info("Current Indexer Memory Quota is {}".format(indexer_memQuota))
        for cnt in range(5):
            indexer_memQuota += 200
            log.info("Increasing Indexer Memory Quota to {0}".format(indexer_memQuota))
            rest = RestConnection(self.dgmServer)
            rest.set_indexer_memoryQuota(indexMemoryQuota=indexer_memQuota)
            self.sleep(60)
            indexer_dgm = self._get_indexer_out_of_dgm(indexer_nodes=[self.dgmServer])
            if not indexer_dgm:
                break
        self.assertFalse(self._get_indexer_out_of_dgm(indexer_nodes=[self.dgmServer]),
                         "Indexer still in DGM")
        self.sleep(60)
        log.info("=== Indexer out of DGM ===")
        self._verify_bucket_count_with_index_count(self.query_definitions)
        self.multi_query_using_index(buckets=self.buckets,
                    query_definitions=self.query_definitions)

    def test_dgm_drop_indexes(self):
        """
        1. Get DGM
        2. Drop Already existing indexes
        3. Verify if state of indexes is changed to ready
        :return:
        """
        self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        temp_query_definitions = copy.copy(self.query_definitions[1:])
        for query_definition in temp_query_definitions:
            for bucket in self.buckets:
                log.info("Dropping {0} from bucket {1}".format(query_definition.index_name,
                                                               bucket.name))
                self.drop_index(bucket=bucket, query_definition=query_definition)
                self.sleep(10)
            self.query_definitions.remove(query_definition)
            indexer_dgm = self._get_indexer_out_of_dgm(indexer_nodes=[self.dgmServer])
            if not indexer_dgm:
               log.info("Indexer out of DGM...")
               break
        self.sleep(60)
        self.assertFalse(self._get_indexer_out_of_dgm(indexer_nodes=[self.dgmServer]),
                         "Indexer still in DGM")
        self._verify_bucket_count_with_index_count(self.query_definitions)
        self.multi_query_using_index(buckets=self.buckets,
                                     query_definitions=self.query_definitions)

    def test_dgm_flush_bucket(self):
        """
        1. Get DGM
        2. Flush a bucket
        3. Verify if state of indexes is changed
        :return:
        """
        self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        rest = RestConnection(self.dgmServer)
        for bucket in self.buckets[1:]:
            log.info("Flushing bucket {0}...".format(bucket.name))
            rest.flush_bucket(bucket)
            self.sleep(60)
            indexer_dgm = self._get_indexer_out_of_dgm(indexer_nodes=[self.dgmServer])
            if not indexer_dgm:
                log.info("Indexer out of DGM...")
                break
        self.sleep(60)
        self.assertFalse(self._get_indexer_out_of_dgm(indexer_nodes=[self.dgmServer]),
                         "Indexer still in DGM")
        self._verify_bucket_count_with_index_count(self.query_definitions)
        self.multi_query_using_index(buckets=self.buckets,
                                     query_definitions=self.query_definitions)

    def test_oom_delete_bucket(self):
        """
        1. Get DGM
        2. Delete a bucket
        3. Verify if state of indexes is changed
        :return:
        """
        self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        for i in range(len(self.buckets)):
            log.info("Deleting bucket {0}...".format(self.buckets[i].name))
            BucketOperationHelper.delete_bucket_or_assert(
                serverInfo=self.dgmServer, bucket=self.buckets[i].name)
            self.sleep(60)
            indexer_dgm = self._get_indexer_out_of_dgm(indexer_nodes=[self.dgmServer])
            if not indexer_dgm:
                if i < len(self.buckets):
                    self.buckets = self.buckets[i+1:]
                else:
                    #TODO: Pras: Need better solution here
                    self.buckets = []
                break
            log.info("Indexer Still in DGM...")
        self.sleep(60)
        self.assertFalse(self._get_indexer_out_of_dgm(indexer_nodes=[self.dgmServer]),
                         "Indexer still in DGM")
        self._verify_bucket_count_with_index_count(self.query_definitions)
        self.multi_query_using_index(buckets=self.buckets,
                        query_definitions=self.query_definitions)

    def _get_indexer_out_of_dgm(self, indexer_nodes=None):
        body = {"stale": "False"}
        for bucket in self.buckets:
            for query_definition in self.query_definitions:
                index_id = self.index_map[bucket.name][query_definition.index_name]["id"]
                self.rest.full_table_scan_gsi_index_with_rest(index_id, body)
        disk_writes = self.validate_disk_writes(indexer_nodes)
        return disk_writes

    def validate_disk_writes(self, indexer_nodes=None):
        if not indexer_nodes:
            indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                get_all_nodes=True)
        for node in indexer_nodes:
            indexer_rest = RestConnection(node)
            content = indexer_rest.get_index_storage_stats()
            for index in content.values():
                for stats in index.values():
                    if stats["MainStore"]["resident_ratio"] == 1.0:
                        return False
        return True

    def get_indexer_mem_quota(self):
        """
        Sets Indexer memory Quota
        :param memQuota:
        :return:
        int indexer memory quota
        """
        rest = RestConnection(self.dgmServer)
        content = rest.cluster_status()
        return int(content['indexMemoryQuota'])
