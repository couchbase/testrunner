import copy
import logging

from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.tuq_generators import TuqGenerators
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from .base_gsi import BaseSecondaryIndexingTests

log = logging.getLogger(__name__)


class SecondaryIndexDGMTests(BaseSecondaryIndexingTests):
    def setUp(self):
        super(SecondaryIndexDGMTests, self).setUp()
        self.num_plasma_buckets = self.input.param("standard_buckets", 1)
        self.indexMemQuota = self.input.param("indexMemQuota", 256)
        self.in_mem_comp = self.input.param("in_mem_comp", None)
        self.sweep_interval = self.input.param("sweep_interval", 120)
        self.dgmServer = self.get_nodes_from_services_map(service_type="index")
        self.rest = RestConnection(self.dgmServer)
        if self.indexMemQuota > 256:
            log.info("Setting indexer memory quota to {0} MB...".format(self.indexMemQuota))
            self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=self.indexMemQuota)
            self.sleep(30)
        if self.in_mem_comp:
            self.rest.set_index_settings({"indexer.plasma.mainIndex.evictSweepInterval": self.sweep_interval})
            self.rest.set_index_settings({"indexer.plasma.backIndex.evictSweepInterval": self.sweep_interval})
            self.rest.set_index_settings({"indexer.plasma.backIndex.enableInMemoryCompression": True})
            self.rest.set_index_settings({"indexer.plasma.mainIndex.enableInMemoryCompression": True})
            # add if condition
            self.rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": 60000})
        self.rest.set_index_settings({"indexer.plasma.mainIndex.minCPUsForInMemCompression": 1})
        self.rest.set_index_settings({"indexer.plasma.backIndex.minCPUsForInMemCompression": 1})
        self.deploy_node_info = ["{0}:{1}".format(self.dgmServer.ip, self.node_port)]
        self.load_query_definitions = []
        self.initial_index_number = self.input.param("initial_index_number", 5)
        for x in range(self.initial_index_number):
            index_name = "index_name_" + str(x)
            query_definition = QueryDefinition(index_name=index_name, index_fields=["VMs"],
                                               query_template="SELECT * FROM %s ", groups=["simple"],
                                               index_where_clause=" VMs IS NOT NULL ")
            self.load_query_definitions.append(query_definition)
        if self.load_query_definitions:
            self.multi_create_index(buckets=self.buckets,
                                    query_definitions=self.load_query_definitions,
                                    deploy_node_info=self.deploy_node_info)

    def tearDown(self):
        super(SecondaryIndexDGMTests, self).tearDown()

    def test_dgm_increase_mem_quota(self):
        """
        1. Get DGM
        2. Drop Already existing indexes
        3. Verify if state of indexes is changed to ready
        :return:
        """
        self.multi_create_index(
            buckets=self.buckets, query_definitions=self.query_definitions,
            deploy_node_info=self.deploy_node_info)
        self.index_map = self.rest.get_index_status()
        self.sleep(30)
        self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        indexer_memQuota = self.get_indexer_mem_quota()
        log.info("Current Indexer Memory Quota is {0}".format(indexer_memQuota))
        for cnt in range(5):
            indexer_memQuota += 200
            log.info("Increasing Indexer Memory Quota to {0}".format(indexer_memQuota))
            rest = RestConnection(self.dgmServer)
            rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=indexer_memQuota)
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
        if self.in_mem_comp:
            self.sleep(60)
            if not self.verify_compression_stat(self.query_definitions):
                self.fail("Seeing index data not compressed")

    def test_dgm_drop_indexes(self):
        """
        1. Get DGM
        2. Drop Already existing indexes
        3. Verify if state of indexes is changed to ready
        :return:
        """
        self.multi_create_index(
            buckets=self.buckets, query_definitions=self.query_definitions,
            deploy_node_info=self.deploy_node_info)
        self.index_map = self.rest.get_index_status()
        self.sleep(30)
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
        self.retry_time = self.input.param("retry_time", 5)
        count = 0
        if self.in_mem_comp:
            while count < self.retry_time:
                self.sleep(self.sweep_interval)
                if self.verify_compression_stat(self.query_definitions):
                    break
                count += 1
        if not self.verify_compression_stat(self.query_definitions):
            self.fail("Seeing index data not compressed")

    def test_dgm_flush_bucket(self):
        """
        1. Get DGM
        2. Flush a bucket
        3. Verify if state of indexes is changed
        :return:
        """
        self.multi_create_index(
            buckets=self.buckets, query_definitions=self.query_definitions,
            deploy_node_info=self.deploy_node_info)
        self.index_map = self.rest.get_index_status()
        self.sleep(30)
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
        self.retry_time = self.input.param("retry_time", 5)
        count = 0
        if self.in_mem_comp:
            while count < self.retry_time:
                self.sleep(self.sweep_interval)
                if self.verify_compression_stat(self.query_definitions):
                    break
                count += 1
        if not self.verify_compression_stat(self.query_definitions):
            self.fail("Seeing index data not compressed")

    def test_oom_delete_bucket(self):
        """
        1. Get DGM
        2. Delete a bucket
        3. Verify if state of indexes is changed
        :return:
        """
        self.multi_create_index(
            buckets=self.buckets, query_definitions=self.query_definitions,
            deploy_node_info=self.deploy_node_info)
        self.index_map = self.rest.get_index_status()
        self.sleep(30)
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

    def test_increase_indexer_memory_quota(self):
        pre_operation_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_operation_tasks])
        kvOps_tasks = self.async_run_doc_ops()
        self.sleep(30)
        mid_operation_tasks = self.async_run_operations(phase="in_between")
        indexer_memQuota = self.get_indexer_mem_quota()
        log.info("Current Indexer Memory Quota is {0}".format(indexer_memQuota))
        for cnt in range(3):
            indexer_memQuota += 50
            log.info("Increasing Indexer Memory Quota to {0}".format(indexer_memQuota))
            rest = RestConnection(self.dgmServer)
            rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=indexer_memQuota)
            self.sleep(30)
        self._run_tasks([kvOps_tasks, mid_operation_tasks])
        post_operation_tasks = self.async_run_operations(phase="after")
        self._run_tasks([post_operation_tasks])
        if self.in_mem_comp:
            self.sleep(2 * self.sweep_interval)
            if not self.verify_compression_stat(self.query_definitions):
                self.fail("Seeing index data not compressed")

    def test_decrease_indexer_memory_quota(self):
        pre_operation_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_operation_tasks])
        kvOps_tasks = self.async_run_doc_ops()
        mid_operation_tasks = self.async_run_operations(phase="in_between")
        indexer_memQuota = self.get_indexer_mem_quota()
        log.info("Current Indexer Memory Quota is {0}".format(indexer_memQuota))
        for cnt in range(3):
            indexer_memQuota -= 50
            log.info("Decreasing Indexer Memory Quota to {0}".format(indexer_memQuota))
            rest = RestConnection(self.dgmServer)
            rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=indexer_memQuota)
            self.sleep(30)
        self._run_tasks([kvOps_tasks, mid_operation_tasks])
        post_operation_tasks = self.async_run_operations(phase="after")
        self._run_tasks([post_operation_tasks])
        if self.in_mem_comp:
            self.sleep(2 * self.sweep_interval)
            if not self.verify_compression_stat(self.query_definitions):
                self.fail("Seeing index data not compressed")

    def test_decrease_indexer_memory_quota_in_dgm(self):
        self.index_map = self.rest.get_index_status()
        self.sleep(30)
        self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer], memory_quota=400)
        pre_operation_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_operation_tasks])
        kvOps_tasks = self.async_run_doc_ops()
        mid_operation_tasks = self.async_run_operations(phase="in_between")
        indexer_memQuota = self.get_indexer_mem_quota()
        log.info("Current Indexer Memory Quota is {0}".format(indexer_memQuota))
        for cnt in range(3):
            indexer_memQuota -= 50
            log.info("Decreasing Indexer Memory Quota to {0}".format(indexer_memQuota))
            rest = RestConnection(self.dgmServer)
            rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=indexer_memQuota)
            self.sleep(30)
        self._run_tasks([kvOps_tasks, mid_operation_tasks])
        post_operation_tasks = self.async_run_operations(phase="after")
        self._run_tasks([post_operation_tasks])
        if self.in_mem_comp:
            self.sleep(2 * self.sweep_interval)
            if not self.verify_compression_stat(self.query_definitions):
                self.fail("Seeing index data not compressed")

    def test_increase_decrease_mem_quota(self):
        memory_quanta = [50, 100, 150, 200]
        pre_operation_tasks = self.async_run_operations(phase="before")
        self._run_tasks([pre_operation_tasks])
        kvOps_tasks = self.async_run_doc_ops()
        mid_operation_tasks = self.async_run_operations(phase="in_between")
        indexer_memQuota = self.get_indexer_mem_quota()
        log.info("Current Indexer Memory Quota is {0}".format(indexer_memQuota))
        for cnt in range(5):
            for memory_quantum in memory_quanta:
                indexer_memQuota += memory_quantum
                log.info("Increasing Indexer Memory Quota to {0}".format(indexer_memQuota))
                rest = RestConnection(self.dgmServer)
                rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=indexer_memQuota)
                self.sleep(30)
                indexer_memQuota -= memory_quantum
                log.info("Decreasing Indexer Memory Quota to {0}".format(indexer_memQuota))
                rest = RestConnection(self.dgmServer)
                rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=indexer_memQuota)
                self.sleep(30)
        self._run_tasks([kvOps_tasks, mid_operation_tasks])
        post_operation_tasks = self.async_run_operations(phase="after")
        self._run_tasks([post_operation_tasks])
        if self.in_mem_comp:
            self.sleep(60)
            if not self.verify_compression_stat(self.query_definitions):
                self.fail("Seeing index data not compressed")

    def test_plasma_dgm_with_multiple_resident_ratio(self):
        self.dgm_rasident_ratio = self.input.param("dgm_resident_ratio", None)
        self.multi_create_index(
            buckets=self.buckets, query_definitions=self.query_definitions,
            deploy_node_info=self.deploy_node_info)
        self.index_map = self.rest.get_index_status()

        def validate_disk_writes(indexer_nodes=None, resident_ratio=.99):
            if not indexer_nodes:
                indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                    get_all_nodes=True)
            for node in indexer_nodes:
                indexer_rest = RestConnection(node)
                content = indexer_rest.get_index_storage_stats()
                for index in list(content.values()):
                    for stats in list(index.values()):
                        if stats["MainStore"]["resident_ratio"] <= (resident_ratio+.05) \
                            and stats["MainStore"]["resident_ratio"] >= (resident_ratio-.05):
                            return True
            return False

        def kv_mutations(self, docs=1):
            if not docs:
                docs = self.docs_per_day
            gens_load = self.generate_docs(docs)
            self.full_docs_list = self.generate_full_docs_list(gens_load)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            tasks = self.async_load(generators_load=gens_load, op_type="create",
                                batch_size=self.batch_size)
            return tasks

        log.info("Trying to get all indexes in DGM...")
        log.info("Setting indexer memory quota to 256 MB...")
        node = self.get_nodes_from_services_map(service_type="index")
        rest = RestConnection(node)
        rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=256)
        cnt = 0
        docs = 50
        validate_dgm = False
        while cnt < 100:
            validate_dgm = validate_disk_writes([self.dgmServer])
            if validate_dgm:
                log.info("========== DGM is achieved ==========")
                break
            for task in kv_mutations(self, docs):
                task.result()
            self.sleep(30)
            cnt += 1
            docs += 20
        self.assertTrue(validate_dgm, "DGM is not achieved")
        if self.in_mem_comp:
            self.sleep(60)
            if not self.verify_compression_stat(self.query_definitions):
                self.fail("Seeing index data not compressed")
        self.multi_query_using_index()

    def test_lru(self):
        self.multi_create_index(
            buckets=self.buckets, query_definitions=self.query_definitions,
            deploy_node_info=self.deploy_node_info)
        self.index_map = self.rest.get_index_status()
        self.sleep(30)
        self.get_dgm_for_plasma(indexer_nodes=[self.dgmServer])
        for i in range(5):
            self.multi_query_using_index()
        query_definitions = []

        query_definitions.append(QueryDefinition(index_name="lru_job_title", index_fields=["job_title"],
                                                 query_template="SELECT * FROM %s WHERE {0}".format(
                                                     " %s " % "job_title = \"Engineer\" ORDER BY _id"),
                                                 groups=["employee"], index_where_clause=" job_title IS NOT NULL "))
        query_definitions.append(QueryDefinition(index_name="lru_join_yr", index_fields=["join_yr"],
                                                 query_template="SELECT * FROM %s WHERE {0}".format(
                                                     " %s " % "join_yr = 2008  ORDER BY _id"), groups=["employee"],
                                                 index_where_clause=" join_yr IS NOT NULL "))
        cache_misses = {}
        for bucket in self.buckets:
            if bucket.name not in list(cache_misses.keys()):
                cache_misses[bucket.name] = {}
            for query_definition in self.query_definitions:
                content = self.rest.get_index_storage_stats()
                if query_definition.index_name not in list(cache_misses[bucket.name].keys()):
                    cache_misses[bucket.name][query_definition.index_name] = \
                        content[bucket.name][query_definition.index_name]["MainStore"]["cache_misses"]
        self.multi_query_using_index(query_definitions=query_definitions)
        if self.in_mem_comp:
            self.sleep(60)
            if not self.verify_compression_stat(self.query_definitions):
                self.fail("Seeing index data not compressed")
        self.sleep(30)
        for bucket in self.buckets:
            for query_definition in self.query_definitions:
                content = self.rest.get_index_storage_stats()
                cache_miss_before_query = cache_misses[bucket.name][query_definition.index_name]
                cache_miss_after_query = content[bucket.name][query_definition.index_name]["MainStore"]["cache_misses"]
                if (cache_miss_after_query > cache_miss_before_query):
                    log.info("Reads happen from disk as expected")
                else:
                    log.info("Reads happen from memory for index {0} on bucket {1}".format(query_definition.index_name, bucket.name))

    def test_compaction(self):
        self.multi_create_index(
            buckets=self.buckets, query_definitions=self.query_definitions,
            deploy_node_info=self.deploy_node_info)
        self.index_map = self.rest.get_index_status()
        self.sleep(30)
        for i in range(10):
            self.multi_query_using_index(verify_results=False)
        self.multi_query_using_index()
        num_rec_compressed_before = {}
        num_disk_snaps_prev = dict()
        index_stats = self.rest.get_indexer_stats()
        for bucket in self.buckets:
            if bucket.name not in list(num_rec_compressed_before.keys()):
                num_rec_compressed_before[bucket.name] = {}
            for query_definition in self.query_definitions:
                content = self.rest.get_index_storage_stats()
                self.log.info(f'Logging some stats for index {query_definition.index_name}')
                self.log.info(f'compacts : {content[bucket.name][query_definition.index_name]["MainStore"]["compacts"]}')
                self.log.info(f'num_rec_allocs : {content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_allocs"]}')
                self.log.info(f'num_rec_frees : {content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_frees"]}')
                self.log.info(f'num_rec_swapout : {content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_swapout"]}')
                self.log.info(f'num_rec_swapin : {content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_swapin"]}')
                self.log.info(f'num_rec_compressed : {content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_compressed"]}')
                self.log.info(f'num_pages : {content[bucket.name][query_definition.index_name]["MainStore"]["num_pages"]}')
                if query_definition.index_name not in list(num_rec_compressed_before[bucket.name].keys()):
                    num_rec_compressed_before[bucket.name][query_definition.index_name] = \
                        content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_compressed"]

        for bucket in self.buckets:
            for query_definition in self.query_definitions:
                index_stats = self.rest.get_indexer_stats()
                if "num_disk_snapshots" in index_stats[bucket.name][query_definition.index_name]:
                    self.log.info("Got the num_disk_snapshots")
                    num_disk_snaps_prev[bucket.name + query_definition.index_name] = index_stats[bucket.name][
                        query_definition.index_name]["num_disk_snapshots"]

        self.run_doc_ops()
        count = 0
        while not self._verify_items_count() and count < 15:
            self.log.info("All Items Yet to be Indexed...")
            self.sleep(10)
            count += 1
        if not self._verify_items_count():
            raise Exception("All Items didn't get Indexed...")
        self.sleep(60, "wait before compaction so that every should be flushed")

        is_snaps_incr = False
        for count in range(40):
            if is_snaps_incr:
                break
            for bucket in self.buckets:
                for query_definition in self.query_definitions:
                    index_stats = self.rest.get_indexer_stats()
                    self.log.info("Got the index_map and comparing it with previous num_disk_snapshots")
                    if "num_disk_snapshots" in index_stats[bucket.name][query_definition.index_name]:
                        if num_disk_snaps_prev[bucket.name + query_definition.index_name] < index_stats[bucket.name][
                            query_definition.index_name]["num_disk_snapshots"]:
                            is_snaps_incr = True
                            break
                        else:
                            self.sleep(5)
        self.rest.trigger_compaction()
        for bucket in self.buckets:
            for query_definition in self.query_definitions:
                content = self.rest.get_index_storage_stats()
                self.log.info(f'Logging some stats for index {query_definition.index_name}')
                self.log.info(f'compacts : {content[bucket.name][query_definition.index_name]["MainStore"]["compacts"]}')
                self.log.info(f'num_rec_allocs : {content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_allocs"]}')
                self.log.info(f'num_rec_frees : {content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_frees"]}')
                self.log.info(f'num_rec_swapout : {content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_swapout"]}')
                self.log.info(f'num_rec_swapin : {content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_swapin"]}')
                self.log.info(f'num_rec_compressed : {content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_compressed"]}')
                self.log.info(f'num_pages : {content[bucket.name][query_definition.index_name]["MainStore"]["num_pages"]}')
                num_rec_compressed_before_compact = num_rec_compressed_before[bucket.name][query_definition.index_name]
                num_rec_compressed_after_compact = content[bucket.name][query_definition.index_name]["MainStore"]["num_rec_compressed"]
                self.log.info(f'num_rec_compressed before: {num_rec_compressed_before_compact} and num_rec_compressed after: {num_rec_compressed_after_compact}')
                if num_rec_compressed_after_compact <= num_rec_compressed_before_compact:
                    self.log.info("num_rec_compressed_after_compact found to be less than num_rec_compressed_before_compact as expected")
                else:
                    self.fail("num_rec_compressed_after_compact found to be greater than num_rec_compressed_before_compact which is not expected")

        self.multi_query_using_index()

        query_definitions = []

        query_definitions.append(QueryDefinition(index_name="lru_job_title", index_fields=["job_title"],
                                                 query_template="SELECT * FROM %s WHERE {0}".format(
                                                     " %s " % "job_title = \"Engineer\" ORDER BY _id"),
                                                 groups=["employee"], index_where_clause=" job_title IS NOT NULL "))
        query_definitions.append(QueryDefinition(index_name="lru_join_yr", index_fields=["join_yr"],
                                                 query_template="SELECT * FROM %s WHERE {0}".format(
                                                     " %s " % "join_yr = 2008  ORDER BY _id"), groups=["employee"],
                                                 index_where_clause=" join_yr IS NOT NULL "))

        self.multi_query_using_index(query_definitions=query_definitions)

    def _get_indexer_out_of_dgm(self, indexer_nodes=None):
        body = {"stale": "False"}
        for bucket in self.buckets:
            for query_definition in self.query_definitions:
                index_id = self.index_map[bucket.name][query_definition.index_name]["id"]
                for i in range(3):
                    log.info("Running Full Table Scan on {0}".format(
                        query_definition.index_name))
                    self.rest.full_table_scan_gsi_index_with_rest(index_id, body)
                self.sleep(10)
        disk_writes = self.validate_disk_writes(indexer_nodes)
        return disk_writes

    def validate_disk_writes(self, indexer_nodes=None):
        if not indexer_nodes:
            indexer_nodes = self.get_nodes_from_services_map(service_type="index",
                                                get_all_nodes=True)
        for node in indexer_nodes:
            indexer_rest = RestConnection(node)
            content = indexer_rest.get_index_storage_stats()
            for index in list(content.values()):
                for stats in list(index.values()):
                    if stats["MainStore"]["resident_ratio"] >= 1.0:
                        return False
        return True

    def _run_tasks(self, tasks_list):
        for tasks in tasks_list:
            for task in tasks:
                task.result()
