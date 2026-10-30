import time

from couchbase_helper.documentgenerator import SDKDataLoader
from couchbase_helper.query_definitions import QueryDefinition
from .base_gsi import BaseSecondaryIndexingTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection

class SecondaryIndexingPlasmaBasicTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingPlasmaBasicTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingPlasmaBasicTests, self).tearDown()

    def test_plasma_index_with_ephemeral_bucket(self):
        if self.gsi_type == "plasma":
            try:
                self.multi_create_index(query_definitions=self.query_definitions)
            except Exception as ex:
                msg = "Error=Ephemeral Buckets Must Use MOI Storage"
                if msg not in str(ex):
                    self.log.info(str(ex))
                    raise

    def test_increase_memory_beyond_max_limit(self):
        rest = RestConnection(self.master)
        memory_quota = 5000
        content = rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=memory_quota)
        if content:
            msg = "exceeds the maximum allowed quota"
            if not msg in content:
                raise

    def test_plasma_skiplist_padding_memory_size(self):
        """MB-74156: Node.Size() ignored the padding flag, under-counting 8 bytes per padded node.
        Padded nodes should account for ~8 bytes more per page than unpadded nodes on the same build.
        Note: this test is for the x86_64 architecture."""
        num_docs = self.input.param("num_docs", 200000)
        expected_delta = self.input.param("expected_delta", 8)
        tolerance = self.input.param("tolerance", 1.5)
        index_name = "idx_name"
        namespace = f"default:`{self.test_bucket}`"
        bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                   replicas=self.num_replicas)
        self.cluster.create_standard_bucket(name=self.test_bucket, port=11222, bucket_params=bucket_params)
        self.buckets = self.rest.get_buckets()
        self.gen_create = SDKDataLoader(num_ops=num_docs, percent_create=100, percent_update=0,
                                        percent_delete=0, scope="_default", collection="_default",
                                        json_template="Hotel", output=True, username=self.username,
                                        password=self.password)
        task = self.cluster.async_load_gen_docs(self.master, bucket=self.test_bucket, generator=self.gen_create,
                                                use_magma_loader=True)
        task.result()
        self.sleep(10, "Allowing time for the data load to settle")
        index_def = QueryDefinition(index_name=index_name, index_fields=["name"])
        bytes_per_page = {}
        try:
            for padded in (True, False):
                self.set_padded_skiplist_nodes(enabled=padded)
                for index_node in self.get_nodes_from_services_map(service_type="index", get_all_nodes=True):
                    self._kill_all_processes_index(index_node)
                self.sleep(10, "Allowing time for the indexer to restart")
                self.run_cbq_query(index_def.generate_index_create_query(namespace=namespace))
                self.wait_until_specific_index_online(index_name=index_name)
                stats = self.index_rest.get_index_storage_stats()[self.test_bucket][index_name]
                self.assertEqual(stats["MainStore"]["count"], num_docs, "Index item count mismatch")
                bytes_per_page[padded] = {
                    store: stats[store]["memory_size_index"] / (stats[store]["num_pages"] - 1)
                    for store in ("MainStore", "BackStore")}
                self.log.info(f"padded={padded}, bytes per page: {bytes_per_page[padded]}")
                self.run_cbq_query(index_def.generate_index_drop_query(namespace=namespace))
        finally:
            self.set_padded_skiplist_nodes(enabled=True)
        for store in ("MainStore", "BackStore"):
            delta = bytes_per_page[True][store] - bytes_per_page[False][store]
            self.log.info(f"{store}: padded={bytes_per_page[True][store]:.2f}, "
                          f"unpadded={bytes_per_page[False][store]:.2f}, delta={delta:.2f}")
            self.assertTrue(abs(delta - expected_delta) <= tolerance,
                            f"{store} padded-unpadded delta {delta:.2f} bytes/page is not within "
                            f"{expected_delta}±{tolerance}")
