import unittest
import uuid
import logger

from membase.helper.spatial_helper import SpatialHelper
from basetestcase import BaseTestCase


class SpatialCompactionTests(BaseTestCase):
    def setUp(self):
        super(SpatialCompactionTests, self).setUp()
        self.start_cluster = self.input.param('start-cluster', len(self.servers))
        self.servers_in = self.input.param('servers_in', 0)
        self.servers_out = self.input.param('servers_out', 0)
        self.bucket_name = "default"
        if self.standard_buckets:
            self.bucket_name = "standard_bucket0"
        if self.sasl_buckets:
            self.bucket_name = "bucket0"
        self.helper = SpatialHelper(self, self.bucket_name)
        try:
            if self.start_cluster > 1:
                rebalance = self.cluster.async_rebalance(self.servers[:1],
                                                         self.servers[1:self.start_cluster], [])
                rebalance.result()
        except:
            super(SpatialCompactionTests, self).tearDown()

    def tearDown(self):
        super(SpatialCompactionTests, self).tearDown()


    def test_spatial_compaction(self):
        self.log.info(
            "description : test manual compaction for spatial indexes")
        prefix = str(uuid.uuid4())[:7]
        design_name = "dev_test_spatial_compaction"

        self.helper.create_index_fun(design_name, prefix)

        # Insert (resp. update, as they have the same prefix) and query
        # the spatial index several time so that the compaction makes sense
        for i in range(0, 8):
            self.helper.insert_docs(2000, prefix)
            self.helper.get_results(design_name)

        # Get the index size prior to compaction
        status, info = self.helper.info(design_name)
        disk_size = info["spatial_index"]["disk_size"]

        if self.servers_in or self.servers_out:
            servs_in = servs_out = []
            if self.servers_in:
                servs_in = self.servers[self.start_cluster:self.servers_in + 1]
            if self.servers_out:
                servs_out = self.servers[-self.servers_out:]
            rebalance = self.cluster.async_rebalance(self.servers, servs_in, servs_out)

        # Do the compaction
        self.helper.compact(design_name)

        # Check if the index size got smaller
        status, info = self.helper.info(design_name)
        self.assertTrue(info["spatial_index"]["disk_size"] < disk_size,
                        "The file size ({0}) isn't smaller than the "
                        "pre compaction size ({1})."
                        .format(info["spatial_index"]["disk_size"],
                                disk_size))
        if self.servers_in or self.servers_out:
            rebalance.result()