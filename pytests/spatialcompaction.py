import unittest
import uuid
import logger

from membase.helper.spatial_helper import SpatialHelper


class SpatialCompactionTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.helper = SpatialHelper(self, "default")
        self.helper.setup_cluster()


    def tearDown(self):
        self.helper.cleanup_cluster()


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

        # Do the compaction
        self.helper.compact(design_name)

        # Check if the index size got smaller
        status, info = self.helper.info(design_name)
        self.assertTrue(info["spatial_index"]["disk_size"] < disk_size,
                        "The file size ({0}) isn't smaller than the "
                        "pre compaction size ({1})."
                        .format(info["spatial_index"]["disk_size"],
                                disk_size))
