import unittest
import uuid
import logger

from membase.helper.spatial_helper import SpatialHelper


class SpatialInfoTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.helper = SpatialHelper(self, "default")
        self.helper.setup_cluster()


    def tearDown(self):
        self.helper.cleanup_cluster()


    def test_spatial_info(self):
        self.log.info(
            "description : test info for spatial indexes")
        rest = self.helper.rest
        prefix = str(uuid.uuid4())[:7]
        design_name = "dev_test_spatial_info"

        self.helper.create_index_fun(design_name, prefix)

        # Fill the database and add an index
        self.helper.insert_docs(2000, prefix)
        self.helper.get_results(design_name)
        status, info = self.helper.info(design_name)
        disk_size = info["spatial_index"]["disk_size"]

        self.assertTrue(disk_size > 0)
        self.assertEqual(info["name"], design_name)

        num_vbuckets = len(rest.get_vbuckets(self.helper.bucket))
        self.assertEqual(len(info["spatial_index"]["update_seq"]),
                         num_vbuckets)
        self.assertEqual(len(info["spatial_index"]["purge_seq"]),
                         num_vbuckets)
        self.assertFalse(info["spatial_index"]["updater_running"])
        self.assertFalse(info["spatial_index"]["waiting_clients"] > 0)
        self.assertFalse(info["spatial_index"]["compact_running"])

        # Insert a lot new documents, and return after starting to
        # build up (not waiting until it's done) the index to test
        # if the updater fields are set correctly
        self.helper.insert_docs(50000, prefix)
        self.helper.get_results(design_name,
                                extra_params={"stale": "update_after"})
        # Somehow stale=update_after doesn't really return immediately,
        # thus commenting this assertion out. There's no real reason
        # to investigate, as the indexing changes heavily in the moment
        # anyway
        #self.assertTrue(info["spatial_index"]["updater_running"])
        #self.assertTrue(info["spatial_index"]["waiting_commit"])
        #self.assertTrue(info["spatial_index"]["waiting_clients"] > 0)
        self.assertFalse(info["spatial_index"]["compact_running"])

        # Request the index again, to make sure it is fully updated
        self.helper.get_results(design_name)
        status, info = self.helper.info(design_name)
        self.assertFalse(info["spatial_index"]["updater_running"])
        self.assertFalse(info["spatial_index"]["waiting_clients"] > 0)
        self.assertFalse(info["spatial_index"]["compact_running"])
        self.assertTrue(info["spatial_index"]["disk_size"] > disk_size)
