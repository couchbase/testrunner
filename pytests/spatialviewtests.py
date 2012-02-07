import random
from threading import Thread
import unittest
import uuid
import logger
import time

from membase.helper.spatial_helper import SpatialHelper


class SpatialViewTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.helper = SpatialHelper(self, "default")
        self.helper.setup_cluster()


    def tearDown(self):
        self.helper.cleanup_cluster()


    def test_create_multiple_development_spatial(self):
        self.log.info("description : create multiple spatial views without "
                      "running any spatial view query")
        rest = self.helper.rest
        bucket = self.helper.bucket
        prefix = str(uuid.uuid4())
        name = "dev_test_spatial_multiple"

        design_names = ["{0}-{1}-{2}".format(name, i, prefix) \
                             for i in range(0, 5)]
        for design_name in design_names:
            self.helper.create_index_fun(design_name)
            response = rest.get_spatial(bucket, design_name)
            self.assertTrue(response)
            self.assertEquals(response["_id"],
                              "_design/{0}".format(design_name))
            self.log.info(response)

    def test_insert_100_docs_full_verification(self):
        self._test_insert_docs_full_verification(100)

    def test_insert_10k_docs_full_verification(self):
        self._test_insert_docs_full_verification(10000)

    def test_insert_100k_docs_full_verification(self):
        self._test_insert_docs_full_verification(100000)

    # This test fails, but works when time.sleep(1) is added after the
    # self.helper.insert_docs() call
    def test_insert_100_docs(self):
        self._test_insert_docs(100)

    def test_insert_10k_docs(self):
        self._test_insert_docs(10000)

    def test_insert_100k_docs(self):
        self._test_insert_docs(100000)


    # Average case
    def test_insert_15k_delete_10k_docs(self):
        self._test_delete_docs(15000, 10000)

    # Delete all docs
    def test_insert_15k_delete_15k_docs(self):
        self._test_delete_docs(15000, 15000)

    # Delete almost all docs, should lead to a view empty vBuckets
    def test_insert_15k_delete_14980_docs(self):
        self._test_delete_docs(15000, 14980)

    # Delete more docs than there are (leads to deleting documents
    # that doesn't exist)
    def test_insert_10k_delete_12k_docs(self):
        self._test_delete_docs(10000, 12000)


    # Average case
    def test_insert_15k_update_100_docs(self):
        self._test_update_docs(15000, 100)

    # Update large number of docs
    def test_insert_15k_update_12k_docs(self):
        self._test_update_docs(15000, 12000)

    # Update all docs
    def test_insert_15k_update_15k_docs(self):
        self._test_update_docs(15000, 15000)


    def test_get_spatial_during_1_min_load_10k_working_set(self):
        self._test_insert_and_get_spatial_x_mins(1, 10000)

    def test_get_spatial_during_1_min_load_100k_working_set(self):
        self._test_insert_and_get_spatial_x_mins(1, 100000)

    def test_get_spatial_during_5_min_load_10k_working_set(self):
        self._test_insert_and_get_spatial_x_mins(5, 10000)

    def test_get_spatial_during_5_min_load_100k_working_set(self):
        self._test_insert_and_get_spatial_x_mins(5, 100000)

    def test_get_spatial_during_30_min_load_100k_working_set(self):
        self._test_insert_and_get_spatial_x_mins(30, 100000)


    def _test_insert_docs(self, num_of_docs):
        self.log.info("description : create a spatial view on {0} documents"\
                          .format(num_of_docs))
        design_name = "dev_test_insert_docs_{0}"\
            .format(num_of_docs)
        prefix = str(uuid.uuid4())[:7]

        inserted_keys = self._setup_index(design_name, num_of_docs, prefix)
        self.assertEqual(len(inserted_keys), num_of_docs)


    # Does verify the full docs and not only the keys
    def _test_insert_docs_full_verification(self, num_of_docs):
        self.log.info("description : create a spatial view on {0} documents"\
                          .format(num_of_docs))
        design_name = "dev_test_insert_docs_{0}"\
            .format(num_of_docs)
        prefix = str(uuid.uuid4())[:7]

        self.helper.create_index_fun(design_name)
        inserted_docs = self.helper.insert_docs(num_of_docs, prefix,
                                                return_docs=True)
        self.helper.query_index_for_verification(design_name, inserted_docs,
                                                 full_docs=True)


    def _test_delete_docs(self, num_of_docs, num_of_deleted_docs):
        self.log.info("description : create spatial view with {0} docs "
                      " and delete {1} docs".format(num_of_docs,
                                                    num_of_deleted_docs))
        design_name = "dev_test_insert_{0}_delete_{1}_docs"\
            .format(num_of_docs, num_of_deleted_docs)
        prefix = str(uuid.uuid4())[:7]

        inserted_keys = self._setup_index(design_name, num_of_docs, prefix)

        # Delete documents and very that the documents got deleted
        deleted_keys = self.helper.delete_docs(num_of_deleted_docs, prefix)
        results = self.helper.get_results(design_name, 2*num_of_docs)
        result_keys = self.helper.get_keys(results)
        self.assertEqual(len(result_keys), num_of_docs-len(deleted_keys))
        self.helper.verify_result(inserted_keys, deleted_keys + result_keys)


    def _test_update_docs(self, num_of_docs, num_of_updated_docs):
        self.log.info("description : create spatial view with {0} docs "
                      " and update {1} docs".format(num_of_docs,
                                                    num_of_updated_docs))
        design_name = "dev_test_insert_{0}_delete_{1}_docs"\
            .format(num_of_docs, num_of_updated_docs)
        prefix = str(uuid.uuid4())[:7]

        self._setup_index(design_name, num_of_docs, prefix)

        # Update documents and verify that the documents got updated
        updated_keys = self.helper.insert_docs(num_of_updated_docs, prefix,
                                               dict(updated=True))
        results = self.helper.get_results(design_name, 2*num_of_docs)
        result_updated_keys = self._get_updated_docs_keys(results)
        self.assertEqual(len(updated_keys), len(result_updated_keys))
        self.helper.verify_result(updated_keys, result_updated_keys)


    def _test_insert_and_get_spatial_x_mins(self, duration, num_of_docs):
        self.log.info("description : this test will continuously insert data "
                      "and get the spatial view results for {0} minutes")
        design_name = "dev_test_insert_and_get_spatial_{0}_mins"\
            .format(duration)
        prefix = str(uuid.uuid4())[:7]

        self.helper.create_index_fun(design_name)

        self.docs_inserted = []
        self.shutdown_load_data = False
        load_thread = Thread(
            target=self._insert_data_till_stopped,
            args=(num_of_docs, prefix))
        load_thread.start()

        self._get_results_for_x_minutes(design_name, duration)

        self.shutdown_load_data = True
        load_thread.join()

        # self.docs_inserted was set by the insertion thread
        # (_insert_data_till_stopped)
        self.helper.query_index_for_verification(design_name,
                                                 self.docs_inserted)


    # Create the index and insert documents including verififaction that
    # the index contains them
    # Returns the keys of the inserted documents
    def _setup_index(self, design_name, num_of_docs, prefix):
        self.helper.create_index_fun(design_name)
        inserted_keys = self.helper.insert_docs(num_of_docs, prefix)
        self.helper.query_index_for_verification(design_name, inserted_keys)

        return inserted_keys


    # Return the keys for all docs that contain a key called "updated"
    # in the value
    def _get_updated_docs_keys(self, results):
        keys = []
        if results:
            rows = results["rows"]
            for row in rows:
                if "updated" in row["value"]:
                    keys.append(row["id"].encode("ascii", "ignore"))
            self.log.info("{0} documents to updated".format(len(keys)))
        return keys


    def _get_results_for_x_minutes(self, design_name, duration, delay=5):
        random.seed(0)
        start = time.time()
        while (time.time() - start) < duration * 60:
            limit = random.randint(1, 1000)
            self.log.info("{0} seconds has passed ....".format(
                    (time.time() - start)))
            results = self.helper.get_results(design_name, limit)
            keys = self.helper.get_keys(results)
            self.log.info("spatial view returned {0} rows".format(len(keys)))
            time.sleep(delay)

    def _insert_data_till_stopped(self, number_of_docs, prefix):
        while not self.shutdown_load_data:
            # Will be read after the function is terminated
            self.docs_inserted = self.helper.insert_docs(
                number_of_docs, prefix, wait_for_persistence=False)
