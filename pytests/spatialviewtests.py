import random
import threading
from threading import Thread
import unittest
import uuid
import logger
import time
import string

from basetestcase import BaseTestCase
from couchbase.document import DesignDocument, View
from membase.api.rest_client import RestConnection
from membase.helper.spatial_helper import SpatialHelper
from membase.helper.failover_helper import FailoverHelper


class SpatialViewsTests(BaseTestCase):

    def setUp(self):
        super(SpatialViewsTests, self).setUp()
        self.skip_rebalance = self.input.param("skip_rebalance", False)
        self.use_dev_views = self.input.param("use-dev-views", False)
        self.default_map = "function (doc) {emit(doc.geometry, doc.age);}"
        self.default_ddoc_name = self.input.param("default_ddoc_name", "test-ddoc")
        self.default_view_name = self.input.param("default_view_name", "test-view")
        self.bucket_name = "default"
        if self.standard_buckets:
            self.bucket_name = "standard_bucket0"
        if self.sasl_buckets:
            self.bucket_name = "bucket0"
        self.helper = SpatialHelper(self, self.bucket_name)
        if not self.skip_rebalance:
            self.cluster.rebalance(self.servers[:], self.servers[1:], [])
        #load some items to verify
        self.docs = self.helper.insert_docs(self.num_items, 'spatial-doc',
                                            wait_for_persistence=True,
                                            return_docs=True)

    def tearDown(self):
        super(SpatialViewsTests, self).tearDown()

    def test_add_spatial_views(self):
        num_ddoc = self.input.param('num-ddoc', 1)
        views_per_ddoc = self.input.param('views-per-ddoc', 1)
        non_spatial_views_per_ddoc = self.input.param('non-spatial-views-per-ddoc', 0)
        ddocs =  self.make_ddocs(num_ddoc, views_per_ddoc, non_spatial_views_per_ddoc)
        self.create_ddocs(ddocs)

    def test_add_spatial_views_case_sensative(self):
        ddoc = DesignDocument(self.default_ddoc_name, [], spatial_views=[
                                  View(self.default_view_name, self.default_map,
                                       dev_view=self.use_dev_views, is_spatial=True),
                                  View(self.default_view_name.upper(), self.default_map,
                                       dev_view=self.use_dev_views, is_spatial=True)])
        self.create_ddocs([ddoc])

    def test_add_single_spatial_view(self):
        name_lenght = self.input.param('name_lenght', None)
        view_name = self.input.param('view_name', self.default_view_name)
        if name_lenght:
            view_name = ''.join(random.choice(string.lowercase) for x in xrange(name_lenght))
        not_compilable = self.input.param('not_compilable', False)
        error = self.input.param('error', None)
        map_fn = (self.default_map, 'function (doc) {emit(doc.geometry, doc.age);')[not_compilable]

        ddoc = DesignDocument(self.default_ddoc_name, [], spatial_views=[
                                  View(view_name, map_fn,
                                  dev_view=self.use_dev_views, is_spatial=True)])
        try:
            self.create_ddocs([ddoc])
        except Exception as ex:
            if error and ex.message.find(error) != -1:
                self.log.info("Error caught as expected %s" % error)
            self.fail("Unexpected error appeared during run %s" % ex)

    def make_ddocs(self, ddocs_num, views_per_ddoc, non_spatial_views_per_ddoc):
        ddocs = []
        for i in xrange(ddocs_num):
            views = []
            for k in xrange(views_per_ddoc):
                views.append(View(self.default_view_name + str(k), self.default_map,
                                  dev_view=self.use_dev_views, is_spatial=True))
            non_spatial_views = []
            if non_spatial_views_per_ddoc:
                for k in xrange(non_spatial_views_per_ddoc):
                    views.append(View(self.default_view_name + str(k), 'function (doc) { emit(null, doc);}',
                                      dev_view=self.use_dev_views))
            ddocs.append(DesignDocument(self.default_ddoc_name + str(i), non_spatial_views, spatial_views=views))
        return ddocs

    def create_ddocs(self, ddocs):
        for ddoc in ddocs:
            if not (ddoc.views or ddoc.spatial_views):
                self.cluster.create_view(self.master, ddoc.name, [], bucket=self.bucket_name)
            for view in ddoc.views:
                self.cluster.create_view(self.master, ddoc.name, view, bucket=self.bucket_name)
            for view in ddoc.spatial_views:
                self.cluster.create_view(self.master, ddoc.name, view, bucket=self.bucket_name)

class SpatialViewTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.helper = SpatialHelper(self, "default")
        self.helper.setup_cluster()


    def tearDown(self):
        self.helper.cleanup_cluster()


    def test_create_x_design_docs(self):
        num_design_docs = self.helper.input.param("num-design-docs", 5)
        self.log.info("description : create {0} spatial views without "
                      "running any spatial view query".format(num_design_docs))
        prefix = str(uuid.uuid4())

        fun = "function (doc) {emit(doc.geometry, doc);}"
        self._insert_x_design_docs(num_design_docs, prefix, fun)


    def test_update_x_design_docs(self):
        num_design_docs = self.helper.input.param("num-design-docs", 5)
        self.log.info("description : update {0} spatial views without "
                      "running any spatial view query".format(num_design_docs))
        prefix = str(uuid.uuid4())

        fun = "function (doc) {emit(doc.geometry, doc);}"
        self._insert_x_design_docs(num_design_docs, prefix, fun)

        # Update the design docs with a different function
        fun = "function (doc) {emit(doc.geometry, null);}"
        self._insert_x_design_docs(num_design_docs, prefix, fun)


    def _insert_x_design_docs(self, num_design_docs, prefix, fun):
        rest = self.helper.rest
        bucket = self.helper.bucket
        name = "dev_test_multiple_design_docs"

        for i in range(0, num_design_docs):
            design_name = "{0}-{1}-{2}".format(name, i, prefix)
            self.helper.create_index_fun(design_name, prefix, fun)

            # Verify that the function was really stored
            response, meta = rest.get_spatial(bucket, design_name)
            self.assertTrue(response)
            self.assertEquals(meta["id"],
                              "_design/{0}".format(design_name))
            self.assertEquals(
                response["spatial"][design_name].encode("ascii",
                                                                "ignore"),
                fun)


    def test_insert_x_docs(self):
        num_docs = self.helper.input.param("num-docs", 100)
        self.log.info("description : create a spatial view on {0} documents"\
                          .format(num_docs))
        design_name = "dev_test_insert_{0}_docs".format(num_docs)
        self._insert_x_docs_and_query(num_docs, design_name)


    # Does verify the full docs and not only the keys
    def test_insert_x_docs_full_verification(self):
        num_docs = self.helper.input.param("num-docs", 100)
        self.log.info("description : create a spatial view with {0} docs"
                      " and verify the full documents".format(num_docs))
        design_name = "dev_test_insert_{0}_docs_full_verification"\
            .format(num_docs)
        prefix = str(uuid.uuid4())[:7]

        self.helper.create_index_fun(design_name, prefix)
        inserted_docs = self.helper.insert_docs(num_docs, prefix,
                                                return_docs=True)
        self.helper.query_index_for_verification(design_name, inserted_docs,
                                                 full_docs=True)


    def test_insert_x_delete_y_docs(self):
        num_docs = self.helper.input.param("num-docs", 15000)
        num_deleted_docs = self.helper.input.param("num-deleted-docs", 10000)
        self.log.info("description : create spatial view with {0} docs "
                      " and delete {1} docs".format(num_docs,
                                                    num_deleted_docs))
        design_name = "dev_test_insert_{0}_delete_{1}_docs"\
            .format(num_docs, num_deleted_docs)
        prefix = str(uuid.uuid4())[:7]

        inserted_keys = self._setup_index(design_name, num_docs, prefix)

        # Delete documents and verify that the documents got deleted
        deleted_keys = self.helper.delete_docs(num_deleted_docs, prefix)
        num_expected = num_docs - len(deleted_keys)
        results = self.helper.get_results(design_name, 2 * num_docs,
                                          num_expected=num_expected)
        result_keys = self.helper.get_keys(results)
        self.assertEqual(len(result_keys), num_expected)
        self.helper.verify_result(inserted_keys, deleted_keys + result_keys)


    def test_insert_x_update_y_docs(self):
        num_docs = self.helper.input.param("num-docs", 15000)
        num_updated_docs = self.helper.input.param("num-updated-docs", 100)
        self.log.info("description : create spatial view with {0} docs "
                      " and update {1} docs".format(num_docs,
                                                    num_updated_docs))
        design_name = "dev_test_insert_{0}_delete_{1}_docs"\
            .format(num_docs, num_updated_docs)
        prefix = str(uuid.uuid4())[:7]

        self._setup_index(design_name, num_docs, prefix)

        # Update documents and verify that the documents got updated
        updated_keys = self.helper.insert_docs(num_updated_docs, prefix,
                                               dict(updated=True))
        results = self.helper.get_results(design_name, 2 * num_docs)
        result_updated_keys = self._get_updated_docs_keys(results)
        self.assertEqual(len(updated_keys), len(result_updated_keys))
        self.helper.verify_result(updated_keys, result_updated_keys)


    def test_get_spatial_during_x_min_load_y_working_set(self):
        num_docs = self.helper.input.param("num-docs", 10000)
        duration = self.helper.input.param("load-time", 1)
        self.log.info("description : this test will continuously insert data "
                      "and get the spatial view results for {0} minutes")
        design_name = "dev_test_insert_and_get_spatial_{0}_mins"\
            .format(duration)
        prefix = str(uuid.uuid4())[:7]

        self._query_x_mins_during_loading(num_docs, duration, design_name,
                                         prefix)

    def _query_x_mins_during_loading(self, num_docs, duration, design_name, prefix):
        self.helper.create_index_fun(design_name, prefix)

        load_thread = InsertDataTillStopped(self.helper, num_docs, prefix)
        load_thread.start()

        self._get_results_for_x_minutes(design_name, duration)

        load_thread.stop_insertion()
        load_thread.join()

        self.helper.query_index_for_verification(design_name,
                                                 load_thread.inserted())

    def test_get_spatial_during_x_min_load_y_working_set_multiple_design_docs(
        self):
        num_docs = self.helper.input.param("num-docs", 10000)
        num_design_docs = self.helper.input.param("num-design-docs", 10)
        duration = self.helper.input.param("load-time", 1)
        self.log.info("description : will create {0} docs per design doc and "
                      "{1} design docs that will be queried while the data "
                      "is loaded for {2} minutes"
                      .format(num_docs, num_design_docs, duration))
        name = "dev_test_spatial_test_{0}_docs_{1}_design_docs_{2}_mins_load"\
            .format(num_docs, num_design_docs, duration)

        view_test_threads = []
        for i in range(0, num_design_docs):
            prefix = str(uuid.uuid4())[:7]
            design_name = "{0}-{1}-{2}".format(name, i, prefix)
            thread_result = []
            t = Thread(
                target=SpatialViewTests._test_multiple_design_docs_thread_wrapper,
                name="Insert documents and query multiple design docs in parallel",
                args=(self, num_docs, duration, design_name, prefix,
                      thread_result))
            t.start()
            view_test_threads.append((t, thread_result))
        for (t, failures) in view_test_threads:
            t.join()
        for (t, failures) in view_test_threads:
            if len(failures) > 0:
                self.fail("view thread failed : {0}".format(failures[0]))

    def _test_multiple_design_docs_thread_wrapper(self, num_docs, duration,
                                                  design_name, prefix,
                                                  failures):
        try:
            self._query_x_mins_during_loading(num_docs, duration, design_name,
                                              prefix)
        except Exception as ex:
            failures.append(ex)


    def test_spatial_view_on_x_docs_y_design_docs(self):
        num_docs = self.helper.input.param("num-docs", 10000)
        num_design_docs = self.helper.input.param("num-design-docs", 21)
        self.log.info("description : will create {0} docs per design doc and "
                      "{1} design docs that will be queried")
        name = "dev_test_spatial_test_{0}_docs_y_design_docs"\
            .format(num_docs, num_design_docs)
        prefix = str(uuid.uuid4())[:7]

        design_names = ["{0}-{1}-{2}".format(name, i, prefix) \
                            for i in range(0, num_design_docs)]

        view_test_threads = []
        for design_name in design_names:
            thread_result = []
            t = Thread(
                target=SpatialViewTests._test_spatial_view_thread_wrapper,
                name="Insert documents and query in parallel",
                args=(self, num_docs, design_name, thread_result))
            t.start()
            view_test_threads.append((t, thread_result))
        for (t, failures) in view_test_threads:
            t.join()
        for (t, failures) in view_test_threads:
            if len(failures) > 0:
                self.fail("view thread failed : {0}".format(failures[0]))


    def _test_spatial_view_thread_wrapper(self, num_docs, design_name,
                                          failures):
        try:
            self._insert_x_docs_and_query(num_docs, design_name)
        except Exception as ex:
            failures.append(ex)


    # Create the index and insert documents including verififaction that
    # the index contains them
    # Returns the keys of the inserted documents
    def _setup_index(self, design_name, num_docs, prefix):
        self.helper.create_index_fun(design_name, prefix)
        inserted_keys = self.helper.insert_docs(num_docs, prefix)
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


    def _insert_x_docs_and_query(self, num_docs, design_name):
        prefix = str(uuid.uuid4())[:7]

        inserted_keys = self._setup_index(design_name, num_docs, prefix)
        self.assertEqual(len(inserted_keys), num_docs)


    def test_x_docs_failover(self):
        num_docs = self.helper.input.param("num-docs", 10000)
        self.log.info("description : test failover with {0} documents"\
                          .format(num_docs))
        design_name = "dev_test_failover_{0}".format(num_docs)
        prefix = str(uuid.uuid4())[:7]
        inserted_keys = self._setup_index(design_name, num_docs, prefix)
        try:
            fh = FailoverHelper(self.helper.servers, self)

            failover_nodes = fh.failover(1)
            self.helper.query_index_for_verification(design_name, inserted_keys,
                                                 wait_for_persistence=False)

            # The test cleanup expects all nodes running, hence spin the
            # full cluster up again
            fh.undo_failover(failover_nodes)
        finally:
            fh._start_servers(failover_nodes)


    def test_update_view_x_docs(self):
        num_docs = self.helper.input.param("num-docs", 100)
        self.log.info("description : create a spatial view on {0} documents "
                      "and update the view so that it returns only a subset"\
                          .format(num_docs))
        design_name = "dev_test_update_view_{0}_docs".format(num_docs)
        prefix = str(uuid.uuid4())[:7]

        # Create an index that emits all documents
        self.helper.create_index_fun(design_name, prefix)
        keys_b = self.helper.insert_docs(num_docs / 3, prefix + "bbb")
        keys_c = self.helper.insert_docs(num_docs - (num_docs / 3), prefix + "ccc")
        self.helper.query_index_for_verification(design_name, keys_b + keys_c)

        # Update index to only a subset of the documents
        self.helper.create_index_fun(design_name, prefix + "ccc")
        self.helper.query_index_for_verification(design_name, keys_c)


    def test_compare_views_all_nodes_x_docs(self):
        num_docs = self.helper.input.param("num-docs", 100)
        self.log.info("description : creates view on {0} documents, queries "
                      "all nodes (not only the master node) and compares "
                      "if the results are all the same"\
                          .format(num_docs))
        design_name = "dev_test_compare_views_{0}_docs".format(num_docs)
        prefix = str(uuid.uuid4())[:7]

        inserted_keys = self._setup_index(design_name, num_docs, prefix)

        nodes = self.helper.rest.get_nodes()
        params = {"connection_timeout": 60000, "full_set": True}

        # Query every single node and verify
        for n in nodes:
            n_rest = RestConnection({
                    "ip": n.ip,
                    "port": n.port,
                    "username": self.helper.master.rest_username,
                    "password": self.helper.master.rest_password})
            results = n_rest.spatial_results(self.helper.bucket, design_name,
                                             params, None)
            result_keys = self.helper.get_keys(results)
            self.helper.verify_result(inserted_keys, result_keys)



class InsertDataTillStopped(threading.Thread):
    def __init__(self, helper, num_docs, prefix):
        threading.Thread.__init__(self)
        self._helper = helper
        self._num_docs = num_docs
        self._prefix = prefix
        self._stop_insertion = False
        self._last_inserted = []

    def run(self):
        i = 0
        while not self._stop_insertion:
            i += 1
            prefix = self._prefix + "-{0}".format(i)
            self._last_inserted = self._helper.insert_docs(
                self._num_docs, prefix, wait_for_persistence=False)

    def stop_insertion(self):
        self._stop_insertion = True

    # Return the last inserted set of docs
    def inserted(self):
        return self._last_inserted
