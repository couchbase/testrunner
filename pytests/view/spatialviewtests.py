import random
import threading
from threading import Thread, Event
import unittest
import uuid
import logger
import time
import string

from basetestcase import BaseTestCase
from couchbase_helper.document import DesignDocument, View
from membase.api.rest_client import RestConnection
from membase.helper.spatial_helper import SpatialHelper
from membase.helper.rebalance_helper import RebalanceHelper
from remote.remote_util import RemoteMachineShellConnection


class SpatialViewsTests(BaseTestCase):

    def setUp(self):
        super(SpatialViewsTests, self).setUp()
        self.thread_crashed = Event()
        self.thread_stopped = Event()
        self.skip_rebalance = self.input.param("skip_rebalance", False)
        self.use_dev_views = self.input.param("use-dev-views", False)
        self.default_map = "function (doc) {emit(doc.geometry, doc.age);}"
        self.map_updated = "function (doc) {emit(doc.geometry, doc.name);}"
        self.default_ddoc_name = self.input.param("default_ddoc_name", "test-ddoc")
        self.default_view_name = self.input.param("default_view_name", "test-view")
        self.ddoc_op = self.input.param("ddoc-ops", "create") #create\update\delete
        self.bucket_name = "default"
        if self.standard_buckets:
            self.bucket_name = "standard_bucket0"
        self.helper = SpatialHelper(self, self.bucket_name)
        if not self.skip_rebalance:
            self.cluster.rebalance(self.servers[:], self.servers[1:], [])
        #load some items to verify
        self.docs = self.helper.insert_docs(self.num_items, 'spatial-doc',
                                            return_docs=True)
        self.num_ddoc = self.input.param('num-ddoc', 1)
        self.views_per_ddoc = self.input.param('views-per-ddoc', 1)
        self.non_spatial_views_per_ddoc = self.input.param('non-spatial-views-per-ddoc', 0)
        if self.ddoc_op == 'update' or self.ddoc_op == 'delete':
            ddocs =  self.make_ddocs(self.num_ddoc, self.views_per_ddoc,
                                     self.non_spatial_views_per_ddoc)
            self.create_ddocs(ddocs)

    def tearDown(self):
        super(SpatialViewsTests, self).tearDown()

    def test_add_spatial_views(self):
        ddocs =  self.make_ddocs(self.num_ddoc, self.views_per_ddoc, self.non_spatial_views_per_ddoc)
        self.perform_ddoc_ops(ddocs)

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
            view_name = ''.join(random.choice(string.ascii_lowercase) for x in range(name_lenght))
        not_compilable = self.input.param('not_compilable', False)
        error = self.input.param('error', None)
        map_fn = (self.default_map, 'function (doc) {emit(doc.geometry, doc.age);')[not_compilable]

        ddoc = DesignDocument(self.default_ddoc_name, [], spatial_views=[
                                  View(view_name, map_fn,
                                  dev_view=self.use_dev_views, is_spatial=True)])
        try:
            self.create_ddocs([ddoc])
        except Exception as ex:
            if error and str(ex).find(error) != -1:
                self.log.info("Error caught as expected %s" % error)
                return
            else:
                self.fail("Unexpected error appeared during run %s" % ex)
        if error:
                self.fail("Expected error '%s' didn't appear" % error)

    def test_add_views_to_1_ddoc(self):
        same_names = self.input.param('same-name', False)
        error = self.input.param('error', None)
        num_views_per_ddoc = 10
        create_threads = []
        try:
            for i in range(num_views_per_ddoc):
                ddoc = DesignDocument(self.default_ddoc_name, [], spatial_views=[
                                      View(self.default_view_name + (str(i), "")[same_names],
                                           self.default_map,
                                           dev_view=self.use_dev_views, is_spatial=True)])
                create_thread = Thread(target=self.create_ddocs,
                                       name="create_thread" + str(i),
                                       args=([ddoc,],))
                create_threads.append(create_thread)
                create_thread.start()
            for create_thread in create_threads:
                create_thread.join()
        except Exception as ex:
            if error and str(ex).find(error) != -1:
               self.log.info("Error caught as expected %s" % error)
               return
            else:
               self.fail("Unexpected error appeared during run %s" % ex)
        if error:
            self.fail("Expected error '%s' didn't appear" % error)

    def test_add_spatial_views_threads(self):
        same_names = self.input.param('same-name', False)
        num_views_per_ddoc = 10
        create_threads = []
        ddocs = []
        for i in range(num_views_per_ddoc):
            ddoc = DesignDocument(self.default_ddoc_name + str(i), [], spatial_views=[
                                  View(self.default_view_name + (str(i), "")[same_names],
                                       self.default_map,
                                       dev_view=self.use_dev_views, is_spatial=True)])
            ddocs.append(ddoc)
        if self.ddoc_op == 'update' or self.ddoc_op == 'delete':
            self.create_ddocs(ddocs)
        i = 0
        for ddoc in ddocs:
            create_thread = Thread(target=self.perform_ddoc_ops,
                                   name="ops_thread" + str(i),
                                   args=([ddoc,],))
            i +=1
            create_threads.append(create_thread)
            create_thread.start()
        for create_thread in create_threads:
            create_thread.join()
        if self.thread_crashed.is_set():
            self.fail("Error occured during run")

    def test_create_with_other_ddoc_ops(self):
        operation = self.input.param('operation', 'create')
        ddocs =  self.make_ddocs(self.num_ddoc, self.views_per_ddoc, 0)
        other_ddocs = self.make_ddocs(self.num_ddoc, 0, self.views_per_ddoc)
        if operation == 'delete' or operation == 'update':
            self.create_ddocs(other_ddocs)
        other_ddoc_threads = []
        for ddoc in other_ddocs:
            if operation == 'create' or operation == 'update':
                other_ddoc_thread = Thread(target=self.create_ddocs,
                                           name="other_doc_thread",
                                           args=(other_ddocs,))
            else:
                other_ddoc_thread = Thread(target=self.delete_views,
                                           name="other_doc_thread",
                                           args=(other_ddocs,))
            other_ddoc_threads.append(other_ddoc_thread)
            other_ddoc_thread.start()
        self.perform_ddoc_ops(ddocs)
        for thread in other_ddoc_threads:
            thread.join()

    def test_create_views_during_rebalance(self):
        start_cluster = self.input.param('start-cluster', 1)
        servers_in = self.input.param('servers_in', 0)
        servers_out = self.input.param('servers_out', 0)
        ddocs =  self.make_ddocs(self.num_ddoc, self.views_per_ddoc, self.non_spatial_views_per_ddoc)
        if start_cluster > 1:
            rebalance = self.cluster.async_rebalance(self.servers[:1],
                                                     self.servers[1:start_cluster], [])
            rebalance.result()
        servs_in = []
        servs_out = []
        if servers_in:
            servs_in = self.servers[start_cluster:servers_in + 1]
        if servers_out:
            if start_cluster > 1:
                servs_out = self.servers[1:start_cluster]
                servs_out = servs_out[-servers_out:]
            else:
                servs_out = self.servers[-servers_out:]
        rebalance_thread = Thread(target=self.cluster.rebalance,
                                           name="reb_thread",
                                           args=(self.servers[:1], servs_in, servs_out))
        rebalance_thread.start()
        self.perform_ddoc_ops(ddocs)
        rebalance_thread.join()

    def test_views_node_pending_state(self):
        operation = self.input.param('operation', 'add_node')
        ddocs =  self.make_ddocs(self.num_ddoc, self.views_per_ddoc, 0)
        rest = RestConnection(self.master)
        if operation == 'add_node':
            self.log.info("adding the node %s:%s" % (
                        self.servers[1].ip, self.servers[1].port))
            otpNode = rest.add_node(self.master.rest_username, self.master.rest_password,
                                    self.servers[1].ip, self.servers[1].port)
        elif operation == 'failover':
            nodes = rest.node_statuses()
            nodes = [node for node in nodes
                     if node.ip != self.master.ip or node.port != self.master.port]
            rest.fail_over(nodes[0].id)
        else:
            self.fail("There is no operation %s" % operation)
        self.perform_ddoc_ops(ddocs)

    def test_views_failover(self):
        num_nodes = self.input.param('num-nodes', 1)
        ddocs =  self.make_ddocs(self.num_ddoc, self.views_per_ddoc, 0)
        RebalanceHelper.wait_for_persistence(self.master, self.bucket_name)
        self.cluster.failover(self.servers,
                              self.servers[1:num_nodes])
        self.cluster.rebalance(self.servers, [], self.servers[1:num_nodes])
        self.perform_ddoc_ops(ddocs)

    def test_views_with_warm_up(self):
        warmup_node = self.servers[-1]
        shell = RemoteMachineShellConnection(warmup_node)
        shell.stop_couchbase()
        time.sleep(20)
        shell.start_couchbase()
        shell.disconnect()
        ddocs =  self.make_ddocs(self.num_ddoc, self.views_per_ddoc, 0)
        self.perform_ddoc_ops(ddocs)

    def test_views_during_index(self):
        ddocs =  self.make_ddocs(1, 1, 1)
        self.create_ddocs(ddocs)
        #run query stale=false to start index
        rest = RestConnection(self.master)
        for ddoc in ddocs:
            for view in ddoc.spatial_views:
                self.helper.query_view(rest, ddoc, view, bucket=self.bucket_name, extra_params={})
        ddocs =  self.make_ddocs(self.num_ddoc, self.views_per_ddoc, 1)
        self.perform_ddoc_ops(ddocs)

    def test_views_during_ddoc_compaction(self):
        fragmentation_value = self.input.param("fragmentation_value", 80)
        ddoc_to_compact = DesignDocument("ddoc_to_compact", [], spatial_views=[
                                  View(self.default_view_name,
                                       'function (doc) { emit(doc.age, doc.name);}',
                                       dev_view=self.use_dev_views)])
        ddocs =  self.make_ddocs(self.num_ddoc, self.views_per_ddoc, 0)
        self.disable_compaction()
        self.create_ddocs([ddoc_to_compact,])
        fragmentation_monitor = self.cluster.async_monitor_view_fragmentation(self.master,
                             ddoc_to_compact.name, fragmentation_value, self.default_bucket_name)
        end_time = time.time() + self.wait_timeout * 30
        while fragmentation_monitor.state != "FINISHED" and end_time > time.time():
            self.helper.insert_docs(self.num_items, 'spatial-doc')

        if end_time < time.time() and fragmentation_monitor.state != "FINISHED":
            self.fail("impossible to reach compaction value after %s sec" % (self.wait_timeout * 20))
        fragmentation_monitor.result()
        compaction_task = self.cluster.async_compact_view(self.master, ddoc_to_compact.name,
                                                          self.default_bucket_name)
        self.perform_ddoc_ops(ddocs)
        result = compaction_task.result(self.wait_timeout * 10)
        self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")

    def make_ddocs(self, ddocs_num, views_per_ddoc, non_spatial_views_per_ddoc):
        ddocs = []
        for i in range(ddocs_num):
            views = []
            for k in range(views_per_ddoc):
                views.append(View(self.default_view_name + str(k), self.default_map,
                                  dev_view=self.use_dev_views, is_spatial=True))
            non_spatial_views = []
            if non_spatial_views_per_ddoc:
                for k in range(non_spatial_views_per_ddoc):
                    non_spatial_views.append(View(self.default_view_name + str(k), 'function (doc) { emit(null, doc);}',
                                      dev_view=self.use_dev_views))
            ddocs.append(DesignDocument(self.default_ddoc_name + str(i), non_spatial_views, spatial_views=views))
        return ddocs

    def create_ddocs(self, ddocs, bucket=None):
        bucket_views = bucket or self.buckets[0]
        for ddoc in ddocs:
            if not (ddoc.views or ddoc.spatial_views):
                self.cluster.create_view(self.master, ddoc.name, [], bucket=bucket_views)
            for view in ddoc.views:
                self.cluster.create_view(self.master, ddoc.name, view, bucket=bucket_views)
            for view in ddoc.spatial_views:
                self.cluster.create_view(self.master, ddoc.name, view, bucket=bucket_views)

    def delete_views(self, ddocs, views=[], spatial_views=[], bucket=None):
        bucket_views = bucket or self.buckets[0]
        for ddoc in ddocs:
            vs = views or ddoc.views
            sp_vs = spatial_views or ddoc.spatial_views
            for view in vs:
                self.cluster.delete_view(self.master, ddoc.name, view, bucket=bucket_views)
            for view in sp_vs:
                self.cluster.delete_view(self.master, ddoc.name, view, bucket=bucket_views)

    def perform_ddoc_ops(self, ddocs):
        try:
            if self.ddoc_op == 'update':
                for ddoc in ddocs:
                    for view in ddoc.spatial_views:
                        view.map_func = self.map_updated
            if self.ddoc_op == 'delete':
                self.delete_views(ddocs)
            else:
                self.create_ddocs(ddocs)
        except Exception as ex:
            self.thread_crashed.set()
            self.log.error("****ERROR***** \n At least one of threads is crashed: %s" % (ex))
            raise ex
        finally:
            if not self.thread_stopped.is_set():
                self.thread_stopped.set()




class SpatialViewQueriesTests(BaseTestCase):

    def setUp(self):
        self.helper = SpatialHelper(self, self.bucket_name)
        super(SpatialViewQueriesTests, self).setUp()
        self.thread_crashed = Event()
        self.thread_stopped = Event()
        self.skip_rebalance = self.input.param("skip_rebalance", False)
        self.use_dev_views = self.input.param("use-dev-views", False)
        self.all_view_one_ddoc = self.input.param("all-view-one-ddoc", False)
        self.default_ddoc_name = "test-ddoc-query"
        self.default_view_name = "test-view-query"
        self.params = self.get_query_params()
        self.bucket_name = "default"
        if self.standard_buckets:
            self.bucket_name = "standard_bucket0"

        if not self.skip_rebalance:
            self.cluster.rebalance(self.servers[:], self.servers[1:], [])
        #load some items to verify
        self.docs = self.helper.insert_docs(self.num_items, 'spatial-doc',
                                            return_docs=True)
        self.ddocs = self.helper.create_default_views(
                                        is_one_ddoc=self.all_view_one_ddoc)

    def tearDown(self):
        super(SpatialViewQueriesTests, self).tearDown()

    def test_spatial_view_queries(self):
        error = self.input.param('error', None)
        try:
            self.query_and_verify_result(self.docs, self.params)
        except Exception as ex:
            if error and str(ex).find(error) != -1:
               self.log.info("Error caught as expected %s" % error)
               return
            else:
               self.fail("Unexpected error appeared during run %s" % ex)
        if error:
            self.fail("Expected error '%s' didn't appear" % error)

    def test_add_spatial_view_queries_threads(self):
        diff_nodes = self.input.param("diff-nodes", False)
        query_threads = []
        for i in range(len(self.servers)):
            node = (self.master, self.servers[i])[diff_nodes]
            self.query_and_verify_result(self.docs, self.params, node=node)
            q_thread = Thread(target=self.query_and_verify_result,
                                   name="query_thread" + str(i),
                                   args=([self.docs, self.params, node]))
            query_threads.append(q_thread)
            q_thread.start()
        for q_thread in query_threads:
            q_thread.join()
        if self.thread_crashed.is_set():
            self.fail("Error occured during run")

    def test_view_queries_during_rebalance(self):
        start_cluster = self.input.param('start-cluster', 1)
        servers_in = self.input.param('servers_in', 0)
        servers_out = self.input.param('servers_out', 0)
        if start_cluster > 1:
            rebalance = self.cluster.async_rebalance(self.servers[:1],
                                                     self.servers[1:start_cluster], [])
            rebalance.result()
        servs_in = []
        servs_out = []
        if servers_in:
            servs_in = self.servers[start_cluster:servers_in + 1]
        if servers_out:
            if start_cluster > 1:
                servs_out = self.servers[1:start_cluster]
                servs_out = servs_out[-servers_out:]
            else:
                servs_out = self.servers[-servers_out:]
        rebalance = self.cluster.async_rebalance(self.servers, servs_in, servs_out)
        self.query_and_verify_result(self.docs, self.params)
        rebalance.result()

    def test_view_queries_node_pending_state(self):
        operation = self.input.param('operation', 'add_node')
        rest = RestConnection(self.master)
        if operation == 'add_node':
            self.log.info("adding the node %s:%s" % (
                        self.servers[1].ip, self.servers[1].port))
            otpNode = rest.add_node(self.master.rest_username, self.master.rest_password,
                                    self.servers[1].ip, self.servers[1].port)
        elif operation == 'failover':
            nodes = rest.node_statuses()
            nodes = [node for node in nodes
                     if node.ip != self.master.ip or node.port != self.master.port]
            rest.fail_over(nodes[0].id)
        else:
            self.fail("There is no operation %s" % operation)
        self.query_and_verify_result(self.docs, self.params)

    def test_view_queries_failover(self):
        num_nodes = self.input.param('num-nodes', 1)
        self.cluster.failover(self.servers,
                              self.servers[1:num_nodes])
        self.cluster.rebalance(self.servers, [], self.servers[1:num_nodes])
        self.query_and_verify_result(self.docs, self.params)

    def test_views_with_warm_up(self):
         warmup_node = self.servers[-1]
         shell = RemoteMachineShellConnection(warmup_node)
         shell.stop_couchbase()
         time.sleep(20)
         shell.start_couchbase()
         shell.disconnect()
         self.query_and_verify_result(self.docs, self.params)

    def test_view_queries_during_ddoc_compaction(self):
        fragmentation_value = self.input.param("fragmentation_value", 80)
        self.disable_compaction()
        fragmentation_monitor = self.cluster.async_monitor_view_fragmentation(self.master,
                             self.ddocs[0].name, fragmentation_value, self.default_bucket_name)
        end_time = time.time() + self.wait_timeout * 30
        while fragmentation_monitor.state != "FINISHED" and end_time > time.time():
            self.docs = self.helper.insert_docs(self.num_items, 'spatial-doc',
                                                return_docs=True)

        if end_time < time.time() and fragmentation_monitor.state != "FINISHED":
            self.fail("impossible to reach compaction value after %s sec" % (self.wait_timeout * 20))
        fragmentation_monitor.result()
        compaction_task = self.cluster.async_compact_view(self.master, self.ddocs[0].name,
                                                          self.default_bucket_name)
        self.query_and_verify_result(self.docs, self.params)
        result = compaction_task.result(self.wait_timeout * 10)
        self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")

    def get_query_params(self):
        current_params = {}
        for key in self.input.test_params:
            if key == 'skip' or key == 'limit':
                current_params[key] = int(self.input.test_params[key])
            elif key == 'bbox':
                current_params[key] = [int(x) for x in
                                       self.input.test_params[key][1:-1].split(",")]
            elif key == 'stale':
                current_params[key] = self.input.test_params[key]
        return current_params

    def query_and_verify_result(self, doc_inserted, params, node=None):
        try:
            rest = RestConnection(self.master)
            if node:
                rest = RestConnection(node)
            expected_ddocs = self.helper.generate_matching_docs(doc_inserted, params)
            for ddoc in self.ddocs:
                for view in ddoc.spatial_views:
                    result_ddocs = self.helper.query_view(rest, ddoc, view,
                                                          bucket=self.bucket_name,
                                                          extra_params=params,
                                                          num_expected=len(expected_ddocs),
                                                          num_tries=20)
                    self.helper.verify_matching_keys(expected_ddocs, result_ddocs)
        except Exception as ex:
            self.thread_crashed.set()
            self.log.error("****ERROR***** \n At least one of threads is crashed: %s" % (ex))
            raise ex
        finally:
            if not self.thread_stopped.is_set():
                self.thread_stopped.set()

class SpatialViewTests(BaseTestCase):
    def setUp(self):
        self.helper = SpatialHelper(self, "default")
        super(SpatialViewTests, self).setUp()
        self.log = logger.Logger.get_logger()

        self.helper.setup_cluster()

    def tearDown(self):
        super(SpatialViewTests, self).tearDown()

    def test_create_x_design_docs(self):
        num_design_docs = self.helper.input.param("num-design-docs")
        self.log.info("description : create {0} spatial views without "
                      "running any spatial view query".format(num_design_docs))

        fun = "function (doc) {emit(doc.geometry, doc);}"
        self._insert_x_design_docs(num_design_docs, fun)


    def test_update_x_design_docs(self):
        num_design_docs = self.helper.input.param("num-design-docs")
        self.log.info("description : update {0} spatial views without "
                      "running any spatial view query".format(num_design_docs))

        fun = "function (doc) {emit(doc.geometry, doc);}"
        self._insert_x_design_docs(num_design_docs, fun)

        # Update the design docs with a different function
        fun = "function (doc) {emit(doc.geometry, null);}"
        self._insert_x_design_docs(num_design_docs, fun)


    def _insert_x_design_docs(self, num_design_docs, fun):
        rest = self.helper.rest
        bucket = self.helper.bucket
        name = "dev_test_multiple_design_docs"

        for i in range(0, num_design_docs):
            design_name = "{0}-{1}".format(name, i)
            self.helper.create_index_fun(design_name, fun)

            # Verify that the function was really stored
            response, meta = rest.get_spatial(bucket, design_name)
            self.assertTrue(response)
            self.assertEqual(meta["id"],
                              "_design/{0}".format(design_name))
            self.assertEqual(response["spatial"][design_name], fun)

    def test_insert_x_docs(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : create a spatial view on {0} documents"\
                          .format(num_docs))
        design_name = "dev_test_insert_{0}_docs".format(num_docs)
        self._insert_x_docs_and_query(num_docs, design_name)


    # Does verify the full docs and not only the keys
    def test_insert_x_docs_full_verification(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : create a spatial view with {0} docs"
                      " and verify the full documents".format(num_docs))
        design_name = "dev_test_insert_{0}_docs_full_verification"\
            .format(num_docs)

        self.helper.create_index_fun(design_name)
        inserted_docs = self.helper.insert_docs(num_docs, return_docs=True)
        self.helper.query_index_for_verification(design_name, inserted_docs,
                                                 full_docs=True)


    def test_insert_x_delete_y_docs(self):
        num_docs = self.helper.input.param("num-docs")
        num_deleted_docs = self.helper.input.param("num-deleted-docs")
        self.log.info("description : create spatial view with {0} docs "
                      " and delete {1} docs".format(num_docs,
                                                    num_deleted_docs))
        design_name = "dev_test_insert_{0}_delete_{1}_docs"\
            .format(num_docs, num_deleted_docs)

        inserted_keys = self._setup_index(design_name, num_docs)

        # Delete documents and verify that the documents got deleted
        deleted_keys = self.helper.delete_docs(num_deleted_docs)
        num_expected = num_docs - len(deleted_keys)
        results = self.helper.get_results(design_name, 2 * num_docs,
                                          num_expected=num_expected)
        result_keys = self.helper.get_keys(results)
        self.assertEqual(len(result_keys), num_expected)
        self.helper.verify_result(inserted_keys, deleted_keys + result_keys)


    def test_insert_x_update_y_docs(self):
        num_docs = self.helper.input.param("num-docs")
        num_updated_docs = self.helper.input.param("num-updated-docs")
        self.log.info("description : create spatial view with {0} docs "
                      " and update {1} docs".format(num_docs,
                                                    num_updated_docs))
        design_name = "dev_test_insert_{0}_delete_{1}_docs"\
            .format(num_docs, num_updated_docs)

        self._setup_index(design_name, num_docs)

        # Update documents and verify that the documents got updated
        updated_keys = self.helper.insert_docs(num_updated_docs,
                                               extra_values=dict(updated=True))
        results = self.helper.get_results(design_name, 2 * num_docs)
        result_updated_keys = self._get_updated_docs_keys(results)
        self.assertEqual(len(updated_keys), len(result_updated_keys))
        self.helper.verify_result(updated_keys, result_updated_keys)


    def test_get_spatial_during_x_min_load_y_working_set(self):
        num_docs = self.helper.input.param("num-docs")
        duration = self.helper.input.param("load-time")
        self.log.info("description : this test will continuously insert data "
                      "and get the spatial view results for {0} minutes")
        design_name = "dev_test_insert_and_get_spatial_{0}_mins"\
            .format(duration)

        self._query_x_mins_during_loading(num_docs, duration, design_name)

    def _query_x_mins_during_loading(self, num_docs, duration, design_name):
        self.helper.create_index_fun(design_name)

        load_thread = InsertDataTillStopped(self.helper, num_docs)
        load_thread.start()

        self._get_results_for_x_minutes(design_name, duration)

        load_thread.stop_insertion()
        load_thread.join()

        self.helper.query_index_for_verification(design_name,
                                                 load_thread.inserted())

    def test_get_spatial_during_x_min_load_y_working_set_multiple_design_docs(
        self):
        num_docs = self.helper.input.param("num-docs")
        num_design_docs = self.helper.input.param("num-design-docs")
        duration = self.helper.input.param("load-time")
        self.log.info("description : will create {0} docs per design doc and "
                      "{1} design docs that will be queried while the data "
                      "is loaded for {2} minutes"
                      .format(num_docs, num_design_docs, duration))
        name = "dev_test_spatial_test_{0}_docs_{1}_design_docs_{2}_mins_load"\
            .format(num_docs, num_design_docs, duration)

        view_test_threads = []
        for i in range(0, num_design_docs):
            design_name = "{0}-{1}".format(name, i)
            thread_result = []
            t = Thread(
                target=SpatialViewTests._test_multiple_design_docs_thread_wrapper,
                name="Insert documents and query multiple design docs in parallel",
                args=(self, num_docs, duration, design_name, thread_result))
            t.start()
            view_test_threads.append((t, thread_result))
        for (t, failures) in view_test_threads:
            t.join()
        for (t, failures) in view_test_threads:
            if len(failures) > 0:
                self.fail("view thread failed : {0}".format(failures[0]))

    def _test_multiple_design_docs_thread_wrapper(self, num_docs, duration,
                                                  design_name, failures):
        try:
            self._query_x_mins_during_loading(num_docs, duration, design_name)
        except Exception as ex:
            failures.append(ex)


    def test_spatial_view_on_x_docs_y_design_docs(self):
        num_docs = self.helper.input.param("num-docs")
        num_design_docs = self.helper.input.param("num-design-docs")
        self.log.info("description : will create {0} docs per design doc and "
                      "{1} design docs that will be queried")
        name = "dev_test_spatial_test_{0}_docs_y_design_docs"\
            .format(num_docs, num_design_docs)

        design_names = ["{0}-{1}".format(name, i) \
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
    def _setup_index(self, design_name, num_docs):
        self.helper.create_index_fun(design_name)
        inserted_keys = self.helper.insert_docs(num_docs)
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
                    keys.append(row["id"])
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
        inserted_keys = self._setup_index(design_name, num_docs)
        self.assertEqual(len(inserted_keys), num_docs)


    def test_update_view_x_docs(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : create a spatial view on {0} documents "
                      "and update the view so that it returns only a subset"\
                          .format(num_docs))
        design_name = "dev_test_update_view_{0}_docs".format(num_docs)

        # Create an index that emits all documents
        self.helper.create_index_fun(design_name)
        keys_b = self.helper.insert_docs(num_docs // 3, "bbb")
        keys_c = self.helper.insert_docs(num_docs - (num_docs // 3), "ccc")
        self.helper.query_index_for_verification(design_name, keys_b + keys_c)

        # Update index to only a subset of the documents
        spatial_fun = ('function (doc, meta) {'
                       'if(meta.id.indexOf("ccc") != -1) {'
                       'emit(doc.geometry, doc);}}')
        self.helper.create_index_fun(design_name, spatial_fun)
        self.helper.query_index_for_verification(design_name, keys_c)


    def test_compare_views_all_nodes_x_docs(self):
        num_docs = self.helper.input.param("num-docs")
        self.log.info("description : creates view on {0} documents, queries "
                      "all nodes (not only the master node) and compares "
                      "if the results are all the same"\
                          .format(num_docs))
        design_name = "dev_test_compare_views_{0}_docs".format(num_docs)

        inserted_keys = self._setup_index(design_name, num_docs)

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
    def __init__(self, helper, num_docs):
        threading.Thread.__init__(self)
        self._helper = helper
        self._num_docs = num_docs
        self._stop_insertion = False
        self._last_inserted = []

    def run(self):
        i = 0
        while not self._stop_insertion:
            i += 1
            self._last_inserted = self._helper.insert_docs(
                self._num_docs)

    def stop_insertion(self):
        self._stop_insertion = True

    # Return the last inserted set of docs
    def inserted(self):
        return self._last_inserted
