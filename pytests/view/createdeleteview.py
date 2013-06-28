import json
import time
from view_base import ViewBaseTest
from couchbase.document import DesignDocument, View
from membase.api.rest_client import RestConnection
from membase.helper.rebalance_helper import RebalanceHelper
from membase.api.exception import ReadDocumentException
from membase.api.exception import DesignDocCreationException
from couchbase.documentgenerator import DocumentGenerator

class CreateDeleteViewTests(ViewBaseTest):

    def setUp(self):
        super(CreateDeleteViewTests, self).setUp()
        self.bucket_ddoc_map = {}
        self.ddoc_ops = self.input.param("ddoc_ops", None)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        self.test_with_view = self.input.param("test_with_view", False)
        self.num_views_per_ddoc = self.input.param("num_views_per_ddoc", 1)
        self.num_ddocs = self.input.param("num_ddocs", 1)
        self.gen = None
        self.default_design_doc_name = "Doc1"
        self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
        self.updated_map_func = 'function (doc) { emit(null, doc);}'
        self.default_view = View("View", self.default_map_func, None, False)
        self.fragmentation_value = self.input.param("fragmentation_value", 80)

    def tearDown(self):
        super(CreateDeleteViewTests, self).tearDown()

    def _get_complex_map(self, get_compile):
        map_fun = 'function(multiline){ '
        multi_line = 'if(doc.age >0) \
                            emit(doc.age) \
                            if(doc.age == 0) \
                            emit(doc.null,doc.null) \
                            if(doc.name.length >0) \
                            emit(doc.name,doc.age) \
                            if(doc.name.length == 0) \
                            emit(doc.null,doc.null)' * 30
        map_fun = map_fun + multi_line
        if get_compile:
            map_fun = map_fun + '}'
        return map_fun

    def _create_multiple_ddocs_name(self, num_ddocs):
        design_docs = []
        for i in range(0, num_ddocs):
            design_docs.append(self.default_design_doc_name + str(i))
        return design_docs

    """Synchronously execute create/update/delete operations on a bucket and
    create an internal dictionary of the objects created. For update/delete operation,
    number of ddocs/views to be updated/deleted with be taken from sequentially from the position specified by start_pos_for_mutation.

    Parameters:
        bucket - The name of the bucket on which to execute the operations. (String)
        ddoc_op_type - Operation Type (create/update/delete). (String)
        test_with_view - If operations need to be executed on views. (Boolean)
        num_ddocs - Number of Design Documents to be created/updated/deleted. (Number)
        num_views_per_ddoc - Number of Views per DDoc to be created/updated/deleted. (Number)
        prefix_ddoc - Prefix for the DDoc name. (String)
        prefix_view - Prefix of the View name. (String)
        start_pos_for_mutation=0 - Start index for the update/delete operation

    Returns:
        None"""

    def _execute_ddoc_ops(self, ddoc_op_type, test_with_view, num_ddocs,
                          num_views_per_ddoc, prefix_ddoc="dev_ddoc",
                          prefix_view="views", start_pos_for_mutation=0,
                          bucket="default", check_replication=True):
        if ddoc_op_type == "create":
            self.log.info("Processing Create DDoc Operation On Bucket {0}".format(bucket))
            #if there are ddocs already, add to that else start with an empty map
            ddoc_view_map = self.bucket_ddoc_map.pop(bucket, {})
            for ddoc_count in xrange(num_ddocs):
                design_doc_name = prefix_ddoc + str(ddoc_count)
                view_list = []
                #Add views if flag is true
                if test_with_view:
                    #create view objects as per num_views_per_ddoc
                    view_list = self.make_default_views(prefix_view, num_views_per_ddoc)
                #create view in the database
                self.create_views(self.master, design_doc_name, view_list, bucket, self.wait_timeout * 2, check_replication=check_replication)
                #store the created views in internal dictionary
                ddoc_view_map[design_doc_name] = view_list
            #store the ddoc-view dict per bucket
            self.bucket_ddoc_map[bucket] = ddoc_view_map
        elif ddoc_op_type == "update":
            self.log.info("Processing Update DDoc Operation On Bucket {0}".format(bucket))
            #get the map dict for the bucket
            ddoc_view_map = self.bucket_ddoc_map[bucket]
            ddoc_map_loop_cnt = 0
            #iterate for all the ddocs
            for ddoc_name, view_list in ddoc_view_map.items():
                if ddoc_map_loop_cnt < num_ddocs:
                    #Update views if flag is true
                    if test_with_view:
                        #iterate and update all the views as per num_views_per_ddoc
                        for view_count in xrange(num_views_per_ddoc):
                            #create new View object to be updated
                            updated_view = View(view_list[start_pos_for_mutation + view_count].name, self.updated_map_func, None, False)
                            self.cluster.create_view(self.master, ddoc_name, updated_view, bucket, self.wait_timeout * 2, check_replication=check_replication)
                    else:
                        #update the existing design doc(rev gets updated with this call)
                        self.cluster.create_view(self.master, ddoc_name, None, bucket, self.wait_timeout * 2, check_replication=check_replication)
                    ddoc_map_loop_cnt += 1
        elif ddoc_op_type == "delete":
            self.log.info("Processing Delete DDoc Operation On Bucket {0}".format(bucket))
            #get the map dict for the bucket
            ddoc_view_map = self.bucket_ddoc_map[bucket]
            ddoc_map_loop_cnt = 0
            #iterate for all the ddocs
            for ddoc_name, view_list in ddoc_view_map.items():
                if ddoc_map_loop_cnt < num_ddocs:
                    #Update views if flag is true
                    if test_with_view:
                        for view_count in xrange(num_views_per_ddoc):
                            #iterate and update all the views as per num_views_per_ddoc
                            self.cluster.delete_view(self.master, ddoc_name, view_list[start_pos_for_mutation + view_count], bucket, self.wait_timeout * 2)
                        #store the updated view list
                        ddoc_view_map[ddoc_name] = view_list[:start_pos_for_mutation] + view_list[start_pos_for_mutation + num_views_per_ddoc:]
                    else:
                        #delete the design doc
                        self.cluster.delete_view(self.master, ddoc_name, None, bucket, self.wait_timeout * 2)
                        #remove the ddoc_view_map
                        del ddoc_view_map[ddoc_name]
                    ddoc_map_loop_cnt += 1
            #store the updated ddoc dict
            self.bucket_ddoc_map[bucket] = ddoc_view_map
        else:
            self.log.fail("Invalid ddoc operation {0}. No execution done.".format(ddoc_op_type))

    """Asynchronously execute create/update/delete operations on a bucket and
    create an internal dictionary of the objects created. For update/delete operation,
    number of ddocs/views to be updated/deleted with be taken from sequentially from the position specified by start_pos_for_mutation

    Parameters:
        bucket - The name of the bucket on which to execute the operations. (String)
        ddoc_op_type - Operation Type (create/update/delete). (String)
        test_with_view - If operations need to be executed on views. (Boolean)
        num_ddocs - Number of Design Documents to be created/updated/deleted. (Number)
        num_views_per_ddoc - Number of Views per DDoc to be created/updated/deleted. (Number)
        prefix_ddoc - Prefix for the DDoc name. (String)
        prefix_view - Prefix of the View name. (String)
        start_pos_for_mutation=0 - Start index for the update/delete operation

    Returns:
        A list of task futures that is a handle to the scheduled task."""

    def _async_execute_ddoc_ops(self, ddoc_op_type, test_with_view, num_ddocs,
                                num_views_per_ddoc, prefix_ddoc="dev_ddoc",
                                prefix_view="views", start_pos_for_mutation=0,
                                bucket="default", check_replication=True):
        if ddoc_op_type == "create":
            self.log.info("Processing Create DDoc Operation On Bucket {0}".format(bucket))
            tasks = []
            #if there are ddocs already, add to that else start with an empty map
            ddoc_view_map = self.bucket_ddoc_map.pop(bucket, {})
            for ddoc_count in xrange(num_ddocs):
                design_doc_name = prefix_ddoc + str(ddoc_count)
                view_list = []
                #Add views if flag is true
                if test_with_view:
                    #create view objects as per num_views_per_ddoc
                    view_list = self.make_default_views(prefix_view, num_views_per_ddoc)
                #create view in the database
                tasks = self.async_create_views(self.master, design_doc_name, view_list, bucket, check_replication=check_replication)
                #store the created views in internal dictionary
                ddoc_view_map[design_doc_name] = view_list
            #store the ddoc-view dict per bucket
            self.bucket_ddoc_map[bucket] = ddoc_view_map
            return tasks
        elif ddoc_op_type == "update":
            self.log.info("Processing Update DDoc Operation On Bucket {0}".format(bucket))
            #get the map dict for the bucket
            ddoc_view_map = self.bucket_ddoc_map[bucket]
            ddoc_map_loop_cnt = 0
            #iterate for all the ddocs
            tasks = []
            for ddoc_name, view_list in ddoc_view_map.items():
                if ddoc_map_loop_cnt < num_ddocs:
                    #Update views if flag is true
                    if test_with_view:
                        #iterate and update all the views as per num_views_per_ddoc
                        for view_count in xrange(num_views_per_ddoc):
                            #create new View object to be updated
                            updated_view = View(view_list[start_pos_for_mutation + view_count].name, self.updated_map_func, None, False)
                            t_ = self.cluster.async_create_view(self.master, ddoc_name, updated_view, bucket, check_replication=check_replication)
                            tasks.append(t_)
                    else:
                        #update the existing design doc(rev gets updated with this call)
                        t_ = self.cluster.async_create_view(self.master, ddoc_name, None, bucket, check_replication=check_replication)
                        tasks.append(t_)
                    ddoc_map_loop_cnt += 1
            return tasks
        elif ddoc_op_type == "delete":
            self.log.info("Processing Delete DDoc Operation On Bucket {0}".format(bucket))
            #get the map dict for the bucket
            ddoc_view_map = self.bucket_ddoc_map[bucket]
            tasks = []
            ddoc_map_loop_cnt = 0
            #iterate for all the ddocs
            for ddoc_name, view_list in ddoc_view_map.items():
                if ddoc_map_loop_cnt < num_ddocs:
                    #Update views if flag is true
                    if test_with_view:
                        for view_count in xrange(num_views_per_ddoc):
                            #iterate and update all the views as per num_views_per_ddoc
                            t_ = self.cluster.async_delete_view(self.master, ddoc_name, view_list[start_pos_for_mutation + view_count], bucket)
                            tasks.append(t_)
                        #store the updated view list
                        ddoc_view_map[ddoc_name] = view_list[:start_pos_for_mutation] + view_list[start_pos_for_mutation + num_views_per_ddoc:]
                    else:
                        #delete the design doc
                        t_ = self.cluster.async_delete_view(self.master, ddoc_name, None, bucket)
                        tasks.append(t_)
                        #remove the ddoc_view_map
                        del ddoc_view_map[ddoc_name]
                    ddoc_map_loop_cnt += 1
            #store the updated ddoc dict
            self.bucket_ddoc_map[bucket] = ddoc_view_map
            return tasks
        else:
            self.log.fail("Invalid ddoc operation {0}. No execution done.".format(ddoc_op_type))

    """Verify number of Design Docs/Views on all buckets
    comparing with the internal dictionary of the create/update/delete ops

    Parameters:
        None

    Returns:
        None. Fails the test on validation error"""
    def _verify_ddoc_ops_all_buckets(self):
        self.log.info("DDoc Validation Started")
        rest = RestConnection(self.master)
        #Iterate over all the DDocs/Views stored in the internal dictionary
        for bucket, self.ddoc_view_map in self.bucket_ddoc_map.items():
            for ddoc_name, view_list in self.ddoc_view_map.items():
                try:
                    #fetch the DDoc information from the database
                    ddoc_json, header = rest.get_ddoc(bucket, ddoc_name)
                    self.log.info('Database Document {0} details : {1}'.format(ddoc_name, json.dumps(ddoc_json)))
                    ddoc = DesignDocument._init_from_json(ddoc_name, ddoc_json)
                    for view in view_list:
                        if view.name not in [v.name for v in ddoc.views]:
                            self.fail("Validation Error: View - {0} in Design Doc - {1} and Bucket - {2} is missing from database".format(view.name, ddoc_name, bucket))

                except ReadDocumentException:
                    self.fail("Validation Error: Design Document - {0} is missing from Bucket - {1}".format(ddoc_name, bucket))

        self.log.info("DDoc Validation Successful")

    """Verify the number of Documents stored in DDoc/Views for all buckets

    Parameters:
        None

    Returns:
        None. Fails the test on data validation error"""
    def _verify_ddoc_data_all_buckets(self):
        self.log.info("DDoc Data Validation Started. Expected Data Items {0}".format(self.num_items))
        rest = RestConnection(self.master)
        query = {"stale" : "false", "full_set" : "true", "connection_timeout" : 60000}
        for bucket, self.ddoc_view_map in self.bucket_ddoc_map.items():
            for ddoc_name, view_list in self.ddoc_view_map.items():
                for view in view_list:
                    result = self.cluster.query_view(self.master, ddoc_name, view.name, query, self.num_items, bucket)
                    if not result:
                        self.fail("DDoc Data Validation Error: View - {0} in Design Doc - {1} and Bucket - {2}".format(view.name, ddoc_name, bucket))
        self.log.info("DDoc Data Validation Successful")


    def test_invalid_view(self):
        self._load_doc_data_all_buckets()
        invalid_view_name_list = ["", " leadingspace", "\nleadingnewline",
                                  "\rleadingcarriagereturn", "\tleadingtab",
                                  "trailingspace ", "trailingnewline\n",
                                  "trailingcarriagereturn\r", "trailingtab\t"]
        for view_name in invalid_view_name_list:
            view = View(view_name, self.default_map_func, None)
            with self.assertRaises(DesignDocCreationException):
                self.cluster.create_view(
                    self.master, self.default_design_doc_name, view,
                    'default', self.wait_timeout * 2)
                self.fail("server allowed creation of invalid "
                               "view named `{0}`".format(view_name))

    def test_invalid_map_fn_view(self):
        self._load_doc_data_all_buckets()
        self.log.info("sleep in 5 seconds before create invalid map fn view")
        time.sleep(5)
        views = [View("view1", 'function (doc) { emit(doc.age, doc.first_name);',
                      red_func=None, dev_view=False),
                 View("view1", self.default_map_func,
                      red_func='abc', dev_view=False),
                 View("view1", 'function (doc)',
                      red_func=None, dev_view=False)]
        for view in views:
            with self.assertRaises(DesignDocCreationException):
                self.cluster.create_view(
                    self.master, self.default_design_doc_name, view,
                    'default', self.wait_timeout * 2)
                self.fail("server allowed creation of invalid view")

    def test_create_view_with_duplicate_name(self):
        self._load_doc_data_all_buckets()
        for i in xrange(2):
            self._execute_ddoc_ops('create', True, 1, 1)
        self._wait_for_stats_all_buckets([self.master])
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

    def test_create_view_same_name_parallel(self):
        self._load_doc_data_all_buckets()
        for i in xrange(2):
            tasks = self._async_execute_ddoc_ops('create', True, 1, 1)
        for task in tasks:
            task.result(self.wait_timeout * 2)
        self._wait_for_stats_all_buckets([self.master])
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

    """It will test case when map functions are complicated such as more than 200
        line or it function does not get compiled"""
    def test_create_view_multi_map_fun(self):
        self._load_doc_data_all_buckets()
        get_compile = self.input.param("get_compile", True)
        map_fun = self._get_complex_map(get_compile)
        view = View("View1", map_fun, None, False)
        self.cluster.create_view(self.master, self.default_design_doc_name, view, 'default', self.wait_timeout * 2)
        self.view_list.append(view.name)
        self.ddoc_view_map[self.default_design_doc_name] = self.view_list
        self.bucket_ddoc_map['default'] = self.ddoc_view_map
        self._wait_for_stats_all_buckets([self.master])
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

    def test_view_ops(self):
        self._load_doc_data_all_buckets()
        self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc)
        if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view,
                                        self.num_ddocs / 2, self.num_views_per_ddoc / 2)

        self._wait_for_stats_all_buckets([self.master])
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

        """ Create views while deleting or updating few views using another thread """

    def test_view_ops_parallel(self):
        self._load_doc_data_all_buckets()
        create_tasks = self._async_execute_ddoc_ops("create", self.test_with_view, self.num_ddocs,
                                                     self.num_views_per_ddoc)
        views_to_ops = self.input.param("views_to_ops", self.num_views_per_ddoc)
        start_view = self.input.param("start_view", 0)
        tasks = self._async_execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs,
                                                       views_to_ops, "dev_ddoc", "views", start_view)
        for task in create_tasks + tasks:
            task.result(self.wait_timeout * 2)

        self._wait_for_stats_all_buckets([self.master])
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

    def test_update_delete_parallel(self):
        self._load_doc_data_all_buckets()
        self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc)
        views_to_ops = self.input.param("views_to_ops", self.num_views_per_ddoc)
        start_view = self.input.param("start_view", 0)
        tasks_update = self._async_execute_ddoc_ops("update", self.test_with_view, self.num_ddocs, views_to_ops, "dev_ddoc", "views", start_view)
        tasks_delete = self._async_execute_ddoc_ops("delete", self.test_with_view, self.num_ddocs, views_to_ops, "dev_ddoc", "views", start_view)
        for task in tasks_update + tasks_delete:
            task.result(self.wait_timeout * 2)

    """Rebalances nodes into a cluster while doing design docs/views ops:create, delete, update.
    This test begins by loading a given number of items into the cluster. It then
    adds nodes_in nodes at a time and rebalances that nodes into the cluster.
    During the rebalance we perform design docs/views ops(create/update/delete)
    in the cluster. Once the cluster has been rebalanced, wait for the disk queues to drain,
    and then verify that there has been no data loss. Once all nodes have been
    rebalanced in the test is finished."""
    def rebalance_in_with_ddoc_ops(self):
        self._load_doc_data_all_buckets()

        servs_in = self.servers[1:self.nodes_in + 1]
        rebalance = self.cluster.async_rebalance(self.servers[:1], servs_in, [])
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc)
            if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2)
        rebalance.result()
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_in + 1])
        max_verify = None
        if self.num_items > 500000:
            max_verify = 100000
        self._verify_all_buckets(server=self.master, timeout=900, max_verify=max_verify)
        self._verify_stats_all_buckets(self.servers[:self.nodes_in + 1])
        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()

    """Rebalances nodes out of a cluster while doing design doc/view operations.
    This test begins with all servers clustered together and loads a user defined
    number of items into the cluster. It then performs Create/Delete/Update operation on Document
    or view as defined during the rebalance. Once the cluster has been rebalanced the test waits for the disk queues
    to drain and then verifies that there has been no data loss. Once all nodes have
    been rebalanced out of the cluster the test finishes."""
    def incremental_rebalance_out_with_ddoc_ops(self):
        self.log.info("create a cluster of all the available servers")
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])

        self._load_doc_data_all_buckets()

        for i in reversed(range(self.num_servers)[1:]):
            rebalance = self.cluster.async_rebalance(self.servers[:i], [], [self.servers[i]])
            for bucket in self.buckets:
                self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, "dev_ddoc" + str(i))
                if self.ddoc_ops in ["update", "delete"]:
                    self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2, "dev_ddoc" + str(i))
            rebalance.result()
            self._wait_for_stats_all_buckets(self.servers[:i])
            max_verify = None
            if self.num_items > 500000:
                max_verify = 100000
            self._verify_all_buckets(server=self.master, timeout=900, max_verify=max_verify)
            self._verify_stats_all_buckets(self.servers[:i])
            self._verify_ddoc_ops_all_buckets()
            if self.test_with_view:
                self._verify_ddoc_data_all_buckets()

    """Rebalances nodes in and out of a cluster while doing design doc/view operations."""
    def rebalance_in_and_out_with_ddoc_ops(self):
        #assert if number of nodes_in and nodes_out are not sufficient
        self.assertTrue(self.num_servers > self.nodes_in + self.nodes_out,
                            "ERROR: Not enough nodes to do rebalance in and out")

        servs_in = self.servers[self.num_servers - self.nodes_in:]
        #subtract the servs_in from the list of servers
        servs_for_rebal = [serv for serv in self.servers if serv not in servs_in]
        servs_out = servs_for_rebal[self.num_servers - self.nodes_in - self.nodes_out:]
        #list of server which will be available after the in/out operation
        servs_after_rebal = [serv for serv in self.servers if serv not in servs_out]

        self.log.info("create a cluster of all the available servers except nodes_in")
        self.cluster.rebalance(servs_for_rebal[:1],
                               servs_for_rebal[1:], [])

        # load initial documents
        self._load_doc_data_all_buckets()

        #start the rebalance in/out operation
        rebalance = self.cluster.async_rebalance(servs_for_rebal, servs_in, servs_out)

        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc)
            if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2)
        rebalance.result()
        self._wait_for_stats_all_buckets(servs_after_rebal)
        max_verify = None
        if self.num_items > 500000:
            max_verify = 100000
        self._verify_all_buckets(server=self.master, timeout=900, max_verify=max_verify)
        self._verify_stats_all_buckets(servs_after_rebal)
        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()

    """MB-7285
    Check ddoc compaction during rebalance.
    NOTE: 1 ddoc has multiply views with different total number of emitted results

    This test begins by loading a given number of items into the cluster.
    It creates views in 1 ddoc as development/production view with given
    funcs(is_dev_ddoc = True by default). Then we disabled compaction for
    ddoc. Start rebalance. While we don't reach expected fragmentation for
    ddoc we update docs and perform view queries. Start compation when
    fragmentation was reached fragmentation_value. Wait while compaction
    will be completed. """
    def rebalance_in_with_ddoc_compaction(self):
        fragmentation_value = self.input.param("fragmentation_value", 80)
        is_dev_ddoc = False
        ddoc_name = "ddoc_compaction"
        map_fn_2 = "function (doc) { if (doc.first_name == 'sharon') {emit(doc.age, doc.first_name);}}"

        ddoc = DesignDocument(ddoc_name, [View(ddoc_name + "0", self.default_map_func,
                                               None,
                                               dev_view=is_dev_ddoc),
                                          View(ddoc_name + "1",
                                               map_fn_2, None,
                                               dev_view=is_dev_ddoc)])
        prefix = ("", "dev_")[is_dev_ddoc]
        query = {"connectionTimeout" : 60000}
        self.disable_compaction()

        for view in ddoc.views:
            self.cluster.create_view(self.master, ddoc.name, view, bucket=self.default_bucket_name)

        generator = self._load_doc_data_all_buckets()
        RebalanceHelper.wait_for_persistence(self.master, self.default_bucket_name)

        # generate load until fragmentation reached
        rebalance = self.cluster.async_rebalance([self.master], self.servers[1:self.nodes_in + 1], [])
        while rebalance.state != "FINISHED":
            fragmentation_monitor = self.cluster.async_monitor_view_fragmentation(self.master,
                             prefix + ddoc_name, fragmentation_value, self.default_bucket_name)
            end_time = time.time() + self.wait_timeout * 30
            while fragmentation_monitor.state != "FINISHED" and end_time > time.time():
                # update docs to create fragmentation
                self._load_doc_data_all_buckets("update", gen_load=generator)
                for view in ddoc.views:
                    # run queries to create indexes
                    self.cluster.query_view(self.master, prefix + ddoc_name, view.name, query)
            if end_time < time.time() and fragmentation_monitor.state != "FINISHED":
                self.fail("impossible to reach compaction value after %s sec" % (self.wait_timeout * 20))
            fragmentation_monitor.result()
            compaction_task = self.cluster.async_compact_view(self.master, prefix + ddoc_name,
                                                              self.default_bucket_name, with_rebalance=True)
            result = compaction_task.result(self.wait_timeout * 10)
            self.assertTrue(result, "Compaction didn't finished correctly. Please check diags")
        rebalance.result()

    """Add nodes to the cluster and execute design doc/view operations while nodes are in pending add."""
    def pending_add_with_ddoc_ops(self):

        # load initial documents
        self._load_doc_data_all_buckets()

        rest = RestConnection(self.master)
        for node in self.servers[1:]:
            self.log.info("adding node {0}:{1} to cluster".format(node.ip, node.port))
            rest.add_node(self.master.rest_username, self.master.rest_password, node.ip, node.port)

        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc)
            if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2)

        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()

    """Add nodes to a cluster while doing design doc/view operations."""
    def add_nodes_with_ddoc_ops(self):

        # load initial documents
        self._load_doc_data_all_buckets()

        #create ddocs
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, "dev_test", "v1")

        #execute ddocs asynchronously
        for bucket in self.buckets:
            if self.ddoc_ops == "create":
                #create some more ddocs
                tasks_ddoc = self._async_execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2, "dev_test_1", "v1")
            elif self.ddoc_ops in ["update", "delete"]:
                #update delete the same ddocs
                tasks_ddoc = self._async_execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2, "dev_test", "v1")

        rest = RestConnection(self.master)
        for node in self.servers[1:]:
            self.log.info("adding node {0}:{1} to cluster".format(node.ip, node.port))
            rest.add_node(self.master.rest_username, self.master.rest_password, node.ip, node.port)

        for task in tasks_ddoc:
            task.result(self.wait_timeout * 2)

        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()

    """Put nodes in pending removal and then execute design doc/view operations."""
    def pending_removal_with_ddoc_ops(self):

        #assert if number of nodes_out are not sufficient
        self.assertTrue(self.num_servers > self.nodes_out,
                            "ERROR: Not enough nodes to do failover")

        self.log.info("create a cluster of all the available servers")
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])

        # load initial documents
        self._load_doc_data_all_buckets()

        #wait for persistence before verification
        self._wait_for_stats_all_buckets(self.servers)

        self.cluster.failover(self.servers[:self.num_servers],
                                  self.servers[self.num_servers - self.nodes_out:])

        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc)
            if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2)

        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()

    """Failover nodes while doing design doc/view operations."""
    def ddoc_ops_during_failover(self):

        #assert if number of nodes_out are not sufficient
        self.assertTrue(self.num_servers > self.nodes_out,
                            "ERROR: Not enough nodes to do failover")

        self.log.info("create a cluster of all the available servers")
        self.cluster.rebalance(self.servers[:self.num_servers],
                               self.servers[1:self.num_servers], [])

        # load initial documents
        self._load_doc_data_all_buckets()

        #create ddocs
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, "dev_test", "v1")

        #execute ddocs asynchronously
        for bucket in self.buckets:
            if self.ddoc_ops == "create":
                #create some more ddocs
                tasks_ddoc = self._async_execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2, "dev_test_1", "v1")
            elif self.ddoc_ops in ["update", "delete"]:
                #update delete the same ddocs
                tasks_ddoc = self._async_execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2, "dev_test", "v1")

        #failover nodes
        self.cluster.failover(self.servers[:self.num_servers],
                                  self.servers[self.num_servers - self.nodes_out:])

        for task in tasks_ddoc:
            task.result(self.wait_timeout * 2)

        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()

        #rebalance the cluster, do more ddoc ops and then verify again
        self.cluster.rebalance(self.servers[:self.num_servers], [], self.servers[self.num_servers - self.nodes_out:])

        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, "dev_test_2", "v2")
            if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2, "dev_test_2", "v2")

        #wait for persistence before verification
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers - self.nodes_out])

        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()

    def create_design_doc(self):

        # load initial documents
        self._load_doc_data_all_buckets()

        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, "dev_test", "v2")
            if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2, "dev_test", "v2")
        self._verify_ddoc_ops_all_buckets()

    """Remove the master node while doing design doc/view operations."""
    def ddoc_ops_removing_master(self):
        #assert if there is only 1 node
        self.assertTrue(self.num_servers > 1,
                            "ERROR: Need atleast 2 servers to remove master")

        self.log.info("create a cluster of all the available servers")
        self.cluster.rebalance(self.servers[:self.num_servers],
                           self.servers[1:self.num_servers], [])

        # load initial documents
        self._load_doc_data_all_buckets()

        #create ddocs
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, "dev_test", "v1")

        #execute ddocs asynchronously
        for bucket in self.buckets:
            if self.ddoc_ops == "create":
                #create some more ddocs
                tasks_ddoc = self._async_execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2, "dev_test_1", "v1")
            elif self.ddoc_ops in ["update", "delete"]:
                #update delete the same ddocs
                tasks_ddoc = self._async_execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2, self.num_views_per_ddoc / 2, "dev_test", "v1")

        self.log.info("rebalance out the master node")
        self.cluster.rebalance(self.servers[:self.num_servers], [], self.servers[:1])

        for task in tasks_ddoc:
            task.result(self.wait_timeout * 2)

        #update the server list as master is no longer there
        self.servers = self.servers[1:]
        self.master = self.servers[1]

        #wait for persistence before verification
        self._wait_for_stats_all_buckets(self.servers)

        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()

    """Trigger Compaction When specified Fragmentation is reached"""
    def ddoc_ops_during_compaction(self):

        # disable auto compaction
        self.disable_compaction()

        # load initial documents
        self._load_doc_data_all_buckets()

        # create ddoc and add views
        self._execute_ddoc_ops('create', True, self.num_ddocs,
                                self.num_views_per_ddoc)

        # start fragmentation monitor
        for bucket, ddoc_view_map in self.bucket_ddoc_map.items():
            for ddoc_name, view_list in ddoc_view_map.items():
                fragmentation_monitor = self.cluster.async_monitor_view_fragmentation(self.master,
                                                                                      ddoc_name,
                                                                                      self.fragmentation_value,
                                                                                      'default')
                # generate load until fragmentation reached
                while fragmentation_monitor.state != "FINISHED":
                    # update docs to create fragmentation
                    self._load_doc_data_all_buckets("update")
                    for view in view_list:
                        # run queries to create indexes
                        query = {"stale" : "false", "full_set" : "true"}
                        self.cluster.query_view(self.master, ddoc_name, view.name, query)
                fragmentation_monitor.result()

                # compact ddoc and make sure fragmentation is less than high_mark
                # will throw exception if failed
                compaction_task = self.cluster.async_compact_view(self.master, ddoc_name, 'default')

                #create more ddocs, update/delete existing depending on the ddoc_ops type
                if self.ddoc_ops == "create":
                    self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2,
                                           self.num_views_per_ddoc / 2, "dev_test_1", "v1")

                elif self.ddoc_ops in ["update", "delete"]:
                    self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs / 2,
                                           self.num_views_per_ddoc / 2, "dev_test", "v1")

                result = compaction_task.result()
                self.assertTrue(result)

        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()

    def ddoc_ops_during_indexing(self):

        self._load_doc_data_all_buckets()

        # create ddoc and add views
        self._execute_ddoc_ops('create', True, self.num_ddocs,
                                self.num_views_per_ddoc)

        # run queries to create indexes
        query_ops = []
        query = {"stale" : "false"}
        for bucket, self.ddoc_view_map in self.bucket_ddoc_map.items():
            for ddoc_name, view_list in self.ddoc_view_map.items():
                for view in view_list:
                    query_ops.append(self.cluster.async_query_view(self.master, ddoc_name, view.name, query))

        #create more ddocs, update/delete existing depending on the ddoc_ops type
        if self.ddoc_ops == "create":
            ops_task = self._async_execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs,
                                   self.num_views_per_ddoc, "dev_test_1", "v1")

        elif self.ddoc_ops in ["update", "delete"]:
            #update delete the same ddocs
            ops_task = self._async_execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs,
                                   self.num_views_per_ddoc, "dev_test", "v1")

        for task in ops_task:
            result = task.result()
            self.assertTrue(result)

        for task in query_ops:
            result = task.result()

        self._verify_ddoc_ops_all_buckets()
        if self.test_with_view:
            self._verify_ddoc_data_all_buckets()


    def test_view_big_int_positive(self):

        num_items = 10
        self._big_int_test_setup(num_items)

        start_key = [929342299234203, 13403751757202]
        end_keys = [ [929342299234203, 13990000000000], [929342299234203, 13900000000000] ]

        for key in end_keys:
            query_negative = {"startkey" : start_key,
                              "endkey" : key,
                              "stale" : "false", "connection_timeout" : 60000}

            result = self.cluster.query_view(self.master, 'ddoc_big_int', 'view_big_int', query_negative, num_items)
            if not result:
                self.fail("View query for big int(positive test) didn't return expected result")
            self.log.info("View query for big int(positive test) Successful")


    def test_view_big_int_negative(self):

        num_items = 10
        self._big_int_test_setup(num_items)

        start_key = [929342299234203, 13403751757602]
        end_keys = [ [929342299234203, 13990000000000], [929342299234203, 13900000000000] ]

        for key in end_keys:
            query_negative = {"startkey" : start_key,
                              "endkey" : key,
                              "stale" : "false", "connection_timeout" : 60000}

            result = self.cluster.query_view(self.master, 'ddoc_big_int', 'view_big_int', query_negative, 0)
            if not result:
                self.fail("View query for big int(negative test) didn't return expected result")
            self.log.info("View query for big int(negative test) Successful")


    def _big_int_test_setup(self, num_items):

        timestamp = [13403751757202, 13403751757402, 13403751757302]
        docId = ['0830c075-2a81-448a-80d6-85214ee3ad64', '0830c075-2a81-448a-80d6-85214ee3ad65', '0830c075-2a81-448a-80d6-85214ee3ad66']
        conversationId = [929342299234203]
        msg = ['msg1', 'msg2']
        template = '{{ "docId": "{0}", "conversationId": {1}, "timestamp": {2}, "msg": "{3}" }}'

        gen_load = DocumentGenerator('test_docs', template, docId, conversationId, timestamp, msg, start=0, end=num_items)

        self.log.info("Inserting json data into bucket")
        self._load_all_buckets(self.master, gen_load, "create", 0)
        self._wait_for_stats_all_buckets([self.master])

        map_fn = 'function (doc) {emit([doc.conversationId, doc.timestamp], doc);}'
        view = [View('view_big_int', map_fn, dev_view=False)]

        self.create_views(self.master, 'ddoc_big_int', view)

