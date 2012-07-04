import json
import time
from view_base import ViewBaseTest
from couchbase.document import DesignDocument, View
from membase.api.rest_client import RestConnection
from membase.api.exception import ReadDocumentException
from membase.api.exception import DesignDocCreationException
from couchbase.documentgenerator import DocumentGenerator

class CreateDeleteViewTests(ViewBaseTest):

    def setUp(self):
        super(CreateDeleteViewTests, self).setUp()
        self.bucket_ddoc_map = {}
        self.ddoc_ops=self.input.param("ddoc_ops", None)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.test_with_view = self.input.param("test_with_view", False)
        self.num_views_per_ddoc = self.input.param("num_views_per_ddoc", 1)
        self.num_ddocs = self.input.param("num_ddocs", 1)
        self.gen = None
        self.default_design_doc_name = "Doc1"
        self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
        self.updated_map_func = 'function (doc) { emit(null, doc);}'
        self.default_view = View("View", self.default_map_func, None, False)

    def tearDown(self):
        super(CreateDeleteViewTests, self).tearDown()

    def _get_complex_map(self,get_compile):
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

    def _create_multiple_ddocs_name(self,num_ddocs):
        design_docs= []
        for i in range(0,num_ddocs):
            design_docs.append(self.default_design_doc_name + str(i))
        return design_docs

    def _load_doc_data_all_buckets(self):
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen_load = DocumentGenerator('test_docs', template, age, first, start=0, end=self.num_items)
        self._load_all_buckets(self.servers[0], gen_load,'create', 0)

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

    def _execute_ddoc_ops(self, ddoc_op_type, test_with_view, num_ddocs, num_views_per_ddoc, prefix_ddoc="dev_ddoc", prefix_view="views", start_pos_for_mutation=0, bucket="default"):
        if ddoc_op_type == "create":
            self.log.info("Processing Create DDoc Operation On Bucket {0}".format(bucket))
            ddoc_view_map = {}
            for ddoc_count in xrange(num_ddocs):
                design_doc_name = prefix_ddoc + str(ddoc_count)
                view_list = []
                #Add views if flag is true
                if test_with_view:
                    #create view objects as per num_views_per_ddoc
                    views = self.make_default_views(prefix_view, num_views_per_ddoc)
                    server = self.servers[0]
                    for view in views:
                        view_list.append(view)
                        #create view in the database
                    self.create_views(server, design_doc_name, views, bucket, 120)
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
                            self.cluster.create_view(self.servers[0], ddoc_name, updated_view, bucket, 120)
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
                            self.cluster.delete_view(self.servers[0], ddoc_name, view_list[start_pos_for_mutation + view_count], bucket, 120)
                        #store the updated view list
                        ddoc_view_map[ddoc_name] = view_list[:start_pos_for_mutation] + view_list[start_pos_for_mutation + num_views_per_ddoc:]
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

    def _async_execute_ddoc_ops(self, ddoc_op_type, test_with_view, num_ddocs, num_views_per_ddoc, prefix_ddoc="dev_ddoc", prefix_view="views", start_pos_for_mutation=0, bucket="default"):
        if ddoc_op_type == "create":
            self.log.info("Processing Create DDoc Operation On Bucket {0}".format(bucket))
            tasks = []
            ddoc_view_map = {}
            for ddoc_count in xrange(num_ddocs):
                design_doc_name = prefix_ddoc + str(ddoc_count)
                view_list = []
                #Add views if flag is true
                if test_with_view:
                    #create view objects as per num_views_per_ddoc
                    views = self.make_default_views(prefix_view, num_views_per_ddoc)
                    server = self.servers[0]
                    for view in views:
                        view_list.append(view)
                        #create view in the database
                    tasks = self.async_create_views(server, design_doc_name, views, bucket)
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
                            t_ = self.cluster.async_create_view(self.servers[0], ddoc_name, updated_view, bucket)
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
                            t_ = self.cluster.async_delete_view(self.servers[0], ddoc_name, view_list[start_pos_for_mutation + view_count], bucket)
                            tasks.append(t_)
                        #store the updated view list
                        ddoc_view_map[ddoc_name] = view_list[:start_pos_for_mutation] + view_list[start_pos_for_mutation + num_views_per_ddoc:]
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
        rest = RestConnection(self.servers[0])
        #Iterate over all the DDocs/Views stored in the internal dictionary
        for bucket, self.ddoc_view_map in self.bucket_ddoc_map.items():
            for ddoc_name, view_list in self.ddoc_view_map.items():
                try:
                    #fetch the DDoc information from the database
                    ddoc_json = rest.get_ddoc(bucket, ddoc_name)
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
        rest = RestConnection(self.servers[0])
        query = {"stale" : "false", "full_set" : "true", "connection_timeout": 60000}
        for bucket, self.ddoc_view_map in self.bucket_ddoc_map.items():
            for ddoc_name, view_list in self.ddoc_view_map.items():
                for view in view_list:
                    result = self.cluster.query_view(self.servers[0], ddoc_name, view.name, query, self.num_items, bucket)
                    if not result:
                        self.fail("DDoc Data Validation Error: View - {0} in Design Doc - {1} and Bucket - {2}".format(view.name, ddoc_name, bucket))
        self.log.info("DDoc Data Validation Successful")


    """Create view design doc i) tests create single view in single doc (test_create_views,num_views = 1,num_docs=1)
    ii) tests create multiple views in single docs(test_create_views,num_views = 10,num_docs=1)
    iii) tests create multiple views in multiple docs(test_create_views,num_views = 5,num_docs=5)
    iv) Tests create view without  existing designdoc
    v) Tests create view with invalid name having non alpha numeric characters
    and View with long name(test_create_views,inavlid_view = True)
    vi) Tests same name or duplicate view in different design doc"""
    def test_invalid_view(self):
            self._load_doc_data_all_buckets()
            invalid_view_name_list = ["","#","{"]
            long_view_name = 'V'*2000
            invalid_view_name_list.append(long_view_name)
            for view_name in invalid_view_name_list:
                view = View(view_name,self.default_map_func,None)
                try:
                    self.cluster.create_view(self.servers[0], self.default_design_doc_name, view, 'default', 120)
                    self.log.error("server allowed creation of invalid view:{0}",view_name)
                except DesignDocCreationException:
                    self.log.info("view creation has been failed for view name as"+ view_name )

    def test_create_view_with_duplicate_name(self):
            self._load_doc_data_all_buckets()
            for i in xrange(2):
                self._execute_ddoc_ops('create', True, 1, 1)
            self._verify_ddoc_ops_all_buckets()
            self._verify_ddoc_data_all_buckets()

    def test_create_view_same_name_parallel(self):
            self._load_doc_data_all_buckets()
            for i in xrange(2):
                tasks = self._async_execute_ddoc_ops('create', True, 1, 1)
            for task in tasks:
                task.result(120)
            self._verify_ddoc_ops_all_buckets()
            self._verify_ddoc_data_all_buckets()

    """It will test case when map functions are complicated such as more than 200
        line or it function does not get compiled"""
    def test_create_view_multi_map_fun(self):
        self._load_doc_data_all_buckets()
        get_compile = self.input.param("get_compile", True)
        map_fun = self._get_complex_map(get_compile)
        view = View("View1", map_fun, None, False)
        self.cluster.create_view(self.servers[0], self.default_design_doc_name, view, 'default', 120)
        self.view_list.append(view.name)
        self.ddoc_view_map[self.default_design_doc_name] = self.view_list
        self.bucket_ddoc_map['default'] = self.ddoc_view_map
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

    """Rebalances nodes into a cluster while doing design docs/views ops:create, delete, update.
    This test begins by loading a given number of items into the cluster. It then
    adds nodes_in nodes at a time and rebalances that nodes into the cluster.
    During the rebalance we perform design docs/views ops(create/update/delete)
    in the cluster. Once the cluster has been rebalanced, wait for the disk queues to drain,
    and then verify that there has been no data loss. Once all nodes have been
    rebalanced in the test is finished."""
    def rebalance_in_with_ddoc_ops(self):
        self._load_doc_data_all_buckets()

        servs_in=[self.servers[i+1] for i in range(self.nodes_in)]
        rebalance = self.cluster.async_rebalance(self.servers[:1], servs_in, [])
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc)
            if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs/2, self.num_views_per_ddoc/2)
        rebalance.result()
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_in+1])
        self._verify_all_buckets(self.servers[0])
        self._verify_stats_all_buckets(self.servers[:self.nodes_in+1])
        self._verify_ddoc_ops_all_buckets()
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
        self.log.info("load document data")
        self._load_doc_data_all_buckets()

        for i in reversed(range(self.num_servers)[1:]):
            rebalance = self.cluster.async_rebalance(self.servers[:i], [], [self.servers[i]])
            for bucket in self.buckets:
                self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, "dev_ddoc"+str(i))
                if self.ddoc_ops in ["update","delete"]:
                    self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs/2, self.num_views_per_ddoc/2, "dev_ddoc"+str(i))
            rebalance.result()
            self._wait_for_stats_all_buckets(self.servers[:i])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:i])
            self._verify_ddoc_ops_all_buckets()
            self._verify_ddoc_data_all_buckets()

    def test_view_ops(self):
        self._load_doc_data_all_buckets()
        self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc)
        if self.ddoc_ops in ["update", "delete"]:
                self._execute_ddoc_ops(self.ddoc_ops, self.test_with_view,
                                        self.num_ddocs/2, self.num_views_per_ddoc/2)
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()
        """ Create views while deleting or updating few views using another thread """

    def test_view_ops_parallel(self):
        self._load_doc_data_all_buckets()
        create_tasks = self._async_execute_ddoc_ops("create", self.test_with_view, self.num_ddocs,
                                                     self.num_views_per_ddoc)
        views_to_ops = self.input.param("views_to_ops", self.num_views_per_ddoc)
        start_view = self.input.param("start_view",0)
        tasks = self._async_execute_ddoc_ops(self.ddoc_ops, self.test_with_view, self.num_ddocs,
                                                       views_to_ops, "dev_ddoc", "views", start_view)
        for task in create_tasks + tasks:
            task.result(120)
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
            task.result(120)
