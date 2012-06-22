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
        self.view_name_list = []
        self.ddoc_view_map = {}
        self.ddoc_ops=self.input.param("ddoc_ops", None)
        if self.ddoc_ops is not None:
            self.ddoc_ops = self.ddoc_ops.split(";")
        self.nodes_in = self.input.param("nodes_in", 1)
        self.test_with_view = self.input.param("test_with_view",False)
        self.num_views_per_ddoc = self.input.param("num_views_per_ddoc", 1)
        self.num_ddocs = self.input.param("num_ddocs", 1)
        self.gen = None
        self.default_design_doc_name = "Doc1"
        self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
        self.default_view = View("View",self.default_map_func,None,False)

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

    def _execute_ddoc_ops(self, bucket):
        if(self.ddoc_ops is not None):
            if("create" in self.ddoc_ops):
                for ddoc_count in range(0,self.num_ddocs):
                    design_doc_name = "dev_ddoc"+str(ddoc_count)
                    if self.test_with_view:
                        views = self.make_default_views("views", self.num_views_per_ddoc)
                        server = self.servers[0]
                        for view in views:
                            self.view_name_list.append(view.name)
                        view_task = self.create_views(server, design_doc_name, views, bucket,120)

                    self.ddoc_view_map[design_doc_name] = self.view_name_list
                self.bucket_ddoc_map[bucket] = self.ddoc_view_map

    def _verify_ddoc_ops_all_buckets(self):
        rest = RestConnection(self.servers[0])
        for bucket, self.ddoc_view_map in self.bucket_ddoc_map.items():
            for ddoc_name, self.view_name_list in self.ddoc_view_map.items():
                try:
                    ddoc_json = rest.get_ddoc(bucket, ddoc_name)
                    self.log.info('Document {0} details : {1}'.format(ddoc_name,json.dumps(ddoc_json)))
                    ddoc = DesignDocument._init_from_json(ddoc_name, ddoc_json)
                    for view_name in self.view_name_list:
                        if view_name not in [view.name for view in ddoc.views]:
                            self.fail("Validation Error: View - {0} in Design Doc - {1} and Bucket - {2} is missing".format(view_name,ddoc_name, bucket))

                except ReadDocumentException:
                    self.fail("Validation Error: Design Document - {0} is missing".format(ddoc_name))

    """Create view design doc i) tests create single view in single doc (test_create_views,num_views = 1,num_docs=1)
    ii) tests create multiple views in single docs(test_create_views,num_views = 10,num_docs=1)
    iii) tests create multiple views in multiple docs(test_create_views,num_views = 5,num_docs=5)
    iv) Tests create view without  existing designdoc
    v) Tests create view with invalid name having non alpha numeric characters
    and View with long name(test_create_views,inavlid_view = True)
    vi) Tests same name or duplicate view in different design doc"""
    def test_create_view(self):
            self._load_doc_data_all_buckets()
            invalid_view = self.input.param("invalid_view",False)
            if invalid_view:
                invalid_view_name_list = ["","#","{"]
                long_view_name = 'V'*2000
                invalid_view_name_list.append(long_view_name)
                for view_name in invalid_view_name_list:
                    view = View(view_name,self.default_map_func,None)
                    try:
                        self.cluster.create_view(self.servers[0], self.default_design_doc_name, view, 'default',120)
                    except DesignDocCreationException:
                        self.log.info("view creation has been failed for view name as"+ view_name )
            else:
                self._execute_ddoc_ops('default')
                self._verify_ddoc_ops_all_buckets()

    def test_create_view_with_duplicate_name(self):
            self._load_doc_data_all_buckets()
            for i in range(0,2):
                self.cluster.create_view(self.servers[0],self.default_design_doc_name, self.default_view, 'default',120)
                self.view_name_list.append(self.default_view.name)
            self.ddoc_view_map[self.default_design_doc_name] = self.view_name_list
            self.bucket_ddoc_map['default'] = self.ddoc_view_map
            self._verify_ddoc_ops_all_buckets()

    def test_create_view_same_name_parallel(self):
            self._load_doc_data_all_buckets()
            task_handler = []
            for i in range(0,2):
                task_handler.append(self.cluster.async_create_view(self.servers[0],self.default_design_doc_name, self.default_view, 'default'))
                self.view_name_list.append(self.default_view.name)
            self.ddoc_view_map[self.default_design_doc_name] = self.view_name_list
            self.bucket_ddoc_map['default'] = self.ddoc_view_map
            for _task in task_handler:
                _task.result(120)
            self._verify_ddoc_ops_all_buckets()

    """ It will test parallel creation of multiple views in different thread
    and It will also test the scenario in which we create view when design doc getting updated"""
    def test_create_multiple_view_parallel(self):
        self._load_doc_data_all_buckets()
        task_handler = []
        num_views_per_ddoc = self.input.param("num_views_per_ddoc",2)
        num_ddocs = self.input.param("num_ddocs",4)
        design_Docs = self._create_multiple_ddocs_name(num_ddocs)
        Views = self.make_default_views("MultiViews", num_views_per_ddoc)
        for doc in design_Docs:
            for view in Views:
                task_handler.append(self.cluster.async_create_view(self.servers[0], doc, view, 'default'))
                self.view_name_list.append(view.name)
            self.ddoc_view_map[doc] = self.view_name_list
        self.bucket_ddoc_map['default'] = self.ddoc_view_map
        for _task in task_handler:
            _task.result(120)
        self._verify_ddoc_ops_all_buckets()

        """It will test case when map functions are complicated such as more than 200
        line or it function does not get compiled"""
    def test_create_view_multi_map_fun(self):
        self._load_doc_data_all_buckets()
        get_compile = self.input.param("get_compile",True)
        map_fun = self._get_complex_map(get_compile)
        view = View("View1",map_fun,None,False)
        self.cluster.create_view(self.servers[0], self.default_design_doc_name, view, 'default',60)
        self.view_name_list.append(view.name)
        self.ddoc_view_map[self.default_design_doc_name] = self.view_name_list
        self.bucket_ddoc_map['default'] = self.ddoc_view_map
        self._verify_ddoc_ops_all_buckets()

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
        for bucket, kv_stores in self.buckets.items():
            self._execute_ddoc_ops(bucket)
        rebalance.result()
        self._wait_for_stats_all_buckets(self.servers[:self.nodes_in+1])
        self._verify_all_buckets(self.servers[0])
        self._verify_stats_all_buckets(self.servers[:self.nodes_in+1])
        self._verify_ddoc_ops_all_buckets()

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
            for bucket, kv_stores in self.buckets.items():
                self._execute_ddoc_ops(bucket)
            rebalance.result()
            self._wait_for_stats_all_buckets(self.servers[:i])
            self._verify_all_buckets(self.servers[0])
            self._verify_stats_all_buckets(self.servers[:i])
            self._verify_ddoc_ops_all_buckets()



