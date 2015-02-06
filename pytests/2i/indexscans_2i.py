from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.tuq_generators import TuqGenerators
QUERY_TEMPLATE = "SELECT {0} FROM %s "
class SecondaryIndexingScanTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingScanTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingScanTests, self).tearDown()

    def test_create_query_explain_drop_index(self):
        self.use_primary_index= self.input.param("use_primary_index",False)
    	self.indexes= self.input.param("indexes","").split(":")
    	self.emitFields= self.input.param("emitFields","*").split(":")
    	self.whereCondition= self.input.param("whereCondition",None)
    	self.emitFields = ",".join(self.emitFields)
    	query_template = QUERY_TEMPLATE
    	query_template = query_template.format(self.emitFields)
        self.index_name = "test_create_query_explain_drop_index"
        if self.use_primary_index:
            self.run_create_index = False
            self.run_drop_index = False
            self.index_name = "primary"
    	if self.whereCondition:
    		query_template += " WHERE {0}".format(self.whereCondition)
    	query_template = self._translate_where_clause(query_template)
    	query_definition = QueryDefinition(
    		index_name=self.index_name,
    		index_fields = self.indexes,
    		query_template = query_template,
    		groups = [])
    	self.run_multi_operations(
			buckets = self.buckets,
			query_definitions = [query_definition],
			create_index = self.run_create_index, drop_index = self.run_drop_index,
			query_with_explain = self.run_query_with_explain, query = self.run_query)

    def test_multi_create_query_explain_drop_index(self):
        if self.run_async:
            try:
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = self.run_create_index, drop_index = False,
                    query_with_explain = False, query = False)
                self._run_tasks(tasks)
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = False,
                    query_with_explain = False, query = self.run_query)
                self._run_tasks(tasks)
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = False,
                    query_with_explain = self.run_query_with_explain, query = False)
                self._run_tasks(tasks)
                # runs operations
                self.run_doc_ops()
                # verify results
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = False,
                    query_with_explain = False, query = self.run_query)
                self._run_tasks(tasks)
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = False,
                    query_with_explain = self.run_query_with_explain, query = False)
            except Exception, ex:
                self.log.info(ex)
                raise
            finally:
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = self.run_drop_index,
                    query_with_explain = False, query = False)
                self._run_tasks(tasks)
        else:
            self.run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = self.run_create_index, drop_index = self.run_drop_index,
                query_with_explain = self.run_query_with_explain, query = self.run_query)

    def test_failure_query_with_non_existing_primary_index(self):
        self.indexes= self.input.param("indexes","").split(":")
        self.emitFields= self.input.param("emitFields","*").split(":")
        self.whereCondition= self.input.param("whereCondition",None)
        self.emitFields = ",".join(self.emitFields)
        query_template = QUERY_TEMPLATE
        query_template = query_template.format(self.emitFields)
        self.index_name = "test_failure_query_with_non_existing_primary_index"
        if self.whereCondition:
            query_template += " WHERE {0}".format(self.whereCondition)
        query_template = self._translate_where_clause(query_template)
        query_definition = QueryDefinition(
            index_name=self.index_name,
            index_fields = self.indexes,
            query_template = query_template,
            groups = [])
        try:
            self.run_multi_operations(
            buckets = self.buckets,
            query_definitions = [query_definition],
                create_index = False, drop_index = False,
                query_with_explain = False, query = self.run_query)
            self.fail(" querying without indexes and primary indexes is not allowed")
        except Exception, ex:
            msg = "No primary index on keyspace default. Use CREATE PRIMARY INDEX to create one."
            self.assertTrue(msg in str(ex),"did not recieve message as expected : {0}".format(ex))


    def _run_tasks(self, tasks):
    	for task in tasks:
    		task.result()

    def _translate_where_clause(self, query):
    	query = query.replace("EQUALS","==")
    	query = query.replace("NOT_EQUALS","!=")
    	query = query.replace("LESS_THAN","<")
    	query = query.replace("LESS_THAN_EQUALS","<=")
    	query = query.replace("GREATER_THAN",">")
    	query = query.replace("GREATER_THAN_EQUALS",">=")
    	return query
