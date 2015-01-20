from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition
QUERY_TEMPLATE = "SELECT {0} FROM %s "
class SecondaryIndexingScanTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingScanTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingScanTests, self).tearDown()

    def test_create_query_explain_drop_index(self):
    	self.indexes= self.input.param("indexes","").split(":")
    	self.emitFields= self.input.param("emitFields","*").split(":")
    	self.whereCondition= self.input.param("whereCondition",None)
    	self.emitFields = ",".join(self.emitFields)
    	query_template = QUERY_TEMPLATE
    	query_template = query_template.format(self.emitFields)
    	if self.whereCondition:
    		query_template += " WHERE {0}".format(self.whereCondition)
    	query_template = self._translate_where_clause(query_template)
    	query_definition = QueryDefinition(
    		index_name="test_create_query_explain_drop_index",
    		index_fields = self.indexes,
    		query_template = query_template,
    		groups = [])
    	self.run_multi_operations(
			buckets = self.buckets,
			query_definitions = [query_definition],
			create_index = True, drop_index = True,
			query_with_explain = True, query = True)

    def test_multi_create_query_explain_drop_index(self):
    	if self.async_run:
    		tasks = self.async_run_multi_operations(buckets = self.buckets,
            	query_definitions = self.query_definitions,
            	create_index = True, drop_index = False,
            	query_with_explain = False, query = False)
    		self._run_tasks(tasks)
    		tasks = self.async_run_multi_operations(buckets = self.buckets,
            	query_definitions = self.query_definitions,
            	create_index = False, drop_index = False,
            	query_with_explain = False, query = True)
    		self._run_tasks(tasks)
    		tasks = self.async_run_multi_operations(buckets = self.buckets,
            	query_definitions = self.query_definitions,
            	create_index = False, drop_index = False,
            	query_with_explain = True, query = False)
    		self._run_tasks(tasks)
    		tasks = self.async_run_multi_operations(buckets = self.buckets,
            	query_definitions = self.query_definitions,
            	create_index = False, drop_index = True,
            	query_with_explain = False, query = False)
    		self._run_tasks(tasks)
    	else:
        	self.run_multi_operations(buckets = self.buckets,
            	query_definitions = self.query_definitions,
            	create_index = True, drop_index = True,
            	query_with_explain = True, query = True)

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
