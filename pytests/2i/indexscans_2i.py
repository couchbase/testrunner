from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.query_definitions import SQLDefinitionGenerator
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
                    query_with_explain = False, query = False, scan_consistency = self.scan_consistency)
                self._run_tasks(tasks)
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = False,
                    query_with_explain = self.run_query_with_explain, query = False, scan_consistency = self.scan_consistency)
                self._run_tasks(tasks)
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = False,
                    query_with_explain = False, query = self.run_query)
                self._run_tasks(tasks)
                # runs operations
                if self.doc_ops:
                    self.run_doc_ops()
                    # verify results
                    tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = self.run_query_with_explain, query = False, scan_consistency = self.scan_consistency)
                    self._run_tasks(tasks)
                    tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query, scan_consistency = self.scan_consistency)
                    self._run_tasks(tasks)
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
                query_with_explain = self.run_query_with_explain, query = self.run_query, scan_consistency = self.scan_consistency)

    def test_multi_create_query_explain_drop_index_with_concurrent_mutations(self):
        try:
            kvops_tasks = self.async_run_doc_ops()
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = self.run_create_index, drop_index = False,
                query_with_explain = False, query = False, scan_consistency = self.scan_consistency)
            # runs operations
            self._run_tasks(kvops_tasks)
            self._run_tasks(tasks)
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = False, drop_index = False,
                query_with_explain = self.run_query_with_explain, query = False)
            self._run_tasks(tasks)
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = False, drop_index = False,
                query_with_explain = False, query = self.run_query, scan_consistency = self.scan_consistency)
            self._run_tasks(tasks)
        except Exception, ex:
            self.log.info(ex)
            raise
        finally:
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = self.run_drop_index,
                    query_with_explain = False, query = False, scan_consistency = self.scan_consistency)
            self._run_tasks(tasks)

    def test_multi_create_query_explain_drop_index_primary(self):
        qdfs = []
        for query_definition in self.query_definitions:
            query_definition.index_name = "#primary"
            qdfs.append(query_definition)
        self.query_definitions = qdfs
        if self.run_async:
            try:
                self._verify_primary_index_count()
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = False,
                    query_with_explain = self.run_query_with_explain, query = False, scan_consistency = self.scan_consistency)
                self._run_tasks(tasks)
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = False,
                    query_with_explain = False, query = self.run_query, scan_consistency = self.scan_consistency)
                self._run_tasks(tasks)
                # runs operations
                self.run_doc_ops()
                self._verify_primary_index_count()
                # verify results
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = False,
                    query_with_explain = self.run_query_with_explain, query = False, scan_consistency = self.scan_consistency)
                self._run_tasks(tasks)
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                    query_definitions = self.query_definitions,
                    create_index = False, drop_index = False,
                    query_with_explain = False, query = self.run_query, scan_consistency = self.scan_consistency)
                self._run_tasks(tasks)
            except Exception, ex:
                self.log.info(ex)
                raise
        else:
            self.run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = False, drop_index = False,
                query_with_explain = self.run_query_with_explain, query = self.run_query)

    def test_multi_create_query_explain_drop_index_with_index_where_clause(self):
        query_definition_generator = SQLDefinitionGenerator()
        self.query_definitions = query_definition_generator.generate_employee_data_query_definitions_for_index_where_clause()
        self.use_where_clause_in_index = True
        self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.run_async = False
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = True, drop_index = False,
            query_with_explain = True, query = True)
        if self.doc_ops:
            self.run_doc_ops()
            self.run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = False, drop_index = False,
                query_with_explain = True, query = True, scan_consistency = self.scan_consistency)
        self.run_multi_operations(buckets = self.buckets,
            query_definitions = self.query_definitions,
            create_index = False, drop_index = True,
            query_with_explain = False, query = False)

    def test_multi_create_query_explain_drop_index_scan_consistency(self):
        self.random_scan_vector= self.input.param("random_scan_vector",False)
        scan_vector_ranges = []
        scan_vectors = None
        if self.scan_vector_per_values:
            scan_vector_ranges = self._generate_scan_vector_ranges(self.scan_vector_per_values)
        try:
            self.run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = self.run_create_index, drop_index = False,
                query_with_explain = False, query = False)
            if len(scan_vector_ranges) > 0:
                for use_percentage in scan_vector_ranges:
                    scan_vectors = self.gen_scan_vector(use_percentage = use_percentage,
                        use_random = self.random_scan_vector)
                    tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                    self._run_tasks(tasks)
            else:
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                self._run_tasks(tasks)
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = False, drop_index = False,
                query_with_explain = self.run_query_with_explain, query = False)
            self._run_tasks(tasks)
            # runs operations
            self.run_doc_ops()
            if self.scan_vector_per_values:
                scan_vector_ranges = self._generate_scan_vector_ranges(self.scan_vector_per_values)
            # verify results
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = False, drop_index = False,
                query_with_explain = self.run_query_with_explain, query = False)
            self._run_tasks(tasks)
            if len(scan_vector_ranges) > 0:
                for use_percentage in scan_vector_ranges:
                    scan_vectors = self.gen_scan_vector(use_percentage = use_percentage,
                    use_random = self.random_scan_vector)
                    tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                    self._run_tasks(tasks)
            else:
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                self._run_tasks(tasks)
        except Exception, ex:
            self.log.info(ex)
            raise
        finally:
            tasks = self.async_run_multi_operations(buckets = self.buckets,
                query_definitions = self.query_definitions,
                create_index = False, drop_index = self.run_drop_index,
                query_with_explain = False, query = False)
            self._run_tasks(tasks)

    def test_primary_query_scan_consistency(self):
        self.random_scan_vector= self.input.param("random_scan_vector",False)
        scan_vector_ranges = []
        scan_vectors = None
        if self.scan_vector_per_values:
            scan_vector_ranges = self._generate_scan_vector_ranges(self.scan_vector_per_values)
        try:
            if len(scan_vector_ranges) > 0:
                for use_percentage in scan_vector_ranges:
                    scan_vectors = self.gen_scan_vector(use_percentage = use_percentage,
                    use_random = self.random_scan_vector)
                    tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                    self._run_tasks(tasks)
            else:
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                self._run_tasks(tasks)
            # runs operations
            self.run_doc_ops()
            if self.scan_vector_per_values:
                scan_vector_ranges = self._generate_scan_vector_ranges(self.scan_vector_per_values)
            self._verify_primary_index_count()
            # verify results
            if len(scan_vector_ranges) > 0:
                for use_percentage in scan_vector_ranges:
                    scan_vectors = self.gen_scan_vector(use_percentage = use_percentage,
                    use_random = self.random_scan_vector)
                    tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                    self._run_tasks(tasks)
            else:
                tasks = self.async_run_multi_operations(buckets = self.buckets,
                        query_definitions = self.query_definitions,
                        create_index = False, drop_index = False,
                        query_with_explain = False, query = self.run_query,
                        scan_consistency = self.scan_consistency,
                        scan_vectors = scan_vectors)
                self._run_tasks(tasks)
        except Exception, ex:
            self.log.info(ex)
            raise

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

    def _generate_scan_vector_ranges(self, scan_vector_per_values = None):
        scan_vector_per_values = str(scan_vector_per_values)
        values = scan_vector_per_values.split(":")
        new_values = []
        for val in values:
            new_values.append(float(val))
        return new_values

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
