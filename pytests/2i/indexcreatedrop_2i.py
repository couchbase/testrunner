from base_2i import BaseSecondaryIndexingTests
from couchbase_helper.query_definitions import QueryDefinition

class SecondaryIndexingCreateDropTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingCreateDropTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingCreateDropTests, self).tearDown()

    def test_multi_create_drop_index(self):
        self.log.info("test_multi_create_drop_index")
        self.run_multi_operations(buckets = self.buckets, query_definitions = self.query_definitions, create_index = True, drop_index = True)

    def test_create_primary_using_views_with_existing_primary_index_gsi(self):
    	query_definition = QueryDefinition(
    		index_name="test_failure_create_primary_using_views_with_existing_primary_index_gsi",
    		index_fields = "crap",
    		query_template = "",
    		groups = [])
    	check = False
    	self.query = "CREATE PRIMARY INDEX ON {0}".format(self.buckets[0].name)
    	try:
    		# create index
    		server = self.get_nodes_from_services_map(service_type = "n1ql")
        	self.n1ql_helper.run_cbq_query(query = self.query, server = server)
    	except Exception, ex:
    		self.log.info(ex)
    		raise

    def test_create_primary_using_gsi_with_existing_primary_index_views(self):
    	query_definition = QueryDefinition(
    		index_name="test_failure_create_primary_using_gsi_with_existing_primary_index_views",
    		index_fields = "crap",
    		query_template = "",
    		groups = [])
    	check = False
    	self.query = "CREATE PRIMARY INDEX ON {0} USING GSI".format(self.buckets[0].name)
    	try:
    		# create index
    		server = self.get_nodes_from_services_map(service_type = "n1ql")
        	self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception, ex:
        	self.log.info(ex)
        	raise

    def test_create_gsi_index_existing_view_index(self):
    	self.indexes= self.input.param("indexes","").split(":")
    	query_definition = QueryDefinition(
    		index_name="test_failure_create_index_existing_index",
    		index_fields = self.indexes,
    		query_template = "",
    		groups = [])
    	self.query = query_definition.generate_index_create_query(bucket = self.buckets[0].name,
    	 use_gsi_for_secondary = False)
    	try:
    		# create index
    		server = self.get_nodes_from_services_map(service_type = "n1ql")
        	self.n1ql_helper.run_cbq_query(query = self.query, server = server)
    		# create same index again
    		self.query += " USING GSI "
    		self.n1ql_helper.run_cbq_query(query = self.query, server = server)
    	except Exception, ex:
    		self.log.info(ex)
    		raise
    	finally:
    		self.query = query_definition.generate_index_drop_query(bucket = self.buckets[0].name)
    		actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = server)

    def test_failure_create_index_big_fields(self):
        field_name = ""
        field_name += ",".join([ str(a) for a in range(1,100)]).replace(",","_")
        query_definition = QueryDefinition(
            index_name="test_failure_create_index_existing_index",
            index_fields = field_name,
            query_template = "",
            groups = [])
        self.query = query_definition.generate_index_create_query(bucket = self.buckets[0])
        try:
            # create index
            server = self.get_nodes_from_services_map(service_type = "n1ql")
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception, ex:
            msg="Expression not indexable"
            self.assertTrue(msg in str(ex),
                " 5000 error not recived as expected {0}".format(ex))

    def test_create_gsi_index_without_primary_index(self):
        self.indexes= self.input.param("indexes","").split(":")
        query_definition = QueryDefinition(
            index_name="test_failure_create_index_existing_index",
            index_fields = self.indexes,
            query_template = "",
            groups = [])
        self.query = query_definition.generate_index_create_query(bucket = self.buckets[0].name)
        try:
            # create index
            server = self.get_nodes_from_services_map(service_type = "n1ql")
            self.n1ql_helper.run_cbq_query(query = self.query, server = server)
        except Exception, ex:
            msg="Keyspace not_present_bucket name not found - cause: Bucket not_present_bucket not found. - cause: No bucket named not_present_bucket"
            self.assertTrue(msg in str(ex),
                " 5000 error not recived as expected {0}".format(ex))

    def test_failure_create_index_non_existing_bucket(self):
    	self.indexes= self.input.param("indexes","").split(":")
    	query_definition = QueryDefinition(
    		index_name="test_failure_create_index_existing_index",
    		index_fields = self.indexes,
    		query_template = "",
    		groups = [])
    	self.query = query_definition.generate_index_create_query(bucket = "not_present_bucket")
    	try:
    		# create index
    		server = self.get_nodes_from_services_map(service_type = "n1ql")
        	self.n1ql_helper.run_cbq_query(query = self.query, server = server)
    	except Exception, ex:
    		msg="Keyspace not_present_bucket name not found - cause: Bucket not_present_bucket not found. - cause: No bucket named not_present_bucket"
    		self.assertTrue(msg in str(ex),
    			" 5000 error not recived as expected {0}".format(ex))

    def test_failure_drop_index_non_existing_bucket(self):
    	query_definition = QueryDefinition(
    		index_name="test_failure_create_index_existing_index",
    		index_fields = "crap",
    		query_template = "",
    		groups = [])
    	self.query = query_definition.generate_index_drop_query(bucket = "not_present_bucket")
    	try:
    		# create index
    		server = self.get_nodes_from_services_map(service_type = "n1ql")
        	self.n1ql_helper.run_cbq_query(query = self.query, server = server)
    	except Exception, ex:
    		msg="Keyspace not_present_bucket name not found - cause: Bucket not_present_bucket not found. - cause: No bucket named not_present_bucket"
    		self.assertTrue(msg in str(ex),
    			" 5000 error not recived as expected {0}".format(ex))

    def test_failure_drop_index_non_existing_index(self):
    	query_definition = QueryDefinition(
    		index_name="test_failure_create_index_existing_index",
    		index_fields = "crap",
    		query_template = "",
    		groups = [])
    	self.query = query_definition.generate_index_drop_query(bucket = self.buckets[0].name)
    	try:
    		# create index
    		server = self.get_nodes_from_services_map(service_type = "n1ql")
        	self.n1ql_helper.run_cbq_query(query = self.query, server = server)
    	except Exception, ex:
    		msg="2i index test_failure_create_index_existing_index not found."
    		self.assertTrue(msg in str(ex),
    			" 5000 error not recived as expected {0}".format(ex))

    def test_failure_create_index_existing_index(self):
    	self.indexes= self.input.param("indexes","").split(":")
    	query_definition = QueryDefinition(
    		index_name="test_failure_create_index_existing_index",
    		index_fields = self.indexes,
    		query_template = "",
    		groups = [])
    	self.query = query_definition.generate_index_create_query(bucket = self.buckets[0].name)
    	try:
    		# create index
    		server = self.get_nodes_from_services_map(service_type = "n1ql")
        	self.n1ql_helper.run_cbq_query(query = self.query, server = server)
    		# create same index again
    		self.n1ql_helper.run_cbq_query(query = self.query, server = server)
    	except Exception, ex:
    		self.assertTrue("Duplicate Index Name" in str(ex),
    			" 5000 error not recived as expected {0}".format(ex))
    	finally:
    		self.query = query_definition.generate_index_drop_query(bucket = self.buckets[0].name)
    		self.n1ql_helper.run_cbq_query(query = self.query, server = server)
