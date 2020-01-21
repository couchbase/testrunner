from .tuq import QueryTests
from membase.api.exception import CBQError
from lib.membase.api.rest_client import RestConnection
from pytests.fts.fts_base import CouchbaseCluster
from remote.remote_util import RemoteMachineShellConnection
import json
from pytests.security.rbac_base import RbacBase
from lib.remote.remote_util import RemoteMachineShellConnection
from deepdiff import DeepDiff

class N1qlFTSIntegrationTest(QueryTests):
    test_fts_query = "select primary_key from `test_bucket` where meta().id in " \
                          "(select raw ht.id from (SELECT result.hits FROM " \
                          "SEARCH_QUERY('idx_test_bucket_fts', " \
                          "{ 'explain' : FALSE, " \
                          "'fields' : [ 'string_field' ], " \
                          "'highlight' : {}, " \
                          "'query' : { 'query' : 'string_field:test_string' }, " \
                          "'size':100}) AS result) " \
                          "aa unnest aa.hits as ht) " \
                          "order by primary_key"
    test_fts_query_big_bucket = "select count(*) from `big_bucket` where meta().id in " \
                          "(select raw ht.id from (SELECT result.hits FROM " \
                          "SEARCH_QUERY('idx_big_bucket_fts', " \
                          "{ 'explain' : FALSE, " \
                          "'fields' : [ '_all' ], " \
                          "'highlight' : {}, " \
                          "'query' : { 'query' : 'age:>0' }, " \
                          "'size':120000}) AS result) " \
                          "aa unnest aa.hits as ht) "
    test_fts_alias_query = "select primary_key from `test_bucket` where meta().id in " \
                                "(select raw ht.id from (SELECT result.hits FROM " \
                                "SEARCH_QUERY('idx_test_bucket_fts_alias', " \
                                "{ 'explain' : FALSE, " \
                                "'fields' : [ 'string_field' ], " \
                                "'highlight' : {}, " \
                                "'query' : { 'query' : 'string_field:test_string' }, " \
                                "'size':100}) AS result) " \
                                "aa unnest aa.hits as ht) " \
                                "order by primary_key"
    test_n1ql_query = "select primary_key from test_bucket where string_field like 'test_string%' order by primary_key"


    n1ql_syntax_tests = {
        'select_term': {'fts_query':test_fts_query,
                       'fts_alias_query':test_fts_alias_query,
                       'n1ql_query':test_n1ql_query
                       },
        'from_select_term': {'fts_query':"select primary_key from `test_bucket` where meta().id in "
                    "("
                        "from ("
                                "SELECT result.hits FROM "
                                "SEARCH_QUERY('idx_test_bucket_fts', "
                                "{ 'explain' : FALSE, "
                                "  'fields' : [ 'string_field' ], "
                                "  'highlight' : {}, "
                                "  'query' : { 'query' : 'string_field:\"test_string\"' }, "
                                "  'size':100}) AS result"
                              ") aa unnest aa.hits as ht "
                    "select raw ht.id"
                    ") order by primary_key",
                       'fts_alias_query':"select primary_key from `test_bucket` where meta().id in "
                    "("
                        "from ("
                                "SELECT result.hits FROM "
                                "SEARCH_QUERY('idx_test_bucket_fts_alias', "
                                "{ 'explain' : FALSE, "
                                "  'fields' : [ 'string_field' ], "
                                "  'highlight' : {}, "
                                "  'query' : { 'query' : 'string_field:\"test_string\"' }, "
                                "  'size':100}) AS result"
                              ") aa unnest aa.hits as ht "
                    "select raw ht.id"
                    ") order by primary_key",
                       'n1ql_query':test_n1ql_query
                       },
        'subselect': {'fts_query': "select string_field, (select raw ht.id from (SELECT result.hits FROM "
                        "SEARCH_QUERY('idx_test_bucket_fts', "
                        "{ 'explain' : FALSE, "
                        "'fields' : [ 'string_field' ], "
                        "'highlight' : {}, "
                        "'query' : { 'query' : 'string_field:\"test_string\"' }, "
                        "'size':100}) AS result) "
                        "aa unnest aa.hits as ht order by ht.id) from test_bucket order by string_field limit 10",
                        'fts_alias_query': "select string_field, (select raw ht.id from (SELECT result.hits FROM "
                        "SEARCH_QUERY('idx_test_bucket_fts_alias', "
                        "{ 'explain' : FALSE, "
                        "'fields' : [ 'string_field' ], "
                        "'highlight' : {}, "
                        "'query' : { 'query' : 'string_field:\"test_string\"' }, "
                        "'size':100}) AS result) "
                        "aa unnest aa.hits as ht order by ht.id) from test_bucket order by string_field limit 10",
                        'n1ql_query': "select string_field, (select raw primary_key from  test_bucket where string_field like 'test_string%') from test_bucket a order by string_field limit 10"
                        },
        'where_condition_plus_let': {'fts_query': "SELECT primary_key FROM `test_bucket` "
                        "LET ids=(select raw ht.id from (SELECT result.hits FROM "
                                    "SEARCH_QUERY('idx_test_bucket_fts', "
                                    "{ 'explain' : FALSE, 'fields' : [ 'string_field' ], "
                                    "'highlight' : {}, 'query' : { 'query' : 'string_field:\"test_string\"' }, "
                                    "'size':100}) AS result) "
                                  "aa unnest aa.hits as ht) "
                    "WHERE meta().id in ids",
                        'fts_alias_query': "SELECT primary_key FROM `test_bucket` "
                        "LET ids=(select raw ht.id from (SELECT result.hits FROM "
                                    "SEARCH_QUERY('idx_test_bucket_fts_alias', "
                                    "{ 'explain' : FALSE, 'fields' : [ 'string_field' ], "
                                    "'highlight' : {}, 'query' : { 'query' : 'string_field:\"test_string\"' }, "
                                    "'size':100}) AS result) "
                                  "aa unnest aa.hits as ht) "
                    "WHERE meta().id in ids",
                        'n1ql_query': "SELECT primary_key FROM `test_bucket` "
                     "LET ids=(select raw meta().id from test_bucket a where string_field like 'test_string%') "
                     "WHERE meta().id in ids"
                        },
        'where_condition_from_term': {'fts_query': "select array_length(result.hits) cnt from search_query('idx_test_bucket_fts', "
                    "{ 'explain' : FALSE, 'fields' : [ 'string_field' ], "
                    "'highlight' : {}, 'query' : { 'query' : 'string_field:\"test_string\"' }, "
                    "'size':100}) as result",
                        'fts_alias_query': "select array_length(result.hits) cnt from search_query('idx_test_bucket_fts_alias', "
                    "{ 'explain' : FALSE, 'fields' : [ 'string_field' ], "
                    "'highlight' : {}, 'query' : { 'query' : 'string_field:\"test_string\"' }, "
                    "'size':100}) as result",
                        'n1ql_query': "select count(*) as cnt from test_bucket where string_field like 'test_string%'"
                        },
        'where_condition_in_from_select': {'fts_query': "select primary_key from `test_bucket` where meta().id in "
                    "( "
                    "   from ("
                    "           SELECT result.hits FROM "
                    "               SEARCH_QUERY('idx_test_bucket_fts', "
                    "                           {'explain' : FALSE, "
                    "                            'fields' : [ 'string_field' ], "
                    "                            'highlight' : {}, "
                    "                            'query' : { 'query' : 'string_field:\"test_string\"' }, "
                    "                            'size':100}) AS result "
                    "        ) aa unnest aa.hits as ht where ht.id='primary_key_0' select raw ht.id"
                    ") order by primary_key",
                        'fts_alias_query': "select primary_key from `test_bucket` where meta().id in "
                    "( "
                    "   from ("
                    "           SELECT result.hits FROM "
                    "               SEARCH_QUERY('idx_test_bucket_fts_alias', "
                    "                           {'explain' : FALSE, "
                    "                            'fields' : [ 'string_field' ], "
                    "                            'highlight' : {}, "
                    "                            'query' : { 'query' : 'string_field:\"test_string\"' }, "
                    "                            'size':100}) AS result "
                    "        ) aa unnest aa.hits as ht where ht.id='primary_key_0' select raw ht.id"
                    ") order by primary_key",
                        'n1ql_query': "select primary_key from `test_bucket` a where meta().id in (from ("
                     "      SELECT meta().id FROM test_bucket where meta().id='primary_key_0' ) ht "
                     "select raw ht.id) order by primary_key"
                        },
        'where_condition_plus_let_in_from_select': {'fts_query': "from ("
                    "       select meta().id from test_bucket let ids=("
                    "           select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY("
                    "                                                           'idx_test_bucket_fts', "
                    "                                                           { 'explain' : FALSE, 'fields' : [ 'string_field' ], "
                    "                                                           'highlight' : {}, 'query' : { 'query' : 'string_field:\"test_string\"' }, "
                    "                                                           'size':100}) AS result) "
                    "                                   aa unnest aa.hits as ht"
                    "           ) where meta().id in ids) a select a.*",
                        'fts_alias_query': "from ("
                    "       select meta().id from test_bucket let ids=("
                    "           select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY("
                    "                                                           'idx_test_bucket_fts_alias', "
                    "                                                           { 'explain' : FALSE, 'fields' : [ 'string_field' ], "
                    "                                                           'highlight' : {}, 'query' : { 'query' : 'string_field:\"test_string\"' }, "
                    "                                                           'size':100}) AS result) "
                    "                                   aa unnest aa.hits as ht"
                    "           ) where meta().id in ids) a select a.*",
                        'n1ql_query': "from ("
                     "      select meta().id from test_bucket let ids=(select raw meta().id from test_bucket b where string_field like 'test_string%') "
                     "      where meta().id in ids) a "
                     "select a.*"
                        },

    }

    prepareds_tests = {
        'test_name': {
            'prepared':"",
            'prepared_alias':"",
            'call_prepared':"",
            'call_prepared_alias':""
            },
        'simple_prepared': {
            'prepared': "'prepare prepared_stmt from select primary_key from `test_bucket` where meta().id in "
                                            "(select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY(\"idx_test_bucket_fts\", "
                                            "{ \"explain\" : FALSE, \"fields\" : [ \"string_field\" ], \"highlight\" : {}, "
                                            "\"query\" : { \"query\" : \"string_field:test_string\" }, "
                                            "\"size\":100}) AS result) aa unnest aa.hits as ht) order by primary_key'",
            'prepared_alias': "'prepare prepared_stmt_alias from select primary_key from `test_bucket` where meta().id in "
                                            "(select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY(\"idx_test_bucket_fts_alias\", "
                                            "{ \"explain\" : FALSE, \"fields\" : [ \"string_field\" ], \"highlight\" : {}, "
                                            "\"query\" : { \"query\" : \"string_field:test_string\" }, "
                                            "\"size\":100}) AS result) aa unnest aa.hits as ht) order by primary_key'",
            'call_prepared': "'execute prepared_stmt'",
            'call_prepared_alias': "'execute prepared_stmt_alias'"
        },
        'positional_prepared': {
            'prepared': "'prepare prepared_stmt from select primary_key from `test_bucket` where meta().id in "
                                        "(select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY($1, "
                                        "{ \"explain\" : $2, \"fields\" : [ $3 ], \"highlight\" : {}, "
                                        "\"query\" : { \"query\" : $4 }, "
                                        "\"size\":$5}) AS result) aa unnest aa.hits as ht) order by primary_key'",
            'prepared_alias': "'prepare prepared_stmt_alias from select primary_key from `test_bucket` where meta().id in "
                                        "(select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY($1, "
                                        "{ \"explain\" : $2, \"fields\" : [ $3 ], \"highlight\" : {}, "
                                        "\"query\" : { \"query\" : $4 }, "
                                        "\"size\":$5}) AS result) aa unnest aa.hits as ht) order by primary_key'",
            'call_prepared': "'execute prepared_stmt&args=[\"idx_test_bucket_fts\",false,\"string_field\",\"string_field:test_string\",100]'",
            'call_prepared_alias': "'execute prepared_stmt_alias&args=[\"idx_test_bucket_fts_alias\",false,\"string_field\",\"string_field:test_string\",100]'"
        },
        'named_prepared': {
            'prepared': "'prepare prepared_stmt from select primary_key from `test_bucket` where meta().id in "
                                        "(select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY($index_name, "
                                        "{ \"explain\" : $is_explain, \"fields\" : [ $fields ], \"highlight\" : {}, "
                                        "\"query\" : { \"query\" : $query }, "
                                        "\"size\":$size}) AS result) aa unnest aa.hits as ht) order by primary_key'",
            'prepared_alias': "'prepare prepared_stmt_alias from select primary_key from `test_bucket` where meta().id in "
                                        "(select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY($index_name, "
                                        "{ \"explain\" : $is_explain, \"fields\" : [ $fields ], \"highlight\" : {}, "
                                        "\"query\" : { \"query\" : $query }, "
                                        "\"size\":$size}) AS result) aa unnest aa.hits as ht) order by primary_key'",
            'call_prepared': "'execute prepared_stmt&$index_name=\"idx_test_bucket_fts\"&$is_explain=false&$fields=\"string_field\"&$query=\"string_field:test_string\"&$size=100'",
            'call_prepared_alias': "'execute prepared_stmt_alias&$index_name=\"idx_test_bucket_fts_alias\"&$is_explain=false&$fields=\"string_field\"&$query=\"string_field:test_string\"&$size=100'"
        },
    }

    parameterized_queries_test = {
        'test_name': {
            'prepared': "",
            'prepared_alias': ""
        },
        'named': {
            'statement': "'select primary_key from `test_bucket` where meta().id in "
                         "(select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY($index_name, "
                         "{ \"explain\" : $is_explain, \"fields\" : [ $fields ], \"highlight\" : {}, "
                         "\"query\" : { \"query\" : $query }, "
                         "\"size\":$size}) AS result) aa unnest aa.hits as ht) order by primary_key&$index_name=\"idx_test_bucket_fts\"&$is_explain=false&$fields=\"string_field\"&$query=\"string_field:test_string\"&$size=100'",
            'statement_alias': "'select primary_key from `test_bucket` where meta().id in "
                               "(select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY($index_name, "
                               "{ \"explain\" : $is_explain, \"fields\" : [ $fields ], \"highlight\" : {}, "
                               "\"query\" : { \"query\" : $query }, "
                               "\"size\":$size}) AS result) aa unnest aa.hits as ht) order by primary_key&$index_name=\"idx_test_bucket_fts_alias\"&$is_explain=false&$fields=\"string_field\"&$query=\"string_field:test_string\"&$size=100'"
        },
        'positional': {
            'statement': "'select primary_key from `test_bucket` where meta().id in "
                         "(select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY($1, "
                         "{ \"explain\" : $2, \"fields\" : [ $3 ], \"highlight\" : {}, "
                         "\"query\" : { \"query\" : $4 }, "
                         "\"size\":$5}) AS result) aa unnest aa.hits as ht) order by primary_key&args=[\"idx_test_bucket_fts\",false,\"string_field\",\"string_field:test_string\",100]'",
            'statement_alias': "'select primary_key from `test_bucket` where meta().id in "
                               "(select raw ht.id from (SELECT result.hits FROM SEARCH_QUERY($1, "
                               "{ \"explain\" : $2, \"fields\" : [ $3 ], \"highlight\" : {}, "
                               "\"query\" : { \"query\" : $4 }, "
                               "\"size\":$5}) AS result) aa unnest aa.hits as ht) order by primary_key&args=[\"idx_test_bucket_fts_alias\",false,\"string_field\",\"string_field:test_string\",100]'",
        },
    }

    test_bucket_size = 999
    users = {}
    fts_indexes = {}
    cbcluster = None


    def setUp(self):
        super(N1qlFTSIntegrationTest, self).setUp()
        self._init_nodes()
        self._load_test_data(number_of_records=self.test_bucket_size)
        self._open_curl_access()
        self._create_all_indexes()
        self._create_all_users()
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationTest setup has started ==============")
        self.log.info("==============  N1qlFTSIntegrationTest setup has completed ==============")


    def tearDown(self):
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationTest tearDown has started ==============")
        super(N1qlFTSIntegrationTest, self).tearDown()
        self.log.info("==============  N1qlFTSIntegrationTest tearDown has completed ==============")


    def suite_setUp(self):
        super(N1qlFTSIntegrationTest, self).suite_setUp()


    def suite_tearDown(self):
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationTest suite_tearDown has started ==============")
        super(N1qlFTSIntegrationTest, self).suite_tearDown()
        self.log.info("==============  N1qlFTSIntegrationTest suite_tearDown has completed ==============")

    #=================== Tests ============================

    ''' Test for user permissions - user is granted to run select queries and fts searches. '''
    def test_rbac_full_grants(self):
        user = self.input.param("username", '')
        username = self.users[user]['username']
        password = self.users[user]['password']

        services_map = self._get_services_map()
        master_result = self._validate_query_against_node(self.master, services_map, self.test_fts_query, self.test_n1ql_query, username=username, password=password)
        fts_node_result = self._validate_query_against_node(self.servers[1], services_map, self.test_fts_query, self.test_n1ql_query, username=username, password=password)
        self.assertEqual(master_result, True, username+" query run failed on non-fts node")
        self.assertEqual(fts_node_result, True, username+" query run failed on fts node")

        master_result = self._validate_query_against_node(self.master, services_map, self.test_fts_alias_query, self.test_n1ql_query, username=username, password=password)
        fts_node_result = self._validate_query_against_node(self.servers[1], services_map, self.test_fts_alias_query, self.test_n1ql_query, username=username, password=password)
        self.assertEqual(master_result, True, username+" alias query run failed on non-fts node")
        self.assertEqual(fts_node_result, True, username+" alias query run failed on fts node")


    ''' Test for user permissions - user is granted to run select queries and is not granted to run fts searches'''
    def test_rbac_not_granted_fts(self):
        user = self.input.param("username", '')
        username = self.users[user]['username']
        password = self.users[user]['password']

        services_map = self._get_services_map()

        master_result = self._validate_query_against_node(self.master, services_map, self.test_fts_query, self.test_n1ql_query, username=username, password=password)
        fts_node_result = self._validate_query_against_node(self.servers[1], services_map, self.test_fts_query, self.test_n1ql_query, username=username, password=password)
        self.assertEqual(master_result, False, username+" query run failed on non-fts node")
        self.assertEqual(fts_node_result, False, username+" query run failed on fts node")

        alias_master_result = self._validate_query_against_node(self.master, services_map, self.test_fts_alias_query, self.test_n1ql_query, username=username, password=password)
        alias_fts_node_result = self._validate_query_against_node(self.servers[1], services_map, self.test_fts_alias_query, self.test_n1ql_query, username=username, password=password)
        self.assertEqual(alias_master_result, False, username+" alias query run failed on non-fts node")
        self.assertEqual(alias_fts_node_result, False, username+" alias query run failed on fts node")


    ''' Test for user permissions - user is not granted to run select queries'''
    def test_rbac_not_granted_n1ql(self):
        user = self.input.param("username", '')
        username = self.users[user]['username']
        password = self.users[user]['password']

        errors_count = 0
        try:
            self._run_query_against_node(self.master, self.test_fts_query, username=username, password=password)
        except CBQError as e:
            if "User does not have credentials to run SELECT queries" in str(e):
                errors_count = errors_count+1
        try:
            self._run_query_against_node(self.servers[1], self.test_fts_query, username=username, password=password)
        except CBQError as e:
            if "User does not have credentials to run SELECT queries" in str(e):
                errors_count = errors_count+1
        self.assertEqual(errors_count, 2, username+" query run failed.")

        errors_count = 0
        try:
            self._run_query_against_node(self.master, self.test_fts_alias_query, username=username, password=password)
        except CBQError as e:
            if "User does not have credentials to run SELECT queries" in str(e):
                errors_count = errors_count+1
        try:
            self._run_query_against_node(self.servers[1], self.test_fts_alias_query, username=username, password=password)
        except CBQError as e:
            if "User does not have credentials to run SELECT queries" in str(e):
                errors_count = errors_count+1
        self.assertEqual(errors_count, 2, username+" alias query run failed.")


    ''' Test mixed cluster configuration specified below. 
        Test queries will be run against all nodes with n1ql service.
        node1:['data']
        node2:['index', 'query', 'search'] '''
    def test_mixed_cluster(self):
        services_map = self._get_services_map()
        for node in self.servers:
            test_result = self._validate_query_against_node(node, services_map, self.test_fts_query, self.test_n1ql_query)
            self.assertEqual(test_result, True, "Node " + str(node) + " test is failed.")

            test_result = self._validate_query_against_node(node, services_map, self.test_fts_alias_query,
                                                       self.test_n1ql_query)
            self.assertEqual(test_result, True, "Node " + str(node) + " alias test is failed.")


    ''' N1QL syntax check'''
    def test_n1ql_syntax(self):
        test_name = self.input.param("test_name", '')
        test_fts_query = self.n1ql_syntax_tests[test_name]['fts_query']
        test_fts_alias_query = self.n1ql_syntax_tests[test_name]['fts_alias_query']
        test_n1ql_query = self.n1ql_syntax_tests[test_name]['n1ql_query']
        services_map = self._get_services_map()

        for node in self.servers:
            test_result = self._validate_query_against_node(node, services_map, test_fts_query, test_n1ql_query)
            self.assertEqual(test_result, True, "Node "+str(node)+" test "+test_name+" is failed.")

            test_result = self._validate_query_against_node(node, services_map, test_fts_alias_query, test_n1ql_query)
            self.assertEqual(test_result, True, "Node "+str(node)+" alias test "+test_name+" is failed.")


    ''' N1QL syntax check - UNION/INTERSECT/EXCEPT '''
    def test_n1ql_syntax_union_intersect_except_correct_indexes(self):
        tests = {
            "test_union": {
                "operator": "union",
                "left_set_value": "test_string",
                "right_set_value": "string data 1"
            },
            "test_intersect": {
                "operator": "intersect",
                "left_set_value": "test_string",
                "right_set_value": "test_string"
            },
            "test_except": {
                "operator": "except",
                "left_set_value": "test_string",
                "right_set_value": "string data 1"
            },
        }
        all_distinct = [' ', ' all ']
        services_map = self._get_services_map()
        test_name = self.input.param("test_name", '')
        operator = tests[test_name]['operator']
        left_set_value = tests[test_name]['left_set_value']
        right_set_value = tests[test_name]['right_set_value']

        for a_d in all_distinct:
            for idx1 in ['idx_test_bucket_fts', 'idx_test_bucket_fts_alias', 'idx_test_bucket_fts-copy', 'idx_test_bucket_fts-copy_alias']:
                for idx2 in ['idx_test_bucket_fts', 'idx_test_bucket_fts_alias', 'idx_test_bucket_fts-copy', 'idx_test_bucket_fts-copy_alias']:
                    fts_query = ("select * from (select * from ( select raw ht.id from (SELECT result.hits FROM "
                                     "       SEARCH_QUERY('" + idx1 + "',"
                                     "       { 'explain' : FALSE, 'fields' : [ 'string_field' ],"
                                     "       'highlight' : {}, 'query' : { 'query' : 'string_field:\""+left_set_value+"\"' },"
                                     "       'size':100}) AS result) "
                                     "       aa unnest aa.hits as ht order by ht.id) q1 "
                                     " " + operator + " " + a_d + " "
                                     " select * from (select raw ht.id from (SELECT result.hits FROM "
                                     "       SEARCH_QUERY('" + idx2 + "',"
                                     "       { 'explain' : FALSE, 'fields' : [ 'string_field' ],"
                                     "       'highlight' : {}, 'query' : { 'query' : 'string_field:\""+right_set_value+"\"' },"
                                     "       'size':100}) AS result) "
                                     "aa unnest aa.hits as ht order by ht.id) q1 ) a order by q1")
                    n1ql_query = ("select * from (select * from (select raw meta().id from test_bucket where string_field like '"+left_set_value+"%') q1"
                                     " " + operator + " "+ a_d + " " 
                                     "select * from (select raw meta().id from test_bucket where string_field like '"+right_set_value+"%') q1) a order by q1")

                    for node in self.servers:
                        test_result = self._validate_query_against_node(node, services_map, fts_query, n1ql_query)
                        self.assertEqual(test_result, True, "Node " + str(node) + " test is failed.")


    ''' N1QL syntax check - UNION/INTERSECT/EXCEPT, invalid index for both subselects '''
    def test_n1ql_syntax_union_intersect_except_invalid_index(self):
        tests = {
            "test_union": {
                "operator": "union",
                "left_set_value": "test_string",
                "right_set_value": "string data 1"
            },
            "test_intersect": {
                "operator": "intersect",
                "left_set_value": "test_string",
                "right_set_value": "test_string"
            },
            "test_except": {
                "operator": "except",
                "left_set_value": "test_string",
                "right_set_value": "string data 1"
            },
        }
        all_distinct = [' ', ' all ']
        fts_indexes = ['idx_test_bucket_fts-invalid']
        services_map = self._get_services_map()
        test_name = self.input.param("test_name", '')
        operator = tests[test_name]['operator']
        left_set_value = tests[test_name]['left_set_value']
        right_set_value = tests[test_name]['right_set_value']

        for a_d in all_distinct:
            for fts_idx in fts_indexes:
                fts_query = "select * from (select * from (select raw ht.id from (SELECT result.hits FROM " \
                            "       SEARCH_QUERY('"+fts_idx+"'," \
                            "       { 'explain' : FALSE, 'fields' : [ 'string_field' ]," \
                            "       'highlight' : {}, 'query' : { 'query' : 'string_field:\""+left_set_value+"\"' }," \
                            "       'size':100}) AS result) " \
                            "       aa unnest aa.hits as ht order by ht.id) q1 " \
                            + operator + a_d + \
                            " select * from (select raw ht.id from (SELECT result.hits FROM " \
                            "       SEARCH_QUERY('"+fts_idx+"'," \
                            "       { 'explain' : FALSE, 'fields' : [ 'string_field' ]," \
                            "       'highlight' : {}, 'query' : { 'query' : 'string_field:\""+right_set_value+"\"' }," \
                            "       'size':100}) AS result) " \
                            "aa unnest aa.hits as ht order by ht.id ) q1) a order by q1"
                n1ql_query = "select * from (select * from (select raw meta().id from test_bucket where string_field like '"+left_set_value+"%') q1 " \
                             + operator + a_d + \
                             "select * from ( select raw meta().id from test_bucket where string_field like '"+right_set_value+"%') q1) a order by q1"
                for node in self.servers:
                    test_result = self._validate_query_against_node(node, services_map, fts_query, n1ql_query)
                    self.assertEqual(test_result, False, "Node " + str(node) + " test is not failed.")


    ''' N1QL syntax check - ALL/DISTINCT RAW/ELEMENT/VALUE '''
    def test_n1ql_syntax_all_distinct_raw_element_value(self):
        all_distinct = [' all ', ' distinct ']
        raw_element_value = [' raw ', ' element ', ' value ']
        services_map = self._get_services_map()

        for a_d in all_distinct:
            for r_e_v in raw_element_value:
                for idx in ['idx_test_bucket_fts', 'idx_test_bucket_fts_alias']:
                    fts_query = "select " + a_d + r_e_v + " ht.id from (SELECT result.hits FROM " \
                                "   SEARCH_QUERY('"+idx+"'," \
                                "   { 'explain' : FALSE, 'fields' : [ 'string_field' ]," \
                                "   'highlight' : {}, 'query' : { 'query' : 'string_field:\"test_string\"' }," \
                                "   'size':100}) AS result) " \
                                "aa unnest aa.hits as ht order by ht.id"
                    n1ql_query = "select " + a_d + r_e_v + " ht.id from (SELECT meta().id from test_bucket where string_field like 'test_string%') ht order by ht.id"
                    for node in self.servers:
                        test_result = self._validate_query_against_node(node, services_map, fts_query, n1ql_query)
                        self.assertEqual(test_result, True, "Node " + str(node) + " test is failed.")


    ''' Test idea: initially have 3-nodes cluster, containing 2 fts nodes.
        Create replica for fts index
        Get results for fts index and fts alias queries against 1'st node.
        Failover 3'rd node.
        Get results for fts index and fts alias queries against 1'st node.
        Compare with initial results
        Add 3'rd node back, rebalance
        Get results for fts index and fts alias queries against 1'st node.
        Compare with initial results.        
        '''
    def test_clusterops_fts_node_failover(self):
        self._update_replica_for_fts_index(self.fts_indexes['index1'], 1)
        self.sleep(30)
        services_string = self.input.param("services_init", '')
        services = services_string.split("-")
        for service in services:
            service.replace(":", ",")
        idx_result_node1_before_failover = self._run_query_against_node(self.servers[0], self.test_fts_query)


        alias_result_node1_before_failover = self._run_query_against_node(self.servers[0], self.test_fts_alias_query)
        self.assertEqual(idx_result_node1_before_failover == alias_result_node1_before_failover, True, "Results before failover are not the same.")

        self.cluster.failover(servers=self.servers, failover_nodes=[self.servers[2]], graceful=False)

        idx_result_node1_after_failover = self._run_query_against_node(self.servers[0], self.test_fts_query)
        alias_result_node1_after_failover = self._run_query_against_node(self.servers[0], self.test_fts_alias_query)
        self.assertEqual(idx_result_node1_before_failover == idx_result_node1_after_failover and
                          idx_result_node1_before_failover == alias_result_node1_after_failover, True, "Results after failover are not the same.")

        self.cluster.rebalance(self.servers, [], [self.servers[2]])
        self.cluster.rebalance(self.servers, [self.servers[2]], [], services=[services[2]])

        idx_result_node1_after_rebalance = self._run_query_against_node(self.servers[0], self.test_fts_query)
        alias_result_node1_after_rebalance = self._run_query_against_node(self.servers[0], self.test_fts_alias_query)

        self.assertEqual(idx_result_node1_before_failover == idx_result_node1_after_rebalance and
                          idx_result_node1_before_failover == alias_result_node1_after_rebalance, True, "Results after rebalance are not the same.")



    ''' Test idea: initially have 4-nodes cluster, 1,4 - non-fts nodes and 2,3 - fts nodes.
        Create replica for fts index.
        Get results for fts index and fts alias queries against 2 and 3 nodes.
        Failover 2 node. 
        Check that index and alias query results against node 3 remain the same.
        Add fts service to node 4, add node 4 to cluster, rebalance.
        Check that index and alias queries results remains the same with initial results against node 2.'''
    def test_clusterops_multiple_fts_nodes_failover(self):
        self._update_replica_for_fts_index(self.fts_indexes['index1'], 1)

        idx_result_node2_before_failover = self._run_query_against_node(self.servers[1], self.test_fts_query)
        alias_result_node2_before_failover = self._run_query_against_node(self.servers[1], self.test_fts_alias_query)
        idx_result_node3_before_failover = self._run_query_against_node(self.servers[2], self.test_fts_query)
        alias_result_node3_before_failover = self._run_query_against_node(self.servers[2], self.test_fts_alias_query)

        self.assertEqual(idx_result_node2_before_failover == alias_result_node2_before_failover and
                          idx_result_node2_before_failover == idx_result_node3_before_failover and
                          idx_result_node2_before_failover == alias_result_node3_before_failover, True, "Results before failover are not the same.")

        self.cluster.failover(servers=self.servers, failover_nodes=[self.servers[1]], graceful=False)

        idx_result_node3_after_failover = self._run_query_against_node(self.servers[2], self.test_fts_query)
        alias_result_node3_after_failover = self._run_query_against_node(self.servers[2], self.test_fts_alias_query)

        self.assertEqual(idx_result_node2_before_failover == idx_result_node3_after_failover and
                          idx_result_node2_before_failover == alias_result_node3_after_failover, True, "Results after failover are not the same.")

        self.cluster.rebalance(self.servers, [], [self.servers[1], self.servers[3]], services=[])
        self.cluster.rebalance(self.servers, [self.servers[3]], [], services=['fts,n1ql'])

        idx_result_node4_after_rebalance = self._run_query_against_node(self.servers[3], self.test_fts_query)
        alias_result_node4_after_rebalance = self._run_query_against_node(self.servers[3], self.test_fts_alias_query)

        self.assertEuqals(idx_result_node2_before_failover == idx_result_node4_after_rebalance and
                          idx_result_node2_before_failover == alias_result_node4_after_rebalance, True, "Results after rebalance are not the same.")


    ''' Non-parameterized prepared statement call against fts and non-fts nodes. '''
    def test_prepareds(self):
        test_name = self.input.param("test_name", '')
        prepared_statement = self.prepareds_tests[test_name]['prepared']
        prepared_statement_alias = self.prepareds_tests[test_name]['prepared_alias']
        call_prepared = self.prepareds_tests[test_name]['call_prepared']
        call_prepared_alias = self.prepareds_tests[test_name]['call_prepared_alias']

        for server in [self.master, self.servers[1]]:
            self.run_cbq_query("delete from system:prepareds")

            # ------------------ create simple prepared -----------------------
            self.run_cbq_query_curl(query=prepared_statement, server=server)
            self.sleep(5)
            # -------------- execute simple prepared -----------------------
            prepared_results = self.run_cbq_query_curl(query=call_prepared, server=server)['results']
            test_result = self._run_query_against_node(self.master, self.test_fts_query)
            self.assertEqual(test_result==prepared_results, True, "Node " + str(self.master) + " test is failed.")
            # ------------------ create simple prepared using index alias -----------------------
            self.run_cbq_query_curl(query=prepared_statement_alias, server=server)
            self.sleep(5)
            # -------------- execute simple prepared using index alias -----------------------
            prepared_results = self.run_cbq_query_curl(query=call_prepared_alias, server=server)['results']
            test_result = self._run_query_against_node(self.master, self.test_fts_alias_query)
            self.assertEqual(test_result==prepared_results, True)


    ''' Test for parameterized query with named parameters. '''
    def test_parameterized_query(self):
        test_name = self.input.param("test_name", '')

        statement = self.parameterized_queries_test[test_name]['statement']
        statement_alias = self.parameterized_queries_test[test_name]['statement_alias']

        for server in [self.master, self.servers[1]]:
            prepared_results = self.run_cbq_query_curl(query=statement, server=server)['results']
            test_result = self._run_query_against_node(server, self.test_fts_query)
            self.assertEqual(test_result==prepared_results, True, "Node " + str(server) + " test is failed.")

            prepared_results = self.run_cbq_query_curl(query=statement_alias, server=server)['results']
            test_result = self._run_query_against_node(server, self.test_fts_alias_query)
            self.assertEqual(test_result==prepared_results, True, "Node " + str(server) + " alias test is failed.")


    ''' Test how bleveMaxResultWindow variable change reflects count of search results for SEARCH_QUERY() query. '''
    def test_bleve_max_result_window(self):
        shell = RemoteMachineShellConnection(self.servers[1])
        cmd = ("curl -XPUT -H \"Content-type:Application/json\" http://"+self.servers[1].rest_username+":" +
        self.servers[1].rest_password+"@"+self.servers[1].ip+":8094/api/managerOptions -d '{\"bleveMaxResultWindow\":\""+str(1000000)+"\"}'")

        shell.execute_command(cmd)
        fts_result = self.run_cbq_query(query=self.test_fts_query_big_bucket, server=self.servers[1])['results'][0]['$1']
        n1ql_result = self.run_cbq_query("select count(*) from big_bucket where age>0")['results'][0]['$1']
        self.assertEqual(fts_result, n1ql_result, "Test is failed.")

    def test_facets_query(self):
        self.rest.load_sample("beer-sample")
        self.wait_for_buckets_status({"beer-sample": "healthy"}, 5, 120)
        self.wait_for_bucket_docs({"beer-sample": 7303}, 5, 120)
        idx1 = self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name="beer-sample")

        fts_request = {"explain" : False,
                       "fields" : ["country"],
                       "highlight" : {},
                       "facets": {"states": {"size": 1000, "field":"state"}},
                       "query" : {"query": "country:'United States'"},
                       "size":100}

        query = "select * from SEARCH_QUERY(\"idx_beer_sample_fts\", " + json.dumps(fts_request) + ") a"
        result = self.run_cbq_query(query)
        n1ql_facets = result['results'][0]['a']['facets']
        rest = RestConnection(self.servers[1])
        total_hits, hits, took, status, fts_facets = \
            rest.run_fts_query_with_facets(index_name="idx_beer_sample_fts",
                               query_json = fts_request)
        self.assertEqual(fts_facets['states']['terms'], n1ql_facets['states']['terms'], "Facets are not the same for n1ql and fts requests.")


    # ================================= Utils =============================

    def _get_services_map(self):
        rest = RestConnection(self.servers[0])
        return rest.get_nodes_services()

    def _validate_query_against_node(self, node=None, services_map=None, fts_query=None, n1ql_query=None, username=None, password=None):
        node_ip = node.ip
        node_port = node.port
        if str(node_ip)+":"+str(node_port) in list(services_map.keys()):
            node_services = services_map[str(node_ip)+":"+str(node_port)]

            # https://issues.couchbase.com/browse/MB-32999
            # when this issue will be fixed, fts service will be no longer required.
            if 'n1ql' in node_services:
                fts_result = self.run_cbq_query(query=fts_query, server=node, username=username, password=password)
                n1ql_result = self.run_cbq_query(query=n1ql_query, server=node, username=username, password=password)
                diffs = DeepDiff(fts_result['results'], n1ql_result['results'], ignore_order=True)
                if diffs:
                    self.log.info("Diffs: "+diffs)
                    return False

        return True

    def _run_query_against_node(self, node, fts_query, username=None, password=None):
        fts_result = self.run_cbq_query(query=fts_query, server=node, username=username, password=password)
        return fts_result['results']

    # ================================= Init =============================

    def _init_nodes(self):
        test_bucket_params = self._create_bucket_params(server=self.master, size=self.bucket_size,
                                                        replicas=self.num_replicas, bucket_type=self.bucket_type,
                                                        enable_replica_index=self.enable_replica_index,
                                                        eviction_policy=self.eviction_policy, lww=self.lww)
        self.cluster.create_standard_bucket("test_bucket", 11222, test_bucket_params)
        self.cluster.create_standard_bucket("big_bucket", 11222, test_bucket_params)


    def _load_test_data(self, bucket_name='test_bucket', number_of_records=999):
        for i in range(0, number_of_records, 1):
            if i%100 == 0:
                initial_statement = (" INSERT INTO {0} (KEY, VALUE) VALUES ('primary_key_"+str(i)+"',").format(bucket_name)
                initial_statement += "{"
                initial_statement += "'primary_key':'primary_key_"+str(i) + "','string_field': 'test_string " + str(i) + "','int_field':"+str(i)+"})"
            else:
                initial_statement = (" INSERT INTO {0} (KEY, VALUE) VALUES ('primary_key_"+str(i)+"',").format(bucket_name)
                initial_statement += "{"
                initial_statement += "'primary_key':'primary_key_"+str(i) + "','string_field': 'string data " + str(i) + "','int_field':"+str(i)+"})"
            self.run_cbq_query(initial_statement)

        shell = RemoteMachineShellConnection(self.master)
        shell.execute_cbworkloadgen(self.username, self.password, 15000, 100, "big_bucket", 1024, '-j')
        self.run_cbq_query("CREATE PRIMARY INDEX `#primary` ON `test_bucket`")
        self.run_cbq_query("CREATE PRIMARY INDEX `#primary` ON `big_bucket`")
        self.wait_for_all_indexes_online()

    def _create_all_indexes(self):
        idx1 = self._create_fts_index(index_name="idx_test_bucket_fts", doc_count=self.test_bucket_size)
        self.fts_indexes['index1'] = idx1

        idx1_alias = self._create_alias(idx1, idx1.name+"_alias")
        self.fts_indexes['index1_alias'] = idx1_alias

        idx1_copy = self._create_fts_index(index_name='idx_test_bucket_fts-copy', doc_count=self.test_bucket_size)
        self.fts_indexes['index1_copy'] = idx1_copy

        idx1_copy_alias = self._create_alias(idx1_copy, idx1_copy.name+"_alias")
        self.fts_indexes['index1_copy_alias'] = idx1_copy_alias

        big_bucket_idx = self._create_fts_index(index_name='idx_big_bucket_fts', doc_count=15000, source_name='big_bucket')
        self.fts_indexes['big_bucket_idx'] = big_bucket_idx


    def _create_fts_index(self, index_name='idx_test_bucket_fts', doc_count=0, source_name='test_bucket'):
        cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_index = cbcluster.create_fts_index(name=index_name, source_name=source_name)

        indexed_doc_count = 0
        while indexed_doc_count < doc_count:
            try:
                indexed_doc_count = fts_index.get_indexed_doc_count()
            except KeyError as k:
                continue

        return fts_index

    def _create_alias(self, target_index, name):
        cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        alias_def = {"targets": {}}
        alias_def['targets'][target_index.name] = {}
        return cbcluster.create_fts_index(name=name, index_type='fulltext-alias', index_params=alias_def)

    def _update_replica_for_fts_index(self, idx, replicas):
        idx.update_num_replicas(replicas)

    def _open_curl_access(self):
        shell = RemoteMachineShellConnection(self.master)

        cmd = (self.curl_path+' -u '+self.master.rest_username+':'+self.master.rest_password+' http://'+self.master.ip+':'+self.master.port+'/settings/querySettings/curlWhitelist -d \'{"all_access":true}\'')
        shell.execute_command(cmd)

    def _create_all_users(self):
        admin_user = [{'id': 'admin_user', 'name': 'admin_user', 'password': 'password'}]
        rolelist = [{'id': 'admin_user', 'name': 'admin_user', 'roles': 'admin'}]
        RbacBase().create_user_source(admin_user, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['admin_user'] = {'username': 'admin_user', 'password': 'password'}

        all_buckets_data_reader_search_admin = [{'id': 'all_buckets_data_reader_search_admin', 'name': 'all_buckets_data_reader_search_admin', 'password': 'password'}]
        rolelist = [{'id': 'all_buckets_data_reader_search_admin', 'name': 'all_buckets_data_reader_search_admin', 'roles': 'query_select[*],fts_admin[*],query_external_access'}]
        RbacBase().create_user_source(all_buckets_data_reader_search_admin, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_data_reader_search_admin'] = {'username': 'all_buckets_data_reader_search_admin', 'password': 'password'}

        all_buckets_data_reader_search_reader = [{'id': 'all_buckets_data_reader_search_reader', 'name': 'all_buckets_data_reader_search_reader', 'password': 'password'}]
        rolelist = [{'id': 'all_buckets_data_reader_search_reader', 'name': 'all_buckets_data_reader_search_reader', 'roles': 'query_select[*],fts_searcher[*],query_external_access'}]
        RbacBase().create_user_source(all_buckets_data_reader_search_reader, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_data_reader_search_reader'] = {'username': 'all_buckets_data_reader_search_reader', 'password': 'password'}

        test_bucket_data_reader_search_admin = [{'id': 'test_bucket_data_reader_search_admin', 'name': 'test_bucket_data_reader_search_admin', 'password': 'password'}]
        rolelist = [{'id': 'test_bucket_data_reader_search_admin', 'name': 'test_bucket_data_reader_search_admin', 'roles': 'query_select[test_bucket],fts_admin[test_bucket],query_external_access'}]
        RbacBase().create_user_source(test_bucket_data_reader_search_admin, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_data_reader_search_admin'] = {'username': 'test_bucket_data_reader_search_admin', 'password': 'password'}

        test_bucket_data_reader_null = [{'id': 'test_bucket_data_reader_null', 'name': 'test_bucket_data_reader_null', 'password': 'password'}]
        rolelist = [{'id': 'test_bucket_data_reader_null', 'name': 'test_bucket_data_reader_null', 'roles': 'query_select[test_bucket],query_external_access'}]
        RbacBase().create_user_source(test_bucket_data_reader_null, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_data_reader_null'] = {'username': 'test_bucket_data_reader_null', 'password': 'password'}

        test_bucket_data_reader_search_reader = [{'id': 'test_bucket_data_reader_search_reader', 'name': 'test_bucket_data_reader_search_reader', 'password': 'password'}]
        rolelist = [{'id': 'test_bucket_data_reader_search_reader', 'name': 'test_bucket_data_reader_search_reader', 'roles': 'query_select[test_bucket],fts_searcher[test_bucket],query_external_access'}]
        RbacBase().create_user_source(test_bucket_data_reader_search_reader, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_data_reader_search_reader'] = {'username': 'test_bucket_data_reader_search_reader', 'password': 'password'}

        all_buckets_data_reader_null = [{'id': 'all_buckets_data_reader_null', 'name': 'all_buckets_data_reader_null', 'password': 'password'}]
        rolelist = [{'id': 'all_buckets_data_reader_null', 'name': 'all_buckets_data_reader_null', 'roles': 'query_select[*],query_external_access'}]
        RbacBase().create_user_source(all_buckets_data_reader_null, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_data_reader_null'] = {'username': 'all_buckets_data_reader_null', 'password': 'password'}

        all_buckets_null_search_admin = [{'id': 'all_buckets_null_search_admin', 'name': 'all_buckets_null_search_admin', 'password': 'password'}]
        rolelist = [{'id': 'all_buckets_null_search_admin', 'name': 'all_buckets_null_search_admin', 'roles': 'fts_admin[*],query_external_access'}]
        RbacBase().create_user_source(all_buckets_null_search_admin, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_null_search_admin'] = {'username': 'all_buckets_null_search_admin', 'password': 'password'}

        all_buckets_null_null = [{'id': 'all_buckets_null_null', 'name': 'all_buckets_null_null', 'password': 'password'}]
        rolelist = [{'id': 'all_buckets_null_null', 'name': 'all_buckets_null_null', 'roles': 'query_external_access'}]
        RbacBase().create_user_source(all_buckets_null_null, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_null_null'] = {'username': 'all_buckets_null_null', 'password': 'password'}

        all_buckets_null_search_reader = [{'id': 'all_buckets_null_search_reader', 'name': 'all_buckets_null_search_reader', 'password': 'password'}]
        rolelist = [{'id': 'all_buckets_null_search_reader', 'name': 'all_buckets_null_search_reader', 'roles': 'fts_searcher[*],query_external_access'}]
        RbacBase().create_user_source(all_buckets_null_search_reader, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_null_search_reader'] = {'username': 'all_buckets_null_search_reader', 'password': 'password'}

        test_bucket_null_search_admin = [{'id': 'test_bucket_null_search_admin', 'name': 'test_bucket_null_search_admin', 'password': 'password'}]
        rolelist = [{'id': 'test_bucket_null_search_admin', 'name': 'test_bucket_null_search_admin', 'roles': 'fts_admin[test_bucket],query_external_access'}]
        RbacBase().create_user_source(test_bucket_null_search_admin, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_null_search_admin'] = {'username': 'test_bucket_null_search_admin', 'password': 'password'}

        test_bucket_null_search_reader = [{'id': 'test_bucket_null_search_reader', 'name': 'test_bucket_null_search_reader', 'password': 'password'}]
        rolelist = [{'id': 'test_bucket_null_search_reader', 'name': 'test_bucket_null_search_reader', 'roles': 'fts_searcher[test_bucket],query_external_access'}]
        RbacBase().create_user_source(test_bucket_null_search_reader, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_null_search_reader'] = {'username': 'test_bucket_null_search_reader', 'password': 'password'}

        test_bucket_null_null = [{'id': 'test_bucket_null_null', 'name': 'test_bucket_null_null', 'password': 'password'}]
        rolelist = [{'id': 'test_bucket_null_null', 'name': 'test_bucket_null_null', 'roles': 'query_external_access'}]
        RbacBase().create_user_source(test_bucket_null_null, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_null_null'] = {'username': 'test_bucket_null_null', 'password': 'password'}


