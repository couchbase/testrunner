from .base_test_rqg import BaseRQGTests
from .new_rqg_mysql_client import RQGMySQLClientNew
from .new_rqg_query_helper import RQGQueryHelperNew
import threading
from .rqg_mysql_client import RQGMySQLClient
from .rqg_postgres_client import RQGPostgresClient
import traceback
from deepdiff import DeepDiff

class RQGTestsNew(BaseRQGTests):

    ''' Call of super class setUp() function with defining new input parameter: test_name.
        This parameter will be used in dispatcher function to identify desired transformation logic. '''
    def setUp(self):
        super(RQGTestsNew, self).setUp()
        self.test_name = self.input.param("test_name", "")
        self.debug_logging = self.input.param("debug_logging", True)
        self.use_new_rqg = self.input.param("use_new_rqg", False)

    def tearDown(self):
        super(RQGTestsNew, self).tearDown()

    def test_rqg(self):
        super(RQGTestsNew, self).test_rqg()

    def _rqg_worker(self, table_name, table_map, input_queue, result_queue, failure_record_queue=None):
        count = 0
        while True:
            if self.total_queries <= self.query_count:
                break
            if not input_queue.empty():
                data = input_queue.get()
                start_test_case_number = data["start_test_case_number"]
                query_template_list = data["query_template_list"]
                # create strings for queries and indexes but doesnt send indexes to Couchbase

                query_input_list = []
                self.query_helper.debug_logging = self.debug_logging
                conversion_func = self.query_helper._get_conversion_func(self.test_name)
                conversion_map = {'table_name': str(table_name), "table_map": table_map, 'database_name': self.database}

                for n1ql_query in query_template_list:
                    sql_n1ql_index_map = conversion_func(n1ql_query, conversion_map)
                    query_input_list.append(sql_n1ql_index_map)

                # build indexes
                if self.use_secondary_index:
                    self._generate_secondary_indexes_in_batches(query_input_list)
                thread_list = []
                test_case_number = start_test_case_number
                for test_case_input in query_input_list:
                    if self.use_new_rqg:
                        t = threading.Thread(target=self._run_basic_test_new, args=(test_case_input, test_case_number, result_queue, failure_record_queue))
                    else:
                        t = threading.Thread(target=self._run_basic_test, args=(test_case_input, test_case_number, result_queue, failure_record_queue))
                    test_case_number += 1
                    t.daemon = True
                    t.start()
                    thread_list.append(t)
                    # Drop all the secondary Indexes

                for t in thread_list:
                    t.join()

                if self.use_secondary_index and self.drop_secondary_indexes:
                    self._drop_secondary_indexes_in_batches(query_input_list)
            else:
                count += 1
                if count > 1000:
                    return

    def _run_basic_test_new(self, query_test_map, test_case_number, result_queue, failure_record_queue=None):
        self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< BEGIN RUNNING TEST {0}  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(test_case_number))

        n1ql_query = query_test_map["n1ql"]
        sql_query = query_test_map["sql"]
        indexes = query_test_map["indexes"]
        expected_result = query_test_map["expected_result"]
        tests_to_run = query_test_map['tests']

        # results dict
        result_run = dict()
        result_run["n1ql_query"] = n1ql_query
        result_run["sql_query"] = sql_query
        result_run["test_case_number"] = test_case_number

        # run the query
        for test in tests_to_run:
            if test == "BASIC":
                if self.use_new_rqg:
                    result_run["run_query_without_index_hint"] = self._run_queries_and_verify_new(n1ql_query=n1ql_query,
                                                                                              sql_query=sql_query,
                                                                                              expected_result=expected_result)
                else:
                    result_run["run_query_without_index_hint"] = self._run_queries_and_verify(subquery=self.subquery,
                                                                                  n1ql_query=n1ql_query,
                                                                                  sql_query=sql_query,
                                                                                  expected_result=expected_result)
            else:
                print("Unknown test type to run")
                exit(1)

        result_queue.put(result_run)
        self._check_and_push_failure_record_queue(result_run, query_test_map, failure_record_queue)
        self.query_count += 1
        self.log.info(" <<<<<<<<<<<<<<<<<<<<<<<<<<<< END RUNNING TEST {0}  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(test_case_number))

    def _run_queries_and_verify_new(self, n1ql_query=None, sql_query=None, expected_result=None):
        self.log.info(" SQL QUERY :: {0}".format(sql_query))
        self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
        client = RQGMySQLClientNew(database=self.database, host=self.mysql_url, user_id=self.user_id, password=self.password)

        try:

            actual_result = self.n1ql_helper.run_cbq_query(query=n1ql_query, server=self.n1ql_server, scan_consistency="request_plus")
            n1ql_result = actual_result["results"]
            # Run SQL Query
            sql_result = expected_result
            if expected_result is None:
                columns, rows = client._execute_query(query=sql_query)
                self.log.info(" result from n1ql query returns {0} items".format(len(n1ql_result)))
                self.log.info(" result from sql query returns {0} items".format(len(rows)))
                sql_result = self._gen_json_from_results(columns, rows)

            if len(n1ql_result) != len(sql_result):
                self.log.info("number of results returned from sql and n1ql are different")
                self.log.info("sql query is {0}".format(sql_query))
                self.log.info("n1ql query is {0}".format(n1ql_query))

                if (len(sql_result) == 0 and len(n1ql_result) == 1) or (len(n1ql_result) == 0 and len(sql_result) == 1) or (len(sql_result) == 0):
                    return {"success": True, "result": "Pass"}
                return {"success": False, "result": str("different results")}

            try:
                self._verify_results_rqg_new(sql_result=sql_result, n1ql_result=n1ql_result)
            except Exception as ex:
                self.log.info(ex)
                traceback.print_exc()
                return {"success": False, "result": str(ex)}
            return {"success": True, "result": "Pass"}
        except Exception as ex:
            self.log.info(ex)
            traceback.print_exc()
            return {"success": False, "result": str(ex)}
        finally:
            client._close_connection()

    def _gen_json_from_results(self, columns, rows):
        data = []
        # Convert to JSON and capture in a dictionary
        for row in rows:
            index = 0
            map = {}
            for column in columns:
                value = row[index]
                converted_val = self._convert_to_mysql_json_compatible_val(value, column["type"])
                if converted_val is not None:
                    map[column["column_name"]] = converted_val
                index += 1
            data.append(map)
        return data

    def _convert_to_mysql_json_compatible_val(self, value, type):
        if value is None:
            return None
        elif isinstance(value, float):
            return round(value, 0)
        elif "tiny" in str(type):
            if value == 0:
                return False
            elif value == 1:
                return True
            else:
                return None
        elif "int" in str(type):
            return value
        elif "long" in str(type):
            return value
        elif "datetime" in str(type):
            return str(value)
        elif ("float" in str(type)) or ("double" in str(type)):
            return round(value, 0)
        elif "decimal" in str(type):
            if isinstance(value, float):
                return round(value, 0)
            else:
                return int(round(value, 0))
        else:
            return str(value)

    def _verify_results_rqg_new(self, n1ql_result=[], sql_result=[]):
        new_n1ql_result = []
        for result in n1ql_result:
            new_n1ql_result.append(result)

        n1ql_result = new_n1ql_result
        actual_result = n1ql_result

        expected_result = sql_result

        #if len(actual_result) != len(expected_result):
        #    extra_msg = self._get_failure_message(expected_result, actual_result)
        #    raise Exception("Results are incorrect. Actual num %s. Expected num: %s. :: %s \n" % (len(actual_result), len(expected_result), extra_msg))

        diffs = DeepDiff(actual_result, expected_result, ignore_order=True)
        if diffs:
            raise Exception("Results are incorrect. " + diffs)

        msg = "The number of rows match but the results mismatch, please check"
        sorted_actual = self._sort_data(actual_result)
        sorted_expected = self._sort_data(expected_result)

        combined_results = list(zip(sorted_expected, sorted_actual))
        for item in combined_results:
            expected = item[0]
            actual = item[1]
            for result in expected:
                if result not in actual:
                    extra_msg = self._get_failure_message(expected_result, actual_result)
                    raise Exception(msg+"\n "+extra_msg)

    def _sort_data(self, result):
        new_data = []
        for data in result:
            new_data.append(sorted(data))
        return new_data

    def _get_failure_message(self, expected_result, actual_result):
        if expected_result is None:
            expected_result = []
        if actual_result is None:
            actual_result = []
        len_expected_result = len(expected_result)
        len_actual_result = len(actual_result)
        len_expected_result = min(5, len_expected_result)
        len_actual_result = min(5, len_actual_result)
        extra_msg = "mismatch in results :: expected :: {0}, actual :: {1} ".format(expected_result[0:len_expected_result], actual_result[0:len_actual_result])
        return extra_msg

    def _initialize_rqg_query_helper(self):
        return RQGQueryHelperNew()

    def _initialize_mysql_client(self):
        if self.reset_database:
            self.client = RQGMySQLClientNew(host=self.mysql_url, user_id=self.user_id, password=self.password)
            if self.subquery:
                path = "b/resources/rqg/{0}/database_definition/definition-subquery.sql".format(self.database)
            else:
                path = "b/resources/rqg/{0}/database_definition/definition.sql".format(self.database)
            self.database = self.database+"_"+str(self.query_helper._random_int())
            populate_data = False
            if not self.populate_with_replay:
                populate_data = True
            if self.subquery:
                self.client.reset_database_add_data(database=self.database, items=self.items, sql_file_definiton_path=path, populate_data=populate_data, number_of_tables=1)
            else:
                self.client.reset_database_add_data(database=self.database, items=self.items, sql_file_definiton_path=path, populate_data=populate_data, number_of_tables=self.number_of_buckets)
            self._copy_table_for_merge()
        else:
            self.client = RQGMySQLClientNew(database=self.database, host=self.mysql_url, user_id=self.user_id, password=self.password)

    def _run_queries_and_verify(self, aggregate=False, subquery=False, n1ql_query=None, sql_query=None, expected_result=None):
        if not self.create_primary_index:
            n1ql_query = n1ql_query.replace("USE INDEX(`#primary` USING GSI)", " ")
        if self.prepared:
            n1ql_query = "PREPARE " + n1ql_query
        self.log.info(" SQL QUERY :: {0}".format(sql_query))
        self.log.info(" N1QL QUERY :: {0}".format(n1ql_query))
        # Run n1ql query
        if self.test_name and self.test_name == 'window_functions':
            hints = []
        else:
            hints = self.query_helper._find_hints(sql_query)

        for i, item in enumerate(hints):
            if "simple_table" in item:
                hints[i] = hints[i].replace("simple_table", self.database+"_"+"simple_table")
        try:
            if subquery:
                query_params = {'timeout': '1200s'}
            else:
                query_params={}
            actual_result = self.n1ql_helper.run_cbq_query(query=n1ql_query, server=self.n1ql_server, query_params=query_params, scan_consistency="request_plus")
            if self.prepared:
                name = actual_result["results"][0]['name']
                prepared_query = "EXECUTE '%s'" % name
                self.log.info(" N1QL QUERY :: {0}".format(prepared_query))
                actual_result = self.n1ql_helper.run_cbq_query(query=prepared_query, server=self.n1ql_server, query_params=query_params, scan_consistency="request_plus")
            n1ql_result = actual_result["results"]

            # Run SQL Query
            sql_result = expected_result

            client = None
            if self.use_mysql:
                client = RQGMySQLClient(database=self.database, host=self.mysql_url, user_id=self.user_id, password=self.password)
            elif self.use_postgres:
                client = RQGPostgresClient()

            if expected_result is None:
                columns, rows = client._execute_query(query=sql_query)
                if self.aggregate_pushdown:
                    sql_result = client._gen_json_from_results_repeated_columns(columns, rows)
                else:
                    sql_result = client._gen_json_from_results(columns, rows)
            client._close_connection()
            self.log.info(" result from n1ql query returns {0} items".format(len(n1ql_result)))
            self.log.info(" result from sql query returns {0} items".format(len(sql_result)))

            if len(n1ql_result) != len(sql_result):
                self.log.info("number of results returned from sql and n1ql are different")
                self.log.info("sql query is {0}".format(sql_query))
                self.log.info("n1ql query is {0}".format(n1ql_query))

                if (len(sql_result) == 0 and len(n1ql_result) == 1) or (len(n1ql_result) == 0 and len(sql_result) == 1) or (len(sql_result) == 0):
                        return {"success": True, "result": "Pass"}
                return {"success": False, "result": str("different results")}
            try:
                self.n1ql_helper._verify_results_rqg(subquery, aggregate, sql_result=sql_result, n1ql_result=n1ql_result, hints=hints, aggregate_pushdown=self.aggregate_pushdown)
            except Exception as ex:
                self.log.info(ex)
                traceback.print_exc()
                return {"success": False, "result": str(ex)}
            return {"success": True, "result": "Pass"}
        except Exception as ex:
            self.log.info(ex)
            traceback.print_exc()
            return {"success": False, "result": str(ex)}
