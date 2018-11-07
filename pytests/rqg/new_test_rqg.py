from base_test_rqg import BaseRQGTests
import threading
from new_rqg_mysql_client import RQGMySQLClientNew


class RQGTestsNew(BaseRQGTests):

    ''' Call of super class setUp() function with defining new input parameter: test_name.
        This parameter will be used in dispatcher function to identify desired transformation logic. '''
    def setUp(self):
        super(RQGTestsNew, self).setUp()
        self.test_name = self.input.param("test_name", "")

    def tearDown(self):
        super(RQGTestsNew, self).tearDown()

    def test_rqg(self):
        super(RQGTestsNew, self).test_rqg()


    def _rqg_worker(self, table_name, table_map, input_queue, result_queue, failure_record_queue=None):
        count = 0
        table_name_description_map = {table_name: table_map[table_name]}
        while True:
            if self.total_queries <= self.query_count:
                break
            if not input_queue.empty():
                data = input_queue.get()
                start_test_case_number = data["start_test_case_number"]
                query_template_list = data["query_template_list"]
                # create strings for queries and indexes but doesnt send indexes to Couchbase
                sql_n1ql_index_map_list = self.client._convert_template_query_info(table_map=table_name_description_map,
                                                                                   n1ql_queries=query_template_list,
                                                                                   define_gsi_index=self.use_secondary_index,
                                                                                   partitioned_indexes=self.partitioned_indexes,
                                                                                   test_name=self.test_name
                                                                                   )

                for sql_n1ql_index_map in sql_n1ql_index_map_list:
                    sql_n1ql_index_map["n1ql"] = sql_n1ql_index_map['n1ql'].replace("simple_table", self.database+"_"+"simple_table")

                # build indexes
                if self.use_secondary_index:
                    self._generate_secondary_indexes_in_batches(sql_n1ql_index_map_list)
                thread_list = []
                test_case_number = start_test_case_number
                for test_case_input in sql_n1ql_index_map_list:
                    t = threading.Thread(target=self._run_basic_test, args=(test_case_input, test_case_number, result_queue, failure_record_queue))
                    test_case_number += 1
                    t.daemon = True
                    t.start()
                    thread_list.append(t)
                    # Drop all the secondary Indexes

                for t in thread_list:
                    t.join()

                if self.use_secondary_index and self.drop_secondary_indexes:
                    self._drop_secondary_indexes_in_batches(sql_n1ql_index_map_list)
            else:
                count += 1
                if count > 1000:
                    return

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
