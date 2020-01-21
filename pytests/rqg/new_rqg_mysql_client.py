from .base_rqg_mysql_client import BaseRQGMySQLClient
from .new_rqg_query_helper import RQGQueryHelperNew

class RQGMySQLClientNew(BaseRQGMySQLClient):

    ''' Using test_name input parameter, function calls dispatcher function to transform
        templates into SQL and N1QL queries using desired logic. '''
    def _convert_template_query_info(self, n1ql_queries=[], table_map={}, define_gsi_index=True, gen_expected_result=False, partitioned_indexes=False, test_name=""):
        helper = RQGQueryHelperNew()
        query_input_list = []
        for n1ql_query in n1ql_queries:
            sql_n1ql_index_map = helper._convert_sql_template_to_value(n1ql_query,
                                                                       table_map=table_map,
                                                                       define_gsi_index=define_gsi_index,
                                                                       partitioned_indexes=partitioned_indexes,
                                                                       test_name=test_name)
            if gen_expected_result:
                sql_query = sql_n1ql_index_map["sql"]
                try:
                    sql_result = self._query_and_convert_to_json(sql_query)
                    sql_n1ql_index_map["expected_result"] = sql_result
                except Exception as ex:
                    print(ex)


            sql_n1ql_index_map = self._translate_function_names(sql_n1ql_index_map)
            query_input_list.append(sql_n1ql_index_map)

        return query_input_list


