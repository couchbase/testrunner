#!/usr/bin/env python
"""
Python based MySQL interface
"""
import itertools
import json
import os
import shutil
import zipfile
from os.path import basename
from lib.mysql_client import MySQLClient
from pytests.rqg.rqg_query_helper import RQGQueryHelper
import re

class BaseRQGMySQLClient(MySQLClient):

    def _gen_json_from_results_with_primary_key(self, columns, rows, primary_key=""):
        primary_key_index = 0
        count = 0
        dict = {}
        # Trace_index_of_primary_key
        for column in columns:
            if column["column_name"] == primary_key:
                primary_key_index = count
            count += 1
        # Convert to JSON and capture in a dictionary
        for row in rows:
            index = 0
            map = {}
            for column in columns:
                value = row[index]
                map[column["column_name"]] = self._convert_to_mysql_json_compatible_val(value, column["type"])
                index += 1
            dict[str(row[primary_key_index])] = map
        return dict

    def _gen_json_from_results(self, columns, rows):
        data = []
        # Convert to JSON and capture in a dictionary
        for row in rows:
            index = 0
            map = {}
            for column in columns:
                value = row[index]
                map[column["column_name"]] = self._convert_to_mysql_json_compatible_val(value, column["type"])
                index += 1
            data.append(map)
        return data

    def _gen_json_from_results_repeated_columns(self, columns, rows):
        data = []
        # Convert to JSON and capture in a dictionary
        for row in rows:
            index = 0
            map = {}
            for column in columns:
                value = row[index]
                map[column["column_name"]+str(index)] = self._convert_to_mysql_json_compatible_val(value, column["type"])
                index += 1
            data.append(map)
        return data

    def _convert_to_mysql_json_compatible_val(self, value, type):
        if isinstance(value, float):
            return round(value, 0)
        if "tiny" in str(type):
            if value == 0:
                return False
            elif value == 1:
                return True
            else:
                return None
        if "int" in str(type):
            return value
        if "long" in str(type):
            return value
        if "datetime" in str(type):
            return str(value)
        if ("float" in str(type)) or ("double" in str(type)):
            if value is None:
                return None
            else:
                return round(value, 0)
        if "decimal" in str(type):
            if value is None:
                return None
            else:
                if isinstance(value, float):
                    return round(value, 0)
                return int(round(value, 0))
        return str(value)

    def _get_table_list(self):
        table_list = []
        columns, rows = self._execute_query(query="SHOW TABLES")
        for row in rows:
            table_list.append(row[0].decode("utf-8"))
        return table_list

    def _get_databases(self):
        table_list = []
        columns, rows = self._execute_query(query="SHOW DATABASES")
        for row in rows:
            if "table" in row[0]:
                table_list.append(row[0])
        return table_list

    def _get_table_info(self, table_name=""):
        columns, rows = self._execute_query(query="DESCRIBE {0}".format(table_name))
        return self._gen_json_from_results(columns, rows)

    def _get_tables_information(self, table_name=None):
        map = {}
        list = self._get_table_list()
        if table_name is not None:
            for table_name_1 in list:
                if table_name == table_name_1:
                    map[table_name_1] = self._get_table_info(table_name_1)
        else:
            for table_name_1 in list:
                map[table_name_1] = self._get_table_info(table_name_1)
        return map

    def _get_field_list_map_for_tables(self, table_name=None):
        target_map = {}
        map = self._get_tables_information(table_name)
        for table_name in list(map.keys()):
            field_list = []
            for field_info in map[table_name]:
                field_list.append(field_info['Field'])
            target_map[table_name] = field_list
        return target_map

    def _get_field_with_types_list_map_for_tables(self, can_remove_copy_table=True, table_name=None):
        target_map = {}
        map = self._get_tables_information(table_name)
        for table_name in list(map.keys()):
            field_list = []
            for field_info in map[table_name]:
                field_list.append({field_info['Field']: field_info['Type']})
            target_map[table_name] = field_list
        if can_remove_copy_table and "copy_simple_table" in target_map:
            target_map.pop("copy_simple_table")
        return target_map

    def _get_primary_key_map_for_tables(self, table_name=None):
        target_map = {}
        map = self._get_tables_information(table_name)
        for table_name in list(map.keys()):
            for field_info in map[table_name]:
                if field_info['Key'] == "PRI":
                    target_map[table_name] = field_info['Field']
        return target_map

    def _gen_index_combinations_for_tables(self, index_type="GSI", partitioned_indexes=False):
        index_map = {}
        map = self._get_pkey_map_for_tables_without_primary_key_column()
        for table_name in list(map.keys()):
            index_map[table_name] = {}
            number_field_list = []
            string_field_list = []
            datetime_field_list = []
            for key in list(map[table_name].keys()):
                if "int" in key or "decimal" in key:
                    number_field_list.append(key)
                if "char" in key or "text" in key:
                    string_field_list.append(key)
                if "tinyint" in key:
                    datetime_field_list.append(key)
            key_list = list(map[table_name].keys())
            index_list_map = {}
            prefix = table_name+"_idx_"
            for pair in list(itertools.permutations(key_list, 1)):
                index_list_map[prefix+"_".join(pair)] = pair
            for pair in list(itertools.permutations(key_list, 3)):
                index_list_map[prefix+"_".join(pair)] = pair
            for pair in list(itertools.permutations(string_field_list, len(string_field_list))):
                index_list_map[prefix+"_".join(pair)] = pair
            for pair in list(itertools.permutations(number_field_list, len(number_field_list))):
                index_list_map[prefix+"_".join(pair)] = pair
            index_list_map[prefix+"_".join(key_list)] = key_list
            index_map[table_name] = index_list_map
            index_list_map = {}
            final_map = {}
            for table_name in list(index_map.keys()):
                final_map[table_name] = {}
                for index_name in list(index_map[table_name].keys()):
                    if partitioned_indexes:
                        definition = "CREATE INDEX {0} ON {1}({2}) PARTITION BY HASH(meta().id) USING {3}".format(
                            index_name, self.database + "_" + table_name,
                            ",".join(index_map[table_name][index_name]),
                            index_type)
                    else:
                        definition = "CREATE INDEX {0} ON {1}({2}) USING {3}".format(
                            index_name, self.database + "_" + table_name,
                            ",".join(index_map[table_name][index_name]),
                            index_type)
                    final_map[table_name][index_name] =\
                        {
                            "type": index_type,
                            "definition": definition,
                            "name": index_name
                        }
        return final_map

    def _get_pkey_map_for_tables_with_primary_key_column(self, can_remove_copy_table=True, table_name=None):
        target_map = {}
        map = self._get_tables_information(table_name)
        if can_remove_copy_table and "copy_simple_table" in list(map.keys()):
            map.pop("copy_simple_table")
        number_of_tables = len(list(map.keys()))
        count = 1
        for table_name in list(map.keys()):
            target_map[table_name] ={}
            field_map = {}
            primary_key_field = "primary_key_field"
            for field_info in map[table_name]:
                field_map[field_info['Field']] = {"type": field_info['Type']}
                if field_info['Key'] == "PRI":
                    primary_key_field = field_info['Field']
            target_map[table_name]["fields"] = field_map
            target_map[table_name]["primary_key_field"] = primary_key_field
            if number_of_tables > 1:
                table_name_alias = "t_"+str(count)
                target_map[table_name]["alias_name"] = table_name_alias
            count += 1
        return target_map

    def _get_pkey_map_for_tables_without_primary_key_column(self):
        target_map = {}
        map = self._get_tables_information()
        for table_name in list(map.keys()):
            target_map[table_name] ={}
            field_map = {}
            for field_info in map[table_name]:
                if field_info['Key'] != "PRI":
                    field_map[field_info['Field']] = {"type": field_info['Type']}
            target_map[table_name] = field_map
        return target_map

    def _get_pkey_map_for_tables_wit_primary_key_column(self):
        target_map = {}
        map = self._get_tables_information()
        for table_name in list(map.keys()):
            target_map[table_name] ={}
            field_map = {}
            for field_info in map[table_name]:
                field_map[field_info['Field']] = {"type": field_info['Type']}
            target_map[table_name] = field_map
        return target_map

    def _get_distinct_values_for_fields(self, table_name, field):
        query = "Select DISTINCT({0}) from {1} ORDER BY {0}".format(field, table_name)
        list = []
        columns, rows = self._execute_query(query)
        for row in rows:
            list.append(row[0])
        return list

    def _get_values_with_type_for_fields_in_table(self, can_remove_copy_table=True, table_name=None):
        map = self._get_field_with_types_list_map_for_tables(can_remove_copy_table=can_remove_copy_table, table_name=table_name)
        gen_map = self._get_pkey_map_for_tables_with_primary_key_column(can_remove_copy_table=can_remove_copy_table, table_name=table_name)
        for table_name in list(map.keys()):
            for vals in map[table_name]:
                field_name = list(vals.keys())[0]
                value_list = self._get_distinct_values_for_fields(table_name, field_name)
                gen_map[table_name]["fields"][field_name]["distinct_values"] = sorted(value_list)
        return gen_map

    def _gen_data_simple_table(self, number_of_rows=1000):
        helper = RQGQueryHelper()
        map = self._get_pkey_map_for_tables_wit_primary_key_column()
        for table_name in list(map.keys()):
            for x in range(0, number_of_rows):
                statement = helper._generate_insert_statement(table_name, map[table_name], "\"" + str(x + 1) + "\"")
                self._insert_execute_query(statement)

    def _gen_queries_from_template(self, query_path="./queries.txt", table_name=None):
        helper = RQGQueryHelper()
        map = self._get_values_with_type_for_fields_in_table()
        table_map = map[table_name]
        with open(query_path) as f:
            content = f.readlines()
        for query in content:
            helper._convert_sql_template_to_value(sql=query, table_map=table_map, table_name=table_name)

    def _query_and_convert_to_json(self, query):
        columns, rows = self._execute_query(query=query)
        sql_result = self._gen_json_from_results(columns, rows)
        return sql_result

    def _convert_template_query_info_with_gsi(self, file_path, gsi_index_file_path=None, table_map={}, table_name="simple_table", define_gsi_index=True, gen_expected_result=False):
        helper = RQGQueryHelper()
        f = open(gsi_index_file_path, 'w')
        n1ql_queries = self._read_from_file(file_path)
        for n1ql_query in n1ql_queries:
            check = True
            if not helper._check_deeper_query_condition(n1ql_query):
                if "SUBQUERY" in n1ql_query:
                    map = helper._convert_sql_template_to_value_with_subqueries(n1ql_query, table_map=table_map,
                                                                                define_gsi_index=define_gsi_index)
                else:
                    map=helper._convert_sql_template_to_value_for_secondary_indexes(n1ql_query, table_map=table_map,
                                                                                    table_name=table_name,
                                                                                    define_gsi_index=define_gsi_index)
            else:
                map=helper._convert_sql_template_to_value_for_secondary_indexes_sub_queries(n1ql_query, table_map=table_map,
                                                                                            table_name=table_name, define_gsi_index=define_gsi_index)
            if gen_expected_result:
                query = map["sql"]
                try:
                    sql_result = self._query_and_convert_to_json(query)
                    map["expected_result"] = sql_result
                except Exception as ex:
                    print(ex)
                    check = False
            if check:
                f.write(json.dumps(map)+"\n")
        f.close()

    def _convert_template_query_info(self, n1ql_queries=[], table_map={}, define_gsi_index=True, gen_expected_result=False, ansi_joins=False, aggregate_pushdown=False, partitioned_indexes=False, with_let=False):
        helper = RQGQueryHelper()
        query_input_list = []
        for n1ql_query in n1ql_queries:
            # check if ["UNION ALL", "INTERSECT ALL", "EXCEPT ALL", "UNION", "INTERSECT", "EXCEPT"] not in query
            if not helper._check_deeper_query_condition(n1ql_query):
                if "SUBTABLE" in n1ql_query:
                     sql_n1ql_index_map = helper._convert_sql_template_to_value_with_subqueryenhancements(n1ql_query,
                                                                                                          table_map=table_map,
                                                                                                          define_gsi_index=define_gsi_index)
                elif "SUBQUERY" in n1ql_query:
                    sql_n1ql_index_map = helper._convert_sql_template_to_value_with_subqueries(n1ql_query,
                                                                                               table_map=table_map,
                                                                                               define_gsi_index=define_gsi_index)
                else:
                    # takes in sql and n1ql queries and create indexes for them
                    sql_n1ql_index_map = helper._convert_sql_template_to_value_for_secondary_indexes(n1ql_query,
                                                                                                     table_map=table_map,
                                                                                                     define_gsi_index=define_gsi_index,
                                                                                                     ansi_joins=ansi_joins,
                                                                                                     aggregate_pushdown=aggregate_pushdown,
                                                                                                     partitioned_indexes=partitioned_indexes,
                                                                                                     with_let=with_let)
            else:
                sql_n1ql_index_map = helper._convert_sql_template_to_value_for_secondary_indexes_sub_queries(n1ql_query,
                                                                                                             table_map=table_map,
                                                                                                             define_gsi_index=define_gsi_index,
                                                                                                             partitioned_indexes=partitioned_indexes)
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

    def _translate_function_names(self, query_map):
        sql_n1ql_synonim_functions = {'NVL': {"n1ql_name": "NVL", "sql_name": "IFNULL"},
                                      'RAW': {"n1ql_name": "RAW ", "sql_name": " "},
                                      'VARIANCE_POP' : {"n1ql_name": "VARIANCE_POP", "sql_name": "VAR_POP"},
                                      'VARIANCE_SAMP': {"n1ql_name": "VARIANCE_SAMP", "sql_name": "VAR_SAMP"},
                                      'VARIANCE': {"n1ql_name": "VARIANCE", "sql_name": "VAR_SAMP"},
                                      'STDDEV': {"n1ql_name": "STDDEV", "sql_name": "STDDEV_SAMP"},
                                      'MEAN': {"n1ql_name": "MEAN", "sql_name": "AVG"},
                                      'NULLS FIRST': {'n1ql_name': 'NULLS FIRST', 'sql_name': ''},
                                      'NULLS LAST': {'n1ql_name': 'NULLS LAST', 'sql_name': ''}}
        for key in sql_n1ql_synonim_functions:
            if key.lower() in query_map['sql'].lower():
                n1ql_name = key
                sql_name = sql_n1ql_synonim_functions[key]['sql_name']
                query_map['sql'] = re.sub(r'([^\w]{1})'+key+'([^\w]{1})', r'\1'+sql_name+r'\2', query_map['sql'])

        return query_map

    def _convert_update_template_query_info(self, n1ql_queries=[], table_map={}):
        helper = RQGQueryHelper()
        query_input_list = []
        for n1ql_query in n1ql_queries:
            query_input_list.append(helper._update_sql_template_to_values(sql=n1ql_query, table_map=table_map))
        return query_input_list

    def _convert_update_template_query_info_with_merge(self, source_table="copy_simple_table", target_table="simple_table" ,n1ql_queries=[], table_map={}):
        helper = RQGQueryHelper()
        query_input_list = []
        for n1ql_query in n1ql_queries:
            query_input_list.append(helper._update_sql_template_to_values_with_merge(source_table=source_table,
                                                                                     target_table=target_table,
                                                                                     sql=n1ql_query,
                                                                                     table_map=table_map))
        return query_input_list

    def _convert_delete_template_query_info_with_merge(self, source_table="copy_simple_table", target_table="simple_table" ,n1ql_queries=[], table_map={}):
        helper = RQGQueryHelper()
        query_input_list = []
        for n1ql_query in n1ql_queries:
            query_input_list.append(helper._delete_sql_template_to_values_with_merge(source_table=source_table,
                                                                                     target_table=target_table,
                                                                                     sql=n1ql_query, table_map=table_map))
        return query_input_list

    def _convert_delete_template_query_info(self, n1ql_queries=[], table_map={}):
        helper = RQGQueryHelper()
        query_input_list = []
        for n1ql_query in n1ql_queries:
            query_input_list.append(helper._delete_sql_template_to_values(sql=n1ql_query, table_map=table_map))
        return query_input_list

    def _read_from_file(self, file_path):
        with open(file_path) as f:
            content = f.readlines()
        return content

    def drop_database(self, database):
        query ="DROP SCHEMA IF EXISTS {0}".format(database)
        self._db_execute_query(query)

    def remove_databases(self):
        list_databases = self._get_databases()
        for database in list_databases:
            self.drop_database(database)

    def reset_database_add_data(self, database="", items=1000, sql_file_definiton_path="/tmp/definition.sql", populate_data=True, number_of_tables=None):
        sqls = self._read_from_file(sql_file_definiton_path)
        sqls = " ".join(sqls).replace("DATABASE_NAME", database).replace("\n", "")
        self._db_execute_query(sqls)
        self.database = database
        self._reset_client_connection()
        if number_of_tables is not None:
            table_list = self._get_table_list()
            for table_name in table_list[number_of_tables:]:
                query = "DROP TABLE {0}.{1}".format(database, table_name)
                self._db_execute_query(query)
        if populate_data:
            self._gen_data_simple_table(number_of_rows=items)

    def database_add_data(self, database="", items=1000, sql_file_definiton_path="/tmp/definition.sql"):
        sqls = self._read_from_file(sql_file_definiton_path)
        sqls = " ".join(sqls).replace("DATABASE_NAME", database).replace("\n", "")
        self._db_execute_query(sqls)

    def dump_database(self, data_dump_path="/tmp"):
        zip_path= data_dump_path+"/database_dump.zip"
        data_dump_path = data_dump_path+"/"+self.database
        os.mkdir(data_dump_path)
        table_key_map = self._get_primary_key_map_for_tables()
        # Make a list of buckets that we want to create for querying
        bucket_list = list(table_key_map.keys())
        # Read Data from mysql database and populate the couchbase server
        print("in dump database, reading data from mysql database and populating couchbase server")
        print("bucket_list is {0}".format(bucket_list))
        for bucket_name in bucket_list:
            query = "select * from {0}".format(bucket_name)
            columns, rows = self._execute_query(query = query)
            dict = self._gen_json_from_results_with_primary_key(columns, rows, table_key_map[bucket_name])
            # Take snap-shot of Data in
            f = open(data_dump_path+"/"+bucket_name+".txt", 'w')
            f.write(json.dumps(dict))
            f.close()
        zipf = zipfile.ZipFile(zip_path, 'w')
        for root, dirs, files in os.walk(data_dump_path):
            for file in files:
                path = os.path.join(root, file)
                filter_path = path.replace(self.database, "")
                zipf.write(path, basename(filter_path))
        shutil.rmtree(data_dump_path)

    def _gen_gsi_index_info_from_n1ql_query_template(self, query_path="./queries.txt", output_file_path="./output.txt", table_name="simple_table", gen_expected_result=True):
        map = self._get_values_with_type_for_fields_in_table()
        table_map = map
        self._convert_template_query_info_with_gsi(query_path, gsi_index_file_path=output_file_path, table_map=table_map, table_name=table_name, gen_expected_result=gen_expected_result)

    def _gen_gsi_index_info_from_n1ql_query_template(self, query_path="./queries.txt", output_file_path="./output.txt", table_name="simple_table", gen_expected_result=True):
        map = self._get_values_with_type_for_fields_in_table()
        self._convert_template_query_info_with_gsi(query_path, gsi_index_file_path=output_file_path, table_map=map, gen_expected_result=gen_expected_result)

if __name__ == "__main__":
    client = BaseRQGMySQLClient(host="localhost", user_id="root", password="")
    client.remove_databases()
