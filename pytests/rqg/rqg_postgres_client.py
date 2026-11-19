#!/usr/bin/env python
"""
Python based MySQL interface
"""
from .rqg_query_helper import RQGQueryHelper
from lib.postgres_client import PostgresClient
import pdb
import psycopg2
import psycopg2.extras
import psycopg2.extensions
#from psycopg2 import
import itertools
import re
import os
import json
import shutil
import zipfile
from os.path import basename
from psycopg2 import Timestamp


class RQGPostgresClient(PostgresClient):

    def __init__(self, host='localhost', user='root', password='password', database='test', port=5432):
        super(RQGPostgresClient, self).__init__(host=host, user=user, password=password, database=database, port=port)

    def reset_database_add_data(self, database="", items=1000, sql_file_definiton_path="/tmp/definition.sql", populate_data=True, number_of_tables=None):
        self._gen_data_simple_table(number_of_rows=items)

    def _gen_data_simple_table(self, number_of_rows=1000):

        helper = RQGQueryHelper()
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

        try:
            print(f"[PostgreSQL] Dropping table if exists...")
            # Use CASCADE to drop table even if it has dependencies
            drop_table_query = "DROP TABLE IF EXISTS simple_table CASCADE"
            cursor.execute(drop_table_query)
            print(f"[PostgreSQL] Table dropped successfully.")
        except Exception as ex:
            print(f"[PostgreSQL] Warning during drop table: {str(ex)}")

        print(f"[PostgreSQL] Creating simple_table...")
        create_table_query = "CREATE TABLE simple_table(bool_field1 boolean, char_field1 char(1), datetime_field1 timestamp, decimal_field1 numeric, int_field1 numeric, primary_key_id text, varchar_field1 text)"
        try:
            cursor.execute(create_table_query)
            print(f"[PostgreSQL] Table created successfully.")
        except Exception as ex:
            print(f"[PostgreSQL] ERROR: Cannot create table - {str(ex)}")
            raise

        print(f"[PostgreSQL] Inserting {number_of_rows} rows (this may take a while)...")
        map = self._get_pkey_map_for_tables_wit_primary_key_column()
        batch_size = 100

        for table_name in list(map.keys()):
            for x in range(0, number_of_rows):
                try:
                    statement = self._generate_insert_statement(helper, table_name, map[table_name], "\'" + str(x + 1) + "\'")
                    cursor.execute(statement)
                    # Progress logging
                    if (x + 1) % batch_size == 0:
                        print(f"[PostgreSQL] Inserted {x + 1}/{number_of_rows} rows...")
                except Exception as ex:
                    print(f"[PostgreSQL] Error inserting row {x + 1}: {str(ex)}")
                    raise

        print(f"[PostgreSQL] Successfully inserted all {number_of_rows} rows!")



    def _generate_insert_statement(self, helper, table_name="TABLE_NAME", table_map={}, primary_key=''):
        intial_statement = ""
        intial_statement += " INSERT INTO {0} ".format(table_name)
        column_names = "( "+",".join(list(table_map.keys()))+" ) "
        values = ""
        for field_name in list(table_map.keys()):
            if "datetime_field1" == field_name:
                values += "\'"+str(helper._random_datetime())+"\',"
            elif "bool_field1" == field_name:
                intval = helper._random_tiny_int()
                val = False
                if intval == 1:
                    val = True
                values += str(val) + ","
            if "primary_key_id" == field_name:
                values += primary_key+","
            elif "varchar_field1" == field_name:
                values += "\'" + helper._random_alphabet_string() + "\',"
            elif "decimal_field1" == field_name:
                values += str(helper._random_float()) + ","
            elif "char_field1" == field_name:
                values += "\'" + helper._random_char() + "\',"
            elif "int_field1" == field_name:
                values += str(helper._random_int()) + ","

        return intial_statement+column_names+" VALUES ( "+values[0:len(values)-1]+" )"

    def _get_pkey_map_for_tables_wit_primary_key_column(self):
        target_map = {}
        map = self._get_tables_information()
        for table_name in list(map.keys()):
            target_map[table_name] ={}
            field_map = {}
            for field_info in map[table_name]:
                field_map[field_info['field']] = {"type": field_info['type']}
            target_map[table_name] = field_map
        return target_map

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

    def _get_table_list(self):
        table_list = ['simple_table']
        return table_list

    def _get_table_info(self, table_name=""):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("select '' as Extra, column_default as Default, column_name as Field, '' as Key, is_nullable as Null, data_type as Type from INFORMATION_SCHEMA.COLUMNS where table_name='simple_table';")
        columns = []
        for c in cursor.description:
            columns.append(c.name)
        rows = cursor.fetchall()

        return self._gen_json_from_results(columns, rows)

    def _execute_query(self, query=""):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(query)

        rows = cursor.fetchall()
        desc = cursor.description
        columns = []
        for row in desc:
            columns.append({"column_name" : row[0], "type" : row[1]})

        return columns, rows

    def _gen_json_from_results(self, columns, rows, round_level=0):
        data = []
        # Convert to JSON and capture in a dictionary
        for row in rows:
            index = 0
            map = {}
            for column in columns:
                value = row[index]
                if isinstance(column, dict):
                    map[column["column_name"]] = self._convert_to_mysql_json_compatible_val(value, column["type"], round_level=round_level)
                else:
                    map[column] = value
                index += 1
            data.append(map)
        return data

    def _convert_to_mysql_json_compatible_val(self, value, type, round_level=0):
        if "16" == str(type): # boolean
            return value
        if str(type) in ["1700", "701", "23", "20"]: # numeric
            if value is None:
                return None
            else:
                if "." in str(value):
                    right_part = str(value).split(".")[1]
                    while len(right_part) > 1:
                        if right_part[len(right_part)-1:] == "0":
                            right_part = right_part[:len(right_part)-1]
                        else:
                            break
                    if right_part == "0":
                        return int(str(str(value).split(".")[0]))
                    if round_level > 0:
                        return round(float(str(str(value).split(".")[0]+"."+right_part)), round_level)
                    else:
                        return float(str(str(value).split(".")[0] + "." + right_part))
                else:
                    try:
                        return int(str(value))
                    except ValueError as e:
                        print(("####### INVALID NUMBER ::" + str(value) + ":: "))
        if "1114" == str(type): # datetime
            return str(value)

        return str(value)


    def _gen_index_combinations_for_tables(self, index_type="GSI", partitioned_indexes=False):
        index_map = {}
        map = self._get_pkey_map_for_tables_without_primary_key_column()
        final_map = {}
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
            prefix = table_name + "_idx_"
            for pair in list(itertools.permutations(key_list, 1)):
                index_list_map[prefix + "_".join(pair)] = pair
            for pair in list(itertools.permutations(key_list, 3)):
                index_list_map[prefix + "_".join(pair)] = pair
            for pair in list(itertools.permutations(string_field_list, len(string_field_list))):
                index_list_map[prefix + "_".join(pair)] = pair
            for pair in list(itertools.permutations(number_field_list, len(number_field_list))):
                index_list_map[prefix + "_".join(pair)] = pair
            index_list_map[prefix + "_".join(key_list)] = key_list
            index_map[table_name] = index_list_map
            index_list_map = {}
            for table_name in list(index_map.keys()):
                final_map[table_name] = {}
                for index_name in list(index_map[table_name].keys()):
                    if partitioned_indexes:
                        definition = "CREATE INDEX {0} ON {1}({2}) PARTITION BY HASH(meta().id) USING {3}".format(
                            index_name, "test" + "_" + table_name,
                            ",".join(index_map[table_name][index_name]),
                            index_type)
                    else:
                        definition = "CREATE INDEX {0} ON {1}({2}) USING {3}".format(
                            index_name, "test" + "_" + table_name,
                            ",".join(index_map[table_name][index_name]),
                            index_type)
                    final_map[table_name][index_name] = \
                        {
                            "type": index_type,
                            "definition": definition,
                            "name": index_name
                        }
        return final_map

    def _get_pkey_map_for_tables_without_primary_key_column(self):
        target_map = {}
        map = self._get_tables_information()
        for table_name in list(map.keys()):
            target_map[table_name] ={}
            field_map = {}
            for field_info in map[table_name]:
                if field_info['field'] != "primary_key_id":
                    field_map[field_info['field']] = {"type": field_info['type']}
            target_map[table_name] = field_map
        return target_map

    def _get_field_list_map_for_tables(self, table_name=None):
        target_map = {}
        map = self._get_tables_information(table_name)
        for table_name in list(map.keys()):
            field_list = []
            for field_info in map[table_name]:
                field_list.append(field_info['field'])
            target_map[table_name] = field_list
        return target_map

    def _get_values_with_type_for_fields_in_table(self, can_remove_copy_table=True, table_name=None):
        map = self._get_field_with_types_list_map_for_tables(can_remove_copy_table=can_remove_copy_table, table_name=table_name)
        gen_map = self._get_pkey_map_for_tables_with_primary_key_column(can_remove_copy_table=can_remove_copy_table, table_name=table_name)
        for table_name in list(map.keys()):
            for vals in map[table_name]:
                field_name = list(vals.keys())[0]
                value_list = self._get_distinct_values_for_fields(table_name, field_name)
                gen_map[table_name]["fields"][field_name]["distinct_values"] = sorted(value_list)
        return gen_map

    def _get_field_with_types_list_map_for_tables(self, can_remove_copy_table=True, table_name=None):
        target_map = {}
        map = self._get_tables_information(table_name)
        for table_name in list(map.keys()):
            field_list = []
            for field_info in map[table_name]:
                field_list.append({field_info['field']: field_info['type']})
            target_map[table_name] = field_list
        if can_remove_copy_table and "copy_simple_table" in target_map:
            target_map.pop("copy_simple_table")
        return target_map

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
                field_map[field_info['field']] = {"type": field_info['type']}
                if field_info['field'] == "primary_key_id":
                    primary_key_field = field_info['field']
            target_map[table_name]["fields"] = field_map
            target_map[table_name]["primary_key_field"] = primary_key_field
            if number_of_tables > 1:
                table_name_alias = "t_"+str(count)
                outer_table_name_alias = "t_"+str(count)+"_"+str(count)
                target_map[table_name]["alias_name"] = table_name_alias
                target_map[table_name]["outer_alias_name"] = outer_table_name_alias
            count += 1
        return target_map

    def _get_distinct_values_for_fields(self, table_name, field):
        query = "Select DISTINCT({0}) from {1} ORDER BY {0}".format(field, table_name)
        list = []

        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(query)
        columns = []
        for c in cursor.description:
            columns.append(c.name)

        rows = cursor.fetchall()
        for row in rows:
            list.append(row[0])
        return list

    def _translate_function_names(self, query_map):
        sql_n1ql_synonim_functions = {'NVL': {"n1ql_name": "NVL", "sql_name": "NULLIF"},
                                      'RAW': {"n1ql_name": "RAW ", "sql_name": " "},
                                      'VARIANCE_POP' : {"n1ql_name": "VARIANCE_POP", "sql_name": "VAR_POP"},
                                      'VARIANCE_SAMP': {"n1ql_name": "VARIANCE_SAMP", "sql_name": "VAR_SAMP"},
                                      'VARIANCE': {"n1ql_name": "VARIANCE", "sql_name": "VAR_SAMP"},
                                      'STDDEV': {"n1ql_name": "STDDEV", "sql_name": "STDDEV_SAMP"},
                                      'MEAN': {"n1ql_name": "MEAN", "sql_name": "AVG"},
                                      'COUNTN':{"n1ql_name": "COUNTN", "sql_name": "COUNT"}}
        for key in sql_n1ql_synonim_functions:
            if key.lower() in query_map['sql'].lower():
                n1ql_name = key
                sql_name = sql_n1ql_synonim_functions[key]['sql_name']
                query_map['sql'] = re.sub(r'([^\w]{1})'+key+'([^\w]{1})', r'\1'+sql_name+r'\2', query_map['sql'], flags=re.IGNORECASE)

        return query_map

    def _get_primary_key_map_for_tables(self, table_name=None):
        target_map = {}
        map = self._get_tables_information(table_name)
        for table_name in list(map.keys()):
            for field_info in map[table_name]:
                if field_info['field'] == "primary_key_id":
                    target_map[table_name] = field_info['field']
        return target_map

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

    def _close_connection(self):
        self.connection.close()

    def dump_database(self, data_dump_path="/tmp"):
        zip_path= data_dump_path+"/database_dump.zip"
        self.database = "simple_table_db"
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

