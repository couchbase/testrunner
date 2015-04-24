#!/usr/bin/env python
"""
Python based MySQL interface
"""
import mysql.connector
import zipfile
import os
from os.path import basename
import shutil
from mysql.connector import FieldType
from couchbase_helper.query_helper import QueryHelper

class MySQLClient(object):
    """Python MySQLClient Client Implementation for testrunner"""

    def __init__(self, database = "flightstats", host = "127.0.0.1", user_id = "root", password = ""):
        self.database = database
        self.host = host
        self.user_id = user_id
        self.password = password
        self._set_mysql_client(self.database , self.host , self.user_id , self.password)
        self.table_key_type_map = self._get_pkey_map_for_tables_with_primary_key_column()

    def _reset_client_connection(self):
        self._reset_client_connection()
        self._set_mysql_client(self.database , self.host , self.user_id , self.password)

    def _set_mysql_client(self, database = "flightstats", host = "127.0.0.1", user_id = "root", password = ""):
        self.mysql_connector_client = mysql.connector.connect(user = user_id, password = password,
         host = host, database = database)

    def _close_mysql_connection(self):
        self.mysql_connector_client.close()

    def _insert_execute_query(self, query = ""):
        cur = self.mysql_connector_client.cursor()
        cur.execute(query)
        self.mysql_connector_client.commit()

    def _execute_query(self, query = ""):
        column_names = []
        cur = self.mysql_connector_client.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        desc = cur.description
        columns =[]
        for row in desc:
            columns.append({"column_name":row[0], "type":FieldType.get_info(row[1]).lower()})
        return columns, rows

    def _gen_json_from_results_with_primary_key(self, columns, rows, primary_key = ""):
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

    def _convert_to_mysql_json_compatible_val(self, value, type):
        if not hasattr(self, 'table_key_type_map'):
            return value
        if "datetime" in str(type):
            return str(value)
        if "decimal" in str(type):
            if value == None:
                return 0
            else:
                return int(value)
        return value

    def _get_table_list(self):
        table_list = []
        columns, rows = self._execute_query(query = "SHOW TABLES")
        for row in rows:
            table_list.append(row[0])
        return table_list

    def _get_table_info(self, table_name = ""):
        columns, rows = self._execute_query(query = "DESCRIBE {0}".format(table_name))
        return self._gen_json_from_results(columns, rows)

    def _get_tables_information(self):
        map ={}
        list = self._get_table_list()
        for table_name in list:
            map[table_name] = self._get_table_info(table_name)
        return map

    def _get_field_list_map_for_tables(self):
        target_map = {}
        map = self._get_tables_information()
        for table_name in map.keys():
            field_list = []
            for field_info in map[table_name]:
                field_list.append(field_info['Field'])
            target_map[table_name] = field_list
        return target_map

    def _get_field_with_types_list_map_for_tables(self):
        target_map = {}
        map = self._get_tables_information()
        for table_name in map.keys():
            field_list = []
            for field_info in map[table_name]:
                field_list.append({field_info['Field']:field_info['Type']})
            target_map[table_name] = field_list
        return target_map

    def _get_primary_key_map_for_tables(self):
        target_map = {}
        map = self._get_tables_information()
        for table_name in map.keys():
            for field_info in map[table_name]:
                if field_info['Key'] == "PRI":
                    target_map[table_name] = field_info['Field']
        return target_map


    def _get_pkey_map_for_tables_with_primary_key_column(self):
        target_map = {}
        map = self._get_tables_information()
        for table_name in map.keys():
            target_map[table_name] ={}
            field_map = {}
            for field_info in map[table_name]:
                field_map[field_info['Field']] ={"type":field_info['Type']}
            target_map[table_name] = field_map
        return target_map

    def _get_pkey_map_for_tables_without_primary_key_column(self):
        target_map = {}
        map = self._get_tables_information()
        for table_name in map.keys():
            target_map[table_name] ={}
            field_map = {}
            for field_info in map[table_name]:
                if field_info['Key'] != "PRI":
                    field_map[field_info['Field']] ={"type":field_info['Type']}
            target_map[table_name] = field_map
        return target_map

    def _get_distinct_values_for_fields(self, table_name, field):
        query = "Select DISTINCT({0}) from {1} ORDER BY {0}".format(field, table_name)
        list = []
        columns, rows = self._execute_query(query)
        for row in rows:
            list.append(row[0])
        return list

    def _get_values_with_type_for_fields_in_table(self):
        map = self._get_field_with_types_list_map_for_tables()
        gen_map = {}
        for table_name in map.keys():
            gen_map[table_name] = {}
            for vals in map[table_name]:
                field_name = vals.keys()[0]
                value_list = self._get_distinct_values_for_fields(table_name, field_name)
                gen_map[table_name][field_name] = {"type": vals[field_name], "distinct_values": sorted(value_list)}
                #print "For table {0} and field {1} we have read {2} distinct values ".format(table_name, field_name, len(value_list))
        return gen_map

    def _gen_data_simple_table(self, number_of_rows = 10000, table_name = "simple_table"):
        helper = QueryHelper()
        map = self._get_pkey_map_for_tables_without_primary_key_column()
        table_map = map[table_name]
        for x in range(0, number_of_rows):
            statement = helper._generate_insert_statement("simple_table", table_map)
            print statement
            self._insert_execute_query(statement)

    def _gen_queries_from_template(self, query_path = "./queries.txt", table_name = "simple_table"):
        helper = QueryHelper()
        map = self._get_values_with_type_for_fields_in_table()
        table_map = map[table_name]
        with open(query_path) as f:
            content = f.readlines()
        for query in content:
            n1ql = helper._convert_sql_template_to_value(sql = query, table_map = table_map, table_name= table_name)
            print n1ql

    def _query_and_convert_to_json(self, query):
        columns, rows = self._execute_query(query = query)
        sql_result = self._gen_json_from_results(columns, rows)
        return sql_result

    def _convert_template_query_info_with_gsi(self, file_path, gsi_index_file_path = None, table_map= {}, table_name = "simple_table", define_gsi_index = True, gen_expected_result = True):
        helper = QueryHelper()
        f = open(gsi_index_file_path,'w')
        n1ql_queries = self._read_from_file(file_path)
        for n1ql_query in n1ql_queries:
            check = True
            map=helper._convert_sql_template_to_value_for_secondary_indexes(
                n1ql_query, table_map = table_map, table_name = table_name, define_gsi_index= define_gsi_index)
            if gen_expected_result:
                query = map["sql"]
                try:
                    sql_result = self._query_and_convert_to_json(query)
                    map["expected_result"] = sql_result
                except Exception, ex:
                    print ex
                    check = False
            if check:
                f.write(json.dumps(map)+"\n")
        f.close()

    def  _read_from_file(self, file_path):
        with open(file_path) as f:
            content = f.readlines()
        return content

    def dump_database(self, data_dump_path = "/tmp"):
        zip_path= data_dump_path+"/"+self.database+".zip"
        data_dump_path = data_dump_path+"/"+self.database
        os.mkdir(data_dump_path)
        table_key_map = self._get_primary_key_map_for_tables()
        # Make a list of buckets that we want to create for querying
        bucket_list = table_key_map.keys()
        # Read Data from mysql database and populate the couchbase server
        for bucket_name in bucket_list:
            query = "select * from {0}".format(bucket_name)
            columns, rows = self._execute_query(query = query)
            dict = self._gen_json_from_results_with_primary_key(columns, rows, table_key_map[bucket_name])
            # Take snap-shot of Data in
            f = open(data_dump_path+"/"+bucket_name+".txt",'w')
            f.write(json.dumps(dict))
            f.close()
        zipf = zipfile.ZipFile(zip_path, 'w')
        for root, dirs, files in os.walk(data_dump_path):
            for file in files:
                path = os.path.join(root, file)
                filter_path = path.replace(self.database,"")
                zipf.write(path, basename(filter_path))
        shutil.rmtree(data_dump_path)

    def _gen_gsi_index_info_from_n1ql_query_template(self, query_path = "./queries.txt", output_file_path = "./output.txt",  table_name = "simple_table", gen_expected_result= True):
        map = self._get_values_with_type_for_fields_in_table()
        table_map = map[table_name]
        self._convert_template_query_info_with_gsi(query_path, gsi_index_file_path = output_file_path, table_map = table_map, table_name = table_name, gen_expected_result = gen_expected_result)

if __name__=="__main__":
    import json
    client = MySQLClient(database = "simple_table_db", host = "localhost", user_id = "root", password = "")
    #query = "select * from simple_table LIMIT 1"
    #print query
    #column_info, rows = client._execute_query(query = query)
    #dict = client._gen_json_from_results_with_primary_key(column_info, rows, "primary_key_id")
    #print dict


    #client._gen_data_simple_table()
    #query_path="/Users/parag/fix_testrunner/testrunner/b/resources/rqg/simple_table/query_template/n1ql_query_template_10000.txt"
    #client.dump_database()
    client._gen_gsi_index_info_from_n1ql_query_template(query_path="./template.txt")
    #with open("./output.txt") as f:
    #    content = f.readlines()
    #for data in content:
    #    json_data= json.loads(data)
    #    print "<<<<<<<<<<< BEGIN >>>>>>>>>>"
    #    print json_data["sql"]
    #    print json_data["n1ql"]
    #    print json_data["gsi_indexes"]
    #    print "<<<<<<<<<<< END >>>>>>>>>>"
    #with open("./queries.txt") as f:
    #    content = f.readlines()
    #for sql in content:
    #    print " <<<<<< START >>>>>"
    #    print sql
    #    new_sql = helper._convert_sql_template_to_value(sql = sql, table_map = table_map, table_name= "airports")
    #    print new_sql
    #    print " <<<<< END >>>> "