#!/usr/bin/env python
"""
Python based MySQL interface
"""
import mysql.connector
from couchbase_helper.query_helper import QueryHelper

class MySQLClient(object):
    """Python MySQLClient Client Implementation for testrunner"""

    def __init__(self, database = "flightstats", host = "127.0.0.1", user_id = "root", password = ""):
        self.database = database
        self.host = host
        self.user_id = user_id
        self.password = password
        self._set_mysql_client(self.database , self.host , self.user_id , self.password)

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
        column_names = [row[0]  for row in desc]
        return column_names, rows

    def _gen_json_from_results_with_primary_key(self, columns, rows, primary_key = ""):
        primary_key_index = 0
        count = 0
        dict = {}
        # Trace_index_of_primary_key
        for column in columns:
            if column == primary_key:
                primary_key_index = count
            count += 1
        # Convert to JSON and capture in a dictionary
        for row in rows:
            index = 0
            map = {}
            for column in columns:
                map[column] = row[index]
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
                map[column] = row[index]
                index += 1
            data.append(map)
        return data

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
                print "For table {0} and field {1} we have read {2} distinct values ".format(table_name, field_name, len(value_list))
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

if __name__=="__main__":
    client = MySQLClient(database = "simple_table_db", host = "localhost", user_id = "root", password = "")
    #client._gen_data_simple_table()
    client._gen_queries_from_template()
    #with open("./queries.txt") as f:
    #    content = f.readlines()
    #for sql in content:
    #    print " <<<<<< START >>>>>"
    #    print sql
    #    new_sql = helper._convert_sql_template_to_value(sql = sql, table_map = table_map, table_name= "airports")
    #    print new_sql
    #    print " <<<<< END >>>> "