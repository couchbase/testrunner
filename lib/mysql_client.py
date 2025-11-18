#!/usr/bin/env python
"""
Python based MySQL interface
"""
import mysql.connector
from mysql.connector import FieldType


class MySQLClient(object):
    """Python MySQLClient Client Implementation for testrunner"""
    def __init__(self, database=None, host="127.0.0.1", user_id="root", password=""):
        self.database = database
        self.host = host
        self.user_id = user_id
        self.password = password
        if self.database:
            self._set_mysql_client(self.database, self.host, self.user_id, self.password)
        else:
            self._set_mysql_client_without_database(self.host, self.user_id, self.password)

    def _reset_client_connection(self):
        self._close_connection()
        self._set_mysql_client(self.database, self.host, self.user_id, self.password)

    def _set_mysql_client(self, database="flightstats", host="127.0.0.1", user_id="root", password=""):
        self.mysql_connector_client = mysql.connector.connect(user=user_id, password=password, host=host, database=database, auth_plugin='mysql_native_password')

    def _set_mysql_client_without_database(self, host="127.0.0.1", user_id="root", password=""):
        self.mysql_connector_client = mysql.connector.connect(user=user_id, password=password, host=host, auth_plugin='mysql_native_password')

    def _close_connection(self):
        self.mysql_connector_client.close()

    def _insert_execute_query(self, query=""):
        cur = self.mysql_connector_client.cursor()
        try:
            cur.execute(query)
            self.mysql_connector_client.commit()
        except Exception as ex:
            print(ex)
            raise

    def _db_execute_query(self, query=""):
        cur = self.mysql_connector_client.cursor()
        try:
            # Split multiple statements and execute them individually
            statements = [stmt.strip() for stmt in query.split(';') if stmt.strip()]
            for statement in statements:
                cur.execute(statement)
                # Check if there are results to fetch
                if cur.description:
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
                # Commit after each statement
                self.mysql_connector_client.commit()
        except Exception as ex:
            print(ex)
            raise
        finally:
            cur.close()

    def _execute_query(self, query=""):
        cur = self.mysql_connector_client.cursor(buffered=True)
        cur.execute(query)
        rows = cur.fetchall()
        desc = cur.description
        columns = []
        for row in desc:
            columns.append({"column_name": row[0], "type": FieldType.get_info(row[1]).lower()})

        cur.close()
        return columns, rows

    def _execute_sub_query(self, query=""):
        row_subquery = []
        cur = self.mysql_connector_client.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            if len(rows) == 1:
                return row[0]
            row_subquery.append(row[0])
        return row_subquery
