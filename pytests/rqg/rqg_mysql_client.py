#!/usr/bin/env python
"""
Python based MySQL interface
"""

from base_rqg_mysql_client import BaseRQGMySQLClient
from lib.mysql_client import MySQLClient

class RQGMySQLClient(BaseRQGMySQLClient):

    def _gen_json_from_results_with_primary_key(self, columns, rows, primary_key=""):
        return super(RQGMySQLClient, self)._gen_json_from_results_with_primary_key(columns, rows, primary_key="")

if __name__ == "__main__":
    client = MySQLClient(host="localhost", user_id="root", password="")
    client.remove_databases()

