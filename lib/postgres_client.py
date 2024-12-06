import psycopg2
import pdb

class PostgresClient(object):

    """Python PostgresClient Client Implementation for testrunner"""
    def __init__(self):
        self.connection = self.connect("dbname='test' user='rqg' password='password'")

    def connect(self, connstr):
        connection = None
        try:
            connection = psycopg2.connect(connstr)
        except Exception as e:
            print("Cannot connect to Postgres database.")
            print(("Exception - "+str(e)))
        return connection

