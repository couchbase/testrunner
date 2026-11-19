import psycopg2
import pdb

class PostgresClient(object):

    """Python PostgresClient Client Implementation for testrunner"""
    def __init__(self, host='localhost', user='root', password='password', database='test', port=5432):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        # Use space-separated format without quotes for psycopg2
        connstr = f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password} connect_timeout=10"
        print(f"[PostgreSQL] Attempting to connect to: host={self.host}, port={self.port}, database={self.database}, user={self.user}")
        self.connection = self.connect(connstr)

    def connect(self, connstr):
        connection = None
        try:
            print(f"[PostgreSQL] Connection string: {connstr}")
            connection = psycopg2.connect(connstr)
            # Enable autocommit to avoid transaction locks
            connection.autocommit = True
            print(f"[PostgreSQL] Successfully connected to database!")
        except Exception as e:
            print(f"[PostgreSQL] ERROR: Cannot connect to PostgreSQL database.")
            print(f"[PostgreSQL] Connection string: {connstr}")
            print(f"[PostgreSQL] Exception: {str(e)}")
            print(f"[PostgreSQL] Make sure PostgreSQL is running and the database exists.")
            raise
        return connection

