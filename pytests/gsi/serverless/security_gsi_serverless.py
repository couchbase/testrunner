"""security_gsi_serverless.py: "This class test for tenant isolation  for GSI"

__author__ = "Yash Dodderi"
__maintainer = "Yash Dodderi"
__email__ = "yash.dodderi@couchbase.com"
__git_user__ = "yash-dodderi7"
"""

import requests
from gsi.serverless.base_gsi_serverless import BaseGSIServerless
import subprocess



class GSI_Security(BaseGSIServerless):
    def setUp(self):
        super(GSI_Security, self).setUp()
        self.log.info("==============  GSI_Security serverless setup has started ==============")

    def tearDown(self):
        self.log.info("==============  GSI_Security serverless tearDown has started ==============")
        super(GSI_Security, self).tearDown()
        self.log.info("==============  GSI_Security serverless tearDown has completed ==============")

    def test_run_queries_against_different_tenants(self):
        self.provision_databases(count=self.num_of_tenants)
        index_fields = ['age', 'country']
        user1, password1, user2, password2 = None, None, None, None
        queries = ['select * from _default where age>20', 'select * from _default where country like "A%"']
        system_queries = ['select * from system:queries', 'select * from system:nodes']
        for counter, database in enumerate(self.databases.values()):
            index_name = f'idx_db{counter}'
            self.create_index(database,
                              query_statement=f"create index {index_name} on _default({index_fields[counter]})")
            self.load_databases(load_all_databases=False, num_of_docs=10,
                                database_obj=database, scope="_default", collection="_default")
            if counter == 0:
                user1, password1 = database.access_key, database.secret_key
            else:
                user2, password2 = database.access_key, database.secret_key
        for counter, database in enumerate(self.databases.values()):
            test_pass = False
            user1, user2, password1, password2 = user2, user1, password2, password1
            try:
                self.run_query(database=database, username=user1, password=password1, query=queries[counter])
                self.log.info("Query ran without errors. Test failure")
                test_pass = False
            except requests.exceptions.HTTPError as err:
                self.log.info(str(err))
                if '401 Client Error: Unauthorized' in str(err):
                    test_pass = True
            if not test_pass:
                self.fail(
                    f"User {user1} with password {password1} able to run queries on {database.id} a database the user is unauthorised for")

        for counter, database in enumerate(self.databases.values()):
            test_pass = False
            user1, user2, password1, password2 = user2, user1, password2, password1
            try:
                self.run_query(database=database, username=user1, password=password1, query=system_queries[counter])
                self.log.info("Query ran without errors. Test failure")
                test_pass = False
            except requests.exceptions.HTTPError as err:
                self.log.info(str(err))
                if '401 Client Error: Unauthorized' in str(err):
                    test_pass = True
            if not test_pass:
                self.fail(
                    f"User {user1} with password {password1} able to run queries on {database.id} a database the user is unauthorised for")

    def test_negative_security(self):
        self.provision_databases(count=self.num_of_tenants)
        index_field = 'age'
        index_name = f'idx_db_1'
        database = list(self.databases)[0]
        database_obj = self.databases[database]
        self.create_index(database_obj,
                          query_statement=f"create index {index_name} on _default({index_field})")
        self.load_databases(load_all_databases=False, num_of_docs=10,
                            database_obj=database_obj, scope="_default", collection="_default")
        endpoints = ["/getIndexMetadata", "/getIndexStatus", "/getIndexStatement", "/getLocalIndexMetadata",
                     "/dropIndex", "/createIndex", "/createIndexRebalance", "/buildIndexRebalance",
                     "/buildRecoveredIndexesRebalance", "/restoreIndexMetadata", "/settings/storageMode",
                     "/settings/planner", "/postScheduleCreateRequest", "/recoverIndexRebalance",
                     "/getCachedIndexTopology", "/getCachedIndexerNodeUUIDs", "/planIndex", "/listReplicaCount",
                     "/getCachedLocalIndexMetadata", "/getCachedStats", "/getInternalVersion", "/listMetadataTokens",
                     "/listCreateTokens", "/listDeleteTokens", "/listDeleteTokenPaths", "/listDropInstanceTokens",
                     "/listDropInstanceTokenPaths", "/listScheduleCreateTokens", "/listStopScheduleCreateTokens",
                     "/transferScheduleCreateTokens", "/test/CancelTask", "/test/GetTaskList", "/debug/pprof/",
                     "/debug/pprof/goroutine", "/debug/pprof/block", "/debug/pprof/heap", "/debug/pprof/threadcreate",
                     "/debug/pprof/profile", "/debug/pprof/cmdline", "/debug/pprof/symbol", "/debug/pprof/trace",
                     "/debug/vars", "/pauseMgr/FailedTask", "/pauseMgr/Pause", "/test/Pause", "/test/PreparePause",
                     "/test/PrepareResume", "/test/Resume", "/registerRebalanceToken", "/listRebalanceTokens",
                     "/cleanupRebalance", "/moveIndex", "/moveIndexInternal", "/nodeuuid", "/rebalanceCleanupStatus",
                     "/internal/indexes", "/internal/index/", "/settings", "/internal/settings", "/triggerCompaction",
                     "/settings/runtime/freeMemory", "/settings/runtime/forceGC", "/plasmaDiag", "/stats", "/stats/mem",
                     "/stats/storage/mm", "/stats/storage", "/stats/reset", "/storage/jemalloc/profile",
                     "/storage/jemalloc/profileActivate", "/storage/jemalloc/profileDeactivate",
                     "/storage/jemalloc/profileDump", "/stats/cinfolite", "/_prometheusMetrics",
                     "/_prometheusMetricsHigh", "/_metering", "/lockShards",
                     "/unlockShards"]
        for endpoint in endpoints:
            testpass = False
            curl_url = f"curl -X GET -H 'Content-Type:application/json' -u '{database_obj.access_key}:{database_obj.secret_key}' 'https://{database_obj.data_api}{endpoint}'"
            self.log.info(curl_url)
            try:
                resp = subprocess.getstatusoutput(curl_url)
            except UnicodeDecodeError:
                self.fail(
                    f"User {database_obj.access_key} with password {database_obj.secret_key} able to access internal endpoints on {database_obj.id} which are not supposed to be exposed")
            if '1006' in list(resp)[1] and '404' in list(resp)[1]:
                testpass = True
            if not testpass:
                self.fail(
                    f"User {database_obj.access_key} with password {database_obj.secret_key} able to access internal endpoints on {database_obj.id} which are not supposed to be exposed")
