"""tenant_management.py: "This class test cluster affinity  for GSI"

__author__ = "Pavan PB"
__maintainer = Pavan PB"
__email__ = "pavan.pb@couchbase.com"
__git_user__ = "pavan-couchbase"
"""

from gsi.serverless.base_gsi_serverless import BaseGSIServerless
from membase.api.serverless_rest_client import ServerlessRestConnection as RestConnection



class TenantManagement(BaseGSIServerless):
    def setUp(self):
        super(TenantManagement, self).setUp()
        self.log.info("==============  TenantManagement serverless setup has started ==============")


    def tearDown(self):
        self.log.info("==============  TenantManagement serverless tearDown has started ==============")
        super(TenantManagement, self).tearDown()
        self.log.info("==============  TenantManagement serverless tearDown has completed ==============")

    def suite_tearDown(self):
        pass

    def suite_setUp(self):
        pass

    def test_cluster_affinity(self):
        tasks = []
        for _ in range(0, 1):
            task = self.create_database_async()
            tasks.append(task)
        for task in tasks:
            task.result()
        for database in self.databases.values():
            # self.load_database(database)
            self.create_index(database, query_statement="create index idx2 on _default(b)")
            self.create_index(database, query_statement="create index idx3 on _default(b)")
            self.rest_obj = RestConnection(database.admin_username, database.admin_password, database.rest_host)
            nodes_obj = self.rest_obj.get_all_dataplane_nodes()
            self.rest_obj.get_all_nodes_in_subcluster()
            self.run_query("select * from _default", database_obj=database)
            for node in nodes_obj:
                if 'index' in node['services']:
                    indexer_metadata = self.get_indexer_metadata(database, node['hostname'].split(":")[0])
                    ## Need to add logic to validate metadata from the nodes
                    # idx_host_list = set()
                    # for idx in indexer_metadata:
                    #     idx_host = idx['hosts'][0]
                    #     idx_host_list.add(idx_host)
                    #
                    # self.assertEqual(len(idx_host_list), 2, "Indexes are hosted on more than 2 node of sub-cluster")
