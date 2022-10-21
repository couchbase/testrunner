import requests

from pytests.serverless.serverless_basetestcase import ServerlessBaseTestCase


class QueryBaseServerless(ServerlessBaseTestCase):
    def setUp(self):
        super(QueryBaseServerless, self).setUp()

    def tearDown(self):
        super(QueryBaseServerless, self).tearDown()

    def get_all_query_nodes(self, database):
        return self.get_nodes_from_services_map(database=database, service="n1ql")

    def get_query_node_stats(self, database, node):
        endpoint = f"https://{node}:18093/admin/vitals"
        resp = requests.get(endpoint, auth=(database.admin_username, database.admin_password), verify=False)
        resp.raise_for_status()
        result = resp.json()
        self.log.debug(f"Query node{node} vital stats {result}")
        return result

    def get_all_query_node_usage_stats(self, database):
        query_nodes = self.get_nodes_from_services_map(database=database, service="n1ql")
        nodes = {}
        for node in query_nodes:
            nodes[node] = self.get_query_node_stats(database=database, node=node)
        return nodes

