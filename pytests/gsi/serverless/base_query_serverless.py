import requests

from pytests.serverless.serverless_basetestcase import ServerlessBaseTestCase


class QueryBaseServerless(ServerlessBaseTestCase):
    def setUp(self):
        super(QueryBaseServerless, self).setUp()

    def tearDown(self):
        super(QueryBaseServerless, self).tearDown()

    def get_all_query_nodes(self, rest_info):
        return self.get_nodes_from_services_map(rest_info=rest_info, service="n1ql")

    def get_query_node_stats(self, rest_info, node):
        endpoint = f"https://{node}:18093/admin/stats"
        resp = requests.get(endpoint, auth=(rest_info.admin_username, rest_info.admin_password), verify=False)
        resp.raise_for_status()
        result = resp.json()
        self.log.debug(f"Query node{node} Stats: {result}")
        return result

    def get_average_load_factor(self, rest_info):
        query_nodes = self.get_all_query_nodes(rest_info=rest_info)
        load_sum = 0
        for node in query_nodes:
            load_sum += self.get_query_node_stats(rest_info=rest_info, node=node)['load_factor.value']
        return float(load_sum/len(query_nodes))

    def get_all_query_node_usage_stats(self, rest_info):
        query_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="n1ql")
        nodes = {}
        for node in query_nodes:
            nodes[node] = self.get_query_node_stats(rest_info=rest_info, node=node)
        return nodes

