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
        self.log.debug(f"Query node {node} Stats: {result}")
        return result

    def get_average_load_factor(self, rest_info):
        query_nodes = self.get_all_query_nodes(rest_info=rest_info)
        load_sum = 0
        for node in query_nodes:
            load_sum += self.get_query_node_stats(rest_info=rest_info, node=node)['load_factor.value']
        return float(load_sum / len(query_nodes))

    def get_all_query_node_usage_stats(self):
        node_wise_stats = {}
        query_nodes = self.get_all_query_nodes(rest_info=self.dp_obj)
        for node in query_nodes:
            node_wise_stats[node] = self.get_query_node_stats(rest_info=self.dp_obj, node=node)
        return node_wise_stats

    def scale_up_query_subcluster(self, dataplane):
        rest_info = self.create_rest_info_obj(username=dataplane.admin_username,
                                              password=dataplane.admin_password,
                                              rest_host=dataplane.rest_host)
        query_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="n1ql")
        num_nodes = len(query_nodes)
        self.log.info(f"Number of query nodes in the DP: {num_nodes}")
        self.update_specs(dataplane.id, new_count=num_nodes + 1, service='n1ql')

    def scale_down_query_subcluster(self, dataplane):
        rest_info = self.create_rest_info_obj(username=dataplane.admin_username,
                                              password=dataplane.admin_password,
                                              rest_host=dataplane.rest_host)
        query_nodes = self.get_all_query_nodes(rest_info=rest_info)
        num_nodes = len(query_nodes)
        self.log.info(f"Number of query nodes in the DP: {num_nodes}")
        self.log.info(f"Will remove a n1ql node")
        if num_nodes == 2:
            self.log.error("Cannot scale down the query subcluster further since it only has 2 nodes")
            return
        self.update_specs(dataplane.id, new_count=num_nodes - 1, service='n1ql')

    def set_log_level_query_service(self, rest_info, level='info'):
        query_nodes = self.get_nodes_from_services_map(rest_info=rest_info, service="n1ql")
        node = query_nodes[0]
        endpoint = f"https://{node}:18091/settings/querySettings"
        resp = requests.post(endpoint, data={"queryLogLevel": f"{level}"},
                             auth=(rest_info.admin_username, rest_info.admin_password), verify=False)
        resp.raise_for_status()
