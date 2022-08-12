import requests


class ServerlessRestConnection:
    def __init__(self, rest_username, rest_password, rest_srv):
        self.rest_username = rest_username
        self.rest_password = rest_password
        self.rest_srv = rest_srv

    def get_all_dataplane_nodes(self):
        endpoint = "https://{}:18091/pools/default".format(self.rest_srv)
        resp = requests.get(endpoint, auth=(self.rest_username, self.rest_password), verify=False)
        resp.raise_for_status()
        return resp.json()['nodes']

    def get_all_nodes_in_subcluster(self, server_group=None, service=None):
        nodes_obj = self.get_all_dataplane_nodes()
        sub_cluster_obj = []
        for node in nodes_obj:
            if node['serverGroup'] == server_group:
                if service is not None:
                    if service in node['services']:
                        sub_cluster_obj.append(node)
                else:
                    sub_cluster_obj.append(node)
        return sub_cluster_obj

