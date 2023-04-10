import time
from json import loads

from membase.api.on_prem_rest_client import RestConnection as OnPremRestConnection, RestParser
import requests
from lib.Cb_constants.CBServer import CbServer
import logger


class ServerlessRestConnection(OnPremRestConnection):
    def __new__(cls, rest_username, rest_password, rest_srv):
        server_info = {"ip": rest_srv,
                       "username": rest_username,
                       "password": rest_password,
                       "port": 18091}
        CbServer.use_https = True
        return OnPremRestConnection.__new__(cls, server_info)

    def __init__(self, rest_username, rest_password, rest_srv):
        self.rest_username = rest_username
        self.rest_password = rest_password
        self.rest_srv = rest_srv
        self.log = logger.Logger.get_logger()
        server_info = {"ip": self.rest_srv,
                       "username": self.rest_username,
                       "password": self.rest_password,
                       "port": 18091}
        super(ServerlessRestConnection, self).__init__(server_info)

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

    def poll_for_tasks(self, task_type):
        api = "https://{}:18091/pools/default/tasks".format(self.rest_srv)
        resp = requests.get(api, auth=(self.rest_username, self.rest_password), verify=False)
        resp.raise_for_status()
        tasks = resp.json()
        for task in tasks:
            if task['type'] == task_type:
                return task

    def collect_logs(self, upload_url="cb-engineering.s3.amazonaws.com", nodes="*", test_name="QE", timeout=20):
        """
        collects the log bundles for all the nodes in the DP by default. Can be parameterised to
        target specific nodes
        upload_url: S3 bucket to which the logs need to be uploaded
        nodes: * refers to all the nodes
        test_name: Customer field in the upload request
        timeout: duration in minutes
        """
        data = f'uploadHost={upload_url}%2F&customer={test_name}&&nodes={nodes}'
        api = f"https://{self.rest_srv}:18091/controller/startLogsCollection"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        resp = requests.post(api, data=data, verify=False, headers=headers,
                             auth=(self.rest_username, self.rest_password))
        resp.raise_for_status()
        time.sleep(30)
        log_collection_complete, cb_collect_list, time_now = False, [], time.time()
        while not log_collection_complete and time.time() - time_now < timeout * 60:
            log_collection_task = self.poll_for_tasks("clusterLogsCollection")
            self.log.info(
                f"Waiting for the log collection process to complete. Current task status {log_collection_task}")
            if log_collection_task['status'] == 'completed':
                for perNode in log_collection_task['perNode']:
                    if log_collection_task['perNode'][perNode]['status'] == 'uploaded':
                        cb_collect_list.append(log_collection_task['perNode'][perNode]['url'])
                log_collection_complete = True
            time.sleep(30)
        self.log.info(f"cb_collect bundle for the test {test_name} list: {cb_collect_list}")
        return cb_collect_list

    def query_prometheus(self, query):
        api = f"{self.baseUrl}_prometheus/api/v1/query?query={query}"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        auth = (self.rest_username, self.rest_password)
        resp = requests.get(api, verify=False, headers=headers, auth=auth)
        return resp.status_code, loads(resp.content)
