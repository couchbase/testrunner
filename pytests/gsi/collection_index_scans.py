from couchbase_helper.query_definitions import QueryDefinition
from .base_gsi import BaseSecondaryIndexingTests
from membase.api.on_prem_rest_client import RestHelper, RestConnection
from serverless.gsi_utils import GSIUtils
import random
import string
from remote.remote_util import RemoteMachineShellConnection
from lib import testconstants


class Collection_index_scans(BaseSecondaryIndexingTests):
    def setUp(self):
        super(Collection_index_scans, self).setUp()
        self.log.info("==============  CollectionsIndexScans setup has started ==============")
        self.rest.delete_all_buckets()
        self.query_node = self.get_nodes_from_services_map(service_type="n1ql", get_all_nodes=True)[0]
        self.gsi_util_obj = GSIUtils(self.run_cbq_query)
        self.num_scans = self.input.param('num_scans', 100)
        self.log.info("==============  CollectionsIndexScans setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  CollectionsIndexScans tearDown has started ==============")
        super(Collection_index_scans, self).tearDown()
        self.log.info("==============  CollectionsIndexScans tearDown has completed ==============")

    def test_index_scans_balance(self):
        self.prepare_tenants(index_creations=False)
        self.buckets = self.rest.get_buckets()
        index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        query_1 = QueryDefinition(index_name='idx_1', index_fields=['address'])
        query_2 = QueryDefinition(index_name='idx_2', index_fields=['name'])
        create_query_1 = query_1.generate_index_create_query(namespace=self.buckets[0], deploy_node_info=[f"{index_nodes[0].ip}:{self.node_port}"])
        create_query_2 = query_2.generate_index_create_query(namespace=self.buckets[1], deploy_node_info=[f"{index_nodes[1].ip}:{self.node_port}", f"{index_nodes[2].ip}:{self.node_port}"])

        index_create_list = [create_query_1, create_query_2]
        for query in index_create_list:
            self.run_cbq_query(query=query)
            self.sleep(1)
        for i in range(1, self.num_scans+1):
            index_scan_query = f"SELECT * FROM {self.buckets[1].name} WHERE name LIKE \"%a%\";"
            self.log.info(index_scan_query)
            self.run_cbq_query(query=index_scan_query)
            self.sleep(1)
        index_stats = []
        for index_node in index_nodes[1:]:
            index_rest = RestConnection(index_node)
            stat_data = index_rest.get_index_stats()
            index_stats.append(stat_data)
        num_requests_1 = num_requests_2 = None

        try:
            self.log.info(f"Num requests for node 1 : {index_stats[0][self.buckets[1].name]['idx_2']['num_requests']}")
            num_requests_1 = index_stats[0][self.buckets[1].name]['idx_2']['num_requests']
        except:
            self.log.info(
                f"Num requests for node 2 : {index_stats[0][self.buckets[1].name]['idx_2 (replica 1)']['num_requests']}")
            num_requests_1 = index_stats[0][self.buckets[1].name]['idx_2 (replica 1)']['num_requests']
        try:
            self.log.info(
                f"Num requests for node 2 : {index_stats[1][self.buckets[1].name]['idx_2 (replica 1)']['num_requests']}")
            num_requests_2 = index_stats[1][self.buckets[1].name]['idx_2 (replica 1)']['num_requests']
        except:
            self.log.info(
                f"Num requests for node 2 : {index_stats[1][self.buckets[1].name]['idx_2']['num_requests']}")
            num_requests_2 = index_stats[1][self.buckets[1].name]['idx_2']['num_requests']
        num_requests_difference = (abs(num_requests_2-num_requests_1)//(self.num_scans//2))*100
        self.assertLessEqual(num_requests_difference, 20, 'Too much difference in the scans served by each node')
