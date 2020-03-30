import re
import logger
import time
import unittest
import copy
from .uibasetest import *
from .uisampletests import NavigationHelper, BucketHelper, ServerTestControls, ServerHelper
from TestInput import TestInputSingleton
from couchbase_helper.cluster import Cluster
from membase.api.rest_client import RestConnection, Bucket
from couchbase_helper.documentgenerator import BlobGenerator

STANDARD_BUCKET_PORT = 11211


class  NodeServiceTests(BaseUITestCase):
    def setUp(self):
        super(NodeServiceTests, self).setUp()
        self.helper = ServerHelper(self)
        num_buckets = self.input.param("num_buckets", 1)
        compression = self.input.param("sdk_compression", True)
        for i in range(num_buckets):
            RestConnection(self.servers[0]).create_bucket(bucket='bucket%s' % i, ramQuotaMB=100, proxyPort=STANDARD_BUCKET_PORT + i + 1)
            gen_load = BlobGenerator('ui', 'ui-', 256, start=0, end=10)
            cluster = Cluster()
            try:
                gen = copy.deepcopy(gen_load)
                task = cluster.async_load_gen_docs(self.servers[0], 'bucket%s' % i, gen,
                                                   Bucket().kvs[1], 'create',
                                                   0, 0, True, 1, 1, 30, compression=compression)
                task.result()
            finally:
                cluster.shutdown()
        BaseHelper(self).login()

    def tearDown(self):
        super(NodeServiceTests, self).tearDown()

    def test_add_node(self):
        # kv n1ql moxi index
        services = self.input.param("services", '').split(';')
        error = self.input.param("error", '')
        NavigationHelper(self).navigate('Server Nodes')
        #try:
        #     self.helper.add(self.input, services=services)
        # except Exception, ex:
        #     if error and str(ex).find(error) != -1:
        #         self.log.info('Error appeared as expected')
        #     else:
        #         self.fail('unexpected error appeared %s' % str(ex))
        # else:
        #     if error:
        #         self.fail('Expected error didn\'t appered')

    def test_add_node_warning(self):
        services = self.input.param("services", '').split(';')
        warning = self.input.param("warning", '')
        NavigationHelper(self).navigate('Server Nodes')
        current_warning = self.helper.add_warnings(self.input, services=services)
        self.assertTrue(current_warning.find(warning) !=-1, 'there was no expected warnings')

    def test_rebalance_in(self):
        service = self.input.param("services", '').split(';')
        NavigationHelper(self).navigate('Server Nodes')
        self.helper.add(self.input, services=service)
        self.helper.start_rebalancing()
        self.sleep(3, 'Wait some progress')
        self.helper.wait_for_rebalance_stops()
        self.log.info("Check finished")
        if 'kv' in service:
            self.assertTrue(not self.helper.get_items(self.servers[1]).startswith('0'), 'Data is not in server!')
        else:
            self.assertTrue(self.helper.get_items(self.servers[1]).startswith('0'), 'Data is in server!')
        self.log.info("Check finished")

    def test_rebalance_out(self):
        service = self.input.param("services", '').split(';')
        NavigationHelper(self).navigate('Server Nodes')
        self.helper.add(self.input, index=1, services=service)
        self.helper.start_rebalancing()
        self.sleep(3, 'Wait some progress')
        self.helper.wait_for_rebalance_stops()
        self.helper.remove_node(ip=self.servers[1].ip, skip_wait=False)
        self.helper.wait_for_rebalance_stops()

    def test_rebalance_out_last_data(self):
        service = self.input.param("services", '').split(';')
        warn = self.input.param("warning", '')
        NavigationHelper(self).navigate('Server Nodes')
        self.helper.add(self.input, index=1, services=service)
        self.helper.start_rebalancing()
        self.sleep(3, 'Wait some progress')
        self.helper.wait_for_rebalance_stops()
        self.helper.remove_node(ip=self.servers[0].ip, skip_wait=True)
        self.helper.start_rebalancing()
        alert, text = self.helper.get_alert_text()
        self.assertTrue(text.find(warn) != -1, 'Alert text does not match: %s' % alert)
        alert.accept()

    def test_rebalance_swap(self):
        self.assertTrue(len(self.servers) > 2, 'Test needs more than 2 node')
        service = self.input.param("services", '').split(';')
        NavigationHelper(self).navigate('Server Nodes')
        self.helper.add(self.input, index=1, services=service)
        self.helper.start_rebalancing()
        self.sleep(3, 'Wait some progress')
        self.helper.wait_for_rebalance_stops()
        self.helper.remove_node(ip=self.servers[1].ip)
        self.helper.add(self.input, index=2, services=service)
        self.helper.start_rebalancing()
        self.sleep(3, 'Wait some progress')
        self.helper.wait_for_rebalance_stops()
        self.assertTrue(self.helper.get_number_server_rows() == 2, 'Node was not added')

    def test_failover(self):
        service = self.input.param("services", '').split(';')
        NavigationHelper(self).navigate('Server Nodes')
        self.helper.add(self.input, index=1, services=service)
        self.helper.start_rebalancing()
        self.sleep(3, 'Wait some progress')
        self.helper.wait_for_rebalance_stops()
        if len(self.servers) < 2:
            self.fail("There is no enough VMs. Need at least 2")
        ServerHelper(self).failover(self.servers[1], confirm=True, graceful=True)