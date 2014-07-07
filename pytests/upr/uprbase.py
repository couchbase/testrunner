import time
from upr.constants import *
from upr_bin_client import UprClient
from basetestcase import BaseTestCase
from mc_bin_client import MemcachedClient
from lib.cluster_run_manager  import CRManager
from membase.api.rest_client import RestConnection
from TestInput import TestInputSingleton
from remote.remote_util import RemoteMachineShellConnection

class UPRBase(BaseTestCase):


    def __init__(self, args):
        super(UPRBase, self).__init__(args)
        self.is_setup = False
        self.crm = None
        self.input = TestInputSingleton.input
        self.use_cluster_run = self.input.param('dev', False)
        self.test = self.input.param('test', None)
        self.stopped_nodes = []

        self.doc_num = 0
        if self.use_cluster_run:
            num_nodes = self.input.param('num_nodes', 4)
            self.crm = CRManager(num_nodes, 0)

    def setUp(self):

        if self.test:
            if self.test !=  self._testMethodName:
                self.skipTest("disabled")

        self.is_setup = True

        if self.use_cluster_run:
            assert self.crm.clean()
            assert self.crm.start_nodes()
            time.sleep(5)

        super(UPRBase, self).setUp()
        self.is_setup = False

    def tearDown(self):

        for index in self.stopped_nodes:
            self.start_node(index)

        if self.use_cluster_run and not self.is_setup:
            assert self.crm.stop_nodes()
            self.cluster.shutdown(force=True)
        else:
            super(UPRBase, self).tearDown()

    def load_docs(self, node, vbucket, num_docs, exp = 0, flags = 0, update = False):
        """ using direct mcd client to control vbucket seqnos.
            keeps track of vbucket and keys stored """

        mcd_client = self.mcd_client(node, vbucket)
        for i in range(num_docs):
            key = "key%s"%self.doc_num
            rc = mcd_client.set(key, exp, flags, "val", vbucket)
            if not update:
                self.doc_num += 1


    def upr_client(self, node, connection_type = PRODUCER, vbucket = None, name = None):

        """ create an upr client from Node spec and opens connnection of specified type"""
        client = self.client_helper(node, UPR, vbucket)
        assert connection_type in (PRODUCER, CONSUMER, NOTIFIER)

        name = name or DEFAULT_CONN_NAME
        if connection_type == PRODUCER:
            response = client.open_producer(name)
        if connection_type == CONSUMER:
            response = client.open_consumer(name)
        if connection_type == NOTIFIER:
            response = client.open_notifier(name)

        assert response['status'] == SUCCESS
        return client

    def mcd_client(self, node, vbucket = None):
        """ create a mcd client from Node spec """
        return self.client_helper(node, MCD, vbucket)

    def client_helper(self, node, type_, vbucket):
        assert type_ in (MCD, UPR)

        client = None
        ip = None
        port = None
        rest = RestConnection(node)

        if vbucket is not None:
            host = self.vbucket_host(rest, vbucket)
            ip = host.split(':')[0]
            port = int(host.split(':')[1])

        else:
            client_node = rest.get_nodes_self()
            ip = client_node.ip
            port = client_node.memcached

        if type_ == MCD:
            client = MemcachedClient(ip, port)
        else:
            client = UprClient(ip, port)

        return client

    def vbucket_host(self, rest, vbucket):
        info = rest.get_bucket_json()
        return info['vBucketServerMap']['serverList']\
            [info['vBucketServerMap']['vBucketMap'][vbucket][0]]

    def vbucket_host_index(self, rest, vbucket):
        info = rest.get_bucket_json()
        host = self.vbucket_host(rest, vbucket)
        return info['vBucketServerMap']['serverList'].index(host)
        
    def flow_control_info(self, node, connection = None):

        connection = connection or DEFAULT_CONN_NAME
        mcd_client = self.mcd_client(node)
        stats = mcd_client.stats(UPR)

        acked = 'eq_uprq:{0}:total_acked_bytes'.format(connection)
        unacked = 'eq_uprq:{0}:unacked_bytes'.format(connection)
        sent = 'eq_uprq:{0}:total_bytes_sent'.format(connection)
        return int(stats[acked]), int(stats[sent]), int(stats[unacked])

    def vb_info(self, node, vbucket, table_entry = 0):
        vb_uuid, seqno = self.vb_failover_entry(node, vbucket, table_entry)
        high_seqno = self.vb_seqno(node, vbucket)

        return vb_uuid, seqno, high_seqno

    def vb_failover_entry(self, node, vbucket, table_entry = 0):
        mcd_client = self.mcd_client(node, vbucket)
        stats = mcd_client.stats(FAILOVER_STAT)
        assert len(stats) > vbucket,  ENO_STAT
        id_key = 'vb_{0}:{1}:id'.format(vbucket, table_entry)
        seq_key = 'vb_{0}:{1}:seq'.format(vbucket, table_entry)
        return long(stats[id_key]), long(stats[seq_key])

    def vb_seqno(self, node, vbucket):
        mcd_client = self.mcd_client(node, vbucket)
        stats = mcd_client.stats(VBSEQNO_STAT)
        assert len(stats) > vbucket, ENO_STAT
        id_key = 'vb_{0}:high_seqno'.format(vbucket)
        return long(stats[id_key])

    def stop_node(self, index):
        status = False
        if self.use_cluster_run:
            status = self.crm.stop(index)
        elif len(self.servers) >= index:
            node = self.servers[index]
            shell = RemoteMachineShellConnection(node)
            shell.stop_couchbase()
            shell.disconnect()
            status = True

        return status

    def start_node(self, index):
        status = False
        if self.use_cluster_run:
            status = self.crm.start(index)
        elif len(self.servers) >= index:
            node = self.servers[index]
            shell = RemoteMachineShellConnection(node)
            shell.start_couchbase()
            shell.disconnect()
            status = True

        return status
