import time
from dcp.constants import *
from dcp_bin_client import DcpClient
from basetestcase import BaseTestCase
from mc_bin_client import MemcachedClient, MemcachedError
from lib.cluster_run_manager import CRManager
from membase.api.rest_client import RestConnection
from TestInput import TestInputSingleton
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper


class DCPBase(BaseTestCase):
    def __init__(self, args):
        super(DCPBase, self).__init__(args)
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
            if self.test != self._testMethodName:
                self.skipTest("disabled")

        self.is_setup = True

        if self.use_cluster_run:
            assert self.crm.clean()
            assert self.crm.start_nodes()
            time.sleep(5)

        super(DCPBase, self).setUp()
        self.is_setup = False

    def tearDown(self):
        for index in self.stopped_nodes:
            self.start_node(index)

        if self.use_cluster_run and not self.is_setup:
            assert self.crm.stop_nodes()
            self.cluster.shutdown(force=True)
        else:
            super(DCPBase, self).tearDown()
        for server in self.servers:
            ClusterOperationHelper.cleanup_cluster(self.servers, master=server)

    def load_docs(self, node, vbucket, num_docs,
                  bucket='default', password='',
                  exp=0, flags=0, update=False):
        """ using direct mcd client to control vbucket seqnos.
            keeps track of vbucket and keys stored """

        mcd_client = self.mcd_client(
            node, vbucket, auth_user=True)

        for i in range(num_docs):
            t = 0
            while False or t < 5:
                key = "key%s" % self.doc_num
                try:
                    mcd_client.set(key, exp, flags, "val", vbucket)
                    break
                except MemcachedError:
                    self.sleep(0.5 * t)
                    t += 1
                    mcd_client = self.mcd_client(node, vbucket, auth_user=True)
            if not update:
                self.doc_num += 1

    def dcp_client(
            self, node, connection_type=PRODUCER, vbucket=None, name=None,
            auth_user="cbadminbucket", auth_password="password", bucket_name="default"):
        """ create an dcp client from Node spec and opens connnection of specified type"""
        client = self.client_helper(node, DCP, vbucket)
        if auth_user:
            client.sasl_auth_plain(auth_user, auth_password)
            client.bucket_select(bucket_name)

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

    def mcd_client(
            self, node, vbucket=None,
            auth_user=None, auth_password=None):
        """ create a mcd client from Node spec """

        client = self.client_helper(node, MCD, vbucket)
        if auth_user:
            # admin_user='cbadminbucket',admin_pass='password'
            client.sasl_auth_plain('cbadminbucket', 'password')
            client.bucket_select('default')
        return client

    def client_helper(self, node, type_, vbucket):
        assert type_ in (MCD, DCP)

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
            ip = client_node.hostname.split(':')[0]
            port = client_node.memcached

        if type_ == MCD:
            client = MemcachedClient(ip, port)
        else:
            client = DcpClient(ip, port)

        return client

    def vbucket_host(self, rest, vbucket):
        info = rest.get_bucket_json()
        return info['vBucketServerMap']['serverList'] \
            [info['vBucketServerMap']['vBucketMap'][vbucket][0]]

    def vbucket_host_index(self, rest, vbucket):
        info = rest.get_bucket_json()
        host = self.vbucket_host(rest, vbucket)
        return info['vBucketServerMap']['serverList'].index(host)

    def flow_control_info(self, node, connection=None):

        connection = connection or DEFAULT_CONN_NAME
        mcd_client = self.mcd_client(node)
        stats = mcd_client.stats(DCP)

        acked = 'eq_dcpq:{0}:total_acked_bytes'.format(connection)
        unacked = 'eq_dcpq:{0}:unacked_bytes'.format(connection)
        sent = 'eq_dcpq:{0}:total_bytes_sent'.format(connection)
        return int(stats[acked]), int(stats[sent]), int(stats[unacked])

    def all_vb_info(self, node, table_entry=0, bucket='default', password=''):

        print('*****in all vbinfo')

        vbInfoMap = {}
        rest = RestConnection(node)
        vbuckets = rest.get_vbuckets()

        mcd_client = self.mcd_client(
            node, auth_user=bucket, auth_password=password)
        failoverStats = mcd_client.stats(FAILOVER_STAT)
        seqnoStats = mcd_client.stats(VBSEQNO_STAT)

        for vb in vbuckets:
            vbucket = vb.id
            id_key = 'vb_{0}:{1}:id'.format(vbucket, table_entry)
            seq_key = 'vb_{0}:{1}:seq'.format(vbucket, table_entry)
            hi_key = 'vb_{0}:high_seqno'.format(vbucket)
            vb_uuid, seqno, high_seqno = \
                (int(failoverStats[id_key]),
                 int(failoverStats[seq_key]),
                 int(seqnoStats[hi_key]))
            vbInfoMap[vbucket] = (vb_uuid, seqno, high_seqno)

        return vbInfoMap

    def vb_info(self, node, vbucket, table_entry=0, bucket='default', password=''):
        vb_uuid, seqno = self.vb_failover_entry(
            node, vbucket, table_entry, bucket, password)
        high_seqno = self.vb_seqno(
            node, vbucket, bucket, password)

        return vb_uuid, seqno, high_seqno

    def vb_failover_entry(self, node, vbucket, table_entry=0,
                          bucket='default', password=''):

        mcd_client = self.mcd_client(
            node, vbucket, auth_user=bucket, auth_password=password)
        stats = mcd_client.stats(FAILOVER_STAT)
        assert len(stats) > vbucket, ENO_STAT
        id_key = 'vb_{0}:{1}:id'.format(vbucket, table_entry)
        seq_key = 'vb_{0}:{1}:seq'.format(vbucket, table_entry)
        return int(stats[id_key]), int(stats[seq_key])

    def vb_seqno(self, node, vbucket, bucket='default', password=''):
        mcd_client = self.mcd_client(
            node, vbucket, auth_user=bucket, auth_password=password)
        stats = mcd_client.stats(VBSEQNO_STAT)
        assert len(stats) > vbucket, ENO_STAT
        id_key = 'vb_{0}:high_seqno'.format(vbucket)
        return int(stats[id_key])

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
