from upr.constants import *
from upr_bin_client import UprClient
from basetestcase import BaseTestCase
from mc_bin_client import MemcachedClient
from membase.api.rest_client import RestConnection

class UPRBase(BaseTestCase):

    def setUp(self):
        super(UPRBase, self).setUp()
        rest = RestConnection(self.master)

    def tearDown(self):

        # undo any clustering
        for rest in [RestConnection(node) for node in self.servers]:
            nodes_out = rest.get_nodes()
            if len(nodes_out) > 1:
                self.cluster.rebalance([nodes_out[0]], [], nodes_out[1:])

        super(UPRBase, self).tearDown()


    def load_docs(self, node, vbucket, num_docs, exp = 0, flags = 0, unique = True):
        """ using direct mcd client to control vbucket seqnos.
            keeps track of vbucket and keys stored """

        mcd_client = self.mcd_client(node)
        for i in range(num_docs):
            key = ("key", "key%s"%i)[unique]
            rc = mcd_client.set(key, exp, flags, "val", vbucket)

    def upr_client(self, node, connection_type = PRODUCER, name = None):

        """ create an upr client from Node spec and opens connnection of specified type"""
        client = self.client_helper(node, UPR)
        assert connection_type in (PRODUCER, CONSUMER, NOTIFIER)

        name = name or DEFAULT_CONN_NAME
        if connection_type == PRODUCER:
            client.open_producer(name)
        if connection_type == CONSUMER:
            client.open_consumer(name)
        if connection_type == NOTIFIER:
            client.open_notifier(name)

        return client

    def mcd_client(self, node):
        """ create a mcd client from Node spec """
        return self.client_helper(node, MCD)

    def client_helper(self, node, type_):
        client = None
        assert type_ in (MCD, UPR)

        rest = RestConnection(node)
        client_node = rest.get_nodes_self()
        if type_ == MCD:
            client = MemcachedClient(client_node.ip, client_node.memcached)
        else:
            client = UprClient(client_node.ip, client_node.memcached)

        assert client is not None, ENO_CLIENT
        return client

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
        mcd_client = self.mcd_client(node)
        stats = mcd_client.stats(FAILOVER_STAT)
        assert len(stats) > vbucket,  ENO_STAT
        id_key = 'vb_{0}:{1}:id'.format(vbucket, table_entry)
        seq_key = 'vb_{0}:{1}:seq'.format(vbucket, table_entry)
        return long(stats[id_key]), long(stats[seq_key])

    def vb_seqno(self, node, vbucket):
        mcd_client = self.mcd_client(node)
        stats = mcd_client.stats(VBSEQNO_STAT)
        assert len(stats) > vbucket, ENO_STAT
        id_key = 'vb_{0}:high_seqno'.format(vbucket)
        return long(stats[id_key])
