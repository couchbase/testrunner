from membase.api.rest_client import RestConnection, Bucket, BucketStats, OtpNode, Node
from remote.remote_util import RemoteMachineShellConnection
from TestInput import TestInputSingleton
import pyes
import logger
import time

log = logger.Logger.get_logger()

class EsRestConnection(RestConnection):
    def __init__(self, serverInfo, proto = "http"):
        #serverInfo can be a json object
        #only connect pyes to master es node
        #in the case that other nodes are taken down
        #because http requests will fail
        # TODO: dynamic master node detection
        if isinstance(serverInfo, dict):
            self.ip = serverInfo["ip"]
            self.username = serverInfo["username"]
            self.password = serverInfo["password"]
            self.port = 9091 #serverInfo["port"]
        else:
            self.ip = serverInfo.ip
            self.username = serverInfo.rest_username
            self.password = serverInfo.rest_password
            self.port = 9091 # serverInfo.port

        self.baseUrl = "http://{0}:{1}/".format(self.ip, self.port)
        self.capiBaseUrl = self.baseUrl
        self.http_port = str(int(self.port) + 109)
        self.proto = proto
        self.conn = pyes.ES((self.proto,self.ip,self.http_port))
        self.test_params = TestInputSingleton.input

    def get_index_stats(self):
        return pyes.index_stats()

    def get_indices(self):
        return self.conn.indices.get_indices()

    def get_indices_as_buckets(self):
        buckets = []
        indices = self.get_indices()

        for index in indices:
            bucket = Bucket()
            stats = self.conn.indices.stats()['indices'][index]
            bucket.name = index
            bucket.type = "es"
            bucket.port = self.port
            bucket.authType = None
            bucket.saslPassword = self.password
            bucket.nodes = list()

            #vBucketServerMap
            bucketStats = BucketStats()
            bucketStats.itemCount = stats['primaries']['docs']['count']
            bucket.stats = bucketStats
            buckets.append(bucket)
            bucket.master_id = "es@"+self.ip

        return buckets

    def get_bucket(self, bucket_name):
        for bucket in self.get_indices_as_buckets():
            if bucket.name == bucket_name:
                return bucket
        return

    def get_buckets(self):
        return self.get_indices_as_buckets()

    def delete_index(self, name):
        self.conn.indices.delete_index(name)
        return self.conn.indices.exists_index(name)

    def create_index(self, name):

        if self.conn.indices.exists_index(name):
            self.delete_index(name)

        self.conn.indices.create_index(name)
        return self.conn.indices.exists_index(name)

    def delete_bucket(self, name):
        return self.delete_index(name)

    def create_bucket(self, *args, **kwargs):
        name  = 'default'

        if len(args) > 0:
            name = args[0]
        else:
            name = kwargs['bucket']

        return self.create_index(name)

    def is_ns_server_running(self, timeout_in_seconds=360):
        return True


    def node_statuses(self, timeout=120):

        otp_nodes = []

        for node in self.get_nodes():

            #get otp,get status
            otp_node = OtpNode(id=node.id,
                               status=node.status)

            otp_node.ip = node.ip
            otp_node.port = node.port
            otp_node.replication = None
            otp_nodes.append(node)

        return otp_nodes


    def get_nodes_self(self, timeout=120):

        for node in self.get_nodes():
            # force to return master node
            if node.port == 9091:
                return node

        return

    def get_nodes(self):

        es_nodes = []
        nodes = self.conn.cluster_nodes()['nodes']
        status = self.conn.cluster_health()['status']
        if status == "green":
            status = "healthy"

        for node_key in nodes:
            nodeInfo = nodes[node_key]
            ex_params = self.get_node_params(nodeInfo)
            nodeInfo.update({'ssh_password' : ex_params.ssh_password,
                             'ssh_username' : ex_params.ssh_username})
            nodeInfo['key'] = node_key
            node = ESNode(nodeInfo)
            node.status = status
            es_nodes.append(node)

        return es_nodes

    def get_node_params(self, info):
        ip, port = parse_addr(info["couchbase_address"])
        clusters = self.test_params.clusters
        master_node = None
        for _id in clusters:
            for node in clusters[_id]:
                if node.ip == ip and int(node.port) == port:
                    return node
                if int(node.port) == 9091:
                    master_node = node

        # use params from master node
        return master_node

    def search_term(self, key, indices=["default"]):
        result = None
        params = {"term":{"_id":key}}
        query = pyes.Search(params)
        row = self.conn.search(query, indices = indices)
        if row.total > 0:
           result = row[0]
        return result

    def term_exists(self, key, indices=["default"]):
        return self.search_term(key, indices = indices) is not None

    def all_docs(self, keys_only = False, indices=["default"],size=10000):

        query = pyes.Search({'match_all' : {}})
        rows = self.conn.search(query, indices=indices, size=size)
        docs = []

        for row in rows:
            if keys_only:
                row = row['meta']['id']
            docs.append(row)

        return docs

    def fetch_bucket_stats(self, bucket='default', zoom='minute'):

        return { "op" : { "samples" : { "xdc_ops" : [0] } } }

    def start_replication(self, *args, **kwargs):
        return "es",self.ip

    def _rebalance_progress(self, *args, **kwargs):
        return 100

    def get_vbuckets(self, *args, **kwargs):
        return ()

    def add_node(self, user='', password='', remoteIp='', port='8091'):
        new_node = self.get_nodes_self()
        new_node.ip = remoteIp
        new_node.port = port

        self.start_es_node(new_node)

    def start_es_node(self, node):

        rmc = RemoteMachineShellConnection(node)

        # define es exec path if not in $PATH environment
        es_bin = 'elasticsearch'
        if 'es_bin' in TestInputSingleton.input.test_params:
            es_bin = TestInputSingleton.input.test_params['es_bin']


        # connect to remote node
        log.info('Starting node: %s:%s' % (node.ip, node.port))
        shell=rmc._ssh_client.invoke_shell()

        # start es service
        shell.send(es_bin+' \n')
        while not shell.recv_ready():
            time.sleep(2)

        rc = shell.recv(1024)
        log.info(rc)

        # wait for new node
        tries = 0
        while tries < 10:
            for cluster_node in self.get_nodes():
                if cluster_node.ip == node.ip and cluster_node.port == int(node.port):
                    return
                else:
                    log.info('Waiting for new node to appear')
                    time.sleep(5)
                    tries = tries + 1

        raise Exception("failed to add node to cluster: %s:%s" % (node.ip,node.port))

    def log_client_error(self, post):
        # cannot post req errors to 9091
        pass

    def vbucket_map_ready(self, *args, **kwargs):
        return True

    def init_cluster(self, *args, **kwargs):
        pass

    def init_cluster_memoryQuota(self, *args, **kwargs):
        pass

    def set_reb_cons_view(self, *args, **kwargs):
        pass

    def set_reb_index_waiting(self, *args, **kwargs):
        pass

    def set_rebalance_index_pausing(self, *args, **kwargs):
        pass

    def set_max_parallel_indexers(self, *args, **kwargs):
        pass

    def set_max_parallel_replica_indexers(self, *args, **kwargs):
        pass

    def rebalance(self, otpNodes, ejectedNodes):
        # shutdown ejected nodes
        # wait for shards to be rebalanced

        nodesToShutdown = \
            [node for node in self.get_nodes() if node.id in ejectedNodes]

        for node in nodesToShutdown:
            self.eject_node(node)

    def eject_node(self, node):
        api = "http://%s:%s/_cluster/nodes/%s/_shutdown" % (node.ip, node.ht_port, node.key)
        status, content, header = self._http_request(api, 'POST', '')
        if status:
            log.info('ejected node: '+node.id)
        else:
            raise Exception("failed to eject node: "+node.id)

    def log_client_error(self, post):
        # cannot post req errors to 9091
        pass

    def vbucket_map_ready(self, *args, **kwargs):
        return True

    def init_cluster(self, *args, **kwargs):
        pass

    def init_cluster_memoryQuota(self, *args, **kwargs):
        pass

    def set_reb_cons_view(self, *args, **kwargs):
        pass

    def set_reb_index_waiting(self, *args, **kwargs):
        pass

    def set_rebalance_index_pausing(self, *args, **kwargs):
        pass

    def set_max_parallel_indexers(self, *args, **kwargs):
        pass

    def set_max_parallel_replica_indexers(self, *args, **kwargs):
        pass

    def rebalance(self, otpNodes, ejectedNodes):
        # shutdown ejected nodes
        # wait for shards to be rebalanced

        nodesToShutdown = \
            [node for node in self.get_nodes() if node.id in ejectedNodes]

        for node in nodesToShutdown:
            self.eject_node(node)

    def eject_node(self, node):
        api = "http://%s:%s/_cluster/nodes/%s/_shutdown" % (node.ip, node.ht_port, node.key)
        status, content, header = self._http_request(api, 'POST', '')
        if status:
            log.info('ejected node: '+node.id)
        else:
            log.error('rebalance operation failed: {0}'.format(content))


    def monitorRebalance(self, stop_if_loop=False):
        # since removed nodes are shutdown use master node for monitoring
        return self.get_nodes_self()

    def get_pools_info(self):
        return {'pools' : []}

    def add_remote_cluster(self, *args, **kwargs):

        # detect 2:1 mapping and do spectial cluster add
        # otherwise run super method
        pass

    def remove_all_remote_clusters(self):
        pass

    def remove_all_replications(self):
        pass

    def is_cluster_mixed(self):
        return False

def parse_addr(addr):
    ip = addr[addr.rfind('/')+1:addr.rfind(':')]
    port = addr[addr.rfind(':')+1:-1]
    return str(ip), int(port)

class ESNode(Node):
    def __init__(self, info):
        super(ESNode, self).__init__()
        self.key = str(info['key'])
        self.ip, self.port = parse_addr(info["couchbase_address"])
        self.tr_ip, self.tr_port = parse_addr(info["transport_address"])

        if 'http_address' in info:
            self.ht_ip, self.ht_port = parse_addr(info["http_address"])

        # truncate after space, or comma
        name = str(info['name'][:info['name'].find(' ')])
        name = name[:name.find(',')]
        self.id = "es_%s@%s" % (name, self.ip)

        self.ssh_username = info['ssh_username']
        self.ssh_password = info['ssh_password']
        self.ssh_key = ''
