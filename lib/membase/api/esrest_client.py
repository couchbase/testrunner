from membase.api.rest_client import RestConnection, Bucket, BucketStats, OtpNode, Node
import pyes

class EsRestConnection(RestConnection):
    def __init__(self, serverInfo, proto = "http"):
        #serverInfo can be a json object
        if isinstance(serverInfo, dict):
            self.ip = serverInfo["ip"]
            self.username = serverInfo["username"]
            self.password = serverInfo["password"]
            self.port = serverInfo["port"]
        else:
            self.ip = serverInfo.ip
            self.username = serverInfo.rest_username
            self.password = serverInfo.rest_password
            self.port = serverInfo.port

        self.baseUrl = "http://{0}:{1}/".format(self.ip, self.port)
        self.capiBaseUrl = self.baseUrl
        http_port = str(int(self.port) + 109)
        self.conn = pyes.ES((proto,self.ip,http_port))

    def get_index_stats(self):
        return pyes.index_stats()

    def get_indices(self):
        return self.conn.indices.get_indices()

    def get_indices_as_buckets(self):
        buckets = []
        indices = self.get_indices()

        for index in indices:
            bucket = Bucket()
            stats = self.conn.indices.stats()['_all']['indices'][index]
            bucket.name = index
            bucket.type = "es"
            bucket.port = self.port
            bucket.authType = None
            bucket.saslPassword = self.password
            bucket.nodes = list()

            #vBucketServerMap
            bucketStats = BucketStats()
            bucketStats.itemCount = stats['total']['docs']['count']
            bucket.stats = bucketStats
            buckets.append(bucket)
            bucket.master_id = "es@"+self.ip

        return buckets

    def get_buckets(self):
        return self.get_indices_as_buckets()

    def delete_index(self, name):
        self.conn.indices.delete_index(name)
        return self.conn.indices.exists_index(name)

    def create_index(self, name):
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
        nodes = []
        node_stats = self.conn.cluster_nodes()['nodes']

        for key in node_stats:
            status = self.conn.cluster_health()['status']
            if status == "green":
                status = "healthy"

            #get otp,get status
            node = OtpNode(id="es@"+self.ip,
                           status=status)

            addr = node_stats[key]['http_address']
            node.ip = addr[addr.rfind('/')+1:addr.rfind(':')]
            node.port = addr[addr.rfind(':')+1:]
            node.replication = None
            nodes.append(node)

        # for now only return master node
        # to prevent automation from trying
        # to rebalance
        return [nodes[0]]


    def get_nodes_self(self, timeout=120):
        master = self.node_statuses(timeout)[0]

        node = Node()
        node.status = master.status
        node.id = master.id
        node.ip = master.ip
        node.port = master.port
        return node

    def get_nodes(self):
        return [self.get_nodes_self()]

    def start_replication(self, *args, **kwargs):
        return "es",self.ip

    def _rebalance_progress(self, *args, **kwargs):
        return 100

    def get_vbuckets(self, *args, **kwargs):
        return ()

    def add_node(self, *args, **kwargs):
        #TODO: add es nodes
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
        pass

    def add_remote_cluster(self, *args, **kwargs):
        pass

    def remove_all_remote_clusters(self):
        pass

    def remove_all_replications(self):
        pass


