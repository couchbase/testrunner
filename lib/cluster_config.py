class ClusterConfig:
    def __init__(self, username, password, servers):
        self.username = username
        self.password = password
        self.cbas_node = None
        self.servers = servers
        self.update_servers(servers)

    def update_servers(self, servers):
        for server in self.servers:
            server.dummy = True
        for i, server in enumerate(servers):
            server.dummy = False
            self.servers[i] = server
        
        self.master = self.servers[0]
        self.indexManager = self.servers[0]
        self.cbas_servers = []
        self.kv_servers = []
        for server in self.servers:
            if "cbas" in server.services:
                self.cbas_servers.append(server)
            if "kv" in server.services:
                self.kv_servers.append(server)
        if not self.cbas_node and len(self.cbas_servers) >= 1:
            self.cbas_node = self.cbas_servers[0]