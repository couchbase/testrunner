from memcached.helper.data_helper import MemcachedClientHelper

class CollectionsStats(object):
    def __init__(self, node):
        self.node = node
        self.clients = {}

    def client(self, bucket):
        if bucket not in self.clients:
            self.clients[bucket] = MemcachedClientHelper.direct_client(self.node, bucket)
        return self.clients[bucket]

    def get_collection_stats(self, bucket, client=None):
        client = client or self.client(bucket)
        stats = client.stats("collections")
        # Produce same output as cbstats
        output = []
        for key, value in stats.items():
            output.append("{}: {}".format(key, value))
        return output

    def get_collection_vb_stats(self, bucket, vbid=None):
        cmd = "collections-details"
        if vbid:
            cmd += vbid
        return self.client(bucket).stats(cmd)

    def get_scope_stats(self, bucket):
        return self.client(bucket).stats("scopes")

    def get_scope_vb_stats(self, bucket, vbid=None):
        cmd = "scopes-details"
        if vbid:
            cmd += vbid
        output, error = self.shell.execute_cbstats(bucket, cmd)
        return output, error

    def get_scope_id(self, bucket, scope, cbstats=None):
        if not cbstats:
            cbstats = self.get_collection_stats(bucket)
        for stat in cbstats:
            stat = stat.replace(' ', '')
            if ":scope_name:" + scope in stat:
                return stat.split(":")[0]
        return None

    def get_collection_id(self, bucket, scope, collection, cbstats=None, hex=False):
        if not cbstats:
            cbstats = self.get_collection_stats(bucket)
        scope_id = self.get_scope_id(bucket, scope, cbstats)
        for stat in cbstats:
            stat = stat.replace(' ', '')
            if ":name:" + collection in str(stat):
                if stat.split(":")[0] == scope_id.strip():
                    return stat.split(":name:")[0].split(":")[1]
        return None

    def get_scope_item_count(self, bucket, scope, node=None, cbstats=None):
        count = 0
        if not node:
            nodes = [self.node]
        elif isinstance(node, list):
            nodes = node
        else:
            nodes = [node]
        for node in nodes:
            if not cbstats:
                cbstats = self.get_collection_stats(bucket, MemcachedClientHelper.direct_client(node, bucket))
            scope_id = self.get_scope_id(bucket, scope, cbstats)
            id_counts = {}
            for stat in cbstats:
                if ":items:" in stat:
                    stat = stat.replace(' ', '')
                    id_counts[stat.split(":items:")[0]] = int(stat.split(":items:")[1])
            for id in id_counts.keys():
                if id.split(':')[0] == scope_id:
                    count += id_counts[id]
        return count

    def get_collection_item_count(self, bucket, scope, collection, node=None, cbstats=None):
        count = 0
        if not node:
            nodes = [self.node]
        elif isinstance(node, list):
            nodes = node
        else:
            nodes = [node]
        for node in nodes:
            if not cbstats:
                cbstats = self.get_collection_stats(bucket, MemcachedClientHelper.direct_client(node, bucket))
            collection_id = self.get_collection_id(bucket, scope, collection, cbstats)
            id_counts = {}
            for stat in cbstats:
                if ":items:" in stat:
                    stat = stat.replace(' ', '')
                    id_count = stat.split(":items:")
                    id_counts[id_count[0].strip()] = int(id_count[1])

            for id in id_counts.keys():
                if id == collection_id:
                    count += id_counts[id]
        return count

    def get_collection_item_count_cumulative(self, bucket, scope, collection, node=None):
        count = 0
        if not node:
            nodes = [self.node]
        elif isinstance(node, list):
            nodes = node
        else:
            nodes = [node]
        for node in nodes:
            cbstats = self.get_collection_stats(bucket, MemcachedClientHelper.direct_client(node, bucket))
            collection_id = self.get_collection_id(bucket, scope, collection, cbstats)
            scope_id = self.get_scope_id(bucket, scope, cbstats)
            id_counts = {}
            for stat in cbstats:
                if ":items:" in stat:
                    stat = stat.replace(' ', '')
                    id_count = stat.split(":items:")
                    id_counts[id_count[0].strip()] = int(id_count[1])

            for id in id_counts.keys():
                if id == f'{scope_id}:{collection_id}':
                    count += id_counts[id]
        return count