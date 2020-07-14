from lib.remote.remote_util import RemoteMachineShellConnection


class CollectionsStats(object):
    def __init__(self, node):
        self.node = node
        self.shell = RemoteMachineShellConnection(self.node)

    def get_collection_stats(self, bucket):
        output, error = self.shell.execute_cbstats(bucket, "collections", cbadmin_user="Administrator")
        return output, error

    def get_collection_vb_stats(self, bucket, vbid=None):
        cmd = "collections-details"
        if vbid:
            cmd += vbid
        output, error = self.shell.execute_cbstats(bucket, cmd)
        return output, error

    def get_scope_stats(self, bucket):
        output, error = self.shell.execute_cbstats(bucket, "scopes")
        return output, error

    def get_scope_vb_stats(self, bucket, vbid=None):
        cmd = "scopes-details"
        if vbid:
            cmd += vbid
        output, error = self.shell.execute_cbstats(bucket, cmd)
        return output, error

    def get_scope_id(self, bucket, scope, cbstats=None):
        if not cbstats:
            cbstats, _ = self.get_collection_stats(bucket)
        for stat in cbstats:
            stat = stat.replace(' ', '')
            if ":scope_name:" + scope in stat:
                return stat.split(":")[0]
        return None

    def get_collection_id(self, bucket, scope, collection, cbstats=None):
        if not cbstats:
            cbstats, _ = self.get_collection_stats(bucket)
        scope_id = self.get_scope_id(bucket, scope, cbstats)
        for stat in cbstats:
            stat = stat.replace(' ', '')
            if ":name:" + collection in stat:
                if stat.split(":")[0] == scope_id:
                    return stat.split(":name:")[0]
        return None

    def get_scope_item_count(self, bucket, scope, node=None):
        count = 0
        if not node:
            nodes = [self.node]
        elif isinstance(node, list):
            nodes = node
        else:
            nodes = [node]
        for node in nodes:
            cbstats, _ = RemoteMachineShellConnection(node).get_collection_stats(bucket)
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

    def get_collection_item_count(self, bucket, scope, collection, node=None):
        count = 0
        if not node:
            nodes = [self.node]
        elif isinstance(node, list):
            nodes = node
        else:
            nodes = [node]
        for node in nodes:
            cbstats, _ = RemoteMachineShellConnection(node).execute_cbstats(bucket, "collections",
                                                                            cbadmin_user="Administrator")
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


