from lib.remote.remote_util import RemoteMachineShellConnection



class CollectionsStats(object):
    def __init__(self, node):
        self.shell = RemoteMachineShellConnection(node)

    def get_collection_stats(self, bucket):
        output, error = self.shell.execute_cbstats(bucket, "collections")
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
