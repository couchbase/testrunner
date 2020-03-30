class rbacRoles:
    ''' Variable setup for comparison of roles using checkPermission'''

    @staticmethod
    def _admin_role_master():
        per_set = {
            "name":"Role master for Admin Role",
            "permissionSet":"cluster.admin.internal!all, cluster.admin.setup!write, \
                    cluster.admin.security!read,cluster.admin.security!write,cluster.admin.logs!read,cluster.pools!read,cluster.pools!write,\
                    cluster.nodes!read,cluster.nodes!write,cluster.samples!read,cluster.settings!read,cluster.settings!write,cluster.tasks!read,\
                    cluster.stats!read,cluster.server_groups!read,cluster.server_groups!write,cluster.indexes!read,cluster.indexes!write,\
                    cluster.xdcr.settings!read,cluster.xdcr.settings!write,cluster.xdcr.remote_clusters!read,cluster.xdcr.remote_clusters!write,\
                    cluster.bucket[<bucket_name>]!create,cluster.bucket[<bucket_name>]!delete,cluster.bucket[<bucket_name>]!compact,\
                    cluster.bucket[<bucket_name>].settings!read,cluster.bucket[<bucket_name>].settings!write,cluster.bucket[<bucket_name>].password!read,\
                    cluster.bucket[<bucket_name>].data!read,cluster.bucket[<bucket_name>].data!write,cluster.bucket[<bucket_name>].recovery!read,\
                    cluster.bucket[<bucket_name>].recovery!write,cluster.bucket[<bucket_name>].views!read,cluster.bucket[<bucket_name>].views!write,\
                    cluster.bucket[<bucket_name>].xdcr!read,cluster.bucket[<bucket_name>].xdcr!write,cluster.bucket[<bucket_name>].xdcr!execute"
            }
        return per_set

    @staticmethod
    def _admin_role_expected():
        per_set = {
            "name":"Expected Admin roles",
            "permissionSet": {'cluster.bucket[<bucket_name>]!create': True, 'cluster.bucket[<bucket_name>].xdcr!write': True, 'cluster.admin.logs!read': True, 'cluster.bucket[<bucket_name>]!delete': True, \
                              'cluster.pools!read': True, 'cluster.bucket[<bucket_name>].xdcr!read': True, 'cluster.bucket[<bucket_name>].views!write': True, 'cluster.tasks!read': True,\
                              'cluster.nodes!write': True, 'cluster.server_groups!read': True,  'cluster.bucket[<bucket_name>].recovery!write': True , \
                              'cluster.bucket[<bucket_name>].password!read': True, 'cluster.admin.internal!all': True, 'cluster.admin.setup!write': True, 'cluster.pools!write': True,\
                              'cluster.indexes!write': True, 'cluster.indexes!read': True, 'cluster.nodes!read': True, 'cluster.xdcr.remote_clusters!read': True, 'cluster.admin.security!write': True,
                              'cluster.bucket[<bucket_name>].settings!write': True, 'cluster.xdcr.settings!read': True, 'cluster.samples!read': True, 'cluster.bucket[<bucket_name>]!compact': True,
                              'cluster.admin.security!read': True, 'cluster.bucket[<bucket_name>].views!read': True, 'cluster.bucket[<bucket_name>].recovery!read': True, 'cluster.bucket[<bucket_name>].settings!read': True,
                              'cluster.xdcr.settings!write': True, 'cluster.bucket[<bucket_name>].xdcr!execute': True, 'cluster.settings!read': True,'cluster.settings!write': True,
                              'cluster.server_groups!write': True, 'cluster.xdcr.remote_clusters!write': True, 'cluster.bucket[<bucket_name>].data!write': True}
        }
        return per_set

    @staticmethod
    def _cluster_admin_role_master():
        per_set = {
            "name":"Master role for cluster admin",
            "permissionSet": "cluster.pools!read,cluster.pools!write,\
                    cluster.nodes!read,cluster.nodes!write,cluster.samples!read,cluster.settings!read,cluster.settings!write,cluster.tasks!read,\
                    cluster.stats!read,cluster.server_groups!read,cluster.server_groups!write,cluster.indexes!read,cluster.indexes!write,\
                    cluster.xdcr.settings!read,cluster.xdcr.settings!write,cluster.xdcr.remote_clusters!read,cluster.xdcr.remote_clusters!write,\
                    cluster.bucket[<bucket_name>]!create,cluster.bucket[<bucket_name>]!delete,cluster.bucket[<bucket_name>]!compact,\
                    cluster.bucket[<bucket_name>].settings!read,cluster.bucket[<bucket_name>].settings!write,cluster.bucket[<bucket_name>].password!read,\
                    cluster.bucket[<bucket_name>].data!read,cluster.bucket[<bucket_name>].data!write,cluster.bucket[<bucket_name>].recovery!read,\
                    cluster.bucket[<bucket_name>].recovery!write,cluster.bucket[<bucket_name>].views!read,cluster.bucket[<bucket_name>].views!write,\
                    cluster.bucket[<bucket_name>].xdcr!read,cluster.bucket[<bucket_name>].xdcr!write,cluster.bucket[<bucket_name>].xdcr!execute"
        }
        return per_set

    @staticmethod
    def _cluster_admin_role_expected():
        per_set = {
            "name":"Cluster Admin expected",
            "permissionSet": {'cluster.bucket[<bucket_name>].xdcr!write': True, 'cluster.bucket[<bucket_name>].xdcr!read': True, 'cluster.bucket[<bucket_name>]!delete': True,\
                              'cluster.pools!read': True, 'cluster.bucket[<bucket_name>].views!write': False, 'cluster.tasks!read': True, 'cluster.nodes!write': True,\
                              'cluster.server_groups!read': True, 'cluster.bucket[<bucket_name>]!create': True, 'cluster.bucket[<bucket_name>].recovery!write': True, \
                              'cluster.bucket[<bucket_name>].password!read': False, 'cluster.pools!write': True, 'cluster.indexes!write': True, 'cluster.indexes!read': True,\
                              'cluster.nodes!read': True, 'cluster.xdcr.remote_clusters!read': True, 'cluster.bucket[<bucket_name>].settings!write': True, \
                              'cluster.xdcr.settings!read': True, 'cluster.samples!read': True, 'cluster.bucket[<bucket_name>]!compact': True, 'cluster.bucket[<bucket_name>].views!read': False,\
                              'cluster.bucket[<bucket_name>].recovery!read': True, 'cluster.bucket[<bucket_name>].settings!read': True, 'cluster.xdcr.settings!write': True,\
                              'cluster.bucket[<bucket_name>].xdcr!execute': True, 'cluster.settings!read': True, 'cluster.settings!write': True, 'cluster.server_groups!write': True,\
                              'cluster.stats!read': True, 'cluster.xdcr.remote_clusters!write': True, 'cluster.bucket[<bucket_name>].data!write': False, \
                              'cluster.bucket[<bucket_name>].data!read': False}
            }
        return per_set

    @staticmethod
    def _cluster_admin_roadmin_role_expected():
        per_set = {
            "name": "Cluster Admin expected",
            "permissionSet": {'cluster.bucket[<bucket_name>].xdcr!write': True,
                              'cluster.bucket[<bucket_name>].xdcr!read': True,
                              'cluster.bucket[<bucket_name>]!delete': True, \
                              'cluster.pools!read': True, 'cluster.bucket[<bucket_name>].views!write': False,
                              'cluster.tasks!read': True, 'cluster.nodes!write': True, \
                              'cluster.server_groups!read': True, 'cluster.bucket[<bucket_name>]!create': True,
                              'cluster.bucket[<bucket_name>].recovery!write': True, \
                              'cluster.bucket[<bucket_name>].password!read': False, 'cluster.pools!write': True,
                              'cluster.indexes!write': True, 'cluster.indexes!read': True, \
                              'cluster.nodes!read': True, 'cluster.xdcr.remote_clusters!read': True,
                              'cluster.bucket[<bucket_name>].settings!write': True, \
                              'cluster.xdcr.settings!read': True, 'cluster.samples!read': True,
                              'cluster.bucket[<bucket_name>]!compact': True,
                              'cluster.bucket[<bucket_name>].views!read': True, \
                              'cluster.bucket[<bucket_name>].recovery!read': True,
                              'cluster.bucket[<bucket_name>].settings!read': True,
                              'cluster.xdcr.settings!write': True, \
                              'cluster.bucket[<bucket_name>].xdcr!execute': True, 'cluster.settings!read': True,
                              'cluster.settings!write': True, 'cluster.server_groups!write': True, \
                              'cluster.stats!read': True, 'cluster.xdcr.remote_clusters!write': True,
                              'cluster.bucket[<bucket_name>].data!write': False, \
                              'cluster.bucket[<bucket_name>].data!read': False}
        }
        return per_set

    @staticmethod
    def _cluster_view_admin_role_expected():
        per_set = {
            "name":"Cluster Admin expected",
            "permissionSet": {'cluster.bucket[<bucket_name>].xdcr!write': True, 'cluster.bucket[<bucket_name>].xdcr!read': True, 'cluster.bucket[<bucket_name>]!delete': True,\
                              'cluster.pools!read': True, 'cluster.bucket[<bucket_name>].views!write': True, 'cluster.tasks!read': True, 'cluster.nodes!write': True,\
                              'cluster.server_groups!read': True, 'cluster.bucket[<bucket_name>]!create': True, 'cluster.bucket[<bucket_name>].recovery!write': True, \
                              'cluster.bucket[<bucket_name>].password!read': False, 'cluster.pools!write': True, 'cluster.indexes!write': True, 'cluster.indexes!read': True,\
                              'cluster.nodes!read': False, 'cluster.xdcr.remote_clusters!read': True, 'cluster.bucket[<bucket_name>].settings!write': True, \
                              'cluster.xdcr.settings!read': True, 'cluster.samples!read': True, 'cluster.bucket[<bucket_name>]!compact': True, 'cluster.bucket[<bucket_name>].views!read': True,\
                              'cluster.bucket[<bucket_name>].recovery!read': True, 'cluster.bucket[<bucket_name>].settings!read': True, 'cluster.xdcr.settings!write': True,\
                              'cluster.bucket[<bucket_name>].xdcr!execute': True, 'cluster.settings!read': True, 'cluster.settings!write': True, 'cluster.server_groups!write': True,\
                              'cluster.stats!read': True, 'cluster.xdcr.remote_clusters!write': True, 'cluster.bucket[<bucket_name>].data!write': False, \
                              'cluster.bucket[<bucket_name>].data!read': True}
            }
        return per_set

    @staticmethod
    def _cluster_admin_not_allowed_perm_master():
        per_set = {
            "name":"Check for cluster Admin role not allowed for cluster admin",
            "permissionSet":"cluster.admin.internal!all,cluster.admin.setup!write,\
                            cluster.admin.security!read,cluster.admin.security!write,cluster.admin.logs!read"
        }
        return per_set

    @staticmethod
    def _cluster_admin_not_allowed_perm_expected():
        per_set = {
            "name":"Check for cluster Admin role not allowed for cluster admin expected",
            "permissionSet":{'cluster.admin.internal!all':False, 'cluster.admin.setup!write':True,\
                            'cluster.admin.security!read':False, 'cluster.admin.security!write':False, 'cluster.admin.logs!read':True}
        }
        return per_set

    @staticmethod
    def _bucket_admin_role_master():
        per_set = {
            "name":"Bucket Admin master permission list",
            "permissionSet":"cluster.pools!read, cluster.nodes!read,cluster.samples!read, cluster.settings!read,cluster.tasks!read,cluster.stats!read,cluster.server_groups!read,\
                    cluster.indexes!read,cluster.xdcr.settings!read,cluster.xdcr.remote_clusters!read,cluster.bucket[<bucket_name>]!create,cluster.bucket[<bucket_name>]!delete,cluster.bucket[<bucket_name>]!compact,\
                    cluster.bucket[<bucket_name>].settings!read,cluster.bucket[<bucket_name>].password!read,cluster.bucket[<bucket_name>].data!read,cluster.bucket[<bucket_name>].data!write,\
                    cluster.bucket[<bucket_name>].recovery!read,cluster.bucket[<bucket_name>].recovery!write,cluster.bucket[<bucket_name>].views!read,\
                    cluster.bucket[<bucket_name>].views!write,cluster.bucket[<bucket_name>].xdcr!execute"
        }
        return per_set

    @staticmethod
    def _bucket_admin_role_expected_incorrect_bucket():
        per_set = {
            "name":"Bucket Admin expected result for incorrect bucket",
            "permissionSet": {'cluster.bucket[<bucket_name>].recovery!read': True, 'cluster.bucket[<bucket_name>].password!read': False, 'cluster.bucket[<bucket_name>].data!read': False,\
                              'cluster.pools!read': True, 'cluster.bucket[<bucket_name>]!delete': False, 'cluster.bucket[<bucket_name>].xdcr!execute': False, 'cluster.tasks!read': True,\
                              'cluster.server_groups!read': True, 'cluster.bucket[<bucket_name>].recovery!write': False, 'cluster.indexes!read': True, 'cluster.nodes!read': True,\
                              'cluster.xdcr.remote_clusters!read': False, 'cluster.xdcr.settings!read': False, 'cluster.samples!read': True, 'cluster.bucket[<bucket_name>].views!read': False,\
                              'cluster.bucket[<bucket_name>].data!write': False, 'cluster.bucket[<bucket_name>]!compact': False, 'cluster.bucket[<bucket_name>]!create': False, 'cluster.settings!read': True,\
                              'cluster.stats!read': True, 'cluster.bucket[<bucket_name>].settings!read': True, 'cluster.bucket[<bucket_name>].views!write': False}
        }
        return  per_set

    @staticmethod
    def _bucket_admin_role_expected_correct_bucket():
        per_set = {
            "name":"Bucket Admin expected result for correct bucket",
            "permissionSet": {'cluster.bucket[<bucket_name>].recovery!read': True, 'cluster.bucket[<bucket_name>].password!read': False, 'cluster.bucket[<bucket_name>].data!read': False,\
                              'cluster.pools!read': True, 'cluster.bucket[<bucket_name>]!delete': True, 'cluster.bucket[<bucket_name>].xdcr!execute': True, 'cluster.tasks!read': True,\
                              'cluster.server_groups!read': True, 'cluster.bucket[<bucket_name>].recovery!write': True, 'cluster.indexes!read': True, 'cluster.nodes!read': True,\
                              'cluster.xdcr.remote_clusters!read': False, 'cluster.xdcr.settings!read': False, 'cluster.samples!read': True, 'cluster.bucket[<bucket_name>].views!read': False,\
                              'cluster.bucket[<bucket_name>].data!write': False, 'cluster.bucket[<bucket_name>]!compact': True, 'cluster.settings!read': True,\
                              'cluster.stats!read': True, 'cluster.bucket[<bucket_name>].settings!read': True, 'cluster.bucket[<bucket_name>].views!write': False}
        }
        return  per_set

    @staticmethod
    def _bucket_view_admin_role_expected_correct_bucket():
        per_set = {
            "name": "Bucket Admin expected result for correct bucket",
            "permissionSet": {'cluster.bucket[<bucket_name>].recovery!read': True,
                              'cluster.bucket[<bucket_name>].password!read': False,
                              'cluster.bucket[<bucket_name>].data!read': True, \
                              'cluster.pools!read': True, 'cluster.bucket[<bucket_name>]!delete': True,
                              'cluster.bucket[<bucket_name>].xdcr!execute': True, 'cluster.tasks!read': True, \
                              'cluster.server_groups!read': True, 'cluster.bucket[<bucket_name>].recovery!write': True,
                              'cluster.indexes!read': True, 'cluster.nodes!read': True, \
                              'cluster.xdcr.remote_clusters!read': False, 'cluster.xdcr.settings!read': False,
                              'cluster.samples!read': True, 'cluster.bucket[<bucket_name>].views!read': True, \
                              'cluster.bucket[<bucket_name>].data!write': False,
                              'cluster.bucket[<bucket_name>]!compact': True, 'cluster.settings!read': True, \
                              'cluster.stats!read': True, 'cluster.bucket[<bucket_name>].settings!read': True,
                              'cluster.bucket[<bucket_name>].views!write': True}
        }
        return per_set

    @staticmethod
    def _bucket_admin_role_not_allowed_perm_master():
        per_set = {
            "name":"List of permission not allowed for bucket admin",
            "permissionSet":"cluster.admin.internal!all,cluster.admin.diag!read,cluster.admin.diag!write,cluster.admin.setup!write,\
                            cluster.admin.security!read,cluster.admin.security!write,cluster.admin.logs!read,cluster.pools!write,\
                            cluster.nodes!write,cluster.settings!write,cluster.server_groups!write,cluster.indexes!write"
        }
        return per_set

    @staticmethod
    def _bucket_admin_role_not_allowed_perm_expected():
        per_set = {
            "name":"List of permission not allowed for bucket admin",
            "permissionSet": {'cluster.admin.internal!all':False,'cluster.admin.diag!read':False,'cluster.admin.diag!write':False,'cluster.admin.setup!write':False,\
                            'cluster.admin.security!read':False,'cluster.admin.security!write':False,'cluster.admin.logs!read':False,'cluster.pools!write':False,\
                            'cluster.nodes!write':False,'cluster.settings!write':False,'cluster.server_groups!write':False,'cluster.indexes!write':False}
        }
        return per_set

    @staticmethod
    def _view_admin_role_master():
        per_set = {
            "name":"View Admin role master",
            "permissionSet":"cluster.bucket[<bucket_name>].views!read,cluster.bucket[<bucket_name>].views!write,cluster.bucket[<bucket_name>].data!read, cluster.bucket[<bucket_name>].settings!read,\
                            cluster.pools!read, cluster.nodes!read,cluster.samples!read, cluster.settings!read,cluster.tasks!read,cluster.stats!read,cluster.server_groups!read,\
                            cluster.indexes!read,cluster.xdcr.settings!read,cluster.xdcr.remote_clusters!read,cluster.bucket[<bucket_name>].settings!read"
        }
        return per_set

    @staticmethod
    def _view_admin_role_expected():
        per_set = {
            "name":"View Admin expected result",
            "permissionSet": {'cluster.indexes!read': True, 'cluster.bucket[<bucket_name>].views!write': True, 'cluster.bucket[<bucket_name>].settings!read': True, \
                              'cluster.settings!read': True, 'cluster.samples!read': True, 'cluster.xdcr.settings!read': False, \
                              'cluster.bucket[<bucket_name>].views!read': True, 'cluster.stats!read': True, 'cluster.tasks!read': True, \
                              'cluster.nodes!read': True, 'cluster.xdcr.remote_clusters!read': False, 'cluster.server_groups!read': True, \
                              'cluster.pools!read': True, 'cluster.bucket[<bucket_name>].data!read': True}
        }
        return per_set

    @staticmethod
    def _view_admin_role_not_allowed_perm_master():
        per_set = {
            "name":"View Admin permission not allowed",
            "permissionSet":"cluster.admin.internal!all,cluster.admin.diag!read,cluster.admin.diag!write,cluster.admin.setup!write,\
                            cluster.admin.security!read,cluster.admin.security!write,cluster.admin.logs!read,cluster.pools!write,\
                            cluster.nodes!write,cluster.settings!write,cluster.server_groups!write,cluster.indexes!write,\
                            cluster.bucket[<bucket_name>].xdcr!read,cluster.bucket[<bucket_name>].xdcr!write,cluster.bucket[<bucket_name>].xdcr!execute,\
                            cluster.bucket[<bucket_name>]!create,cluster.bucket[<bucket_name>]!delete,cluster.bucket[<bucket_name>]!compact,\
                            cluster.bucket[<bucket_name>].settings!read,cluster.bucket[<bucket_name>].settings!write,cluster.bucket[<bucket_name>].password!read,\
                            cluster.bucket[<bucket_name>].data!write,cluster.bucket[<bucket_name>].recovery!read,cluster.bucket[<bucket_name>].recovery!write,\
                            cluster.xdcr.settings!read,cluster.xdcr.settings!write,cluster.xdcr.remote_clusters!read,cluster.xdcr.remote_clusters!write"
        }
        return per_set

    @staticmethod
    def _view_admin_role_not_allowed_perm_expected():
        per_set = {
            "name":"View Admin permission not allowed",
            "permissionSet":{'cluster.admin.internal!all':False,'cluster.admin.diag!read':False,'cluster.admin.diag!write':False,'cluster.admin.setup!write':False,\
                            'cluster.admin.security!read':False,'cluster.admin.security!write':False,'cluster.admin.logs!read':False,'cluster.pools!write':False,\
                            'cluster.nodes!write':False,'cluster.settings!write':False,'cluster.server_groups!write':False,'cluster.indexes!write':False,\
                            'cluster.bucket[<bucket_name>].xdcr!read':False,'cluster.bucket[<bucket_name>].xdcr!write':False,'cluster.bucket[<bucket_name>].xdcr!execute':False,\
                            'cluster.bucket[<bucket_name>]!create':False,'cluster.bucket[<bucket_name>]!delete':False,'cluster.bucket[<bucket_name>]!compact':False,\
                            'cluster.bucket[<bucket_name>].settings!read':True,'cluster.bucket[<bucket_name>].settings!write':False,'cluster.bucket[<bucket_name>].password!read':False,\
                            'cluster.bucket[<bucket_name>].data!write':False,'cluster.bucket[<bucket_name>].recovery!read':False,'cluster.bucket[<bucket_name>].recovery!write':False,\
                            'cluster.xdcr.settings!read':False,'cluster.xdcr.settings!write':False,'cluster.xdcr.remote_clusters!read':False,'cluster.xdcr.remote_clusters!write':False}
        }
        return per_set

    @staticmethod
    def _replication_admin_role_master():
        per_set = {
            "name":"XDCR Admin expected result",
            "permissionSet":"cluster.bucket[<bucket_name>].xdcr!read,cluster.bucket[<bucket_name>].xdcr!write,cluster.bucket[<bucket_name>].xdcr!execute,\
                    cluster.xdcr.settings!read,cluster.xdcr.settings!write,cluster.xdcr.remote_clusters!read,cluster.xdcr.remote_clusters!write,\
                cluster.pools!read, cluster.nodes!read,cluster.samples!read, cluster.settings!read,cluster.tasks!read,cluster.stats!read,cluster.server_groups!read,\
                cluster.admin.security!read,cluster.bucket[<bucket_name>].recovery!read,cluster.indexes!read,cluster.xdcr.settings!read,cluster.xdcr.remote_clusters!read"
        }
        return per_set

    @staticmethod
    def _replication_admin_role_expected():
        per_set = {
            "name":"Replication Admin role expected",
            "permissionSet":{'cluster.settings!read': True, 'cluster.nodes!read': True, 'cluster.bucket[<bucket_name>].xdcr!read': True, \
                             'cluster.xdcr.settings!read': True, 'cluster.samples!read': True, 'cluster.pools!read': True, 'cluster.stats!read': True, \
                             'cluster.xdcr.remote_clusters!write': True, 'cluster.bucket[<bucket_name>].xdcr!write': True, 'cluster.xdcr.remote_clusters!read': True, \
                             'cluster.xdcr.settings!write': True, 'cluster.server_groups!read': True,'cluster.admin.security!read':False,'cluster.bucket[<bucket_name>].recovery!read':False, \
                             'cluster.bucket[<bucket_name>].xdcr!execute': True, 'cluster.indexes!read': True, 'cluster.tasks!read': True}
        }
        return per_set

    @staticmethod
    def _replication_admin_not_allowed_perm_master():
        per_set = {
            "name":"Replication admin not allowed permission master",
            "permissionSet":"cluster.admin.internal!all,cluster.admin.diag!read,cluster.admin.diag!write,cluster.admin.setup!write,\
                            cluster.admin.security!write,cluster.admin.logs!read,cluster.pools!write,\
                            cluster.nodes!write,cluster.settings!write,cluster.server_groups!write,cluster.indexes!write,\
                            cluster.bucket[<bucket_name>].xdcr!read,cluster.bucket[<bucket_name>].xdcr!write,cluster.bucket[<bucket_name>].xdcr!execute,\
                            cluster.bucket[<bucket_name>]!create,cluster.bucket[<bucket_name>]!delete,cluster.bucket[<bucket_name>]!compact,\
                            cluster.bucket[<bucket_name>].settings!read,cluster.bucket[<bucket_name>].settings!write,cluster.bucket[<bucket_name>].password!read,\
                            cluster.bucket[<bucket_name>].data!write,cluster.bucket[<bucket_name>].recovery!write"
        }
        return per_set

    @staticmethod
    def _replication_admin_not_allowed_perm_expected():
        per_set = {
            "name":"Replication admin not allowed permission master",
            "permissionSet":{'cluster.admin.internal!all':False,'cluster.admin.diag!read':False,'cluster.admin.diag!write':False,'cluster.admin.setup!write':False,\
                            'cluster.admin.security!write':False,'cluster.admin.logs!read':False,'cluster.pools!write':False,\
                            'cluster.nodes!write':False,'cluster.settings!write':False,'cluster.server_groups!write':False,'cluster.indexes!write':False,\
                            'cluster.bucket[<bucket_name>].xdcr!read':True,'cluster.bucket[<bucket_name>].xdcr!write':True,'cluster.bucket[<bucket_name>].xdcr!execute':True,\
                            'cluster.bucket[<bucket_name>]!create':False,'cluster.bucket[<bucket_name>]!delete':False,'cluster.bucket[<bucket_name>]!compact':False,\
                            'cluster.bucket[<bucket_name>].settings!read':True,'cluster.bucket[<bucket_name>].settings!write':False,'cluster.bucket[<bucket_name>].password!read':False,\
                            'cluster.bucket[<bucket_name>].data!write':False,'cluster.bucket[<bucket_name>].recovery!write':False}
        }
        return  per_set

    @staticmethod
    def _read_only_role_master():
        per_set = {
            "name":"Permission for read only user master",
            "permissionSet":"cluster.admin.security!read,cluster.pools!read,cluster.nodes!read,cluster.samples!read,\
                            cluster.settings!read,cluster.tasks!read,cluster.stats!read,cluster.server_groups!read,\
                            cluster.indexes!read,cluster.xdcr.settings!read,cluster.xdcr.remote_clusters!read, \
                            cluster.bucket[<bucket_name>].settings!read,cluster.bucket[<bucket_name>].password!read,\
                            cluster.bucket[<bucket_name>].recovery!read,cluster.bucket[<bucket_name>].views!read,\
                            cluster.bucket[<bucket_name>].xdcr!read"
        }
        return per_set

    @staticmethod
    def _read_only_role_expected():
        per_set = {
            "name":"Permission for read only user",
            "permissionSet":{'cluster.admin.security!read':True,'cluster.pools!read':True,'cluster.nodes!read':True,'cluster.samples!read':True,\
                            'cluster.settings!read':True,'cluster.tasks!read':True,'cluster.stats!read':True,'cluster.server_groups!read':True,\
                            'cluster.indexes!read':True,'cluster.xdcr.settings!read':True,'cluster.xdcr.remote_clusters!read':True, \
                            'cluster.bucket[<bucket_name>].settings!read':True,'cluster.bucket[<bucket_name>].password!read':False,\
                            'cluster.bucket[<bucket_name>].recovery!read':True,'cluster.bucket[<bucket_name>].views!read':True,\
                            'cluster.bucket[<bucket_name>].xdcr!read':True}
        }
        return per_set

    @staticmethod
    def _read_only_role_not_allowed_master():
        per_set = {
            "name":"Permission for read only user master",
            "permissionSet":"cluster.admin.internal!all,cluster.admin.diag!write,cluster.admin.setup!write,cluster.admin.security!write,\
                    cluster.admin.logs!read,cluster.pools!write,cluster.nodes!write,cluster.settings!write,cluster.server_groups!write,\
                    cluster.indexes!write,cluster.xdcr.settings!write,cluster.xdcr.remote_clusters!write,cluster.bucket[<bucket_name>]!create,\
                    cluster.bucket[<bucket_name>]!delete,cluster.bucket[<bucket_name>]!compact,cluster.bucket[<bucket_name>].settings!write,\
                    cluster.bucket[<bucket_name>].password!read,cluster.bucket[<bucket_name>].data!read,cluster.bucket[<bucket_name>].data!write,\
                    cluster.bucket[<bucket_name>].recovery!read,cluster.bucket[<bucket_name>].recovery!write,cluster.bucket[<bucket_name>].views!write,\
                    cluster.bucket[<bucket_name>].xdcr!read,cluster.bucket[<bucket_name>].xdcr!execute"
        }
        return per_set

    @staticmethod
    def _read_only_role_not_allowed_expected():
        per_set = {
            "name":"Permission for read only user master",
            "permissionSet":{'cluster.admin.internal!all':False,'cluster.admin.diag!write':False,'cluster.admin.setup!write':False,'cluster.admin.security!write':False,\
                    'cluster.admin.logs!read':False,'cluster.pools!write':False,'cluster.nodes!write':False,'cluster.settings!write':False,'cluster.server_groups!write':False,\
                    'cluster.indexes!write':False,'cluster.xdcr.settings!write':False,'cluster.xdcr.remote_clusters!write':False,'cluster.bucket[<bucket_name>]!create':False,\
                    'cluster.bucket[<bucket_name>]!delete':False,'cluster.bucket[<bucket_name>]!compact':False,'cluster.bucket[<bucket_name>].settings!write':False,\
                    'cluster.bucket[<bucket_name>].password!read':False,'cluster.bucket[<bucket_name>].data!read':False,'cluster.bucket[<bucket_name>].data!write':False,\
                    'cluster.bucket[<bucket_name>].recovery!read':True,'cluster.bucket[<bucket_name>].recovery!write':False,'cluster.bucket[<bucket_name>].views!write':False,\
                    'cluster.bucket[<bucket_name>].xdcr!read':True,'cluster.bucket[<bucket_name>].xdcr!execute':False}
        }
        return per_set

    @staticmethod
    def _replication_view_admin_role_master():
        per_set = {
            "name":"XDCR Admin expected result",
            "permissionSet":"cluster.bucket[<bucket_name>].xdcr!read,cluster.bucket[<bucket_name>].xdcr!write,cluster.bucket[<bucket_name>].xdcr!execute,\
                    cluster.xdcr.settings!read,cluster.xdcr.settings!write,cluster.xdcr.remote_clusters!read,cluster.xdcr.remote_clusters!write,\
                    cluster.pools!read, cluster.nodes!read,cluster.samples!read, cluster.settings!read,cluster.tasks!read,cluster.stats!read,cluster.server_groups!read,\
                    cluster.indexes!read,cluster.xdcr.settings!read,cluster.xdcr.remote_clusters!read,cluster.bucket[<bucket_name>].views!read,\
                    cluster.bucket[<bucket_name>].views!write,cluster.bucket[<bucket_name>].data!read"
        }
        return per_set

    @staticmethod
    def _replication_view_admin_role_expected():
        per_set = {
            "name":"Replication Admin role expected",
            "permissionSet":{'cluster.settings!read': True, 'cluster.nodes!read': True, 'cluster.bucket[<bucket_name>].xdcr!read': True, \
                             'cluster.xdcr.settings!read': True, 'cluster.samples!read': True, 'cluster.pools!read': True, 'cluster.stats!read': True, \
                             'cluster.xdcr.remote_clusters!write': True, 'cluster.bucket[<bucket_name>].xdcr!write': True, 'cluster.xdcr.remote_clusters!read': True, \
                             'cluster.xdcr.settings!write': True, 'cluster.server_groups!read': True, \
                             'cluster.bucket[<bucket_name>].xdcr!execute': True, 'cluster.indexes!read': True, 'cluster.tasks!read': True,'cluster.bucket[<bucket_name>].views!write': True,\
                             'cluster.bucket[<bucket_name>].views!read':True}
        }
        return per_set

    @staticmethod
    def _read_only_replication_role_master():
        per_set = {
            "name":"Permission for read only user master",
            "permissionSet":"cluster.admin.security!read,cluster.pools!read,cluster.nodes!read,cluster.samples!read,\
                            cluster.settings!read,cluster.tasks!read,cluster.stats!read,cluster.server_groups!read,\
                            cluster.indexes!read,cluster.xdcr.settings!read,cluster.xdcr.remote_clusters!read, \
                            cluster.bucket[<bucket_name>].settings!read,cluster.bucket[<bucket_name>].password!read,\
                            cluster.bucket[<bucket_name>].recovery!read,cluster.bucket[<bucket_name>].views!read,\
                            cluster.bucket[<bucket_name>].xdcr!read,cluster.bucket[<bucket_name>].xdcr!write"
        }
        return per_set

    @staticmethod
    def _read_only_replication_role_expected():
        per_set = {
            "name":"Permission for read only user",
            "permissionSet":{'cluster.admin.security!read':True,'cluster.pools!read':True,'cluster.nodes!read':True,'cluster.samples!read':True,\
                            'cluster.settings!read':True,'cluster.tasks!read':True,'cluster.stats!read':True,'cluster.server_groups!read':True,\
                            'cluster.indexes!read':True,'cluster.xdcr.settings!read':True,'cluster.xdcr.remote_clusters!read':True, \
                            'cluster.bucket[<bucket_name>].settings!read':True,'cluster.bucket[<bucket_name>].password!read':True,\
                            'cluster.bucket[<bucket_name>].recovery!read':True,'cluster.bucket[<bucket_name>].views!read':True,\
                            'cluster.bucket[<bucket_name>].xdcr!read':True,'cluster.bucket[<bucket_name>].xdcr!execute': True,\
                            'cluster.xdcr.settings!write': True}
        }
        return per_set


    @staticmethod
    def _bucket_admin_view_replication_admin_role_master():
        per_set = {
            "name":"Bucket Admin master permission list",
            "permissionSet":"cluster.pools!read, cluster.nodes!read,cluster.samples!read, cluster.settings!read,cluster.tasks!read,cluster.stats!read,cluster.server_groups!read,\
                    cluster.indexes!read,cluster.xdcr.settings!read,cluster.xdcr.remote_clusters!read,cluster.bucket[<bucket_name>]!create,cluster.bucket[<bucket_name>]!delete,cluster.bucket[<bucket_name>]!compact,\
                    cluster.bucket[<bucket_name>].settings!read,cluster.bucket[<bucket_name>].password!read,cluster.bucket[<bucket_name>].data!read,cluster.bucket[<bucket_name>].data!write,\
                    cluster.bucket[<bucket_name>].recovery!read,cluster.bucket[<bucket_name>].recovery!write,cluster.bucket[<bucket_name>].views!read,\
                    cluster.bucket[<bucket_name>].views!write,cluster.bucket[<bucket_name>].xdcr!execute"
        }
        return per_set

    @staticmethod
    def _bucket_admin_view_replication_admin_role_master_expected():
        per_set = {
            "name":"Bucket Admin expected result for correct bucket",
            "permissionSet": {'cluster.bucket[<bucket_name>].recovery!read': True, 'cluster.bucket[<bucket_name>].password!read': False, 'cluster.bucket[<bucket_name>].data!read': True,\
                              'cluster.pools!read': True, 'cluster.bucket[<bucket_name>]!delete': True, 'cluster.bucket[<bucket_name>].xdcr!execute': True, 'cluster.tasks!read': True,\
                              'cluster.server_groups!read': True, 'cluster.bucket[<bucket_name>].recovery!write': True, 'cluster.indexes!read': True, 'cluster.nodes!read': True,\
                              'cluster.xdcr.remote_clusters!read': True, 'cluster.xdcr.settings!read': True, 'cluster.samples!read': True, 'cluster.bucket[<bucket_name>].views!read': True,\
                              'cluster.bucket[<bucket_name>].data!write': False, 'cluster.bucket[<bucket_name>]!compact': True, 'cluster.bucket[<bucket_name>]!create': True, 'cluster.settings!read': True,\
                              'cluster.stats!read': True, 'cluster.bucket[<bucket_name>].settings!read': True, 'cluster.bucket[<bucket_name>].views!write': True}
        }
        return  per_set

    @staticmethod
    def _return_permission_set(role=None):
        return_role_master = []
        return_role_expected = []
        return_role_expected_negative = []

        if role == "admin":
            return_role_master = rbacRoles._admin_role_master()
            return_role_expected = rbacRoles._admin_role_expected()

        if role == "roadmin":
            return_role_master = rbacRoles._read_only_role_master()
            return_role_expected = rbacRoles._read_only_role_expected()

        if 'cluster_admin' in role:
            return_role_master = rbacRoles._cluster_admin_role_master()
            return_role_expected = rbacRoles._cluster_admin_role_expected()

        if 'cluster_ro_admin' in role:
            return_role_master = rbacRoles._cluster_admin_role_master()
            return_role_expected = rbacRoles._cluster_admin_roadmin_role_expected()

        if 'cluster_view_admin' in role:
            print("Into cluster view admin")
            return_role_master = rbacRoles._cluster_admin_role_master()
            return_role_expected = rbacRoles._cluster_view_admin_role_expected()

        if 'bucket_admin' in role:
            return_role_master = rbacRoles._bucket_admin_role_master()
            return_role_expected = rbacRoles._bucket_admin_role_expected_correct_bucket()
            return_role_expected_negative = rbacRoles._bucket_admin_role_expected_incorrect_bucket()

        if 'bucket_view_admin' in role:
            return_role_master = rbacRoles._bucket_admin_role_master()
            return_role_expected = rbacRoles._bucket_view_admin_role_expected_correct_bucket()

        if 'view_admin' in role:
            return_role_master = rbacRoles._view_admin_role_master()
            return_role_expected = rbacRoles._view_admin_role_expected()

        if 'replication_admin' in role:
            return_role_master = rbacRoles._replication_admin_role_master()
            return_role_expected = rbacRoles._replication_admin_role_expected()

        if  'roadmin_no_access' in role:
            return_role_master = rbacRoles._read_only_role_not_allowed_master()
            return_role_expected = rbacRoles._read_only_role_not_allowed_expected()

        if  'cluster_admin_no_access' in role:
            return_role_master = rbacRoles._cluster_admin_not_allowed_perm_master()
            return_role_expected = rbacRoles._cluster_admin_not_allowed_perm_expected()

        if 'bucket_admin_no_access' in role:
            return_role_master = rbacRoles._bucket_admin_role_not_allowed_perm_master()
            return_role_expected = rbacRoles._bucket_admin_role_not_allowed_perm_expected()

        if  'view_admin_no_access' in role:
            return_role_master = rbacRoles._view_admin_role_not_allowed_perm_master()
            return_role_expected = rbacRoles._view_admin_role_not_allowed_perm_expected()

        if 'replication_admin_no_access' in role:
            return_role_master = rbacRoles._replication_admin_not_allowed_perm_master()
            return_role_expected = rbacRoles._replication_admin_not_allowed_perm_expected()

        if 'view_replication_admin' in role:
            return_role_master = rbacRoles._replication_view_admin_role_master()
            return_role_expected = rbacRoles._replication_view_admin_role_expected()

        if 'replication_ro_admin' in role:
            return_role_master = rbacRoles._read_only_replication_role_master()
            return_role_expected = rbacRoles._read_only_role_expected()

        if  'bucket_view_replication_admin' in role:
            return_role_master = rbacRoles._bucket_admin_view_replication_admin_role_master()
            return_role_expected = rbacRoles._bucket_admin_view_replication_admin_role_master_expected()


        return return_role_master, return_role_expected, return_role_expected_negative