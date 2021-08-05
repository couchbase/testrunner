class CbServer(object):
    port = 8091
    capi_port = 8092
    fts_port = 8094
    n1ql_port = 8093
    index_port = 9102
    eventing_port = 8096
    backup_port = 8097
    cbas_port = 8095

    ssl_port = 18091
    ssl_capi_port = 18092
    ssl_fts_port = 18094
    ssl_n1ql_port = 18093
    ssl_index_port = 19102
    ssl_eventing_port = 18096
    ssl_backup_port = 18097
    ssl_cbas_port = 18095

    memcached_port = 11210
    moxi_port = 11211

    ssl_memcached_port = 11207

    # map of {non-ssl,ssl} ports
    ssl_port_map = {str(port): str(ssl_port),
                    str(fts_port): str(ssl_fts_port),
                    str(n1ql_port): str(ssl_n1ql_port),
                    str(index_port): str(ssl_index_port),
                    str(eventing_port): str(ssl_eventing_port),
                    str(backup_port): str(ssl_backup_port),
                    str(cbas_port): str(ssl_cbas_port),
                    str(memcached_port): str(ssl_memcached_port),
                    str(capi_port): str(ssl_capi_port)}
    use_https = False

    default_scope = "_default"
    default_collection = "_default"

    total_vbuckets = 1024

    # Name length limits
    max_bucket_name_len = 100
    max_scope_name_len = 251
    max_collection_name_len = 251

    # Count excluding the default scope/collection
    max_scopes = 1200
    max_collections = 1200
