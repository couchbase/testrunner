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

    ssl_memcached_port = 11207
    cr_memcached_port = 12000

    # map of {non-ssl,ssl} ports
    ssl_port_map = {str(port): str(ssl_port),
                    str(fts_port): str(ssl_fts_port),
                    str(n1ql_port): str(ssl_n1ql_port),
                    str(index_port): str(ssl_index_port),
                    str(eventing_port): str(ssl_eventing_port),
                    str(backup_port): str(ssl_backup_port),
                    str(cbas_port): str(ssl_cbas_port),
                    str(memcached_port): str(ssl_memcached_port),
                    str(capi_port): str(ssl_capi_port),
                    # cluster run ports
                    "9000": "19000",
                    "9001": "19001",
                    "9002": "19002",
                    "9003": "19003"}
    use_https = False
    n2n_encryption = False
    multiple_ca = False
    x509 = None
    use_client_certs = False
    cacert_verify = False

    default_scope = "_default"
    default_collection = "_default"

    system_scope = "_system"

    query_collection = "_query"

    total_vbuckets = 1024

    # Name length limits
    max_bucket_name_len = 100
    max_scope_name_len = 251
    max_collection_name_len = 251

    # Count excluding the default scope/collection
    max_scopes = 1200
    max_collections = 1200

    # Max supported system_event_logs
    sys_event_min_logs = 3000
    sys_event_max_logs = 20000
    sys_event_def_logs = 10000
    # Size in bytes
    sys_event_log_max_size = 3072
    # Time within which the UUID cannot be duplicated in server (in seconds)
    sys_event_log_uuid_uniqueness_time = 60

    # Encryption at rest default settings
    encryption_at_rest_dek_rotation_interval = 2592000
    secret_rotation_interval_in_seconds = 2592000
    encryption_at_rest_dek_lifetime_interval = 31536000

    capella_run = False
    cluster_profile = "provisioned"
    rest_username = "cbadminbucket"
    rest_password = "password"

    capella_credentials = None
    capella_cluster_id = None
