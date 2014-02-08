{
    "name" : "upgrade_setup",
    "desc" : "upgrade setup",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "Source Cluster",
                    "desc" :  "Course Cluster Set up",
                    "cluster" : {"add" : "3"}},
                "1" :
                {
                    "name" : "create_local_buckets",
                    "desc" :  "create_local_buckets",
                    "buckets" : {"default" : {"quota": "956", "replicas": "1",
                                              "replica_index": "1"},
                                 "sasl": {"count": "3", "quota": "728", "replicas": "1",
                                          "replica_index": "1"}
                                }}
                "2" :
                 {
                    "name" : "pair_sites_for_standardbuckets",
                    "desc" :  "set_replication_type_for_standardbuckets",
                    "xdcr" : {
                              "dest_cluster_name" : "source",
                              "dest_cluster_rest_username" : "Administrator",
                              "dest_cluster_rest_pwd" : "password",
                              "replication_type" : "unidirection",
                              "buckets" : ["default","standardbucket"]
                             }
                 }
       }
}
