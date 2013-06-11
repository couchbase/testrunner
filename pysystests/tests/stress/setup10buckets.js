{
    "name" : "online_offline_upgrade_setup",
    "desc" : "spec for online offline upgrade",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "rebalance_in",
                    "desc" :  "rebalance_in",
                    "cluster" : {"add" : "4"}},
                "1" :
                {
                    "name" : "create_local_buckets",
                    "desc" :  "create_local_buckets",
                    "buckets" : {"default" : {"quota": "512", "replicas": "1",
                                              "replica_index": "1"},
                                 "sasl": {"count": "8", "quota": "512", "replicas": "1",
                                          "replica_index": "1"}
                                }},

                "3" :
                {
                    "name" : "pair_sites",
                    "desc" :  "pair_sites",
                    "xdcr" : {
                              "dest_cluster_name" : "remote1",
                              "dest_cluster_rest_username" : "Administrator",
                              "dest_cluster_rest_pwd" : "password",
                              "replication_type" : "unidirection",
                              "buckets" : ["default"]
		     }
		}
       }
}


