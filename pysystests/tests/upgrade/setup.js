{
    "name" : "online_offline_upgrade_setup",
    "desc" : "spec for online offline upgrade",
    "loop" : false,
    "phases" : {
                "2" :
                {
                    "name" : "rebalance_in",
                    "desc" :  "rebalance_in",
                    "cluster" : {"add" : "4"}},
                "0" :
                {
                    "name" : "create_local_buckets",
                    "desc" :  "create_local_buckets",
                    "buckets" : {"default" : {"quota": "956", "replicas": "1",
                                              "replica_index": "1"},
                                 "sasl": {"count": "3", "quota": "728", "replicas": "1",
                                          "replica_index": "1"}
                                }}
       }
}


