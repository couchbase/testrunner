{
  "name" : "ent_backup_setup",
  "desc" : "Enterprise Backup setup",
  "loop" : false,
  "phases" :{
                "0" :
                {
                    "name" : "rebalance_in",
                    "desc" :  "rebalance_in",
                    "cluster" : {"add" : "4"}
                },
                "1" :
                {
                    "name" : "create_local_buckets",
                    "desc" :  "create_local_buckets",
                    "buckets" : {"default" : {"quota": "256", "replicas": "1",
                                              "replica_index": "1"},
                                 "standard": {"count": "5", "quota": "256", "replicas": "1",
                                          "replica_index": "0"}
                                }
                 }
               }
}