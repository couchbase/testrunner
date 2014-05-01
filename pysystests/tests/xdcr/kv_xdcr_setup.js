{
    "name" : "kv_xdcr_setup",
    "desc" : "kv_xdcr_setup",
    "loop" : "",
    "phases" : {
                "0" :
                {
                    "name" : "rebalance_in",
                    "desc" :  "rebalance_in",
                    "cluster" : {"add" : "7"}
                },
                "1" :
                {
                    "name" : "create_local_buckets",
                    "desc" :  "create_local_buckets",
                    "buckets" : {"standard" : {"count": "2", "quota": "5000", "replicas": "1",
                                              "replica_index": "1"},
                                 "sasl": {"count": "1",  "quota": "1000", "replicas": "1",
                                          "replica_index": "0"}
                                },
                     "runtime" : 120
                 },
                 "2" :
                 {
                    "name" : "test_load",
                    "desc" :  "test_load",
                    "workload" : [{"spec" : "b:standardbucket,t:template512,s:100,e:20,ttl:86400,ccq:std1ph5keys,ops:60000",
                                  "conditions" : "post:count = 100"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph5keys,ops:60000",
                                   "conditions" :  "post:count = 100"}]
                 }
               }
}