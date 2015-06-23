{
    "name" : "kv_xdcr_tpcc__dest",
    "desc" : "kv_xdcr_tpcc_dest",
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
                    "name" : "create_buckets_indexes",
                    "desc" :  "create_buckets_indexes",
                    "ssh"  : {"hosts"    : ["127.0.0.1"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "screen -dmS tpcc sh -c \"python tests/kv_xdcr_tpcc/create_buckets_indexes.py; exec bash;\""},
                    "runtime" : 60
                },
                "2" :
                {
                    "name" : "load",
                    "desc" :  "load phase",
                    "workload" : [{"spec" : "b:ITEM,t:template512,s:100,e:20,ttl:86400,ccq:std1ph5keys,ops:60000",
                                  "conditions" : "post:count = 10000000"},
                                  {"spec" : "b:DISTRICT,t:template512,s:100,e:20,ttl:86400,ccq:std1ph5keys,ops:60000",
                                  "conditions" : "post:count = 5000000"},
                                  {"spec" : "b:HISTORY,t:template512,s:100,e:20,ttl:86400,ccq:std1ph5keys,ops:60000",
                                  "conditions" : "post:count = 5000000"},
                                  {"spec" : "b:STOCK,t:template512,s:100,e:20,ttl:86400,ccq:std1ph5keys,ops:60000",
                                  "conditions" : "post:count = 5000000"},
                                  {"spec" : "b:ORDERS,t:template512,s:100,e:20,ttl:86400,ccq:std1ph5keys,ops:60000",
                                  "conditions" : "post:count = 5000000"},
                                  {"spec" : "b:NEW_ORDER,t:template512,s:100,e:20,ttl:86400,ccq:std1ph5keys,ops:60000",
                                  "conditions" : "post:count = 5000000"},
                                  {"spec" : "b:WAREHOUSE,t:template512,s:100,e:20,ttl:86400,ccq:std1ph5keys,ops:60000",
                                  "conditions" : "post:count = 5000000"}]
                },
                "3" :
                {
                    "name" : "pair_sites_for ITEM",
                    "desc" :  "set_replication_type_for_ITEM",
                    "xdcr" : {
                              "dest_cluster_name" : "source",
                              "dest_cluster_rest_username" : "Administrator",
                              "dest_cluster_rest_pwd" : "password",
                              "replication_type" : "unidirection",
                              "buckets" : ["ITEM"]
                             }
                }
                "4" :
                {
                    "name" : "access_phase",
                    "desc" :  "post_upgrade_access",
                    "workload" : ["b:ITEM,coq:defaultkeys,ccq:std1ph5keys,d:3,g:97,ops:10000",
                                  "b:DISTRICT,coq:defaultkeys,ccq:std2sph5keys,g:100,ops:10000",
                                  "b:STOCK,ccq:saslph5keys,g:100,ops:10000"],
                    "runtime" : 20800
                }
               }
}
