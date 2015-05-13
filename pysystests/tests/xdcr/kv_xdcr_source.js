{
    "name" : "kv_xdcr_linux_source",
    "desc" : "kv_xdcr_linux_source",
    "loop" : "",
    "phases" : {
                "0" :
                 {
                    "name" : "load_dgm",
                    "desc" :  "load_hotset",
                    "workload" : [{"spec" : "b:standardbucket,t:template512,s:100,e:20,ttl:86400,ccq:std1ph5keys,ops:60000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 20"},
                                  {"spec" : "b:standardbucket1,e:30,s:100,ttl:86400,ccq:std2ph5keys,ops:40000",
                                  "conditions" : "post:count = 20000000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph5keys,ops:60000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 50"}]
                 },
                 "1" :
                 {
                    "name" : "pair_sites_for_standardbuckets",
                    "desc" : "set_replication_type_for_standardbuckets",
                    "xdcr" : {
                              "dest_cluster_name" : "remote1",
                              "dest_cluster_rest_username" : "Administrator",
                              "dest_cluster_rest_pwd" : "password",
                              "replication_type" : "unidirection",
                              "buckets" : ["saslbucket","standardbucket","standardbucket1"],
                              "filter_expression" : {"saslbucket": "50$"}
                             }
                 },
                "2" :
                 {
                    "name" : "access_phase",
                    "desc" :  "post_upgrade_access",
                    "workload" : ["b:standardbucket,coq:defaultkeys,ccq:std1ph5keys,d:50,g:50,ops:10000",
                                  "b:standardbucket1,coq:defaultkeys,ccq:std2sph5keys,d:50,g:50,ops:10000",
                                  "b:saslbucket,pwd:password,ccq:saslph5keys,g:50,d:50,ops:10000"],
                    "runtime" : 10800
                 },
                 "3" :
                 {
                    "name" : "rebalance_out_one_at_source",
                    "desc" : "Rebalance-out-1",
                    "workload" : ["b:standardbucket,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"rm" : "1"}
                 },
                 "4" :
                 {
                    "name" : "rebalance_in_one_source",
                    "desc" : "Rebalance-in-1",
                    "workload" : ["b:standardbucket,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"add" : "1"}
                 },
                 "5" :
                 {
                    "name" : "failover_one_and_rebalance_out",
                    "desc" : "failover_one_and_rebalance_out_at_source",
                    "workload" : ["b:standardbucket,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"auto_failover" : "1"}
                 },
                 "6" :
                 {
                    "name" : "failover_one_and_add_back",
                    "desc" : "failover_one_and_add_back_at_source",
                    "workload" : ["b:standardbucket,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"auto_failover" : "1", "add_back": "1"}
                 },
                 "7" :
                 {
                    "name" : "rebalance_out_one_at_destination",
                    "desc" : "Rebalance-out-1node at target cluster",
                    "workload" : ["b:standardbucket,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"remote" : "remote1", "rm" : "1"}
                 },
                 "8" :
                 {
                     "name" : "rebalance_in_one_destination",
                     "desc" : "Rebalance-in 1 node at target cluster",
                     "workload" : ["b:standardbucket,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                     "cluster" : {"remote" : "remote1", "add" : "1"}
                 },
                 "9" :
                 {
                    "name" : "failover_one_and_rebalance_out_dest",
                    "desc" : "failover_one_and_rebalance_out_at_dest",
                    "workload" : ["b:standardbucket,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"remote" : "remote1", "auto_failover" : "1"}
                 },
                 "10" :
                 {
                    "name" : "failover_one_and_add_back_dest",
                    "desc" : "failover_one_and_add_back_at_dest",
                    "workload" : ["b:standardbucket,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:default,ops:3000",
                                  "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:default,ops:3000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:default,ops:3000"],
                    "cluster" : {"remote" : "remote1", "auto_failover" : "1", "add_back": "1"}
                 },
                 "11" :
                 {
                    "name" : "reboot_source",
                    "desc" :  "reboot all nodes on source",
                    "workload" : [{"spec" : "b:standardbucket,g:100,ccq:std1ph5keys,ops:0"},
                                  {"spec" : "b:standardbucket1,g:100,ccq:std2ph5keys,ops:0"},
                                  {"spec" : "b:saslbucket,pwd:password,g:100,ccq:saslph5keys,ops:0"}],
                    "cluster" : {"soft_restart" : "7"},
                    "runtime": 1200
                 },
                 "12" :
                 {
                    "name" : "reboot_dest",
                    "desc" :  "reboot all nodes on dest",
                    "workload" : [{"spec" : "b:standardbucket,g:100,ccq:default,ops:0"},
                                  {"spec" : "b:standardbucket1,g:100,ccq:default,ops:0"},
                                  {"spec" : "b:saslbucket,pwd:password,g:100,ccq:default,ops:0"}],
                    "cluster" : {"remote" : "remote1", "soft_restart" : "7"},
                    "runtime": 1200
                 }
        }
}
