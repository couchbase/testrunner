{
    "name" : "kv_xdcr_tpcc_cluster1",
    "desc" : "kv_xdcr_tpcc_cluster1",
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
                    "name" : "run_tpcc_load",
                    "desc" :  "run tpcc load",
                    "ssh"  : {"hosts"    : ["127.0.0.1"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "screen -dmS tpcc_load sh -c \"cd tests/kv_xdcr_tpcc/cwpy/ && python tpcc.py --debug --warehouses 100 --clients 100 --no-execute n1ql; exec bash;\""},
                    "runtime" : 30
                 },
                 "3" :
                 {
                    "name" : "load_in parallel_to_tpcc_load",
                    "desc" :  "load_in parallel_to_tpcc_load",
                    "workload" : [{"spec" : "b:ITEM,t:template512,s:100,e:20,ttl:86400,ccq:std1ph5keys,ops:60000",
                                  "conditions" : "post:count = 10000000"},
                                  {"spec" : "b:ORDER_LINE,e:30,s:100,ttl:86400,ccq:std2ph5keys,ops:40000",
                                  "conditions" : "post:count = 10000000"},
                                  {"spec" : "b:CUSTOMER,s:100,ccq:saslph5keys,ops:60000",
                                   "conditions" : "post:count = 20000000"},
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
                 "4" :
                 {
                    "name" : "pair_sites_for_CUSTOMER,ITEM,ORDER_LINE",
                    "desc" : "set_replication_type_for_CUSTOMER,ITEM,ORDER_LINE",
                    "xdcr" : {
                              "dest_cluster_name" : "remote1",
                              "dest_cluster_rest_username" : "Administrator",
                              "dest_cluster_rest_pwd" : "password",
                              "replication_type" : "unidirection",
                              "buckets" : ["CUSTOMER","ITEM","ORDER_LINE"],
                              "filter_expression" : {"CUSTOMER": "50$"}
                             }
                 },
                 "5" :
                 {
                    "name" : "tpcc_fire_queries",
                    "desc" : "phase where tpcc fires transaction queries",
                    "ssh"  : {"hosts"    : ["127.0.0.1"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "screen -dmS tpcc_query sh -c \"cd tests/kv_xdcr_tpcc/cwpy/ && python tpcc.py --debug --no-load  --warehouses 100  --duration 10800  --clients 100 n1ql; exec bash;\""
                             },
                    "runtime" : 120
                 },
                "6" :
                 {
                    "name" : "access_phase",
                    "desc" :  "post_upgrade_access",
                    "workload" : ["b:ITEM,coq:defaultkeys,ccq:std1ph5keys,d:20,g:80,ops:10000",
                                  "b:ORDER_LINE,coq:defaultkeys,ccq:std2sph5keys,d:80,g:20,ops:10000",
                                  "b:CUSTOMER,ccq:saslph5keys,g:80,d:20,ops:10000"],
                    "runtime" : 7200
                 },
                 "7" :
                 {
                    "name" : "rebalance_out_one_at_source",
                    "desc" : "Rebalance-out-1",
                    "workload" : ["b:ITEM,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:ORDER_LINE,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:CUSTOMER,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"rm" : "1"}
                 },
                 "8" :
                 {
                    "name" : "rebalance_in_one_source",
                    "desc" : "Rebalance-in-1",
                    "workload" : ["b:ITEM,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:ORDER_LINE,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:CUSTOMER,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"add" : "1"}
                 },
                 "9" :
                 {
                    "name" : "failover_one_and_rebalance_out",
                    "desc" : "failover_one_and_rebalance_out_at_source",
                    "workload" : ["b:ITEM,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:ORDER_LINE,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:CUSTOMER,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"auto_failover" : "1"}
                 },
                 "10" :
                 {
                    "name" : "failover_one_and_add_back",
                    "desc" : "failover_one_and_add_back_at_source",
                    "workload" : ["b:ITEM,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:ORDER_LINE,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:CUSTOMER,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"auto_failover" : "1", "add_back": "1"}
                 },
                 "11" :
                 {
                    "name" : "rebalance_out_one_at_destination",
                    "desc" : "Rebalance-out-1node at target cluster",
                    "workload" : ["b:ITEM,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:ORDER_LINE,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:CUSTOMER,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"remote" : "remote1", "rm" : "1"}
                 },
                 "12" :
                 {
                     "name" : "rebalance_in_one_destination",
                     "desc" : "Rebalance-in 1 node at target cluster",
                     "workload" : ["b:ITEM,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:ORDER_LINE,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:CUSTOMER,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                     "cluster" : {"remote" : "remote1", "add" : "1"}
                 },
                 "13" :
                 {
                    "name" : "failover_one_and_rebalance_out_dest",
                    "desc" : "failover_one_and_rebalance_out_at_dest",
                    "workload" : ["b:ITEM,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std1ph5keys,ops:3000",
                                  "b:ORDER_LINE,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:std2ph5keys,ops:3000",
                                  "b:CUSTOMER,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:saslph5keys,ops:3000"],
                    "cluster" : {"remote" : "remote1", "auto_failover" : "1"}
                 },
                 "14" :
                 {
                    "name" : "failover_one_and_add_back_dest",
                    "desc" : "failover_one_and_add_back_at_dest",
                    "workload" : ["b:ITEM,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:default,ops:3000",
                                  "b:ORDER_LINE,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:default,ops:3000",
                                  "b:CUSTOMER,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:default,ops:3000"],
                    "cluster" : {"remote" : "remote1", "auto_failover" : "1", "add_back": "1"}
                 },
                 "15" :
                 {
                    "name" : "reboot_source",
                    "desc" :  "reboot all nodes on source",
                    "workload" : [{"spec" : "b:ITEM,g:100,ccq:std1ph5keys,ops:0"},
                                  {"spec" : "b:ORDER_LINE,g:100,ccq:std2ph5keys,ops:0"},
                                  {"spec" : "b:CUSTOMER,g:100,ccq:saslph5keys,ops:0"}],
                    "cluster" : {"soft_restart" : "7"},
                    "runtime": 1200
                 },
                 "16" :
                 {
                    "name" : "reboot_dest",
                    "desc" :  "reboot all nodes on dest",
                    "workload" : [{"spec" : "b:ITEM,g:100,ccq:default,ops:0"},
                                  {"spec" : "b:ORDER_LINE,g:100,ccq:default,ops:0"},
                                  {"spec" : "b:CUSTOMER,g:100,ccq:default,ops:0"}],
                    "cluster" : {"remote" : "remote1", "soft_restart" : "7"},
                    "runtime": 1200
                 }
        }
}
