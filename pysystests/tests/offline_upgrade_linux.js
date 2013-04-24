{
    "name" : "offline_upgrade_linux",
    "desc" : "rel_build_",
    "loop" : false,
    "phases" : {

                "0" :
                {
                    "name" : "create_buckets",
                    "desc" :  "create buckets",
                    "buckets" : {"default" : {"quota": "1500", "replicas": "1", "replica_index": "1"},
                                 "sasl": {"count": "1", "quota": "1000", "replicas": "2", "replica_index": "0"}},
                    "ddocs" : {"create": [{"ddoc":"ddoc1", "view":"view1", "map":"function(doc){emit(doc.key,doc.key_num);}", "bucket":"default"},
                                          {"ddoc":"ddoc2", "view":"view1", "map":"function(doc){emit(doc.key,doc.key_num);}", "bucket":"saslbucket"}]}
                },

                "1" :
                {
                    "name" : "load_init",
                    "desc" :  "load_non_hotset",
                    "workload" : [{"spec" : "s:100,ccq:defaultph1keys,ops:2000",
                                  "conditions" : "post:curr_items > 200000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph1keys,ops:2000",
                                   "conditions" : "post:curr_items > 200000"}]
                },

                "2" :
                {
                    "name" : "access_phase",
                    "desc" :  "access data for 2 hours",
                    "workload" : [{"spec" : "s:2,u:15,g:80,d:2,ccq:defaultph1keys,coq:defaultph1keys,ops:5000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:2,u:15,g:80,d:2,ccq:defaultph1keys,coq:defaultph1keys,ops:5000"}],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key"],
                    "runtime" : 7200
                },


               "3" :
                {
                    "name" : "wait_for_ddoc1",
                    "desc" :  "wait for ddoc1 initial index building",
                    "workload" : [{"spec" : "ops:0",
                                  "conditions" : {"post": {"type":"indexer", "target":"_design/ddoc1", "conditions": "progress > 99"}}}]
                },

               "4" :
                {
                    "name" : "wait_for_ddoc2",
                    "desc" :  "wait for ddoc2 initial index building",
                    "workload" : [{"spec" : "ops:0",
                                  "conditions" : {"post": {"type":"indexer", "target":"_design/ddoc2", "conditions": "progress > 99"}}}]
                },

                "5" :
                {
                    "name" : "drain_disks_for_reb",
                    "desc" :  "drain_disks",
                    "workload" : [{"spec" : "ops:0",
                                  "conditions" : "post:ep_queue_size < 1"},
                                  {"spec": "b:saslbucket,pwd:password,ops:0",
                                  "conditions" : "post:ep_queue_size < 1"}]
                },

                "6" :
                {
                    "name" : "offline_upgrade",
                    "desc" : "offline upgrade to current version"

                },

                "7" :
                {
                    "name" : "reb_in_one",
                    "desc" :  "add 1 node in source and destination",
                    "workload" : [{"spec" : "s:2,u:15,g:80,d:2,ccq:defaultph1keys,coq:defaultph1keys,ops:5000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:2,u:15,g:80,d:2,ccq:defaultph1keys,coq:defaultph1keys,ops:5000"}],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key"],
                    "cluster" :  {"add" : "1"}
                },


                "8" :
                {
                    "name" : "drain_disks_for_reb",
                    "desc" :  "drain_disks",
                    "workload" : [{"spec" : "g:100,coq:defaultph1keys,ops:1000",
                                  "conditions" : "post:ep_queue_size < 100000"},
                                  {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph1keys,ops:1000",
                                  "conditions" : "post:ep_queue_size < 100000"}]

                },

                "9" :
                {
                    "name" : "swap_rebalance",
                    "desc" :  "swap rebalance 1 node in source",
                    "workload" : [{"spec" : "s:2,u:15,g:80,d:2,ccq:defaultph1keys,coq:defaultph1keys,ops:5000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:2,u:15,g:80,d:2,ccq:defaultph1keys,coq:defaultph1keys,ops:5000"}],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key"],
                    "cluster" :  {"add": "1", "rm": "1"}
                },

                "10" :
                {
                    "name" : "replication_finish",
                    "desc" :  "verify replication is completed",
                    "workload" : [{"spec" : "ops:0",
                                  "conditions" : "post:replication_docs_rep_queue < 1"},
                                  {"spec": "b:saslbucket,pwd:password,ops:0",
                                  "conditions" : "post:replication_docs_rep_queue < 1"}]

                },

                "11" :
                {
                    "name" : "failover_add_node",
                    "desc" :  "failover and add 2.0.1 node on destination",
                    "workload" : [{"spec" : "s:2,u:15,g:80,d:2,ccq:defaultph1keys,coq:defaultph1keys,ops:5000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:2,u:15,g:80,d:2,ccq:defaultph1keys,coq:defaultph1keys,ops:5000"}],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key"],
                    "cluster" :  {"add": "1", "failover": "1"}
                },

                "12" :
                {
                    "name" : "create_ddoc",
                    "desc" : "create ddoc on destination",
                    "ddocs" : {"create": [{"ddoc":"ddoc1", "view":"view1", "map":"function(doc){emit(doc.key,doc.key_num);}", "bucket":"default"},
                                          {"ddoc":"ddoc2", "view":"view1", "map":"function(doc){emit(doc.key,doc.key_num);}", "bucket":"saslbucket"}]}
                },

               "13" :
                {
                    "name" : "wait_for_ddoc1",
                    "desc" :  "wait for ddoc1 initial index building on destination",
                    "workload" : [{"spec" : "ops:0",
                                  "conditions" : {"post": {"type":"indexer", "target":"_design/ddoc1", "conditions": "progress > 99"}}}]
                },

               "14" :
                {
                    "name" : "wait_for_ddoc2",
                    "desc" :  "wait for ddoc2 initial index building on destination",
                    "workload" : [{"spec" : "ops:0",
                                  "conditions" : {"post": {"type":"indexer", "target":"_design/ddoc2", "conditions": "progress > 99"}}}]
                },

                "15" :
                {
                    "name" : "reb_in_one",
                    "desc" :  "add 1 node in destination",
                    "workload" : [{"spec" : "s:2,u:15,g:80,d:2,ccq:defaultph1keys,coq:defaultph1keys,ops:5000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:2,u:15,g:80,d:2,ccq:defaultph1keys,coq:defaultph1keys,ops:5000"}],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key"],
                    "cluster" :  {"add" : "1"}
                },

                "16" :
                {
                    "name" : "restart_source",
                    "desc" : "restart source cluster",
                    "workload" : [{"spec" : "g:100,coq:defaultph1keys,ops:0",
                                  "conditions" : {"post": {"conditions": "ep_warmup_thread = complete"}}},
                                  {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph1keys,ops:0",
                                  "conditions" : {"post": {"conditions": "ep_warmup_thread = complete"}}}],
                    "cluster" :  {"soft_restart" : "2"}
                }
        }
}