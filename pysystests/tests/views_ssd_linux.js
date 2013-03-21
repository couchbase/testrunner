{
    "name" : "views_ssd_linux",
    "desc" : "rel_build_",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "set_up_cluster",
                    "desc" :  "rebalance in all nodes",
                    "cluster" : {"add" : "10.6.2.37 10.6.2.38 10.6.2.39 10.6.2.40 10.6.2.42 10.6.2.43"}
                },

                "1" :
                {
                    "name" : "create_buckets",
                    "desc" :  "create buckets",
                    "buckets" : {"default" : {"quota": "12600", "replicas": "1", "replica_index": "1"},
                                 "sasl": {"count": "1", "quota": "7000", "replicas": "1", "replica_index": "0"}},
                    "ddocs" : {"create": [{"ddoc":"ddoc1", "view":"view1", "map":"function(doc){emit(doc.key,doc.key_num);}", "bucket":"default"},
                                          {"ddoc":"ddoc1", "view":"view2", "map":"function(doc,meta){emit(meta.id,doc.key);}", "bucket":"default"},
                                          {"ddoc":"ddoc2", "view":"view1", "map":"function(doc){emit(doc.key,doc.key_num);}", "bucket":"saslbucket"},
                                          {"ddoc":"ddoc2", "view":"view2", "map":"function(doc,meta){emit(meta.id,doc.key);}", "bucket":"saslbucket"}]}
                },

                "2" :
                {
                    "name" : "load_init",
                    "desc" :  "load_non_hotset",
                    "workload" : [{"spec" : "s:100,ccq:defaultph1keys,ops:12000",
                                  "conditions" : "post:curr_items > 50000000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph1keys,ops:12000",
                                   "conditions" : "post:curr_items > 40000000"}]
                },

                "3" :
                {
                    "name" : "load_dgm",
                    "desc" :  "load_hotset",
                    "workload" : [{"spec" : "s:100,ccq:defaultph2keys,ops:12000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 70"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph2keys,ops:12000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 70"}]

                },

                "4" :
                {
                    "name" : "access_hotset",
                    "desc" : "eject_nonhotset_from_memory",
                    "workload" :[{"spec": "g:100,coq:defaultph2keys:,ops:20000"},
                                 {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph2keys,ops:20000"}],
                    "runtime": 1800
                },

                "5" :
                {
                    "name" : "access_phase",
                    "desc" :  "create cachemiss",
                    "workload" : [{"spec" : "s:5,u:5,g:80,d:5,e:5,m:5,ttl:86400,ccq:defaultph2keys,coq:defaultph1keys,ops:30000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:5,g:5,d:20,e:70,ttl:86400,ccq:saslph2keys,coq:saslph1keys,ops:30000"}],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:100,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc1,view:view2,bucket:default,t:default,qps:100,limit:50,include:startkey_docid endkey_docid",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:100,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view2,bucket:saslbucket,password:password,t:default,qps:100,limit:50,include:startkey_docid endkey_docid"],
                    "runtime" : 7200
                },

                "6" :
                {
                    "name" : "drain_disks_for_reb",
                    "desc" :  "drain_disks",
                    "workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:1000",
                                  "conditions" : "post:ep_queue_size < 100000"},
                                  {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph2keys,ops:1000",
                                  "conditions" : "post:ep_queue_size < 100000"}]

                },

                "7" :
                {
                    "name" : "reb_in_one",
                    "desc" :  "RB-1",
                    "workload" : ["s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:15000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:15000"],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc1,view:view2,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view2,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid"],
                    "cluster" :  {"add" : "10.6.2.44"}
                },

                "8" :
                {
                    "name" : "drain_disks_for_reb",
                    "desc" :  "drain_disks",
                    "workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:1000",
                                  "conditions" : "post:ep_queue_size < 100000"},
                                  {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph2keys,ops:1000",
                                  "conditions" : "post:ep_queue_size < 100000"}]

                },

                "9" :
                {
                    "name" : "swap_orchestrator",
                    "desc" :  "RB-2",
                    "workload" : ["s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:15000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:15000"],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc1,view:view2,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view2,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid"],
                    "cluster" :  {"add": "10.6.2.45", "rm": "10.6.2.37"}
                },

                "10" :
                {
                    "name" : "drain_disks_for_reb",
                    "desc" :  "drain_disks",
                    "workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:10",
                                  "conditions" : "post:ep_queue_size < 100000"},
                                  {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph2keys,ops:10",
                                  "conditions" : "post:ep_queue_size < 100000"}]

                },

                "11" :
                {
                    "name" : "reb_out_one",
                    "desc" :  "RB-3",
                    "workload" : ["s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:15000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:15000"],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc1,view:view2,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view2,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid"],
                    "cluster" :  {"rm" : "10.6.2.45"}
                },

                "12" :
                {
                    "name" : "drain_disks_for_reb",
                    "desc" :  "drain_disks",
                    "workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:10",
                                  "conditions" : "post:ep_queue_size < 100000"},
                                  {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph2keys,ops:10",
                                  "conditions" : "post:ep_queue_size < 100000"}]

                },

                "13" :
                {
                    "name" : "reb_in_one",
                    "desc" :  "RB-4",
                    "workload" : ["s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:15000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:15000"],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc1,view:view2,bucket:default,t:default,qps:50,limit:50,include:startkey_docid endkey_docid",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view2,bucket:saslbucket,password:password,t:default,qps:50,limit:50,include:startkey_docid endkey_docid"],
                    "cluster" :  {"add" : "10.6.2.45 10.6.2.37"}
                },

                "14" :
                {
                    "name" : "delete_ddoc",
                    "desc" :  "DDOC-1",
                    "ddocs" : {"delete": [{"ddoc":"ddoc1", "bucket":"default"}]},
                    "runtime": 600
                },

                "15" :
                {
                    "name" : "recreate_ddoc",
                    "desc" :  "DDOC-1",
                    "ddocs" : {"create": [{"ddoc":"ddoc1", "view":"view1", "map":"function(doc){emit(doc.key,doc.key_num);}", "bucket":"default"},
                                          {"ddoc":"ddoc1", "view":"view2", "map":"function(doc,meta){emit(meta.id,doc.key);}", "bucket":"default"}]}
                },

                "16" :
                {
                    "name" : "failover_one",
                    "desc" : "FL-1",
                    "workload" : ["s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:15000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:15000"],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:100,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc1,view:view2,bucket:default,t:default,qps:100,limit:50,include:startkey_docid endkey_docid",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:100,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view2,bucket:saslbucket,password:password,t:default,qps:100,limit:50,include:startkey_docid endkey_docid"],
                    "cluster" : {"auto_failover" : "10.6.2.44", "add_back": "10.6.2.44"}
                },

                "17" :
                {
                    "name" : "drain_disks_for_restart",
                    "desc" :  "drain_disks",
                    "workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:0",
                                  "conditions" : "post:ep_queue_size < 1"},
                                  {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph2keys,ops:0",
                                  "conditions" : "post:ep_queue_size < 1"}]
                },

                "18" :
                {
                    "name" : "restart_one_no_load",
                    "desc" :  "CR-1",
                    "workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:0",
                                  "conditions" : "post: 10.6.2.44, ep_warmup_thread = complete"},
                                  {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph2keys,ops:0",
                                  "conditions" : "post: 10.6.2.44, ep_warmup_thread = complete"}],
                    "cluster" :  {"soft_restart" : "10.6.2.44"}
                },

                "19" :
                {
                    "name" : "restart_one_with_load",
                    "desc" :  "CR-2",
                    "workload" : [{"spec": "s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:15000",
                                   "conditions" : "post: 10.6.2.43, ep_warmup_thread = complete"},
                                  {"spec": "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:15000",
                                   "conditions" : "post: 10.6.2.43, ep_warmup_thread = complete"}],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:100,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc1,view:view2,bucket:default,t:default,qps:100,limit:50,include:startkey_docid endkey_docid",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:100,limit:50,include:startkey_docid endkey_docid,idx:key",
                               "ddoc:ddoc2,view:view2,bucket:saslbucket,password:password,t:default,qps:100,limit:50,include:startkey_docid endkey_docid"],
                    "cluster" :  {"soft_restart" : "10.6.2.43"}
                },

                "20" :
                {
                    "name" : "restart_all",
                    "desc" :  "CR-3",
                    "cluster" : {"soft_restart" : "10.6.2.37 10.6.2.38 10.6.2.39 10.6.2.40 10.6.2.42 10.6.2.43 10.6.2.44 10.6.2.45"},
                    "runtime": 7200
                }
        }
}