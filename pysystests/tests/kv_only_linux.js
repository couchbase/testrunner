{
    "name" : "kv_linux",
    "desc" : "2.0.2build",
    "loop" : false,
    "phases" : {

                "0" :
                {
                    "name" : "create_buckets",
                    "desc" :  "create buckets",
                    "buckets" : {"default" : {"quota": "6000", "replicas": "1", "replica_index": "0"},
                                 "sasl": {"count": "1", "quota": "4000", "replicas": "1", "replica_index": "0"}}
                },

                "1" :
                {
                    "name" : "load_init",
                    "desc" :  "load_non_hotset",
                    "workload" : [{"spec" : "s:100,ccq:defaultph1keys,ops:8000",
                                  "conditions" : "post:curr_items > 10000000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph1keys,ops:8000",
                                   "conditions" : "post:curr_items > 10000000"}]
                },

                "2" :
                {
                    "name" : "load_dgm",
                    "desc" :  "load_hotset",
                    "workload" : [{"spec" : "s:100,ccq:defaultph2keys,ops:8000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 70"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph2keys,ops:6000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 70"}]

                },

                "3" :
                {
                    "name" : "access_hotset",
                    "desc" : "eject_nonhotset_from_memory",
                    "workload" :[{"spec": "g:100,coq:defaultph2keys,ops:8000"},
                                 {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph2keys,ops:8000"}],
                    "runtime": 1800
                },

                "4" :
                {
                    "name" : "access_phase",
                    "desc" :  "create cachemiss",
                    "workload" : [{"spec" : "s:5,u:5,g:80,d:5,e:5,m:5,ttl:86400,ccq:defaultph2keys,coq:defaultph1keys,ops:16000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:5,u:5,g:20,d:10,e:60,m:5,ttl:86400,ccq:saslph2keys,coq:saslph1keys,ops:16000"}],
                    "runtime" : 7200
                },

                "5" :
                {
                    "name" : "reb_in_one",
                    "desc" :  "RB-1",
                    "workload" : ["s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:16000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:16000"],
                    "cluster" :  {"add" : "1"}
                },

                "6" :
                {
                    "name" : "swap_orchestrator",
                    "desc" :  "RB-2",
                    "workload" : ["s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:16000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:16000"],
                    "cluster" :  {"add": "1", "rm": "1", "orchestrator": "True"}
                },

                "7" :
                {
                    "name" : "reb_out_one",
                    "desc" :  "RB-3",
                    "workload" : ["s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:16000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:16000"],
                    "cluster" :  {"rm" : "1"}
                },

                "8" :
                {
                    "name" : "reb_in_two",
                    "desc" :  "RB-4",
                    "workload" : ["s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:16000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:16000"],
                    "cluster" :  {"add" : "2"}
                },


                "9" :
                {
                    "name" : "failover_one",
                    "desc" : "FL-1",
                    "workload" : ["s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:8000",
                                  "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:8000"],
                    "cluster" : {"auto_failover" : "1", "add_back": "1"}
                },

                "10" :
                {
                    "name" : "drain_disks_for_restart",
                    "desc" :  "drain_disks",
                    "workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:0",
                                  "conditions" : "post:ep_queue_size < 1"},
                                  {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph2keys,ops:0",
                                  "conditions" : "post:ep_queue_size < 1"}]
                },

                "11" :
                {
                    "name" : "restart_one_no_load",
                    "desc" :  "CR-1",
                    "workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:0",
                                  "conditions" : {"post": {"conditions": "ep_warmup_thread = complete", "ip": "172.23.105.29"}}},
                                  {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph2keys,ops:0",
                                  "conditions" : {"post": {"conditions": "ep_warmup_thread = complete", "ip": "172.23.105.29"}}}],
                    "cluster" :  {"soft_restart" : "172.23.105.29"}
                },

                "12" :
                {
                    "name" : "restart_one_with_load",
                    "desc" :  "CR-2",
                    "workload" : [{"spec": "s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:16000",
                                   "conditions" : {"post": {"conditions": "ep_warmup_thread = complete", "ip": "172.23.105.29"}}},
                                  {"spec": "b:saslbucket,pwd:password,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:16000",
                                   "conditions" : {"post": {"conditions": "ep_warmup_thread = complete", "ip": "172.23.105.29"}}}],
                    "cluster" :  {"soft_restart" : "172.23.105.29"}
                },

                "13" :
                {
                    "name" : "restart_all",
                    "desc" :  "CR-3",
                    "workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:0"},
                                  {"spec": "b:saslbucket,pwd:password,g:100,coq:saslph2keys,ops:0"}],
                    "cluster" : {"soft_restart" : "10"},
                    "runtime": 7200
                },

                "14" :
                {
                    "name" : "post_long_run",
                    "desc" :  "post",
                    "workload" : ["s:5,u:20,g:68,d:5,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:20000",
                                  "b:saslbucket,pwd:password,s:5,u:20,g:68,d:5,e:2,m:5,ttl:3000,coq:saslph1keys,ccq:saslph2keys,ops:20000"],
                    "runtime": 259200
                }

        }
}
