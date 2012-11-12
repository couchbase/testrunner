
{
    "name" : "plum",
    "desc" : "online upgrade testing",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "load_init",
                    "desc" :  "load 20M items",
                    "workload" : [{"spec" : "s:100,ccq:defaultph1keyes,ops:10000",
                                  "conditions" : "post:curr_items > 5000000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph1keys,ops:8000",
                                   "conditions" : "post:curr_items > 5000000"}]
                },

                "1" :
                {
                    "name" : "load_dgm",
                    "desc" :  "load until dgm",
                    "workload" : [{"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ccq:defaultph2keyes,ops:8000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 80"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph2keys,ops:8000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 80"}]

                },

                "2" :
                {
                    "name" : "swap_reb_cache_miss",
                    "desc" :  "remove 1 1.8.1 node and add a 2.0.0 node, while in access mode from set1",
                    "workload" : ["s:10,u:60,g:20,d:5,e:5,m:2,ttl:30,coq:defaultph1keyes,ops:4000",
                                  "b:saslbucket,pwd:password,s:10,u:60,g:20,d:5,e:5,m:2,ttl:30,coq:saslph1keys,ops:4000"],
                    "cluster" :  {"rm" : "10.6.2.68",
                                  "add" : "10.6.2.69" }
                },

                "3" :
                {
                    "name" : "after_upgrade_load",
                    "desc" :  "load when swap rebalance finishes",
                    "workload" : [{"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:10,u:60,g:20,d:5,e:5,m:2,ttl:30,ccq:defaultph2keyes,ops:4000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:10,u:60,g:20,d:5,e:5,m:2,ttl:30,ccq:saslph2keys,ops:4000"}],
                    "runtime" : 43200

                }
        }
}
