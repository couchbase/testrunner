{
    "name" : "orange",
    "desc" : "view  use case testingi with rebalance",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "load_init",
                    "desc" :  "load 20M items",
                    "workload" : [{"spec" : "s:100,ccq:defaultph1keys,ops:8000",
                                  "conditions" : "post:curr_items > 20000000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph1keys,ops:8000",
                                   "conditions" : "post:curr_items > 20000000"}]
                },

                "1" :
                {
                    "name" : "load_dgm",
                    "desc" :  "load until dgm",
                    "workload" : [{"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ccq:defaultph2keys,ops:8000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 90"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph2keys,ops:8000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 90"}]

                },

                "2" :
                {
                    "name" : "RB-3",
                    "desc" :  "swap rebalance non orchestrator node",
                    "workload" : ["s:10,u:60,g:25,d:3,e:2,m:2,ttl:30,coq:defaultph1keys,ccq:defaultph2keys,ops:5000",
                                  "b:saslbucket,pwd:password,s:10,u:60,g:25,d:3,e:2,m:2,ttl:30,coq:saslph1keys,ccq:saslph2keys,ops:5000"],
                    "cluster" :  {"rm" : "10.6.2.43",
                                  "add" : "10.6.2.44" }
                },

                "3" :
                {
                    "name" : "RB-1",
                    "desc" :  "add a node and rebalance",
                    "workload" : ["s:10,u:60,g:25,d:3,e:2,m:2,ttl:30,coq:defaultph1keys,ccq:defaultph2keys,ops:5000",
                                  "b:saslbucket,pwd:password,s:10,u:60,g:25,d:3,e:2,m:2,ttl:30,coq:saslph1keys,ccq:saslph2keys,ops:5000"],
                    "cluster" :  { "add" : "10.6.2.45" }
                },

                "4" :
                {
                    "name" : "RB-2",
                    "desc" :  "swap the orchestrator with a new node",
                    "workload" : ["s:10,u:60,g:25,d:3,e:2,m:2,ttl:30,coq:defaultph2keys,ccq:defaultph1keys,ops:5000",
                                  "b:saslbucket,pwd:password,s:10,u:60,g:25,d:3,e:2,m:2,ttl:30,coq:saslph2keys,ccq:saslph1keys,ops:5000"],
                    "cluster" :  { "add" : "10.6.2.43", "rm": "10.6.2.37" }
                },

                "5" :
                {
                    "name" : "RB-4",
                    "desc" :  "remove one node and rebalance",
                    "workload" : ["s:10,u:60,g:25,d:3,e:2,m:2,ttl:30,coq:defaultph2keys,ccq:defaultph1keys,ops:5000",
                                  "b:saslbucket,pwd:password,s:10,u:60,g:25,d:3,e:2,m:2,ttl:30,coq:saslph2keys,ccq:saslph1keys,ops:5000"],
                    "cluster" :  { "rm": "10.6.2.45" }
                }

        }
}

