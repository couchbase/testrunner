
{
    "name" : "plum",
    "desc" : "181 key-value use case testing",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "load_init",
                    "desc" :  "load 20M items",
                    "workload" : [{"spec" : "s:100,ccq:defaultph1keyes,ops:10000",
                                  "conditions" : "post:curr_items > 20000000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph1keys,ops:10000",
                                   "conditions" : "post:curr_items > 20000000"}]
                },

                "1" :
                {
                    "name" : "reb_out_4",
                    "desc" :  "remove 4 nodes, lower ops, and move to access mode",
                    "cluster" :  {"rm" : "10.3.2.104 10.3.2.105 10.3.2.106 10.3.2.107" },
                    "workload" : ["s:10,u:60,g:20,d:5,e:5,ttl:30,coq:defaultph1keyes,ops:5000",
                                  "b:saslbucket,pwd:password,s:10,u:60,g:20,d:5,e:5,ttl:30,coq:saslph1keys,ops:5000"]
                },

                "2" :
                {
                    "name" : "load_dgm",
                    "desc" :  "load until dgm",
                    "workload" : [{"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ccq:defaultph2keyes,ops:10000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 70"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,ccq:saslph2keys,ops:10000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 70"}]

                },

                "3" :
                {
                    "name" : "swap_reb_cache_miss",
                    "desc" :  "remove 4 nodes and add nodes, while in access mode from set1",
                    "workload" : ["s:10,u:60,g:20,d:5,e:5,ttl:30,coq:defaultph1keyes,ops:4000",
                                  "b:saslbucket,pwd:password,s:10,u:60,g:20,d:5,e:5,ttl:30,coq:saslph1keys,ops:4000"],
                    "cluster" :  {"rm" : "10.3.2.108 10.3.2.109 10.3.2.110 10.3.2.111",
                                  "add" : "10.3.2.104 10.3.2.105 10.3.2.106 10.3.2.107" }
                },

                "4" :
                {
                    "name" : "add_back",
                    "desc" :  "add_back 4 nodes from swap, while in access mode from set2",
                    "workload" : ["s:10,u:60,g:20,d:5,e:5,ttl:30,coq:defaultph2keyes,ops:4000",
                                  "b:saslbucket,pwd:password,s:10,u:60,g:20,d:5,e:5,ttl:30,coq:saslph2keys,ops:4000"],
                    "cluster" :  {"add" : "10.3.2.108 10.3.2.109 10.3.2.110 10.3.2.111"}
                }
        }
}
