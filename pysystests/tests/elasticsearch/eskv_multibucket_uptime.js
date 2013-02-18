{
    "name" : "ESKV-multibucket_uptime",
    "desc" : "ESKV-multibucket_uptime",
    "loop" : true,
    "phases" : {
                "0" :
                {
                    "name" : "load_init",
                    "desc" :  "load_init",
                    "workload" : [{"spec" : "s:10,u:60,g:20,d:5,ccq:defaultph1keyes,ops:20000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:10,u:60,g:20,d:5,ccq:saslph1keys,ops:20000"}],
                    "runtime" : 1800
                },

                "1" :
                {
                    "name" : "load_10M_exp",
                    "desc" :  "load_10M_exp",
                    "workload" : [{"spec" : "s:100,e:100,ttl:1800,ops:20000",
                                  "conditions" : "post:curr_items > 10000000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:100,e:100,ttl:1800,ops:20000",
                                   "conditions" : "post:curr_items > 10000000"}]
                },
                "2" :
                {
                    "name" : "kv_till_expire",
                    "desc" :  "kv_till_expire",
                    "workload" : [{"spec" : "s:10,u:60,g:20,d:5,ccq:defaultph2keyes,ops:20000"},
                                  {"spec" : "b:saslbucket,pwd:password,s:10,u:60,g:20,d:5,ccq:saslph2keys,ops:20000"}],
                    "runtime" : 1800
                },
                "3" :
                {
                    "name" : "cache_miss_till_active",
                    "desc" :  "cache_miss_till_active",
                    "workload" : [{ "spec" : "s:3,u:60,g:35,d:2,m:1,coq:defaultph1keyes,ops:10000",
                                    "conditions" : "post:vb_active_resident_items_ratio > 99"},
                                   { "spec" : "b:saslbucket,pwd:password,s:3,u:60,g:35,d:2,m:1,coq:saslph1keys,ops:10000",
                                    "conditions" : "post:vb_active_resident_items_ratio > 99"}]
                },
                "4" :
                {
                    "name" : "cleanup_ph1",
                    "desc" :  "cleanup_ph1",
                    "workload" : [{"spec" : "d:100,coq:defaultph1keyes,ops:20000"},
                                  {"spec" : "b:saslbucket,pwd:password,d:100,coq:saslph1keys,ops:20000"}],

                    "runtime" : 600
                },
                "5" :
                {
                    "name" : "cleanup_ph2",
                    "desc" :  "cleanup_ph2",
                    "workload" : [{"spec" : "d:100,coq:defaultph2keyes,ops:20000"},
                                  {"spec" : "b:saslbucket,pwd:password,d:100,coq:saslph2keys,ops:20000"}],
                    "runtime" : 300
                }
        }
}
