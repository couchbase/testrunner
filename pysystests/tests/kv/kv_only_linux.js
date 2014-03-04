{
    "name" : "kv_linux",
    "desc" : "2.5.1",
    "loop" : "",
    "phases" : {
                "0" :
                {
                    "name" : "load_init",
                    "desc" :  "load_initial",
                    "workload" : [{"spec" : "b:default,s:100,ccq:defkeys,ops:20000"},
                                  {"spec" : "b:standardbucket1,s:100,e:90,ttl:21600,ccq:b0keys,ops:5000"},
                                  {"spec" : "b:standardbucket2,t:1kdocs,s:100,ccq:b1keys,ops:5000"},
                                  {"spec" : "b:standardbucket3,s:100,ccq:b2keys,ops:5000"}],
                    "runtime" : 60
                },
                "1" :
                {
                    "name" : "reb_out_two",
                    "desc" :  "RB-2",
                    "workload" : [{"spec" : "s:10,u:5,g:80,d:5,ops:20000"},
                                  {"spec" : "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,ttl:3000,ops:1000"},
                                  {"spec" : "b:standardbucket3,s:5,u:5,g:20,d:10,ops:5000"}],
                    "cluster" :  {"rm" : "2"}
                },
                "2" :
                {
                    "name" : "load1",
                    "desc" :  "load1",
                    "workload" : [{"spec" : "b:default,s:100,ccq:defkeys,ops:20000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 90"},
                                  {"spec" : "b:standardbucket1,s:100,e:90,ttl:21600,ccq:b0keys,ops:5000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 70"},
                                  {"spec" : "b:standardbucket3,s:100,ccq:b2keys,ops:5000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 70"}]

                },
                "3" :
                {
                    "name" : "failover_one",
                    "desc" : "FL-1",
                    "workload" : [{"spec" : "s:10,u:5,g:80,d:5,ops:20000"},
                                  {"spec" : "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,ttl:3000,ops:1600"},
                                  {"spec" : "b:standardbucket3,s:5,u:5,g:20,d:10,ops:5000"}],
                    "cluster" : {"auto_failover" : "1"}
                },
                "4" :
                {
                    "name" : "load2",
                    "desc" :  "load2",
                    "workload" : [{"spec" : "b:default,s:100,ops:20000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 70"},
                                 {"spec" : "b:standardbucket1,s:100,e:90,ttl:21600,ops:5000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 50"},
                                 {"spec" : "b:standardbucket3,s:100,ops:5000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 50"}]

                },
                "5" :
                {
                    "name" : "eject_phase",
                    "desc" :  "eject",
                    "workload" : [{"spec" : "m:5,b:default,s:50,g:50,ops:20000"},
                                 {"spec"  : "b:standardbucket1,g:50,s:50,ops:5000"},
                                 {"spec" : "b:standardbucket3,g:50,s:50,ops:5000"}],
                    "runtime":  600
                },
                "6" :
                {
                    "name" : "reb_in_one",
                    "desc" :  "RB-4",
                    "workload" : [{"spec" : "s:10,u:5,g:80,d:5,ops:20000"},
                                  {"spec" : "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,ttl:3000,ops:1600"},
                                  {"spec" : "b:standardbucket3,s:5,u:5,g:20,d:10,ops:5000"}],
                    "cluster" :  {"add" : "1"}
                },
                "7" :
                {
                    "name" : "access_miss",
                    "desc" :  "create cachemiss",
                    "workload" : [{"spec" : "s:10,u:5,g:80,d:5,m:30,coq:defkeys,ops:20000"},
                                  {"spec" : "b:standardbucket1,s:5,u:5,g:80,d:5,e:5,m:5,ttl:86400,coq:b0keys,ops:5000"},
                                  {"spec" : "b:standardbucket3,t:1kdocs,s:5,u:5,g:20,d:10,m:5,coq:b2keys,ops:5000"}],
                    "runtime": 1200
                },
                "8" :
                {
                    "name" : "drain_disks",
                    "desc" :  "drain_disks",
                    "workload" : [{"spec" : "g:100,ops:0",
                                  "conditions" : "post:ep_queue_size < 1"}]
                },
                "9" :
                {
                    "name" : "add_back",
                    "desc" :  "RB-1",
                    "workload" : [{"spec" : "s:10,u:5,g:80,d:5,m:5,ops:20000,ccq:defkeys"},
                                  {"spec" : "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:b0keys,ops:1600"},
                                  {"spec" : "b:standardbucket3,s:5,u:5,g:20,d:10,m:5,ccq:b2keys,ops:5000"}],

                    "cluster" :  {"add" : "1"}
                },
                "10" :
                {
                    "name" : "extensive_deletes",
                    "desc" :  "extensive_deletes",
                    "workload" : [{"spec" : "d:100,coq:defkeys,ops:20000"},
                                  {"spec" : "b:standardbucket1,d:100,coq:b0keys,ops:5000"},
                                  {"spec" : "b:standardbucket3,d:100,coq:b2keys,ops:5000"}],
                    "runtime": 1200
                },
                "11" :
                {
                    "name" : "swap_orchestrator",
                    "desc" :  "RB-2",
                    "workload" : [{"spec" : "s:10,u:5,g:80,d:5,m:5,ops:20000,ccq:defkeys"},
                                  {"spec" : "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,ccq:b0keys,ops:1600"},
                                  {"spec" : "b:standardbucket3,s:5,u:5,g:20,d:10,m:5,ccq:b2keys,ops:5000"}],
                    "cluster" :  {"add": "1", "rm": "1", "orchestrator": "True"}
                }
        }
}
