{
    "name" : "ElasticSearch_Baseline_Key-Value",
    "desc" : "ElasticSearch_Baseline_Key-Value",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "load_init",
                    "desc" :  "load_10M_items",
                    "workload" : [{"spec" : "s:100,ccq:defaultph1keyes,ops:40000",
                                  "conditions" : "post:curr_items > 20000000"}]
                },
                "1" :
                {
                    "name" : "drain_disks_ph1",
                    "desc" :  "drain_disks_ph1",
                    "workload" : [{"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "g:100,coq:defaultph1keyes,ops:0",
                                  "conditions" : "post:ep_queue_size < 100"}]

                },
                "2" :
                {
                    "name" : "reb_out_4",
                    "desc" :  "remove_4_nodes_lower_ops,_and_move_to_access_mode",
                    "cluster" :  {"rm" : "10.3.121.101 10.3.121.176 10.3.121.250 10.3.121.253" },
                    "workload" : ["s:10,u:60,g:20,d:5,ccq:defaultph2keyes,ops:10000"]
                },
                "3" :
                {
                    "name" : "load_dgm",
                    "desc" :  "load_until_dgm",
                    "workload" : [{"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ccq:defaultph2keyes,ops:40000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 70"}]

                },
                "4" :
                {
                    "name" : "15min_cache_miss",
                    "desc" :  "15min_cache_miss",
                    "workload" : {"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:10,u:60,g:20,d:5,m:1,coq:defaultph1keyes,ops:20000"},
                    "runtime" : 900
                }
        }
}
