{
    "name" : "cachemiss",
    "desc" : "push system into dgm, eject items and fetch with controlled miss ratio",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "init_load",
                    "desc" :  "load items at 10k ops till light dgm",
                    "workload" : {"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ccq:ejectset,ops:10000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 95"}
                    },
                "1" :
                {
                    "name" : "load_hostset ",
                    "desc" :  "load 100k items",
                    "workload" : {"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ccq:hotset,ops:10000",
                                  "conditions" : "post:count>100000"}
                    },
                "2" :
                {
                    "name" : "access workingset",
                    "desc" : "Access workingset items for 2 mins",
                    "workload" :"g:100,coq:hotset,ops:15000"
                    },
                "3" :
                {
                    "name" : "cache_miss",
                    "desc" : "Access ejected set with 2% miss ratio",
                    "workload" :"g:80,m:2,coq:ejectset,s:20,ccq:hotset,ops:5000"
                    }
        }
}
