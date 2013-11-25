{
    "name" : "dgm ejections",
    "desc" : "push system into dgm, eject items and fetch",
    "loop" : "",
    "phases" : {
                "0" :
                {
                    "name" : "load_misskeys",
                    "desc" :  "load items till dgm",
                    "workload" : {"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ccq:misskeys,ops:30000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 70"},
                    "runtime" : 60
                    },
                "1" :
                {
                    "name" : "load_dgm",
                    "desc" :  "load further into dgm",
                    "workload" : {"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ccq:workingset,ops:30000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 50"}
                    },
                "2" :
                {
                    "name" : "eject_misskeys",
                    "desc" : "eject misskeys",
                    "workload" :"g:100,coq:workingset,ops:15000",
                    "runtime" : 60
                    },
                "3" :
                {
                    "name" : "access_misskeys",
                    "desc" : "Access hotset items for 2 mins",
                    "workload" :"g:100,coq:misskeys,m:10,ops:15000",
                    "runtime" : 240
                    }
        }
}
