{
    "name" : "dgm ejections",
    "desc" : "push system into dgm, eject items and fetch",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "load hotset",
                    "desc" :  "load items at 10k ops till dgm",
                    "workload" : {"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ccq:dgmhotset,ops:10000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 80"}
                    },
                "1" :
                {
                    "name" : "load workingset",
                    "desc" :  "load 100k items",
                    "workload" : {"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ccq:workingset,ops:10000",
                                  "conditions" : "post:count>100000"}
                    },
                "2" :
                {
                    "name" : "access workingset",
                    "desc" : "Access workingset items for 2 mins",
                    "workload" :"g:100,coq:workingset,ops:15000"
                    },
                "3" :
                {
                    "name" : "access hotset",
                    "desc" : "Access hotset items for 2 mins",
                    "workload" :"g:100,coq:hotset,ops:15000"
                    }
        }
}
