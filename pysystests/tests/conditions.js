{
    "name" : "conditons",
    "desc" : "basic test that uses conditions in each phase",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "load 10k",
                    "desc" :  "load 10k items at 1k ops",
                    "workload" : {"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ccq:loadph1keys,ops:1000",
                                  "conditions" : "post:curr_items>10000"}
                    },
                "1" :
                {
                    "name" : "write queue 1M",
                    "desc" :  "Load at 20k to push disk write queue to 1 Million",
                    "workload" : {"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ops:20000",
                                  "conditions" : "post:disk_write_queue>1000000"}
                    },
                "2" :
                {
                    "name" : "access till 5k",
                    "desc" : "Access Mode until 50k new items created",
                    "workload" : {"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "g:70,s:30,coq:loadph1keys,ops:20000",
                                  "conditions" : "post:count = 50000"}
                    }
        }
}
