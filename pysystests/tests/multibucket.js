{
    "name" : "multi bucket",
    "desc" : "multi-bucket key-value test",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "multi_bucket",
                    "desc" :  "load data at 10k ops ",
                    "workload" : [{"bucket" : "default",
                                  "template" : "default",
                                  "spec" : "s:100,ops:1000"},
                                  {"bucket" : "default1",
                                  "template" : "default",
                                  "spec" : "s:100,ops:1000"}],
                    "runtime" : 40
                    },
                "1" :
                {
                    "name" : "load_access_parallel",
                    "desc" :  "load/access bucket mode",

                    "workload" : ["b:default,s:100,ops:1000",
                                  "b:default1,s:15,g:80,d:5,ccq:simplekeys,ops:1000"],
                    "runtime" : 60
                    }
    }
}

