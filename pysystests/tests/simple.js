{
    "name" : "simple",
    "desc" : "very simple key-value test",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "simple_load",
                    "desc" :  "load items at 1k ops",
                    "workload" : "s:100,ccq:simplekeys,ops:1000",
                    "template" : "default",
                    "runtime" : 20 },
                "1" :
                {
                    "name" : "simple_acces",
                    "desc" :  "access items at 1k ops with 80% gets",
                    "workload" : "s:15,g:80,d:5,ccq:simplekeys,ops:1000",
                    "template" : "default",
                    "runtime" : 40 }
        }
}
