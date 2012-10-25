{
    "name" : "query",
    "desc" : "simple query test",
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
                    "name" : "query_access",
                    "desc" :  "run 200 queries/sec while accessing keys",
                    "workload" : "s:15,g:80,d:5,ccq:simplekeys,ops:1000",
                    "query" : "ddoc:ddoc1,view:view1,qps:200",
                    "template" : "default",
                    "runtime" : 40 },
                "2" : {
                    "name" : "query_multi_view",
                    "desc" :  "run 200 queries/sec while accessing keys",
                    "workload" : "s:15,g:80,d:5,ccq:simplekeys,ops:1000",
                    "query" : ["ddoc:ddoc1,view:view1,qps:40",
                               "ddoc:ddoc1,view:view2,qps:50"],
                    "template" : "default",
                    "runtime" : 40 }
        }
}
