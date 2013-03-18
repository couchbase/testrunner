{
    "name" : "query",
    "desc" : "simple query test",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "create_bucket",
                    "desc" :  "create bucket",
                    "buckets" : {"default" : {"quota": "3082", "replicas": "1"} }
                },
                "1" :
                {
                    "name" : "create_views",
                    "desc" : "create_views",
                    "ddocs" : {"create" : [{"ddoc": "ddoc1", "view": "view1", "bucket": "default", "dev": false, "map": "function (doc) { emit(doc.st, null);}"},
                                    {"ddoc": "ddoc1", "view": "view2", "bucket": "default", "dev": false, "map": "function (doc) { emit(doc.num, null);}"}]}
                },
                "2" :
                {
                    "name" : "simple_load",
                    "desc" :  "load items at 5k ops",
                    "workload" : "s:100,ccq:simplekeys,ops:15000",
                    "template" : "default",
                    "runtime" : 20 },
                "3" :
                {
                    "name" : "query_access",
                    "desc" :  "run 200 queries/sec while accessing keys",
                    "workload" : "s:15,g:80,d:5,ccq:simplekeys,ops:15000",
                    "query" : "ddoc:ddoc1,view:view1,qps:30,idx:st",
                    "runtime" : 60 },
                "4" :
                {
                    "name" : "wait_for_query",
                    "desc" : "wait_for_queries_to_finish",
                    "workload" : "post:couch_views_ops < 10"
                },
                "5" :
                {
                    "name" : "query_with_docid_limit50",
                    "desc" :  "run 200 queries/sec while accessing keys",
                    "workload" : "s:15,g:80,d:5,ccq:simplekeys,ops:15000",
                    "query" : "ddoc:ddoc1,view:view1,qps:30,limit:50,include:startkey_docid endkey_docid,idx:st",
                    "runtime" : 60 },
                "6" :
                {
                    "name" : "query_with_magicvals",
                    "desc" :  "run 200 queries/sec while accessing keys",
                    "workload" : "s:15,g:80,d:5,ccq:simplekeys,ops:15000",
                    "query" : "ddoc:ddoc1,view:view1,qps:30,limit:50,start:$str4,end:$str4",
                    "runtime" : 60 },
                "7" :
                {
                    "name" : "wait_for_query",
                    "desc" : "wait_for_queries_to_finish",
                    "workload" : "post:couch_views_ops < 10"
                },
                "8" : {
                    "name" : "query_multi_view",
                    "desc" :  "run 200 queries/sec while accessing keys",
                    "workload" : "s:15,g:80,d:5,ccq:simplekeys,ops:15000",
                    "query" : ["ddoc:ddoc1,view:view1,qps:20,idx:st",
                               "ddoc:ddoc1,view:view2,qps:20,idx:num"],
                    "template" : "default",
                    "runtime" : 60 }
        }
}
