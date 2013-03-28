{
    "name" : "query",
    "desc" : "simple query test",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "create_views",
                    "desc" : "create_views",
                    "ddocs" : {"create" : [{"ddoc": "ddoc1", "view": "view1", "bucket": "default", "dev": false, "map": "function (doc) { emit(doc.city, null);}"},
                                           {"ddoc": "ddoc1", "view": "view2", "bucket": "default", "dev": false, "map": "function (doc) { emit(doc.city, null);}"},
                                           {"ddoc": "ddoc2", "view": "view1", "bucket": "saslbucket", "dev": false, "map": "function (doc) { emit(doc.city, null);}"},
                                           {"ddoc": "ddoc2", "view": "view2", "bucket": "saslbucket", "dev": false, "map": "function (doc) { emit(doc.city, null);}"}]}
                },
                "1" :
                {
                    "name" : "initial_loading",
                    "desc" :  "load both buckets",
                    "workload" : ["s:100,ccq:defaultph1keys,ops:1000",
                                  "b:saslbucket,ccq:saslph1keys,pwd:password,s:100,ops:1000"],
                    "template" : "default",
                    "runtime" : 60
                },
               "2" :
                {
                    "name" : "wait for ddoc1 indexer",
                    "workload" : [{"spec" : "ops:0",
                                  "conditions" : {"post": {"type":"indexer", "target":"_design/ddoc1", "conditions": "progress > 99"}}}]
                },
               "3" :
                {
                    "name" : "wait for ddoc2 indexer",
                    "workload" : [{"spec" : "ops:0",
                                  "conditions" : {"post": {"type":"indexer", "target":"_design/ddoc2", "conditions": "progress > 99"}}}]
                },
               "4" :
                {
                    "name" : "multi_query_kv",
                    "desc" : "multiple queries and loading accross buckets",
                    "workload" : ["s:10,u:10,g:80,d:5,e:5,m:5,ttl:86400,ccq:defaultph2keys,coq:defaultph1keys,ops:100",
                                  "b:saslbucket,pwd:password,s:10,g:10,d:20,e:70,ttl:86400,coq:saslph1keys,ops:100"],
                    "query" : ["ddoc:ddoc1,view:view1,bucket:default,t:default,qps:5,limit:50,include:startkey_docid endkey_docid,idx:city",
                               "ddoc:ddoc1,view:view2,bucket:default,t:default,qps:5,limit:50,include:startkey_docid endkey_docid,idx:city",
                               "ddoc:ddoc2,view:view1,bucket:saslbucket,password:password,t:default,qps:5,limit:50,include:startkey_docid endkey_docid,idx:city",
                               "ddoc:ddoc2,view:view2,bucket:saslbucket,password:password,t:default,qps:5,limit:50,include:startkey_docid endkey_docid,idx:city"],
                    "runtime" :  120
                }
            }
}
