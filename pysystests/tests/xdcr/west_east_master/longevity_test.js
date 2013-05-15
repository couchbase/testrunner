{
    "name" : "xdcr_example",
    "desc" : "example xdcr test",
    "loop" : true,
    "phases" : {
                "0" :
                {
                    "name" : "initsdk",
                    "desc" :  "initsdk",
                    "workload" : [{"spec" : "s:100,b:bucket0,ops:1000"},
                                  {"remote" : "remote1",
                                   "spec" : "s:100,b:bucket0,ops:1000"},
                                  {"spec" : "s:100,b:bucket1,ops:1000"},
                                  {"spec" : "s:100,b:bucket2,ops:1000"}],
                    "runtime" : 30
                },
                "1" :
                {
                    "name" : "multi_bucket_multi_siteload",
                    "desc" :  "load items in multi_bucket_multiple_sites",
                    "workload" : [{"spec" : "g:70,d:8,u:10,s:10,e:5,ttl:466560000,m:1,t:template3k5k,ccq:b0,b:bucket0,ops:15000"},
                                  {"remote" : "remote1",
                                   "spec" : "g:90,d:2,u:2,s:15,e:5,ttl:466560000,m:1,t:template3k5k,ccq:b0,b:bucket0,ops:4000"},
                                  {"spec" : "g:30,d:4,u:40,s:4,e:10,ttl:5184000,m:1,t:template256,ccq:b1,b:bucket1,ops:10000"},
                                  {"spec" : "s:100,g:20,e:80,d:10,e:5,ttl:3600,m:1,t:template100,ccq:b2,b:bucket2,ops:2000"}],
                    "runtime" : 2200
                },
                "2" :
                {
                    "name" : "long access",
                    "desc" :  "load items in multi_bucket_multiple_sites",
                    "workload" : [{"spec" : "g:70,d:2,s:2,e:5,ttl:466560000,m:1,t:template3k5k,ccq:b0,b:bucket0,ops:20000"},
                                  {"remote" : "remote1",
                                   "spec" : "g:90,d:2,s:2,e:5,ttl:466560000,m:1,t:template3k5k,ccq:b0,b:bucket0,ops:4000"},
                                  {"spec" : "g:70,d:2,s:2,e:10,ttl:5184000,m:1,t:template256,ccq:b1,b:bucket1,ops:5000"},
                                  {"spec" : "g:80,e:80,d:2,s:1,ttl:3600,m:1,t:template100,ccq:b2,b:bucket2,ops:2000"}],
                    "runtime" : 10800
                }



        }
}
