{
    "name" : "multi bucket",
    "desc" : "multi-bucket key-value test",
    "loop" : true,
    "phases" : {
                "0" :
                {
                    "name" : "multi_bucket",
                    "desc" :  "load data at 10k ops ",
                    "workload" : [{"spec" : "s:100,ccq:defaultkeys,ops:5000",
                                  "conditions" : "post:curr_items>5000000"},
                                  {"spec" : "b:saslbucket1,pwd:password,ccq:saslkeys,s:100,ops:5000",
                                  "conditions" : "post:curr_items>4000000"},
                                  {"spec" : "b:saslbucket2,pwd:password,ccq:sas2keys,s:100,ops:5000",
                                  "conditions" : "post:curr_items>4000000"},
                                  {"spec" : "b:saslbucket3,pwd:password,ccq:sas3keys,s:100,ops:5000",
                                  "conditions" : "post:curr_items>4000000"},
                                  {"spec" : "b:saslbucket4,pwd:password,ccq:sas4keys,s:100,ops:5000",
                                  "conditions" : "post:curr_items>4000000"},
                                  {"spec" : "b:saslbucket5,pwd:password,ccq:sas5keys,s:100,ops:5000",
                                  "conditions" : "post:curr_items>4000000"},
                                  {"spec" : "b:saslbucket6,pwd:password,ccq:sas6keys,s:100,ops:5000",
                                  "conditions" : "post:curr_items>4000000"},
                                  {"spec" : "b:saslbucket7,pwd:password,ccq:sas7keys,s:100,ops:5000",
                                  "conditions" : "post:curr_items>4000000"},
                                  {"spec" : "b:saslbucket,pwd:password,ccq:sas0keys,s:100,ops:5000",
                                  "conditions" : "post:curr_items>4000000"}]
                    },
                "1" :
                {
                    "name" : "access_phase",
                    "desc" :  "access_phase",
                    "workload" : ["b:default,coq:defaultkeys,ccq:default2keys,s:2,d:1,e:1,u:13,g:80,m:1,ops:5000",
                                  "b:saslbucket1,pwd:password,coq:saslkeys,ccq:sasl11keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:5000",
                                  "b:saslbucket2,pwd:password,coq:sas2keys,ccq:sasl12keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:5000",
                                  "b:saslbucket3,pwd:password,coq:sas3keys,ccq:sasl13keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:5000",
                                  "b:saslbucket4,pwd:password,coq:sas4keys,ccq:sasl14keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:5000",
                                  "b:saslbucket5,pwd:password,coq:sas5keys,ccq:sasl15keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:5000",
                                  "b:saslbucket6,pwd:password,coq:sas6keys,ccq:sasl16keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:5000",
                                  "b:saslbucket7,pwd:password,coq:sas7keys,ccq:sasl17keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:5000",
                                   "b:saslbucket,pwd:password,coq:sas0keys,ccq:sasl10keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:5000"],
                    "runtime" : 7200
                    }
    }
}
