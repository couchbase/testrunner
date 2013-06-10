{
    "name" : "online upgrade",
    "desc" : "online upgrade test",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "multi_bucket",
                    "desc" :  "load data at 10k ops ",
                    "workload" : [{"spec" : "s:100,ccq:defaultkeys,ops:10000",
                                  "conditions" : "post:curr_items>4000000"},
                                  {"spec" : "b:saslbucket1,pwd:password,ccq:saslkeys,s:100,ops:4000",
                                  "conditions" : "post:curr_items>2000000"},
                                  {"spec" : "b:saslbucket2,pwd:password,ccq:sas2keys,s:100,ops:4000",
                                  "conditions" : "post:curr_items>2000000"},
                                  {"spec" : "b:saslbucket,pwd:password,ccq:sas3keys,s:100,ops:4000",
                                  "conditions" : "post:curr_items>2000000"}]
                    },
                "1" :
                {
                    "name" : "pre_access_phase",
                    "desc" :  "pre_upgrade_access",
                    "workload" : ["b:default,coq:defaultkeys,ccq:default2keys,s:2,d:1,e:1,u:13,g:80,m:1,ops:10000",
                                  "b:saslbucket1,pwd:password,coq:saslkeys,ccq:sasl7keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket2,pwd:password,coq:sas2keys,ccq:sasl8keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket,pwd:password,coq:sas3keys,ccq:sasl9keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000"],
                    "runtime" : 7200
                 },
                "2" :
                {
                    "name" : "stop_servers",
                    "desc" :  "stop_servers",
                    "ssh"  : {"hosts"    : ["10.3.3.60", "10.3.3.66", "10.3.121.90", "10.3.121.91", "10.3.3.69"],
                              "username" : "root",
                              "password" : "password",
                              "command"  : "/etc/init.d/couchbase-server stop"},
                    "runtime" : 30
                },
                "3" :
                {
                    "name" : "offline_upgrade",
                    "desc" :  "offline_upgrade",

                    "ssh"  : {"hosts"    : ["10.3.3.60", "10.3.3.66", "10.3.121.90", "10.3.121.91", "10.3.3.69"],
                              "username" : "root",
                              "password" : "password",
                              "command"  : "dpkg -i /root/couchbase-server-enterprise_x86_64_2.1.0-702-rel.deb"}
                },
                "4" :
                {
                    "name" : "waitforwarmup",
                    "desc" : "waitforwarmup",
                    "workload" : {"spec" : "ops:0",
                                  "conditions" : {"post": {"conditions": "ep_warmup_thread = complete", "ip": "10.3.3.60"}}}
                },
                "5" :
                {
                    "name" : "post_access_phase",
                    "desc" :  "post_upgrade_access",
                    "workload" : ["b:default,coq:defaultkeys,ccq:default2keys,s:2,d:1,e:1,u:13,g:80,m:1,ops:10000",
                                  "b:saslbucket1,pwd:password,coq:saslkeys,ccq:sasl7keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket2,pwd:password,coq:sas2keys,ccq:sasl8keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket,pwd:password,coq:sas3keys,ccq:sasl9keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000"],
                    "cluster" : {"rm" : "10.3.121.90"},
                    "runtime" : 7200
                 },
                "6" :
                {
                    "name" : "swap_rb01",
                    "desc" :  "swap_rebalance",
                    "cluster" : {"rm" : "10.3.3.60", "add" : "10.3.121.90"},
                    "workload" : ["b:default,coq:defaultkeys,ccq:default2keys,s:2,d:1,e:1,u:13,g:80,m:1,ops:10000",
                                  "b:saslbucket1,pwd:password,coq:saslkeys,ccq:sasl7keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket2,pwd:password,coq:sas2keys,ccq:sasl8keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket,pwd:password,coq:sas3keys,ccq:sasl9keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000"]
                }
    }
}
