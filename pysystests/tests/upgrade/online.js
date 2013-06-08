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
                    "name" : "rebalance_out_access",
                    "desc" :  "rebalance_out_access",
                    "cluster" : {"rm" : "10.3.3.60 10.3.3.66"},
                    "workload" : ["b:default,coq:defaultkeys,ccq:default2keys,s:2,d:1,e:1,u:13,g:80,m:1,ops:10000",
                                  "b:saslbucket1,pwd:password,coq:saslkeys,ccq:sasl7keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket2,pwd:password,coq:sas2keys,ccq:sasl8keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket,pwd:password,coq:sas3keys,ccq:sasl9keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000"]
                },
                "2" :
                {
                    "name" : "stop_servers",
                    "desc" :  "stop_servers",
                    "ssh"  : {"hosts"    : ["10.3.3.60", "10.3.3.66"],
                              "username" : "root",
                              "password" : "password",
                              "command"  : "/etc/init.d/couchbase-server stop"},
                    "runtime" : 30
                },
                "3" :
                {
                    "name" : "upgrade01",
                    "desc" :  "upgrade01",
                    "workload" : ["b:default,coq:defaultkeys,ccq:default2keys,s:2,d:1,e:1,u:13,g:80,m:1,ops:10000",
                                  "b:saslbucket1,pwd:password,coq:saslkeys,ccq:sasl7keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket2,pwd:password,coq:sas2keys,ccq:sasl8keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket,pwd:password,coq:sas3keys,ccq:sasl9keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000"],
                    "ssh"  : {"hosts"    : ["10.3.3.60", "10.3.3.66"],
                              "username" : "root",
                              "password" : "password",
                              "command"  : "dpkg -i /root/couchbase-server-enterprise_x86_64_2.1.0-702-rel.deb"}
                },
                "4" :
                {
                    "name" : "swap_rb01",
                    "desc" :  "swap_rebalance",
                    "cluster" : {"rm" : "10.3.121.90 10.3.121.91", "add" : "10.3.3.60 10.3.3.66"},
                    "workload" : ["b:default,coq:defaultkeys,ccq:default2keys,s:2,d:1,e:1,u:13,g:80,m:1,ops:10000",
                                  "b:saslbucket1,pwd:password,coq:saslkeys,ccq:sasl7keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket2,pwd:password,coq:sas2keys,ccq:sasl8keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket,pwd:password,coq:sas3keys,ccq:sasl9keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000"]
                },
                "5" :
                {
                    "name" : "stop_servers",
                    "desc" :  "stop_servers",
                    "ssh"  : {"hosts"    : ["10.3.121.90", "10.3.121.91"],
                              "username" : "root",
                              "password" : "password",
                              "command"  : "/etc/init.d/couchbase-server stop"},
                    "runtime" : 30
                },
                "6" :
                {
                    "name" : "upgrade02",
                    "desc" :  "upgrade02",
                    "workload" : ["b:default,coq:defaultkeys,ccq:default2keys,s:2,d:1,e:1,u:13,g:80,m:1,ops:10000",
                                  "b:saslbucket1,pwd:password,coq:saslkeys,ccq:sasl7keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket2,pwd:password,coq:sas2keys,ccq:sasl8keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket,pwd:password,coq:sas3keys,ccq:sasl9keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000"],
                    "ssh"  : {"hosts"    : ["10.3.121.90", "10.3.121.91"],
                              "username" : "root",
                              "password" : "password",
                              "command"  : "dpkg -i /root/couchbase-server-enterprise_x86_64_2.1.0-702-rel.deb"}
                 },
                "7" :
                {
                    "name" : "swap_rb02",
                    "desc" :  "swap_rebalance02",
                    "cluster" : {"rm" : "10.3.3.60", "add" : "10.3.121.90 10.3.121.91"},
                    "workload" : ["b:default,coq:defaultkeys,ccq:default2keys,s:2,d:1,e:1,u:13,g:80,m:1,ops:10000",
                                  "b:saslbucket1,pwd:password,coq:saslkeys,ccq:sasl7keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket2,pwd:password,coq:sas2keys,ccq:sasl8keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket,pwd:password,coq:sas3keys,ccq:sasl9keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000"]
                },
                "8" :
                {
                    "name" : "stop_servers",
                    "desc" :  "stop_servers",
                    "ssh"  : {"hosts"    : ["10.3.3.60"],
                              "username" : "root",
                              "password" : "password",
                              "command"  : "/etc/init.d/couchbase-server stop"},
                    "runtime" : 30
                },
                "9" :
                {
                    "name" : "upgrade03",
                    "desc" :  "upgrade03",
                    "workload" : ["b:default,coq:defaultkeys,ccq:default2keys,s:2,d:1,e:1,u:13,g:80,m:1,ops:10000",
                                  "b:saslbucket1,pwd:password,coq:saslkeys,ccq:sasl7keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket2,pwd:password,coq:sas2keys,ccq:sasl8keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket,pwd:password,coq:sas3keys,ccq:sasl9keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000"],
                    "ssh"  : {"hosts"    : ["10.3.3.60"],
                              "username" : "root",
                              "password" : "password",
                              "command"  : "dpkg -i /root/couchbase-server-enterprise_x86_64_2.1.0-702-rel.deb"}
                 },
                "10" :
                {
                    "name" : "access_phase",
                    "desc" :  "post_upgrade_access",
                    "workload" : ["b:default,coq:defaultkeys,ccq:default2keys,s:2,d:1,e:1,u:13,g:80,m:1,ops:10000",
                                  "b:saslbucket1,pwd:password,coq:saslkeys,ccq:sasl7keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket2,pwd:password,coq:sas2keys,ccq:sasl8keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000",
                                  "b:saslbucket,pwd:password,coq:sas3keys,ccq:sasl9keys,m:1,s:2,d:1,e:1,u:13,g:80,ops:4000"],
                    "runtime" : 7200
                 }
    }
}
