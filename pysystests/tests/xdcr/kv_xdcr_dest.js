{
    "name" : "kv_xdcr_linux__dest",
    "desc" : "kv_xdcr_linux_dest",
    "loop" : "",
    "phases" : {
                "0" :
                 {
                    "name" : "pair_sites_for_standardbucket",
                    "desc" :  "set_replication_type_for_standardbucket",
                    "xdcr" : {
                              "dest_cluster_name" : "source",
                              "dest_cluster_rest_username" : "Administrator",
                              "dest_cluster_rest_pwd" : "password",
                              "replication_type" : "unidirection",
                              "buckets" : ["standardbucket"]
                             }
                 },
                "1" :
                 {
                    "name" : "load_dgm",
                    "desc" :  "load_hotset",
                    "workload" : [{"spec" : "b:standardbucket,t:template512,s:100,e:50,ttl:86400,ccq:std1ph5keys,ops:60000",
                                  "conditions" : "post:vb_active_resident_items_ratio < 50"}]
                 },
                "2" :
                 {
                    "name" : "access_phase",
                    "desc" :  "post_upgrade_access",
                    "workload" : ["b:standardbucket,coq:defaultkeys,ccq:std1ph5keys,d:50,g:50,ops:10000",
                                  "b:standardbucket1,coq:defaultkeys,ccq:std2sph5keys,g:50,d:50,ops:10000",
                                  "b:saslbucket,pwd:password,ccq:saslph5keys,g:100,ops:10000"],
                    "runtime" : 10800
                 }
               }
}
