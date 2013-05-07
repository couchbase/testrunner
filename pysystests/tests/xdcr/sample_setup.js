{
    "name" : "xdcr_example",
    "desc" : "example xdcr test",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "rebalance_in",
                    "desc" :  "rebalance_in",
                    "cluster" : {"add" : "3"}},
                "1" :
                {
                    "name" : "create_local_buckets",
                    "desc" :  "create_local_buckets",
                    "buckets" : {"default" : {"quota": "256", "replicas": "1",
                                              "replica_index": "1"},
                                 "sasl": {"count": "1", "quota": "256", "replicas": "1",
                                          "replica_index": "0"}
                                }},

                "2" :
                {
                    "name" : "create_remote_buckets",
                    "desc" :  "create_remote_buckets",
                    "buckets" : {"default" : {"quota": "256", "replicas": "1",
                                              "replica_index": "1"},
                                 "sasl": {"count": "1", "quota": "256", "replicas": "1",
                                          "replica_index": "0"},
                                 "remote" : "remote1"
                                }},

                "3":
                {
                    "name" : "create_design_docs",
                    "desc" :  "create_design_docs",
                    "ddocs" : [{"create" :
                                 [{"ddoc":"ddoc1",
                                   "view":"local_default_view1",
                                   "map":"function(doc){emit(doc.key,doc.key_num);}",
                                   "bucket":"default"},

                                  {"ddoc":"ddoc2",
                                   "view":"local_sasl_view1",
                                   "map":"function(doc){emit(doc.key,doc.key_num);}",
                                   "bucket":"saslbucket"}]},

                               {"remote" : "remote1",
                                "create" :
                                [{"ddoc":"ddoc1",
                                  "view":"remote_default_view1",
                                  "map":"function(doc){emit(doc.key,doc.key_num);}",
                                  "bucket":"default"},
                                 {"ddoc":"ddoc2",
                                  "view":"remote_sasl_view1",
                                  "map":"function(doc){emit(doc.key,doc.key_num);}",
                                  "bucket":"saslbucket"}]}],
                    "runtime" : 10 },
                "4" :
                {
                    "name" : "pair_sites",
                    "desc" :  "pair_sites",
                    "xdcr" : {
                              "dest_cluster_name" : "remote1",
                              "dest_cluster_rest_username" : "Administrator",
                              "dest_cluster_rest_pwd" : "asdasd",
                              "replication_type" : "unidirection",
                              "buckets" : ["default"]
                             }}
        }
}
