{
    "name" : "view_setup",
    "desc" : "view_setup",
    "loop" : "",
    "phases" : {
                "0" :
                {
                    "name" : "rebalance_in",
                    "desc" :  "rebalance_in",
                    "cluster" : {"add" : "5"}
                },
                "1" :
                {
                    "name" : "create_buckets",
                    "desc" :  "create buckets",
                    "buckets" : {"default" : {"quota": "10000", "replicas": "1",
					      "replica_index": "0", "priority": "high", "eviction_policy": "fullEviction"},
                                 "sasl": {"count": "1", "quota": "8000", "replicas": "1", 
                                          "replica_index": "1", "priority": "low", "eviction_policy": "fullEviction"}},
                    "ddocs" : {"create": [{"ddoc":"ddoc1", "view":"view1", "map":"function(doc){emit(doc.key,doc.key_num);}", "bucket":"default"},
                                          {"ddoc":"ddoc1", "view":"view2", "map":"function(doc,meta){emit(meta.id,doc.key);}", "bucket":"default"},
                                          {"ddoc":"ddoc2", "view":"view1", "map":"function(doc){emit(doc.key,doc.key_num);}", "bucket":"saslbucket"},
                                          {"ddoc":"ddoc2", "view":"view2", "map":"function(doc,meta){emit(meta.id,doc.key);}", "bucket":"saslbucket"}]}
                }
               }
}
