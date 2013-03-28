{
    "name" : "indexer",
    "desc" : "test indexer progress postconditions",
    "loop" : false,
    "phases" : {
               "0" :
                {
                    "name" : "create_ddoc",
                    "ddocs" : {"create" : [{"ddoc": "ddoc1", "view": "view1", "bucket": "default", "dev": false, "map": "function (doc) { emit(doc.st, null);}"}]}
                },
               "1" :
                {
                    "name" : "load till indexer starts",
                    "workload" : [{"spec" : "s:100,ops:5000",
                                  "conditions" : {"post": {"type":"indexer", "target":"_design/ddoc1", "conditions": "progress > 0"}}}]
                },
               "2" :
                {
                    "name" : "wait for indexing building",
                    "workload" : [{"spec" : "ops:0",
                                  "conditions" : {"post": {"type":"indexer", "target":"_design/ddoc1", "conditions": "progress > 99"}}}]
                }
        }
}
