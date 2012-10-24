{
    "name" : "kvswap",
    "desc" : "kv swap rebalance test",
    "loop " : true,
    "phases" : {
                "0" :
                {
                    "name" : "load_phase1",
                    "desc" :  "load 5M items",
                    "workload" : "s:100,ops:30000,post:count=5000000",
                    "template" : "default",
                    "query" : null,
                    "admin" : null },

                "1" :
                {
                    "name" : "access_phase1",
                    "desc" : "access until 2M items created and do rebalance out",
                    "workload" : "s:20,g:60,u:10,d:10,cc:ap,post:count=2000000",
                    "template" : "default",
                    "query" : null,
                    "admin" : { "rm" : ["10.3.2.104","10.3.2.105","10.3.2.106"] } },

                "2" :
                {
                    "name" : "load_phase2",
                    "desc" : "load 20M items",
                    "workload" : "s:100,ops:30000,post:count=20000000",
                    "template" : "default",
                    "query" : null,
                    "admin" : null },

                "3" :
                {
                    "name" : "access_phase2",
                    "desc" : "access until 2M items created and do swap rebalance",
                    "workload" : "s:20,g:60,u:10,d:10,cc:ap,post:count=2000000",
                    "template" : "default",
                    "query" : null,
                    "admin" : { "add" : ["10.3.2.104","10.3.2.105","10.3.2.106"],
                                "rm" : ["10.3.2.111","10.3.2.112","10.3.2.113"]} }

            }
}
