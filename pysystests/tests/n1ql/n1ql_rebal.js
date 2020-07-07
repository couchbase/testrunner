{
    "name" : "n1ql_gsi_test",
    "desc" : "rebalance test run for n1ql and gsi",
    "loop" : "",
    "phases" : {
		        "0" :
                {
                    "name" : "bucket_warmup",
                    "desc" :  "bucket_warmup",
                    "workload" : [{"spec" : "s:100,ops:1000"}],
                    "runtime" : 40 
		        },
		        "1" :
                {
                    "name" : "rebalance_out_index",
                    "desc" :  "RB-2",
                    "cluster" :  {"rm" : "10.6.2.167"}
                },
                "2" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 60 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 100 --client 1 --no-load --debug n1ql"
                             },
                    "runtime" : 30
                },
                "3" : 
                {
                    "name" : "rebalance_in_index",
                    "desc" :  "RB-1",
                    "cluster" :  {"add" : "10.6.2.167", "services" : "index"}
                },
                "4" :
                {
                    "name" : "rebalance_out_index",
                    "desc" :  "RB-2",
                    "cluster" :  {"rm" : "10.6.2.233"}
                },
		        "5" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 60 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 100 --client 1 --no-load --debug n1ql"
                             },
                    "runtime" : 30
                },
		        "6" :
                {
                    "name" : "rebalance_in_index",
                    "desc" :  "RB-1",
                    "cluster" :  {"add" : "10.6.2.233", "services" : "index"}
                },
		        "7" :
                {
                    "name" : "rebalance_out_query",
                    "desc" :  "RB-1",
                    "cluster" :  {"rm" : "10.6.2.195"}
                },
                "8" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 60 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 100 --client 1 --no-load --debug n1ql"
                             },
                    "runtime" : 30
                },
                "9" :
                {
                    "name" : "rebalance_in_query",
                    "desc" :  "RB-1",
                    "cluster" :  {"add" : "10.6.2.195", "services" : "n1ql"}
                }
	       }
}
