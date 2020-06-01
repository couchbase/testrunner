{
    "name" : "n1ql_gsi_test",
    "desc" : "test run for n1ql and gsi",
    "loop" : "",
    "phases" : {
	        "0" :
                {
                    "name" : "load_tpcc",
                    "desc" :  "insert data for tpcc buckets",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy ; python ./tpcc.py --duration 15000 --client 100 --warehouses 200 --no-execute --debug n1ql >> /tmp/noload.output&"
                             }
                },
                "1" :
                {
                    "name" : "load_default",
                    "desc" :  "insert data for default bucket",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  /root/systest-worker/testrunner ; python /root/systest-worker/testrunner/pysystests/tests/n1ql/insert_query.py -doc 20000000 -q 10.6.2.194 -b default -z 1000"
                             }
                },
	        "2" :
                {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 30 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 100 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
		        "3" :
                {
                    "name" : "rebalance_out_kv",
                    "desc" :  "RB-2",
                    "cluster" :  {"rm" : "10.6.2.167"}
                },
                "4" :
                {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 60 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
                "5" : 
                {
                    "name" : "rebalance_in_kv",
                    "desc" :  "RB-1",
                    "cluster" :  {"add" : "10.6.2.167", "services" : "kv"}
                },
	        "6" :
		        {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 30 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
                "7" :
                {
                    "name" : "rebalance_out_index",
                    "desc" :  "RB-2",
                    "cluster" :  {"rm" : "10.6.2.233"}
                },
		        "8" :
                {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 60 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
	        "9" :
                {
                    "name" : "rebalance_in_index",
                    "desc" :  "RB-1",
                    "cluster" :  {"add" : "10.6.2.233", "services" : "index"}
                },
		"10" :
                {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 30 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
		"11" :
                {
                    "name" : "rebalance_out_query",
                    "desc" :  "RB-1",
                    "cluster" :  {"rm" : "10.6.2.195"}
                },
                "12" :
                {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 60 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
                "13" :
                {
                    "name" : "rebalance_in_query",
                    "desc" :  "RB-1",
                    "cluster" :  {"add" : "10.6.2.195", "services" : "n1ql"}
                },
		"14" :
                {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 30 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
		"15" :
                {
                    "name" : "failover_kv_add_back",
                    "desc" :  "FL-1",
                    "cluster" : {"auto_failover" : "10.6.2.232", "add_back" : "1" }
                },
		"16" :
                {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 30 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
		"17" :
                {
                    "name" : "failover_kv",
                    "desc" :  "FL-2",
                    "cluster" : {"auto_failover" : "10.6.2.232"}
	        },
	        "18" :
                {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 30 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
	        "19" :
                {
                    "name" : "soft_restart_index",
                    "desc" :  "SR-1",
                    "cluster" : {"soft_restart" : "10.6.2.234" }
                },
                "20" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 60 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
                "21" :
                {
                    "name" : "soft_restart_all",
                    "desc" :  "SR-2",
                    "cluster" : {"soft_restart" : "8" }
                },
		"22" :
                {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 30 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
                "23" :
                {
                    "name" : "hard_restart_some",
                    "desc" :  "SR-3",
                    "cluster" : {"hard_restart" : "3" }
                },
		"24" :
                {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 30 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                },
                "25" :
                {
                    "name" : "hard_restart_all",
                    "desc" :  "SR-3",
                    "cluster" : {"hard_restart" : "3" }
                },
		"26" :
                {
                    "name" : "query_phase",
                    "desc" :  "run access phase for 30 seconds",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd /root/details/cwpy; python ./tpcc.py --warehouses 50 --client 100 --no-load --debug n1ql"
                             },
                    "runtime" : 1800
                }
	     }
}
