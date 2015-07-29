{
    "name" : "n1ql_setup",
    "desc" : "Setup Cluster, Indexes",
    "loop" : "",
    "phases" : {
		"0" :
                {
                    "name" : "rebalance in all the nodes",
                    "desc" :  "rebalance in all nodes",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "python /root/systest-worker/testrunner/pysystests/tests/n1ql/rebalance_setup.py"
                             }
                },
		"1" :
                {
                    "name" : "change index settings",
                    "desc" :  "change index mem ",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "python /root/systest-worker/testrunner/pysystests/tests/n1ql/changeIndexersettings.py -idx 10.6.2.234"
                             }
                },
		"2" :
                {
                    "name" : "create_buckets",
                    "desc" :  "create buckets",
                    "buckets" : {"default" : {"quota": "8000", "replicas": "1", "replica_index": "1", "priority": "low", "eviction_policy": "fullEviction"},
                                 "tpcc": {"count": "1", "quota": "1000", "replicas": "2",
                                          "replica_index": "1", "priority": "high", "eviction_policy": "fullEviction"}
				}
                },
		"3" :
                {
                    "name" : "create indexes and replica indexes",
                    "desc" :  "create tpcc indexes",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "python /root/systest-worker/testrunner/pysystests/tests/n1ql/create_index.py -i gsi  -bucket tpcc -q 10.6.2.194 -idx 10.6.2.237 -rep_idx 10.6.2.238"
                             }
                },
		"4" :
                {
                    "name" : "create indexes and replica indexes",
                    "desc" :  "create default indexes",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "python /root/systest-worker/testrunner/pysystests/tests/n1ql/create_index.py -i gsi  -bucket default -q 10.6.2.194 -idx 10.6.2.233 -rep_idx 10.6.2.234"
                             }
                }
                }
}

