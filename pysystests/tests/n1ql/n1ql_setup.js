{
    "name" : "n1ql_setup",
    "desc" : "n1ql_setup",
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
        "buckets" : {"default" : {"quota": "2000", "replicas": "1", "replica_index": "1", "priority": "low", "eviction_policy": "fullEviction"},
        "tpcc": {"count": "1", "quota": "1500", "replicas": "1",
            "replica_index": "1", "priority": "high", "eviction_policy": "fullEviction"}
    }
    },
    "2" :
    {
        "name" : "create_tpcc_indexes",
        "desc" :  "create tpcc indexes",
        "ssh"  : {"hosts"    : ["127.0.0.1"],
        "username" : "root",
        "password" : "couchbase",
        "command"  : "python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/create_index.py -i gsi  -bucket tpcc -q 127.0.0.1"
    }
    },
    "3" :
    {
        "name" : "create_sabre_indexes",
        "desc" :  "create tpcc indexes",
        "ssh"  : {"hosts"    : ["127.0.0.1"],
        "username" : "root",
        "password" : "couchbase",
        "command"  : "python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/create_index.py -i gsi  -bucket sabre -q 127.0.0.1"
    }
    }
}
}

