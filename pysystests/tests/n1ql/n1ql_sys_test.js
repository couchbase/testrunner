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
                              "command"  : "cd  ~/details/cwpy ; python ./tpcc.py --duration 7200 --client 100 --warehouses 100 --client 100 --no-execute --debug n1ql >> /tmp/noload.output&"
		   	     }
                },
                "1" :
                {
                    "name" : "load_sabre",
                    "desc" :  "load data externally",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  /root/n1ql_sysTest/testrunner ; python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/insert_query.py -doc 5000000 -q 10.6.2.164"
                             }
		        },
                "2" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 10",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  ~/details/cwpy ; python ./tpcc.py --duration 1200 --warehouses 100 --client 100 --no-load --debug n1ql&;cd  /root/n1ql_sysTest/testrunner; python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/dml_sabre.py -t 1200 -c 1 -q 10.6.2.164&"
                             },
		    "runtime" : 1200
                },
                "3" :
                {
                    "name" : "rebalance_in_one",
                    "desc" :  "RB-1",
                    "cluster" :  {"add" : "1"},
		    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  ~/details/cwpy ; python ./tpcc.py --duration 1000 --warehouses 100 --client 100 --no-load --debug n1ql&;cd  /root/n1ql_sysTest/testrunner; python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/dml_sabre.py -t 1200 -c 100 -q 10.6.2.164&"
                             }
                },
		"4" :
                {
                    "name" : "drain_disks_for_reb",
                    "desc" :  "drain_disks",
                    "workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:1000",
                                  "conditions" : "post:ep_queue_size < 100000"}
                                  ]

                },
		"5" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 10",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  ~/details/cwpy ; python ./tpcc.py --duration 1000 --warehouses 100 --client 100 --no-load --debug n1ql&;cd  /root/n1ql_sysTest/testrunner; python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/dml_sabre.py -t 1200 -c 100 -q 10.6.2.164&"
                             },
                    "runtime" : 1200
                },
                "6" :
                {
                    "name" : "swap_non-orchestrator",
                    "desc" :  "RB-2",
                    "cluster" :  {"add": "1", "rm": "1", "orchestrator": "False"}
                },
		"7" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 10",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  ~/details/cwpy ; python ./tpcc.py --duration 1000 --warehouses 100 --client 100 --no-load --debug n1ql&;cd  /root/n1ql_sysTest/testrunner; python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/dml_sabre.py -t 1200 -c 100 -q 10.6.2.164&"
                             },
                    "runtime" : 1200
                },
		"8" :
                {
                    "name" : "rebalance_out_one",
                    "desc" :  "RB-1",
                    "cluster" :  {"rm" : "1", "orchestrator": "False"},
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  ~/details/cwpy ; python ./tpcc.py --duration 1000 --warehouses 100 --client 100 --no-load --debug n1ql&;cd  /root/n1ql_sysTest/testrunner; python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/dml_sabre.py -t 1000 -c 100 -q 10.6.2.164&"
                             }
                },
		"9" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 10",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  ~/details/cwpy ; python ./tpcc.py --duration 1000 --warehouses 100 --client 100 --no-load --debug n1ql&;cd  /root/n1ql_sysTest/testrunner; python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/dml_sabre.py -t 1200 -c 100 -q 10.6.2.164&"
                             },
                    "runtime" : 1200
                },
		"10" :
    		{		
        		"name" : "failover_one",
        		"desc" : "FL-1",
       			"cluster" : {"auto_failover" : "1", "add_back": "1"}
    		},
		"11" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 10",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  ~/details/cwpy ; python ./tpcc.py --duration 1000 --warehouses 100 --client 100 --no-load --debug n1ql&;cd  /root/n1ql_sysTest/testrunner; python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/dml_sabre.py -t 1200 -c 100 -q 10.6.2.164&"
                             },
                    "runtime" : 1200
                },
		"12" :
    		{		
        		"name" : "restart_one_no_load",
        		"desc" :  "CR-1",
        		"workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:0",
        		"conditions" : {"post": {"conditions": "ep_warmup_thread = complete", "cluster_check": "True"}}}
        		]		,
        		"cluster" :  {"soft_restart" : "1"}
    		},
		"13" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 10",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  ~/details/cwpy ; python ./tpcc.py --duration 1000 --warehouses 100 --client 100 --no-load --debug n1ql&;cd  /root/n1ql_sysTest/testrunner; python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/dml_sabre.py -t 1000 -c 100 -q 10.6.2.164&"
                             },
                    "runtime" : 1200
                },
		"14" :
    		{
        		"name" : "restart_one_with_load",
        		"desc" :  "CR-2",
        		"workload" : [{"spec": "s:3,u:22,g:70,d:3,e:2,m:5,ttl:3000,coq:defaultph1keys,ccq:defaultph2keys,ops:15000",
        		"conditions" : {"post": {"conditions": "ep_warmup_thread = complete", "cluster_check": "True"}}}],
       			"cluster" :  {"soft_restart" : "1"}
    		},
		"15" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 10",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  ~/details/cwpy ; python ./tpcc.py --duration 1000 --warehouses 100 --client 100 --no-load --debug n1ql&;cd  /root/n1ql_sysTest/testrunner; python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/dml_sabre.py -t 1200 -c 100 -q 10.6.2.164&"
                             },
                    "runtime" : 1200
                },
		"16" :
    		{
        		"name" : "restart_all",
        		"desc" :  "CR-3",
        		"workload" : [{"spec" : "g:100,coq:defaultph2keys,ops:0",
        		"conditions" : {"post": {"conditions": "ep_warmup_thread = complete", "cluster_check": "True"}}}],
        		"cluster" : {"soft_restart" : "8"}
    		},
		"17" :
                {
                    "name" : "access_phase",
                    "desc" :  "run access phase for 10",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "cd  ~/details/cwpy ; python ./tpcc.py --duration 1000 --warehouses 100 --client 100 --no-load --debug n1ql&;cd  /root/n1ql_sysTest/testrunner; python /root/n1ql_sysTest/testrunner/pysystests/tests/n1ql/dml_sabre.py -t 1000 -c 100 -q 10.6.2.164&"
                             },
                    "runtime" : 60
                }
        }
}
