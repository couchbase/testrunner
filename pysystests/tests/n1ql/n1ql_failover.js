{
    "name" : "n1ql_gsi_test",
    "desc" : "failover test run for n1ql and gsi",
    "loop" : "",
    "phases" : {
		        "1" :
                {
                    "name" : "failover_kv",
                    "desc" :  "FL-2",
                    "cluster" : {"auto_failover" : "10.6.2.167", "add_back" : "10.6.2.167"}
                }
	           }
}
