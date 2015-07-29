{
    "name" : "n1ql_2i_test",
    "desc" : "failover test run for n1ql and 2i",
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
