{
    "name" : "n1ql_gsi_test",
    "desc" : "test run for n1ql and gsi",
    "loop" : "",
    "phases" : {
                "3" :
                {
                    "name" : "swap_rebalance",
                    "desc" :  "RB-1",
                    "cluster" :  {"add" : "10.6.2.237", "services":"index", "rm" : "10.6.2.194"}
                }
                }
}
