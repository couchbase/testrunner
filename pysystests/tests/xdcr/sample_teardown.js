{
    "name" : "xdcr_teardown",
    "desc" : "teardown xdcr test",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "local_teardown",
                    "desc" :  "local_teardown",
                    "teardown" : {"ddocs" : ["default/ddoc1",
                                             "saslbucket/ddoc2"],
                                  "buckets" : ["default",
                                               "saslbucket"],
                                  "xdcr_dest_clusters" : ["remote1"]}
                },
                "1" :
                {
                    "name" : "remote_teardown",
                    "desc" :  "remote_teardown",
                    "teardown" : {"remote" : "remote1",
                                  "ddocs" : ["default/ddoc1",
                                             "saslbucket/ddoc2"],
                                  "buckets" : ["default",
                                               "saslbucket"]}
                },
                "2" :
                {
                    "name" : "rebalance_out",
                    "desc" :  "rebalance_out",
                    "cluster" : {"rm" : "3"}}

        }
}
