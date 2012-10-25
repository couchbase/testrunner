{
    "name" : "rebalance",
    "desc" : "simple rebalance test",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "simple_load",
                    "desc" :  "load items at 1k ops for 40s",
                    "workload" : "s:100,ccq:simplekeys,ops:1000",
                    "runtime" : 40
                    },
                "1" :
                {
                    "name" : "rebalance_in_access",
                    "desc" :  "access items at 1k ops with 80% gets",
                    "workload" : "s:15,g:80,d:5,ccq:simplekeys,ops:1000",
                    "cluster" :  {"add" : "192.168.1.133:9001 192.168.1.133:9002" }
                    },
                "2" :
                {
                    "name" : "rebalance_out_access",
                    "desc" :  "access items at 1k ops with 80% gets",
                    "workload" : "s:15,g:80,d:5,ccq:simplekeys,ops:1000",
                    "cluster" :  {"rm" : "192.168.1.133:9001" }
                    },
                "3" :
                {
                    "name" : "swap_no_ops",
                    "desc" :  "swap rebalance without any ops",
                    "cluster" : {"rm" : "192.168.1.133:9002",
                                 "add" : "192.168.1.133:9001"}
                     }
        }
}
