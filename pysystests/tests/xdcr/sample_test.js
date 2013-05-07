{
    "name" : "xdcr_example",
    "desc" : "example xdcr test",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "multi_bucket_multi_siteload",
                    "desc" :  "load items in multi_bucket_multiple_sites",
                    "workload" : [{"spec" : "s:100,ccq:sasl1,b:saslbucket,pwd:password,ops:2000",
                                    "conditions" : "post:count=200000",
                                    "remote" : "remote1"},
                                   {"spec" : "s:100,ccq:sasl2,b:saslbucket1,pwd:password,ops:3000",
                                    "conditions" : "post:count=300000",
                                   "remote" : "remote1"},
                                   {"spec" : "s:100,ccq:def1,ops:1000",
                                    "conditions" : "post:count=100000"},
                                   {"spec" : "s:100,ccq:sasl1,b:saslbucket,pwd:password,ops:2000",
                                    "conditions" : "post:count=200000"},
                                   {"spec" : "s:100,ccq:sasl2,b:saslbucket1,pwd:password,ops:3000",
                                    "conditions" : "post:count=300000"}],
                     "cache" : {"bucket" : "default",
                                "stat"  : "curr_items",
                                "reference"  : "phase2_default_curr_items"}
                    },
                "1" :
                {
                    "name" : "multi_bucket_multi_access",
                    "desc" :  "load items in multi_bucket_multiple_sites",
                    "workload" : [
                                   {"spec" : "ops:0","remote" : "remote1",
                                    "conditions" : "post:curr_items = $phase2_default_curr_items"},

                                   {"spec" : "s:30,u:15,g:50,d:2,ccq:sasl1,b:saslbucket,pwd:password,ops:2000", "remote" : "remote1"},

                                   {"spec" : "s:50,u:15,g:30,d:2,ccq:sasl2,b:saslbucket1,pwd:password,ops:3000", "remote" : "remote1"},

                                   {"spec" : "s:30,u:15,g:50,d:2,ccq:sasl1,b:saslbucket,pwd:password,ops:2000"},
                                   {"spec" : "s:50,u:15,g:30,d:2,ccq:sasl2,b:saslbucket1,pwd:password,ops:3000"}],


                    "runtime" : 120 }

        }
}
