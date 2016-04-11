{
    "name" : "backup_linux",
    "desc" : "backup_linux",
    "loop" : "",
    "phases" : {
                "0" :
                {
                    "name" : "load_init",
                    "desc" :  "load_initial",
                    "workload" : [{"spec" : "b:default,s:100,ccq:defkeys,ops:40000"},
                                  {"spec" : "b:standardbucket1,s:100,e:90,ttl:21600,ccq:b0keys,ops:10000"}],
                    "runtime" : 60
                },
                "1" :
                {
                    "name" : "load1",
                    "desc" :  "load1",
                    "workload" : [{"spec" : "b:default,s:100,ccq:defkeys,ops:40000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 90"},
                                  {"spec" : "b:standardbucket1,s:100,e:90,ttl:21600,ccq:b0keys,ops:10000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 70"}]

                },
                "2" :
                {
                    "name" : "create_backup",
                    "desc" : "Create Backup",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "python /root/systest-worker/testrunner/pysystests/tests/ent_backup_restore/backup.py create --repo backup1"
                             }
                },
                "3" :
                {
                    "name" : "backup1",
                    "desc" : "Take backup",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "python /root/systest-worker/testrunner/pysystests/tests/ent_backup_restore/backup.py backup --repo backup1 --host 10.6.2.238"
                             }
                },
                "4" :
                {
                    "name" : "load2",
                    "desc" :  "load2",
                    "workload" : [{"spec" : "b:default,s:100,ops:40000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 70"},
                                 {"spec" : "b:standardbucket1,s:100,e:90,ttl:21600,ops:10000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 50"}]

                },
                "5" :
                {
                    "name" : "backup2",
                    "desc" : "Take backup",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "python /root/systest-worker/testrunner/pysystests/tests/ent_backup_restore/backup.py backup --repo backup1 --host 10.6.2.238"
                             }
                },
                "6" :
                {
                    "name" : "rebalance_in",
                    "desc" :  "rebalance_in",
                    "cluster" : {"add" : "2"}
                },
                "7" :
                {
                    "name" : "load3",
                    "desc" :  "load3",
                    "workload" : [{"spec" : "b:default,s:100,ops:40000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 70"},
                                 {"spec" : "b:standardbucket1,s:100,e:90,ttl:21600,ops:10000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 50"}]

                },
                "8" :
                {
                    "name" : "backup3",
                    "desc" : "Take backup",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "python /root/systest-worker/testrunner/pysystests/tests/ent_backup_restore/backup.py backup --repo backup1 --host 10.6.2.238"
                             }
                },
                "9" :
                {
                    "name" : "reb_out_two",
                    "desc" :  "RB-2",
                    "workload" : [{"spec" : "s:10,u:5,g:80,d:5,ops:40000"},
                                  {"spec" : "b:standardbucket1,s:3,u:22,g:70,d:3,e:2,ttl:3000,ops:1000"},
                                  {"spec" : "b:standardbucket3,s:5,u:5,g:20,d:10,ops:10000"}],
                    "cluster" :  {"rm" : "2"}
                },
                "10" :
                {
                    "name" : "load4",
                    "desc" :  "load4",
                    "workload" : [{"spec" : "b:default,s:100,ops:40000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 70"},
                                 {"spec" : "b:standardbucket1,s:100,e:90,ttl:21600,ops:10000",
                                   "conditions" : "post:vb_active_resident_items_ratio < 50"}]

                },
                "11" :
                {
                    "name" : "backup3",
                    "desc" : "Take backup",
                    "ssh"  : {"hosts"    : ["10.1.2.80"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "python /root/systest-worker/testrunner/pysystests/tests/ent_backup_restore/backup.py backup --repo backup1 --host 10.6.2.238"
                             }
                },
                "12" :
                {
                    "name" : "local_teardown",
                    "desc" :  "local_teardown",
                    "teardown" : {"buckets" : ["default", "standardbucket1"]}
                }
    }

}