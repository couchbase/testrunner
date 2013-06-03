{
    "name" : "ssh_sample",
    "desc" : "ssh sample",
    "loop" : false,
    "phases" : {
                "0" :
                {
                    "name" : "touchfile",
                    "desc" :  "make a sample file",
                    "ssh"  : {"hosts"    : ["10.3.2.8","10.3.2.9"],
                              "username" : "root",
                              "password" : "couchbase",
                              "command"  : "touch /tmp/newfile"}
                }
        }
}
