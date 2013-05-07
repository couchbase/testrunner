Creating PDF reports on local machine
-------------------------------------

Additional requirements
=======================

* CouchDB
* couchdbkit
* R

Guide
=====

1. Run performance test(s) with `stats=1` option.
2. After the test you will have multiple *json.gz archives in root testrunner directory, aggregate them:

        python -m pytests.performance.do_cluster -i {ini_file} -c {test_conf} -p num_clients={num_clients} tearDown

3. As a result you can find multiple final*.json files in the same folder. Post them one by one to CouchDB:

        python -m scripts.post_perf_data -n http://10.5.2.41:5984 -d eperf -i {filename}

10.5.2.41:5984 is an xisting internal CouchDB instance.
Feel free to use your own database, in this case the following view is required:

        {
            "map": "function(doc) {
                var arr = new Array();
                arr[0] = doc.buildinfo.version;
                arr[1] = doc.name;
                arr[2] = doc.info.test_name;
                arr[3] = doc.time;
                arr[4] = doc.info.json;
                if (doc.info && doc.info.reb_start) {
                    arr[5] = doc.info.reb_start;
                } else {
                    arr[5] = 0
                }
                if (doc.info && doc.info.reb_dur) {
                    arr[6] = doc.info.reb_dur;
                } else {
                    arr[6] = 0;
                }
                if (doc.info && doc.info.testrunner) {
                    arr[7] = doc.info.testrunner;
                } else {
                    arr[7] = \"unknown version\";
                }
                if (doc.info && doc.info.cluster_name) {
                    arr[8] = doc.info.cluster_name;
                } else {
                    arr[8] = \"\";
                }
                emit(arr, doc.info.test_time);
            }
        }

Design document is "data", view name is "by_test_time"

4. Now you can run R script:

        Rscript resources/R/ep1.R {baseline_build} {target_build} {test_case}.{phase_name} 10.5.2.41 eperf

Build parameter should look like "2.0.0-1976-rel-enterprise" or "2.0.0-1976-rel-community".
Please use "-rel-community" suffix for toy builds. Both baseline and target are required (could be the same, though).

As a rule `test_case` parameter matches the name of test configuration file.

Phases are "warmup", "load", "index" or "loop".

5. Yes, this is weird legacy.

