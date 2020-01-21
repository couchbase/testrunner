*** Test Runner Python3: Readme ***

Prerequisites
-------------

* Python 3.x or 3.6 or 3.7
* pip3 or easy_install

See the setup steps in the Py2 to Py3 porting guide: https://docs.google.com/document/d/1fxJjmgEnRKsq7l7h-nJbs9piZhqmvjNAUnXPSjrz-TA/edit?usp=sharing


Dependencies
------------

Use pip3 or pip3.6 or pip3.7 based on the platform.
General:

	pip3 install couchbase

	pip3 install sgmllib3k

	pip3 install paramiko

	pip3 install httplib2

	pip3 install pyyaml

	pip3 install Geohash

	pip3 install python-geohash

	pip3 install deepdiff

	pip3 install pyes

Other:

    pip3 install boto
    
    pip3 install boto3
	
	pip3 install botocore

Performance tests:

    pip3 install btrc

PDF reports:

    pip3 install couchdbkit

Documentation:

    pip3 install sphinx

    pip3 install sphinx-pypi-upload

Buildout:

    pip3 install zc.buildout

Usage
-----

    $ ./testrunner  -h
    Usage: testrunner [options]

    Options:
      -h, --help            show this help message and exit
      -q
      -p PARAMS, --params=PARAMS
                            Optional key=value parameters, comma-separated -p
                            k=v,k2=v2,...
      -n, --noop            NO-OP - emit test names, but don't actually run them
                            e.g -n true
      -l LOGLEVEL, --log-level=LOGLEVEL
                            e.g -l info,warning,error

      TestCase/Runlist Options:
        -i INI, --ini=INI   Path to .ini file containing server information,e.g -i
                            tmp/local.ini
        -c RUNLIST, --config=RUNLIST
                            Config file name (located in the conf subdirectory),
                            e.g -c py-view.conf
        -t TESTCASE, --test=TESTCASE
                            Test name (multiple -t options add more tests) e.g -t
                            performance.perf.DiskDrainRate

Buildout
--------

Initiate buildout directory structure, create sandbox, build packages and scripts, fetch dependencies, and etc.:

    buildout

You can execute testrunner now:

    ./bin/testrunner -h

Resource Files
--------------

Ini files represents the ns_server information which is accessible to the tests.

**[global]** section defines the rest username, password that tests use to login to
ns_server.

Example:

    [global]
    username:Administrator
    password:membase

    [membase]
    rest_username:Administrator
    rest_password:asdasd

**[servers]** section lists port and ssh related information. ssh connection
information is required for small subset of tests where test needs to perform
installation,backup or restore. If ns_server instances are started using
ns_server/cluster_run script then you only need to define ip and port for those
nodes.

Example:

    [servers]
    1:10.1.6.104_1
    2:10.1.6.104_2
    3:10.1.6.104_3
    4:10.1.6.104_4

    [10.1.6.104_1]
    ip:10.1.6.104
    port:9000

    [10.1.6.104_2]
    ip:10.1.6.104
    port:9001

    [10.1.6.104_3]
    ip:10.1.6.104
    port:9002

    [10.1.6.104_4]
    ip:10.1.6.104
    port:9003

Test Execution and Reporting
----------------------------

For every test run testrunner creates a temp folder and dumps the logs and
xunit reports in the newly generated folder.

for instance if you run

    $ python3 testrunner.py -i resources/jenkins/single-node-centos-32.ini -t setgettests.MembaseBucket.value_100b -p your_first_parameter=x,your_second_parameter=y

you will see this summary after each test is ran:

    summary so far suite setgettests.MembaseBucket , pass 1 , fail 0
    logs and results are available under tmp-12-11-47

and logs:

    $ ls  tmp-12-11-47/
    report-12-11-47.xml-setgettests.MembaseBucket.xml	value_100b.log

Development
-----------

When using git on Linux/OSX systems, you might run into issues where git
incorrectly believes Windows-related files have been modified. In reality, git
is merely mis-treating CRLF line endings.
Try the following...

    $ cd testrunner
    $ git config core.autocrlf false

Running End to End Tests Before Submitting a Patch to Gerrit
------------------------------------------------------------

Testrunner project has different test suites which can be run priori to
submitting the code to gerrit. There are test suites that can be run against a
single node which validates basic database operations such as persistence and
bucket management. There are also key-value clustering related test cases which
can be run against Membase/Couchbase 1.8 multiple nodes. Recently we have also
been adding more tests which validates basic view functionalities on a cluster
and on a single.

### "make e2e-kv-single-node"

This make target will start ns_server using ```cluster_run -n1``` and run all the
test cases listed in conf/py-all-dev.conf. The test runtime can vary between
15-30 minutes depending on your machine.

Testrunner prints out a human readable pass/fail of the tests. Please refer to
the "rerunning test" section for more information of how to re-run one single
test against cluster_run.

### "make test-views"

This make target will start four ns_server(s) using ```cluster_run -n4``` and
runs all the test cases listed in conf/py-view.conf. The test runtime can vary
between 30-45 minutes. py-view.conf contains basic test cases which validates
clustering operations such as rebalancing.Each test is also parametrized so you
can easily modify this run list and change the number of docs or the
load_duration.

For instance, ```test_get_view_during_x_min_load_y_working_set,num-docs=10000,load-time=1,run-view-time=1```
test will create a view, inserts 1000 documents, mutate those documents for 1
minute and run view queries in parallel to the load for 1 min. You can easily
change the parameters there to insert 1M items and keep the load running for 10
mins for example.

Using mcsoda to load json documents on to a node/cluster
--------------------------------------------------------

mcsoda is a load generator tool that randomly generates json
documents. It is available under testrunner/lib/perf_engines/ .

Mcsoda uses moxi port to distribute load in a cluster.
The moxi can be set up on the server side or on a client side as follows:

### "server-side moxi"

From a client, load can be run on a cluster represented by: xx.xx.xx.xxx as:

	lib/perf_engines/mcsoda.py xx.xx.xx.xxx:11211 vbuckets=1024 doc-gen=0 doc-cache=0
	ratio-creates=0.5 ratio-sets=0.2 ratio-deletes=0.02  ratio-expirations=0.03
	expiration=3600 min-value-size=2,4 threads=100 max-items=18000000
	exit-after-creates=1 prefix=KEY_ max-creates=18000000

This is going to ensure that there will be 20% sets against 80% gets.
Of the 20% sets, 50% will be creates and the rest updates.
There shall be 2% deletes, and minimum item size varies from 2 to 4 Bytes.
3% of the items set will be marked as expired after a duration of 1 hour.
The tool will use 100 threads to generate the json load, and every item will be
prefixed by "KEY_". max_creates limits the no. of items created. All the load
generated is going to get loaded on bucket "default". Prefix the IP by
"bucket_name:password@", to load on to standard/sasl buckets.

For more options in mcsoda, type in:
```lib/perf_engines/mcsoda.py -h```

### "client-side moxi"

Set up the moxi for a specific bucket on the client side as follows:
(Make sure couchbase-server is installed on the client)

	/opt/couchbase/bin/moxi -Z usr=Administrator,pwd=password,port_listen=11611,
	concurrency=1024,wait_queue_timeout=200,connect_timeout=400,connect_max_errors=3,
	connect_retry_interval=30000,auth_timeout=100,downstream_conn_max=128,downstream_timeout=5000,
	cycle=200 http://xx.xx.xx.xxx:8091/pools/default/bucketsStreaming/[bucket_name] -d

This command sets up moxi for the cluster on the client side at port 11611.
For more information on the moxi command, type in:
```/opt/couchbase/bin/moxi -h```

Once the moxi is all set up, run mcsoda against the moxi just set up:

	lib/perf_engines/mcsoda.py localhost:11611 vbuckets=1024 doc-gen=0 doc-cache=0
	ratio-creates=0.5 ratio-sets=0.2 ratio-deletes=0.02  min-value-size=2,4 threads=100
	max-items=18000000 exit-after-creates=1 prefix=KEY_ max-creates=18000000
