Performance Test Phases
=======================

A performance test run has several phases...

Server machine preparation
--------------------------

This step is usually done manually, where a cluster of machines is
prepared, O/S installed, disks configured, etc.  The user might be
doing this on EC2, leveraging chef recipes, etc.  At the end, a list
of IP addresses is needed, in a testrunner *.ini file.  In this
document, our example is called "CLUSTER.ini".

Example *.ini files are in testrunner/resources/perf/*.ini directory.

Running testrunner across several phases
----------------------------------------

Next, various scripts are run in several phases.  All these phases
comprise a complete "run".  The "inputs" to a run are...

* BUILD - version of software that's the target of testing.  Example: 2.0.0r-1131)
* TESTNAME - the test configuration (*.conf).  Example: write-20M.conf
* CLUSTER - the cluster to test against.  Example: CLUSTER.ini, from above.

## install phase

In the install phase, the software-to-be-tested is downloaded and
installed on the CLUSTER.ini server machines, using this script...

    testrunner/scripts/install.py -i CLUSTER.ini -p product=cb,version=$BUILD

## cluster phase

In this phase, the cluster is configured, nodes joined and rebalanced,
but the cluster is still empty.  The script which does this is...

    testrunner/pytests/performance/do_cluster.py

## load phase

In this phase, data is initially loaded, using...

    cd testrunenr
    ./testrunner -i resources/perf/$CLUSTER.ini -c conf/perf/$TESTNAME.conf \
        -p load_phase=1,access_phase=0,...

This is usually done with multiple, concurrent clients to speed data
loading.  Each client is given a different partition of the dataset to
load.

## reload phase

The reload phase "touches" a specified set of data, so that the
cluster will have that data warm and cached in-memory.  The goal of
the reload phase is to get the cluster to a point of simulated actual
usage of user deployments.

The reload phase is currently done by a single client, but it can be
one-day be partitioned for higher performance.

## indexing phase

In this phase, which only applies for Couchbase 2.0 and those systems
that support indexing, indexes are defined/created on the cluster and
the data in the cluster is indexed.  The actual index definitions are
defined by the test spec.

## access phase

The access phase is the meat of the performance test.

In this phase, one or more concurrent clients access the database
using a defined mix of commands.  The number of clients and actual mix
of command types are defined by a test spec.

In the access phase, testrunner invokes mcsoda with the appropriate,
test-specific parameters.

    ./testrunner -i resources/perf/$CLUSTER.ini -c conf/perf/$TESTNAME.conf \
        -p load_phase=0,access_phase=1,...

An explanation of mcsoda is here...

    testrunner/pytests/performance/README.md

## drain phase (optional)

In this phase, we wait for any dirty write queues to drain to disk, so
that all data is fully persisted.

## last compact phase (optional)

A last compaction phase might be run, to measure the final on-disk
data usage of the system.

## last stats collection phase

The system has been collecting stats all along across the phases on
each node.  But, finally, there's a last stats "collect" phase that
aggregates data, lightly cleanses it, and pushes data to a data
archive where it becomes immutable.

The entire system, roughly, is almost a giant function with inputs of
BUILD, CLUSTER, TESTNAME, START_TIME and outputs in the data archive.

We currently use CouchDB as the data archive, but may also introduce
other file stores like S3.

## report generation phase

This phases uses information from the data archive to generate PDF's
or other reports.  In this phase, results from various runs can be
compared.  The compared runs, naturally, need to have the same CLUSTER
and TESTNAME, but may have differing BUILD and START_TIME.

# During Development

During development, you can run load and access phase all in one shot,
for example, by having all XXX_phase parameter flags set to 1.  For
example...

    ./testrunner -i resources/perf/$CLUSTER.ini -c conf/perf/$TESTNAME.conf \
        -p load_phase=1,access_phase=1,...

# Automated Performance Runs

We use a continuous integraton system, Jenkins, to schedule and
execute the above phases for each run.  Input parameters to the
Jenkins job include the BUILD, CLUSTER, TESTNAME, and Jenkins computes
the START_TIME.

# Immutability Conventions

The conf/perf/$TESTNAME.conf files should be treated as immutable.  If
you want to change a test, don't change an existing file.  Instead,
create a new copy and tweak the copy.

The resources/perf/$CLUSTER.ini files should be similarly considered
as immutable, by convention.
