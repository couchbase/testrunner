Engine tests suite is collection of tests for testing the eventually persistent engine(ep-engine) component of Couchbase Server. The following tests are currently part of this suite:
1)DataTypeTests.cc - Tests for the flexible metadata feature
2)WarmupTests.cc - Tests for parallel Warmup
3)AccessLogTests.cc - Tests for parallel access log generation and loading
4)CompactTests.cc - Tests for ep-engine managed compaction and expired item deletion
The file BaseTest.cc contain the BaseTest class inherited by the Test classes in the other files.

How to run the tests:
The Couchbase C client library "libcouchbase" is needed to run these tests. For DataTypeTests and CompactTests, a special libcouchbase library with the 'lcb_hello' and 'lcb_compact' APIs is needed. A repository with these can custom APIs can be found below.

In addition to libcouchbase, Google Test(gtest) is needed for all tests. Snappy library is needed for DataTypeTests. Once you have all the software mentioned above installed on the machines the tests can be compiled and run as follows:

Warmuptests:
To compile,
   g++ -o Warmuptest -I/root/gtest-1.7.0/include -L/root/gtest-1.7.0/lib -L/root/libcouchbase/.libs -L/root/snappy-1.1.2/WarmupTests.cc -lcouchbase -lsnappy -lgtest -lpthread

   To run Warmuptest you need two machines(VMs). The first machine is the local VM where you plan to launch the test, edit the addNode, removeNode scripts to provide the ip address of the second machine. Test can be launched as below:
   ./Warmuptest localhost:8091
   A specific test case inside a test can be launched as,
   ./Warmuptest localhost:8091 --gtest_filter=WarmupTest.WarmupActive_10DGM_ValueEvic_Test
