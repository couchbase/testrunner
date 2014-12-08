CC=g++
CFLAGS=-g -o
INCLUDES=-I/root/gtest-1.7.0/include -I/include/couchstore/couchstore/src -I/include/couchstore/couchstore -I/include/platform/include/include -I/include/couchstore/couchstore/include
LFLAGS=-L/root/gtest-1.7.0/lib/.libs -L/root/libcouchbase/.libs -L/root/snappy-1.1.2/ -L/include/install/lib/lib -L/opt/couchbase/lib/
LIBS=-lcouchbase -lcouchstore -lsnappy -lgtest -lpthread -lcJSON -lplatform -ldirutils

all: Compacttest AccessLogtest Prioritytest Warmuptest

Compacttest: CompactTests.cc
	$(CC) $(CFLAGS) Compacttest $(INCLUDES) $(LFLAGS) CompactTests.cc $(LIBS)

AccessLogtest: AccessLogTests.cc
	$(CC) $(CFLAGS) AccessLogtest $(INCLUDES) $(LFLAGS) AccessLogTests.cc $(LIBS)

Prioritytest: PriorityTests.cc
	$(CC) $(CFLAGS) Prioritytest $(INCLUDES) $(LFLAGS) PriorityTests.cc $(LIBS)

Warmuptest: WarmupTests.cc
	$(CC) $(CFLAGS) Warmuptest $(INCLUDES) $(LFLAGS) WarmupTests.cc $(LIBS)

clean:
	rm -f Compacttest AccessLogtest Prioritytest Warmuptest
