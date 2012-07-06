mcsoda - sugary streaming load-generator for key-value stores
=============================================================

Is mcsoda good for you? Or, rather, does it work well as a
load-generator?  Even though mcsoda's implemented in python, I've seen
mcsoda perform better than some other C-based load generators in
several cases (but your own mileage may vary).

What is mcsoda's secret ingredient?
-----------------------------------

mcsoda performs well because it (cheats) operates differently by
avoiding the classic single-request / single-response approach.
Instead, mcsoda uses a "streaming" or batched approach similar to
spymemcached's design.

Instead of invoking multiple, individual send() & recv() calls for
each request, mcsoda batches up a (configurable) number of requests
into one buffer and invokes a single, large send() system call. And,
mcsoda invokes read()'s in buffered fashion. This results in fewer
system calls, and the networking stack is better utilized.  On the
server-side, key-value servers like memcached can be more fully
utilized with real request processing work rather than waiting for the
request-response tennis ball to be batted back and forth across the
net.

The ugly/bad
------------

One place where mcsoda does NOT work well is in leveraging multiple
client-side threads.  Even though mcsoda can run with multiple
threads, python's threading is sub-optimal, so multi-threaded mcsoda
is not recommened.

The good
--------

There's an advantage to single-threadedness, however, and that is that
reaching repeatability is much easier to accomplish.  mcsoda supports
repeatability by implementing a functional algorithm to workload
generation.  If you have multiple runs of mcsoda and invoke those
mcsoda's using the same exact command-line parameters, then each of
those separate runs of mcsoda will generate the same exact sequence of
requests.  If you change (or improve) the server, you can repeat your
performance experiments more scientifically by reusing the same exact
mcsoda parameters from previous runs.

Additionally, mcsoda tries (optionally) to use a constant amount of
memory, even as it's tracking histograms & statistics.  Other
load-generators may have bugs (or features) that add a little bit of
memory usage on each request, leaving those other load-generators
vulnerable in long running tests to out-of-memory conditions.

Usage instructions
------------------

    usage: ./mcsoda.py [memcached[-binary|-ascii]://][user[:pswd]@]host[:port] [key=val]*

      default protocol = memcached-binary://
      default port     = 11211

    examples: ./mcsoda.py memcached-binary://127.0.0.1:11211 max-items=1000000 json=1
              ./mcsoda.py memcached://127.0.0.1:11211
              ./mcsoda.py 127.0.0.1:11211
              ./mcsoda.py 127.0.0.1
              ./mcsoda.py my-test-bucket@127.0.0.1
              ./mcsoda.py my-test-bucket:MyPassword@127.0.0.1

    optional key=val's and their defaults:
      backoff-factor     = 2.0   Exponential backoff factor on ETMPFAIL errors.
      batch              = 100   Batch/pipeline up this # of commands per server.
      doc-cache          = 1     When 1, cache docs; faster, but uses O(N) memory.
      doc-gen            = 1     When 1 and doc-cache, pre-generate docs at start.
      exit-after-creates = 0     Exit after max-creates is reached.
      expiration         = 0     Expiration time parameter for SET's
      histo-precision    = 1     Precision of histogram bins.
      hot-shift          = 0     # of keys/sec that hot item subset should shift.
      json               = 1     Use JSON documents. 0 to generate binary documents.
      max-creates        = -1    Max # of creates; defaults to max-items.
      max-items          = -1    Max # of items; default 100000.
      max-ops            = 0     Max # of ops before exiting. 0 means keep going.
      max-ops-per-sec    = 0     When >0, max ops/second target performance.
      min-value-size     = 10    Min value size (bytes) for SET's; comma-separated.
      prefix             =       Prefix for every item key.
      ratio-arpas        = 0.0   Fraction of SET non-DELETE'S to be 'a-r-p-a' cmds.
      ratio-creates      = 0.1   Fraction of SET's that should create new items.
      ratio-deletes      = 0.0   Fraction of SET updates that shold be DELETE's.
      ratio-expirations  = 0.0   Fraction of SET's that use the provided expiration.
      ratio-hot          = 0.2   Fraction of items to have as a hot item subset.
      ratio-hot-gets     = 0.95  Fraction of GET's that hit the hot item subset.
      ratio-hot-sets     = 0.95  Fraction of SET's that hit the hot item subset.
      ratio-misses       = 0.05  Fraction of GET's that should miss.
      ratio-sets         = 0.1   Fraction of requests that should be SET's.
      report             = 40000 Emit performance output after this many requests.
      threads            = 1     Number of client worker threads to use.
      time               = 0     Stop after this many seconds if > 0.
      vbuckets           = 0     When >0, vbucket hash in memcached-binary protocol.
      cur-arpas          = 0     # of add/replace/prepend/append's (a-r-p-a) cmds.
      cur-base           = 0     Base of numeric key range. 0 by default.
      cur-creates        = 0     Number of sets that were creates.
      cur-deletes        = 0     Number of deletes already done.
      cur-gets           = 0     Number of gets already done.
      cur-items          = 0     Number of items known to already exist.
      cur-sets           = 0     Number of sets already done.

      TIP: min-value-size can be comma-separated values: min-value-size=10,256,1024

FAQ
---

Q: How do I use mcsoda to load a million documents, and then exit?

    ./mcsoda.py URL max-items=1000000 ratio-sets=1 ratio-creates=1 exit-after-creates=1 doc-gen=0 doc-create=0

That can be read like "of the requests that mcsoda will perform, 100%
of them should be SET's (related, 0% will be GET's).  That is because
the ratio-sets=1.00

Of those SET's, 100% will be SET's that create new items (related, 0%
will be mutations on existing items).  That is because the
ratio-creates=1.00

Q: Will the cluster-aware version be vbucket-aware or still require
moxi (the memcached/couchbase proxy)?

A: No moxi required -- mcsoda can optionally behave as a so-called
"smart client" and respond to Couchbase's dynamic cluster
server-map/REST topology changes. In other words, you can use
Rebalance to add/remove server nodes while using mcsoda.

Q: What about the non-cluster aware?  I wish to do a bit of testing on
single-node and see if that makes a difference... so I need port
11210 "direct access" compatibility?

A: Yes, you can do that too with mcsoda.

The "protocol" part of the URL-looking parameter control mcsoda's
behavior.  To target a single-node's memcached on its direct port,
11210, use "memcached-binary://HOST:11210".  However, when you target
port 11210, you'll also need to tell mcsoda how many vbuckets there
are (otherwise, it only targets vbucket 0).

Example...

    ./mcsoda.py memcached-binary://HOST:11210 vbuckets=1024

Or, if you're testing Couchbase 2.0...

    ./mcsoda.py memcached-binary://HOST:11210 vbuckets=256

To target moxi, use port 11211...

    ./mcsoda.py memcached-binary://HOST:11211

For completeness, to target an entire Couchbase / Membase cluster, use
"membase-binary://HOST:8091".  mcsoda only targets the default bucket.
Example...

    ./mcsoda.py membase-binary://HOST:8091

Q: What is doc-gen and doc-cache and do I need those?

The doc-gen and doc-cache parameters let you control some time/space
tradeoffs in mcsoda.  It takes time to generate documents.  Instead,
if you want to generate them up-front, use doc-gen=1.  This will give
you better runtime performance.  However, holding onto all these
pre-generated docs won't be good if you're trying to load a huge
number of documents, since mcsoda will spend a lot of time generating
docs before sending its first requests.  So, to turn off document
pre-generation, used doc-gen=0.

Even if doc-gen=0, you might want mcsoda to cache any documents that
it ends up creating during runtimes. To do that, specify doc-cache=1.
Over long runs, however, mcsoda will end up using more and more memory
to hold onto those cached, generated documents.

In short, when trying to do long, multi-hour or multi-day runs, used
doc-cache=0.  If you're also trying to test with a lot of documents,
also use doc-gen=0.

Q: If mcsoda uses request batching, how does it get latency timings?

At the start of each batch, mcsoda performns do a single, classic
request-reply to the server in order to measure latency.  So, if your
batch=100 (the default), then every hundred'th request will be a
latency measurement request.

Q: How do I see what data mcsoda is loading?

Use the 'none://' protocol, which is a NO-OP protocol and make mcsoda
just print out the commands it would have sent to a server.

Q: What's the significance of the number of items (max-items).  Any
reason not use the default?

No reason not to use default.  Just be consistent (you probably are)
for perf tests, and if not already, probably have to reset your server
cluster every time.

Q: If the number of operations (max-ops) is greater than the number of
items (max-items), after max-items is reached, the existing items are
just reused for operations, right?

That's right -- it rolls around.  By that, I mean that mcsoda keeps
track of the # of items it knows it has created itself (in a cur-items
counter), and it can do operations against those known items.

Q: I'd like to pre-load data before running (and re-running) the
actual test.  So, I want to use two (or more) invocations of mcsoda.

The trick is the 2nd invocation of mcsoda needs to be told the number
of items that are already there, via the cur-items parameter.  So...

1) loading phase - mcsoda ... max-items=1000000 ratio-sets=1 ratio-creates=1 exit-after-creates=1 doc-gen=0 doc-cache=0

2) accessing phase - mcsoda ... max-items=1000000 cur-items=1000000 ...

Q: I've been asked to measure performance for up to 16kb documents.  I
found that when I went above 9kb the behavior of mcsoda changed a lot
(i.e., if max-ops was something small like 10k, it would return almost
immediately, until i crossed the 9k document size, then it would
appear to hang forever, maybe not, but I would give up waiting).  I
found that if reduced the batch size to 50, I could safely go up to
the 16kb doc size, so I'm assuming the size of the batch was exceeding
some threshold.  I can use batch=50 for these tests but I was
wondering what might be going on here?

It's likely some bug in mcsoda.  mcsoda's doing a single send() call
per batch.  So, 16K * 100 byte sized buffer for each single send()
call.  There's no error handling if there's a problem with mcsoda's
send() call, and perhaps it's too large / would-block, etc.

You found the right workaround, though, to bring down the batch size.

More info
---------

An older presentation (pdf) is available at: https://github.com/couchbaselabs/mcsoda/blob/master/doc/mcsoda.pdf?raw=true




