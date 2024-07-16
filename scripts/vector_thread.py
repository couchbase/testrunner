from vector_query import QueryVector
from vector_query import SiftVector as sift
import threading
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
import time, random


if __name__ == "__main__":
    auth = PasswordAuthenticator('Administrator', 'password')
    cluster = Cluster('couchbase://127.0.0.1', ClusterOptions(auth))
    thread_count = 10
    query_count = 5

    sift().download_sift()
    xb = sift().read_base()
    xq = sift().read_query()
    gt = sift().read_groundtruth()

    print(f"Running {thread_count} threads X {query_count} queries")
    print()
    # Create threads
    threads = []
    for i in range(thread_count):
        begin = random.randint(0, len(xq) - query_count)
        thread = threading.Thread(target=QueryVector().run_queries,args=(cluster, xb, xq[begin:begin+query_count], gt[begin:begin+query_count], i, 'L2', 'default'))
        threads.append(thread)
    # Start threads
    start_time = time.time()
    for i in range(len(threads)):
        threads[i].start()
    # Wait for threads to finish
    for i in range(len(threads)):
        threads[i].join()
    end_time = time.time()
    execution_time = end_time - start_time

    print()
    print(f"Ran # {thread_count*query_count} queries in {execution_time} seconds")
    print(f"Query per seconds: {round(thread_count*query_count / execution_time, 2)}")