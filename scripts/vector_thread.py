from vector_query import QueryVector
from vector_query import SiftVector as sift
from vector_query import LoadVector, IndexVector
import threading
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
import time, random


if __name__ == "__main__":
    auth = PasswordAuthenticator('Administrator', 'password')
    cluster = Cluster('couchbase://127.0.0.1', ClusterOptions(auth))
    thread_count = 5
    query_count = 5
    load_vector = False
    index_vector = False

    sift().download_sift()
    xb = sift().read_base()
    xq = sift().read_query()
    gt = sift().read_groundtruth()

    if load_vector:
        print("Load sift documents ...")
        LoadVector().load_documents(cluster, xb)
        print

    if index_vector:
        print("Index documents ...")
        IndexVector().create_index(cluster)

    print(f"Running {thread_count} threads X {query_count} queries")
    print()
    # Create threads
    threads = []
    for i in range(thread_count):
        begin = random.randint(0, len(xq) - query_count)
        thread = threading.Thread(target=QueryVector().run_queries,args=(cluster, xb, xq[begin:begin+query_count], gt[begin:begin+query_count], i, 'L2_SQUARED', 'default'))
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
    print(f"Ran # {thread_count*query_count} ({thread_count}x{query_count}) queries in {round(execution_time, 2)} seconds")
    print(f"Query per seconds: {round(thread_count*query_count / execution_time, 2)}")