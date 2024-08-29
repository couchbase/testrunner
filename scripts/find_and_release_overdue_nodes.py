from datetime import datetime, timedelta
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.n1ql import QueryScanConsistency
import sys
import time

SERVER_MANAGER_USER_NAME = 'Administrator'
SERVER_MANAGER_PASSWORD = "esabhcuoc"


def get_last_n_days(n=5):
    last_n_days = list()
    current_date = datetime.now()

    # Calculate the date 3 days before so that we don't accidentally release the nodes that are actually being used
    two_days_before = current_date - timedelta(days=2)

    # Generate the past 10 days
    past_10_days = [(two_days_before - timedelta(days=i)).strftime("%b-%d") for i in range(n)]

    # append the past 10 days
    for date in past_10_days:
        last_n_days.append("%" + date + "%")

    return last_n_days


def main():
    poolId = sys.argv[1]
    os = sys.argv[2]
    state = sys.argv[3]
    num_days = sys.argv[4]

    print("-----------------------------------------------------------------------------------------------------\n")
    print('the poolId is', poolId)
    print('the os is', os)
    print('the state is', state)
    print('num_days is', num_days)
    print("-----------------------------------------------------------------------------------------------------\n")

    auth = PasswordAuthenticator(SERVER_MANAGER_USER_NAME, SERVER_MANAGER_PASSWORD)
    cluster = Cluster.connect('couchbase://172.23.104.162', ClusterOptions(auth))
    cb = cluster.bucket('QE-server-pool')

    query_select_string = ("select * from `QE-server-pool` where os = '{0}' and "
                    "(poolId = '{1}' or '{1}' in poolId) and username like '{2}' and state='{3}'")
    query_update_string = ("update `QE-server-pool` set state='available' where os = '{0}' and "
                    "(poolId = '{1}' or '{1}' in poolId) and username like '{2}' and state='{3}'")

    last_n_days = get_last_n_days(n=int(num_days))
    for day in last_n_days:
        print("-----------------------------------------------------------------------------------------------------\n")
        print(query_select_string.format(os, poolId, day, state))
        query_res_select = cluster.query(query_select_string.format(os, poolId, day, state),
                                         QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS))
        for row in query_res_select.rows():
            print('result: ', row)
        print("\n")

        print(query_update_string.format(os, poolId, day, state))
        query_res_update = cluster.query(query_update_string.format(os, poolId, day, state))
        time.sleep(20)
        print("\n")

        print(query_select_string.format(os, poolId, day, state))
        query_res_select = cluster.query(query_select_string.format(os, poolId, day, state),
                                         QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS))
        for row in query_res_select.rows():
            print('result: ', row)
        print("\n")
        print("-----------------------------------------------------------------------------------------------------\n")

    return


if __name__ == "__main__":
    main()
