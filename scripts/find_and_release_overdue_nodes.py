from datetime import datetime, timedelta
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
import sys
import time

SERVER_MANAGER_USER_NAME = 'Administrator'
SERVER_MANAGER_PASSWORD = "esabhcuoc"


def get_last_n_days(n=5):
    last_n_days = list()
    current_date = datetime.now()

    # Calculate the date 2 days before so that we don't accidentally release the nodes that are actually being used
    two_days_before = current_date - timedelta(days=2)

    # Generate the past 'n' days in 'Month-Day' format
    past_10_days = [(two_days_before - timedelta(days=i)).strftime("%b-%d") for i in range(n)]

    # Append the past days with wildcards for use in the query
    for date in past_10_days:
        last_n_days.append("%" + date + "%")

    return last_n_days


def main():
    # Get command-line arguments for poolId, os, state, and num_days
    poolId = sys.argv[1]
    os = sys.argv[2]
    state = sys.argv[3]
    num_days = sys.argv[4]

    print("-----------------------------------------------------------------------------------------------------\n")
    print('The poolId is:', poolId)
    print('The OS is:', os)
    print('The state is:', state)
    print('Number of days:', num_days)
    print("-----------------------------------------------------------------------------------------------------\n")

    # Initialize the Couchbase cluster connection with authentication
    authenticator = PasswordAuthenticator(SERVER_MANAGER_USER_NAME, SERVER_MANAGER_PASSWORD)
    cluster = Cluster('couchbase://172.23.217.21', authenticator=authenticator)
    bucket = cluster.bucket('QE-server-pool')

    # Query strings
    query_select_string = ("select * from `QE-server-pool` where os = '{0}' and "
                           "(poolId = '{1}' or '{1}' in poolId) and username like '{2}' and state='{3}'")
    query_update_string = ("update `QE-server-pool` set state='available' where os = '{0}' and "
                           "(poolId = '{1}' or '{1}' in poolId) and username like '{2}' and state='{3}'")

    # Fetch the list of last 'n' days
    last_n_days = get_last_n_days(n=int(num_days))

    # Iterate through the generated days and run queries
    for day in last_n_days:
        print("-----------------------------------------------------------------------------------------------------\n")
        # Execute the SELECT query to fetch relevant documents
        print(query_select_string.format(os, poolId, day, state))
        query_res_select = bucket.query(query_select_string.format(os, poolId, day, state))
        for row in query_res_select:
            print('result: ', row)
        print("\n")

        # Execute the UPDATE query to change the state to 'available'
        print(query_update_string.format(os, poolId, day, state))
        query_res_update = bucket.query(query_update_string.format(os, poolId, day, state))
        for row in query_res_update:
            print('result: ', row)
        time.sleep(20)
        print("\n")

        # Re-execute the SELECT query to verify the update
        print(query_select_string.format(os, poolId, day, state))
        query_res_select = bucket.query(query_select_string.format(os, poolId, day, state))
        for row in query_res_select:
            print('result: ', row)
        print("\n")
        print("-----------------------------------------------------------------------------------------------------\n")

    return


if __name__ == "__main__":
    main()
