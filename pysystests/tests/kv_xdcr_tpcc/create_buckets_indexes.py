import sys
import time
sys.path = [".", "../lib"] + sys.path

import testcfg as cfg
from membase.api.rest_client import RestConnection


if __name__ == "__main__":

    port = 11218
    # bucket and ram quota
    buckets_ram = {
        "CUSTOMER": 1024,
        "DISTRICT": 500,
        "HISTORY": 500,
        "ITEM": 1024,
        "NEW_ORDER": 500,
        "ORDERS": 500,
        "ORDER_LINE": 2000,
        "STOCK": 500,
        "WAREHOUSE": 1024
        }

    index_n1ql = ["drop index CUSTOMER.CU_ID_D_ID_W_ID USING GSI",
                  "drop index CUSTOMER.CU_W_ID_D_ID_LAST USING GSI",
                  "drop index DISTRICT.DI_ID_W_ID using gsi",
                  "drop index ITEM.IT_ID using gsi",
                  "drop index NEW_ORDER.NO_D_ID_W_ID using gsi",
                  "drop index ORDERS.OR_O_ID_D_ID_W_ID using gsi",
                  "drop index ORDERS.OR_W_ID_D_ID_C_ID using gsi",
                  "drop index ORDER_LINE.OL_O_ID_D_ID_W_ID using gsi",
                  "drop index STOCK.ST_W_ID_I_ID1 using gsi",
                  "drop index WAREHOUSE.WH_ID using gsi",
                  "create index CU_ID_D_ID_W_ID on CUSTOMER(C_ID, C_D_ID, C_W_ID) using gsi WITH {\"defer_build\":true}",
                  "create index CU_W_ID_D_ID_LAST on CUSTOMER(C_W_ID, C_D_ID, C_LAST) using gsi WITH {\"defer_build\":true}",
                  "build index on CUSTOMER(CU_ID_D_ID_W_ID, CU_W_ID_D_ID_LAST) using gsi",
                  "create index DI_ID_W_ID on DISTRICT(D_ID, D_W_ID) using gsi WITH {\"defer_build\":true}",
                  "build index on DISTRICT(DI_ID_W_ID) using gsi",
                  "create index IT_ID on ITEM(I_ID) using gsi WITH {\"defer_build\":true}",
                  "build index on ITEM(IT_ID) using gsi",
                  "create index NO_D_ID_W_ID on NEW_ORDER(NO_O_ID, NO_D_ID, NO_W_ID) using gsi WITH {\"defer_build\":true}",
                  "build index on NEW_ORDER(NO_D_ID_W_ID) using gsi",
                  "create index OR_O_ID_D_ID_W_ID on ORDERS(O_ID, O_D_ID, O_W_ID, O_C_ID) using gsi WITH {\"defer_build\":true}",
                  "create index OR_W_ID_D_ID_C_ID on ORDERS(O_W_ID, O_D_ID, O_C_ID) using gsi WITH {\"defer_build\":true}",
                  "build index on ORDERS(OR_O_ID_D_ID_W_ID, OR_W_ID_D_ID_C_ID) using gsi",
                  "create index OL_O_ID_D_ID_W_ID on ORDER_LINE(OL_O_ID, OL_D_ID, OL_W_ID) using gsi WITH {\"defer_build\":true}",
                  "build index on ORDER_LINE(OL_O_ID_D_ID_W_ID) using gsi",
                  "create index ST_W_ID_I_ID1 on STOCK(S_W_ID, S_I_ID) using gsi WITH {\"defer_build\":true}",
                  "build index on STOCK(ST_W_ID_I_ID1) using gsi",
                  "create index WH_ID on WAREHOUSE(W_ID) using gsi WITH {\"defer_build\":true}",
                  "build index on WAREHOUSE(WH_ID) using gsi",
                  "select keyspace_id, state from system:indexes",
                  "select keyspace_id, state from system:indexes where state != 'online'"
                 ]

    server_info = {
        "ip": cfg.CLUSTER_IPS[0],
        "port" : 8091,
        "username" : cfg.COUCHBASE_USER,
        "password" : cfg.COUCHBASE_PWD
    }
    #node = type('OBJ', (object,), server_info)
    #print node
    cluster_rest = RestConnection(server_info)

    # drop and recreate buckets
    for i, bucket_name in enumerate(buckets_ram.keys()):
        print("Creating bucket {0}".format(bucket_name))
        cluster_rest.delete_bucket(bucket_name)
        cluster_rest.create_bucket(bucket=bucket_name,
                                   ramQuotaMB=int(buckets_ram[bucket_name]),
                                   proxyPort=port+i)
    time.sleep(60)

    # index drop/create
    for query in index_n1ql:
        print(query)
        result = cluster_rest.query_tool(query)
        print(result)
