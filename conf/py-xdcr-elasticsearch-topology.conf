
xdcr.esXDCR.ESTests:

    ############ ES TOPOLOGY TESTS ###############

    # rebalance in/out swap simple kv
    test_topology,items=10000,rdirection=unidirection,es_in=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,es_out=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,es_swap=True,end_replication_flag=1

    # rebalance in/out/swap mixed doc types
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,es_in=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,es_out=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,es_swap=True,end_replication_flag=1

    # rebalance in/out/swap multi-bucket mixed doc types
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,es_in=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,es_out=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,es_swap=True,end_replication_flag=1


    ############ CB TOPOLOGY TESTS ###############

    # rebalance in/out swap simple kv
    test_topology,items=10000,rdirection=unidirection,cb_in=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,cb_out=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,cb_swap=True,end_replication_flag=1

    # rebalance in/out/swap mixed doc types
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,cb_in=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,cb_out=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,cb_swap=True,end_replication_flag=1
    # failover
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,cb_swap=True,cb_failover=True,end_replication_flag=1

    # rebalance in/out/swap multi-bucket mixed doc types
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,cb_in=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,cb_out=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,cb_swap=True,end_replication_flag=1
    # failover
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,cb_swap=True,cb_failover=True,end_replication_flag=1


    ############ ES+CB TOPOLOGY TESTS ###############
    # rebalance in/out/swap multi-bucket mixed doc types
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,cb_in=True,es_in=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,cb_in=True,es_out=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,cb_out=True,es_in=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,cb_out=True,es_out=True,end_replication_flag=1
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,cb_swap=True,es_swap=True,end_replication_flag=1
    # failover
    test_topology,items=10000,rdirection=unidirection,doc-ops=read-create-delete-update,expires=10,standard_buckets=1,cb_out=True,es_out=True,cb_failover=True,end_replication_flag=1

