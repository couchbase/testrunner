"""
Default static values for performance module.
Access by calling PerfDefaults.*
DO NOT overwrite.
"""

class PerfDefaults:

    # general
    batch               =    50
    kind                =    "nonjson"
    mem_quota           =    6000
    num_nodes           =    10

    # load phase
    items               =    22000000    # 22 million

    # access phase
    # Read:Insert:Update:Delete Ratio = 20:15:60:5.
    ratio_sets          =    0.8
    ratio_misses        =    0.05
    ratio_creates       =    0.1875
    ratio_deletes       =    0.0769
    ratio_hot           =    0.05
    ratio_hot_gets      =    0.995
    ratio_hot_sets      =    0.995
    ratio_expirations   =    0.025
    max_creates         =    30000000    #30 million

    # rebalance
    rebalance_after     =    200000

    # control
    db_compaction           =   2        # db fragmentation percentage triggers compaction
    load_wait_until_drained =   1        # wait to be drained on the load phase, 1: enabled, 0: disabled
    loop_wait_until_drained =   0        # wait to be drained on the access phase, 1: enabled, 0: disabled
    mcsoda_heartbeat        =   0        # health check heartbeat message for mcsoda. 0: no heartbeat, ~: in sec
    mcsoda_max_ops_sec      =   0        # max ops per seconds for mcsoda
    mcsoda_fg_stats_ops     =   1000     # fg ops threshold to persist latency stats
    tear_down               =   1        # 1: enabled, 0: disabled
    tear_down_proxy         =   1        # (prerequsite: tear_down = 1) tear down proxy,  1: enabled, 0: disabled
    tear_down_bucket        =   0        # (prerequsite: tear_down = 1) tear down bucket,  1: enabled, 0: disabled
    tear_down_cluster       =   1        # (prerequsite: tear_down = 1) tear down server cluster, 1: enabled, 0: disabled
    tear_down_on_setup      =   0        # teardown routine to clean up resources during startup. 1: enabled, 0: disabled
    view_compaction         =   10       # view fragmentation percentage triggers compaction
    vbuckets                =   1024     # number of vbuckets