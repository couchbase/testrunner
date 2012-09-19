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
    conf_file           =    "default.conf"
    bucket              =    "default"
    strftime            =     "%b-%d-%Y %H:%M:%S"
    mc_threads          =    4

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
    max_creates         =    30000000    # 30 million
    limit               =    50

    # cbstats collector
    cb_stats            =    0
    cb_stats_exc        =    "/opt/couchbase/bin/cbstats"
    cb_stats_freq       =    1200        # collect cbstats every 20 minutes

    # carbon stats
    carbon              =    0           # save realtime stats to carbon server, 1: enabled, 0: disabled
    carbon_server       =    "127.0.0.1" # ip/hostname of carbon server
    carbon_port         =    2003        # port number of carbon server
    carbon_timeout      =    5           # socket timeout value if carbon server is not responsive
    carbon_cache_size   =    10          # number of stats msgs queue up before flushing to carbon server

    # rebalance
    rebalance_after     =    200000
    num_nodes_after     =    7           # num of nodes after rebalance.
    reb_max_retries     =    0           # num of retries if rebalance fails
    reb_cons_view       =    0           # consistent view for rebalance task, 1: enable, 0: disable
    reb_no_fg           =    0           # rebalance without foreground load. 1: enable, 0: disable

    # control
    avg_value_size          =   2048     # average size of document body
    db_compaction           =   30       # db fragmentation percentage triggers compaction
    ep_compaction           =   50       # ep_engine side fragmentation percentage which triggers compaction
    hot_stack               =   0        # maintain a hot stack to queue up previously SETted keys
    hot_stack_size          =   0        # size of the hot stack. 0: compute from hot-ratio * num-items (caution: in memory)
    hot_stack_rotate        =   0        # rotate the stack upon eviction rather than pop the item. 1: enable, 0: disable
    load_wait_until_drained =   1        # wait to be drained on the load phase, 1: enabled, 0: disabled
    load_wait_until_repl    =   1        # wait for replication on the load phase, 1: enabled, 0: disabled
    loop_wait_until_drained =   0        # wait to be drained on the access phase, 1: enabled, 0: disabled
    loop_wait_until_repl    =   0        # wait for replication on the access phase, 1: enabled, 0: disabled
    mcsoda_heartbeat        =   0        # health check heartbeat message for mcsoda. 0: no heartbeat, ~: in sec
    mcsoda_max_ops_sec      =   0        # max ops per seconds for mcsoda
    mcsoda_fg_stats_ops     =   1000     # fg ops threshold to persist latency stats
    num_value_samples       =   100      # number of value samples (in memory, use large number with caution)
    nru_freq                =   1440     # NRU access scanner running freqency, in minutes
    nru_reb_delay           =   3600     # num of seconds to wait before triggering rebalance
    nru_polling_freq        =   10       # frequency to poll access scanner running status, in seconds
    nru_task                =   0        # schedule the NRU access scanner to run in the next hour, 1: enabled, 0: disabled
    observe                 =   0        # measure observe latencies. 1: enabled, 0: disabled
    obs_backoff             =   0.2      # default backoff time for observe commands, in seconds
    obs_max_backoff         =   1        # max backoff time for observe commands, in seconds
    obs_persist_count       =   1        # num of persisted copies to verify for observe commands
    obs_repl_count          =   1        # num of replica copies to verify for observe commands
    parallel_compaction     =   "true"   # process Database and View compaction in parallel. "true": enabled, "false": disabled
    start_delay             =   0        # delay (seconds) to start access phase
    tear_down               =   1        # 1: enabled, 0: disabled
    tear_down_proxy         =   1        # (prerequsite: tear_down = 1) tear down proxy,  1: enabled, 0: disabled
    tear_down_bucket        =   0        # (prerequsite: tear_down = 1) tear down bucket,  1: enabled, 0: disabled
    tear_down_cluster       =   1        # (prerequsite: tear_down = 1) tear down server cluster, 1: enabled, 0: disabled
    tear_down_on_setup      =   0        # teardown routine to clean up resources during startup. 1: enabled, 0: disabled
    view_compaction         =   30       # view fragmentation percentage triggers compaction
    vbuckets                =   1024     # number of vbuckets
    warmup                  =   0        # restart memcached and measure warmup phase. 1: enabled, 0: disabled
    woq_pattern             =   0        # measure standard write/observe/query pattern
    woq_verbose             =   0        # verbose output for woq pattern
    cor_pattern             =   0        # measure create/observe replication pattern on a different key space
    cor_persist             =   0        # block until item has been persisted to disk, 1: enabled, 0: disabled