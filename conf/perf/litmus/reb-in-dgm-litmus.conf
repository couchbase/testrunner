# rebalance mixed 14M load, 4M hot reload, 3M access creates
# rebalance from 2 - 4 nodes
# 30 clients
# speed limit = 9k cluster-wide
#
performance.eperf.EPerfClient.test_eperf_rebalance

params:

# general
batch=50
kind=nonjson
mem_quota=10000

# load phase
hot_init_items=4000000
items=14000000
hot_load_get=1

# cbm
cbm=0

# access phase
# Read:Insert:Update:Delete Ratio = 50:4:40:6.
ratio_sets=0.5
ratio_misses=0.05
ratio_creates=0.08
ratio_deletes=0.13
ratio_hot=0.05
ratio_hot_gets=0.99
ratio_hot_sets=0.99
ratio_expirations=0.03
max_creates=3000000

# rebalance
rebalance_after=500000
num_nodes_after=3
reb_max_retries=5

# control (defaults: pytests/performance/perf_defaults.py)
load_wait_until_drained=1
loop_wait_until_drained=0
mcsoda_heartbeat=3
mcsoda_max_ops_sec=300
tear_down=1
tear_down_proxy=1
tear_down_bucket=0
tear_down_cluster=1
tear_down_on_setup=0
