import os

NS_NUM_NODES = 4
NS_PATH  = os.environ.get('NS_PATH') or "../ns_server/"
MD_PATH = os.environ.get('MD_PATH') or "../install/bin"
NS_STARTED_STR="Couchbase Server has started"
NS_EXITED_STR="ns_server has exited"
MC_STARTED_STR="activated memcached port server"
REBALANCE_STARTED_STR="Started rebalancing"
COMPACT_STARTED_STR="Forceful compaction"

