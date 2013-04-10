#TODO: allow workers to pull this from cache

RABBITMQ_IP = '127.0.0.1'
OBJECT_CACHE_IP = "127.0.0.1"
OBJECT_CACHE_PORT = "11911"
SERIESLY_IP = ''
COUCHBASE_IP = '127.0.0.1'
COUCHBASE_PORT = '9000'
COUCHBASE_USER = "Administrator"
COUCHBASE_PWD = "asdasd"
SSH_USER = "root"
SSH_PASSWORD = "password"
WORKERS = ['127.0.0.1']
# valid configs ["kv","query","admin"] or ["all"]
WORKER_CONFIGS = ["kv"]
CB_CLUSTER_TAG = "default"
#tags used for workers controlling remote clusters (xdcr use case)
#For example, 2 dest clusters for xdcr then TAG = ["remote1", "remote2"]
CB_REMOTE_CLUSTER_TAG = []
ATOP_LOG_FILE = "/tmp/atop-node.log"
LOGDIR="logs"  # relative to current dir

#Cluster Config
#If you have multiple cluster to setup (xdcr use case), 'initial_nodes' should be a multiple value
#string seperated by ','. For example, 2 clusters to setup, 1 src, 1 dest, 'initial_nodes': '2,3'
CLUSTER = {'ini':'pysystests/cluster.ini', 'sasl_buckets':0, 'standard_buckets':0, 'default_bucket':False,
           'default_mem_quota':12000, 'sasl_mem_quota':7000, 'xdcr':False, 'initial_nodes': "1"}

#Backup Config
ENABLE_BACKUPS = False
BACKUP_DIR = "/tmp/backup"
BACKUP_NODE_IP = "127.0.0.1"
BACKUP_NODE_SSH_USER = "root"
BACKUP_NODE_SSH_PWD = "password"