#TODO: allow workers to pull this from cache

RABBITMQ_IP = '127.0.0.1'
OBJECT_CACHE_IP = "127.0.0.1"
OBJECT_CACHE_PORT = "11911"
SERIESLY_IP = ''
COUCHBASE_IP = '127.0.0.1'
COUCHBASE_PORT = '9000'
COUCHBASE_USER = "Administrator"
COUCHBASE_PWD = "asdasd"
COUCHBASE_OS = "linux" # linux|windows|unix
SSH_USER = "root"
SSH_PASSWORD = "password"
WORKERS = ['127.0.0.1']
# valid configs ["kv","query","admin"] or ["all"]
WORKER_CONFIGS = ["kv"]
CB_CLUSTER_TAG = "default"

# CHANGE THIS! to match all clusters will be managed by this worker
# ex. CLUSTER_IPS = ["127.0.0.1:9000","127.0.0.1:9001","127.0.0.1:9002","127.0.0.1:9003"]
CLUSTER_IPS = []

# xdcr config
"""
" pointer information to remote sites
" remote1 = name for remote site
" RABBITMQ_IP = broker managing remote site (can be same as local broker if using different vhosts)
"               this should equal RABBITMQ_IP of remote site
" CB_CLUSTER_TAG = represents vhost watched by workers remote site.
"                  this should equal CB_CLUSTER_TAG of remote site
" COUCHBASE_IP/PORT = IP/PORT of a couchbase node in remote site
"
" ex.
" REMOTE_SITES = {"remote1" : {"RABBITMQ_IP" : "10.0.0.5",
"                             "CB_CLUSTER_TAG" : "default",
"                             "COUCHBASE_IP" : "10.0.0.10",
"                             "COUCHBASE_PORT" : "9000"}}
"""
REMOTE_SITES = {}

LOGDIR="logs"  # relative to current dir


#Backup Config
ENABLE_BACKUPS = False
BACKUP_DIR = "/tmp/backup"
BACKUP_NODE_IP = "127.0.0.1"
BACKUP_NODE_SSH_USER = "root"
BACKUP_NODE_SSH_PWD = "password"
