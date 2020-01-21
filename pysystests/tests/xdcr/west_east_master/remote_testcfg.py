#TODO: allow workers to pull this from cache

RABBITMQ_IP = '172.23.105.99'
OBJECT_CACHE_IP = "172.23.105.92"
OBJECT_CACHE_PORT = "11911"
SERIESLY_IP = ''
COUCHBASE_IP = '172.23.105.49'
COUCHBASE_PORT = '8091'
COUCHBASE_USER = "Administrator"
COUCHBASE_PWD = "password"
SSH_USER = "root"
SSH_PASSWORD = "password"
WORKERS = ['127.0.0.1']
WORKER_CONFIGS = ["all"]
CB_CLUSTER_TAG = "default"

CLUSTER_IPS = ["172.23.105.58", "172.23.105.61", "172.23.105.60", "172.23.105.63"]


# xdcr config
"""
" pointer information to remote sites
" remote1 = name for remote site
" RABBITMQ_IP = broker managing remote site (can be same as local broker if using different vhosts)
"               this should equal RABBITMQ_IP of remote site
" CB_CLUSTER_TAG = represents vhost watched by workers remote site.
"                  this should equal CB_CLUSTER_TAG of remote site
" COUCHBASE_IP/PORT = IP/PORT of a couchbase node in remote site
"""
REMOTE_SITES = {}

LOGDIR="logs"  # relative to current dir


#Backup Config
ENABLE_BACKUPS = False
BACKUP_DIR = "/tmp/backup"
BACKUP_NODE_IP = "127.0.0.1"
BACKUP_NODE_SSH_USER = "root"
BACKUP_NODE_SSH_PWD = "password"
