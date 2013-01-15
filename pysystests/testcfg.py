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
# valid configs ["kv","query","admin","stats"] or ["all"]
WORKER_CONFIGS = ["kv"]
CB_CLUSTER_TAG = "default"
ATOP_LOG_FILE = "/tmp/atop-node.log"
LOGDIR="logs"  # relative to current dir

#Backup Config
ENABLE_BACKUPS = False
BACKUP_DIR = "/tmp/backup"
BACKUP_NODE_IP = "127.0.0.1"
BACKUP_NODE_SSH_USER = "root"
BACKUP_NODE_SSH_PWD = "password"
