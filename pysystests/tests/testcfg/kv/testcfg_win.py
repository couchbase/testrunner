#TODO: allow workers to pull this from cache

RABBITMQ_IP ='172.23.105.161'
RABBITMQ_LOG_LOCATION = ""
CB_CLUSTER_TAG = "default"

# Add location of cluster's .ini for installation
CLUSTER_INI = "/tmp/kv_win.ini"
CLUSTER_RAM_QUOTA = "7000"

# Add main system test json path and doc templates in triple quoted strings
TEST_JSON = "./tests/kv/kv_only_linux_full.js"
TEST_TEMPLATES = [r"""--name "default" --kvpair '"email":"$str10@couchbase.com" ' \
'"city":"$str5"' '"list":["$int1","$str1","$fl o1"]' '"map":{"sample" : "$str3", "complex" : "$fl o1", "val" : "$int2"}' \
'"num":"$int"' '"fl o":"$fl o"' '"st":"$str"' '"li":"$lis"' '"di":"$dic"' '"b":"$boo"' --size 128 256 512""",
 r"""--name "template512" --kvpair '"email":"$str10@couchbase.com" ' '"city":"$str5"'  '"list":["$int1","$str1","$flo1"]' \
 '"map":{"sample" : "$str3", "complex" : "$flo1", "val" : "$int2"}' '"num":"$int"' '"flo":"$flo"' '"st":"$str"' \
  '"li":"$lis"' '"di":"$dic"' '"b":"$boo"' --size 512"""]

OBJECT_CACHE_IP = "172.23.105.172"
OBJECT_CACHE_PORT = "11911"

# Add CBMonitor IP, absolute dir path and log location
SERIESLY_IP = '172.23.105.162'
SERIESLY_LOCATION = "/opt/gocode/bin"
CBMONITOR_HOME_DIR = "/root/cbmonitor"
SERIESLY_DB_LOCATION = "/root/db"

COUCHBASE_IP = '172.23.105.163'
COUCHBASE_PORT = '8091'
COUCHBASE_USER = "Administrator"
COUCHBASE_PWD = "password"
COUCHBASE_OS = "windows" # linux|windows|unix
COUCHBASE_SSH_USER = "Administrator"
COUCHBASE_SSH_PASSWORD = "Membase123"

SSH_USER = "root"
SSH_PASSWORD = "couchbase"
WORKERS = ['172.23.105.173']
# valid configs ["kv","query","admin"] or ["all"]
WORKER_CONFIGS = ["all"]

# CHANGE THIS! to match all clusters will be managed by this worker
# ex. CLUSTER_IPS = ["127.0.0.1:9000","127.0.0.1:9001","127.0.0.1:9002","127.0.0.1:9003"]
CLUSTER_IPS = ["172.23.105.163", "172.23.105.164", "172.23.105.165", "172.23.105.166",
               "172.23.105.167", "172.23.105.168", "172.23.105.169", "172.23.105.170"]
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
