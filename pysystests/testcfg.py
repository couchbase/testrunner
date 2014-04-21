#TODO: allow workers to pull this from cache

RABBITMQ_IP =""
RABBITMQ_LOG_LOCATION = ""
CB_CLUSTER_TAG = ""

# Add location of cluster's .ini(from where this script is run) for installation
CLUSTER_INI = ""
CLUSTER_RAM_QUOTA = ""

# Add cluster setup json path, templates in triple quoted strings
SETUP_JSON = "/root/systest-worker/testrunner/pysystests/tests/xdcr/kv_xdcr_setup.js"
SETUP_TEMPLATES = [r"""--name "default" --kvpair '"email":"$str10@couchbase.com" ' \
'"city":"$str5"' '"list":["$int1","$str1","$fl o1"]' '"map":{"sample" : "$str3", "complex" : "$fl o1", "val" : "$int2"}' \
'"num":"$int"' '"fl o":"$fl o"' '"st":"$str"' '"li":"$lis"' '"di":"$dic"' '"b":"$boo"' --size 128 256 512"""]

# Add main system test json path and doc templates in triple quoted strings
TEST_JSON = "/root/systest-worker/testrunner/pysystests/tests/xdcr/sample_load.js"
TEST_TEMPLATES = [r"""--name "default" --kvpair '"email":"$str10@couchbase.com" ' \
'"city":"$str5"' '"list":["$int1","$str1","$fl o1"]' '"map":{"sample" : "$str3", "complex" : "$fl o1", "val" : "$int2"}' \
'"num":"$int"' '"fl o":"$fl o"' '"st":"$str"' '"li":"$lis"' '"di":"$dic"' '"b":"$boo"' --size 128 256 512"""]

OBJECT_CACHE_IP = ""
OBJECT_CACHE_PORT = "11911"

# Add CBMonitor IP, absolute dir path and log location
SERIESLY_IP = ''
SERIESLY_LOCATION = "/opt/gocode/bin"
CBMONITOR_HOME_DIR = "/root/cbmonitor"
SERIESLY_DB_LOCATION = "/root/db"

# Must-fill params
COUCHBASE_IP = ''
COUCHBASE_PORT = '8091'
COUCHBASE_USER = "Administrator"
COUCHBASE_PWD = "password"
COUCHBASE_OS = "linux" # linux|windows|unix

SSH_USER = "root"
SSH_PASSWORD = "couchbase"
WORKERS = ['172.23.106.3']
# valid configs ["kv","query","admin"] or ["all"]
WORKER_CONFIGS = ["all"]
WORKER_PYSYSTESTS_PATH = "/root/systest-worker/testrunner/pysystests"

# CHANGE THIS! to match all clusters will be managed by this worker
# ex. CLUSTER_IPS = ["127.0.0.1:9000","127.0.0.1:9001","127.0.0.1:9002","127.0.0.1:9003"]
CLUSTER_IPS = ["172.23.105.44","172.23.105.45","172.23.105.47","172.23.105.48",
               "172.23.105.49","172.23.105.50","172.23.105.51","172.23.105.52"]
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

LOGDIR="logs"  # relative to current dir


#Backup Config
ENABLE_BACKUPS = False
BACKUP_DIR = "/tmp/backup"
BACKUP_NODE_IP = "127.0.0.1"
BACKUP_NODE_SSH_USER = "root"
