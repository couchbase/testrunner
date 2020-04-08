#TODO: allow workers to pull this from cache

RABBITMQ_IP = "10.1.2.77"
RABBITMQ_LOG_LOCATION = ""
CB_CLUSTER_TAG = "n1ql"

# Add location of cluster's .ini(from where this script is run) for installation
CLUSTER_INI = "/tmp/system_config.ini"
CLUSTER_RAM_QUOTA = "22000"

# Add cluster setup json path, templates in triple quoted strings
SETUP_JSON = "/root/systest-worker/testrunner/pysystests/tests/n1ql/n1ql_setup.js"
SETUP_TEMPLATES = [r"""--name "default" --kvpair '"email":"$str10@couchbase.com" ' \
'"city":"$str5"' '"list":["$int1","$str1","$fl o1"]' '"map":{"sample" : "$str3", "complex" : "$fl o1", "val" : "$int2"}' \
'"num":"$int"' '"fl o":"$fl o"' '"st":"$str"' '"li":"$lis"' '"di":"$dic"' '"b":"$boo"' --size 512""",
 r"""--name "template512" --kvpair '"email":"$str10@couchbase.com" ' '"city":"$str5"'  '"list":["$int1","$str1","$flo1"]' \
 '"map":{"sample" : "$str3", "complex" : "$flo1", "val" : "$int2"}' '"num":"$int"' '"flo":"$flo"' '"st":"$str"' \
  '"li":"$lis"' '"di":"$dic"' '"b":"$boo"' --size 512"""]
# Add main system test json path and doc templates in triple quoted strings
TEST_JSON = "/root/systest-worker/testrunner/pysystests/tests/n1ql/n1ql_sys_test.js"

TEST_TEMPLATES = [r"""--name "default" --kvpair '"email":"$str10@couchbase.com" ' \
'"city":"$str5"' '"list":["$int1","$str1","$fl o1"]' '"map":{"sample" : "$str3", "complex" : "$fl o1", "val" : "$int2"}' \
'"num":"$int"' '"fl o":"$fl o"' '"st":"$str"' '"li":"$lis"' '"di":"$dic"' '"b":"$boo"' --size 128 256 512""",
 r"""--name "template512" --kvpair '"email":"$str10@couchbase.com" ' '"city":"$str5"'  '"list":["$int1","$str1","$flo1"]' \
 '"map":{"sample" : "$str3", "complex" : "$flo1", "val" : "$int2"}' '"num":"$int"' '"flo":"$flo"' '"st":"$str"' \
  '"li":"$lis"' '"di":"$dic"' '"b":"$boo"' --size 512"""]
OBJECT_CACHE_IP = "10.1.2.80"
OBJECT_CACHE_PORT = "11911"

# Add CBMonitor IP, absolute dir path and log location
SERIESLY_IP = '10.1.2.82'
SERIESLY_LOCATION = "/opt/go/bin"
CBMONITOR_HOME_DIR = "/root/cbmonitor"
SERIESLY_DB_LOCATION = "/root/db"

# Must-fill params
COUCHBASE_IP = '10.6.2.164'
COUCHBASE_PORT = '8091'
COUCHBASE_USER = "Administrator"
COUCHBASE_PWD = "password"
COUCHBASE_OS = "windows" # linux|windows|unix

COUCHBASE_SSH_USER = "root"
COUCHBASE_SSH_PASSWORD = "couchbase"
SSH_USER = "root"
SSH_PASSWORD = "couchbase"
WORKERS = ['10.1.2.80']
# valid configs ["kv","query","admin"] or ["all"]
WORKER_CONFIGS = ["all"]
WORKER_PYSYSTESTS_PATH = "/root/systest-worker/testrunner/pysystests"

# CHANGE THIS! to match all clusters will be managed by this worker
CLUSTER_IPS = ["10.6.2.237", "10.6.2.164", "10.6.2.167", "10.6.2.233", "10.6.2.234", "10.6.2.194", "10.6.2.195", "10.6.2.168", "10.6.2.232", "10.6.2.237", "10.6.2.238"]

LOGDIR = "logs"  # relative to current dir


#Backup Config
ENABLE_BACKUPS = False
BACKUP_DIR = "/tmp/backup"
BACKUP_NODE_IP = "127.0.0.1"
BACKUP_NODE_SSH_USER = "root"
