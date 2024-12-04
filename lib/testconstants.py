NS_SERVER_TIMEOUT = 120
STANDARD_BUCKET_PORT = 11217
COUCHBASE_SINGLE_DEFAULT_INI_PATH = "/opt/couchbase/etc/couchdb/default.ini"
MEMBASE_DATA_PATH = "/opt/membase/var/lib/membase/data/"
COUCHBASE_DATA_PATH = "/opt/couchbase/var/lib/couchbase/data/"

CB_VERSION_NAME = {"0.0": "master",
                   "6.0": "alice",
                   "6.5": "mad-hatter", "6.6": "mad-hatter",
                   "7.0": "cheshire-cat",
                   "7.1": "neo", "7.2": "neo",
                   "7.5": "elixir",
                   "7.6": "trinity",
                   "7.7": "cypher",
                   "8.0": "morpheus"}

COLUMNAR_VERSION_NAME = {
    "1.0.0": ["1.0.0"],
    "goldfish": ["1.0.1", "1.0.2", "1.0.3", "1.0.4", "1.0.5"],
    "ionic": ["1.1.0"],
}

CB_RELEASE_BUILDS = {"0.0.0": "0000",
                     "6.0.0": "1693", "6.0.1": "2037", "6.0.2": "2413",
                     "6.0.3": "2895", "6.0.4": "3082", "6.0.5": "3340",
                     "6.5.0": "4960", "6.5.1": "6299", "6.5.2": "6634",
                     "6.6.0": "7909", "6.6.1": "9213", "6.6.2": "9600",
                     "6.6.3": "9808", "6.6.4": "9961", "6.6.5": "10080",
                     "7.0.0": "5302", "7.0.1": "6102", "7.0.2": "6703",
                     "7.0.3": "7031", "7.0.4": "7279",
                     "7.1.0": "2556", "7.1.1": "3175", "7.1.2": "3454",
                     "7.1.3": "3479", "7.1.4": "3601", "7.1.5": "3878",
                     "7.1.6": "3955",
                     "7.2.0": "5325", "7.2.1": "5934", "7.2.2": "6401",
                     "7.2.3": "6705", "7.2.4": "7070", "7.2.5": "0000",
                     "7.2.6": "0000",  "7.5.0": "3000",
                     "7.6.0": "0000", "7.6.1": "0000", "7.6.2": "0000",
                     "7.6.3": "0000", "7.6.4": "0000", "7.7.0": "1000",
                     "8.0.0": "0000"}

MACOS_NAME = {"10.10": "Yosemite", "10.11": "El Capitan", "10.12": "Sierra",
              "10.13": "High Sierra", "10.14": "Mojave", "10.15": "Catalina",
              "12.3": "Monterey", "14.2": "Sonoma"}

SYSTEMD_SERVER = ["centos 8", "centos 7",
                  "suse 12", "suse 15",
                  "ubuntu 16.04", "ubuntu 18.04", "ubuntu 20.04", "ubuntu 22.04", "ubuntu 24.04",
                  "debian 8", "debian 9", "debian 10",  "debian 11",
                  "debian 12", "rhel 8", "rhel 9",
                  "oel 7", "oel 8", "oel 9",
                  "alma 9",
                  "rocky 9",
                  "amazon linux release 2 (karoo)",
                  "amazon linux release 2023 (amazon linux)",
                  "cbl-mariner/linux 2"]

WIN_NUM_ERLANG_PROCESS = 4
WIN_MEMBASE_DATA_PATH = '/cygdrive/c/Program\ Files/Membase/Server/var/lib/membase/data/'
WIN_COUCHBASE_DATA_PATH = '/cygdrive/c/Program\ Files/Couchbase/Server/var/lib/couchbase/data/'
WIN_COUCHBASE_DATA_PATH_RAW = 'c:/Program\ Files/Couchbase/Server/var/lib/couchbase/data/'
WIN_CB_PATH = "/cygdrive/c/Program\ Files/Couchbase/Server/"
WIN_CB_PATH_PARA = "/cygdrive/c/Program Files/Couchbase/Server/"
WIN_MB_PATH = "/cygdrive/c/Program Files/Membase/Server/"
WIN_PROCESSES_KILLED = ["msiexec32.exe", "msiexec.exe", "setup.exe", "ISBEW64.*",
                        "iexplore.*", "WerFault.*", "Firefox.*", "bash.exe",
                        "chrome.exe", "cbq-engine.exe"]
WIN_PROCESSES_SPAWNED = ["backup.exe", "cbas.exe", "cbft.exe", "cbq-engine.exe", "erl.exe", "eventing-producer.exe",
                         "goxdcr.exe", "indexer.exe", "java.exe", "memcached.exe", "projector.exe" "prometheus.exe",
                         "saslauthd-port.exe", "eventing-consumer.exe"]
LINUX_CB_PATH = "/opt/couchbase/"
IPV4_REGEX = "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"
VERSION_FILE = "VERSION.txt"
MIN_COMPACTION_THRESHOLD = 2
MAX_COMPACTION_THRESHOLD = 100
MIN_TIME_VALUE = 0
MAX_TIME_MINUTE = 59
MAX_TIME_HOUR = 23
NUM_ERLANG_THREADS = 16
MIN_KV_QUOTA = 256
INDEX_QUOTA = 256
FTS_QUOTA = 512
EVENTING_QUOTA = 512
CBAS_QUOTA = 1024
""" when we run with small server, it needs to increase cluster quota so that small
    server could have many services in.
    Default value is 0.67
"""
CLUSTER_QUOTA_RATIO = 0.67
LINUX_COUCHBASE_BIN_PATH = "/opt/couchbase/bin/"
LINUX_NONROOT_CB_BIN_PATH = "~/cb/opt/couchbase/bin/"
NR_INSTALL_LOCATION_FILE = "nonroot_install_location.txt"
LINUX_COUCHBASE_PORT_CONFIG_PATH = "/opt/couchbase/etc/couchbase"
LINUX_COUCHBASE_OLD_CONFIG_PATH = "/opt/couchbase/var/lib/couchbase/config/"
LINUX_COUCHBASE_SAMPLE_PATH = "/opt/couchbase/samples/"
LINUX_BACKUP_PATH = "/tmp/backup/"
LINUX_ROOT_PATH = "/root/"
WIN_COUCHBASE_BIN_PATH = "/cygdrive/c/Program\ Files/Couchbase/Server/bin/"
WIN_COUCHBASE_SAMPLE_PATH = "/cygdrive/c/Program\ Files/Couchbase/Server/samples/"
WIN_COUCHBASE_SAMPLE_PATH_C = "c:/Program\ Files/Couchbase/Server/samples/"
WIN_COUCHBASE_BIN_PATH_RAW = 'C:/Program\ Files/Couchbase/Server/bin/'
WIN_COUCHBASE_PORT_CONFIG_PATH = "/cygdrive/c/Program\ Files/couchbase/Server/etc/couchbase"
WIN_COUCHBASE_OLD_CONFIG_PATH = "/cygdrive/c/Program\ Files/couchbase/Server/var/lib/couchbase/config"
WIN_CYGWIN_BIN_PATH = "/cygdrive/c/cygwin64/bin/"
WIN_TMP_PATH = '/cygdrive/c/tmp/'
WIN_TMP_PATH_RAW = 'C:/tmp/'
WIN_BACKUP_C_PATH = "c:/tmp/backup/"
WIN_BACKUP_PATH = "/cygdrive/c/tmp/backup/"
WIN_ROOT_PATH = "/home/Administrator/"
MAC_COUCHBASE_BIN_PATH = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin/"
MAC_COUCHBASE_SAMPLE_PATH = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/samples/"
MAC_CB_PATH = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/"
LINUX_COUCHBASE_LOGS_PATH = '/opt/couchbase/var/lib/couchbase/logs'
WIN_COUCHBASE_LOGS_PATH = '/cygdrive/c/Program\ Files/Couchbase/Server/var/lib/couchbase/logs/'
MISSING_UBUNTU_LIB = ["libcurl3","python-httplib2"]
LINUX_GOPATH = '/root/tuq/gocode'
WINDOWS_GOPATH = '/cygdrive/c/tuq/gocode'
LINUX_GOROOT = '/root/tuq/go'
WINDOWS_GOROOT = '/cygdrive/c/Go'
LINUX_STATIC_CONFIG = '/opt/couchbase/etc/couchbase/static_config'
LINUX_DIST_CONFIG='/opt/couchbase/var/lib/couchbase/config/dist_cfg'
LINUX_LOG_PATH = '/opt'
LINUX_CAPI_INI = '/opt/couchbase/etc/couchdb/default.d/capi.ini'
LINUX_CONFIG_FILE = '/opt/couchbase/var/lib/couchbase/config/config.dat'
LINUX_CW_LOG_PATH = "/opt/couchbase/var/lib/couchbase/tmp/"
LINUX_DISTRIBUTION_NAME = ["ubuntu", "centos", "red hat", "opensuse", "suse", "oracle linux"]
RPM_DIS_NAME = ["centos", "red hat", "opensuse", "suse", "oracle linux"]
MAC_CW_LOG_PATH = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/var/lib/couchbase/tmp"
WINDOWS_CW_LOG_PATH = "c:/Program\ Files/Couchbase/Server/var/lib/couchbase/tmp/"
CLI_COMMANDS = ["cbbackup", "cbbrowse_logs", "cbcollect_info", "cbcompact", "cbdump-config", "cbenable_core_dumps.sh", \
                "cbepctl", "cbhealthchecker", "cbrecovery", "cbreset_password", "cbrestore", "cbsasladm", "cbstats", \
                "cbtransfer", "cbvbucketctl", "cbworkloadgen", "couchbase-cli", "couchbase-server", "couch_compact", \
                "couchdb", "couch_dbdump", "couch_dbinfo", "couchjs", "couch_view_file_merger", "couch_view_file_sorter", \
                "couch_view_group_cleanup", "couch_view_group_compactor", "couch_view_index_builder", "couch_view_index_updater", \
                "ct_run", "curl", "curl-config", "derb", "dialyzer", "dump-guts", "epmd", "erl", "erlc", "escript", "genbrk", \
                "gencfu", "gencnval", "genctd", "generate_cert", "genrb", "icu-config", "install", "makeconv", "mctimings", \
                "memcached", "reports", "sigar_port", "sqlite3", "to_erl", "tools", "typer", "uconv", "vbmap"]
LOG_FILE_NAMES = ['cbcollect_info.log', 'couchbase.log', 'couchstore_local.log', 'ddocs.log', 'diag.log',
                  'ini.log', 'kv_trace.json', 'master_events.log', 'memcached.log',
                  'ns_server.analytics_dcpdebug.log', 'ns_server.analytics_debug.log', 'ns_server.analytics_error.log', \
                  'ns_server.analytics_info.log', 'ns_server.analytics_trace.json', \
                  'ns_server.analytics_warn.log', 'ns_server.babysitter.log', 'ns_server.couchdb.log', 'ns_server.debug.log',
                  'ns_server.error.log', 'ns_server.eventing.log', 'ns_server.fts.log', 'ns_server.goxdcr.log',
                  'ns_server.http_access.log', 'ns_server.http_access_internal.log', 'ns_server.indexer.log',
                  'ns_server.info.log', 'ns_server.json_rpc.log', 'ns_server.mapreduce_errors.log', 'ns_server.metakv.log',
                  'ns_server.ns_couchdb.log', 'ns_server.projector.log', 'ns_server.query.log', 'ns_server.reports.log',
                  'ns_server.stats.log', 'ns_server.views.log', 'ns_server.xdcr_target.log', 'projector_pprof.log',
                  'stats.log', 'syslog.tar.gz', 'systemd_journal.gz', 'users.dets']
# Allow for easy switch to a local mirror of the download stuff
# (for people outside the mountain view office it's nice to be able to
# be running this locally without being on VPN (which my test machines isn't)
CB_DOWNLOAD_SERVER = "172.23.126.166"
CB_DOWNLOAD_SERVER_FQDN = "latestbuilds.service.couchbase.com"
#CB_DOWNLOAD_SERVER = "10.0.0.117:8080"
# test from current version to future version, build number needs to add 30000 to get correct build
FUTURE_BUILD_NUMBER=30000

# old url MV_LATESTBUILD_REPO = "http://builds.hq.northscale.net/latestbuilds/"
MV_LATESTBUILD_REPO = "http://latestbuilds.service.couchbase.com/"
#SHERLOCK_BUILD_REPO = "http://latestbuilds.hq.couchbase.com/couchbase-server/sherlock/"
SHERLOCK_BUILD_REPO = "http://{0}/builds/latestbuilds/couchbase-server/sherlock/".format(CB_DOWNLOAD_SERVER)
#COUCHBASE_REPO = "http://latestbuilds.hq.couchbase.com/couchbase-server/"
COUCHBASE_REPO = "http://{0}/builds/latestbuilds/couchbase-server/".format(CB_DOWNLOAD_SERVER)
CB_LATESTBUILDS_REPO = "http://{0}/builds/latestbuilds/"
#CB_LATESTBUILDS_REPO = "http://latestbuilds.hq.couchbase.com/latestbuilds/"
CB_REPO = "http://{0}/builds/latestbuilds/couchbase-server/".format(CB_DOWNLOAD_SERVER)
CB_COLUMNAR_REPO = "http://{0}/builds/latestbuilds/couchbase-columnar/".format(CB_DOWNLOAD_SERVER)
CB_FQDN_REPO = "http://{0}/builds/latestbuilds/couchbase-server/".format(CB_DOWNLOAD_SERVER_FQDN)
#CB_REPO = "http://latestbuilds.hq.couchbase.com/couchbase-server/"
CB_RELEASE_REPO = "http://{0}/builds/releases/".format(CB_DOWNLOAD_SERVER)
#MV_RELEASE_REPO = "http://latestbuilds.hq.couchbase.com/release"
MC_BIN_CLIENT = "mc_bin_client"
PYTHON_SDK = "python_sdk"
TESTRUNNER_CLIENT = "testrunner_client"
CB_RELEASE_APT_GET_REPO = "http://latestbuilds.service.couchbase.com/couchbase-release/10/couchbase-release-1.0-0.deb"
CB_RELEASE_YUM_REPO = "http://latestbuilds.service.couchbase.com/couchbase-release/10/couchbase-release-1.0-0.noarch.rpm"
DEWIKI = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/dewiki.txt.gz"
ENWIKI = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/enwiki.txt.gz"
ESWIKI = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/eswiki.txt.gz"
FRWIKI = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/frwiki.txt.gz"
NAPADATASET = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/napa_dataset.txt.gz"
GEOJSONDATASET = " https://qebucket.s3.us-west-1.amazonaws.com/testrunner/data/geoshape2.json"
QUERY_5K_FIELDS = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/query_50000_fields.txt"
QUERY_5K_NUM_DATE = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/query_50000_functions_numeric_string_datetime.txt"
QUERY_JOIN = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/queries_joins_50000.txt"
QUERY_SUBQUERY = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/queries_subqueries_1000.txt"
ENT_BKRS = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/entbackup-6.6.0.tar.gz"
ENT_BKRS_FTS = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/entbackup-fts.tgz"
WIN_UNZIP = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/win-cmd/unzip.exe"
WIN_PSSUSPEND = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/win-cmd/pssuspend.exe"
# the maximum number of processes to allow under high_throughput data loading
THROUGHPUT_CONCURRENCY = 4
# determine wether or not to use high throughput
ALLOW_HTP = True
IS_CONTAINER = False
FUZZY_FTS_LARGE_DATASET = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/fuzzy_large_dataset.json"
FUZZY_FTS_SMALL_DATASET = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/fuzzy_small_dataset.json"
GCP_AUTH_PATH = "/root/.config/gcloud/application_default_credentials.json"
FILTER_TEST_FIELDS = ['logs_folder', 'conf_file', 'cluster_name',
                      'ini', 'case_number','num_nodes', 'spec',
                      'last_case_fail', 'teardown_run', 'is_container',
                      'total_testcases', 'get-cbcollect-info']

# AWS compute combination
CAPELLA_AWS_COMPUTES_ORDER = ['m5.xlarge', 'r5.xlarge', 'c5.2xlarge', 'm5.2xlarge', 'r5.2xlarge', 'c5.4xlarge',
                              'm5.4xlarge', 'r5.4xlarge', 'm5.8xlarge', 'r5.8xlarge', 'c5.9xlarge', 'c5.12xlarge',
                              'm5.12xlarge', 'r5.12xlarge', 'm5.16xlarge', 'r5.16xlarge', 'c5.18xlarge',
                              'c5.24xlarge', 'm5.24xlarge', 'r5.24xlarge']
CAPELLA_AWS_COMPUTES = {'m5.xlarge': {'cpu': 4, 'mem': 16},
                        'r5.xlarge': {'cpu': 4, 'mem': 32},
                        'c5.2xlarge': {'cpu': 8, 'mem': 16},
                        'm5.2xlarge': {'cpu': 8, 'mem': 32},
                        'r5.2xlarge': {'cpu': 8, 'mem': 64},
                        'c5.4xlarge': {'cpu': 16, 'mem': 32},
                        'm5.4xlarge': {'cpu': 16, 'mem': 64},
                        'r5.4xlarge': {'cpu': 16, 'mem': 128},
                        'm5.8xlarge': {'cpu': 32, 'mem': 128},
                        'r5.8xlarge': {'cpu': 32, 'mem': 256},
                        'c5.9xlarge': {'cpu': 36, 'mem': 72},
                        'c5.12xlarge': {'cpu': 48, 'mem': 96},
                        'm5.12xlarge': {'cpu': 48, 'mem': 192},
                        'r5.12xlarge': {'cpu': 48, 'mem': 384},
                        'm5.16xlarge': {'cpu': 64, 'mem': 256},
                        'r5.16xlarge': {'cpu': 64, 'mem': 512},
                        'c5.18xlarge': {'cpu': 72, 'mem': 144},
                        'c5.24xlarge': {'cpu': 96, 'mem': 192},
                        'm5.24xlarge': {'cpu': 96, 'mem': 384},
                        'r5.24xlarge': {'cpu': 96, 'mem': 768}}


# ELIXIR Tenant Management Constants
INDEX_MAX_CAP_PER_TENANT = 200
INDEX_SUB_CLUSTER_LENGTH = 2
GSI_MEMORY_LWM = 50
GSI_MEMORY_HWM = 70
GSI_UNITS_LWM = 40
GSI_UNITS_HWM = 60
