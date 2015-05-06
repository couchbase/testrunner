NS_SERVER_TIMEOUT = 120
STANDARD_BUCKET_PORT = 11217
COUCHBASE_SINGLE_DEFAULT_INI_PATH = "/opt/couchbase/etc/couchdb/default.ini"
MEMBASE_DATA_PATH = "/opt/membase/var/lib/membase/data/"
MEMBASE_VERSIONS = ["1.5.4", "1.6.5.4-win64", "1.7.0", "1.7.1", "1.7.1.1", "1.7.2"]
COUCHBASE_DATA_PATH = "/opt/couchbase/var/lib/couchbase/data/"
# remember update WIN_REGISTER_ID also when update COUCHBASE_VERSION
COUCHBASE_VERSIONS = ["1.8.0r", "1.8.0", "1.8.1", "2.0.0", "2.0.1", "2.0.2", "2.1.0",
                      "2.1.1", "2.2.0", "2.2.1", "2.5.0", "2.5.1", "2.5.2", "3.0.0",
                      "3.0.1", "3.0.2", "3.0.3", "3.5.0", "4.0.0"]
COUCHBASE_VERSION_2 = ["2.0.0", "2.0.1", "2.0.2", "2.1.0", "2.1.1", "2.2.0", "2.2.1",
                       "2.5.0", "2.5.1", "2.5.2"]
COUCHBASE_VERSION_3 = ["3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.5.0"]
COUCHBASE_VERSION_4 = ["4.0.0"]
WIN_CB_VERSION_3 = ["3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.5.0"]
SHERLOCK_VERSION = ["3.5.0", "4.0.0"]
WIN_MEMBASE_DATA_PATH = '/cygdrive/c/Program\ Files/Membase/Server/var/lib/membase/data/'
WIN_COUCHBASE_DATA_PATH = '/cygdrive/c/Program\ Files/Couchbase/Server/var/lib/couchbase/data/'
WIN_CB_PATH = "/cygdrive/c/Program Files/Couchbase/Server/"
WIN_MB_PATH = "/cygdrive/c/Program Files/Membase/Server/"
LINUX_CB_PATH = "/opt/couchbase/"
WIN_REGISTER_ID = {"1654":"70668C6B-E469-4B72-8FAD-9420736AAF8F", "170":"AF3F80E5-2CA3-409C-B59B-6E0DC805BC3F",
                   "171":"73C5B189-9720-4719-8577-04B72C9DC5A2", "1711":"73C5B189-9720-4719-8577-04B72C9DC5A2",
                   "172":"374CF2EC-1FBE-4BF1-880B-B58A86522BC8", "180":"D21F6541-E7EA-4B0D-B20B-4DDBAF56882B",
                   "181":"A68267DB-875D-43FA-B8AB-423039843F02", "200":"9E3DC4AA-46D9-4B30-9643-2A97169F02A7",
                   "201":"4D3F9646-294F-4167-8240-768C5CE2157A", "202":"7EDC64EF-43AD-48BA-ADB3-3863627881B8",
                   "210":"7EDC64EF-43AD-48BA-ADB3-3863627881B8", "211":"6B91FC0F-D98E-469D-8281-345A08D65DAF",
                   "220":"CC4CF619-03B8-462A-8CCE-7CA1C22B337B", "221":"3A60B9BB-977B-0424-2955-75346C04C586",
                   "250":"22EF5D40-7518-4248-B932-4536AAB7293E", "251":"AB8A4E81-D502-AE14-6979-68E4C4658CF7",
                   "252":"6E10D93C-76E0-DCA4-2111-73265D001F56",
                   "300":"3D361F67-7170-4CB4-494C-3E4E887BC0B3", "301":"3D361F67-7170-4CB4-494C-3E4E887BC0B3",
                   "302":"DD309984-2414-FDF4-11AA-85A733064291", "303":"0410F3F3-9F5F-5614-51EC-7DC9F7050055",
                   "350":"24D9F882-481C-2B04-0572-00B273CE17B3", "400": "24D9F882-481C-2B04-0572-00B273CE17B3"}
""" This "220":"CC4CF619-03B8-462A-8CCE-7CA1C22B337B" is for build 2.2.0-821 and earlier
    The new build register ID for 2.2.0-837 id is set in create_windows_capture_file in remote_util
    old "211":"7EDC64EF-43AD-48BA-ADB3-3863627881B8"
    current at 2014.12.03 "211":"6B91FC0F-D98E-469D-8281-345A08D65DAF" """
VERSION_FILE = "VERSION.txt"
MIN_COMPACTION_THRESHOLD = 2
MAX_COMPACTION_THRESHOLD = 100
MIN_TIME_VALUE = 0
MAX_TIME_MINUTE = 59
MAX_TIME_HOUR = 23
NUM_ERLANG_THREADS = 16
LINUX_COUCHBASE_BIN_PATH = "/opt/couchbase/bin/"
WIN_COUCHBASE_BIN_PATH = '/cygdrive/c/Program\ Files/Couchbase/Server/bin/'
WIN_COUCHBASE_BIN_PATH_RAW = 'C:\Program Files\Couchbase\Server\\bin\\'
WIN_TMP_PATH = '/cygdrive/c/tmp/'
MAC_COUCHBASE_BIN_PATH = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin/"
MAC_CB_PATH = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/"
LINUX_COUCHBASE_LOGS_PATH = '/opt/couchbase/var/lib/couchbase/logs'
WIN_COUCHBASE_LOGS_PATH = '/cygdrive/c/Program\ Files/Couchbase/Server/var/lib/couchbase/logs/'
MISSING_UBUNTU_LIB = ["libcurl3"]
LINUX_GOPATH = '/root/tuq/gocode'
WINDOWS_GOPATH = '/cygdrive/c/tuq/gocode'
LINUX_GOROOT = '/root/tuq/go'
WINDOWS_GOROOT = '/cygdrive/c/Go'
LINUX_STATIC_CONFIG = '/opt/couchbase/etc/couchbase/static_config'
LINUX_LOG_PATH = '/opt'
LINUX_CAPI_INI = '/opt/couchbase/etc/couchdb/default.d/capi.ini'
LINUX_CONFIG_FILE = '/opt/couchbase/var/lib/couchbase/config/config.dat'
LINUX_MOXI_PATH = '/opt/moxi/bin/'
LINUX_CW_LOG_PATH = "/opt/couchbase/var/lib/couchbase/tmp/"
LINUX_DISTRIBUTION_NAME = ["ubuntu", "centos", "red hat", "opensuse", "oracle linux"]
RPM_DIS_NAME = ["centos", "red hat", "opensuse", "oracle linux"]
MAC_CW_LOG_PATH = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/var/lib/couchbase/tmp"
WINDOWS_CW_LOG_PATH = "c:/Program Files/Couchbase/Server/var/lib/couchbase/tmp/"
CLI_COMMANDS = ["cbbackup", "cbbrowse_logs", "cbcollect_info", "cbcompact", "cbdump-config", "cbenable_core_dumps.sh", \
                "cbepctl", "cbhealthchecker", "cbrecovery", "cbreset_password", "cbrestore", "cbsasladm", "cbstats", \
                "cbtransfer", "cbvbucketctl", "cbworkloadgen", "couchbase-cli", "couchbase-server", "couch_compact", \
                "couchdb", "couch_dbdump", "couch_dbinfo", "couchjs", "couch_view_file_merger", "couch_view_file_sorter", \
                "couch_view_group_cleanup", "couch_view_group_compactor", "couch_view_index_builder", "couch_view_index_updater", \
                "ct_run", "curl", "curl-config", "derb", "dialyzer", "dump-guts", "epmd", "erl", "erlc", "escript", "genbrk", \
                "gencfu", "gencnval", "genctd", "generate_cert", "genrb", "icu-config", "install", "makeconv", "mctimings", \
                "memcached", "moxi", "reports", "sigar_port", "sqlite3", "to_erl", "tools", "typer", "uconv", "vbmap"]
# old url MV_LATESTBUILD_REPO = "http://builds.hq.northscale.net/latestbuilds/"
MV_LATESTBUILD_REPO = "http://latestbuilds.hq.couchbase.com/"
SHERLOCK_BUILD_REPO = "http://latestbuilds.hq.couchbase.com/couchbase-server/sherlock/"
COUCHBASE_REPO = "http://latestbuilds.hq.couchbase.com/couchbase-server/"
MC_BIN_CLIENT = "mc_bin_client"
TESTRUNNER_CLIENT = "testrunner_client"
