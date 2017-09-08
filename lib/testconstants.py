NS_SERVER_TIMEOUT = 120
STANDARD_BUCKET_PORT = 11217
COUCHBASE_SINGLE_DEFAULT_INI_PATH = "/opt/couchbase/etc/couchdb/default.ini"
MEMBASE_DATA_PATH = "/opt/membase/var/lib/membase/data/"
MEMBASE_VERSIONS = ["1.5.4", "1.6.5.4-win64", "1.7.0", "1.7.1", "1.7.1.1", "1.7.2"]
COUCHBASE_DATA_PATH = "/opt/couchbase/var/lib/couchbase/data/"
# remember update WIN_REGISTER_ID also when update COUCHBASE_VERSION
COUCHBASE_VERSIONS = ["1.8.0r", "1.8.0", "1.8.1", "2.0.0", "2.0.1", "2.0.2", "2.1.0",
                      "2.1.1", "2.2.0", "2.2.1", "2.5.0", "2.5.1", "2.5.2", "3.0.0",
                      "3.0.1", "3.0.2", "3.0.3", "3.1.0", "3.1.1", "3.1.2", "3.1.3",
                      "3.1.4", "3.1.5", "3.1.6", "3.5.0", "4.0.0", "4.0.1", "4.1.0",
                      "4.1.1", "4.1.2", "4.5.0", "4.5.1", "4.6.0", "4.6.1", "4.6.2",
                      "4.6.3", "4.6.4", "4.7.0", "5.0.0"]
CB_RELEASE_BUILDS = {"2.1.1":"764", "2.2.0":"821", "2.5.0":"1059", "2.5.1":"1083",
                     "2.5.2":"1154","3.0.3":"1716", "3.1.5":"1859","3.1.6":"1904",
                     "4.0.0":"4051", "4.1.0":"5005", "4.1.1":"5914", "4.1.2":"6088",
                     "4.5.0":"2061", "4.5.1":"2844", "4.6.0":"3573", "4.6.1":"3652",
                     "4.6.2":"3905", "4.6.3":"0000", "4.7.0":"0000", "5.0.0":"0000"}
COUCHBASE_FROM_VERSION_3 = ["3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.1.0", "3.1.1",
                            "3.1.2", "3.1.3", "3.1.4", "3.1.5", "3.1.6", "3.5.0",
                            "4.0.0", "4.0.1", "4.1.0", "4.1.1", "4.1.2", "4.5.0",
                            "4.5.1", "4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4",
                            "4.7.0", "5.0.0"]
COUCHBASE_RELEASE_FROM_VERSION_3 = ["3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.1.0",
                                    "3.1.1", "3.1.2", "3.1.3", "3.1.5", "4.0.0",
                                    "4.1.0", "4.1.1", "4.1.2", "4.5.0", "4.5.1",
                                    "4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4"]
COUCHBASE_FROM_VERSION_4 = ["4.0.0", "4.0.1", "4.1.0", "4.1.1", "4.1.2", "4.5.0",
                            "4.5.1", "4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4",
                            "4.7.0", "5.0.0"]
COUCHBASE_FROM_SHERLOCK = ["4.0.0", "4.0.1", "4.1.0", "4.1.1", "4.1.2", "4.5.0",
                           "4.5.1", "4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4",
                           "4.7.0", "5.0.0"]
COUCHBASE_FROM_4DOT6 = ["4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4", "4.7.0", "5.0.0"]
COUCHBASE_FROM_WATSON = ["4.5.0", "4.5.1", "4.6.0", "4.6.1", "4.6.2", "4.6.3",
                         "4.6.4", "4.7.0", "5.0.0"]
COUCHBASE_FROM_SPOCK = ["4.7.0", "5.0.0"]
COUCHBASE_RELEASE_VERSIONS_3 = ["3.0.1", "3.0.1-1444", "3.0.2", "3.0.2-1603", "3.0.3",
                                "3.0.3-1716", "3.1.0", "3.1.0-1797", "3.1.1", "3.1.1-1807",
                                "3.1.2", "3.1.2-1815", "3.1.3", "3.1.3-1823", "3.1.5"]
CE_EE_ON_SAME_FOLDER = ["2.0.0", "2.0.1", "2.0.2", "2.1.0", "2.1.1", "2.2.0",
                        "2.2.1", "2.5.0", "2.5.1", "2.5.2", "3.0.0", "3.0.1"]
COUCHBASE_MP_VERSION = ["3.1.3", "3.1.6"]
COUCHBASE_VERSION_2 = ["2.0.0", "2.0.1", "2.0.2", "2.1.0", "2.1.1", "2.2.0", "2.2.1",
                       "2.5.0", "2.5.1", "2.5.2"]
COUCHBASE_VERSION_2_WITH_REL = ["2.5.0", "2.5.1"]
COUCHBASE_VERSION_3 = ["3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.1.0", "3.1.1", "3.1.2",
                       "3.1.3", "3.1.4", "3.1.5", "3.1.6", "3.5.0"]
WIN_CB_VERSION_3 = ["3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.1.0", "3.1.1","3.1.2",
                    "3.1.3", "3.1.4", "3.1.5", "3.1.6"]
SHERLOCK_VERSION = ["4.0.0", "4.0.1", "4.0", "4.1.0", "4.1", "4.1.1", "4.1.2"]
WATSON_VERSION = ["4.5.0", "4.5.1", "4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4"]
CB_VERSION_NAME = {"4.0":"sherlock", "4.1":"sherlock", "4.5":"watson", "4.6":"watson",
                   "4.7":"spock", "5.0":"spock"}
WIN_MEMBASE_DATA_PATH = '/cygdrive/c/Program\ Files/Membase/Server/var/lib/membase/data/'
WIN_COUCHBASE_DATA_PATH = '/cygdrive/c/Program\ Files/Couchbase/Server/var/lib/couchbase/data/'
WIN_CB_PATH = "/cygdrive/c/Program Files/Couchbase/Server/"
WIN_MB_PATH = "/cygdrive/c/Program Files/Membase/Server/"
WIN_PROCESSES_KILLED = ["msiexec32.exe", "msiexec.exe", "setup.exe", "ISBEW64.*",
                        "iexplore.*", "WerFault.*", "Firefox.*", "bash.exe",
                        "chrome.exe", "cbq-engine.exe"]
LINUX_CB_PATH = "/opt/couchbase/"
WIN_REGISTER_ID = {"1654":"70668C6B-E469-4B72-8FAD-9420736AAF8F",
                   "170":"AF3F80E5-2CA3-409C-B59B-6E0DC805BC3F",
                   "171":"73C5B189-9720-4719-8577-04B72C9DC5A2",
                   "1711":"73C5B189-9720-4719-8577-04B72C9DC5A2",
                   "172":"374CF2EC-1FBE-4BF1-880B-B58A86522BC8",
                   "180":"D21F6541-E7EA-4B0D-B20B-4DDBAF56882B",
                   "181":"A68267DB-875D-43FA-B8AB-423039843F02",
                   "200":"9E3DC4AA-46D9-4B30-9643-2A97169F02A7",
                   "201":"4D3F9646-294F-4167-8240-768C5CE2157A",
                   "202":"7EDC64EF-43AD-48BA-ADB3-3863627881B8",
                   "210":"7EDC64EF-43AD-48BA-ADB3-3863627881B8",
                   "211":"4D92395A-BB95-4E46-9D95-B7BFB97F7446",
                   "220":"CC4CF619-03B8-462A-8CCE-7CA1C22B337B",
                   "221":"3A60B9BB-977B-0424-2955-75346C04C586",
                   "250":"22EF5D40-7518-4248-B932-4536AAB7293E",
                   "251":"AB8A4E81-D502-AE14-6979-68E4C4658CF7",
                   "252":"6E10D93C-76E0-DCA4-2111-73265D001F56",
                   "300":"3D361F67-7170-4CB4-494C-3E4E887BC0B3",
                   "301":"3D361F67-7170-4CB4-494C-3E4E887BC0B3",
                   "302":"DD309984-2414-FDF4-11AA-85A733064291",
                   "303":"0410F3F3-9F5F-5614-51EC-7DC9F7050055",
                   "310":"5E5D7293-AC1D-3424-E583-0644411FDA20",
                   "311":"41276A8D-2A65-88D4-BDCC-8C4FE109F4B8",
                   "312":"F0794D16-BD9D-4638-9EEA-0E591F170BD7",
                   "313":"71C57EAD-8903-0DA4-0919-25A0B17E20F0",
                   "314":"040F761F-28D9-F0F4-99F1-9D572E6EB826",
                   "315":"BC2C3394-12A0-A334-51F0-F1204EC43243",
                   "316":"7E2E785B-3330-1334-01E8-F6E9C27F0B61",
                   "350":"24D9F882-481C-2B04-0572-00B273CE17B3",
                   "400":"24D9F882-481C-2B04-0572-00B273CE17B3",
                   "401":"898C4818-1F6D-C554-1163-6DF5C0F1F7D8",
                   "410":"898C4818-1F6D-C554-1163-6DF5C0F1F7D8",
                   "411":"8A2472CD-C408-66F4-B53F-6797FCBF7F4C",
                   "412":"37571560-C662-0F14-D59B-76867D360689",
                   "450":"A4BB2687-E63E-F424-F9F3-18D739053798",
                   "451":"B457D40B-E596-E1D4-417A-4DD6219B64B0",
                   "460":"C87D9A6D-C189-0C44-F1DF-91ADF99A9CCA",
                   "461":"02225782-B8EE-CC04-4932-28981BC87C72",
                   "462":"C4EC4311-8AE1-28D4-4174-A48CD0291F77",
                   "463":"963479CF-8C24-BB54-398E-0FF6F2A0128C",
                   "470":"5F8BB367-A796-1104-05DE-00BCD7A787A5",
                   "500":"5F8BB367-A796-1104-05DE-00BCD7A787A5"}
""" This "220":"CC4CF619-03B8-462A-8CCE-7CA1C22B337B" is for build 2.2.0-821 and earlier
    The new build register ID for 2.2.0-837 id is set in create_windows_capture_file
    in remote_util
    old "211":"7EDC64EF-43AD-48BA-ADB3-3863627881B8"
    old one at 2014.12.03 "211":"6B91FC0F-D98E-469D-8281-345A08D65DAF"
    change one more time; current at 2015.11.10 "211":"4D92395A-BB95-4E46-9D95-B7BFB97F7446" """
VERSION_FILE = "VERSION.txt"
MIN_COMPACTION_THRESHOLD = 2
MAX_COMPACTION_THRESHOLD = 100
MIN_TIME_VALUE = 0
MAX_TIME_MINUTE = 59
MAX_TIME_HOUR = 23
NUM_ERLANG_THREADS = 16
MIN_KV_QUOTA = 250
INDEX_QUOTA = 512
FTS_QUOTA = 256
LINUX_COUCHBASE_BIN_PATH = "/opt/couchbase/bin/"
LINUX_NONROOT_CB_BIN_PATH = "~/opt/couchbase/bin/"
NR_INSTALL_LOCATION_FILE = "nonroot_install_location.txt"
LINUX_COUCHBASE_PORT_CONFIG_PATH = "/opt/couchbase/etc/couchbase"
LINUX_COUCHBASE_OLD_CONFIG_PATH = "/opt/couchbase/var/lib/couchbase/config/"
LINUX_COUCHBASE_SAMPLE_PATH = "/opt/couchbase/samples/"
LINUX_BACKUP_PATH = "/tmp/backup/"
LINUX_ROOT_PATH = "/root/"
WIN_COUCHBASE_BIN_PATH = "/cygdrive/c/Program\ Files/Couchbase/Server/bin/"
WIN_COUCHBASE_SAMPLE_PATH = "/cygdrive/c/Program\ Files/Couchbase/Server/samples/"
WIN_COUCHBASE_BIN_PATH_RAW = 'C:/Program\ Files/Couchbase/Server/bin/'
WIN_COUCHBASE_PORT_CONFIG_PATH = "/cygdrive/c/Program\ Files/couchbase/Server/etc/couchbase"
WIN_COUCHBASE_OLD_CONFIG_PATH = "/cygdrive/c/Program\ Files/couchbase/Server/var/lib/couchbase/config"
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
LINUX_DISTRIBUTION_NAME = ["ubuntu", "centos", "red hat", "opensuse", "suse", "oracle linux"]
RPM_DIS_NAME = ["centos", "red hat", "opensuse", "suse", "oracle linux"]
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

# Allow for easy switch to a local mirror of the download stuff
# (for people outside the mountain view office it's nice to be able to
# be running this locally without being on VPN (which my test machines isn't)
CB_DOWNLOAD_SERVER = "172.23.120.24"
#CB_DOWNLOAD_SERVER = "10.0.0.117:8080"

# old url MV_LATESTBUILD_REPO = "http://builds.hq.northscale.net/latestbuilds/"
MV_LATESTBUILD_REPO = "http://latestbuilds.hq.couchbase.com/"
#SHERLOCK_BUILD_REPO = "http://latestbuilds.hq.couchbase.com/couchbase-server/sherlock/"
SHERLOCK_BUILD_REPO = "http://{0}/builds/latestbuilds/couchbase-server/sherlock/".format(CB_DOWNLOAD_SERVER)
#COUCHBASE_REPO = "http://latestbuilds.hq.couchbase.com/couchbase-server/"
COUCHBASE_REPO = "http://{0}/builds/latestbuilds/couchbase-server/".format(CB_DOWNLOAD_SERVER)
CB_LATESTBUILDS_REPO = "http://{0}/builds/latestbuilds/"
#CB_LATESTBUILDS_REPO = "http://latestbuilds.hq.couchbase.com/latestbuilds/"
CB_REPO = "http://{0}/builds/latestbuilds/couchbase-server/".format(CB_DOWNLOAD_SERVER)
#CB_REPO = "http://latestbuilds.hq.couchbase.com/couchbase-server/"
CB_RELEASE_REPO = "http://{0}/builds/releases/".format(CB_DOWNLOAD_SERVER)
#MV_RELEASE_REPO = "http://latestbuilds.hq.couchbase.com/release"
MC_BIN_CLIENT = "mc_bin_client"
TESTRUNNER_CLIENT = "testrunner_client"
PYTHON_SDK = "python_sdk"
CB_RELEASE_APT_GET_REPO = "http://latestbuilds.hq.couchbase.com/couchbase-release/10/couchbase-release-1.0-0.deb"
CB_RELEASE_YUM_REPO = "http://latestbuilds.hq.couchbase.com/couchbase-release/10/couchbase-release-1.0-0.noarch.rpm"
DEWIKI = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/dewiki.txt.gz"
ENWIKI = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/enwiki.txt.gz"
ESWIKI = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/eswiki.txt.gz"
FRWIKI = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/frwiki.txt.gz"
QUERY_5K_FIELDS = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/query_50000_fields.txt"
QUERY_5K_NUM_DATE = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/query_50000_functions_numeric_string_datetime.txt"
QUERY_JOIN = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/queries_joins_50000.txt"
QUERY_SUBQUERY = "https://s3-us-west-1.amazonaws.com/qebucket/testrunner/data/queries_subqueries_1000.txt"
# the maximum number of processes to allow under high_throughput data loading
THROUGHPUT_CONCURRENCY = 4
# determine wether or not to use high throughput
ALLOW_HTP = True
IS_CONTAINER = False
