NS_SERVER_TIMEOUT = 120
STANDARD_BUCKET_PORT = 11217
COUCHBASE_SINGLE_DEFAULT_INI_PATH = "/opt/couchbase/etc/couchdb/default.ini"
MEMBASE_DATA_PATH = "/opt/membase/var/lib/membase/data/"
MEMBASE_VERSIONS = ["1.5.4", "1.6.5.4-win64", "1.7.0", "1.7.1", "1.7.1.1", "1.7.2"]
COUCHBASE_DATA_PATH = "/opt/couchbase/var/lib/couchbase/data/"
# remember update WIN_REGISTER_ID also when update COUCHBASE_VERSION
COUCHBASE_VERSIONS = ["0.0.0",
                      "1.8.0r", "1.8.0", "1.8.1", "2.0.0", "2.0.1", "2.0.2", "2.1.0",
                      "2.1.1", "2.2.0", "2.2.1", "2.5.0", "2.5.1", "2.5.2", "3.0.0",
                      "3.0.1", "3.0.2", "3.0.3", "3.1.0", "3.1.1", "3.1.2", "3.1.3",
                      "3.1.4", "3.1.5", "3.1.6", "3.5.0", "4.0.0", "4.0.1", "4.1.0",
                      "4.1.1", "4.1.2", "4.5.0", "4.5.1", "4.6.0", "4.6.1", "4.6.2",
                      "4.6.3", "4.6.4", "4.6.5", "4.7.0", "5.0.0", "5.0.1", "5.0.2",
                      "5.1.0", "5.1.1", "5.1.2", "5.1.3", "5.5.0", "5.5.1", "5.5.2",
                      "5.5.3", "5.5.4", "5.5.5", "5.5.6", "6.0.0", "6.0.1", "6.0.2",
                      "6.0.3", "6.0.4", "6.0.5", "6.5.0", "6.5.1", "6.5.2", "6.6.0",
                      "6.6.1", "6.6.2", "6.6.3", "6.6.4", "6.6.5", "7.0.0", "7.0.1",
                      "7.0.2", "7.0.3", "7.0.4", "7.1.0", "7.1.1", "7.1.2", "7.1.3",
                      "7.1.4", "7.1.5", "7.1.6", "7.2.0", "7.2.1", "7.2.2", "7.2.3",
                      "7.2.4", "7.2.5", "7.2.6", "7.2.7", "7.2.8",
                      "8.0.0"]
CB_RELEASE_BUILDS = {"0.0.0":"0000",
                     "2.1.1":"764", "2.2.0":"821", "2.5.0":"1059", "2.5.1":"1083",
                     "2.5.2":"1154", "3.0.3":"1716", "3.1.5":"1859", "3.1.6":"1904",
                     "4.0.0":"4051", "4.1.0":"5005", "4.1.1":"5914", "4.1.2":"6088",
                     "4.5.0":"2601", "4.5.1":"2844", "4.6.0":"3573", "4.6.1":"3652",
                     "4.6.2":"3905", "4.6.3":"4136", "4.6.4":"4590", "4.7.0":"0000",
                     "4.6.5":"4742", "5.0.0":"3519", "5.0.1":"5003", "5.0.2":"5509",
                     "5.1.0":"5552", "5.1.1":"5723", "5.1.2":"6030", "5.1.3":"6212",
                     "5.5.0":"2958", "5.5.1":"3511", "5.5.2":"3733", "5.5.3":"4041",
                     "5.5.4":"4338", "5.5.5":"4523", "5.5.6":"0000", "6.0.0":"1693",
                     "6.0.1":"2037", "6.0.2":"2413", "6.0.3":"2895", "6.0.4":"3082",
                     "6.0.5":"3340", "6.5.0":"4960", "6.5.1":"6299", "6.5.2":"6634",
                     "6.6.0":"7909", "6.6.1":"9213", "6.6.2":"9600", "6.6.3":"9808",
                     "6.6.4":"9961", "6.6.5":"10080",
                     "7.0.0":"5302", "7.0.1":"6102", "7.0.2":"6703", "7.0.3":"7031",
                     "7.0.4":"7279", "7.1.0":"2556", "7.1.1":"3175", "7.1.2":"3454",
                     "7.1.3":"3479", "7.1.4":"3601", "7.1.5":"3878", "7.1.6":"3955",
                     "7.2.0":"5325", "7.2.1":"5934", "7.2.2":"6401", "7.2.3":"6705",
                     "7.2.4":"7070", "7.2.5":"0000", "7.2.6":"0000", "7.2.7":"0000",
                     "7.2.8":"0000"}
COUCHBASE_FROM_VERSION_3 = ["3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.1.0", "3.1.1",
                            "3.1.2", "3.1.3", "3.1.4", "3.1.5", "3.1.6", "3.5.0",
                            "4.0.0", "4.0.1", "4.1.0", "4.1.1", "4.1.2", "4.5.0",
                            "4.5.1", "4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4",
                            "4.6.5", "4.7.0", "5.0.0", "5.0.1", "5.0.2", "5.1.0",
                            "5.1.1", "5.1.2", "5.1.3", "5.5.0", "5.5.1", "5.5.2",
                            "5.5.3", "5.5.4", "5.5.5", "5.5.6", "6.0.0", "6.0.1",
                            "6.0.2", "6.0.3", "6.0.4", "6.0.5", "6.5.0", "6.5.1",
                            "6.5.2", "6.6.0", "6.6.1", "6.6.2", "6.6.3", "6.6.4",
                            "6.6.5", "7.0.0", "7.0.1", "7.0.2", "7.0.3", "7.0.4",
                            "7.1.0", "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5",
                            "7.1.6", "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4",
                            "7.2.5", "7.2.6", "7.2.7", "7.2.8",
                            "8.0.0"]
# Update release build here for upgrade tests
COUCHBASE_RELEASE_FROM_VERSION_3 = ["3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.1.0",
             "3.1.1", "3.1.2", "3.1.3", "3.1.5", "4.0.0", "4.1.0", "4.1.1", "4.1.2",
             "4.5.0", "4.5.1", "4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4", "4.6.5",
             "5.0.0", "5.0.1", "5.1.0", "5.1.1", "5.1.2", "5.1.3", "5.5.0", "5.5.1",
             "5.5.2", "5.5.3", "5.5.4", "5.5.5", "5.5.6", "6.0.0", "6.0.1", "6.0.2",
             "6.0.3", "6.0.4", "6.0.5", "6.5.0", "6.5.1", "6.5.2", "6.6.0", "6.6.1",
             "6.6.2", "6.6.3", "6.6.4", "6.6.5", "7.0.0", "7.0.1", "7.0.2", "7.0.3", "7.0.4"]
COUCHBASE_RELEASE_FROM_SPOCK = ["5.0.0", "5.0.1", "5.1.0", "5.1.1", "5.1.2", "5.1.3", "5.5.0",
                                "5.5.1", "5.5.2", "5.5.3", "5.5.4", "5.5.5", "5.5.6", "6.0.0",
                                "6.0.1", "6.0.2", "6.0.3", "6.0.4", "6.0.5", "6.5.0", "6.5.1",
                                "6.6.0", "6.6.1", "6.6.2", "6.6.3"]
COUCHBASE_FROM_VERSION_4 = ["0.0.0",
                            "4.0.0", "4.0.1", "4.1.0", "4.1.1", "4.1.2", "4.5.0",
                            "4.5.1", "4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4",
                            "4.6.5", "4.7.0", "5.0.0", "5.0.1", "5.0.2", "5.1.0",
                            "5.1.1", "5.1.2", "5.1.3", "5.5.0", "5.5.1", "5.5.2",
                            "5.5.3", "5.5.4", "5.5.5", "5.5.6", "6.0.0", "6.0.1",
                            "6.0.2", "6.0.3", "6.0.4", "6.0.5", "6.5.0", "6.5.1",
                            "6.5.2", "6.6.0", "6.6.1", "6.6.2", "6.6.3", "6.6.4",
                            "6.6.5", "7.0.0", "7.0.1", "7.0.2", "7.0.3", "7.0.4",
                            "7.1.0", "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5",
                            "7.1.6", "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4",
                            "7.2.5", "7.2.6", "7.2.7", "8.0.0"]
COUCHBASE_FROM_SHERLOCK = ["4.0.0", "4.0.1", "4.1.0", "4.1.1", "4.1.2", "4.5.0",
                           "4.5.1", "4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4",
                           "4.6.5", "4.7.0", "5.0.0", "5.0.1", "5.0.2", "5.1.0",
                           "5.1.1", "5.1.2", "5.1.3", "5.5.0", "5.5.1", "5.5.2",
                           "5.5.3", "5.5.4", "5.5.5", "5.5.6", "6.0.0", "6.0.1",
                           "6.0.2", "6.0.3", "6.0.4", "6.0.5", "6.5.0", "6.5.1",
                           "6.5.2", "6.6.0", "6.6.1", "6.6.2", "6.6.3", "6.6.4",
                           "6.6.5", "7.0.0", "7.0.1", "7.0.2", "7.0.3", "7.0.4",
                           "7.1.0", "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5",
                           "7.1.6", "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4",
                           "7.2.5", "7.2.6", "7.2.7", "8.0.0"]
COUCHBASE_FROM_4DOT6 = ["4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4", "4.6.5", "4.7.0",
                        "5.0.0", "5.0.1", "5.0.2", "5.1.0", "5.1.1", "5.1.2", "5.1.3",
                        "5.5.0", "5.5.1", "5.5.2", "5.5.3", "5.5.4", "5.5.5", "5.5.6",
                        "6.0.0", "6.0.1", "6.0.2", "6.0.3", "6.0.4", "6.0.5", "6.5.0",
                        "6.5.1", "6.5.2", "6.6.0", "6.6.1", "6.6.2", "6.6.3", "6.6.4",
                        "6.6.5", "7.0.0", "7.0.1", "7.0.2", "7.0.3", "7.0.4", "7.1.0",
                        "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5", "7.1.6", "7.2.0",
                        "7.2.1", "7.2.2", "7.2.3", "7.2.4", "7.2.5", "7.2.6", "7.2.7",
                        "8.0.0"]
COUCHBASE_FROM_WATSON = ["0.0.0",
                         "4.5.0", "4.5.1", "4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4",
                         "4.6.5", "4.7.0", "5.0.0", "5.0.1", "5.0.2", "5.1.0", "5.1.1",
                         "5.1.2", "5.1.3", "5.5.0", "5.5.1", "5.5.2", "5.5.3", "5.5.4",
                         "5.5.5", "5.5.6", "6.0.0", "6.0.1", "6.0.2", "6.0.3", "6.0.4",
                         "6.0.5", "6.5.0", "6.5.1", "6.5.2", "6.6.0", "6.6.1", "6.6.2",
                         "6.6.3", "6.6.4", "6.6.5", "7.0.0", "7.0.1", "7.0.2", "7.0.3",
                         "7.0.4", "7.1.0", "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5",
                         "7.1.6", "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4", "7.2.5",
                         "7.2.6", "7.2.7", "8.0.0"]
"""
    If new version is greater than 5.0.0, we need to add new version to constant
    COUCHBASE_FROM_SPOCK below so that windows will get correct build (msi) to un/install
"""
COUCHBASE_FROM_SPOCK = ["0.0.0", "4.7.0", "5.0.0", "5.0.1", "5.0.2", "5.1.0", "5.1.1",
                        "5.1.2", "5.1.3", "5.5.0", "5.5.1", "5.5.2", "5.5.3", "5.5.4",
                        "5.5.5", "5.5.6", "6.0.0", "6.0.1", "6.0.2", "6.0.3", "6.0.4",
                        "6.0.5", "6.5.0", "6.5.1", "6.5.2", "6.6.0", "6.6.1", "6.6.2",
                        "6.6.3", "6.6.4", "6.6.5", "7.0.0", "7.0.1", "7.0.2", "7.0.3",
                        "7.0.4", "7.1.0", "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5",
                        "7.1.6", "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4", "7.2.5",
                        "7.2.6", "7.2.7", "8.0.0"]

COUCHBASE_FROM_VULCAN = ["0.0.0", "5.5.0", "5.5.1", "5.5.2", "5.5.3", "5.5.4", "5.5.5",
                         "5.5.6", "6.0.0", "6.0.1", "6.0.2", "6.0.3", "6.0.4", "6.0.5",
                         "6.5.0", "6.5.1", "6.5.2", "6.6.0", "6.6.1", "6.6.2", "6.6.3",
                         "6.6.4", "6.6.5", "7.0.0", "7.0.1", "7.0.2", "7.0.3", "7.0.4",
                         "7.1.0", "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5", "7.1.6",
                         "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4", "7.2.5", "7.2.6",
                         "7.2.7", "8.0.0"]
COUCHBASE_FROM_ALICE = ["0.0.0", "6.0.0", "6.0.1", "6.0.2", "6.0.3", "6.0.4", "6.0.5", "6.5.0", "6.5.1",
                        "6.5.2", "6.6.0", "6.6.1", "6.6.2", "6.6.3", "6.6.4", "6.6.5", "7.0.0", "7.0.1",
                        "7.0.2", "7.0.3", "7.0.4", "7.1.0", "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5",
                        "7.1.6", "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4", "7.2.5", "7.2.6", "7.2.7",
                        "8.0.0"]
COUCHBASE_FROM_601 = ["0.0.0", "6.0.1", "6.0.2", "6.0.3", "6.0.4", "6.0.5", "6.5.0", "6.5.1", "6.5.2",
                      "6.6.0", "6.6.1", "6.6.2", "6.6.3", "6.6.4", "6.6.5", "7.0.0", "7.0.1", "7.0.2",
                      "7.0.3", "7.0.4", "7.1.0", "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5", "7.1.6",
                      "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4", "7.2.5", "7.2.6", "7.2.7", "8.0.0"]
# Ubuntu 20.04 support from 6.6.2
COUCHBASE_FROM_662 = ["0.0.0", "6.6.2", "6.6.3", "6.6.4", "6.6.5", "7.0.0", "7.0.1", "7.0.2", "7.0.3", "7.0.4", "7.1.0",
                      "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5", "7.1.6", "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4",
                      "7.2.5", "7.2.6", "7.2.7", "8.0.0"]
COUCHBASE_FROM_MAD_HATTER = ["0.0.0", "6.5.0", "6.5.1", "6.5.2", "6.6.0", "6.6.1", "6.6.2", "6.6.3",
                             "6.6.4", "6.6.5", "7.0.0", "7.0.1", "7.0.2", "7.0.3", "7.0.4", "7.1.0",
                             "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5", "7.1.6", "7.2.0", "7.2.1", "7.2.2",
                             "7.2.3", "7.2.4", "7.2.5", "7.2.6", "7.2.7", "8.0.0"]
# Update release build here for upgrade tests
COUCHBASE_FROM_CHESHIRE_CAT = ["0.0.0", "7.0.0", "7.0.1", "7.0.2", "7.0.3", "7.0.4",
                               "7.1.0", "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5",
                               "7.1.6", "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4",
                               "7.2.5", "7.2.6", "7.2.7", "8.0.0"]
COUCHBASE_FROM_NEO = ["0.0.0", "7.1.0", "7.1.1", "7.1.2", "7.1.3", "7.1.4", "7.1.5",
                      "7.1.6", "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4", "7.2.5",
                      "7.2.6", "7.2.7", "8.0.0"]
COUCHBASE_FROM_ELIXIR = ["0.0.0", "7.2.0", "7.2.1", "7.2.2", "7.2.3", "7.2.4", "7.2.5", "7.2.6", "7.2.7", "8.0.0"]
COUCHBASE_FROM_MORPHEUS = ["0.0.0", "8.0.0"]
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
WATSON_VERSION = ["4.5.0", "4.5.1", "4.6.0", "4.6.1", "4.6.2", "4.6.3", "4.6.4", "4.6.5"]

CB_VERSION_NAME = {"0.0":"master", "4.0":"sherlock", "4.1":"sherlock",
                   "4.5":"watson", "4.6":"watson",
                   "4.7":"spock", "5.0":"spock", "5.1":"spock",
                   "5.5":"vulcan",
                   "6.0":"alice",
                   "6.5":"mad-hatter",
                   "6.6":"mad-hatter",
                   "7.0":"cheshire-cat",
                   "7.1":"neo",
                   "7.2":"neo",
                   "8.0":"morpheus"}

MACOS_NAME = {"10.10":"Yosemite", "10.11":"El Capitan", "10.12":"Sierra", "10.13":"High Sierra",
              "10.14":"Mojave", "10.15":"Catalina", "12.3":"Monterey"}

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
                   "464":"83E54668-E2D2-A014-815C-CE8B51BE15CB",
                   "465":"E38D9F16-B7A2-CC64-0D6C-D9D274D44B4F",
                   "470":"5F8BB367-A796-1104-05DE-00BCD7A787A5",
                   "500":"5F8BB367-A796-1104-05DE-00BCD7A787A5"}

"""                From spock version, we don't need windows product code above
"""
""" This "220":"CC4CF619-03B8-462A-8CCE-7CA1C22B337B" is for build 2.2.0-821 and earlier
    The new build register ID for 2.2.0-837 id is set in create_windows_capture_file
    in remote_util
    old "211":"7EDC64EF-43AD-48BA-ADB3-3863627881B8"
    old one at 2014.12.03 "211":"6B91FC0F-D98E-469D-8281-345A08D65DAF"
    change one more time; current at 2015.11.10 "211":"4D92395A-BB95-4E46-9D95-B7BFB97F7446" """
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
                  'ns_server.analytics_info.log', 'ns_server.analytics_shutdown.log', 'ns_server.analytics_trace.json', \
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
