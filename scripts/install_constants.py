USAGE = """\
            Syntax: install.py [options]
            
            Options:
             -p <key=val,...> Comma-separated key=value info.
             -i <file>        Path to .ini file containing cluster information.
            
            Available keys:
             product=cb|mb              Used to specify couchbase or membase.
             version=SHORT_VERSION      Example: "2.0.0r-71".
             parallel=false             Useful when you're installing a cluster.
             toy=                       Install a toy build
             init_nodes=False           Initialize nodes
             vbuckets=                  The number of vbuckets in the server installation.
             sync_threads=True          Sync or acync threads(+S or +A)
             erlang_threads=            Number of erlang threads (default=16:16 for +S type)
             upr=True                   Enable UPR replication
             xdcr_upr=                  Enable UPR for XDCR (temporary param until XDCR with UPR is stable), values: None | True | False
             fts_query_limit=1000000    Set a limit for the max results to be returned by fts for any query
             cbft_env_options           Additional fts environment variables
             change_indexer_ports=false Sets indexer ports values to non-default ports
             storage_mode=plasma        Sets indexer storage mode
             enable_ipv6=False          Enable ipv6 mode in ns_server
             ntp=True                   Check if ntp is installed. Default is true. Set ntp=False, in case systemctl is not allowed, such as in docker container
             fts_quota=256              Set quota for fts services.  It must be equal or greater 256.  If fts_quota does not pass,
                                        it will take FTS_QUOTA value in lib/testconstants.py
             debug_logs=false            If you don't want to print install logs, set this param value to false. By default, this value preset to true.
            
            
            Examples:
             install.py -i /tmp/ubuntu.ini -p product=cb,version=2.2.0-792
             install.py -i /tmp/ubuntu.ini -p product=cb,version=2.2.0-792,url=http://builds.hq.northscale.net/latestbuilds....
             install.py -i /tmp/ubuntu.ini -p product=mb,version=1.7.1r-38,parallel=true,toy=keith
             install.py -i /tmp/ubuntu.ini -p product=mongo,version=2.0.2
             install.py -i /tmp/ubuntu.ini -p product=cb,version=0.0.0-704,toy=couchstore,parallel=true,vbuckets=1024
            
             # to run with build with require openssl version 1.0.0
               install.py -i /tmp/ubuntu.ini -p product=cb,version=2.2.0-792,openssl=1
            
             # to install latest release of couchbase server via repo (apt-get and yum)
               install.py -i /tmp/ubuntu.ini -p product=cb,linux_repo=true
            
             # to install non-root non default path, add nr_install_dir params
               install.py -i /tmp/ubuntu.ini -p product=cb,version=5.0.0-1900,nr_install_dir=testnow1
            
            """
DEFAULT_INSTALL_TASKS = ["uninstall", "install", "init", "cleanup"]
SUPPORTED_PRODUCTS = ["couchbase", "couchbase-server", "cb"]
AMAZON = ["amzn2"]
CENTOS = ["centos6", "centos7", "centos8"]
DEBIAN = ["debian8", "debian9", "debian10"]
OEL = ["oel7"]
RHEL = ["rhel8"]
SUSE = ["suse12", "suse15"]
UBUNTU = ["ubuntu16.04", "ubuntu18.04"]
LINUX_DISTROS = AMAZON + CENTOS + DEBIAN + OEL + RHEL + SUSE + UBUNTU
MACOS_VERSIONS = ["10.13.5", "10.13.6", "10.14", "10.15", "macos"]
WINDOWS_SERVER = ["2016", "2019", "windows"]
SUPPORTED_OS = LINUX_DISTROS + MACOS_VERSIONS + WINDOWS_SERVER
X86 = CENTOS + SUSE + RHEL + OEL + AMAZON
AMD64 = DEBIAN + UBUNTU + WINDOWS_SERVER
DOWNLOAD_DIR = {"LINUX_DISTROS": "/tmp/",
                "MACOS_VERSIONS": "~/Downloads/",
                "WINDOWS_SERVER": "/cygdrive/c/tmp/"
                }

DEFAULT_INSTALL_DIR = {"LINUX_DISTROS": "/opt/couchbase",
                       "WINDOWS_SERVER": "/cygdrive/c/Program\ Files/Couchbase/Server/"}

WGET_CMD = "cd {0}; wget -N {1}"
CURL_CMD = "curl {0} -o {1} -z {1} -s -m {2}"
CB_ENTERPRISE = "couchbase-server-enterprise"
CB_COMMUNITY = "couchbase-server-community"
CB_EDITIONS = [CB_COMMUNITY, CB_ENTERPRISE]
CB_DOWNLOAD_SERVER = "172.23.120.24"

WIN_BROWSERS = ["MicrosoftEdge.exe", "iexplore.exe"]

CMDS = {
    "deb": {
        "uninstall": "systemctl stop couchbase-server.service; "
                     "rm /var/lib/dpkg/info/couchbase-server.*; "
                     "rm /var/lib/dpkg/lock; "
                     "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS"] +
                     "; dpkg --remove couchbase-server; ",
        "pre_install": None,
        "install": "dpkg -i buildpath",
        "post_install": "systemctl -q is-active couchbase-server.service && echo 1 || echo 0",
        "post_install_retry": "systemctl restart couchbase-server.service",
        "init": None
    },
    "dmg": {
        "uninstall": "rm -rf /Applications\Couchbase\ Server.app; "
                     "rm -rf ~/Library/Application\ Support/Couchbase; "
                     "rm -rf ~/Library/Application\ Support/membase; "
                     "rm -rf ~/Library/Python/couchbase-py;"
                     "rm -rf /Volumes\Couchbase\ Server*;"
                     "osascript -e 'quit app \"Couchbase Server\"'; ",
        "pre_install": "HDIUTIL_DETACH_ATTACH",
        "install": "cp -R mountpoint/Couchbase\ Server.app /Applications; "
                   "sudo xattr -d -r com.apple.quarantine /Applications/Couchbase\ Server.app; "
                   "open /Applications/Couchbase\ Server.app",
        "post_install": "launchctl list | grep couchbase-server > /dev/null && echo 1 || echo 0",
        "post_install_retry": None,
        "init": None
    },
    "msi": {
        "uninstall": "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; msiexec /x installed-msi /passive",
        "pre_install": "",
        "install": "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; msiexec /i buildbinary /passive /L*V install_status.txt",
        "post_install": "cd " + DOWNLOAD_DIR[
            "WINDOWS_SERVER"] + "; vi +\"set nobomb | set fenc=ascii | x\" install_status.txt; grep 'buildversion.*Configuration completed successfully.' install_status.txt && echo 1 || echo 0",
        "post_install_retry": "cd " + DOWNLOAD_DIR[
            "WINDOWS_SERVER"] + "; msiexec /i buildbinary /passive /L*V install_status.txt",
        # "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; msiexec /fa buildbinary /norestart /Lx install_status.txt ",
        "init": None
    },
    "rpm": {
        "uninstall":
            "systemctl stop couchbase-server; "
            # "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS"] + "; "
            "rm -rf /var/lib/rpm/.rpm.lock; "
            "pkill -u couchbase; "
            "rpm -e couchbase-server",
        "pre_install": None,
        "install": "yes | yum localinstall -y buildpath",
        # "install": "yes | INSTALL_DONT_START_SERVER=1 yum localinstall -y buildpath",
        "suse_install": "rpm -i buildpath",
        "post_install": "systemctl -q is-active couchbase-server && echo 1 || echo 0",
        "post_install_retry": "systemctl daemon-reexec; systemctl restart couchbase-server",
        "init": None,
    }

}

INSTALL_TIMEOUT = 600
INSTALL_POLL_INTERVAL = INSTALL_TIMEOUT // 10

WAIT_TIMES = {
    "msi": {
        "download_binary": 10,
        "install": None,
        "post_install": (30, "Waiting {0}s for couchbase-service to become active on {1}..", 120)
    },
    "rpm": {
        "download_binary": 10,
        "install": 100,
        "post_install": (10, "Waiting {0}s for couchbase-service to become active on {1}..", 60)
    },
    "deb": {
        "download_binary": 10,
        "install": None,  # Install time varies too much
        "post_install": (10, "Waiting {0}s for couchbase-service to become active on {1}..", 60)

    },
    "dmg": {
        "download_binary": 150,
        "pre_install": (10, "Waiting for dmg to be mounted..", 30),
        "install": None,
        "post_install": (10, "Waiting {0}s for couchbase-service to become active on {1}..", 60)
    }

}

DOWNLOAD_CMD = {
    "msi": WGET_CMD,
    "rpm": WGET_CMD,
    "deb": WGET_CMD,
    "dmg": CURL_CMD
}
