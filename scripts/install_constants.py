USAGE = """\
            Syntax: new_install.py [options]

            Options:
             -p <param=val,...> Comma-separated key=value info
             -i <file>          Path to .ini file containing cluster information

            Available params:
             debug_logs=False                               Print debug logs
             install_tasks=uninstall-install-init-cleanup   Pick 1 or more tasks
             v, version=<numeric version>                   Example: "6.5.0-1234".
             url=<build url>                                Example: "http://172.23.126.166/builds/latestbuilds/couchbase-server/mad-hatter/1234/couchbase-server-enterprise-6.5.0-1234-centos7.x86_64.rpm
             edition, type=enterprise                       CB edition, community or enterprise
             timeout=600                                    End install after timeout seconds
             storage_mode=plasma                            Sets indexer storage mode
             enable_ipv6=False                              Enable ipv6 mode in ns_server

            Examples:
             new_install.py -i /tmp/test.ini -p install_tasks=uninstall,debug_logs=true
             new_install.py -i /tmp/test.ini -p url=http://...,timeout=100
             new_install.py -i /tmp/test.ini -p version=6.5.0-1234,enable_ipv6=True
            """
DEFAULT_INSTALL_TASKS = ["uninstall", "download_build", "install", "init", "cleanup"]
DEFAULT_INSTALL_COLUMNAR_TASKS = ["uninstall","download_build", "install", "cleanup"]
REINIT_NODE_TASKS = ["init", "cleanup"]
SUPPORTED_PRODUCTS = ["couchbase", "couchbase-server", "cb"]
AMAZON = ["amzn2","al2023"]
CENTOS = ["centos6", "centos7", "centos8"]
DEBIAN = ["debian","debian8", "debian9", "debian10", "debian11", "debian12", "debian13"]
OEL = ["oel7", "oel8","oel9"]
RHEL = ["rhel7", "rhel8", "rhel9", "rhel10"]
SUSE = ["suse12", "suse15"]
UBUNTU = ["ubuntu16.04", "ubuntu18.04", "ubuntu20.04","ubuntu22.04", "ubuntu24.04"]
ALMA = ["alma9"]
ROCKY = ['rocky9', 'rocky10']
MARINER = ["mariner2"]
LINUX_DISTROS = AMAZON + CENTOS + DEBIAN + OEL + RHEL + SUSE + UBUNTU + ALMA + ROCKY + MARINER
MACOS_VERSIONS = ["10.13", "10.14", "10.15", "11.1", "11.2", "11.3", "12.3", "14.2", "macos"]
WINDOWS_SERVER = ["2016", "2019", "2022", "windows"]
SUPPORTED_OS = LINUX_DISTROS + MACOS_VERSIONS + WINDOWS_SERVER
X86 = CENTOS + SUSE + RHEL + OEL + AMAZON + ALMA + ROCKY + MARINER
LINUX_AMD64 = DEBIAN + UBUNTU
AMD64 = DEBIAN + UBUNTU + WINDOWS_SERVER
DEBUG_INFO_SUPPORTED = CENTOS + SUSE + RHEL + OEL + AMAZON + DEBIAN + UBUNTU + ALMA + ROCKY

DOWNLOAD_DIR = {"LINUX_DISTROS": "/tmp/",
                "MACOS_VERSIONS": "~/Downloads/",
                "WINDOWS_SERVER": "/cygdrive/c/tmp/"
                }
NON_ROOT_DOWNLOAD_DIR = {"LINUX_DISTROS": "/home/nonroot/",
                "MACOS_VERSIONS": "~/Downloads/",
                "WINDOWS_SERVER": "/cygdrive/c/tmp/"
                }

DEFAULT_INSTALL_DIR = {"LINUX_DISTROS": "/opt/couchbase",
                       "MACOS_VERSIONS": "/Applications/Couchbase\ Server.app",
                       "WINDOWS_SERVER": "/cygdrive/c/Program\ Files/Couchbase/Server",
                       "LINUX_DISTROS_EA": "/opt/enterprise-analytics"}
DEFAULT_NONROOT_INSTALL_DIR = {"LINUX_DISTROS": "/home/nonroot/cb/opt/couchbase/",
                       "MACOS_VERSIONS": "/Applications/Couchbase\ Server.app",
                       "WINDOWS_SERVER": "/cygdrive/c/Program\ Files/Couchbase/Server",
                       "LINUX_DISTROS_EA": "/home/nonroot/cb/opt/enterprise-analytics/"}

CB_NON_PACKAGE_INSTALLER_URL = "https://packages.couchbase.com/cb-non-package-installer/cb-non-package-installer"
CB_NON_PACKAGE_INSTALLER_NAME = "cb-non-package-installer"

DEFAULT_CLI_PATH = \
    {
        "LINUX_DISTROS": DEFAULT_INSTALL_DIR["LINUX_DISTROS"] + "/bin/couchbase-cli",
        "MACOS_VERSIONS": DEFAULT_INSTALL_DIR[
                              "MACOS_VERSIONS"] + "/Contents/Resources/couchbase-core/bin/couchbase-cli",
        "WINDOWS_SERVER": DEFAULT_INSTALL_DIR["WINDOWS_SERVER"] + "/bin/couchbase-cli"
    }
DEFAULT_NONROOT_CLI_PATH = \
    {
        "LINUX_DISTROS": DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"] + "bin/couchbase-cli",
        "MACOS_VERSIONS": DEFAULT_NONROOT_INSTALL_DIR[
                              "MACOS_VERSIONS"] + "/Contents/Resources/couchbase-core/bin/couchbase-cli",
        "WINDOWS_SERVER": DEFAULT_NONROOT_INSTALL_DIR["WINDOWS_SERVER"] + "/bin/couchbase-cli"
    }

WGET_CMD = "cd {0}; wget -Nq -O {1} {2}"
CURL_CMD = "curl {0} -o {1} -z {1} -s -m 30"
LOCAL_BUILD_SIZE_CMD = "cd {} && wc -c {}"
CB_ENTERPRISE = "couchbase-server-enterprise"
CB_COMMUNITY = "couchbase-server-community"
CB_ENTERPRISE_ANALYTICS = "enterprise-analytics"
CB_EDITIONS = [CB_COMMUNITY, CB_ENTERPRISE]
CB_DOWNLOAD_SERVER = "172.23.126.166"

WIN_BROWSERS = ["MicrosoftEdge.exe", "iexplore.exe"]
RETAIN_NUM_BINARIES_AFTER_INSTALL = "2"

CBFT_ENV_OPTIONS = \
    {
        "fts_query_limit":
            "sed -i 's/export PATH/export PATH\\nexport CBFT_ENV_OPTIONS=bleveMaxResultWindow={0}/' /opt/couchbase/bin/couchbase-server; "
            "grep bleveMaxResultWindow={0} /opt/couchbase/bin/couchbase-server > /dev/null && echo 1 || echo 0"
    }

PROCESSES_TO_TERMINATE = ["beam.smp", "memcached", "vbucketmigrator", "couchdb", "epmd", "memsup", "cpu_sup",
                          "goxdcr", "erlang", "eventing", "erl", "godu", "goport", "gosecrets", "projector"]

UNMOUNT_NFS_CMD = "umount -a -t nfs,nfs4 -f -l;"

# server profile change
RM_CONF_PROFILE_FILE = {
    "LINUX_DISTROS": "rm -f /etc/couchbase.d/config_profile"
}
CREATE_PROFILE_FILE = {
    "LINUX_DISTROS": "mkdir -p /etc/couchbase.d ; "
                     "echo %s > /etc/couchbase.d/config_profile ; "
                     "chmod ugo+r /etc/couchbase.d/"
}
# End of profile change

CMDS = {
    "deb": {
        "uninstall":
            "rm -rf /tmp/tmp* ; " +
            "rm -rf /tmp/cbbackupmgr-staging;" +
            "rm -rf /tmp/entbackup*;" +
            "systemctl -q stop couchbase-server;" +
            "systemctl -q stop " + CB_ENTERPRISE_ANALYTICS + ";" +
            UNMOUNT_NFS_CMD +

            # ### Block for fixing ntp service using chrony
            # Disable/remove conflicting services
            "systemctl disable --now systemd-timesyncd 2>/dev/null || true ;"
            "systemctl disable --now ntp 2>/dev/null || true;"
            "apt purge -y ntp 2>/dev/null || true;"
            "apt autoremove -y 2>/dev/null || true;"

            # Restart just in case to make apt-update work
            # which can fail due to TLS errors due to time mismatch
            "systemctl restart systemd-timesyncd ;"
            # Force set the date from a reliable source
            "date -s \"$(curl -k -s --head https://www.google.com | grep ^date: | sed 's/date: //g')\" ;"

            # Install chrony
            "apt update && apt install -y chrony ; "
            # Enable and start chrony
            "systemctl enable --now chrony;"
            # Force immediate sync
            "chronyc makestep;"
            # ### End of block for fixing ntp service issues

            "apt-get purge -y 'couchbase*' > /dev/null; sleep 10;"
            "dpkg --purge $(dpkg -l | grep -e couchbase -e " + CB_ENTERPRISE_ANALYTICS + " | awk '{print $2}'"
            " | xargs echo); sleep 10; "
            "rm /var/lib/dpkg/info/couchbase-*; sleep 10;"
            "rm /var/lib/dpkg/info/" + CB_ENTERPRISE_ANALYTICS + "*; sleep 10;"
            "kill -9 `ps -ef |egrep couchbase|cut -f3 -d' '`;" +
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS"] +
            " > /dev/null && echo 1 || echo 0; "
            "dpkg -P couchbase-server; dpkg -P " + CB_ENTERPRISE_ANALYTICS + "; "
            "rm -rf /var/lib/dpkg/info/couchbase-*;"
            "rm -rf /var/lib/dpkg/info/" + CB_ENTERPRISE_ANALYTICS + "*;"
            "du -ch /data | grep total; rm -rf /data/*;"
            "apt install -y wget curl; "
            "dpkg --configure -a; apt-get update; "
            "journalctl --vacuum-size=100M; journalctl --vacuum-time=10d; "
            "grep 'kernel.dmesg_restrict=0' /etc/sysctl.conf || "
            "(echo 'kernel.dmesg_restrict=0' >> /etc/sysctl.conf "
            "&& service procps restart) ; "
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS"] + ";" +
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS_EA"] + ";",
        "pre_install": "kill -9 `lsof -ti:4369`;" +
                       "kill -9 `lsof -ti:8091`;" +
                       "kill -9 `lsof -ti:21100`;" +
                       "kill -9 `lsof -ti:21200`;" +
                       "kill -9 `lsof -ti:21300`;" +
                       "kill -9 `lsof -ti:21150`;" +
                       "kill -9 `lsof -ti:21250`;" +
                       "kill -9 `lsof -ti:21350`;",
        "install": "DEBIAN_FRONTEND='noninteractive' apt-get -y -f install buildpath > /dev/null && echo 1 || echo 0",
        "post_install": "usermod -aG adm couchbase && systemctl -q is-active couchbase-server.service && echo 1" +
        " || systemctl -q is-active " + CB_ENTERPRISE_ANALYTICS + " && echo 1" +
        " || echo 0",
        "post_install_retry": "systemctl restart couchbase-server.service; systemctl restart " + CB_ENTERPRISE_ANALYTICS + ".service",
        "init": None,
        "cleanup": "ls -td " + DOWNLOAD_DIR["LINUX_DISTROS"] + "couchbase*.deb | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f"
    },
    "dmg": {
        "uninstall":
            "osascript -e 'quit app \"Couchbase Server\"'; "
            "rm -rf " + DEFAULT_INSTALL_DIR["MACOS_VERSIONS"] + "; "
            "rm -rf ~/Library/Application\ Support/Couchbase; "
            "rm -rf ~/Library/Application\ Support/membase; "
            "rm -rf ~/Library/Python/couchbase-py; "
            "launchctl list | grep couchbase-server | xargs -n 3 | cut -f3 -d' ' | xargs -n 1 launchctl stop; "
            "kill -9 `ps -ef |egrep Couchbase | xargs|cut -f1 -d' '`; "
            "umount /Volumes/Couchbase* > /dev/null && echo 1 || echo 0",
        "pre_install": "HDIUTIL_DETACH_ATTACH",
        "install":
            "rm -rf /Applications\Couchbase\ Server.app; "
            "launchctl list | grep couchbase-server | xargs -n 3 | cut -f3 -d' ' | xargs -n 1 launchctl stop; "
            "kill -9 `ps -ef |egrep Couchbase | xargs|cut -f1 -d' '`; "
            "cp -R mountpoint/Couchbase\ Server.app /Applications/Couchbase\ Server.app; "
            "open /Applications/Couchbase\ Server.app > /dev/null && echo 1 || echo 0",
        "post_install": "launchctl list | grep couchbase-server > /dev/null && echo 1 || echo 0",
        "post_install_retry": None,
        "init": None,
        "cleanup": "ls -td " + DOWNLOAD_DIR["MACOS_VERSIONS"] + "couchbase*.dmg | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f"
    },
    "msi": {
        "uninstall":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; "
            "msiexec /x installed-msi /passive",
        "pre_install": "",
        "install":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; "
            "msiexec /i buildbinary /passive /L*V install_status.txt",
        "post_install":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; " +
            "vi +\"set nobomb | set fenc=ascii | x\" install_status.txt; " +
            "grep 'buildversion.*[Configuration\|Installation] completed successfully.' install_status.txt && echo 1 || echo 0",
        "post_install_retry":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; " +
            "msiexec /i buildbinary /passive /L*V install_status.txt",
        "init": None,
        "cleanup": "ls -td " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "couchbase*.msi | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f"
    },
    "rpm": {
        "uninstall":
            UNMOUNT_NFS_CMD +
            "yes | yum remove 'couchbase*' > /dev/null; " +
            "yes | yum remove '" + CB_ENTERPRISE_ANALYTICS + "*' > /dev/null; " +
            "rm -rf /tmp/tmp* ; " +
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS"] + "; " +
            "rm -rf " + DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"] + " > /dev/null && echo 1 || echo 0; " +
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS_EA"] + "; " +
            "du -ch /data | grep total; rm -rf /data/* " +
            "> /dev/null && echo 1 || echo 0",
        "pre_install": "kill -9 `lsof -ti:4369`;" +
                       "kill -9 `lsof -ti:8091`;" +
                       "kill -9 `lsof -ti:21100`;" +
                       "kill -9 `lsof -ti:21200`;" +
                       "kill -9 `lsof -ti:21300`;" +
                       "kill -9 `lsof -ti:21150`;" +
                       "kill -9 `lsof -ti:21250`;" +
                       "kill -9 `lsof -ti:21350`;",
        "install": "yes | yum localinstall -y buildpath > /dev/null && echo 1 || echo 0",
        "set_vm_swappiness_and_thp":
            "/sbin/sysctl vm.swappiness=0; " +
            "echo never > /sys/kernel/mm/transparent_hugepage/enabled; " +
            "echo never > /sys/kernel/mm/transparent_hugepage/defrag; ",
        "suse_install": "registercloudguest --force-new;zypper --no-gpg-checks in -y buildpath > /dev/null && echo 1 || echo 0",
        "suse_uninstall": UNMOUNT_NFS_CMD +
            "zypper --ignore-unknown rm -y 'couchbase*' > /dev/null; " +
            "rm -rf /var/cache/zypper/RPMS/couchbase* ;" +
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS"] + "; " +
            "rm -rf " + DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"] + " > /dev/null && echo 1 || echo 0; " +
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS_EA"] + " " +
            "> /dev/null && echo 1 || echo 0",
        "mariner_install" : "tdnf -y install buildpath > /dev/null && echo 1 || echo 0",
        "mariner_uninstall" : "tdnf -y remove couchbase-server.x86_64 > /dev/null; rm -rf /opt/couchbase",
        "post_install": "systemctl -q is-active couchbase-server && echo 1 || echo 0",
        "post_install_retry": "systemctl daemon-reexec; systemctl restart couchbase-server",
        "init": None,
        "cleanup": "ls -td " + DOWNLOAD_DIR["LINUX_DISTROS"] + "couchbase*.rpm | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f",
    }
}

NON_ROOT_CMDS = {
    "deb": {
        "uninstall":
            UNMOUNT_NFS_CMD +
            "dpkg --purge $(dpkg -l | grep couchbase | awk '{print $2}' | xargs echo); kill -9 `ps -ef |egrep couchbase|cut -f3 -d' '`; " +
            "rm /var/lib/dpkg/info/couchbase-server.*; " +
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS"] + " > /dev/null && echo 1 || echo 0;"
            "rm -rf " + DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"] + " > /dev/null && echo 1 || echo 0;"
            "rm -rf " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb ",
        "pre_install": "kill -9 `lsof -ti:4369`;" +
                       "kill -9 `lsof -ti:8091`;" +
                       "kill -9 `lsof -ti:21100`;" +
                       "kill -9 `lsof -ti:21200`;" +
                       "kill -9 `lsof -ti:21300`;" +
                       "kill -9 `lsof -ti:21150`;" +
                       "kill -9 `lsof -ti:21250`;" +
                       "kill -9 `lsof -ti:21350`;",
        "install":
            "mkdir " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb;"
            "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "; "
            "./{} --install --package buildpath --install-location " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb/",
        "post_install":
            "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb/opt/couchbase/; "
            "./bin/couchbase-server --start",
        "post_install_retry": "./bin/couchbase-server --start",
        "init": None,
        "cleanup": "ls -td " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "couchbase*.deb | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f"
    },
    "dmg": {
        "uninstall":
            "osascript -e 'quit app \"Couchbase Server\"'; "
            "rm -rf " + DEFAULT_INSTALL_DIR["MACOS_VERSIONS"] + "; "
            "rm -rf ~/Library/Application\ Support/Couchbase; "
            "rm -rf ~/Library/Application\ Support/membase; "
            "rm -rf ~/Library/Python/couchbase-py; "
            "launchctl list | grep couchbase-server | xargs -n 3 | cut -f3 -d' ' | xargs -n 1 launchctl stop; "
            "umount /Volumes/Couchbase* > /dev/null && echo 1 || echo 0",
        "pre_install": "HDIUTIL_DETACH_ATTACH",
        "install":
            "rm -rf /Applications\Couchbase\ Server.app; "
            "launchctl list | grep couchbase-server | xargs -n 3 | cut -f3 -d' ' | xargs -n 1 launchctl stop; "
            "cp -R mountpoint/Couchbase\ Server.app /Applications/Couchbase\ Server.app; "
            "open /Applications/Couchbase\ Server.app > /dev/null && echo 1 || echo 0",
        "post_install": "launchctl list | grep couchbase-server > /dev/null && echo 1 || echo 0",
        "post_install_retry": None,
        "init": None,
        "cleanup": "ls -td " + DOWNLOAD_DIR["MACOS_VERSIONS"] + "couchbase*.dmg | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f"
    },
    "msi": {
        "uninstall":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; "
            "msiexec /x installed-msi /passive",
        "pre_install": "",
        "install":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; "
            "msiexec /i buildbinary /passive /L*V install_status.txt",
        "post_install":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; "
            "vi +\"set nobomb | set fenc=ascii | x\" install_status.txt; " +
            "grep 'buildversion.*[Configuration\|Installation] completed successfully.' install_status.txt && echo 1 || echo 0",
        "post_install_retry":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; "
            "msiexec /i buildbinary /passive /L*V install_status.txt",
        "init": None,
        "cleanup": "ls -td " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "couchbase*.msi | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f"
    },
    "rpm": {
        "pre_install":
            "ls -l "+DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"]+"bin/",
        "uninstall":
            NON_ROOT_DOWNLOAD_DIR[
                "LINUX_DISTROS"] + "cb/opt/couchbase/bin/couchbase-server --stop; " +
            UNMOUNT_NFS_CMD +
            DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"]+"bin/couchbase-server -k; kill -9 `ps "
                                                         "-ef |egrep couchbase|cut -f3 -d' '`; " +
            "rm -rf " + DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"] + " > /dev/null && echo 1 || echo 0; " +
            "rm -rf " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb ",
        "install":
            # cb-non-package-installer requires empty dir to extract files to
            "mkdir " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb;"
            "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "; "
            "./cb-non-package-installer --install --package buildpath --install-location " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb/",
        "suse_install":
            "mkdir " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb;"
            "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "; "
            "./{} --install --package buildpath --install-location " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb/",
        "post_install": NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb/opt/couchbase/bin/couchbase-server --start",
        "post_install_retry": None,
        "init": None,
        "cleanup": "rm -f *-diag.zip; ls -td " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "couchbase*.rpm | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f"
    }
}
NON_ROOT_MANUAL_CMDS = {
    "deb": {
        "uninstall":
            UNMOUNT_NFS_CMD +
            "dpkg --purge $(dpkg -l | grep couchbase | awk '{print $2}' | xargs echo); kill -9 `ps -ef |egrep couchbase|cut -f3 -d' '`; " +
            "rm /var/lib/dpkg/info/couchbase-server.*; " +
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS"] + " > /dev/null && echo 1 || echo 0;"
            "rm -rf " + DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"] + " > /dev/null && echo 1 || echo 0;",
        "pre_install": "kill -9 `lsof -ti:4369`;" +
                       "kill -9 `lsof -ti:8091`;" +
                       "kill -9 `lsof -ti:21100`;" +
                       "kill -9 `lsof -ti:21200`;" +
                       "kill -9 `lsof -ti:21300`;" +
                       "kill -9 `lsof -ti:21150`;" +
                       "kill -9 `lsof -ti:21250`;" +
                       "kill -9 `lsof -ti:21350`;",
        "install":
            "dpkg-deb -x buildpath $HOME > /dev/null && echo 1 || echo 0;"
            "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "/opt/couchbase/; "
            "./bin/install/reloc.sh `pwd`  > /dev/null && echo 1 || echo 0; ",
        "post_install":
            "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "/opt/couchbase/; "
            "./bin/couchbase-server -- -noinput -detached",
        "post_install_retry": "./bin/couchbase-server -- -noinput -detached",
        "init": None,
        "cleanup": "ls -td " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "couchbase*.deb | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f"
    },
    "dmg": {
        "uninstall":
            "osascript -e 'quit app \"Couchbase Server\"'; "
            "rm -rf " + DEFAULT_INSTALL_DIR["MACOS_VERSIONS"] + "; "
            "rm -rf ~/Library/Application\ Support/Couchbase; "
            "rm -rf ~/Library/Application\ Support/membase; "
            "rm -rf ~/Library/Python/couchbase-py; "
            "launchctl list | grep couchbase-server | xargs -n 3 | cut -f3 -d' ' | xargs -n 1 launchctl stop; "
            "umount /Volumes/Couchbase* > /dev/null && echo 1 || echo 0",
        "pre_install": "HDIUTIL_DETACH_ATTACH",
        "install":
            "rm -rf /Applications\Couchbase\ Server.app; "
            "launchctl list | grep couchbase-server | xargs -n 3 | cut -f3 -d' ' | xargs -n 1 launchctl stop; "
            "cp -R mountpoint/Couchbase\ Server.app /Applications/Couchbase\ Server.app; "
            "open /Applications/Couchbase\ Server.app > /dev/null && echo 1 || echo 0",
        "post_install": "launchctl list | grep couchbase-server > /dev/null && echo 1 || echo 0",
        "post_install_retry": None,
        "init": None,
        "cleanup": "ls -td " + DOWNLOAD_DIR["MACOS_VERSIONS"] + "couchbase*.dmg | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f"
    },
    "msi": {
        "uninstall":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; "
            "msiexec /x installed-msi /passive",
        "pre_install": "",
        "install":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; "
            "msiexec /i buildbinary /passive /L*V install_status.txt",
        "post_install":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; "
            "vi +\"set nobomb | set fenc=ascii | x\" install_status.txt; " +
            "grep 'buildversion.*[Configuration\|Installation] completed successfully.' install_status.txt && echo 1 || echo 0",
        "post_install_retry":
            "cd " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "; "
            "msiexec /i buildbinary /passive /L*V install_status.txt",
        "init": None,
        "cleanup": "ls -td " + DOWNLOAD_DIR["WINDOWS_SERVER"] + "couchbase*.msi | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f"
    },
    "rpm": {
        "pre_install":
            "ls -l "+DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"]+"bin/",
        "uninstall":
            UNMOUNT_NFS_CMD +
            DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"]+"bin/couchbase-server -k; kill -9 `ps "
                                                         "-ef |egrep couchbase|cut -f3 -d' '`; " +
            "rm -rf " + DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"] + " > /dev/null && echo 1 || echo 0; ",
        "install":
            "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "; "
            "rpm2cpio buildpath | cpio --extract --make-directories --no-absolute-filenames  > /dev/null && echo 1 || echo 0; "
            "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "/opt/couchbase/; "
            "./bin/install/reloc.sh `pwd`  > /dev/null && echo 1 || echo 0; ",
        "suse_install": "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "; "
            "rpm2cpio buildpath | cpio --extract --make-directories --no-absolute-filenames  > /dev/null && echo 1 || echo 0; "
            "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "/opt/couchbase/; "
            "./bin/install/reloc.sh `pwd`  > /dev/null && echo 1 || echo 0; ",
        "post_install": NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "opt/couchbase/bin/couchbase-server \-- -noinput -detached",
        "post_install_retry": None,
        "init": None,
        "cleanup": "rm -f *-diag.zip; ls -td " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "couchbase*.rpm | awk 'NR>" + RETAIN_NUM_BINARIES_AFTER_INSTALL + "' | xargs rm -f"
    }
}

NODE_INIT = {
    "ipv4_hostname": "{0} node-init -c {1} --node-init-hostname {1} -u {2} -p {3} > /dev/null && echo 1 || echo 0;",
    "ipv4": "{0} node-init -c {1} -u {2} -p {3} > /dev/null && echo 1 || echo 0;",
    "ipv6": "{0} node-init -c {1} --node-init-hostname {2} --ipv6 -u {3} -p {4} > /dev/null && echo 1 || echo 0;"
}

INSTALL_TIMEOUT = 600
INSTALL_POLL_INTERVAL = 5

WAIT_TIMES = {
    # unit seconds
    # (<sleep between retries>, <message>, <give up after>)
    "deb": {
        "download_binary": (10, "Waiting {0}s for download to complete on {1}..", 300),
        "uninstall": (10, "Waiting {0}s for uninstall to complete on {1}..", 30),
        "install": (20, "Waiting {0}s for install to complete on {1}..", 100),
        "pre_install": (0, "No need to wait after pre install commands", 0),
        "post_install": (10, "Waiting {0}s for couchbase-service to become active on {1}..", 60),
        "init": (30, "Waiting {0}s for {1} to be initialized..", 300)

    },
    "dmg": {
        "download_binary": (20, "Waiting {0}s for download to complete on {1}..", 300),
        "uninstall": (10, "Waiting {0}s for uninstall to complete on {1}..", 30),
        "pre_install": (30, "Waiting for dmg to be mounted..", 60),
        "install": (50, "Waiting {0}s for install to complete on {1}..", 100),
        "post_install": (10, "Waiting {0}s for couchbase-service to become active on {1}..", 60),
        "init": (30, "Waiting {0}s for {1} to be initialized..", 300)
    },
    "msi": {
        "download_binary": (20, "Waiting {0}s for download to complete on {1}..", 300),
        "uninstall": (10, "Waiting {0}s for uninstall to complete on {1}..", 30),
        "install": (50, "Waiting {0}s for install to complete on {1}..", 150),
        "post_install": (30, "Waiting {0}s for couchbase-service to become active on {1}..", 180),
        "init": (30, "Waiting {0}s for {1} to be initialized..", 300)
    },
    "rpm": {
        "download_binary": (10, "Waiting {0}s for download to complete on {1}..", 300),
        "uninstall": (10, "Waiting {0}s for uninstall to complete on {1}..", 30),
        "install": (20, "Waiting {0}s for install to complete on {1}..", 100),
        "pre_install": (0, "No need to wait after pre install commands", 0),
        "post_install": (10, "Waiting {0}s for couchbase-service to become active on {1}..", 60),
        "pre_install": (20, "Waiting {0}s to remove previous yum repo on {1}..", 60),
        "init": (30, "Waiting {0}s for {1} to be initialized..", 300)
    }
}

DOWNLOAD_CMD = {
    "deb": WGET_CMD,
    "dmg": CURL_CMD,
    "msi": WGET_CMD + " --no-check-certificate",
    "rpm": WGET_CMD
}

FATAL_ERRORS = [
    "Another app is currently holding the yum lock"
]
