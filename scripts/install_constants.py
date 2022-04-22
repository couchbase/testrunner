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
DEFAULT_INSTALL_TASKS = ["uninstall", "install", "init", "cleanup"]
SUPPORTED_PRODUCTS = ["couchbase", "couchbase-server", "cb"]
AMAZON = ["amzn2"]
CENTOS = ["centos6", "centos7", "centos8"]
DEBIAN = ["debian8", "debian9", "debian10", "debian11"]
OEL = ["oel7", "oel8"]
RHEL = ["rhel7", "rhel8"]
SUSE = ["suse12", "suse15"]
UBUNTU = ["ubuntu16.04", "ubuntu18.04", "ubuntu20.04"]
LINUX_DISTROS = AMAZON + CENTOS + DEBIAN + OEL + RHEL + SUSE + UBUNTU
MACOS_VERSIONS = ["10.13", "10.14", "10.15", "11.1", "11.2", "11.3", "12.3", "macos"]
WINDOWS_SERVER = ["2016", "2019", "2022", "windows"]
SUPPORTED_OS = LINUX_DISTROS + MACOS_VERSIONS + WINDOWS_SERVER
X86 = CENTOS + SUSE + RHEL + OEL + AMAZON
AMD64 = DEBIAN + UBUNTU + WINDOWS_SERVER
DEBUG_INFO_SUPPORTED = CENTOS + SUSE + RHEL + OEL + AMAZON + DEBIAN + UBUNTU

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
                       "WINDOWS_SERVER": "/cygdrive/c/Program\ Files/Couchbase/Server"}
DEFAULT_NONROOT_INSTALL_DIR = {"LINUX_DISTROS": "/home/nonroot/cb/opt/couchbase/",
                       "MACOS_VERSIONS": "/Applications/Couchbase\ Server.app",
                       "WINDOWS_SERVER": "/cygdrive/c/Program\ Files/Couchbase/Server"}

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

WGET_CMD = "cd {0}; wget -Nq {1}"
CURL_CMD = "curl {0} -o {1} -z {1} -s -m 30"
LOCAL_BUILD_SIZE_CMD = "cd {} && wc -c {}"
CB_ENTERPRISE = "couchbase-server-enterprise"
CB_COMMUNITY = "couchbase-server-community"
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

CMDS = {
    "deb": {
        "uninstall":
            UNMOUNT_NFS_CMD +
            "apt-get remove -y 'couchbase*' > /dev/null; " +
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS"] + " > /dev/null && echo 1 || echo 0",
        "pre_install": None,
        "install": "DEBIAN_FRONTEND='noninteractive' apt-get -f install buildpath > /dev/null && echo 1 || echo 0",
        "post_install": "systemctl -q is-active couchbase-server.service && echo 1 || echo 0",
        "post_install_retry": "systemctl restart couchbase-server.service",
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
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS"] + "; " +
            "rm -rf " + DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"] + " > /dev/null && echo 1 || echo 0",
        "pre_install": "",
        "install": "yes | yum localinstall -y buildpath > /dev/null && echo 1 || echo 0",
        "set_vm_swappiness_and_thp":
            "/sbin/sysctl vm.swappiness=0; " +
            "echo never > /sys/kernel/mm/transparent_hugepage/enabled; " +
            "echo never > /sys/kernel/mm/transparent_hugepage/defrag; ",
        "suse_install": "zypper --no-gpg-checks in -y buildpath > /dev/null && echo 1 || echo 0",
        "suse_uninstall": UNMOUNT_NFS_CMD +
            "zypper --ignore-unknown rm -y 'couchbase*' > /dev/null; " +
            "rm -rf /var/cache/zypper/RPMS/couchbase* ;" +
            "rm -rf " + DEFAULT_INSTALL_DIR["LINUX_DISTROS"] + "; " +
            "rm -rf " + DEFAULT_NONROOT_INSTALL_DIR["LINUX_DISTROS"] + " > /dev/null && echo 1 || echo 0",
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
        "pre_install": None,
        "install":
            "mkdir " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb;"
            "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "; "
            "./cb-non-package-installer --install --package buildpath --install-location " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb/",
        "post_install":
            "cd " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "/cb/opt/couchbase/; "
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
            "./cb-non-package-installer --install --package buildpath --install-location " + NON_ROOT_DOWNLOAD_DIR["LINUX_DISTROS"] + "cb/",
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
        "pre_install": None,
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
        "post_install": (10, "Waiting {0}s for couchbase-service to become active on {1}..", 60),
        "pre_install": (20, "Waiting {0}s to remove previous yum repo on {1}..", 60),
        "init": (30, "Waiting {0}s for {1} to be initialized..", 300)
    }
}

DOWNLOAD_CMD = {
    "deb": WGET_CMD,
    "dmg": CURL_CMD,
    "msi": WGET_CMD,
    "rpm": WGET_CMD
}

FATAL_ERRORS = [
    "Another app is currently holding the yum lock"
]
