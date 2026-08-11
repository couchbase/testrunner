#!/usr/bin/env python3
"""Install/upgrade Elasticsearch 8.x on a fleet of Debian hosts over SSH.

Implements the Confluence recipe (stop old service, install the 8.17.0 deb, set
xpack.security.enabled=false and http.host=0.0.0.0, restart, clean up), with the
robustness the shell version needs when run unattended against ~40 VMs:

  * Nothing can hang.  A prompting dpkg used to block in read() on paramiko's
    never-closed stdin pipe, forever and silently.  Every command now runs with
    stdin at EOF, DEBIAN_FRONTEND=noninteractive, --force-confnew/--force-confdef,
    a remote `timeout`, and a client-side deadline; output is drained while the
    command runs rather than after it exits.
  * vm.max_map_count is raised to 262144.  ES 8 bootstrap-checks it and the
    Debian 10/11 hosts ship 65530, which the page's steps leave untouched.
  * The security auto-configuration block is removed on fresh installs.  A
    package upgrade skips auto-config, so the page's two settings are enough
    there, but a from-scratch install would otherwise come up on HTTPS with
    auth and refuse plain-HTTP curl.
  * Java is not managed at all: 8.x bundles its own JDK in
    /usr/share/elasticsearch/jdk.  (1.7.3 needed a JRE 7/8; that is gone.)

Usage:
    # every VM (hosts.txt if present, else the built-in list), no prompts
    ES_SSH_PASSWORD=couchbase python3 elastic_install.py --no-prompt

    python3 elastic_install.py                        # prompts; empty input = all VMs
    python3 elastic_install.py --check-only           # report status, change nothing
    python3 elastic_install.py --ips 1.2.3.4          # just this host
    python3 elastic_install.py --es-version 8.18.2    # a different 8.x

Hosts come from --ips-file, then --ips, then ./hosts.txt, then the list below.
Each host is independent: one failure never stops the rest, results stream in as
hosts finish, and the summary ends with a paste-ready command to retry only the
hosts that failed.  Per-host transcripts land in logs/<ip>.log.
"""

import argparse
import os
import re
import shlex
import socket
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from getpass import getpass

warnings.filterwarnings("ignore", module="paramiko")  # quiet the TripleDES notices
import paramiko  # noqa: E402

# Every VM in the pool. Edit hosts.txt instead if you prefer a file — it is used
# automatically when it sits next to this script and no --ips/--ips-file is given.
DEFAULT_IPS = [
    "172.23.122.236", "172.23.122.237",
    "172.23.216.66", "172.23.216.73", "172.23.216.175", "172.23.216.179",
    "172.23.216.180", "172.23.216.181", "172.23.216.185",
    "172.23.217.128", "172.23.217.129", "172.23.217.130", "172.23.217.131",
    "172.23.217.132", "172.23.217.134", "172.23.217.136", "172.23.217.138",
    "172.23.217.141", "172.23.217.142", "172.23.217.144", "172.23.217.145",
    "172.23.217.146", "172.23.217.147", "172.23.217.148", "172.23.217.149",
    "172.23.217.151", "172.23.217.153", "172.23.217.154", "172.23.217.159",
    "172.23.217.160", "172.23.217.161", "172.23.217.162", "172.23.217.163",
    "172.23.217.164", "172.23.217.165", "172.23.217.166", "172.23.217.167",
    "172.23.217.168",
    "172.23.218.120", "172.23.218.122", "172.23.218.125",
]
HOSTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hosts.txt")

ES_VERSION = "8.17.0"
ES_DEB_URL = "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{v}-amd64.deb"
MAX_MAP_COUNT = 262144  # ES 8 bootstrap check; Debian ships 65530
LOG_DIR = "logs"

# Take the package's conffiles instead of prompting. This is what keeps a
# 1.7.3 -> 8.17.0 upgrade from stopping on the "what about elasticsearch.yml?"
# question that wedged the old script.
DPKG_FORCE = "--force-confnew --force-confdef"


# --------------------------------------------------------------------------- #
# remote command plumbing
# --------------------------------------------------------------------------- #

class RemoteError(Exception):
    pass


class Host:
    """One SSH connection, with commands that cannot hang."""

    def __init__(self, ip, username, password, connect_timeout=20):
        self.ip = ip
        self.username = username
        self.password = password
        self.connect_timeout = connect_timeout
        self.client = None
        self.log_path = os.path.join(LOG_DIR, f"{ip}.log")
        os.makedirs(LOG_DIR, exist_ok=True)
        self._log = open(self.log_path, "a", buffering=1)
        self._log.write(f"\n===== run started {time.strftime('%Y-%m-%d %H:%M:%S')} =====\n")

    def connect(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(
            self.ip,
            username=self.username,
            password=self.password,
            timeout=self.connect_timeout,
            banner_timeout=self.connect_timeout + 10,
            auth_timeout=self.connect_timeout + 10,
            look_for_keys=False,
            allow_agent=False,
        )

    def close(self):
        if self.client:
            self.client.close()
        self._log.close()

    def note(self, msg):
        self._log.write(msg.rstrip() + "\n")

    def run(self, script, timeout=120):
        """Run a bash snippet. Returns (exit_code, output). Never blocks forever.

        stdin gets EOF immediately, so anything that tries to prompt (dpkg,
        debconf) fails fast instead of sleeping in read() like the old script's
        `dpkg -i` did.
        """
        wrapped = (
            "export DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true; "
            f"timeout -k 10 {timeout} bash -c {shlex.quote(script)} </dev/null 2>&1"
        )
        stdin, stdout, _ = self.client.exec_command(wrapped, timeout=timeout + 30)
        stdin.channel.shutdown_write()  # EOF on stdin — the fix for the dpkg hang
        chan = stdout.channel
        chan.setblocking(False)

        out = bytearray()
        deadline = time.monotonic() + timeout + 25
        while True:
            moved = False
            while chan.recv_ready():
                out += chan.recv(65536)
                moved = True
            while chan.recv_stderr_ready():
                out += chan.recv_stderr(65536)
                moved = True
            if chan.exit_status_ready() and not moved:
                while chan.recv_ready():
                    out += chan.recv(65536)
                while chan.recv_stderr_ready():
                    out += chan.recv_stderr(65536)
                break
            if time.monotonic() > deadline:
                chan.close()
                text = out.decode(errors="replace")
                self.note(f"[client timeout after {timeout}s]\n{text}")
                return 124, text
            if not moved:
                time.sleep(0.05)

        rc = chan.recv_exit_status()
        text = out.decode(errors="replace")
        self.note(text)
        return rc, text

    def step(self, name, script, timeout=120):
        self.note(f"\n--- step: {name} ---")
        rc, out = self.run(script, timeout=timeout)
        if rc != 0:
            tail = "\n".join(out.strip().splitlines()[-6:])
            raise RemoteError(f"{name} failed (rc={rc}): {tail or '(no output)'}")
        return out


# --------------------------------------------------------------------------- #
# remote scripts (plain bash; keep them idempotent)
# --------------------------------------------------------------------------- #

# Clear a dpkg left wedged on an interactive prompt, then finish any half-done
# transaction. A wedged dpkg is blocked in read() on fd 0 (syscall 0, arg0 0x0)
# with no child process running.
PREFLIGHT = r"""
. /etc/os-release 2>/dev/null; echo "host $(hostname) — $PRETTY_NAME"

for p in $(pgrep -x dpkg 2>/dev/null); do
    sc=$(cut -d' ' -f1,2 /proc/$p/syscall 2>/dev/null)
    kids=$(cat /proc/$p/task/*/children 2>/dev/null | tr -d ' \n')
    if [ "$sc" = "0 0x0" ] && [ -z "$kids" ]; then
        echo "killing dpkg pid $p: blocked on an interactive prompt"
        kill -9 "$p" 2>/dev/null || true
    else
        echo "dpkg pid $p is busy (syscall='$sc'), waiting for it"
        for i in $(seq 1 30); do kill -0 "$p" 2>/dev/null || break; sleep 2; done
    fi
done
if dpkg --audit 2>/dev/null | grep -q .; then
    echo "finishing pending dpkg transaction"
    dpkg --configure -a @@FORCE@@ 2>&1 | tail -5 || true
fi

# curl is only needed for the post-install check; apt is best-effort because the
# Debian 10 hosts point at archive.debian.org, whose Release files are expired.
if ! command -v curl >/dev/null; then
    echo "curl missing; trying apt"
    apt-get update -y -o Acquire::Check-Valid-Until=false >/dev/null 2>&1 || true
    apt-get install -y curl >/dev/null 2>&1 || true
    command -v curl >/dev/null || echo "warning: still no curl; will verify with wget"
fi

# ES 8 bootstrap check: max virtual memory areas. The deb ships
# /usr/lib/sysctl.d/elasticsearch.conf but it is not applied until a reboot.
cur=$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)
if [ "$cur" -lt @@MAXMAP@@ ]; then
    printf 'vm.max_map_count=%s\n' @@MAXMAP@@ > /etc/sysctl.d/99-elasticsearch.conf
    sysctl -q -w vm.max_map_count=@@MAXMAP@@
    echo "vm.max_map_count $cur -> $(sysctl -n vm.max_map_count)"
else
    echo "vm.max_map_count already $cur"
fi
"""

# `[.]` in the pkill patterns keeps them from matching this very shell — the old
# script's `pkill -f elasticsearch` matched its own `bash -c` command line and
# could kill the SSH session running it.
STOP_OLD = r"""
systemctl stop elasticsearch >/dev/null 2>&1 || true
systemctl disable elasticsearch >/dev/null 2>&1 || true
[ -x /etc/init.d/elasticsearch ] && /etc/init.d/elasticsearch stop >/dev/null 2>&1 || true

pat='org[.]elasticsearch[.]bootstrap'
pkill -f "$pat" >/dev/null 2>&1 || true
sleep 2
pkill -9 -f "$pat" >/dev/null 2>&1 || true

# hand-placed unit override, if any: the packaged unit lives in /usr/lib and an
# /etc copy would shadow it
rm -f /etc/systemd/system/elasticsearch.service
systemctl daemon-reexec >/dev/null 2>&1 || true
systemctl daemon-reload >/dev/null 2>&1 || true
rm -rf /usr/local/elasticsearch

echo "old service stopped (es jvms left: $(pgrep -c -f "$pat" 2>/dev/null || true))"
"""

# Purge only when asked. The default is an in-place upgrade, which is what the
# Confluence recipe does and what keeps the 8.x install from re-running security
# auto-configuration. Purge additionally discards /etc/elasticsearch and the
# data directory.
PURGE = r"""
cur=$(dpkg-query -W -f='${Version}' elasticsearch 2>/dev/null || true)
if [ -z "$cur" ]; then
    echo "elasticsearch not installed; nothing to purge"
elif [ "@@MODE@@" = "auto" ] && [ "$cur" = "@@VERSION@@" ]; then
    echo "elasticsearch $cur already installed; not purging"
else
    echo "purging elasticsearch $cur"
    dpkg --purge --force-all elasticsearch 2>&1 | tail -5 || true
    rm -f /etc/elasticsearch/*.dpkg-new /etc/init.d/elasticsearch.dpkg-new 2>/dev/null || true
fi
"""

WIPE_DATA = r"""
d=/var/lib/elasticsearch
if [ -d "$d" ] && [ -n "$(ls -A "$d" 2>/dev/null)" ]; then
    echo "wiping $d ($(du -sh "$d" 2>/dev/null | cut -f1))"
    rm -rf "$d"/*
else
    echo "$d already empty"
fi
"""

# -O with a fixed name: the old script's bare `wget` created
# elasticsearch-1.7.3.deb.1 on re-runs while dpkg kept installing the old file.
# The 8.x deb is ~640 MB, hence the long timeout.
DOWNLOAD_DEB = r"""
f=/tmp/elasticsearch-@@VERSION@@-amd64.deb
need=1
if [ -f "$f" ] && dpkg-deb --info "$f" >/dev/null 2>&1; then
    v=$(dpkg-deb -f "$f" Version 2>/dev/null)
    [ "$v" = "@@VERSION@@" ] && need=0 && echo "reusing cached $f ($(stat -c %s "$f") bytes)"
fi
if [ "$need" = 1 ]; then
    rm -f "$f"
    df -h /tmp | awk 'NR==2 {print "free space on /tmp: " $4}'
    wget -q --tries=3 --timeout=60 -O "$f" '@@DEB_URL@@' || { echo "download failed"; rm -f "$f"; exit 1; }
    dpkg-deb --info "$f" >/dev/null 2>&1 || { echo "downloaded file is not a valid .deb"; exit 1; }
    echo "downloaded $(stat -c %s "$f") bytes to $f"
fi
"""

INSTALL_DEB = r"""
f=/tmp/elasticsearch-@@VERSION@@-amd64.deb
dpkg @@FORCE@@ -i "$f" 2>&1 | tail -25
st=$(dpkg-query -W -f='${Status} ${Version}' elasticsearch 2>/dev/null || echo none)
echo "package status: $st"
case "$st" in "install ok installed @@VERSION@@") ;; *) echo "package not properly installed"; exit 1 ;; esac
# obsolete conffile from the 1.x package; harmless but confusing to leave behind
[ -f /etc/init.d/elasticsearch ] && ! dpkg -S /etc/init.d/elasticsearch >/dev/null 2>&1 && rm -f /etc/init.d/elasticsearch
echo "bundled jdk: $(/usr/share/elasticsearch/jdk/bin/java -version 2>&1 | head -1)"
"""

# The two settings from the Confluence page, applied idempotently.
#
# On a *fresh* install the deb also writes a security auto-configuration block
# (TLS keystores, enrollment, an 'elastic' password). Setting security.enabled
# false is not enough there: xpack.security.http.ssl stays on, so the node comes
# up on HTTPS and plain-HTTP curl fails. The block is fenced by BEGIN/END comment
# markers, so drop it wholesale rather than trying to patch nested YAML.
# A package *upgrade* skips auto-config, so on these hosts the block is absent
# and this is a no-op.
CONFIGURE_YML = r"""
yml=/etc/elasticsearch/elasticsearch.yml
[ -f "$yml" ] || { echo "$yml is missing"; exit 1; }
[ -f "$yml.pre-es8" ] || cp -a "$yml" "$yml.pre-es8"

if grep -q 'BEGIN SECURITY AUTO CONFIGURATION' "$yml"; then
    echo "removing security auto-configuration block (fresh install)"
    sed -i '/BEGIN SECURITY AUTO CONFIGURATION/,/END SECURITY AUTO CONFIGURATION/d' "$yml"
fi

set_kv() {
    k=$1; v=$2
    if grep -qE "^[[:space:]]*${k}[[:space:]]*:" "$yml"; then
        sed -ri "s|^[[:space:]]*${k}[[:space:]]*:.*|${k}: ${v}|" "$yml"
    else
        printf '%s: %s\n' "$k" "$v" >> "$yml"
    fi
}
set_kv xpack.security.enabled false
set_kv http.host 0.0.0.0

# a JAVA_HOME left over from a 1.7.3-era install would override the bundled jdk
if grep -q '^JAVA_HOME=' /etc/default/elasticsearch 2>/dev/null; then
    sed -i '/^JAVA_HOME=/d' /etc/default/elasticsearch
    echo "removed stale JAVA_HOME from /etc/default/elasticsearch"
fi

echo "--- effective settings ---"
grep -nE '^[^#]*(xpack\.security|http\.host|discovery|cluster\.name|node\.name)' "$yml" || echo "(none)"
"""

START_ES = r"""
systemctl daemon-reload
systemctl enable elasticsearch >/dev/null 2>&1 || true
systemctl restart elasticsearch || {
    echo "systemctl restart failed"
    systemctl status elasticsearch --no-pager -l 2>&1 | head -25
    exit 1
}
echo "unit is $(systemctl is-active elasticsearch)"
"""

# ES 8 needs appreciably longer than 1.7 to answer on :9200, so poll rather than
# assume.
VERIFY_ES = r"""
get() {
    if command -v curl >/dev/null; then curl -s -m 8 "$1"; else wget -qO- --timeout=8 "$1"; fi
}
for i in $(seq 1 @@TRIES@@); do
    body=$(get 'http://localhost:9200/' 2>/dev/null)
    if [ -n "$body" ]; then
        echo "$body"
        echo "--- health ---"
        get 'http://localhost:9200/_cluster/health?pretty' 2>/dev/null | head -12
        exit 0
    fi
    systemctl is-active elasticsearch >/dev/null 2>&1 || {
        echo "!! unit died while waiting: $(systemctl is-active elasticsearch)"
        journalctl -u elasticsearch --no-pager -n 30 2>&1 | tail -30
        tail -n 40 /var/log/elasticsearch/*.log 2>/dev/null | tail -40
        exit 1
    }
    sleep 3
done
echo "!! elasticsearch did not answer on :9200"
systemctl is-active elasticsearch
journalctl -u elasticsearch --no-pager -n 30 2>&1 | tail -30
tail -n 40 /var/log/elasticsearch/*.log 2>/dev/null | tail -40
exit 1
"""

CLEANUP = r"""
rm -f /root/elasticsearch-*.deb /root/elasticsearch-*.tar.gz /root/elasticsearch-*.zip
rm -f /tmp/elasticsearch-*-amd64.deb
echo "removed downloaded archives"
"""

# Read-only status probe for --check-only.
CHECK_ONLY = r"""
. /etc/os-release 2>/dev/null
echo "os=$PRETTY_NAME"
echo "pkg=$(dpkg-query -W -f='${Status} ${Version}' elasticsearch 2>/dev/null || echo not-installed)"
pat='org[.]elasticsearch[.]bootstrap'
echo "es_jvms=$(pgrep -c -f "$pat" 2>/dev/null || true)"
echo "unit=$(systemctl is-active elasticsearch 2>&1) / $(systemctl is-enabled elasticsearch 2>&1)"
echo "max_map_count=$(sysctl -n vm.max_map_count 2>/dev/null)"
sec=$(grep -E '^[^#]*xpack\.security\.enabled' /etc/elasticsearch/elasticsearch.yml 2>/dev/null | tail -1 | tr -d ' ')
echo "security=${sec:-unset}"
hh=$(grep -E '^[^#]*http\.host' /etc/elasticsearch/elasticsearch.yml 2>/dev/null | tail -1 | tr -d ' ')
echo "http_host=${hh:-unset}"
echo "wedged_dpkg=$(pgrep -x dpkg | tr '\n' ' ')"
body=$(curl -s -m 8 'http://localhost:9200/' 2>/dev/null)
if [ -n "$body" ]; then
    echo "http9200=up"
    echo "$body" | tr -d '\n' | sed -n 's/.*"number"[^"]*"\([^"]*\)".*/es_version=\1/p'
else
    echo "http9200=down"
fi
"""


def fill(template, **kw):
    for k, v in kw.items():
        template = template.replace(f"@@{k}@@", str(v))
    return template


# --------------------------------------------------------------------------- #
# per-host workflow
# --------------------------------------------------------------------------- #

def install_on_host(ip, args):
    """Returns (ip, ok, summary, log_path). Never raises."""
    host = Host(ip, args.username, args.password, connect_timeout=args.connect_timeout)
    try:
        # A few hosts in a big pool are always slow to answer; retry the connect
        # rather than losing the host to a transient failure.
        for attempt in range(1, args.retries + 2):
            try:
                host.connect()
                break
            except (paramiko.SSHException, socket.error, EOFError) as e:
                host.note(f"[connect attempt {attempt} failed: {type(e).__name__}: {e}]")
                if attempt > args.retries:
                    return ip, False, f"ssh failed: {type(e).__name__}: {e}", host.log_path
                time.sleep(3)

        if args.check_only:
            out = host.run(CHECK_ONLY, timeout=90)[1]
            fields = dict(
                line.split("=", 1)
                for line in out.splitlines()
                if "=" in line and " " not in line.split("=", 1)[0]
            )
            up = fields.get("http9200") == "up"
            summary = "  ".join(
                f"{k}={fields.get(k, '?')}"
                for k in ("pkg", "unit", "security", "max_map_count", "http9200", "es_version")
                if k in fields or k != "es_version"
            )
            return ip, up, summary, host.log_path

        deb_url = args.deb_url or ES_DEB_URL.format(v=args.es_version)

        host.step("preflight", fill(PREFLIGHT, FORCE=DPKG_FORCE, MAXMAP=MAX_MAP_COUNT), timeout=300)
        host.step("stop-old-service", STOP_OLD, timeout=120)
        if args.purge != "never":
            host.step("purge", fill(PURGE, MODE=args.purge, VERSION=args.es_version), timeout=300)
        if args.wipe_data:
            host.step("wipe-data", WIPE_DATA, timeout=180)
        host.step("download", fill(DOWNLOAD_DEB, VERSION=args.es_version, DEB_URL=deb_url),
                  timeout=args.download_timeout)
        host.step("install", fill(INSTALL_DEB, VERSION=args.es_version, FORCE=DPKG_FORCE), timeout=900)
        host.step("configure", CONFIGURE_YML, timeout=120)
        host.step("start", START_ES, timeout=300)
        out = host.step("verify", fill(VERIFY_ES, TRIES=args.verify_tries),
                        timeout=args.verify_tries * 3 + 90)
        host.step("cleanup", CLEANUP, timeout=90)

        ver = re.search(r'"number"\s*:\s*"([^"]+)"', out)
        status = re.search(r'"status"\s*:\s*"([^"]+)"', out)
        cluster = re.search(r'"cluster_name"\s*:\s*"([^"]+)"', out)
        summary = (
            f"up on :9200 — es {ver.group(1) if ver else '?'}, "
            f"cluster {cluster.group(1) if cluster else '?'}"
            + (f", health {status.group(1)}" if status else "")
        )
        return ip, True, summary, host.log_path

    except RemoteError as e:
        return ip, False, str(e), host.log_path
    except Exception as e:  # keep one bad host from killing the run
        return ip, False, f"{type(e).__name__}: {e}", host.log_path
    finally:
        host.close()


def timed_install(ip, args):
    """install_on_host plus how long it took: (ip, ok, summary, log_path, seconds)."""
    t0 = time.monotonic()
    ip, ok, summary, log_path = install_on_host(ip, args)
    return ip, ok, summary, log_path, time.monotonic() - t0


# --------------------------------------------------------------------------- #
# cli
# --------------------------------------------------------------------------- #

def parse_args(argv):
    p = argparse.ArgumentParser(
        description="Install/upgrade Elasticsearch 8.x on a fleet of Debian hosts over SSH.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--ips", help="comma-separated IPs (default: hosts.txt, else the built-in list)")
    p.add_argument("--ips-file", help="file with one IP per line (# comments allowed)")
    p.add_argument("--username", default="root")
    p.add_argument("--password", default=os.environ.get("ES_SSH_PASSWORD"),
                   help="SSH password (or set ES_SSH_PASSWORD)")
    p.add_argument("--no-prompt", action="store_true", help="never prompt; use defaults/flags")
    p.add_argument("--workers", type=int, default=6,
                   help="hosts in parallel (the deb is ~640 MB, so keep this modest)")
    p.add_argument("--es-version", default=ES_VERSION)
    p.add_argument("--deb-url", help="override the .deb URL")
    p.add_argument("--purge", choices=["never", "auto", "always"], default="never",
                   help="'never' upgrades in place (what the Confluence steps do); "
                        "'auto' purges unless the target version is already installed; "
                        "'always' purges first. Purging discards /etc/elasticsearch and data")
    p.add_argument("--wipe-data", action="store_true",
                   help="empty /var/lib/elasticsearch before installing (needed if a host "
                        "still holds index data from an older major version)")
    p.add_argument("--verify-tries", type=int, default=40, help="3s polls of :9200 after start")
    p.add_argument("--download-timeout", type=int, default=1800, help="seconds for the deb download")
    p.add_argument("--connect-timeout", type=int, default=20)
    p.add_argument("--retries", type=int, default=1, help="extra SSH connect attempts per host")
    p.add_argument("--check-only", action="store_true", help="report status only, change nothing")
    return p.parse_args(argv)


def resolve_hosts(args):
    """Host list, in precedence order: --ips-file, --ips, ./hosts.txt, built-in list."""
    def clean(lines):
        seen, out = set(), []
        for raw in lines:
            ip = raw.split("#", 1)[0].strip().strip(",").strip('"')
            if ip and ip not in seen:
                seen.add(ip)
                out.append(ip)
        return out

    if args.ips_file:
        with open(args.ips_file) as f:
            return clean(f), args.ips_file
    if args.ips:
        return clean(args.ips.split(",")), "--ips"
    if os.path.exists(HOSTS_FILE):
        with open(HOSTS_FILE) as f:
            hosts = clean(f)
        if hosts:
            return hosts, os.path.basename(HOSTS_FILE)
    if args.no_prompt:
        return list(DEFAULT_IPS), "built-in list"
    entered = input(f"Enter comma-separated IPs [default: all {len(DEFAULT_IPS)} built-in hosts]: ")
    hosts = clean(entered.split(","))
    return (hosts, "--ips") if hosts else (list(DEFAULT_IPS), "built-in list")


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    hosts, source = resolve_hosts(args)
    if not hosts:
        print("no hosts to work on")
        return 2
    if not args.no_prompt and not args.ips and not args.ips_file:
        args.username = input(f"Enter username [default: {args.username}]: ") or args.username
    if not args.password:
        args.password = (None if args.no_prompt else getpass("Enter password [default: couchbase]: ")) or "couchbase"

    mode = "checking" if args.check_only else f"installing elasticsearch {args.es_version} on"
    print(
        f"{mode} {len(hosts)} host(s) from {source}, {args.workers} at a time; "
        f"per-host logs in {LOG_DIR}/\n",
        flush=True,
    )

    started = time.monotonic()
    results = []
    pool = ThreadPoolExecutor(max_workers=max(1, args.workers))
    try:
        futures = {pool.submit(timed_install, ip, args): ip for ip in hosts}
        # Report as each host finishes, so a slow box does not hide the others'
        # progress on a long fleet run.
        for fut in as_completed(futures):
            ip, ok, summary, log_path, secs = fut.result()
            results.append((ip, ok, summary, log_path))
            print(
                f"[{len(results):>3}/{len(hosts)}] {'OK  ' if ok else 'FAIL'} {ip:<16} "
                f"({secs:5.1f}s) {summary}",
                flush=True,
            )
    except KeyboardInterrupt:
        print("\ninterrupted — cancelling hosts that have not started yet", flush=True)
        for fut in futures:
            fut.cancel()
        pool.shutdown(wait=False)
        return 130
    finally:
        pool.shutdown(wait=True)

    ok_hosts = [r for r in results if r[1]]
    failed = [r for r in results if not r[1]]
    print(f"\n{len(ok_hosts)}/{len(results)} ok in {time.monotonic() - started:.0f}s")
    if failed:
        print("failures:")
        for ip, _, summary, log_path in sorted(failed):
            print(f"  {ip:<16} {summary}   (log: {log_path})")
        # Ready to paste: retry just the hosts that did not make it.
        print("\nretry only these:\n  python3 elastic_install.py --no-prompt --ips "
              + ",".join(ip for ip, *_ in sorted(failed)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
