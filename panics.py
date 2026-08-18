#!/usr/bin/env python3
"""
Extract and group panics out of a core_dump_checker Jenkins consoleText.

The raw log is ~300MB and mostly noise. It has two kinds of panic section.

Archive scan -- one per collected diag.zip:

    #######################
    checking: /data/workspace/<suite>/logs/testrunner-.../test_76/<node>-...-diag.zip
    #######################
    === panic found ===
    /root/cbcollect_info_ns_1@172.23.220.13_.../diag.log:{"event_id":4095,...}
    /root/cbcollect_info_ns_1@172.23.220.13_.../diag.log-{"event_id":3075,...}

Server scan -- one per live server, with a build number on the marker:

    --+--+--+--+-- 149. CHECKING ON SERVER: 172.23.122.236 --+--+--+--+--
    172.23.122.236 : === panic found in 8.0.3-5895 ===
    /opt/couchbase/var/lib/couchbase/logs/projector.log:panic: MonitorService... failed
    /opt/couchbase/var/lib/couchbase/logs/projector.log-
    /opt/couchbase/var/lib/couchbase/logs/projector.log-goroutine 96420 [running]:
    /opt/couchbase/var/lib/couchbase/logs/projector.log-github.com/couchbase/indexing/...

What makes this hard to grep, and what this script does about it:

  1. **The grep -A context matters, and cannot simply be dropped.** A hit prints
     as `<logpath>:<match>` and its context as `<logpath>-<line>`. In a
     service log (projector.log, fts.log, babysitter.log) those context lines
     ARE the rest of the same Go panic -- the goroutine header and the stack
     frames that say which component crashed. So we rebuild each contiguous
     run of lines from one logfile into a region and parse the region as Go
     panic output. Only in `diag.log` are the neighbours unrelated JSON events,
     and those are handled per-line instead.
  2. The job greps case-insensitively, so base64 blobs match by accident
     (e.g. "...OpANicFdfi1EXAjg=" contains "pANic"). Those regions are dropped.
  3. A panic's `panic:` header is often outside the grep window, so a region can
     be all stack and no message. Those are still real crashes and are
     identified by their top non-runtime frame.
  4. Massive duplication: the same crash is collected from every node, and
     diag.log re-aggregates the per-service logs. Crashes are normalised to a
     signature and grouped.

Every panic is attributed to a component (search, index, query, projector,
analytics, eventing, xdcr, data, backup, ns_server, ...) from its crash event,
its logfile, or its top stack frame -- and the report says which of those it
used, because a component read off a frame is solid while one guessed from
surrounding log text is not.

Build versions: the console log only states one on a server-scan marker
("=== panic found in 8.0.3-5895 ==="), so that is the only authoritative source.
Archive-scanned panics get no version -- the log simply does not carry it -- and
the version in a suite name is NOT it (e.g. "..._vector_sift_7.6_P0" ran on
8.0.3-5921). Where a node's build is known from the scan it is reported on a
separate "build?:" line, never merged with the authoritative value.

Locating a panic: pass --build-url to print the upstream console URL, and use
the reported "in log: line N" to find the panic inside it. There is no per-panic
deep link, because Jenkins cannot address a console line -- see console_link().

Usage:
    ./panics.py console.txt                     # every unique panic, most frequent first
    ./panics.py console.txt --list-components   # what components/areas are present
    ./panics.py console.txt --component search  # one component (comma separated ok)
    ./panics.py console.txt --component search --related --full
    ./panics.py console.txt --area fts,xdcr     # by test area instead
    ./panics.py console.txt --min-sites 5       # hide one-off noise
    ./panics.py console.txt --all-reports reports/ --build-url <upstream-build-url>
    ./panics.py console.txt --json              # machine-readable
    ./fetch_console.sh <build-url> - | ./panics.py -    # stream, no 300MB on disk
"""

import argparse
import json
import os
import re
import sys
from collections import Counter, OrderedDict, defaultdict

CHECKING = "checking: "

# "=== panic found ===" or "=== panic found in 8.0.3-5895 ==="
MARKER_RE = re.compile(r"=== panic found(?: in (?P<version>[^=]+?))? ===")
# Server-scan lines are prefixed with the host: "172.23.122.236 : ..."
HOST_PREFIX_RE = re.compile(r"^(?P<host>[\w.:@-]+) : ")
SECTION_RE = re.compile(
    r"^--\+--\+--\+--\+-- \d+\. CHECKING ON (?P<kind>SERVER|SLAVE): (?P<host>\S+)")

# "<logpath>:" is a grep match, "<logpath>-" is grep -A context. Both belong to
# the same log region.
# Extensions are listed explicitly on purpose. Broadening this to any extension
# would make stack-frame lines like ".../src/runtime/panic.go:1181 +0x18" look
# like a new log path and split a region in half.
LINE_RE = re.compile(r"^(?P<path>\S*?\.(?:log|txt|out|json))(?P<sep>[:-])(?P<rest>.*)$")

# A genuine panic token: lowercase "panic"/"fatal error" not glued inside a
# base64-ish blob. Rejects "OpANicFdfi..." (wrong case) and "abcpanicdef".
# The "not preceded by /" rule kills base64, but "runtime/panic.go" is a real
# frame with a slash in front of it, so allow "panic.go" explicitly.
REAL_PANIC_RE = re.compile(
    r"(?<![A-Za-z0-9+/=])panic(?![A-Za-z0-9+/=])|panic\.go|runtime\.gopanic|"
    r"\bpanicking\b|\bfatal error:|\bruntime\.fatal|maps\.fatal")

# .../cbcollect_info_ns_1@172.23.220.13_20260807-093144/diag.log
NODE_RE = re.compile(
    r"cbcollect_info_(?:ns_1@)?(?P<node>[^/_]+(?:\.[^/_]+)*)_(?P<stamp>\d{8}-\d{6})")

# /data/workspace/<suite>/(logs|job_logs)/testrunner-<run>/test_<n>/...
ARCHIVE_RE = re.compile(
    r"/workspace/(?P<suite>[^/]+)/[^/]*logs/(?P<run>testrunner-[^/]+)"
    r"(?:/test_(?P<test>\d+))?")

# Frames that never identify *which* bug this is: Go runtime internals and the
# panic plumbing itself.
BORING_FRAME_RE = re.compile(
    r"runtime/panic\.go|runtime/asm_|runtime\.gopanic|^panic\(|created by |"
    r"^goroutine \d+|ServeHTTP\.func|/runtime/(?:proc|signal|sigqueue)\.go|"
    r"^internal/runtime/|^runtime\.|maps\.fatal|^sync\.|^os/signal\.")

# Text tokens that identify the owning service. Order matters: first hit wins.
COMPONENT_TOKENS = [
    (("cbft", "cbgt", "bleve", "zapx", "vellum", "scorch", "/fts/",
      "search_service"), "search"),
    (("plasma", "forestdb", "nitro", "memdb", "secondaryindex",
      "couchbase/indexing", "indexer"), "index"),
    (("projector",), "projector"),
    (("couchbase/query", "n1ql", "datastore/couchbase"), "query"),
    (("cbas", "analytics"), "analytics"),
    (("eventing",), "eventing"),
    (("goxdcr", "xdcr"), "xdcr"),
    (("memcached", "kv_engine", "ep-engine"), "data"),
    (("cbbackupmgr", "cont_backup", "backup"), "backup"),
]

# Filename stem -> component, for a hit in a service-specific log. Archive logs
# are named "ns_server.<svc>.log"; live server logs are just "<svc>.log".
LOGFILE_COMPONENT = {
    "fts": "search",
    "indexer": "index",
    "indexer_mprof": "index",
    "projector": "projector",
    "query": "query",
    "analytics_cbas_debug": "analytics",
    "analytics_info": "analytics",
    "cbas": "analytics",
    "eventing": "eventing",
    "goxdcr": "xdcr",
    "memcached": "data",
    "cont_backup": "backup",
    "backup_service": "backup",
    "debug": "ns_server",
    "info": "ns_server",
    "error": "ns_server",
    "babysitter": "ns_server",
    "metakv": "ns_server",
    "json_rpc": "ns_server",
    "ns_couchdb": "ns_server",
    "cbcollect_info": "cbcollect",
    # diag.log is an aggregate of every service, so it implies no component.
}

# Test-suite names look like "<distro>-p0-<area>-vset00-00-<case>", e.g.
# "debian-p0-fts-vset00-00-read-from-replica-random-b-S" or
# "ubuntu24-p0-os_certify-...". The area says which team's run hit the panic,
# which is independent of which component's code actually crashed.
DISTRO_RE = re.compile(
    r"^(?:centos|debian|ubuntu|rhel|alma|rocky|suse|oel|al\d+|amzn|windows|mad-hatter|"
    r"magma)[\w.]*$", re.I)
PRIORITY_RE = re.compile(r"^p\d+$", re.I)


def suite_area(suite):
    """The test area a suite belongs to (fts, xdcr, 2i, os_certify, ...)."""
    if not suite:
        return ""
    for token in suite.split("-"):
        if not token or DISTRO_RE.match(token) or PRIORITY_RE.match(token):
            continue
        if token.startswith("vset"):
            break
        return token.lower()
    return ""

# Volatile bits that differ between otherwise-identical crashes.
NOISE_SUBS = [
    (re.compile(r"0x[0-9a-fA-F]+"), "0xX"),
    (re.compile(r"goroutine \d+"), "goroutine N"),
    (re.compile(r"\b\d{4}-\d{2}-\d{2}[T ][\d:.+-]+\b"), "<ts>"),
    (re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b"), "<ip>"),
    (re.compile(r'"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"'),
     '"<uuid>"'),
    (re.compile(r"\s+"), " "),
]

# ns_server's babysitter log wraps a crashed child's output in an Erlang binary,
# newlines escaped and the tail often elided:
#   {<<"panic: uninitialized clusterInfo\n\ngoroutine 1 [running]:\n...">>,
CORE_PANIC_RE = re.compile(r"(?:panic|fatal error):\s*(.+)")
GOROUTINE_RE = re.compile(r"^\s*goroutine \d+ \[[^\]]*\]:", re.M)
ERLANG_TAIL_RE = re.compile(r'(?:\\?")?(?:\.\.\.)?(?:\\?")?>>.*$')
# `..."">>` right before the wrapper closes means Erlang elided the message.
ELIDED_RE = re.compile(r'\.\.\.\\?"?>>')
# The core-dump check prints the node's build on its own line: "['8.1.0-2181\n']"
COREDUMP_VERSION_RE = re.compile(r"^\[\s*'([5-9]\.\d+\.\d+-\d{3,5})")

# A frame the log cut off mid-symbol tells us nothing reliable.
TRUNCATED_FRAME_RE = re.compile(r'["<>]|\.\.\.')
# "pkg/path.(*Type).Method(args)" or "pkg/path.Func(args)". The trailing "(" is
# required: without it the regex backtracks on a truncated line like
# 'github.com/couchbase/indexing/secondary"...>>' and yields a bare "github.com".
FRAME_SYM_RE = re.compile(r"^([\w./@~-]+(?:\.\(\*?[\w.]+\))?\.[\w.]+)\(")

# Generic runtime messages say nothing about which bug it is, so those must be
# split by package; a bespoke panic string is already specific enough on its own.
GENERIC_MSG_RE = re.compile(
    r"^(?:runtime error|invalid memory address|index out of range|"
    r"slice bounds out of range|concurrent map|interface conversion|"
    r"close of (?:closed|nil) channel|send on closed channel|"
    r"assignment to entry in nil map|integer divide by zero|"
    r"all goroutines are asleep|out of memory|stack overflow)")


def frame_package(frame):
    """Coarse package key: first three path segments, so a slightly different
    symbol in the same package still groups together."""
    if not frame:
        return ""
    head = frame.rsplit("/", 1)
    if len(head) == 2:
        parts = ("%s/%s" % (head[0], head[1].split(".", 1)[0])).split("/")
    else:
        parts = frame.split(".", 1)[0].split("/")
    return "/".join(parts[:3])


def unescape(text):
    """Turn literal \\n / \\t sequences into real whitespace."""
    if "\\n" in text or "\\t" in text:
        text = text.replace("\\n", "\n").replace("\\t", "\t")
    return text


def normalise(text):
    for pattern, repl in NOISE_SUBS:
        text = pattern.sub(repl, text)
    return text.strip()


def strip_erlang_tail(text):
    """Remove the Erlang binary wrapper's tail (`"...>>,`)."""
    return ERLANG_TAIL_RE.sub("", text).rstrip('\\", \t')


def split_panic_text(text):
    """Split panic output into (core message, stack trace)."""
    expanded = unescape(text)
    stack = ""
    gm = GOROUTINE_RE.search(expanded)
    if gm:
        stack = expanded[gm.start():]
        head = expanded[: gm.start()]
    else:
        head = expanded
        # No goroutine header in the window: the frames are all we have.
        if re.search(r"^\S*\.go:\d+|^\S+\.\(?\*?\w+\)?\.\w+\(", expanded, re.M):
            stack = expanded
    # Search the whole region, not just the part above the goroutine header:
    # babysitter.log's Erlang crash reports list de-duplicated lines with counts
    # in arbitrary order, so "fatal error: ..." can appear after the goroutine.
    cm = CORE_PANIC_RE.search(head) or CORE_PANIC_RE.search(expanded)
    core = cm.group(1) if cm else ""
    core = strip_erlang_tail(core)
    # Panic messages legitimately contain quoted URLs, so unescape rather than
    # cutting at the first quote.
    core = core.replace('\\"', '"').strip().rstrip("\\, \t")
    return core, strip_erlang_tail(stack)


def first_real_frame(stack):
    """Topmost stack frame that actually identifies the bug."""
    lines = [l.strip() for l in stack.splitlines()]
    for line in lines:
        if not line or BORING_FRAME_RE.search(line):
            continue
        if line.startswith("/") or re.match(r"^\S+\.go:\d+", line):
            continue
        sm = FRAME_SYM_RE.match(line)
        if not sm:
            continue
        frame = normalise(sm.group(1))
        if TRUNCATED_FRAME_RE.search(frame) or "." not in frame.rsplit("/", 1)[-1]:
            continue  # cut off mid-symbol; not a usable identifier
        return frame
    # Fall back to the first non-runtime source location.
    for line in lines:
        m = re.search(r"([\w./-]+\.go:\d+)", line)
        if m and "/runtime/" not in m.group(1) and "/internal/" not in m.group(1):
            return m.group(1)
    return ""


class Parser(object):
    """Streams the console log and yields one record per crash region."""

    def __init__(self):
        self.archive = None
        self.section_host = ""
        self.version = ""
        self.node_hint = ""
        self.in_block = False
        self.region_path = None
        self.region_lines = []
        self.dropped_fp = 0
        self.raw_hits = 0
        self.block_id = 0
        # Line numbers let the report say where in the console a panic is.
        # (Byte offsets are useless as links: Jenkins' progressiveText `start`
        # indexes the raw stored log, not the text it serves.)
        self.offset = 0
        self.line_no = 0
        self.context_offset = 0     # the "checking:"/section line above the block
        self.marker_offset = 0
        # The console log only states a build version in two places: on a
        # server-scan panic marker, and on the core-dump check's version line.
        # Collect both, keyed by node, so archive panics can at least be
        # annotated with what that node was running when it was scanned.
        self.node_versions = {}

    def feed(self, stream):
        """`stream` yields bytes lines so line numbering is encoding-independent."""
        offset = 0
        for raw in stream:
            self.offset = offset
            self.line_no += 1
            offset += len(raw)
            line = raw.decode("utf-8", "replace").rstrip("\r\n")
            for rec in self.line(line):
                yield rec
        for rec in self.close_region():
            yield rec

    def line(self, line):
        sm = SECTION_RE.match(line)
        if sm:
            for rec in self.close_region():
                yield rec
            self.in_block = False
            self.section_host = sm.group("host")
            self.archive = None
            self.version = ""
            self.context_offset = self.offset
            return

        # "['8.1.0-2181\n']" -- the core-dump check printing the node's build.
        cv = COREDUMP_VERSION_RE.match(line)
        if cv and self.section_host:
            self.node_versions.setdefault(self.section_host, cv.group(1))
            return

        if line.startswith(CHECKING):
            for rec in self.close_region():
                yield rec
            self.in_block = False
            self.archive = line[len(CHECKING):].strip()
            self.context_offset = self.offset
            return

        mm = MARKER_RE.search(line)
        if mm:
            for rec in self.close_region():
                yield rec
            self.in_block = True
            self.block_id += 1
            self.marker_offset = self.offset
            if not self.archive:
                # Server scan: the marker itself is the most specific anchor.
                self.context_offset = self.offset
            self.version = (mm.group("version") or "").strip()
            hp = HOST_PREFIX_RE.match(line)
            self.node_hint = hp.group("host") if hp else ""
            if self.version:
                host = self.node_hint or self.section_host
                if host:
                    self.node_versions[host] = self.version
            return

        if not self.in_block:
            return

        stripped = line.strip()

        # grep prints "--" between separate match groups: end of this region.
        if stripped == "--":
            for rec in self.close_region():
                yield rec
            return

        if not stripped or stripped.startswith("#####"):
            for rec in self.close_region():
                yield rec
            self.in_block = False
            return

        m = LINE_RE.match(line)
        if not m:
            return

        path, sep, rest = m.group("path"), m.group("sep"), m.group("rest")
        if sep == ":":
            self.raw_hits += 1

        event = as_json_event(rest)
        if event is not None:
            # A diag.log JSON crash event stands alone; its neighbours are
            # unrelated events, so never fold them into a region.
            for rec in self.close_region():
                yield rec
            if sep == ":":
                yield self.json_record(path, event)
            return

        if path != self.region_path:
            for rec in self.close_region():
                yield rec
            self.region_path = path
        self.region_lines.append(rest)

    def close_region(self):
        if not self.region_lines:
            self.region_path = None
            return
        text = "\n".join(self.region_lines)
        path = self.region_path
        self.region_lines = []
        self.region_path = None

        if not REAL_PANIC_RE.search(text):
            self.dropped_fp += 1
            return

        rec = self.base_record(path)
        rec["error"], rec["stack"] = split_panic_text(text)
        rec["truncated"] = bool(ELIDED_RE.search(text))
        rec["region"] = unescape(text)
        yield finish(rec)

    def json_record(self, path, event):
        rec = self.base_record(path)
        details = (event.get("extra_attributes") or {}).get("details") or {}
        rec["component"] = event.get("component", "") or ""
        rec["severity"] = event.get("severity", "") or ""
        rec["error"] = details.get("crashError") or event.get("description", "") or ""
        rec["stack"] = details.get("stackTrace", "") or ""
        rec["event_time"] = event.get("timestamp", "") or ""
        rec["region"] = rec["stack"]
        return finish(rec)

    def base_record(self, path):
        rec = {
            "archive": self.archive or "",
            "logfile": path.rsplit("/", 1)[-1],
            "logpath": path,
            "version": self.version,
            "node": self.node_hint or self.section_host,
            "collected": "", "suite": "", "run": "", "test": "",
            "component": "", "severity": "", "error": "", "stack": "",
            "event_time": "", "truncated": False, "region": "",
            "block": self.block_id,
            "offset": self.context_offset,
            "marker_offset": self.marker_offset,
            "line": self.line_no,
            "node_version": "",
            "scan": "archive" if self.archive else "server",
        }
        nm = NODE_RE.search(path)
        if nm:
            rec["node"] = nm.group("node")
            rec["collected"] = nm.group("stamp")
        if self.archive:
            am = ARCHIVE_RE.search(self.archive)
            if am:
                rec["suite"] = am.group("suite") or ""
                rec["run"] = am.group("run") or ""
                rec["test"] = am.group("test") or ""
        return rec


def as_json_event(rest):
    """Return the parsed dict if this line is a JSON log event, else None."""
    s = rest.strip()
    if not s.startswith("{") or '"component"' not in s:
        return None
    try:
        ev = json.loads(s)
    except ValueError:
        return None          # Erlang term, e.g. {<<"panic: ...">>,7}
    return ev if isinstance(ev, dict) else None


def logfile_stem(logfile):
    stem = logfile
    for suffix in (".log", ".txt", ".out"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    if stem.startswith("ns_server."):
        stem = stem[len("ns_server."):]
    return stem


def infer_component(rec):
    """Return (component, how_we_decided). The "how" matters: a component read
    off a crash event or a stack frame is solid, one guessed from surrounding
    region text is not, and the report shows the difference."""
    if rec["component"]:
        return rec["component"], "crash-event"
    stem = logfile_stem(rec["logfile"])
    if stem in LOGFILE_COMPONENT:
        return LOGFILE_COMPONENT[stem], "logfile"
    # The identifying frame is far more trustworthy than the surrounding region,
    # which in diag.log is a slice of every service's log interleaved.
    blobs = (("frame", rec["frame"].lower()),
             ("region", (rec["region"] or
                         (rec["error"] + " " + rec["stack"])).lower()))
    for how, blob in blobs:
        for tokens, comp in COMPONENT_TOKENS:
            if any(tok in blob for tok in tokens):
                return comp, how
    return "", ""


def finish(rec):
    rec["frame"] = first_real_frame(rec["stack"]) if rec["stack"] else ""
    rec["component"], rec["attribution"] = infer_component(rec)
    rec["area"] = suite_area(rec["suite"])

    msg = re.sub(r"^\[[^\]]*\]\s*", "", rec["error"])
    rec["message"] = normalise(msg)[:400]
    # Group on the message when there is one. babysitter.log elides messages at
    # varying lengths, so keying on the exact frame would split one bug into a
    # dozen entries; distinct frames are reported within the group instead.
    if not rec["message"]:
        # The panic header fell outside the grep window: identify by top frame.
        rec["signature"] = ("frame", rec["frame"] or rec["logfile"])
    elif GENERIC_MSG_RE.match(rec["message"]):
        rec["signature"] = ("msg+pkg", rec["message"], frame_package(rec["frame"]))
    else:
        rec["signature"] = ("msg", rec["message"])
    return rec


def group(records):
    groups = OrderedDict()
    for rec in records:
        g = groups.setdefault(rec["signature"], {
            "message": rec["message"], "frame": rec["frame"],
            "frames": Counter(), "components": Counter(), "count": 0, "trunc": 0,
            "nodes": Counter(), "suites": Counter(), "logfiles": Counter(),
            "versions": Counter(), "node_versions": Counter(),
            "dates": Counter(), "lines": [], "scans": Counter(), "tests": set(),
            "attributions": Counter(), "areas": Counter(),
            "blocks": set(), "sibling_messages": Counter(),
            "sibling_frames": Counter(), "sibling_components": Counter(),
            "example": rec,
        })
        g["count"] += 1
        if rec["truncated"]:
            g["trunc"] += 1
        if rec["component"]:
            g["components"][rec["component"]] += 1
        if rec["node"]:
            g["nodes"][rec["node"]] += 1
        if rec["suite"]:
            g["suites"][rec["suite"]] += 1
        if rec["version"]:
            g["versions"][rec["version"]] += 1
        if rec["node_version"]:
            g["node_versions"][rec["node_version"]] += 1
        if rec["collected"]:
            g["dates"][rec["collected"][:8]] += 1
        if len(g["lines"]) < 25 and rec["line"] not in g["lines"]:
            g["lines"].append(rec["line"])
        g["scans"][rec["scan"]] += 1
        g["blocks"].add(rec["block"])
        g["logfiles"][rec["logfile"]] += 1
        if rec["suite"] and rec["test"]:
            g["tests"].add("%s/test_%s" % (rec["suite"], rec["test"]))
        if rec["frame"]:
            g["frames"][rec["frame"]] += 1
        if rec["attribution"]:
            g["attributions"][rec["attribution"]] += 1
        if rec["area"]:
            g["areas"][rec["area"]] += 1
        if len(rec["frame"]) > len(g["frame"]):
            g["frame"] = rec["frame"]
        if len(rec["stack"]) > len(g["example"]["stack"]):
            g["example"] = rec
    return groups


def absorb(dst, src):
    dst["count"] += src["count"]
    dst["trunc"] += src["trunc"]
    for field in ("components", "nodes", "suites", "logfiles", "frames",
                  "versions", "node_versions", "dates", "scans", "attributions",
                  "areas", "sibling_messages", "sibling_frames",
                  "sibling_components"):
        dst[field].update(src[field])
    dst["tests"] |= src["tests"]
    dst["blocks"] |= src["blocks"]
    merged = dst["lines"] + [n for n in src["lines"] if n not in dst["lines"]]
    dst["lines"] = sorted(merged)[:25]
    if len(src["frame"]) > len(dst["frame"]):
        dst["frame"] = src["frame"]
    if len(src["example"]["stack"]) > len(dst["example"]["stack"]):
        dst["example"] = src["example"]


def attach_node_versions(records, node_versions):
    """A panic block only states a version on the server scan. For archive
    panics, note what that node was running when core_dump_checker scanned it --
    but keep it in a separate field, never merged with the authoritative value:
    nodes get reimaged constantly, so a months-old archive was very likely
    collected on a different build than the one the node runs today."""
    for rec in records:
        if not rec["version"]:
            rec["node_version"] = node_versions.get(rec["node"], "")
    return records


def merge_truncated(groups):
    """babysitter.log elides long panic messages mid-word, so the same crash can
    appear as both a full message and a prefix of it. Fold prefixes into the
    longest message they match."""
    MIN_PREFIX = 25
    keys = sorted(groups, key=lambda k: -len(groups[k]["message"]))
    for short in list(keys):
        if short not in groups or short[0] not in ("msg", "msg+pkg"):
            continue
        smsg = groups[short]["message"]
        if len(smsg) < MIN_PREFIX:
            continue
        # A complete short message is a different bug, not a truncation of a
        # longer one -- only fold when every hit was elided by the log.
        if groups[short]["trunc"] != groups[short]["count"]:
            continue
        for long in keys:
            if long == short or long not in groups or long[0] != short[0]:
                continue
            lmsg = groups[long]["message"]
            if len(lmsg) > len(smsg) and lmsg.startswith(smsg):
                absorb(groups[long], groups.pop(short))
                break
    return groups


def correlate_blocks(groups, records):
    """One crash can straddle two regions of the same panic block: the message
    lands in babysitter.log (Erlang-wrapped, no frames) while the stack lands in
    the service log (frames, no message). Neither region has the whole picture,
    so surface what the *other* regions of the same block saw -- including which
    component they pointed at, which is often the only attribution available."""
    block_msgs = defaultdict(Counter)
    block_frames = defaultdict(Counter)
    block_comps = defaultdict(Counter)
    for rec in records:
        if rec["message"]:
            block_msgs[rec["block"]][rec["message"]] += 1
        if rec["frame"]:
            block_frames[rec["block"]][rec["frame"]] += 1
        # Only frame/crash-event attributions are solid enough to lend out.
        if rec["component"] and rec["attribution"] in ("crash-event", "frame",
                                                       "logfile"):
            block_comps[rec["block"]][rec["component"]] += 1

    for g in groups.values():
        own = set(g["components"])
        for b in g["blocks"]:
            if not g["message"]:
                g["sibling_messages"].update(block_msgs.get(b, {}))
            if not g["frames"]:
                g["sibling_frames"].update(block_frames.get(b, {}))
            for comp, n in block_comps.get(b, {}).items():
                if comp not in own:
                    g["sibling_components"][comp] += n
    return groups


def backfill_components(groups):
    """diag.log aggregates every service log, so a panic seen only there has no
    component of its own. If the identical message appeared in a
    service-specific log elsewhere, borrow that attribution."""
    by_message = defaultdict(Counter)
    for g in groups.values():
        for comp, n in g["components"].items():
            if comp not in ("", "ns_server", "cbcollect"):
                by_message[g["message"]][comp] += n
    for g in groups.values():
        if g["components"] or not g["message"]:
            continue
        candidates = by_message.get(g["message"])
        if candidates:
            comp = candidates.most_common(1)[0][0]
            g["components"][comp] = 0          # 0 = inferred, not observed
            g["attributions"]["same-message"] += 0


def label(g):
    if not g["components"]:
        return "unknown"
    return ",".join("%s%s" % (c, "?" if n == 0 else "")
                    for c, n in g["components"].most_common())


def archive_link(base_url, archive):
    """Turn a collected-archive path into a URL, if the team serves those paths
    somewhere. The archive path is the strongest per-panic identifier the log
    carries, so this is the closest thing to a per-panic link that can actually
    work -- but only the team knows the base, hence the opt-in flag."""
    if not base_url or not archive:
        return ""
    path = archive
    for prefix in ("/data/workspace/", "/data/", "/"):
        if path.startswith(prefix):
            path = path[len(prefix):]
            break
    return "%s/%s" % (base_url.rstrip("/"), path)


def console_link(build_url):
    """Link to the upstream build's console.

    Deliberately NOT a per-panic deep link. Jenkins offers no way to address a
    line: /logText/progressiveText?start=N indexes the raw stored log, not the
    text it returns, and the two drift apart by megabytes -- a "deep link" built
    from a text offset lands somewhere unrelated. Use the reported line number
    and the archive path to locate the panic within the log instead."""
    if not build_url:
        return ""
    return "%s/consoleFull" % build_url.rstrip("/")


def report(groups, show_full, out, build_url="", archive_base=""):
    for i, g in enumerate(sorted(groups.values(), key=lambda x: -x["count"]), 1):
        out.write("=" * 100 + "\n")
        out.write("#%-3d %s  (%d crash site%s, %d node%s, %d suite%s)\n" % (
            i, label(g), g["count"], "" if g["count"] == 1 else "s",
            len(g["nodes"]), "" if len(g["nodes"]) == 1 else "s",
            len(g["suites"]), "" if len(g["suites"]) == 1 else "s"))
        out.write("=" * 100 + "\n")
        msg = g["message"] if show_full else g["message"][:300]
        out.write("  panic : %s\n" % (msg or "(no panic header inside the grep window)"))
        for frame, n in g["frames"].most_common(4):
            out.write("  frame : %-72s x%d\n" % (frame[:72], n))
        if len(g["frames"]) > 4:
            out.write("  frame : ... and %d more\n" % (len(g["frames"]) - 4))
        if g["attributions"]:
            out.write("  how   : component from %s\n" % ", ".join(
                "%s (x%d)" % (k, v) for k, v in g["attributions"].most_common()))
        for msg, n in g["sibling_messages"].most_common(3):
            out.write("  same-block header: %-55s x%d\n" % (msg[:55], n))
        for frame, n in g["sibling_frames"].most_common(3):
            out.write("  same-block frame : %-55s x%d\n" % (frame[:55], n))
        if g["sibling_components"]:
            out.write("  same-block component: %s\n" % ", ".join(
                "%s x%d" % (k, v)
                for k, v in g["sibling_components"].most_common(4)))
        out.write("  logs  : %s\n" % ", ".join(
            "%s x%d" % (k, v) for k, v in g["logfiles"].most_common(4)))
        if g["versions"]:
            out.write("  build : %s\n" % ", ".join(
                "%s x%d" % (k, v) for k, v in g["versions"].most_common(6)))
        if g["node_versions"] and not g["versions"]:
            # Deliberately a separate, hedged line -- see attach_node_versions().
            out.write("  build?: %s\n" % ", ".join(
                "%s x%d" % (k, v) for k, v in g["node_versions"].most_common(4)))
            out.write("          ^ what these nodes ran when scanned, not "
                      "necessarily the build that crashed --\n"
                      "            compare against 'dated' above; nodes are "
                      "reimaged between runs\n")
        elif not g["versions"]:
            out.write("  build : unknown (not stated in the console log for "
                      "archive-scanned panics)\n")
        if g["dates"]:
            days = sorted(g["dates"])
            span = days[0] if len(days) == 1 else "%s .. %s" % (days[0], days[-1])
            out.write("  dated : %s (%d distinct day%s)\n" % (
                span, len(days), "" if len(days) == 1 else "s"))
        out.write("  scan  : %s\n" % ", ".join(
            "%s x%d" % (k, v) for k, v in g["scans"].most_common()))
        if g["areas"]:
            out.write("  areas : %s\n" % ", ".join(
                "%s x%d" % (k, v) for k, v in g["areas"].most_common(8)))
        if g["nodes"]:
            out.write("  nodes : %s%s\n" % (
                ", ".join(k for k, _ in g["nodes"].most_common(5)),
                " ..." if len(g["nodes"]) > 5 else ""))
        for suite, n in g["suites"].most_common(6):
            out.write("  suite : %-72s x%d\n" % (suite[:72], n))
        if len(g["suites"]) > 6:
            out.write("  suite : ... and %d more\n" % (len(g["suites"]) - 6))
        ex = g["example"]
        if ex["archive"]:
            out.write("  sample: %s\n" % ex["archive"])
        elif ex["logpath"]:
            out.write("  sample: %s on %s\n" % (ex["logpath"], ex["node"] or "?"))
        out.write("  in log: line %d%s\n" % (
            ex["line"],
            "" if len(g["lines"]) < 2 else
            " (also lines %s)" % ", ".join(str(n) for n in g["lines"][1:4])))
        link = console_link(build_url)
        if link:
            out.write("  console: %s\n" % link)
        alink = archive_link(archive_base, ex["archive"])
        if alink:
            out.write("  archive: %s\n" % alink)
        if ex["event_time"]:
            out.write("  when  : %s\n" % ex["event_time"])
        if show_full and ex["stack"]:
            out.write("  stack :\n")
            for line in ex["stack"].splitlines():
                out.write("      %s\n" % line)
        out.write("\n")


def build_metrics(groups, records, parser):
    """Counts-only summary, for CI gating and build descriptions."""
    per_comp = {}
    for g in groups.values():
        for comp in (g["components"] or {"unattributed": 0}):
            entry = per_comp.setdefault(comp, {"signatures": 0, "crash_sites": 0,
                                               "frames": set()})
            entry["signatures"] += 1
            entry["crash_sites"] += g["count"]
            entry["frames"].update(g["frames"])
    for entry in per_comp.values():
        entry["frames"] = sorted(entry["frames"])[:20]

    areas = Counter()
    for g in groups.values():
        for area, n in g["areas"].items():
            areas[area] += n

    return {
        "raw_match_lines": parser.raw_hits,
        "crash_sites": len(records),
        "dropped_false_positives": parser.dropped_fp,
        "signatures": len(groups),
        "unidentified_signatures": sum(
            1 for g in groups.values() if not g["message"] and not g["frames"]),
        "unattributed_signatures": sum(
            1 for g in groups.values() if not g["components"]),
        "components": sorted(per_comp.keys()),
        "by_component": dict(
            (k, {"signatures": v["signatures"], "crash_sites": v["crash_sites"],
                 "frames": v["frames"]})
            for k, v in per_comp.items()),
        "by_test_area": areas.most_common(),
        "builds_seen": sorted(set(
            v for g in groups.values() for v in g["versions"])),
        "node_builds_at_scan": sorted(set(
            v for g in groups.values() for v in g["node_versions"])),
    }


def json_payload(selected, build_url="", archive_base=""):
    return [{
        "message": g["message"], "frame": g["frame"],
        "frames": g["frames"].most_common(),
        "components": g["components"].most_common(),
        "attribution": g["attributions"].most_common(),
        "test_areas": g["areas"].most_common(),
        "same_block_headers": g["sibling_messages"].most_common(5),
        "same_block_frames": g["sibling_frames"].most_common(5),
        "same_block_components": g["sibling_components"].most_common(5),
        "crash_sites": g["count"], "nodes": sorted(g["nodes"]),
        "suites": g["suites"].most_common(),
        "logfiles": g["logfiles"].most_common(),
        "versions": g["versions"].most_common(),
        "node_versions_at_scan": g["node_versions"].most_common(),
        "dates": sorted(g["dates"]),
        "scans": g["scans"].most_common(),
        "console_lines": g["lines"],
        "console_url": console_link(build_url),
        "archive_url": archive_link(archive_base, g["example"]["archive"]),
        "tests": sorted(g["tests"]),
        "sample_archive": g["example"]["archive"],
        "sample_logpath": g["example"]["logpath"],
        "when": g["example"]["event_time"],
        "stack": g["example"]["stack"],
    } for g in sorted(selected.values(), key=lambda x: -x["count"])]


def write_header(out, groups, selected, records, parser):
    headerless = [g for g in groups.values() if not g["message"]]
    unattributed = [g for g in groups.values() if not g["components"]]
    out.write("Raw grep match lines           : %d\n" % parser.raw_hits)
    out.write("Crash sites (log regions)      : %d\n" % len(records))
    out.write("Dropped false positives        : %d regions "
              "(case-insensitive base64 matches)\n" % parser.dropped_fp)
    out.write("Unique panic signatures        : %d\n" % len(groups))
    out.write("  identified by frame only     : %d signatures "
              "(panic header outside grep window)\n" % len(headerless))
    out.write("  with no component attributed : %d signatures\n" % len(unattributed))
    server_regions = sum(1 for r in records if r["scan"] == "server")
    if not server_regions:
        out.write("Build versions                 : unavailable -- this log has "
                  "no server-scan section, and that is\n"
                  "                                 the only place a build "
                  "version appears. (An aborted upstream\n"
                  "                                 build often stops before "
                  "that phase.)\n")
    comps = all_components(groups)
    out.write("Signatures per component       : %s\n" % (", ".join(
        "%s=%d" % (k, v) for k, v in comps.most_common()) or "none"))
    sites = Counter()
    for g in groups.values():
        for comp in g["components"]:
            sites[comp] += g["count"]
    out.write("Crash sites per component      : %s\n" % (", ".join(
        "%s=%d" % (k, v) for k, v in sites.most_common()) or "none"))
    out.write("Showing                        : %d signature(s)"
              "   ('comp?' = inferred from an identical panic in a service log)\n\n"
              % len(selected))


def select(groups, component=None, area=None, min_sites=0, include_related=False):
    """Filter signatures. `component` matches the attributed component; with
    `include_related` it also keeps signatures whose sibling regions in the same
    block pointed at that component (the other half of a split crash)."""
    sel = groups
    if component:
        wanted = set(c.strip() for c in component.split(",") if c.strip())
        def hit(g):
            if wanted & set(g["components"]):
                return True
            return include_related and bool(wanted & set(g["sibling_components"]))
        sel = OrderedDict((k, v) for k, v in sel.items() if hit(v))
    if area:
        wanted = set(a.strip() for a in area.split(",") if a.strip())
        sel = OrderedDict((k, v) for k, v in sel.items()
                          if wanted & set(v["areas"]))
    if min_sites:
        sel = OrderedDict((k, v) for k, v in sel.items()
                          if v["count"] >= min_sites)
    return sel


def all_components(groups):
    comps = Counter()
    for g in groups.values():
        for comp in g["components"]:
            comps[comp] += 1
    return comps


def write_all_reports(outdir, groups, records, parser, build_url="",
                      archive_base=""):
    """Emit every report variant from a single parse -- the console log is ~300MB,
    so re-reading it once per report wastes minutes in CI."""
    if not os.path.isdir(outdir):
        os.makedirs(outdir)

    def path(name):
        return os.path.join(outdir, name)

    with open(path("metrics.json"), "w") as fh:
        json.dump(build_metrics(groups, records, parser), fh, indent=2)
        fh.write("\n")

    variants = [
        ("panics_all.txt", select(groups), False),
        ("panics_all_stacks.txt", select(groups), True),
    ]
    for name, sel, full in variants:
        with open(path(name), "w") as fh:
            write_header(fh, groups, sel, records, parser)
            report(sel, full, fh, build_url, archive_base)

    with open(path("panics_all.json"), "w") as fh:
        json.dump(json_payload(select(groups), build_url, archive_base), fh,
                  indent=2)
        fh.write("\n")

    # One file per component, so each team can open just their own artifact.
    comp_dir = os.path.join(outdir, "by-component")
    if not os.path.isdir(comp_dir):
        os.makedirs(comp_dir)
    for comp in all_components(groups):
        sel = select(groups, component=comp, include_related=True)
        with open(os.path.join(comp_dir, "%s.txt" % comp), "w") as fh:
            fh.write("Panics attributed to component: %s\n" % comp)
            fh.write("(includes signatures whose same-block sibling pointed here)\n\n")
            write_header(fh, groups, sel, records, parser)
            report(sel, True, fh, build_url, archive_base)

    with open(path("summary.txt"), "w") as fh:
        write_header(fh, groups, select(groups), records, parser)

    return path("summary.txt")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("logfile", help="consoleText file, or - for stdin")
    ap.add_argument("--component",
                    help="only these components, comma separated "
                         "(search, index, query, projector, analytics, ...)")
    ap.add_argument("--related", action="store_true",
                    help="with --component, also keep signatures whose same-block "
                         "sibling pointed at that component")
    ap.add_argument("--area", help="only these test areas, comma separated "
                                   "(fts, xdcr, 2i, eventing, os_certify, ...)")
    ap.add_argument("--min-sites", type=int, default=0, metavar="N",
                    help="hide signatures with fewer than N crash sites")
    ap.add_argument("--list-components", action="store_true",
                    help="list the components and test areas found, then exit")
    ap.add_argument("--archive-base-url", default="",
                    help="base URL that serves the collected /data/workspace "
                         "archives; turns each panic's diag.zip path into a link")
    ap.add_argument("--build-url", default="",
                    help="upstream core_dump_checker build URL; turns each panic "
                         "into a clickable deep link into that console log")
    ap.add_argument("--full", action="store_true", help="print full stack traces")
    ap.add_argument("--json", action="store_true", help="emit JSON")
    ap.add_argument("--metrics", action="store_true",
                    help="emit a small JSON summary (counts only), for CI thresholds")
    ap.add_argument("--all-reports", metavar="DIR",
                    help="write every report variant into DIR from a single parse "
                         "(use this in CI instead of running the script repeatedly)")
    args = ap.parse_args()

    # Binary read: the log mixes encodings across services, and decoding per
    # line keeps one bad byte from killing the parse.
    stream = sys.stdin.buffer if args.logfile == "-" else open(args.logfile, "rb")
    parser = Parser()
    try:
        records = list(parser.feed(stream))
    finally:
        if args.logfile != "-":
            stream.close()

    attach_node_versions(records, parser.node_versions)
    groups = merge_truncated(group(records))
    correlate_blocks(groups, records)
    backfill_components(groups)

    if args.all_reports:
        summary = write_all_reports(args.all_reports, groups, records, parser,
                                    args.build_url, args.archive_base_url)
        with open(summary) as fh:
            sys.stdout.write(fh.read())
        return

    if args.list_components:
        sys.stdout.write("components (signatures):\n")
        for comp, n in all_components(groups).most_common():
            sys.stdout.write("  %-14s %d\n" % (comp, n))
        areas = Counter()
        for g in groups.values():
            for area, n in g["areas"].items():
                areas[area] += n
        sys.stdout.write("\ntest areas (crash sites):\n")
        for area, n in areas.most_common():
            sys.stdout.write("  %-14s %d\n" % (area, n))
        return

    selected = select(groups, component=args.component, area=args.area,
                      min_sites=args.min_sites, include_related=args.related)

    if args.metrics:
        json.dump(build_metrics(groups, records, parser), sys.stdout, indent=2)
        sys.stdout.write("\n")
        return

    if args.json:
        json.dump(json_payload(selected, args.build_url, args.archive_base_url),
                  sys.stdout, indent=2)
        sys.stdout.write("\n")
        return

    write_header(sys.stdout, groups, selected, records, parser)
    report(selected, args.full, sys.stdout, args.build_url,
           args.archive_base_url)


if __name__ == "__main__":
    main()
