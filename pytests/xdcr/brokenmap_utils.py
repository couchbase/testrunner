"""Pure parsers for XDCR broken-collection-mapping state (CBSE-23458, MB-73479).

Every function takes what a REST call, metakv or goxdcr.log returned and
returns plain Python, so the parsing is unit-tested without a cluster
(test_brokenmap_utils.py). Formats come from goxdcr source:
base.BrokenMappingUIString, pipeline_svc/checkpoint_manager.go (the repaired
message and the checkpoint-before-stop lines), metadata.CollectionNamespaceMapping
.String() (one SOURCE line per source namespace) and
metadata_svc/checkpoints_service.go ("Loaded brokenMap"); the metakv layout
from support's broken_mappings.py (goxdcr change 254675, tools/broken_mappings.py).
"""

import base64
import json
import re

BROKEN_HEADER = "Found following destination collection(s) missing"
REPAIRED_HEADER = "Following collection mappings are now repaired and replicating"
# goxdcr's broken-mapping event type (goxdcr test 2j's BROKENEVENT_TYPE)
BROKEN_EVENT_TYPE = 3
BACKFILL_PREFIX = "backfill_"
# the field a checkpoint record points at its brokenMappings record with
CKPT_SHA_FIELD = "brokenCollectionsMapSha256"
# Variable-VB replications use global checkpoints: a record points at a
# GlobalTimestamp in the pipeline's globalInfo doc, whose snappy-compressed
# JSON holds a broken-map sha per target vb.
GLOBAL_INFO_KEY = "globalInfo"
GLOBAL_TS_SHA_FIELD = "globalTimestampMapSha256"
_GLOBAL_TS_BROKEN_SHA = re.compile(rb'"brokenCollectionsMapSha256":"([0-9a-f]+)"')

CKPT_STOP_START = "Starting checkpointing before stopping"
CKPT_STOP_DONE = "Checkpointing completed"
CKPT_STOP_TIMEOUT = "Checkpointing timed out after"
CKPT_STOP_SKIPPED = "checkpointing is skipped"

_MAPPING_LINE = re.compile(r"SOURCE \|\|(?P<src>[^|]+)\|\| -> TARGET\(s\) (?P<tgts>.*)")
_TARGET = re.compile(r"\|Scope: (?P<scope>\S+) Collection: (?P<coll>\S+)\|")
# goxdcr replaces a mapping too long to log with this summary
_ELIDED = "CollectionNamespaceMapping with"
_LOG_LINE_START = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
_LOADED = "Loaded brokenMap:"


class ElidedMappingError(ValueError):
    """goxdcr logged a mapping count instead of the mappings themselves."""


def _pairs_in_section(text):
    pairs = set()
    for line in text.splitlines():
        match = _MAPPING_LINE.search(line)
        if not match:
            continue
        src = match.group("src").strip()
        for tgt in _TARGET.finditer(match.group("tgts")):
            pairs.add((src, "{}.{}".format(tgt.group("scope"), tgt.group("coll"))))
    if not pairs and _ELIDED in text:
        raise ElidedMappingError(text.strip()[:200])
    return pairs


def _sections(messages, header, other_header):
    for msg in messages:
        if not isinstance(msg, str) or header not in msg:
            continue
        for section in msg.split(header)[1:]:
            yield section.split(other_header, 1)[0]


def parse_broken_mappings(messages):
    """(source "scope.coll", target "scope.coll") pairs reported broken.

    Only lines under a broken-mapping header count; a "now repaired" section
    in the same message, and every other event, is ignored.
    """
    pairs = set()
    for section in _sections(messages, BROKEN_HEADER, REPAIRED_HEADER):
        pairs |= _pairs_in_section(section)
    return pairs


def parse_repaired_mappings(messages):
    """Pairs listed under "now repaired and replicating" (UI log)."""
    pairs = set()
    for section in _sections(messages, REPAIRED_HEADER, BROKEN_HEADER):
        pairs |= _pairs_in_section(section)
    return pairs


def task_error_messages(task):
    """Text of every entry in a /pools/default/tasks xdcr task's errors.

    An entry is a string, or a dict whose string values carry the text.
    """
    messages = []
    for entry in task.get("errors") or []:
        if isinstance(entry, str):
            messages.append(entry)
        elif isinstance(entry, dict):
            messages.extend(v for v in entry.values() if isinstance(v, str))
    return messages


def parse_replication_infos(infos, pipeline_id=None):
    """Broken pairs from goxdcr's GET /pools/default/replicationInfos.

    The broken-mapping event is a tree, walked the way goxdcr test 2j's
    getBrokenMapEntryId walks it: whole map -> source scope -> "S1.col1"
    -> target "S3.col1". pipeline_id is the replication id for the main
    pipeline, or "backfill_" + the replication id for its backfill pipeline;
    without it every pipeline is read.
    """
    pairs = set()
    for info in infos or []:
        if pipeline_id is not None and info.get("Id") != pipeline_id:
            continue
        for event in info.get("ErrorList") or []:
            if event.get("EventType") != BROKEN_EVENT_TYPE:
                continue
            for scope_event in (event.get("EventExtras") or {}).values():
                for src_event in (scope_event.get("EventExtras") or {}).values():
                    for tgt_event in (src_event.get("EventExtras") or {}).values():
                        src, tgt = src_event.get("EventDesc"), tgt_event.get("EventDesc")
                        if src and tgt:
                            pairs.add((src, tgt))
    return pairs


def pipeline_ids(infos):
    """Every pipeline Id in a replicationInfos answer (main and backfill)."""
    return {info.get("Id") for info in infos or [] if info.get("Id")}


def pipeline_stat(infos, pipeline_id, stat):
    """One StatsMap value of one pipeline in a replicationInfos answer, or None."""
    for info in infos or []:
        if info.get("Id") == pipeline_id:
            value = (info.get("StatsMap") or {}).get(stat)
            return None if value is None else int(value)
    return None


def decode_broken_mappings_doc(body):
    """Decode a GET /_metakv/ckpt/<pipeline>/brokenMappings response.

    ns_server answers {"value": "<base64 goxdcr JSON>"}; the JSON holds
    NsMappingRecords (null once broken_mappings.py has patched it) and
    specInternalId. Returns None when the key is absent.
    """
    if isinstance(body, bytes):
        body = body.decode("utf-8")
    if isinstance(body, str):
        body = json.loads(body) if body.strip() else None
    if not body or "value" not in body:
        return None
    return json.loads(base64.b64decode(body["value"]))


def broken_map_shas(decoded):
    """Sha256Digest of every broken-map record in a decoded metakv doc."""
    if not decoded:
        return set()
    return {record.get("Sha256Digest") for record in decoded.get("NsMappingRecords") or []
            if record.get("Sha256Digest")}


def snappy_decompress(data):
    """Decompress one snappy block (the format goxdcr's snappy.Encode writes).

    Stdlib only, so global-checkpoint references can be read on a host without
    python-snappy: a varint length, then literals and back-references.
    """
    pos, length, shift = 0, 0, 0
    while True:
        b = data[pos]
        pos += 1
        length |= (b & 0x7F) << shift
        if b < 0x80:
            break
        shift += 7
    out = bytearray()
    while pos < len(data):
        tag = data[pos]
        pos += 1
        kind = tag & 3
        if kind == 0:
            n = tag >> 2
            if n >= 60:
                width = n - 59
                n = int.from_bytes(data[pos:pos + width], "little")
                pos += width
            n += 1
            out += data[pos:pos + n]
            pos += n
            continue
        if kind == 1:
            n = ((tag >> 2) & 7) + 4
            offset = ((tag >> 5) << 8) | data[pos]
            pos += 1
        else:
            width = 2 if kind == 2 else 4
            n = (tag >> 2) + 1
            offset = int.from_bytes(data[pos:pos + width], "little")
            pos += width
        if offset <= 0 or offset > len(out):
            raise ValueError("snappy back-reference outside the output")
        for _ in range(n):
            out.append(out[-offset])
    if len(out) != length:
        raise ValueError("snappy block decoded to {} bytes, header says {}".format(len(out), length))
    return bytes(out)


def parse_metakv_listing(text):
    """ns_server's answer to GET /_metakv/<dir>/ as a list of {"path", "value", "rev"}.

    The answer is a stream of concatenated JSON objects, not a JSON array
    (broken_mappings.py parses it the same way); keys are lowercased. An
    array is accepted too.
    """
    if isinstance(text, bytes):
        text = text.decode("utf-8")
    text = (text or "").strip()
    if text.startswith("["):
        objs = json.loads(text)
    else:
        decoder, objs, pos = json.JSONDecoder(), [], 0
        while pos < len(text):
            obj, pos = decoder.raw_decode(text, pos)
            objs.append(obj)
            while pos < len(text) and text[pos].isspace():
                pos += 1
    return [{k.lower(): v for k, v in obj.items()} for obj in objs]


def _walk(node, field, found):
    """Every non-empty string value of `field`, at any depth."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == field and isinstance(value, str) and value:
                found.add(value)
            else:
                _walk(value, field, found)
    elif isinstance(node, list):
        for item in node:
            _walk(item, field, found)


def checkpoint_broken_shas(children, decompress=None):
    """Broken-map shas that a pipeline's checkpoints still point at.

    children is parse_metakv_listing() of GET /_metakv/ckpt/<pipeline>/: the
    per-vb checkpoint docs, the brokenMappings doc (skipped) and, for a
    variable-VB replication, the globalInfo doc. A record points at a broken
    map directly, or (global checkpoints) through the GlobalTimestamp it names,
    whose broken-map shas are read only when `decompress` (snappy.decompress)
    is given.

    Returns (referenced, unverified, dangling):
      referenced: {where: {sha, ...}} -- a doc path, or "<globalInfo path>#<ts sha>"
      unverified: what could not be read (not base64 JSON, or a GlobalTimestamp
                  without `decompress`), so is not known to be clean
      dangling:   GlobalTimestamp shas a checkpoint names but globalInfo lacks
    """
    referenced, unverified, ts_refs, global_info = {}, [], set(), None
    for child in children or []:
        path, value = child.get("path", ""), child.get("value")
        key = path.rstrip("/").rsplit("/", 1)[-1]
        if not value or key == "brokenMappings":
            continue
        try:
            doc = json.loads(base64.b64decode(value))
        except (ValueError, TypeError):
            unverified.append(path)
            continue
        if key == GLOBAL_INFO_KEY:
            global_info = (path, doc)
            continue
        found = set()
        _walk(doc, CKPT_SHA_FIELD, found)
        if found:
            referenced[path] = found
        _walk(doc, GLOBAL_TS_SHA_FIELD, ts_refs)
    records, info_path = {}, GLOBAL_INFO_KEY
    if global_info:
        info_path = global_info[0]
        records = {r.get("Sha256Digest"): r for r in global_info[1].get("NsMappingRecords") or [] if r}
    dangling = []
    for ts_sha in sorted(ts_refs):
        where = "{}#{}".format(info_path, ts_sha)
        record = records.get(ts_sha)
        if record is None:
            dangling.append(ts_sha)
            continue
        if decompress is None:
            unverified.append(where)
            continue
        try:
            raw = decompress(base64.b64decode(record.get("CompressedMapping") or ""))
        except Exception:
            unverified.append(where)
            continue
        found = {sha.decode() for sha in _GLOBAL_TS_BROKEN_SHA.findall(raw)}
        if found:
            referenced[where] = found
    return referenced, sorted(unverified), dangling


def global_info_records(children):
    """Records in a pipeline's globalInfo doc, or None when it has none.

    goxdcr 8.5.0-1308 creates a globalInfo doc even for a replication it runs
    with variableVBMode=false, which broken_mappings.py reads as "global
    checkpoints" (measured on cbvm, 2026-10-18).
    """
    for child in children or []:
        if child.get("path", "").rstrip("/").rsplit("/", 1)[-1] == GLOBAL_INFO_KEY:
            try:
                doc = json.loads(base64.b64decode(child.get("value") or ""))
            except (ValueError, TypeError):
                return -1
            return len(doc.get("NsMappingRecords") or [])
    return None


def decoded_children(children):
    """{path: decoded JSON, or the raw base64 value when it is not JSON}.

    Used to compare a pipeline's metakv before and after a script mode: rev
    numbers are left out, so a byte-identical rewrite compares equal.
    """
    decoded = {}
    for child in children or []:
        value = child.get("value") or ""
        try:
            decoded[child.get("path", "")] = json.loads(base64.b64decode(value))
        except (ValueError, TypeError):
            decoded[child.get("path", "")] = value
    return decoded


def loaded_broken_map_pairs(log_text):
    """(count of "Loaded brokenMap:" lines, pairs they list) in goxdcr.log text.

    The map prints across several lines; a block ends at the next line that
    starts with a log timestamp.
    """
    blocks, pairs, current = 0, set(), None
    for line in log_text.splitlines():
        if _LOADED in line:
            if current is not None:
                pairs |= _pairs_in_section("\n".join(current))
            current = [line.split(_LOADED, 1)[1]]
            blocks += 1
        elif current is not None and _LOG_LINE_START.match(line):
            pairs |= _pairs_in_section("\n".join(current))
            current = None
        elif current is not None:
            current.append(line)
    if current is not None:
        pairs |= _pairs_in_section("\n".join(current))
    return blocks, pairs


def pipeline_stop_state(log_text, repl_id):
    """What a pause did to one replication's pipelines, from goxdcr.log text.

    Counts the checkpoint-before-stop lines of its main and backfill
    pipelines (started, completed, timed_out, skipped) and PipelineMgr's
    "Pipeline <repl_id> has been stopped" (stopped). The CBSE-23458 doc says to
    run the script only once checkpointing is done; on 8.5.0-1308 a pause can
    also stop a node's pipeline with no checkpoint-before-stop line at all
    (measured on cbvm, 2026-10-18), so "stopped, nothing in flight" is the
    condition, not one checkpoint per pipeline.
    """
    state = {"started": 0, "completed": 0, "timed_out": 0, "skipped": 0, "stopped": 0}
    stopped_line = "Pipeline {} has been stopped".format(repl_id)
    for line in log_text.splitlines():
        if repl_id not in line:
            continue
        if CKPT_STOP_START in line:
            state["started"] += 1
        elif CKPT_STOP_DONE in line:
            state["completed"] += 1
        elif CKPT_STOP_TIMEOUT in line:
            state["timed_out"] += 1
        elif CKPT_STOP_SKIPPED in line:
            state["skipped"] += 1
        if stopped_line in line:
            state["stopped"] += 1
    return state


def tail_start_line(mark, current_lines):
    """First line to read with `tail -n +N` after a line-count mark.

    A file shorter than the mark has rotated, so read it from the top.
    """
    return 1 if current_lines < mark else mark + 1
