"""Unit tests for brokenmap_utils (CBSE-23458 / MB-73479 parsers).

No cluster and no network are needed. Run from the repo root::

    python3 -m unittest pytests.xdcr.test_brokenmap_utils -v
"""

import base64
import json
import unittest

from . import brokenmap_utils as bmu

BROKEN_MSG = (
    "2026-10-05 13:28:38 Found following destination collection(s) missing "
    "(and will not get replicated to):\n"
    "SOURCE ||S1.col1|| -> TARGET(s) |Scope: S3 Collection: col1| \n")

TWO_BROKEN_ONE_REPAIRED = (
    "Found following destination collection(s) missing "
    "(and will not get replicated to):\n"
    "SOURCE ||S1.col2|| -> TARGET(s) |Scope: S1 Collection: col2| \n"
    "SOURCE ||S1.col3|| -> TARGET(s) |Scope: S3 Collection: col3| |Scope: S4 Collection: col3| \n"
    "Following collection mappings are now repaired and replicating:\n"
    "SOURCE ||S1.col1|| -> TARGET(s) |Scope: S3 Collection: col1| \n")

TOPOLOGY_MSG = ("127.0.0.1:9000 abc/B1/B2 Target: Topology change detected. "
                "Estimated time of pipeline restart: 2026-10-05 13:30:00")

REPL_ID = "928d9f4f3a3a90b55483034654a30980/B1/B1"

# Shape copied from CBSE-23458 (replicationInfos on 9998).
REPLICATION_INFOS = [
    {"Id": REPL_ID, "StatsMap": {"changes_left": 0}, "ErrorList": [
        {"EventId": 9, "EventType": 0, "EventDesc": TOPOLOGY_MSG, "EventExtras": {}},
        {"EventId": 0, "EventType": 3, "EventDesc": "", "EventExtras": {
            "1": {"EventId": 1, "EventType": 3, "EventDesc": "S1", "EventExtras": {
                "2": {"EventId": 2, "EventType": 3, "EventDesc": "S1.C1", "EventExtras": {
                    "3": {"EventId": 3, "EventType": 3, "EventDesc": "S1.C1",
                          "EventExtras": {}}}}}}}},
    ]},
    {"Id": "backfill_" + REPL_ID, "ErrorList": [
        {"EventId": 0, "EventType": 3, "EventDesc": "", "EventExtras": {
            "1": {"EventId": 1, "EventType": 3, "EventDesc": "S9", "EventExtras": {
                "2": {"EventId": 2, "EventType": 3, "EventDesc": "S9.x", "EventExtras": {
                    "3": {"EventId": 3, "EventType": 3, "EventDesc": "T9.x",
                          "EventExtras": {}}}}}}}}]},
]

# The CBSE-23458 doc's sample GET /_metakv/ckpt/<replId>/brokenMappings body ("view" mode).
METAKV_BODY = json.dumps({"value": (
    "eyJOc01hcHBpbmdSZWNvcmRzIjpbeyJDb21wcmVzc2VkTWFwcGluZyI6InFnR2dleUpUYjNWeVkyVkRiMnhzWldOMGFXOXVj"
    "eUk2VzNzaVUyTnZjR1ZPWVcxbElqb2lVekVpTENJWklRMFdlR052YkRFaWZWMHNJa2x1WkdseVpXTjBWR0Z5WjJWMFRXRndJ"
    "anA3SWpCR1JnQUVNeUptUmdBRWZTd05pQUZkVkhOd1lXTmxWSGx3WlUxaGNDSTZleUl3SWpvd2ZYMD0iLCJTaGEyNTZEaWdl"
    "c3QiOiIyZDc1YTNiZjcxM2Y5N2JlMDE5NGI1MjZlOTM0NmQzNzUyNjhlOGY5ODIxMmUwMjhhYjU4NzJkNzFiZjUzOWEwIn1d"
    "LCJzcGVjSW50ZXJuYWxJZCI6ImI3eTZUOTJxdXhxWFdIQl9OVG5pc0E9PSJ9")})
SAMPLE_SHA = "2d75a3bf713f97be0194b526e9346d375268e8f98212e028ab5872d71bf539a0"


def _b64json(doc):
    return base64.b64encode(json.dumps(doc).encode()).decode()


CKPT_CHILDREN = [
    {"path": "/ckpt/{}/brokenMappings".format(REPL_ID), "value": _b64json(
        {"NsMappingRecords": [{"Sha256Digest": SAMPLE_SHA}]})},
    {"path": "/ckpt/{}/0".format(REPL_ID), "value": _b64json(
        {"checkpoints": [{"seqno": 12, bmu.CKPT_SHA_FIELD: SAMPLE_SHA},
                         {"seqno": 3, bmu.CKPT_SHA_FIELD: ""}]})},
    {"path": "/ckpt/{}/1".format(REPL_ID), "value": _b64json(
        {"checkpoints": [{"seqno": 7, bmu.CKPT_SHA_FIELD: ""}]})},
    {"path": "/ckpt/{}/globalInfo".format(REPL_ID),
     "value": base64.b64encode(b"\xff\x06\x00\x00sNaPpY").decode()},
]

GOXDCR_LOG = (
    "2026-10-05T13:30:01.100Z INFO GOXDCR.CheckpointSvc: Loaded brokenMap: map[]\n"
    "2026-10-05T13:31:01.123Z INFO GOXDCR.CheckpointSvc: Loaded brokenMap: map[" + SAMPLE_SHA +
    ":SOURCE ||S1.col1|| -> TARGET(s) |Scope: S3 Collection: col1| \n"
    "SOURCE ||S1.col2|| -> TARGET(s) |Scope: S3 Collection: col2| \n"
    "]\n"
    "2026-10-05T13:31:01.200Z INFO GOXDCR.CheckpointMgr: SOURCE ||S9.zz|| -> TARGET(s) |Scope: S9 Collection: zz| \n")

# Shapes copied from goxdcr.log on 8.5.0-1308 (cbvm, 2026-10-18)
CKPT_STOP_LOG = (
    "2026-10-17T19:05:43.685Z INFO GOXDCR.CheckpointMgr: pipelineFullTopic=" + REPL_ID +
    ", Starting checkpointing before stopping\n"
    "2026-10-17T19:05:43.725Z INFO GOXDCR.CheckpointMgr: pipelineFullTopic=" + REPL_ID +
    ", Checkpointing completed\n"
    "2026-10-17T19:05:43.800Z INFO GOXDCR.CheckpointMgr: pipelineFullTopic=other/B1/B2"
    ", Starting checkpointing before stopping\n"
    "2026-10-17T19:07:09.187Z ERRO GOXDCR.CheckpointMgr: pipelineFullTopic=backfill_" + REPL_ID +
    ", Pipeline has not been started (or has been disabled) - checkpointing is skipped\n"
    "2026-10-17T19:08:02.100Z INFO GOXDCR.CheckpointMgr: pipelineFullTopic=" + REPL_ID +
    ", Checkpointing timed out after 3m0s.\n"
    "2026-10-17T19:11:03.960Z INFO GOXDCR.GenericPipeline: pipelineFullTopic=" + REPL_ID +
    ", MainPipeline " + REPL_ID + "-978702126 has been stopped\n"
    "2026-10-17T19:11:03.960Z INFO GOXDCR.PipelineMgr: Pipeline " + REPL_ID + " has been stopped\n")


class ParseBrokenMappingsTest(unittest.TestCase):

    def test_single_broken_pair(self):
        self.assertEqual(bmu.parse_broken_mappings([BROKEN_MSG]), {("S1.col1", "S3.col1")})

    def test_multi_target_and_repaired_section(self):
        self.assertEqual(bmu.parse_broken_mappings([TWO_BROKEN_ONE_REPAIRED]),
                         {("S1.col2", "S1.col2"), ("S1.col3", "S3.col3"), ("S1.col3", "S4.col3")})

    def test_repaired_section_not_counted_broken(self):
        self.assertNotIn(("S1.col1", "S3.col1"),
                         bmu.parse_broken_mappings([TWO_BROKEN_ONE_REPAIRED]))
        self.assertEqual(bmu.parse_repaired_mappings([TWO_BROKEN_ONE_REPAIRED]),
                         {("S1.col1", "S3.col1")})

    def test_ignores_topology_event_and_non_strings(self):
        self.assertEqual(bmu.parse_broken_mappings([TOPOLOGY_MSG, None, {"x": 1}]), set())

    def test_elided_mapping_raises(self):
        elided = bmu.BROKEN_HEADER + " (and will not get replicated to):\n" \
                 "CollectionNamespaceMapping with 1200 entries"
        with self.assertRaises(bmu.ElidedMappingError):
            bmu.parse_broken_mappings([elided])

    def test_task_error_messages_strings_and_dicts(self):
        task = {"id": REPL_ID, "errors": [BROKEN_MSG, {"time": "x", "msg": TOPOLOGY_MSG}, 7]}
        self.assertEqual(bmu.task_error_messages(task), [BROKEN_MSG, "x", TOPOLOGY_MSG])
        self.assertEqual(bmu.task_error_messages({"id": REPL_ID}), [])


class ParseReplicationInfosTest(unittest.TestCase):

    def test_main_pipeline_only(self):
        self.assertEqual(bmu.parse_replication_infos(REPLICATION_INFOS, REPL_ID),
                         {("S1.C1", "S1.C1")})

    def test_backfill_pipeline_only(self):
        self.assertEqual(
            bmu.parse_replication_infos(REPLICATION_INFOS, bmu.BACKFILL_PREFIX + REPL_ID),
            {("S9.x", "T9.x")})

    def test_without_pipeline_id_reads_every_pipeline(self):
        self.assertEqual(bmu.parse_replication_infos(REPLICATION_INFOS),
                         {("S1.C1", "S1.C1"), ("S9.x", "T9.x")})

    def test_pipeline_ids(self):
        self.assertEqual(bmu.pipeline_ids(REPLICATION_INFOS),
                         {REPL_ID, bmu.BACKFILL_PREFIX + REPL_ID})

    def test_pipeline_stat(self):
        self.assertEqual(bmu.pipeline_stat(REPLICATION_INFOS, REPL_ID, "changes_left"), 0)
        # the backfill pipeline in the sample has no StatsMap; an absent pipeline neither
        self.assertIsNone(bmu.pipeline_stat(REPLICATION_INFOS, bmu.BACKFILL_PREFIX + REPL_ID,
                                            "changes_left"))
        self.assertIsNone(bmu.pipeline_stat(REPLICATION_INFOS, "nope", "changes_left"))

    def test_odd_event_shape_skipped(self):
        odd = [{"Id": REPL_ID, "ErrorList": [{"EventType": 3, "EventExtras": {
            "1": {"EventExtras": {"2": {"EventExtras": {"3": {}}}}}}}]}]
        self.assertEqual(bmu.parse_replication_infos(odd, REPL_ID), set())


class MetakvTest(unittest.TestCase):

    def test_decodes_doc_sample(self):
        decoded = bmu.decode_broken_mappings_doc(METAKV_BODY)
        self.assertEqual(decoded["specInternalId"], "b7y6T92quxqXWHB_NTnisA==")
        self.assertEqual(bmu.broken_map_shas(decoded), {SAMPLE_SHA})

    def test_patched_doc_has_no_shas(self):
        patched = {"value": _b64json({"NsMappingRecords": None, "specInternalId": "x"})}
        self.assertEqual(bmu.broken_map_shas(bmu.decode_broken_mappings_doc(patched)), set())

    def test_absent_key(self):
        self.assertIsNone(bmu.decode_broken_mappings_doc(""))
        self.assertIsNone(bmu.decode_broken_mappings_doc({}))
        self.assertEqual(bmu.broken_map_shas(None), set())

    def test_checkpoint_shas_skip_broken_mappings_and_flag_unreadable(self):
        referenced, unverified, dangling = bmu.checkpoint_broken_shas(CKPT_CHILDREN)
        self.assertEqual(referenced, {"/ckpt/{}/0".format(REPL_ID): {SAMPLE_SHA}})
        self.assertEqual(unverified, ["/ckpt/{}/globalInfo".format(REPL_ID)])
        self.assertEqual(dangling, [])

    def test_checkpoint_shas_after_patch(self):
        patched = [{"path": "/ckpt/x/0", "value": _b64json(
            {"checkpoints": [{"seqno": 12, bmu.CKPT_SHA_FIELD: ""}]})},
            {"path": "/ckpt/x/1", "value": ""}]
        self.assertEqual(bmu.checkpoint_broken_shas(patched), ({}, [], []))
        self.assertEqual(bmu.checkpoint_broken_shas(None), ({}, [], []))

    def test_global_checkpoints_followed_into_global_info(self):
        info_path = "/ckpt/x/globalInfo"
        # what broken_mappings.py decompresses: the GlobalTimestamp JSON, one broken sha per target vb
        ts_raw = (b'{"global_info_type":0,"global_info_data":{"0":{"brokenCollectionsMapSha256":"'
                  + SAMPLE_SHA.encode() + b'"},"1":{"brokenCollectionsMapSha256":""}}}')
        children = [
            {"path": "/ckpt/x/0", "value": _b64json({"checkpoints": [
                {"target_vb_opaque": None, bmu.GLOBAL_TS_SHA_FIELD: "ts1", bmu.CKPT_SHA_FIELD: ""},
                {"target_vb_opaque": None, bmu.GLOBAL_TS_SHA_FIELD: "ts2"}]})},
            {"path": info_path, "value": _b64json({"NsMappingRecords": [
                {"CompressedMapping": base64.b64encode(ts_raw).decode(), "Sha256Digest": "ts1"}]})},
        ]
        self.assertEqual(bmu.checkpoint_broken_shas(children, decompress=lambda b: b),
                         ({info_path + "#ts1": {SAMPLE_SHA}}, [], ["ts2"]))
        # without snappy the GlobalTimestamp is reported unverified, never clean
        self.assertEqual(bmu.checkpoint_broken_shas(children),
                         ({}, [info_path + "#ts1"], ["ts2"]))

    def test_snappy_decompress_doc_sample(self):
        # the CBSE-23458 doc's CompressedMapping and the JSON it says it holds
        compressed = base64.b64decode(
            "qgGgeyJTb3VyY2VDb2xsZWN0aW9ucyI6W3siU2NvcGVOYW1lIjoiUzEiLCIZIQ0WeGNvbDEifV0sIkluZGly"
            "ZWN0VGFyZ2V0TWFwIjp7IjBGRgAEMyJmRgAEfSwNiAFdVHNwYWNlVHlwZU1hcCI6eyIwIjowfX0=")
        self.assertEqual(json.loads(bmu.snappy_decompress(compressed)), {
            "SourceCollections": [{"ScopeName": "S1", "CollectionName": "col1"}],
            "IndirectTargetMap": {"0": [{"ScopeName": "S3", "CollectionName": "col1"}]},
            "SourceNamespaceTypeMap": {"0": 0}})

    def test_snappy_decompress_rejects_corrupt(self):
        with self.assertRaises(ValueError):
            bmu.snappy_decompress(b"\x05\x09\x00")  # copy before any output

    def test_global_info_records(self):
        self.assertIsNone(bmu.global_info_records(CKPT_CHILDREN[:3]))
        self.assertEqual(bmu.global_info_records(CKPT_CHILDREN), -1)  # not JSON
        empty = [{"path": "/ckpt/x/globalInfo", "value": _b64json({"NsMappingRecords": None})}]
        self.assertEqual(bmu.global_info_records(empty), 0)

    def test_parse_metakv_listing(self):
        stream = ('{"Path":"/ckpt/x/0","Value":"e30=","Rev":"g2"}\n'
                  '{"path":"/ckpt/x/1","value":"","rev":"g3"}')
        self.assertEqual(bmu.parse_metakv_listing(stream), [
            {"path": "/ckpt/x/0", "value": "e30=", "rev": "g2"},
            {"path": "/ckpt/x/1", "value": "", "rev": "g3"}])
        self.assertEqual(bmu.parse_metakv_listing('[{"Path":"/a","Value":"e30="}]'),
                         [{"path": "/a", "value": "e30="}])
        self.assertEqual(bmu.parse_metakv_listing(b""), [])

    def test_decoded_children_ignores_rev(self):
        with_rev = [dict(child, rev="AAA") for child in CKPT_CHILDREN]
        self.assertEqual(bmu.decoded_children(with_rev), bmu.decoded_children(CKPT_CHILDREN))
        self.assertNotEqual(bmu.decoded_children(CKPT_CHILDREN[:2]),
                            bmu.decoded_children(CKPT_CHILDREN[:1]))


class GoxdcrLogTest(unittest.TestCase):

    def test_loaded_broken_map_pairs_stop_at_next_log_line(self):
        blocks, pairs = bmu.loaded_broken_map_pairs(GOXDCR_LOG)
        self.assertEqual(blocks, 2)
        self.assertEqual(pairs, {("S1.col1", "S3.col1"), ("S1.col2", "S3.col2")})

    def test_no_load_line(self):
        self.assertEqual(bmu.loaded_broken_map_pairs("2026-10-05T13:30:01Z INFO x\n"), (0, set()))

    def test_pipeline_stop_state_counts_only_this_replication(self):
        self.assertEqual(bmu.pipeline_stop_state(CKPT_STOP_LOG, REPL_ID), {
            "started": 1, "completed": 1, "timed_out": 1, "skipped": 1, "stopped": 1})
        self.assertEqual(bmu.pipeline_stop_state("", REPL_ID), {
            "started": 0, "completed": 0, "timed_out": 0, "skipped": 0, "stopped": 0})

    def test_tail_start_normal(self):
        self.assertEqual(bmu.tail_start_line(mark=100, current_lines=150), 101)

    def test_tail_start_after_rotation(self):
        self.assertEqual(bmu.tail_start_line(mark=100, current_lines=40), 1)


if __name__ == "__main__":
    unittest.main()
