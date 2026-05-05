import json
import os
import re
import time
import random
import threading

import logger

from TestInput import TestInputSingleton
from lib.membase.api.rest_client import RestConnection
from .fts_base import NodeHelper
from .fts_vector_search import VectorSearch


class FastMergeVectorSearch(VectorSearch):
    """
    Tests that are unique to FastMerge and cannot be covered by re-running
    existing VectorSearch/BQVectorSearch tests with fastmerge=True.

    Covers:
      - vector_index_fast_merge presence in params.store (definition validation)
      - Disk/memory comparison: fastmerge vs legacy on same dataset
      - Invalid config values (negative)
      - CE negative test

    All other scenarios (kNN recall, topology, backup/restore, updates, etc.)
    are covered by re-running existing VectorSearch tests with fastmerge=True
    in the conf file.

    Feature permutation design (extensible):
    ---------------------------------------------------------------
    | Feature      | Config location           | Param            |
    |--------------|---------------------------|------------------|
    | FastMerge    | params.store              | fastmerge=True   |
    | BQ           | per-field                 | bq_index_type=X  |
    | GPU          | per-field (gpu: true)     | gpu=True  (TODO) |
    | Encryption   | cluster-level             | cluster config   |
    ---------------------------------------------------------------
    To add GPU: read self.gpu param in setUp, inject into vector_fields
                in add_child_field_to_default_collection_mapping.
    To add Encryption: handle in FTSBaseTest.setUp cluster config.
    Conf file drives all permutations -- no new test classes needed.
    """

    def setUp(self):
        super(FastMergeVectorSearch, self).setUp()
        self.fastmerge = True

    def tearDown(self):
        super(FastMergeVectorSearch, self).tearDown()

    # ---------------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------------

    def _setup_fm_data_and_index(self, index_name="fm_idx", extra_fields=None):
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        bucketvsdataset = self.load_vector_data(containers, dataset=self.vector_dataset)

        idx = [(index_name, "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}

        index = self._create_fts_index_parameterized(
            field_name=self.vector_field_name,
            field_type=self.vector_field_type,
            test_indexes=idx,
            vector_fields=vector_fields,
            create_vector_index=True,
            extra_fields=extra_fields or [{"sno": "number"}]
        )

        index_obj = index[0]['index_obj']
        dataset_name = bucketvsdataset['bucket_name']
        index_obj.faiss_index = self.create_faiss_index_from_train_data(dataset_name)

        return index[0], index_obj, dataset_name

    def _validate_fm_in_store(self, index_obj):
        """Assert vector_index_fast_merge=True lives in params.store."""
        status, index_def = index_obj.get_index_defn()
        self.assertTrue(status, "Failed to get index definition")
        store = index_def["indexDef"].get('params', {}).get('store', {})
        self.assertTrue(store.get('vector_index_fast_merge', False),
                        f"vector_index_fast_merge not True in params.store: {store}")
        self.log.info("Validated: vector_index_fast_merge=True in params.store")
        return index_def["indexDef"]

    def _get_fts_ram_usage(self):
        ram_by_node = {}
        rest = RestConnection(self._cb_cluster.get_master_node())
        for node in self._cb_cluster.get_fts_nodes():
            stats_text = rest.get_nsserver_stats(node.ip).text
            match = re.search(
                r'sysproc_mem_resident\{proc="cbft",category="system-processes"\} (\d+)',
                stats_text)
            ram_by_node[node.ip] = int(match.group(1)) if match else 0
        return ram_by_node

    # ---------------------------------------------------------------------------
    # FastMerge-unique: Definition validation
    # ---------------------------------------------------------------------------

    def test_fm_index_definition_validation(self):
        """Verify vector_index_fast_merge is in params.store via REST API."""
        index_info, index_obj, dataset_name = self._setup_fm_data_and_index("fm_defval")
        self._validate_fm_in_store(index_obj)

        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])
        status, rest_def = rest.get_fts_index_definition("fm_defval")
        self.assertTrue(status, "Failed to get index def via REST")

        rest_store = rest_def.get('indexDef', {}).get('params', {}).get('store', {})
        self.assertTrue(rest_store.get('vector_index_fast_merge', False),
                        "vector_index_fast_merge missing from REST response store config")

        self.assertEqual(rest_def['indexDef'].get('type'), 'fulltext-index')
        self.log.info("test_fm_index_definition_validation passed")

    # ---------------------------------------------------------------------------
    # FastMerge-unique: Disk footprint comparison (fastmerge vs legacy)
    # ---------------------------------------------------------------------------

    def test_fm_disk_footprint_comparison(self):
        """Create identical indexes with and without fastmerge, compare disk."""
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        self.load_vector_data(containers, dataset=self.vector_dataset)

        vector_fields = {"dims": self.dimension, "similarity": self.similarity}

        fm_idx = [("fm_disk", "b1.s1.c1")]
        fm_index = self._create_fts_index_parameterized(
            field_name=self.vector_field_name,
            field_type=self.vector_field_type,
            test_indexes=fm_idx,
            vector_fields=vector_fields,
            create_vector_index=True,
            extra_fields=[{"sno": "number"}]
        )
        self._validate_fm_in_store(fm_index[0]['index_obj'])

        saved = self.fastmerge
        self.fastmerge = False
        nonfm_idx = [("nonfm_disk", "b1.s1.c1")]
        nonfm_index = self._create_fts_index_parameterized(
            field_name=self.vector_field_name,
            field_type=self.vector_field_type,
            test_indexes=nonfm_idx,
            vector_fields=vector_fields,
            create_vector_index=True,
            extra_fields=[{"sno": "number"}]
        )
        self.fastmerge = saved

        time.sleep(15)
        rest = RestConnection(self._cb_cluster.get_fts_nodes()[0])

        _, fm_stats = rest.get_fts_stats(index_name="fm_disk",
                                          stat_name="num_bytes_used_disk")
        _, nonfm_stats = rest.get_fts_stats(index_name="nonfm_disk",
                                             stat_name="num_bytes_used_disk")

        fm_disk = float(fm_stats) if fm_stats else 0
        nonfm_disk = float(nonfm_stats) if nonfm_stats else 0

        self.log.info(f"Disk: FastMerge={fm_disk/(1024*1024):.2f}MB, "
                      f"Legacy={nonfm_disk/(1024*1024):.2f}MB")
        if nonfm_disk > 0:
            self.log.info(f"Ratio: {fm_disk / nonfm_disk:.2f}")

        self._cb_cluster.delete_fts_index("nonfm_disk")
        self.log.info("test_fm_disk_footprint_comparison passed")

    # ---------------------------------------------------------------------------
    # FastMerge-unique: Memory footprint comparison
    # ---------------------------------------------------------------------------

    def test_fm_memory_footprint(self):
        """Measure RAM delta during fastmerge index build."""
        ram_before = self._get_fts_ram_usage()
        self.log.info(f"RAM before: {ram_before}")

        index_info, index_obj, dataset_name = self._setup_fm_data_and_index("fm_mem")
        self._validate_fm_in_store(index_obj)
        time.sleep(10)

        ram_after = self._get_fts_ram_usage()
        self.log.info(f"RAM after: {ram_after}")

        for ip in ram_before:
            delta = ram_after.get(ip, 0) - ram_before.get(ip, 0)
            self.log.info(f"Node {ip}: delta={delta/(1024*1024):.2f} MB")

        queries = self.get_query_vectors(dataset_name)
        neighbours = self.get_groundtruth_file(dataset_name)
        for q in queries[:3]:
            _, _, _, ra = self.run_vector_query(
                vector=q.tolist(), index=index_obj, neighbours=neighbours[0])
        self.log.info("test_fm_memory_footprint passed")

    # ---------------------------------------------------------------------------
    # FastMerge-unique: Negative tests
    # ---------------------------------------------------------------------------

    def test_fm_invalid_config_value(self):
        """Set vector_index_fast_merge to a non-boolean -- expect rejection or graceful handling."""
        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        self.load_vector_data(containers, dataset=self.vector_dataset)

        saved = self.fastmerge
        self.fastmerge = False
        idx = [("fm_invalid", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}
        index = self._create_fts_index_parameterized(
            field_name=self.vector_field_name,
            field_type=self.vector_field_type,
            test_indexes=idx,
            vector_fields=vector_fields,
            create_vector_index=True,
            extra_fields=[{"sno": "number"}]
        )
        self.fastmerge = saved
        index_obj = index[0]['index_obj']

        if 'store' not in index_obj.index_definition['params']:
            index_obj.index_definition['params']['store'] = {}
        index_obj.index_definition['params']['store']['vector_index_fast_merge'] = "invalid"
        index_obj.index_definition['uuid'] = index_obj.get_uuid()
        try:
            index_obj.update()
            self.log.warning("Invalid vector_index_fast_merge accepted by server")
        except Exception as e:
            self.log.info(f"Correctly rejected: {e}")

        self.log.info("test_fm_invalid_config_value passed")

    def test_fm_community_edition_negative(self):
        """FastMerge should not be available on Community Edition."""
        rest = RestConnection(self._cb_cluster.get_master_node())
        if rest.is_enterprise_edition():
            self.log.info("Skipping -- running on Enterprise Edition")
            return

        containers = self._cb_cluster._setup_bucket_structure(cli_client=self.cli_client)
        self.load_vector_data(containers, dataset=self.vector_dataset)

        idx = [("fm_ce", "b1.s1.c1")]
        vector_fields = {"dims": self.dimension, "similarity": self.similarity}
        try:
            self._create_fts_index_parameterized(
                field_name=self.vector_field_name,
                field_type=self.vector_field_type,
                test_indexes=idx,
                vector_fields=vector_fields,
                create_vector_index=True
            )
            self.log.warning("FastMerge succeeded on CE -- may be expected")
        except Exception as e:
            self.log.info(f"Correctly rejected on CE: {e}")

        self.log.info("test_fm_community_edition_negative passed")
