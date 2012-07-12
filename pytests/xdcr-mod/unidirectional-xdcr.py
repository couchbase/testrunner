from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from xdcrbasetests import XDCRReplicationBaseTest

import time

class unidirectionalReplication(XDCRReplicationBaseTest):
    def setUp(self):
        super(unidirectionalReplication, self).setUp()

    def tearDown(self):
        super(unidirectionalReplication, self).tearDown()

    """For unidirectional updates, we are setting up replication from source to destination nodes only, so verification
    of data is done only at destination nodes."""
    # TODO fix exit condition on mismatch error, to check for a range instead of exiting on 1st mismatch
    def unidirectional_load_with_ops(self):
        gen_create = BlobGenerator('loadOne', 'loadOne', self._value_size, end=self._num_items)
        gen_delete = BlobGenerator('loadOne', 'loadOne-', self._value_size, start=self._num_items / 2,
            end=self._num_items)
        gen_update = BlobGenerator('loadOne', 'loadOne-', self._value_size, end=self._num_items / 2 - 1)

        ord_keys = self._clusters_keys_olst
        ord_keys_len = len(ord_keys)

        src_nodes = self._clusters_dic[0]
        src_master = src_nodes[0]
        expires = self._expires

        #Setting up doc-ops at source nodes
        if (self._doc_ops is not None):
            # allows multiple of them but one by one
            if "create" in self._doc_ops:
                self._load_all_buckets(src_master, gen_create, "create", expires)
            if "update" in self._doc_ops:
                self._load_all_buckets(src_master, gen_update, "update", expires)
            if "delete" in self._doc_ops:
                self._load_all_buckets(src_master, gen_delete, "delete", expires)
            self._wait_for_stats_all_buckets(src_nodes)

        # Checking replication at destination clusters
        dest_key_index = 1
        for key in ord_keys[1:]:
            if dest_key_index == ord_keys_len:
                break
            dest_key = ord_keys[dest_key_index]
            dest_nodes = self._clusters_dic[dest_key]
            dest_master = dest_nodes[0]

            if self._num_items >= 100000:
                self._timeout = 300
            time.sleep(self._timeout)

            self._log.info("Verify xdcr replication stats at destination cluster : {0}".format(dest_master))
            self._wait_for_stats_all_buckets(dest_nodes)

            # Flush out all the deleted/expiry items
            self._expiry_pager(src_master)
            self._expiry_pager(dest_master)


            self._verify_all_buckets(dest_master)
            self._verify_stats_all_buckets(dest_nodes)

            if "delete" in self._doc_ops:
                self._verify_revIds_deletes(src_master, dest_master)
            dest_key_index += 1


    def unidirectional_load_with_incremental_ops(self):
        gen_create = BlobGenerator('loadOne', 'loadOne', self._value_size, end=self._num_items)
        gen_delete = BlobGenerator('loadOne', 'loadOne-', self._value_size, start=self._num_items / 2,
            end=self._num_items)
        gen_update = BlobGenerator('loadOne', 'loadOne-', self._value_size, end=self._num_items / 2 - 1)

        ord_keys = self._clusters_keys_olst
        ord_keys_len = len(ord_keys)

        src_nodes = self._clusters_dic[0]
        src_master = src_nodes[0]
        expires = self._expires

        self._load_all_buckets(src_master, gen_create, "create", expires)
        tasks = []
        #Setting up doc-ops at source nodes
        if (self._doc_ops is not None):
            # allows multiple of them but one by one
            if "update" in self._doc_ops:
                    tasks.extend(self._async_load_all_buckets(src_master, gen_update, "update", expires))
            if "create" in self._doc_ops:
                    tasks.extend(self._async_load_all_buckets(src_master, gen_create, "create", expires))
            if "delete" in self._doc_ops:
                    tasks.extend(self._async_load_all_buckets(src_master, gen_delete, "delete", expires))
        time.sleep(5)
        for task in tasks:
            task.result()
        self._wait_for_stats_all_buckets(src_nodes)

        # Checking replication at destination clusters
        dest_key_index = 1
        for key in ord_keys[1:]:
            if dest_key_index == ord_keys_len:
                break
            dest_key = ord_keys[dest_key_index]
            dest_nodes = self._clusters_dic[dest_key]
            dest_master = dest_nodes[0]

            # temp fix
            if self._num_items >= 100000:
                self._timeout = 300

            self._log.info("Verify xdcr replication stats at destination cluster : {0}".format(dest_master))
            self._wait_for_stats_all_buckets(dest_nodes)

            # Flush out expired/delete items.
            self._expiry_pager(src_master)
            self._expiry_pager(dest_master)

            self._verify_all_buckets(dest_master)
            self._verify_stats_all_buckets(dest_nodes)

            if "delete" in self._doc_ops:
                self._verify_revIds_deletes(src_master, dest_master)
            dest_key_index += 1

