from couchbase.documentgenerator import BlobGenerator, DocumentGenerator
from xdcrbasetests import XDCRReplicationBaseTest

import time

class bidirectionalReplication(XDCRReplicationBaseTest):
    def setUp(self):
        super(bidirectionalReplication, self).setUp()

    def tearDown(self):
        super(bidirectionalReplication, self).tearDown()

    """Bidirectional replication between two clusters(currently), create-updates-deletes on DISJOINT sets on same bucket."""
    # TODO fix exit condition on mismatch error, to check for a range instead of exiting on 1st mismatch
    def bidirectional_load_with_ops(self):
        gen_create = BlobGenerator('loadOne', 'loadOne', self._value_size, end=self._num_items)
        gen_delete = BlobGenerator('loadOne', 'loadOne-', self._value_size, start=self._num_items / 2,
            end=self._num_items)
        gen_update = BlobGenerator('loadOne', 'loadOne-', self._value_size, end=self._num_items / 2 - 1)

        gen_create2 = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self._num_items)
        gen_delete2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, start=self._num_items / 2,
            end=self._num_items)
        gen_update2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, end=self._num_items / 2 - 1)

        ord_keys = self._clusters_keys_olst
        ord_keys_len = len(ord_keys)

        src_nodes = self._clusters_dic[0]
        src_master = src_nodes[0]
        expires = self._expires

        # Current fix only for 2 clusters
        dest_nodes = self._clusters_dic[1]
        dest_master = dest_nodes[0]

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

        # Setting up doc_ops_dest at destination nodes
        if (self._doc_ops_dest is not None):
        # allows multiple of them but one by one
            print"dest doc ops {0}".format(self._doc_ops_dest)
            if "create" in self._doc_ops_dest:
                self._load_all_buckets(dest_master, gen_create2, "create", expires)
            if "update" in self._doc_ops_dest:
                self._load_all_buckets(dest_master, gen_update2, "update", expires)
            if "delete" in self._doc_ops_dest:
                self._load_all_buckets(dest_master, gen_delete2, "delete", expires)
            self._wait_for_stats_all_buckets(dest_nodes)

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

            self._log.info(
                "Verify xdcr replication stats at cluster1 : {0} and cluster2 {1}".format(src_master, dest_master))
            self._wait_for_stats_all_buckets(dest_nodes)
            self._wait_for_stats_all_buckets(src_nodes)

            # Flush out all the deletes/expired items
            self._expiry_pager(src_master)
            self._expiry_pager(dest_master)

            # CHecking stats only on source-1 , sice that has the last update
            self._verify_all_buckets(src_master)
            self._verify_stats_all_buckets(src_nodes)

            # Remove this when running on jenkins
            self._verify_all_buckets(dest_master)
            self._verify_stats_all_buckets(dest_nodes)

            if "delete" in self._doc_ops or "delete" in self._doc_ops_dest:
                print"Delete rev id checking here"
                self._verify_revIds_deletes(src_master, dest_master)
            dest_key_index += 1

    """Bidirectional replication between two clusters(currently), create-updates-deletes on DISJOINT sets on same bucket.
    Here running incremental load on both cluster1 and cluster2 as specified by the user/conf file"""

    def bidirectional_load_with_incremental_ops(self):
        gen_create = BlobGenerator('loadOne', 'loadOne', self._value_size, end=self._num_items)
        gen_delete = BlobGenerator('loadOne', 'loadOne-', self._value_size, start=self._num_items / 2,
            end=self._num_items)
        gen_update = BlobGenerator('loadOne', 'loadOne-', self._value_size, end=self._num_items / 2 - 1)

        gen_create2 = BlobGenerator('loadTwo', 'loadTwo', self._value_size, end=self._num_items)
        gen_delete2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, start=self._num_items / 2,
            end=self._num_items)
        gen_update2 = BlobGenerator('loadTwo', 'loadTwo-', self._value_size, end=self._num_items / 2 - 1)

        ord_keys = self._clusters_keys_olst
        ord_keys_len = len(ord_keys)

        src_nodes = self._clusters_dic[0]
        src_master = src_nodes[0]

        # Current fix only for 2 clusters
        dest_nodes = self._clusters_dic[1]
        dest_master = dest_nodes[0]

        expires = self._expires

        if "create" in self._doc_ops:
            self._load_all_buckets(src_master, gen_create, "create", expires)
        if "create" in self._doc_ops_dest:
            self._load_all_buckets(dest_master, gen_create2, "create", expires)

        tasks = []
        print"dest doc ops {0}".format(self._doc_ops_dest)
        #Setting up doc-ops at source nodes
        if (self._doc_ops is not None or self._doc_ops_dest is not None):
            # allows multiple of them but one by one on either of the clusters
            if "update" in self._doc_ops:
                print "update 1"
                if not tasks:
                    tasks = self._async_load_all_buckets(src_master, gen_update, "update", expires)
                else:
                    tasks.extend(self._async_load_all_buckets(src_master, gen_update, "update", expires))
            if "update" in self._doc_ops_dest:
                print "update 2"
                if not tasks:
                    tasks = self._async_load_all_buckets(dest_master, gen_update2, "update", expires)
                else:
                    tasks.extend(self._async_load_all_buckets(dest_master, gen_update2, "update", expires))
            if "delete" in self._doc_ops:
                print "delete 1"
                if not tasks:
                    tasks = self._async_load_all_buckets(src_master, gen_delete, "delete", expires)
                else:
                    tasks.extend(self._async_load_all_buckets(src_master, gen_delete, "delete", expires))
            if "delete" in self._doc_ops_dest:
                print "delete 2"
                if not tasks:
                    tasks = self._async_load_all_buckets(dest_master, gen_delete2, "delete", expires)
                else:
                    tasks.extend(self._async_load_all_buckets(dest_master, gen_delete2, "delete", expires))
            time.sleep(5)
            for task in tasks:
                task.result()

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

            self._log.info(
                "Verify xdcr replication stats at cluster1: {0} and cluster2{1}".format(src_master, dest_master))
            self._wait_for_stats_all_buckets(dest_nodes)
            self._wait_for_stats_all_buckets(src_nodes)

            # Flush out all the deletes/expired items
            if self._expires > 0 or "deletes" in self._doc_ops or "deletes" in self._doc_ops_dest:
                self._expiry_pager(src_master)
                self._expiry_pager(dest_master)

            # CHecking stats only on source-1 , since that has the last update
            print"Waiting for replication to catch-up!"
            self._verify_stats_all_buckets(src_nodes)
            self._verify_all_buckets(src_master)

            self._verify_stats_all_buckets(dest_nodes)
            self._verify_all_buckets(dest_master)

            if "delete" in self._doc_ops or "delete" in self._doc_ops_dest:
                print"Delete rev id checking here"
                self._verify_revIds_deletes(src_master, dest_master)
            dest_key_index += 1

            # ADD CONFLICT RESOLUTION TESTS Here ..
            #   TODO    """Bidirectional replication between two clusters(currently), create-updates-deletes on JOINT
            # sets(10 percent) on same bucket.Loading(create-update-delete) data as specified by the user/conf file"""
            #   TODO    """Bidirectional replication between two clusters(currently), create-updates-deletes on JOINT
            # sets(10 percent) on same bucket.Here running incremental load on both cluster1 and cluster2 as specified
            # by the user/conf file"""