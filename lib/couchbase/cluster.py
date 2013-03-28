from tasks.future import Future
from tasks.taskmanager import TaskManager
from tasks.task import *
import types


"""An API for scheduling tasks that run against Couchbase Server

This module is contains the top-level API's for scheduling and executing tasks. The
API provides a way to run task do syncronously and asynchronously.
"""

class Cluster(object):
    """An API for interacting with Couchbase clusters"""

    def __init__(self):
        self.task_manager = TaskManager("Cluster_Thread")
        self.task_manager.start()

    def async_create_default_bucket(self, server, size, replicas=1):
        """Asynchronously creates the default bucket

        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            size - The size of the bucket to be created. (int)
            replicas - The number of replicas for this bucket. (int)

        Returns:
            BucketCreateTask - A task future that is a handle to the scheduled task."""
        _task = BucketCreateTask(server, 'default', replicas, size)
        self.task_manager.schedule(_task)
        return _task

    def async_create_sasl_bucket(self, server, name, password, size, replicas):
        """Asynchronously creates a sasl bucket

        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            name - The name of the bucket to be created. (String)
            password - The password for this bucket. (String)
            replicas - The number of replicas for this bucket. (int)
            size - The size of the bucket to be created. (int)

        Returns:
            BucketCreateTask - A task future that is a handle to the scheduled task."""
        _task = BucketCreateTask(server, name, replicas, size, password=password)
        self.task_manager.schedule(_task)
        return _task

    def async_create_standard_bucket(self, server, name, port, size, replicas):
        """Asynchronously creates a standard bucket

        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            name - The name of the bucket to be created. (String)
            port - The port to create this bucket on. (String)
            replicas - The number of replicas for this bucket. (int)
            size - The size of the bucket to be created. (int)

        Returns:
            BucketCreateTask - A task future that is a handle to the scheduled task."""
        _task = BucketCreateTask(server, name, replicas, size, port)
        self.task_manager.schedule(_task)
        return _task

    def async_create_memcached_bucket(self, server, name, port, size, replicas):
        """Asynchronously creates a standard bucket

        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            name - The name of the bucket to be created. (String)
            port - The port to create this bucket on. (String)
            replicas - The number of replicas for this bucket. (int)
            size - The size of the bucket to be created. (int)

        Returns:
            BucketCreateTask - A task future that is a handle to the scheduled task."""
        _task = BucketCreateTask(server, name, replicas, size, port, bucket_type="memcached")
        self.task_manager.schedule(_task)
        return _task

    def async_bucket_delete(self, server, bucket='default'):
        """Asynchronously deletes a bucket

        Parameters:
            server - The server to delete the bucket on. (TestInputServer)
            bucket - The name of the bucket to be deleted. (String)

        Returns:
            BucketDeleteTask - A task future that is a handle to the scheduled task."""
        _task = BucketDeleteTask(server, bucket)
        self.task_manager.schedule(_task)
        return _task

    def async_init_node(self, server, disabled_consistent_view=None,
                        rebalanceIndexWaitingDisabled=None, rebalanceIndexPausingDisabled=None,
                        maxParallelIndexers=None, maxParallelReplicaIndexers=None, port=None,
                        quota_percent=None):
        """Asynchronously initializes a node

        The task scheduled will initialize a nodes username and password and will establish
        the nodes memory quota to be 2/3 of the available system memory.

        Parameters:
            server - The server to initialize. (TestInputServer)
            disabled_consistent_view - disable consistent view
            rebalanceIndexWaitingDisabled - index waiting during rebalance(Boolean)
            rebalanceIndexPausingDisabled - index pausing during rebalance(Boolean)
            maxParallelIndexers - max parallel indexers threads(Int)
            maxParallelReplicaIndexers - max parallel replica indexers threads(int)
            port - port to initialize cluster
            quota_percent - percent of memory to initialize
        Returns:
            NodeInitTask - A task future that is a handle to the scheduled task."""
        _task = NodeInitializeTask(server, disabled_consistent_view, rebalanceIndexWaitingDisabled,
                          rebalanceIndexPausingDisabled, maxParallelIndexers, maxParallelReplicaIndexers,
                          port, quota_percent)
        self.task_manager.schedule(_task)
        return _task

    def async_load_gen_docs(self, server, bucket, generator, kv_store, op_type, exp=0, flag=0, only_store_hash=True, batch_size=1, pause_secs=1, timeout_secs=5):
        if batch_size > 1:
            _task = BatchedLoadDocumentsTask(server, bucket, generator, kv_store, op_type, exp, flag, only_store_hash, batch_size, pause_secs, timeout_secs)
        else:
            _task = LoadDocumentsTask(server, bucket, generator, kv_store, op_type, exp, flag, only_store_hash)
        self.task_manager.schedule(_task)
        return _task

    def async_workload(self, server, bucket, kv_store, num_ops, create, read, update,
                       delete, exp):
        _task = WorkloadTask(server, bucket, kv_store, num_ops, create, read, update,
                             delete, exp)
        self.task_manager.schedule(_task)
        return _task

    def async_verify_data(self, server, bucket, kv_store, max_verify=None,
                          only_store_hash=True, batch_size=1, replica_to_read=None):
        if batch_size > 1:
            _task = BatchedValidateDataTask(server, bucket, kv_store, max_verify, only_store_hash, batch_size)
        else:
            _task = ValidateDataTask(server, bucket, kv_store, max_verify, only_store_hash, replica_to_read)
        self.task_manager.schedule(_task)
        return _task

    def async_verify_revid(self, src_server, dest_server, bucket, kv_store, ops_perf):
        _task = VerifyRevIdTask(src_server, dest_server, bucket, kv_store, ops_perf)
        self.task_manager.schedule(_task)
        return _task

    def async_rebalance(self, servers, to_add, to_remove):
        """Asyncronously rebalances a cluster

        Parameters:
            servers - All servers participating in the rebalance ([TestInputServers])
            to_add - All servers being added to the cluster ([TestInputServers])
            to_remove - All servers being removed from the cluster ([TestInputServers])

        Returns:
            RebalanceTask - A task future that is a handle to the scheduled task"""
        _task = RebalanceTask(servers, to_add, to_remove)
        self.task_manager.schedule(_task)
        return _task

    def async_wait_for_stats(self, servers, bucket, param, stat, comparison, value):
        """Asynchronously wait for stats

        Waits for stats to match the criteria passed by the stats variable. See
        couchbase.stats_tool.StatsCommon.build_stat_check(...) for a description of
        the stats structure and how it can be built.

        Parameters:
            servers - The servers to get stats from. Specifying multiple servers will
                cause the result from each server to be added together before
                comparing. ([TestInputServer])
            bucket - The name of the bucket (String)
            param - The stats parameter to use. (String)
            stat - The stat that we want to get the value from. (String)
            comparison - How to compare the stat result to the value specified.
            value - The value to compare to.

        Returns:
            RebalanceTask - A task future that is a handle to the scheduled task"""
        _task = StatsWaitTask(servers, bucket, param, stat, comparison, value)
        self.task_manager.schedule(_task)
        return _task

    def create_default_bucket(self, server, size, replicas=1, timeout=None):
        """Synchronously creates the default bucket

        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            size - The size of the bucket to be created. (int)
            replicas - The number of replicas for this bucket. (int)

        Returns:
            boolean - Whether or not the bucket was created."""
        _task = self.async_create_default_bucket(server, size, replicas)
        return _task.result(timeout)

    def create_sasl_bucket(self, server, name, password, size, replicas, timeout=None):
        """Synchronously creates a sasl bucket

        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            name - The name of the bucket to be created. (String)
            password - The password for this bucket. (String)
            replicas - The number of replicas for this bucket. (int)
            size - The size of the bucket to be created. (int)

        Returns:
            boolean - Whether or not the bucket was created."""
        _task = self.async_create_sasl_bucket(server, name, password, replicas, size)
        self.task_manager.schedule(_task)
        return _task.result(timeout)

    def create_standard_bucket(self, server, name, port, size, replicas, timeout=None):
        """Synchronously creates a standard bucket

        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            name - The name of the bucket to be created. (String)
            port - The port to create this bucket on. (String)
            replicas - The number of replicas for this bucket. (int)
            size - The size of the bucket to be created. (int)

        Returns:
            boolean - Whether or not the bucket was created."""
        _task = self.async_create_standard_bucket(server, name, port, size, replicas)
        return _task.result(timeout)

    def bucket_delete(self, server, bucket='default', timeout=None):
        """Synchronously deletes a bucket

        Parameters:
            server - The server to delete the bucket on. (TestInputServer)
            bucket - The name of the bucket to be deleted. (String)

        Returns:
            boolean - Whether or not the bucket was deleted."""
        _task = self.async_bucket_delete(server, bucket)
        return _task.result(timeout)

    def init_node(self, server, async_init_node=True, disabled_consistent_view=None):
        """Synchronously initializes a node

        The task scheduled will initialize a nodes username and password and will establish
        the nodes memory quota to be 2/3 of the available system memory.

        Parameters:
            server - The server to initialize. (TestInputServer)
            disabled_consistent_view - disable consistent view

        Returns:
            boolean - Whether or not the node was properly initialized."""
        _task = self.async_init_node(server, async_init_node, disabled_consistent_view)
        return _task.result()

    def rebalance(self, servers, to_add, to_remove, timeout=None):
        """Syncronously rebalances a cluster

        Parameters:
            servers - All servers participating in the rebalance ([TestInputServers])
            to_add - All servers being added to the cluster ([TestInputServers])
            to_remove - All servers being removed from the cluster ([TestInputServers])

        Returns:
            boolean - Whether or not the rebalance was successful"""
        _task = self.async_rebalance(servers, to_add, to_remove)
        return _task.result(timeout)

    def load_gen_docs(self, server, bucket, generator, kv_store, op_type, exp=0, timeout=None, flag=0, only_store_hash=True, batch_size=1):
        _task = self.async_load_gen_docs(server, bucket, generator, kv_store, op_type, exp, flag, only_store_hash=only_store_hash, batch_size=batch_size)
        return _task.result(timeout)

    def workload(self, server, bucket, kv_store, num_ops, create, read, update, delete, exp, timeout=None):
        _task = self.async_workload(server, bucket, kv_store, num_ops, create, read, update,
                                    delete, exp)
        return _task.result(timeout)

    def verify_data(self, server, bucket, kv_store, timeout=None):
        _task = self.async_verify_data(server, bucket, kv_store)
        return _task.result(timeout)

    def wait_for_stats(self, servers, bucket, param, stat, comparison, value, timeout=None):
        """Synchronously wait for stats

        Waits for stats to match the criteria passed by the stats variable. See
        couchbase.stats_tool.StatsCommon.build_stat_check(...) for a description of
        the stats structure and how it can be built.

        Parameters:
            servers - The servers to get stats from. Specifying multiple servers will
                cause the result from each server to be added together before
                comparing. ([TestInputServer])
            bucket - The name of the bucket (String)
            param - The stats parameter to use. (String)
            stat - The stat that we want to get the value from. (String)
            comparison - How to compare the stat result to the value specified.
            value - The value to compare to.

        Returns:
            boolean - Whether or not the correct stats state was seen"""
        _task = self.async_wait_for_stats(servers, bucket, param, stat, comparison, value)
        return _task.result(timeout)

    def shutdown(self, force=False):
        self.task_manager.shutdown(force)

    def async_create_view(self, server, design_doc_name, view, bucket="default", with_query=True, check_replication=False):
        """Asynchronously creates a views in a design doc

        Parameters:
            server - The server to handle create view task. (TestInputServer)
            design_doc_name - Design doc to be created or updated with view(s) being created (String)
            view - The view being created (document.View)
            bucket - The name of the bucket containing items for this view. (String) or (Bucket)
            with_query - Wait indexing to get view query results after creation

        Returns:
            ViewCreateTask - A task future that is a handle to the scheduled task."""
        _task = ViewCreateTask(server, design_doc_name, view, bucket, with_query, check_replication)
        self.task_manager.schedule(_task)
        return _task

    def create_view(self, server, design_doc_name, view, bucket="default", timeout=None, with_query=True, check_replication=False):
        """Synchronously creates a views in a design doc

        Parameters:
            server - The server to handle create view task. (TestInputServer)
            design_doc_name - Design doc to be created or updated with view(s) being created (String)
            view - The view being created (document.View)
            bucket - The name of the bucket containing items for this view. (String) or (Bucket)
            with_query - Wait indexing to get view query results after creation

        Returns:
            string - revision number of design doc."""
        _task = self.async_create_view(server, design_doc_name, view, bucket, with_query, check_replication)
        return _task.result(timeout)

    def async_delete_view(self, server, design_doc_name, view, bucket="default"):
        """Asynchronously deletes a views in a design doc

        Parameters:
            server - The server to handle delete view task. (TestInputServer)
            design_doc_name - Design doc to be deleted or updated with view(s) being deleted (String)
            view - The view being deleted (document.View)
            bucket - The name of the bucket containing items for this view. (String) or (Bucket)

        Returns:
            ViewDeleteTask - A task future that is a handle to the scheduled task."""
        _task = ViewDeleteTask(server, design_doc_name, view, bucket)
        self.task_manager.schedule(_task)
        return _task

    def delete_view(self, server, design_doc_name, view, bucket="default", timeout=None):
        """Synchronously deletes a views in a design doc

        Parameters:
            server - The server to handle delete view task. (TestInputServer)
            design_doc_name - Design doc to be deleted or updated with view(s) being deleted (String)
            view - The view being deleted (document.View)
            bucket - The name of the bucket containing items for this view. (String) or (Bucket)

        Returns:
            boolean - Whether or not delete view was successful."""
        _task = self.async_delete_view(server, design_doc_name, view, bucket)
        return _task.result(timeout)


    def async_query_view(self, server, design_doc_name, view_name, query,
                         expected_rows=None, bucket="default", retry_time=2):
        """Asynchronously query a views in a design doc

        Parameters:
            server - The server to handle query view task. (TestInputServer)
            design_doc_name - Design doc with view(s) being queried(String)
            view_name - The view being queried (String)
            expected_rows - The number of rows expected to be returned from the query (int)
            bucket - The name of the bucket containing items for this view. (String)
            retry_time - The time in seconds to wait before retrying failed queries (int)

        Returns:
            ViewQueryTask - A task future that is a handle to the scheduled task."""
        _task = ViewQueryTask(server, design_doc_name, view_name, query, expected_rows, bucket, retry_time)
        self.task_manager.schedule(_task)
        return _task

    def query_view(self, server, design_doc_name, view_name, query,
                   expected_rows=None, bucket="default", retry_time=2, timeout=None):
        """Synchronously query a views in a design doc

        Parameters:
            server - The server to handle query view task. (TestInputServer)
            design_doc_name - Design doc with view(s) being queried(String)
            view_name - The view being queried (String)
            expected_rows - The number of rows expected to be returned from the query (int)
            bucket - The name of the bucket containing items for this view. (String)
            retry_time - The time in seconds to wait before retrying failed queries (int)

        Returns:
            ViewQueryTask - A task future that is a handle to the scheduled task."""
        _task = self.async_query_view(server, design_doc_name, view_name, query, expected_rows, bucket, retry_time)
        return _task.result(timeout)


    def modify_fragmentation_config(self, server, config, bucket="default", timeout=None):
        """Synchronously modify fragmentation configuration spec

        Parameters:
            server - The server to handle fragmentation config task. (TestInputServer)
            config - New compaction configuration (dict - see task)
            bucket - The name of the bucket fragementation config applies to. (String)

        Returns:
            boolean - True if config values accepted."""

        _task = ModifyFragmentationConfigTask(server, config, bucket)
        self.task_manager.schedule(_task)
        return _task.result(timeout)

    def async_monitor_active_task(self, servers,
                                  type_task,
                                  target_value,
                                  wait_progress=100,
                                  num_iteration=100,
                                  wait_task=True):
        """Asynchronously monitor active task.

           When active task reached wait_progress this method  will return.

        Parameters:
            servers - list of servers or The server to handle fragmentation config task. (TestInputServer)
            type_task - task type('indexer' , 'bucket_compaction', 'view_compaction' ) (String)
            target_value - target value (for example "_design/ddoc" for indexing, bucket "default"
                for bucket_compaction or "_design/dev_view" for view_compaction) (String)
            wait_progress - expected progress (int)
            num_iteration - failed test if progress is not changed during num iterations(int)
            wait_task - expect to find task in the first attempt(bool)

        Returns:
            list of MonitorActiveTask - A task future that is a handle to the scheduled task."""
        _tasks = []
        if type(servers) != types.ListType:
            servers = [servers,]
        for server in servers:
            _task = MonitorActiveTask(server, type_task, target_value, wait_progress, num_iteration, wait_task)
            self.task_manager.schedule(_task)
            _tasks.append(_task)
        return _tasks

    def async_monitor_view_fragmentation(self, server,
                                         design_doc_name,
                                         fragmentation_value,
                                         bucket="default"):
        """Asynchronously monitor view fragmentation.

           When <fragmentation_value> is reached on the
           index file for <design_doc_name> this method
           will return.

        Parameters:
            server - The server to handle fragmentation config task. (TestInputServer)
            design_doc_name - design doc with views represented in index file. (String)
            fragmentation_value - target amount of fragmentation within index file to detect. (String)
            bucket - The name of the bucket design_doc belongs to. (String)

        Returns:
            MonitorViewFragmentationTask - A task future that is a handle to the scheduled task."""

        _task = MonitorViewFragmentationTask(server, design_doc_name,
                                             fragmentation_value, bucket)
        self.task_manager.schedule(_task)
        return _task

    def async_generate_expected_view_results(self, doc_generators, view, query):
        """Asynchronously generate expected view query results

        Parameters:
            doc_generators - Generators used for loading docs (DocumentGenerator[])
            view - The view with map function (View)
            query - Query params to filter docs from the generator. (dict)

        Returns:
            GenerateExpectedViewResultsTask - A task future that is a handle to the scheduled task."""

        _task = GenerateExpectedViewResultsTask(doc_generators, view, query)
        self.task_manager.schedule(_task)
        return _task

    def generate_expected_view_query_results(self, doc_generators, view, query, timeout=None):
        """Synchronously generate expected view query results

        Parameters:
            doc_generators - Generators used for loading docs (DocumentGenerator[])
            view - The view with map function (View)
            query - Query params to filter docs from the generator. (dict)

        Returns:
            list - A list of rows expected to be returned for given query"""

        _task = self.async_generate_expected_view_results(doc_generators, view, query)
        return _task.result(timeout)


    def async_view_query_verification(self, server, design_doc_name, view_name, query, expected_rows, num_verified_docs=20, bucket="default", query_timeout=20):
        """Asynchronously query a views in a design doc and does full verification of results

        Parameters:
            server - The server to handle query verification task. (TestInputServer)
            design_doc_name - Design doc with view(s) being queried(String)
            view_name - The view being queried (String)
            query - Query params being used with the query. (dict)
            expected_rows - The number of rows expected to be returned from the query (int)
            num_verified_docs - The number of docs to verify that require memcached gets (int)
            bucket - The name of the bucket containing items for this view. (String)
            query_timeout - The time to allow a query with stale=false to run. (int)
            retry_time - The time in seconds to wait before retrying failed queries (int)

        Returns:
            ViewQueryVerificationTask - A task future that is a handle to the scheduled task."""
        _task = ViewQueryVerificationTask(server, design_doc_name, view_name, query, expected_rows, num_verified_docs, bucket, query_timeout)
        self.task_manager.schedule(_task)
        return _task

    def view_query_verification(self, server, design_doc_name, view_name, query, expected_rows, num_verified_docs=20, bucket="default", query_timeout=20, timeout=None):
        """Synchronously query a views in a design doc and does full verification of results

        Parameters:
            server - The server to handle query verification task. (TestInputServer)
            design_doc_name - Design doc with view(s) being queried(String)
            view_name - The view being queried (String)
            query - Query params being used with the query. (dict)
            expected_rows - The number of rows expected to be returned from the query (int)
            num_verified_docs - The number of docs to verify that require memcached gets (int)
            bucket - The name of the bucket containing items for this view. (String)
            query_timeout - The time to allow a query with stale=false to run. (int)
            retry_time - The time in seconds to wait before retrying failed queries (int)

        Returns:
            dict - An object with keys: passed = True or False
                                        errors = reasons why verification failed """
        _task = self.async_view_query_verification(server, design_doc_name, view_name, query, expected_rows, num_verified_docs, bucket, query_timeout)
        return _task.result(timeout)


    def monitor_view_fragmentation(self, server,
                                   design_doc_name,
                                   fragmentation_value,
                                   bucket="default",
                                   timeout=None):
        """Synchronously monitor view fragmentation.

           When <fragmentation_value> is reached on the
           index file for <design_doc_name> this method
           will return.

        Parameters:
            server - The server to handle fragmentation config task. (TestInputServer)
            design_doc_name - design doc with views represented in index file. (String)
            fragmentation_value - target amount of fragmentation within index file to detect. (String)
            bucket - The name of the bucket design_doc belongs to. (String)

        Returns:
            boolean - True if <fragmentation_value> reached"""

        _task = self.async_monitor_view_fragmentation(server, design_doc_name,
                                                      fragmentation_value,
                                                      bucket)
        self.task_manager.schedule(_task)
        return _task.result(timeout)

    def async_compact_view(self, server, design_doc_name, bucket="default", with_rebalance=False):
        """Asynchronously run view compaction.

        Compacts index file represented by views within the specified <design_doc_name>

        Parameters:
            server - The server to handle fragmentation config task. (TestInputServer)
            design_doc_name - design doc with views represented in index file. (String)
            bucket - The name of the bucket design_doc belongs to. (String)
            with_rebalance - there are two cases that process this parameter:
                "Error occured reading set_view _info" will be ignored if True
                (This applies to rebalance in case),
                and with concurrent updates(for instance, with rebalance)
                it's possible that compaction value has not changed significantly

        Returns:
            ViewCompactionTask - A task future that is a handle to the scheduled task."""


        _task = ViewCompactionTask(server, design_doc_name, bucket, with_rebalance)
        self.task_manager.schedule(_task)
        return _task

    def compact_view(self, server, design_doc_name, bucket="default", timeout=None, with_rebalance=False):
        """Synchronously run view compaction.

        Compacts index file represented by views within the specified <design_doc_name>

        Parameters:
            server - The server to handle fragmentation config task. (TestInputServer)
            design_doc_name - design doc with views represented in index file. (String)
            bucket - The name of the bucket design_doc belongs to. (String)
            with_rebalance - "Error occured reading set_view _info" will be ignored if True
                and with concurrent updates(for instance, with rebalance)
                it's possible that compaction value has not changed significantly

        Returns:
            boolean - True file size reduced after compaction, False if successful but no work done """

        _task = self.async_compact_view(server, design_doc_name, bucket, with_rebalance)
        return _task.result(timeout)

    def async_failover(self, servers, to_failover):
        """Asyncronously fails over nodes

        Parameters:
            servers - All servers participating in the failover ([TestInputServers])
            to_failover - All servers being failed over ([TestInputServers])

        Returns:
            FailoverTask - A task future that is a handle to the scheduled task"""
        _task = FailoverTask(servers, to_failover)
        self.task_manager.schedule(_task)
        return _task

    def failover(self, servers, to_failover, timeout=None):
        """Syncronously fails over nodes

        Parameters:
            servers - All servers participating in the failover ([TestInputServers])
            to_failover - All servers being failed over ([TestInputServers])

        Returns:
            boolean - Whether or not the failover was successful"""
        _task = self.async_failover(servers, to_failover)
        return _task.result(timeout)

    def async_bucket_flush(self, server, bucket='default'):
        """Asynchronously flushes a bucket

        Parameters:
            server - The server to flush the bucket on. (TestInputServer)
            bucket - The name of the bucket to be flushed. (String)

        Returns:
            BucketFlushTask - A task future that is a handle to the scheduled task."""
        _task = BucketFlushTask(server, bucket)
        self.task_manager.schedule(_task)
        return _task

    def bucket_flush(self, server, bucket='default', timeout=None):
        """Synchronously flushes a bucket

        Parameters:
            server - The server to flush the bucket on. (TestInputServer)
            bucket - The name of the bucket to be flushed. (String)

        Returns:
            boolean - Whether or not the bucket was flushed."""
        _task = self.async_bucket_flush(server, bucket)
        return _task.result(timeout)
