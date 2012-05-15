from tasks.future import Future
from tasks.taskmanager import TaskManager
from tasks.task import *


"""An API for scheduling tasks that run against Couchbase Server

This module is contains the top-level API's for scheduling and executing tasks. The
API provides a way to run task do syncronously and asynchronously.
"""

class Cluster(object):
    """An API for interacting with Couchbase clusters"""

    def __init__(self):
        self.task_manager = TaskManager()
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

    def async_init_node(self, server):
        """Asynchronously initializes a node

        The task scheduled will initialize a nodes username and password and will establish
        the nodes memory quota to be 2/3 of the available system memory.

        Parameters:
            server - The server to initialize. (TestInputServer)

        Returns:
            NodeInitTask - A task future that is a handle to the scheduled task."""
        _task = NodeInitializeTask(server)
        self.task_manager.schedule(_task)
        return _task

    def async_load_gen_docs(self, server, bucket, generator, kv_store, op_type, exp = 0):
        _task = LoadDocumentsTask(server, bucket, generator, kv_store, op_type, exp)
        self.task_manager.schedule(_task)
        return _task

    def async_workload(self, server, bucket, kv_store, num_ops, create, read, update,
                       delete, exp):
        _task = WorkloadTask(server, bucket, kv_store, num_ops, create, read, update,
                             delete, exp)
        self.task_manager.schedule(_task)
        return _task

    def async_verify_data(self, server, bucket, kv_store):
        _task = ValidateDataTask(server, bucket, kv_store)
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

    def create_default_bucket(self, server, size, replicas=1):
        """Synchronously creates the default bucket

        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            size - The size of the bucket to be created. (int)
            replicas - The number of replicas for this bucket. (int)

        Returns:
            boolean - Whether or not the bucket was created."""
        _task = self.async_create_default_bucket(server, size, replicas)
        return _task.result()

    def create_sasl_bucket(self, server, name, password, size, replicas):
        """Synchronously creates a sasl bucket

        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            name - The name of the bucket to be created. (String)
            password - The password for this bucket. (String)
            replicas - The number of replicas for this bucket. (int)
            size - The size of the bucket to be created. (int)

        Returns:
            boolean - Whether or not the bucket was created."""
        _task = async_create_sasl_bucket(server, name, password, replicas, size)
        self.task_manager.schedule(_task)
        return _task.result()

    def create_standard_bucket(self, server, name, port, size, replicas):
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
        return _task.result()

    def bucket_delete(self, server, bucket='default'):
        """Synchronously deletes a bucket

        Parameters:
            server - The server to delete the bucket on. (TestInputServer)
            bucket - The name of the bucket to be deleted. (String)

        Returns:
            boolean - Whether or not the bucket was deleted."""
        _task = self.async_bucket_delete(server, bucket)
        return _task.result()

    def init_node(self, server):
        """Synchronously initializes a node

        The task scheduled will initialize a nodes username and password and will establish
        the nodes memory quota to be 2/3 of the available system memory.

        Parameters:
            server - The server to initialize. (TestInputServer)

        Returns:
            boolean - Whether or not the node was properly initialized."""
        _task = self.async_init_node(server)
        return _task.result()

    def rebalance(self, servers, to_add, to_remove):
        """Syncronously rebalances a cluster

        Parameters:
            servers - All servers participating in the rebalance ([TestInputServers])
            to_add - All servers being added to the cluster ([TestInputServers])
            to_remove - All servers being removed from the cluster ([TestInputServers])

        Returns:
            boolean - Whether or not the rebalance was successful"""
        _task = self.async_rebalance(servers, to_add, to_remove)
        return _task.result()

    def load_gen_docs(self, server, bucket, generator, kv_store, op_type, exp = 0):
        _task = self.async_load_gen_docs(server, bucket, generator, kv_store, op_type, exp)
        return _task.result()

    def workload(self, server, bucket, kv_store, num_ops, create, read, update, delete, exp):
        _task = self.async_workload(server, bucket, kv_store, num_ops, create, read, update,
                                    delete, exp)
        return _task.result()

    def verify_data(self, server, bucket, kv_store):
        _task = self.async_verify_data(server, bucket, kv_store)
        return _task.result()

    def wait_for_stats(self, servers, bucket, param, stat, comparison, value):
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
        return _task.result()

    def shutdown(self, force=False):
        self.task_manager.shutdown(force)
