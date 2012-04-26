from tasks.future import Future
from tasks.task import *


"""An API for scheduling tasks that run against Couchbase Server

This module is contains the top-level API's for scheduling and executing tasks. The
API provides a way to run task do syncronously and asynchronously.
"""

class TaskScheduler():
    """An API for scheduling and executing asyncronous and synchronous tasks"""
    @staticmethod
    def async_bucket_create(tm, server, bucket='default', replicas=1, port=11210, size=0,
                           password=None):
        """Asynchronously creates a bucket

        Parameters:
            tm - The task manager that this task should be scheduled to. (TaskManager)
            server - The server to create the bucket on. (TestInputServer)
            bucket - The name of the bucket to be created. (String)
            replicas - The number of replicas for this bucket. (int)
            port - The port to create the bucket on. Only used for non-sasl buckets. (int)
            password - The password for this bucket. If specified a sasl bucket will be
            create. (String)

        Returns:
            BucketCreateTask - A task future that is a handle to the scheduled task."""
        _task = BucketCreateTask(server, bucket, replicas, port, size, password)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_bucket_delete(tm, server, bucket='default'):
        """Asynchronously deletes a bucket

        Parameters:
            tm - The task manager that this task should be scheduled to. (TaskManager)
            server - The server to delete the bucket on. (TestInputServer)
            bucket - The name of the bucket to be deleted. (String)

        Returns:
            BucketDeleteTask - A task future that is a handle to the scheduled task."""
        _task = BucketDeleteTask(server, bucket)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_init_node(tm, server):
        """Asynchronously initializes a node

        The task scheduled will initialize a nodes username and password and will establish
        the nodes memory quota to be 2/3 of the available system memory.

        Parameters:
            server - The server to initialize. (TestInputServer)

        Returns:
            NodeInitTask - A task future that is a handle to the scheduled task."""
        _task = NodeInitializeTask(server)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_load_gen_docs(tm, rest, generator, bucket, expiration, loop=False):
        _task = LoadDocGeneratorTask(rest, generator, bucket, expiration, loop)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_rebalance(tm, servers, to_add, to_remove):
        """Asyncronously rebalances a cluster

        Parameters:
            servers - All servers participating in the rebalance ([TestInputServers])
            to_add - All servers being added to the cluster ([TestInputServers])
            to_remove - All servers being removed from the cluster ([TestInputServers])

        Returns:
            RebalanceTask - A task future that is a handle to the scheduled task"""
        _task = RebalanceTask(servers, to_add, to_remove)
        tm.schedule(_task)
        return _task

    @staticmethod
    def async_wait_for_stats(tm, stats, bucket):
        """Asynchronously wait for stats

        Waits for stats to match the criteria passed by the stats variable. See
        couchbase.stats_tool.StatsCommon.build_stat_check(...) for a description of
        the stats structure and how it can be built.

        Parameters:
            tm - The task manager that this task should be scheduled to. (TaskManager)
            stats - The stats structure that contains the state to look for. (See above)
            bucket - The name of the bucket (String)

        Returns:
            RebalanceTask - A task future that is a handle to the scheduled task"""
        _task = StatsWaitTask(stats, bucket)
        tm.schedule(_task)
        return _task

    @staticmethod
    def bucket_create(tm, server, bucket='default', replicas=1, port=11210, size=0, password=None):
        """Synchronously creates a bucket

        Parameters:
            tm - The task manager that this task should be scheduled to. (TaskManager)
            server - The server to create the bucket on. (TestInputServer)
            bucket - The name of the bucket to be created. (String)
            replicas - The number of replicas for this bucket. (int)
            port - The port to create the bucket on. Only used for non-sasl buckets. (int)
            password - The password for this bucket. If specified a sasl bucket will be
            create. (String)

        Returns:
            boolean - Whether or not the bucket was created."""
        _task = TaskScheduler.async_bucket_create(tm, server, bucket, replicas, port, size,
                                                  password)
        return _task.result()

    @staticmethod
    def bucket_delete(tm, server, bucket='default'):
        """Synchronously deletes a bucket

        Parameters:
            tm - The task manager that this task should be scheduled to. (TaskManager)
            server - The server to delete the bucket on. (TestInputServer)
            bucket - The name of the bucket to be deleted. (String)

        Returns:
            boolean - Whether or not the bucket was deleted."""
        _task = TaskScheduler.async_bucket_delete(tm, server, bucket)
        return _task.result()

    @staticmethod
    def init_node(tm, server):
        """Synchronously initializes a node

        The task scheduled will initialize a nodes username and password and will establish
        the nodes memory quota to be 2/3 of the available system memory.

        Parameters:
            server - The server to initialize. (TestInputServer)

        Returns:
            boolean - Whether or not the node was properly initialized."""
        _task = TaskScheduler.async_init_node(tm, server)
        return _task.result()

    @staticmethod
    def rebalance(tm, servers, to_add, to_remove):
        """Syncronously rebalances a cluster

        Parameters:
            servers - All servers participating in the rebalance ([TestInputServers])
            to_add - All servers being added to the cluster ([TestInputServers])
            to_remove - All servers being removed from the cluster ([TestInputServers])

        Returns:
            boolean - Whether or not the rebalance was successful"""
        _task = TaskScheduler.async_rebalance(tm, servers, to_add, to_remove)
        return _task.result()

    @staticmethod
    def load_gen_docs(tm, rest, generator, bucket, expiration, loop=False):
        _task = TaskScheduler.async_load_gen_docs(tm, rest, generator, bucket, expiration, loop)
        return _task.result()

    @staticmethod
    def wait_for_stats(tm, stats, bucket):
        """Synchronously wait for stats

        Waits for stats to match the criteria passed by the stats variable. See
        couchbase.stats_tool.StatsCommon.build_stat_check(...) for a description of
        the stats structure and how it can be built.

        Parameters:
            tm - The task manager that this task should be scheduled to. (TaskManager)
            stats - The stats structure that contains the state to look for. (See above)
            bucket - The name of the bucket (String)

        Returns:
            boolean - Whether or not the correct stats state was seen"""
        _task = TaskScheduler.async_wait_for_stats(tm, stats, bucket)
        return _task.result()
