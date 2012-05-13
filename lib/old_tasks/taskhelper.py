
from old_tasks.task import NodeInitializeTask, BucketCreateTask

class TaskHelper():
    @staticmethod
    def add_node_init_task(tm, server):
        node_init_task = NodeInitializeTask(server)
        tm.schedule(node_init_task)
        return node_init_task

    @staticmethod
    def add_bucket_create_task(tm, server, bucket='default', replicas=1, port=11210, size=0,
                           password=None):
        bucket_create_task = BucketCreateTask(server, bucket, replicas, port, size, password)
        tm.schedule(bucket_create_task)
        return bucket_create_task
