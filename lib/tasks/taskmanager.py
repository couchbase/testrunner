import time
import Queue
import sys

from threading import Thread
from tasks.task import Task

class TaskManager(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.readyq = Queue.Queue()
        self.sleepq = Queue.Queue()
        self.running = True

    def schedule(self, task, sleep_time=0):
        if not isinstance(task, Task):
            raise TypeError("Tried to schedule somthing that's not a task")
        if sleep_time <= 0:
            self.readyq.put(task)
        else:
            wakeup_time = time.time() + sleep_time
            self.sleepq.put({'task': task, 'time': wakeup_time})

    def run(self):
        while not (self.running == False and self.readyq.empty()):
            if self.readyq.empty():
                time.sleep(1)
            else:
                task = self.readyq.get()
                task.step(self)
            for i in range(self.sleepq.qsize()):
                task = self.sleepq.get()
                if time.time() >= task['time']:
                    self.readyq.put(task['task'])
                else:
                    self.sleepq.put(task)

    def stop(self):
        self.running = False
