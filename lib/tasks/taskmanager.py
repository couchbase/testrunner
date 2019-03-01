import time
import queue as Queue

from threading import Thread
from tasks.task import Task

class TaskManager(Thread):
    def __init__(self, thread_name=None):
        Thread.__init__(self)
        self.readyq = Queue.Queue()
        self.sleepq = Queue.Queue()
        self.running = True
        if thread_name is not None:
            self.name = thread_name

    def schedule(self, task, sleep_time=0):
        if not isinstance(task, Task):
            raise TypeError("Tried to schedule somthing that's not a task")
        if sleep_time <= 0:
            self.readyq.put(task)
        else:
            wakeup_time = time.time() + sleep_time
            self.sleepq.put({'task': task, 'time': wakeup_time})

    def run(self):
        while (self.running == True or self.readyq.empty() != True or self.sleepq.empty() != True):
            self.temp_Q = self.readyq.empty()
            # print("\n\n\n\n","--"*20 , " temp_Q: ", self.temp_Q,"--"*20,"\n\n\n\n")
            if self.readyq.empty():
                time.sleep(1)
            else:
                task = self.readyq.get()
                task.step(self)
            for i in range(self.sleepq.qsize()):
                s_task = self.sleepq.get()
                if time.time() >= s_task['time']:
                    self.readyq.put(s_task['task'])
                else:
                    self.sleepq.put(s_task)

    def shutdown(self, force=False):
        self.running = False
        if force:
            while not self.sleepq.empty():
                task = self.sleepq.get()['task']
                task.cancel()
                self.readyq.put(task)
            while not self.readyq.empty():
                try:
                    task = self.readyq.get()
                    task.cancel()
                except Exception as ex:
                    raise ex