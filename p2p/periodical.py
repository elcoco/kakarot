from typing import Optional, Callable

import threading
import time
from dataclasses import dataclass, field

"""
Periodical bucket refresh:

    1. Do for every bucket that has not had a lookup for > 1 hour:
    2. Pick random UUID from that bucket's UUID range.
    3. Perform FIND_NODE RPC on this UUID
"""

@dataclass
class Task():
    callback: Callable
    t_delta: int
    args: list = field(default_factory=list)
    t_last: Optional[float] = None

    def is_due(self):
        if not self.t_last:
            self.t_last = time.time()
            return True

        if time.time() - self.t_last > self.t_delta:
            self.t_last = time.time()
            return True

    def execute(self):
        self.callback(self.args)


class MaintenanceThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self._stopped = False
        self._tasks = []

    def add_task(self, task: Task):
        self._tasks.append(task)

    def stop(self):
        """ Set stop and wait till thread is joined """
        self._stopped = True
        self.join()

    def run(self):
        while not self._stopped:
            for task in self._tasks:
                if task.is_due():
                    task.execute()
