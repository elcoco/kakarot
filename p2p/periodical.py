from typing import Optional, Callable

import threading
import time
from dataclasses import dataclass, field

from p2p.store import Store
from p2p.routing import RouteTable

from core.utils import debug, info, error


"""
Periodical bucket refresh:

    1. Do for every bucket that has not had a lookup for > 1 hour:
    2. Pick random UUID from that bucket's UUID range.
    3. Perform FIND_NODE RPC on this UUID
"""

SLEEP_THREAD_WAIT_SEC = 5



@dataclass
class Task():
    name: str
    t_delta: int
    t_last: Optional[float] = None

    def is_due(self):
        if not self.t_last:
            self.t_last = time.time()

        elif time.time() - self.t_last > self.t_delta:
            self.t_last = time.time()
            return True

    def run(self):
        """ Needs to be implemented when subclassed """



class MaintenanceThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self._is_stopped = False
        self._tasks = []

    def add_task(self, task: Task):
        self._tasks.append(task)

    def stop(self):
        """ Set stop and wait till thread is joined """
        self._is_stopped = True
        self.join()

    def run(self):
        while not self._is_stopped:
            for task in self._tasks:
                if task.is_due():
                    task.run()

            time.sleep(SLEEP_THREAD_WAIT_SEC)



class TaskDeleteExpiredCache(Task):
    def __init__(self, store: Store, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._store = store

    def run(self):
        info("task_delete_expired", self.name, "running Periodical delete expired cache task")

        for item in self._store.get_items():
            if item.is_expired():
                info("task_delete_expired", self.name, f"remove expired item: {item}")
                self._store.remove_item(item)


class TaskRepublishKeys(Task):
    """
    Periodical store republish:
        - ORIGINATOR_STORE: holds k:v pairs that originate from this node
            1. To prevent information disappearing from the network when all peers with cached
               versions go offline we republish every 24h by sending FIND_NODE and STORE RPC's
               for every k:v

        - CACHE_STORE: 
            1. When a peer does a FIND_KEY call, upon receiving the value, the peer also caches
               this data to the peer closest to the closest peer to the key_uuid.
            2. never republished

        - REPUBLISH_STORE:  
            1. When the k:v originator sends a store request to the peer closest to the key_uuid,
               it also sends the same request to all other closest K-1 peers to the key_uuid it knows of.
            2. the receiving peers should republish these k:v every 1h

            3. Optimization: To reduce messages on the network, when receiving a store, update the 
               timestamp on the key-value. Any key-value that has been touched within the last hour
               is not republished. Because we assume that when we receive a STORE, the same STORE RPC
               is sent to all K-1 nodes, therefore we don't also need to send it.
    """
    def __init__(self, store: Store, publish_callback, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._store = store
        self._callback = publish_callback

    def run(self):
        info("task_republish_keys", self.name, "running Periodical republish keys task")
        for item in self._store.get_items():
            self._callback(item.value, key_uuid=item.uuid)


class TaskRefreshBuckets(Task):
    """
    Periodical bucket refresh:

        1. Do for every bucket that has not had a lookup for > 1 hour:
        2. Pick random UUID from that bucket's UUID range.
        3. Perform FIND_NODE RPC on this UUID
    """
    def __init__(self, table: RouteTable, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._table = table

    def run(self):
        info("task_refresh_buckets", self.name, "running Periodical refresh buckets task")
