#!/usr/bin/env python3

import socket
import threading
import random
import time
import logging
import sys

sys.path.append('../')

from p2p.node import Node
from p2p.peer import Peer

from tests import state
from core.utils import debug, info, error
from core.config import Config

logger = logging.getLogger("kakarot")


class PeerThread(threading.Thread):
    def __init__(self, uuid: int, port: int, keyspace: int, bucket_size: int, alpha: int):
        threading.Thread.__init__(self)
        self._stopped = False
        self._keyspace = keyspace

        self._bucket_size = bucket_size
        self._uuid = uuid
        self._port = port
        self._alpha = alpha

        print(self._uuid, self._port)

    def is_stopped(self):
        return self._stopped

    def __repr__(self):
        return f"THREAD: {self}"

    def _get_uuid(self, max):
        """ To make testing slightly easier we use port numbers equal to the UUID's.
            This means that we should start at UUID=1024 so we don't have to run as root """
        return random.randrange(1024, 2**max)

    def _get_int_ip(self):
        return socket.gethostbyname(socket.gethostname())

    def stop(self):
        self._node.stop()

    def run(self):
        # we don't want the threads to be in sync
        #time.sleep(10)

        t_start = time.time()

        self._node = Node("0.0.0.0", self._port, self._keyspace, self._bucket_size, self._alpha, uuid=self._uuid)
        info("peer_thread", str(self._uuid), f"starting {self._node}")
        self._node.run()
        self._stopped = True
        t_delta = round(time.time()-t_start,1)


class ServerThreadManager():
    """ Manages a pool of threads that get new data from servers """
    def __init__(self, state: Config):
        self._bucket_size = state.bucket_size
        self._amount = state.peers
        self._keyspace = state.keyspace
        self._alpha = state.alpha
        self._pool: list[PeerThread] = []

    def stop(self):
        for t in self._pool:
            t.stop()
            t.join()
        t_elapsed = time.time() - self._t_start
        info("thread_manager", "done", f"all nodes ran in {round(t_elapsed, 1)} seconds")

    def _get_uuid(self, max):
        """ To make testing slightly easier we use port numbers equal to the UUID's.
            This means that we should start at UUID=1024 so we don't have to run as root """
        return random.randrange(1024, 2**max)

    def _pick_random(self, lst: list[Peer], amount: int):
        return [random.choice(lst) for _ in range(amount)]

    def run(self):
        debug("thread_manager", "init", f"Starting {self._amount} nodes")
        self._t_start = time.time()

        # create a list of all uuid's so we can create random bootstrap lists for every node that we start
        node_list = []

        for _ in range(self._amount):
            uuid = self._get_uuid(self._keyspace)
            if self._keyspace > 16:
                port = self._get_uuid(16)
            else:
                port = uuid

            peer = Peer(uuid, "0.0.0.0", port)
            node_list.append(peer)

            # NOTE: we use uuid as port number to make testing with large amount of nodes easier to manage
            t = PeerThread(peer.uuid, peer.port, self._keyspace, self._bucket_size, self._alpha)
            self._pool.append(t)
            t.start()

            # prevent in sync nodes
            #time.sleep(1)

        # Wait till all threads have started

        for t in self._pool:
            bootstrap_lst = self._pick_random(node_list, 1)
            t._node.join_network(bootstrap_lst)

        for t in self._pool:
            bootstrap_lst = self._pick_random(node_list, 1)
            t._node.join_network(bootstrap_lst)

        for i in range(10):
            self._pool[0]._node.call_store(f"key{i}", f"value{i}")

        time.sleep(5)
        for t in self._pool:
            print(t._node)
            print(t._node._table)
            print()

        while True:
            time.sleep(0.1)


if __name__ == "__main__":
    tm = ServerThreadManager(state)
    try:
        tm.run()
    except KeyboardInterrupt:
        tm.stop()
