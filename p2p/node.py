from typing import Optional
import random
import math
import json
import requests
from dataclasses import dataclass
import time

from p2p.api import Api, PingMsg, StoreMsg, FindNodeMsg, FindKeyMsg, ErrorMsg, ResponseMsg
from core.utils import debug, info, error


@dataclass
class Peer():
    """ Represents an external node """

    # An n-bit identifier that represents the peer's location in the binary tree.
    # An XOR(origin, peer) is done to calculate the distance.
    uuid: int
    ip: str
    port: int

    def __repr__(self):
        return f"{self.uuid:04X}@{self.ip}:{self.port}"

    def get_distance(self, origin):
        return self.uuid ^ origin

    def find_significant_common_bits(self, origin: int, keyspace: int) -> int:
        """ Compare uuid's and find common most significant bits.
            This is used to determin the bucket we should store this peer """

        assert(origin != self.uuid)

        # get the non matching bits between peer and origin node
        distance = self.get_distance(origin)
        count = 0

        for i in reversed(range(keyspace)):
            if distance & (0x01 << i):
                break
            else:
                count += 1
        return count

class Lock():
    _is_locked = False

    def __enter__(self):
        info("lock", "enter", "waiting for lock")
        while Lock._is_locked:
            ...
        Lock._is_locked = True
        info("lock", "enter", "aquired lock")

    def __exit__(self, exc_type, exc_val, exc_tb):
        Lock._is_locked = False
        info("lock", "enter", "released lock")


class RouteTable():
    def __init__(self, keyspace: int, bucket_size: int) -> None:
        self._keyspace = keyspace
        self._bucket_size = bucket_size

        r"""
        source: https://www.youtube.com/watch?v=NxhZ_c8YX8E
        The network has the shape of a binary tree. The vertical levels are equal to the amount of
        bits in the keyspace.
        eg: 4Bit keyspace has 4 levels

                        left 0, right 1

           LEVEL 1        /\
           LEVEL 2    /\      /\
           LEVEL 3  /\  /\  /\  /\
           LEVEL 4 /\/\/\/\/\/\/\/\

        The buckets where we store peers based on most significant common bit.
        This means that peers are stored based on distance from origin, NOT based
        on just the uuid.

        8 BIT table
        peer_uuid   = 0b01000100
        origin_uuid = 0b01011100
                      ---------- XOR
        common      = 0b00011000
                           ^
        This example has 3 msb's.
        This means that we have 3 matching steps down the binary tree.
        We put this peer in bucket 3.

        The bucket size (how many peers can we store in a bucket before it's full)
        is called K in the spec.


        The further away we are from the origin node, the more space a bucket covers.
        That means that the closer we are to origin, the more expertise we have of our surroundings

        """
        self._k_buckets: list[list] = [ [] for _ in range(self._keyspace) ]

    def __repr__(self):
        out = []
        for i_bucket, bucket in enumerate(self._k_buckets):
            if not bucket:
                continue
            out.append(f"BUCKET: {i_bucket}:")
            for i_peer, peer in enumerate(bucket):
                out.append(f"  {i_peer:2}: {str(peer)}")
        return "\n".join(out)

    def insert_peer(self, peer: Peer, origin: int):
        if (n := peer.find_significant_common_bits(origin, self._keyspace)) == None:
            error("route_table", "init", f"no common bits")
            return

        # TODO: We need to PING the first peer in the bucket, if it doesn't respond we
        #       will delete it and append the new peer to the head of the bucket.
        #       If it does respond we move the peer to the end of the bucket and ignore the new peer.
        #       This way we make sure we frequently check all our peers so we don't get stuck
        #       with dead peers and become isolated.
        with Lock():
            if len(self._k_buckets[n]) >= self._bucket_size:
                debug("route_table", "insert", f"not inserting, bucket full")
                return

            self._k_buckets[n].append(peer)


class Node():
    def __init__(self, ip: str, port: int, key_size: int, bucket_size: int, uuid: Optional[int]=None) -> None:

        # Size of network uuid/keys in bits
        self._key_size = key_size

        # Amount of peers stored per bucket
        self._bucket_size = bucket_size

        self._ip = ip
        self._port = port

        if uuid == None:
            self._uuid = self._get_uuid()
        else:
            if math.log2(uuid) > self._key_size:
                raise ValueError("node", "init", f"uuid is not in keyspace")

            self._uuid = uuid

        self._table = RouteTable(key_size, bucket_size)

        self._is_stopped = False

    def __repr__(self):
        return f"NODE: {self._ip}:{self._port} => {self._uuid}"

    def _get_uuid(self):
        return random.randrange(0, (2**self._key_size)-1)

    def stop(self):
        self._is_stopped = True

    def ping_callback(self, msg: PingMsg) -> ResponseMsg|ErrorMsg:
        """ Respond to incoming PING message """
        # TODO: Check if we have to save the peer in the routing table
        return ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)

    def store_callback(self, msg: StoreMsg) -> ResponseMsg|ErrorMsg:
        """ Respond to incoming FIND_NODE message """

    def find_node_callback(self, msg: FindNodeMsg) -> ResponseMsg|ErrorMsg:
        """ Respond to incoming FIND_NODE message """

    def find_key_callback(self, msg: FindKeyMsg) -> ResponseMsg|ErrorMsg:
        """ Respond to incoming FIND_NODE message """

    def run(self):
        for _ in range(2000):
            n = random.randrange(0, 2**self._key_size-1)
            self._table.insert_peer(Peer(n, "127.0.0.1", n), self._uuid)

        print(self._table)

        callbacks = { "ping":      self.ping_callback,
                      "store":     self.store_callback,
                      "find_node": self.find_node_callback,
                      "find_key":  self.find_key_callback }

        with Lock():
            try:
                api = Api("", self._port, callbacks)
                api.listen()
            except KeyboardInterrupt:
                api.stop()


        info("node", "run", f"done")
