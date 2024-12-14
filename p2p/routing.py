import threading
from typing import Optional, Callable
import time
import random

from core.utils import debug, info, error

from p2p.store import Store
from p2p.peer import Peer


class TableTraverser():
    """ Traverse table in two directions and return next/prev peer (sorted by distance). """
    def __init__(self, table: "RouteTable", origin: Peer, keyspace: int, start_peer: Optional[Peer]=None) -> None:
        self._table = table
        self._keyspace = keyspace

        # This is the peer that distances are measured from
        self._origin = origin

        if start_peer:
            self._last_bucket_i = origin.find_significant_common_bits(start_peer.uuid, keyspace)
            self._last_peer_i = self._table.buckets[self._last_bucket_i].get_sorted(origin.uuid).index(start_peer)
        else:
            self._last_bucket_i = 0
            self._last_peer_i = -1

    def __iter__(self):
        return self

    def __next__(self):
        if not (p := self.next()):
            raise StopIteration
        return p

    def get_next_non_empty_bucket(self):
        for bucket in self._table.buckets[self._last_bucket_i+1:]:
            if not bucket.is_empty():
                return bucket._index

    def get_prev_non_empty_bucket(self):
        for bucket in reversed(self._table.buckets[:self._last_bucket_i]):
            if not bucket.is_empty():
                return bucket._index


    def next(self, default=None):
        bucket = self._table.buckets[self._last_bucket_i]
        peers = bucket.get_sorted(self._origin.uuid)
        peer = None

        while not peer:
            if self._last_peer_i+1 < bucket.get_size():
                self._last_peer_i += 1
                peer = peers[self._last_peer_i]
            else:
                if not (next_bucket_i := self.get_next_non_empty_bucket()):
                    return default   # StopIteration

                bucket = self._table.buckets[next_bucket_i]
                peers = bucket.get_sorted(self._origin.uuid)
                self._last_peer_i = -1
                self._last_bucket_i = next_bucket_i

        return peer

    def prev(self, default=None):
        bucket = self._table.buckets[self._last_bucket_i]
        peers = bucket.get_sorted(self._origin.uuid)
        peer = None

        while not peer:
            if self._last_peer_i-1 >= 0:
                self._last_peer_i -= 1
                peer = peers[self._last_peer_i]
            else:
                if (prev_bucket_i := self.get_prev_non_empty_bucket()) == None:
                    return default   # StopIteration

                bucket = self._table.buckets[prev_bucket_i]
                peers = bucket.get_sorted(self._origin.uuid)
                self._last_peer_i = bucket.get_size() 
                self._last_bucket_i = prev_bucket_i

        return peer

class Bucket():
    def __init__(self, index: int, keyspace: int, size: int, ping_callback: Callable) -> None:

        # Index of this bucket
        self._index = index

        # Keep track of last update time so we know when to refresh this bucket.
        self._last_update: float = time.time()

        self._size = size
        self._keyspace = keyspace
        self._ping_callback = ping_callback

        self._peers: list[Peer] = []
        self._lock = threading.Lock()

    def __repr__(self):
        out = []
        header = f"  BUCKET {self._index:2}:"
        indent = len(header) * " "

        for i, peer in enumerate(self._peers):
            if i == 0:
                out.append(f"{header} {i:2}: {str(peer)}")
            else:
                out.append(f"{indent} {i:2}: {str(peer)}")
        return "\n".join(out)

    def needs_refresh(self, interval: int):
        return time.time() - self._last_update > interval

    def get_sorted(self, origin_uuid):
        return sorted(self._peers, key=lambda x: x.get_distance(origin_uuid))

    def has_peer(self, peer: Peer):
        """ Look in bucket for a peer that matches all key:value pairs of <peer> """
        return next((p for p in self._peers if p.uuid == peer.uuid), None)

    def is_empty(self):
        return not len(self._peers)

    def get_size(self):
        return len(self._peers)

    def get_random_peer(self):
        if len(self._peers) > 0:
            return random.choice(self._peers)

    def is_full(self):
        return len(self._peers) >= self._size

    def insert_peer(self, peer: Peer, origin_uuid: int):
        """ We have a least seen eviction policy and we prefer old peers because they tend to
            be reliable than new peers. Apparently research concluded that peers that have been
            online for an hour are likely to be online for another hour. """
        # Don't insert origin in table
        if peer.uuid == origin_uuid:
            info("route_table", "insert_peer", "not inserting origin")
            return

        # Set last refresh time so we can later check the freshness of the bucket
        self._last_update = time.time()

        with self._lock:
            if (p := self.has_peer(peer)):
                self._peers.remove(p)
                self._peers.append(peer)

            elif self.is_full():
                # Now we need to ping the least seen node (head) and remove it if it doesn't respond
                oldest_peer = self._peers[0]

                if self._ping_callback(oldest_peer.uuid, oldest_peer.ip, oldest_peer.port):
                    # Old peer is alive, discard new peer because we prefer our old peers (more trustworthy)
                    self._peers.remove(oldest_peer)
                    self._peers.append(oldest_peer)

                else:
                    # peer doesn't respond so we replace it
                    self._peers.remove(oldest_peer)
                    self._peers.append(peer)
            else:
                self._peers.append(peer)

    def remove_peer(self, peer: Peer):
        with self._lock:
            if (p := self.has_peer(peer)):
                info("route_table", "remove", f"removed peer '{peer}' from bucket '{self._index}'")
                self._peers.remove(p)


class RouteTable():
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

    def __init__(self, keyspace: int, bucket_size: int, ping_callback: Callable) -> None:
        self._keyspace = keyspace
        self._bucket_size = bucket_size
        self.buckets = [ Bucket(i, keyspace, bucket_size, ping_callback) for i in range(self._keyspace) ]
        self._lock = threading.Lock()

    def __repr__(self):
        out = ""
        for bucket in self.buckets:
            if bucket._peers:
                out += str(bucket) + "\n"
        return out

    def has_peer(self, peer: Peer, origin: Peer):
        bucket = origin.find_significant_common_bits(peer.uuid, self._keyspace)
        return self.buckets[bucket].has_peer(peer)

    def get_closest_nodes(self, origin: Peer, target: int, amount: Optional[int]=None):
        """ Get <amount> closest nodes from target """
        if amount == None:
            amount = self._bucket_size

        out = []

        bucket = origin.find_significant_common_bits(target, self._keyspace)
        n = bucket

        with self._lock:
            # Go over all buckets and sort peers so that we get a list of sorted peers by closeness
            while len(out) != amount:
                assert n < len(self.buckets), f"n={n}, buckets={len(self.buckets)}"

                peers_sorted = self.buckets[n].get_sorted(origin.uuid)
                out += peers_sorted[:amount-len(out)]

                if n >= self._keyspace-1:
                    if bucket == 0:
                        break
                    else:
                        n = bucket -1
                elif n < bucket:
                    if n == 0:
                        break
                    else:
                        n -= 1
                else:
                    n += 1
            return out

    def find_next_neighbour(self, peer: Peer, origin_uuid: int):
        """ Find the next peer from <peer> """
        bucket = peer.find_significant_common_bits(origin_uuid, self._keyspace)

        # Search forwards for next known peer
        for b in range(bucket, self._keyspace, 1):

            print(f"{b} unsorted:", self.buckets[b])
            print(f"{b} sorted:  ", self.buckets[b].get_sorted(origin_uuid))

            for p in self.buckets[b].get_sorted(origin_uuid):
                if p.uuid > peer.uuid:
                    return p
            else:
                continue

    def insert_peer(self, peer: Peer, origin_uuid: int):
        """ We have a least seen eviction policy and we prefer old peers because they tend to
            be reliable than new peers. Apparently research concluded that peers that have been
            online for an hour are likely to be online for another hour. """
        # Don't insert origin in table
        if peer.uuid == origin_uuid:
            info("route_table", "insert_peer", "not inserting origin")
            return

        bucket = peer.find_significant_common_bits(origin_uuid, self._keyspace)
        self.buckets[bucket].insert_peer(peer, origin_uuid)

    def remove_peer(self, peer: Peer, origin_uuid: int):
        bucket = peer.find_significant_common_bits(origin_uuid, self._keyspace)
        self.buckets[bucket].remove_peer(peer)
