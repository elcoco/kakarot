import threading
from typing import Optional

from core.utils import debug, info, error

from p2p.store import Store
from p2p.peer import Peer


class RouteTable():
    def __init__(self, store: Store, keyspace: int, bucket_size: int, origin_uuid) -> None:
        self._keyspace = keyspace
        self._bucket_size = bucket_size
        self._origin_uuid = origin_uuid
        self._lock = threading.Lock()

        # Reference to k:v store
        self._store = store

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
            header = f"  BUCKET {i_bucket:2}:"
            indent = len(header) * " "

            for i_peer, peer in enumerate(bucket):
                if i_peer == 0:
                    out.append(f"{header} {i_peer:2}: {str(peer)} {peer.get_distance(self._origin_uuid)}")
                else:
                    out.append(f"{indent} {i_peer:2}: {str(peer)} {peer.get_distance(self._origin_uuid)}")
        return "\n".join(out)

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
                assert n < len(self._k_buckets), f"n={n}, k_buckets={len(self._k_buckets)}"

                peers_sorted = sorted(self._k_buckets[n], key=lambda x: x.get_distance(target))
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

    def _bucket_has_peer(self, bucket: int, peer: Peer):
        """ Look in bucket for a peer that matches all key:value pairs of <peer> """
        for p in self._k_buckets[bucket]:
            if p == peer:
                return p

    def _bucket_is_full(self, bucket: int):
        return len(self._k_buckets[bucket]) >= self._bucket_size

    def find_next_neighbour(self, peer: Peer, origin_uuid: int):
        """ Find the next peer from <peer> """
        bucket = peer.find_significant_common_bits(origin_uuid, self._keyspace)

        # Search forwards for next known peer
        for b in range(bucket, self._keyspace, 1):
            for p in sorted(self._k_buckets[b], key=lambda x: x.get_distance(origin_uuid)):
                if p.uuid > peer.uuid:
                    return p
            else:
                continue

    def insert_peer(self, peer: Peer, origin: Peer):
        """ We have a least seen eviction policy and we prefer old peers because they tend to
            be reliable than new peers. Apparently research concluded that peers that have been
            online for an hour are likely to be online for another hour. """
        # Don't insert origin in table
        if peer.uuid == origin.uuid:
            info("route_table", "insert_peer", "not inserting origin")
            return

        bucket = peer.find_significant_common_bits(origin.uuid, self._keyspace)

        with self._lock:
            if (p := self._bucket_has_peer(bucket, peer)):
                self._k_buckets[bucket].remove(p)
                self._k_buckets[bucket].append(peer)
                #info("route_table", "insert_peer", "moved peer to end")

            elif self._bucket_is_full(bucket):
                # Now we need to ping the least seen node (head) and remove it if it doesn't respond
                oldest_peer = self._k_buckets[bucket][0]

                if oldest_peer.ping(origin):
                    # Old peer is alive, discard new peer because we prefer our old peers (more trustworthy)
                    self._k_buckets[bucket].remove(oldest_peer)
                    self._k_buckets[bucket].append(oldest_peer)
                    #info("route_table", "insert_peer", "bucket full, discard peer: {peer}")

                else:
                    # peer doesn't respond so we replace it
                    self._k_buckets[bucket].remove(oldest_peer)
                    self._k_buckets[bucket].append(peer)
                    #info("route_table", "insert_peer", "old peer unreachable, adding new peer: {peer}")

            else:
                self._k_buckets[bucket].append(peer)


                #info("route_table", "insert_peer", f"adding new peer: {peer}")

    def remove_peer(self, peer: Peer, origin_uuid: int):
        bucket = peer.find_significant_common_bits(origin_uuid, self._keyspace)

        with self._lock:
            if (p := self._bucket_has_peer(bucket, peer)):
                self._k_buckets[bucket].remove(p)
            else:
                error("route_table", "remove", f"Failed to remove peer from bucket '{bucket}', peer not found: {peer}")
