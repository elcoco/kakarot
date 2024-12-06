from typing import Optional
import random
import math
import json
import requests
from dataclasses import dataclass
import time
import socket

from p2p.api import Api
from p2p.api_parsers import BencDecodeError
from p2p.message import PingMsg, StoreMsg, FindNodeMsg, FindKeyMsg, ResponseMsg, ErrorMsg
from p2p.message import MsgError
from core.utils import debug, info, error

MSG_LENGTH = 1024


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

    def _send_recv_msg(self, msg_req, timeout=5):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

            t_start = time.time()

            try:
                s.connect((self.ip, self.port))
            except ConnectionRefusedError as e:
                error("peer", "send_recv", str(e))
                return

            s.settimeout(timeout)

            msg_bin = str(msg_req.to_bencoding()).encode()
            bytes_sent = 0

            while bytes_sent < len(msg_bin):
                try:
                    n = s.send(msg_bin)
                except BrokenPipeError as e:
                    error("peer", "send_recv", str(e))
                    return

                if n == 0:
                    error("peer", "send_recv", f"connection broken")
                    return
                bytes_sent += n

            data = b""

            try:
                while (chunk := s.recv(MSG_LENGTH)):
                    data += chunk
            except TimeoutError:
                error("peer", "send_recv", "Connection timedout")
                return
            except ConnectionResetError as e:
                error("peer", "send_recv", str(e))
                return


            try:
                msg_res = ResponseMsg()
                msg_res.from_bencoding(data.decode())
                msg_res.response_time = time.time() - t_start
            except MsgError as e:
                error("peer", "send_recv", str(e))
                return
            except BencDecodeError as e:
                error("peer", "send_recv", str(e))
                return

            if msg_res.transaction_id != msg_req.transaction_id:
                error("peer", "send_recv", f"communication error, transaction_id error: {msg_req.transaction_id} != {msg_res.transaction_id}")
                return

        return msg_res

    def ping(self, origin: "Peer"):
        """ Ping peer and return respond time """
        info("peer", "ping", "sending ping")
        msg_req = PingMsg(uuid=origin.uuid, ip=origin.ip, port=origin.port)
        print(msg_req)
        if (msg_res := self._send_recv_msg(msg_req)):
            info("peer", "send_recv", f"response time: {msg_res.response_time}")
            print(msg_res)
            return msg_res.response_time


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

    def get_closest_nodes(self, origin: Peer, target: Peer, amount: Optional[int]=None):
        """ get <amount> closest nodes from target """
        if amount == None:
            amount = self._bucket_size

        if (n := origin.find_significant_common_bits(target.uuid, self._keyspace)) == None:
            error("route_table", "init", f"no common bits")
            return

        # don't think this is correct
        print(bin(target.uuid))
        for p in self._k_buckets[n]:

            distance = target.get_distance(p.uuid)
            print(bin(distance))

        print("look in bucket:", n)

    def _bucket_has_peer(self, bucket: int, peer: Peer):
        """ Look in bucket for a peer that matches all key:value pairs of <peer> """
        for p in self._k_buckets[bucket]:
            if p == peer:
                return p

    def _bucket_is_full(self, bucket: int):
        return len(self._k_buckets[bucket]) >= self._bucket_size

    def insert_peer(self, peer: Peer, origin: Peer):
        """ We have a least seen eviction policy and we prefer old peers because they tend to
            be reliable than new peers. Apparently research concluded that peers that have been
            online for an hour are likely to be online for another hour. """
        with Lock():
            bucket = peer.find_significant_common_bits(origin.uuid, self._keyspace)

            if (p := self._bucket_has_peer(bucket, peer)):
                self._k_buckets[bucket].remove(p)
                self._k_buckets[bucket].append(peer)
                info("route_table", "insert_peer", "moved peer to end")

            elif self._bucket_is_full(bucket):
                # Now we need to ping the least seen node (head) and remove it if it doesn't respond
                oldest_peer = self._k_buckets[bucket][0]

                if oldest_peer.ping(origin):
                    # Old peer is alive, discard new peer because we prefer our old peers (more trustworthy)
                    self._k_buckets[bucket].remove(oldest_peer)
                    self._k_buckets[bucket].append(oldest_peer)
                    info("route_table", "insert_peer", "bucket full, discard peer")
                else:
                    # peer doesn't respond so we replace it
                    self._k_buckets[bucket].remove(oldest_peer)
                    self._k_buckets[bucket].append(peer)
                    info("route_table", "insert_peer", "old peer unreachable, adding new peer")

            else:
                self._k_buckets[bucket].append(peer)
                info("route_table", "insert_peer", "adding new peer")


class Node():
    def __init__(self, ip: str, port: int, key_size: int, bucket_size: int, alpha: int, uuid: Optional[int]=None) -> None:

        # Size of network uuid/keys in bits
        self._key_size = key_size

        # Amount of peers stored per bucket
        self._bucket_size = bucket_size

        self._ip = ip
        self._port = port

        if uuid == None:
            self._uuid = self._create_uuid()
        else:
            if math.log2(uuid) > self._key_size:
                raise ValueError("node", "init", f"uuid is not in keyspace")

            self._uuid = uuid

        # Concurency parameter when looking up nodes.
        # Lookup max <alpha> nodes at a time
        self._alpha = alpha

        self._table = RouteTable(key_size, bucket_size)

        self._is_stopped = False

    def __repr__(self):
        return f"NODE: {self._ip}:{self._port} => {self._uuid}"

    def _create_uuid(self):
        return random.randrange(0, (2**self._key_size)-1)

    def stop(self):
        self._is_stopped = True

    def res_ping_callback(self, msg: PingMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Api(), respond to incoming PING query message """
        # TODO: Check if we have to save the peer in the routing table
        # echo -n "d1:t2:xx1:y1:q1:q4:ping1:ad2:idd4:uuidi666e2:ip9:127.0.0.14:porti666eeee" | ncat localhost 12345
        sender = Peer(**msg.sender_id)
        origin = Peer(self._uuid, self._ip, self._port)
        self._table.insert_peer(sender, origin)
        return ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)

    def res_find_node_callback(self, msg: FindNodeMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Api(), respond to incoming FIND_NODE query message """
        # echo -n d1:t2:xx1:y1:q1:q9:find_node1:ad2:idd4:uuidi666e2:ip9:127.0.0.14:porti666ee6:targetd4:uuidi98766e2:ip9:127.0.0.14:porti98766eeee | ncat localhost 12345
        sender = Peer(**msg.sender_id)
        target = Peer(**msg.target_id)
        origin = Peer(self._uuid, self._ip, self._port)
        self._table.insert_peer(sender, origin)

        self._table.get_closest_nodes(sender, target)

        res = ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)


    def res_store_callback(self, msg: StoreMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Api(), respond to incoming STORE query message """
        sender = Peer(**msg.sender_id)
        origin = Peer(self._uuid, self._ip, self._port)
        self._table.insert_peer(sender, origin)

    def res_find_key_callback(self, msg: FindKeyMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Api(), respond to incoming FIND_KEY query message """
        sender = Peer(**msg.sender_id)
        origin = Peer(self._uuid, self._ip, self._port)
        self._table.insert_peer(sender, origin)

    def run(self):
        origin = Peer(self._uuid, self._ip, self._port)
        #for _ in range(2000):
        #    n = random.randrange(0, 2**self._key_size-1)
        #    self._table.insert_peer(Peer(n, "127.0.0.1", n), origin)
        ##self._table.insert_peer(Peer(9988, "127.0.0.1", 9988), self._uuid)
        #self._table.insert_peer(Peer(22345, "127.0.0.1", 22345), origin)
        peer = Peer(22345, "127.0.0.1", 22345)
        peer.ping(origin)


        print(self._table)

        callbacks = { "ping":      self.res_ping_callback,
                      "store":     self.res_store_callback,
                      "find_node": self.res_find_node_callback,
                      "find_key":  self.res_find_key_callback }

        api = Api("", self._port, callbacks)
        api.start()

        info("node", "run", f"Entering main event loop")

        while True:
            # main event loop
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                api.stop()
                break

        info("node", "run", f"done")
