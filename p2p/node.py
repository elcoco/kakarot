from typing import Optional
import random
import math
import json
import requests
from dataclasses import dataclass
import time
import socket
import threading

from p2p.server import Server
from p2p.bencode import BencDecodeError
from p2p.message import PingMsg, StoreMsg, FindNodeMsg, FindKeyMsg, ResponseMsg, ErrorMsg
from p2p.message import MsgError
from core.utils import debug, info, error

MSG_LENGTH = 1024


def send_request(ip: str, port: int, msg_req, timeout=5):
    """ Send a request message and receive response """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

        t_start = time.time()

        try:
            s.connect((ip, port))
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


@dataclass
class Peer():
    """ Represents an external node """

    # An n-bit identifier that represents the peer's location in the binary tree.
    # An XOR(origin, peer) is done to calculate the distance.
    uuid: int
    ip: str
    port: int

    def __repr__(self):
        return f"{self.uuid:5} @ {self.ip}:{self.port:5}"
        #return f"{self.uuid:016b}@{self.ip}:{self.port}"
        #return f"{self.uuid:04X}@{self.ip}:{self.port}"

    def to_dict(self):
        return { "uuid" : self.uuid,
                 "ip" : self.ip,
                 "port" : self.port }

    def get_distance(self, origin_uuid):
        return self.uuid ^ origin_uuid

    def find_significant_common_bits(self, origin_uuid: int, keyspace: int) -> int:
        """ Compare uuid's and find common most significant bits.
            This is used to determin the bucket we should store this peer """

        # if peer == origin, all bits match so take last bucket as closest
        if origin_uuid == self.uuid:
            return keyspace - 1

        # get the non matching bits between peer and origin node
        distance = self.get_distance(origin_uuid)
        count = 0

        for i in reversed(range(keyspace)):
            if distance & (0x01 << i):
                break
            else:
                count += 1
        return count

    def ping(self, origin_uuid: int):
        msg_req = PingMsg(uuid=origin_uuid)
        if (msg_res := send_request(self.ip, self.port, msg_req)):
            info("peer", "ping", f"response time: {msg_res.response_time}")
            print(msg_res)
            return msg_res.response_time

""" TODO: Implement recursive algorithm utilizing the PeerList class to keep track of the returned peers """

class PeerList():
    """ A list of peers, ordered by closeness to a uuid """
    def __init__(self, peers: list[Peer], target: Peer, amount: int):
        self._list = []
        self._closest: Optional[int] = None
        self._target = target

        # the max size of list
        self._amount = amount

        for peer in peers:
            self.add(peer)

        # store peers that were not closer so we can query them later if necessary
        self._rejected: list[Peer] = []

    def add(self, peer: Peer):
        """ Add peer to list if it is closer than the peers already in the list """
        if self._closest == None:
            self._closest = self._target.get_distance(peer.uuid)
            self._list.append(peer)
        elif (distance := self._target.get_distance(peer.uuid)) < self._closest:
            self._closest = distance
            self._list.append(peer)
        else:
            self._rejected.append(peer)

    def is_full(self):
        return len(self._list) >= self._amount

    def get_rejected(self, amount):
        """ Get closest peer from list of rejected peers """
        if not self._rejected:
            return
        rejected = sorted(self._rejected, key=lambda x: x.get_distance(self._target.uuid))
        ret = rejected[:amount]
        self._rejected = rejected[amount:]
        return ret






class Lock():
    _is_locked = False

    def __enter__(self):
        #info("lock", "enter", "waiting for lock")
        while Lock._is_locked:
            time.sleep(0.01)
        Lock._is_locked = True
        #info("lock", "enter", "aquired lock")

    def __exit__(self, exc_type, exc_val, exc_tb):
        Lock._is_locked = False
        #info("lock", "enter", "released lock")


class RouteTable():
    def __init__(self, keyspace: int, bucket_size: int, origin_uuid) -> None:
        self._keyspace = keyspace
        self._bucket_size = bucket_size
        self._origin_uuid = origin_uuid
        self._lock = threading.Lock()

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
            header = f"BUCKET {i_bucket:2}:"
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
                #print(target, f"bucket={bucket}", f"n={n}")
                assert n < len(self._k_buckets), f"n={n}, k_buckets={len(self._k_buckets)}"

                peers_sorted = sorted(self._k_buckets[n], key=lambda x: x.get_distance(target))
                out += peers_sorted[:amount-len(out)]

                if n >= self._keyspace-1:
                    if bucket == 0:
                        break
                    else:
                        n = bucket -1
                elif n == 0:
                    break
                elif n < bucket:
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

    def insert_peer(self, peer: Peer, origin_uuid: int):
        """ We have a least seen eviction policy and we prefer old peers because they tend to
            be reliable than new peers. Apparently research concluded that peers that have been
            online for an hour are likely to be online for another hour. """
        bucket = peer.find_significant_common_bits(origin_uuid, self._keyspace)

        with self._lock:
            if (p := self._bucket_has_peer(bucket, peer)):
                self._k_buckets[bucket].remove(p)
                self._k_buckets[bucket].append(peer)
                info("route_table", "insert_peer", "moved peer to end")

            elif self._bucket_is_full(bucket):
                # Now we need to ping the least seen node (head) and remove it if it doesn't respond
                oldest_peer = self._k_buckets[bucket][0]

                if oldest_peer.ping(origin_uuid):
                    # Old peer is alive, discard new peer because we prefer our old peers (more trustworthy)
                    self._k_buckets[bucket].remove(oldest_peer)
                    self._k_buckets[bucket].append(oldest_peer)
                    info("route_table", "insert_peer", "bucket full, discard peer: {peer}")
                else:
                    # peer doesn't respond so we replace it
                    self._k_buckets[bucket].remove(oldest_peer)
                    self._k_buckets[bucket].append(peer)
                    info("route_table", "insert_peer", "old peer unreachable, adding new peer: {peer}")

            else:
                self._k_buckets[bucket].append(peer)
                info("route_table", "insert_peer", f"adding new peer: {peer}")

    def remove_peer(self, peer: Peer, origin_uuid: int):
        bucket = peer.find_significant_common_bits(origin_uuid, self._keyspace)

        with self._lock:
            if (p := self._bucket_has_peer(bucket, peer)):
                self._k_buckets[bucket].remove(p)
            else:
                error("route_table", "remove", f"Failed to remove peer from bucket '{bucket}', peer not found: {peer}")


class Node(Server):
    def __init__(self, ip: str, port: int, key_size: int, bucket_size: int, alpha: int, uuid: Optional[int]=None) -> None:

        # Specify the callbacks that need to run to respond to incoming query messages
        callbacks = { "ping":      self.res_ping_callback,
                      "store":     self.res_store_callback,
                      "find_node": self.res_find_node_callback,
                      "find_key":  self.res_find_key_callback }

        Server.__init__(self, ip, port, callbacks)

        # Size of network uuid/keys in bits
        self._key_size = key_size

        # Amount of peers stored per bucket (K)
        self._bucket_size = bucket_size

        #self._ip = ip
        #self._port = port

        if uuid == None:
            self._uuid = self._create_uuid()
        else:
            if math.log2(uuid) > self._key_size:
                raise ValueError("node", "init", f"uuid is not in keyspace")

            self._uuid = uuid

        # Concurency parameter when looking up nodes.
        # Lookup max <alpha> nodes at a time
        self._alpha = alpha

        self._table = RouteTable(key_size, bucket_size, self._uuid)

    def __repr__(self):
        return f"NODE: {self._uuid}@{self._ip}:{self._port}"

    def _create_uuid(self):
        return random.randrange(0, (2**self._key_size)-1)

    def res_ping_callback(self, msg: PingMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming PING query message """
        # TODO: Check if we have to save the peer in the routing table
        # echo -n "d1:t2:xx1:y1:q1:q4:ping1:ad2:idd4:uuidi666e2:ip9:127.0.0.14:porti666eeee" | ncat localhost 12345
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)
        self._table.insert_peer(sender, self._uuid)
        return ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)

    def res_find_node_callback(self, msg: FindNodeMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming FIND_NODE query message """
        # echo -n d1:t2:xx1:y1:q1:q9:find_node1:ad2:idd4:uuidi666e2:ip9:127.0.0.14:porti666ee6:targetd4:uuidi98766e2:ip9:127.0.0.14:porti98766eeee | ncat localhost 12345
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)

        peers = self._table.get_closest_nodes(sender, msg.target_uuid)

        self._table.insert_peer(sender, self._uuid)
        # TODO: write and read address={ip, port}

        for i,p in enumerate(peers):
            print(f"[{i}] find_node_cb returning: {p}")

        res = ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)
        res.return_values = {"nodes" : [p.to_dict() for p in peers]}
        return res

    def res_store_callback(self, msg: StoreMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming STORE query message """
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)
        self._table.insert_peer(sender, self._uuid)

    def res_find_key_callback(self, msg: FindKeyMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming FIND_KEY query message """
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)
        self._table.insert_peer(sender, self._uuid)

    def ping(self, uuid: int, ip: str, port: int):
        """ Ping peer and return respond time """
        info("peer", "ping", "sending ping")
        target = Peer(uuid, ip, port)
        target.ping(self._uuid)

    def find_node_rec(self, target_uuid, peers: list[Peer], contacted_peers: list[Peer], trace, level=1):
        new_peers = []
        print("iteration:", level)

        trace.append(peers)

        # we found
        if len(contacted_peers) >= self._bucket_size:
            ...

        #for peer in peers:
        #    if peer.uuid == target_uuid:
        #        info("peer", "find_node", f"found peer: {peer}")
        #        trace.append([peer])
        #        return trace

        for peer in peers:
            msg_req = FindNodeMsg(uuid=self._uuid, ip=self._ip, port=self._port)
            msg_req.target_uuid = target_uuid

            if (msg_res := send_request(peer.ip, peer.port, msg_req)):
                contacted_peers.append(peer)

                # add received peers to new_peers if not already contacted
                tmp = [Peer(**x) for x in msg_res.return_values["nodes"]]
                new_peers += [p for p in tmp if p not in contacted_peers]

                for p in new_peers:
                    self._table.insert_peer(p, self._uuid)

                print(f"received uncontacted peers from {peer}:")
                for p in new_peers:
                    print(f"  {p}")

            else:
                # Request failed, remove peer from our routing table (if it's there)
                self._table.remove_peer(peer, self._uuid)


        if new_peers:
            new_peers = sorted(new_peers, key=lambda x: x.get_distance(target_uuid))
            peers = new_peers[:self._alpha]
            self.find_node_rec(target_uuid, peers, contacted_peers, trace, level=level+1)

        return trace

    def find_node(self, target_uuid: int):
        """ Initiate a recursive node lookup """
        info("peer", "find_node", f"sending find_node: {target_uuid}")

        # get the <alpha> closest peers we know of
        origin = Peer(self._uuid, self._ip, self._port)
        closest_peers = self._table.get_closest_nodes(origin, target_uuid, amount=self._bucket_size)
        peers = closest_peers[:self._alpha]
        trace = self.find_node_rec(target_uuid, peers, [], [])

        print("TRACE:")
        for i,p in enumerate(trace):
            print(f"  {i}: {p}")

        return

    def bootstrap(self, peers):
        """ Add bootstrap nodes to table and do a lookup for our own uuid in these new
            nodes to populate our and their routing table. """
        origin = Peer(self._uuid, self._ip, self._port)

        for peer in peers:
            info("node", "bootstrap", str(peer))
            if peer == origin:
                error("node", "bootstrap", "not bootstrapping from ourself!")
                continue
            self._table.insert_peer(peer, self._uuid)

        self.find_node(self._uuid)

        print("end")

    def run(self):
        info("node", "run", f"starting listener")

        # This starts blocking server that listens for incoming connections
        self.listen()

        info("node", "run", f"done")
