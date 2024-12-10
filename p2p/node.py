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
from p2p.message import MsgKey
from p2p.store import Store
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
    parent: Optional["Peer"] = None

    def __repr__(self):
        #return f"{self.uuid:5} @ {self.ip}:{self.port:5}"
        return f"{self.uuid:016b} @ {self.ip}:{self.port:5}"

    def to_dict(self):
        return { "uuid" : self.uuid,
                 "ip" : self.ip,
                 "port" : self.port }

    def trace_back(self):
        """ Trace back peer to get a path to origin. This is used for debuggin only """
        trace = []
        peer = self
        while peer:
            trace.append(f"{peer.uuid:016b}")
            peer = peer.parent
        #print(" > ".join(trace))
        return trace

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

    #def ping(self, origin_uuid: int):
    def ping(self, origin: "Peer"):
        msg_req = PingMsg(uuid=origin.uuid, ip=origin.ip, port=origin.port)
        if (msg_res := send_request(self.ip, self.port, msg_req)):
            #info("peer", "ping", f"response time: {msg_res.response_time}")
            return msg_res.response_time


class ShortList():
    def __init__(self, target_uuid: int, limit: int):
        """
        source: https://www.scs.stanford.edu/17au-cs244b/labs/projects/kaplan-nelson_ma_rachleff.pdf
        1) Select α contacts from the closest non-empty k-bucket to the key being
           searched for. If there are fewer than α contacts in that bucket,
           Kademlia will select from buckets increasingly farther away from the key.
           α represents the number of parallel requests.
        2) The initiator node sends asynchronous FIND_NODE (or FIND_VALUE RPCs in
           parallel to each of the α contacts.
        3) Upon receiving each RPC response, the initiator node adds the received
           list of k-closest nodes to a sorted set, sorting the nodes using the XOR
           distance between a node’s ID and the key.
        4) The initiator repeatedly selects the first α contacts from the list that
           have not been contacted yet. FIND_NODE RPCs are sent to these contacts.
           At most α requests are in parallel at once, and the recursive procedure
           can begin before all requests from one round have returned. The α requests
           will always be to the closest nodes the initiator has heard of.
        5) The search terminates when the k closest nodes to the key have been queried
           and responded, or when all the known nodes in the network have been queried.
           In the case of sending FIND_VALUE RPCs, the search terminates as soon as the
           initiator node receives a response that the value has been found, meaning the
           RPC response payload contains a list of peers whom the initiator node can
           contact for the data. (We were unable to implement removing a node from
           consideration if it does not respond. This is in our future steps).
        6) The list of K-closest nodes is passed to a callback; for put’s, Kademlia sends
           the nodes in the list STORE RPCs. For get’s, the list of nodes is passed to a
           callback which calls the user-specified callback with the list (if found) or
           an empty list if the value was not found.

        source: https://xlattice.sourceforge.net/components/protocol/kademlia/specs.html#FIND_VALUE
        """
        self._list = []
        self._target_uuid = target_uuid

        # the max size of list, probably K
        self._limit = limit

        # Contacted peers
        self._contacted = []

        # Peers that did not respond end up here
        self._rejected = []

    def __repr__(self):
        out = []
        out.append(f"Shortlist:")
        for i,p in enumerate(sorted(self._list, key=lambda x: x.get_distance(self._target_uuid))):
            out.append(f"  {i:2}: {p}")
        out.append(f"contacted:")
        for i,p in enumerate(sorted(self._contacted, key=lambda x: x.get_distance(self._target_uuid))):
            out.append(f"  {i:2}: {p}")
        return "\n".join(out)

    def set_contacted(self, peer: Peer):
        self._list.remove(peer)
        self._contacted.append(peer)

    def set_rejected(self, peer: Peer):
        """ Peer did not respond, let's put it on a list so we can contact it again later if needed """
        try:
            self._list.remove(peer)
        except ValueError:
            pass
        self._rejected.append(peer)

    def is_in(self, src_list: list[Peer], peer: Peer):
        """ Check if peer is in src_list. We cannot just use the "in" keyword because
            that would compare peers by reference instead by attributes. """
        for p in src_list:
            if p.uuid == peer.uuid and p.ip == peer.ip and p.port == peer.port:
                return peer

    def has_uncontacted_peers(self):
        return len(self._list)

    def add(self, peer: Peer):
        if self.is_in(self._list + self._contacted + self._rejected, peer):
            return
        self._list.append(peer)

    def is_complete(self):
        return len(self._contacted) >= self._limit

    def get_results(self):
        """ Return a list of K peers (or less if we couldn't find more) sorted by closeness """
        return sorted(self._contacted, key=lambda x: x.get_distance(self._target_uuid))[:self._limit]

    def get_peers(self, limit: int):
        """ Return the next set of peers from the shortlist """
        return sorted(self._list, key=lambda x: x.get_distance(self._target_uuid))[:limit]

    def print_results(self):
        print(f"FIND_PEER RESULTS ({self._target_uuid:016b}, {self._target_uuid})")
        for i,peer in enumerate(self.get_results()):
            print(f"{i:2} {" > ".join(peer.trace_back())}")


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

    def insert_peer(self, peer: Peer, origin: Peer):
        """ We have a least seen eviction policy and we prefer old peers because they tend to
            be reliable than new peers. Apparently research concluded that peers that have been
            online for an hour are likely to be online for another hour. """
        bucket = peer.find_significant_common_bits(origin.uuid, self._keyspace)

        with self._lock:
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
    def __init__(self, ip: str, port: int, keyspace: int, bucket_size: int, alpha: int, uuid: Optional[int]=None) -> None:

        # Specify the callbacks that need to run to respond to incoming query messages
        callbacks = { "ping":      self.res_ping_callback,
                      "store":     self.res_store_callback,
                      "find_node": self.res_find_node_callback,
                      "find_key":  self.res_find_key_callback }

        Server.__init__(self, ip, port, callbacks)

        # Size of network uuid/keys in bits
        self._keyspace = keyspace

        # Amount of peers stored per bucket (K)
        self._bucket_size = bucket_size

        #self._ip = ip
        #self._port = port

        if uuid == None:
            self._uuid = self._create_uuid()
        else:
            if math.log2(uuid) > self._keyspace:
                raise ValueError("node", "init", f"uuid is not in keyspace")

            self._uuid = uuid

        # Concurency parameter when looking up nodes.
        # Lookup max <alpha> nodes at a time
        self._alpha = alpha

        self._table = RouteTable(keyspace, bucket_size, self._uuid)
        self._store = Store(self._keyspace)

    def __repr__(self):
        return f"NODE: {self._uuid}@{self._ip}:{self._port}"

    def _create_uuid(self):
        return random.randrange(0, (2**self._keyspace)-1)

    def res_ping_callback(self, msg: PingMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming PING query message """
        # TODO: Check if we have to save the peer in the routing table
        # echo -n "d1:t2:xx1:y1:q1:q4:ping1:ad2:idd4:uuidi666e2:ip9:127.0.0.14:porti666eeee" | ncat localhost 12345
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)
        origin = Peer(self._uuid, self._ip, self._port)
        self._table.insert_peer(sender, origin)
        return ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)

    def res_find_node_callback(self, msg: FindNodeMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming FIND_NODE query message """
        # echo -n d1:t2:xx1:y1:q1:q9:find_node1:ad2:idd4:uuidi666e2:ip9:127.0.0.14:porti666ee6:targetd4:uuidi98766e2:ip9:127.0.0.14:porti98766eeee | ncat localhost 12345
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)

        peers = self._table.get_closest_nodes(sender, msg.target_uuid)
        origin = Peer(self._uuid, self._ip, self._port)

        self._table.insert_peer(sender, origin)
        # TODO: write and read address={ip, port}

        #for i,p in enumerate(peers):
        #    print(f"[{i}] find_node_cb returning: {p}")

        res = ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)
        res.return_values = {MsgKey.NODES : [p.to_dict() for p in peers]}
        return res

    def res_store_callback(self, msg: StoreMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming STORE query message """
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)
        origin = Peer(self._uuid, self._ip, self._port)
        info("node", "store_cb", f"storing: {msg.key} : {msg.value}")
        self._store.put_by_uuid(msg.key, msg.value)
        self._table.insert_peer(sender, origin)
        print(self._store)

        # TODO: Respond with a message echoing the key, value
        res = ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)
        return res

    def res_find_key_callback(self, msg: FindKeyMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming FIND_KEY query message.
            If key is found on this node, return key. Otherwise return the K closest
            nodes to key that we know of """
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)
        origin = Peer(self._uuid, self._ip, self._port)
        self._table.insert_peer(sender, origin)
        res = ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)

        if value := self._store.get_by_uuid(msg.key):
            res.return_values = { MsgKey.VALUE : value }
            return res
        else:
            peers = self._table.get_closest_nodes(sender, msg.key)
            res.return_values = {MsgKey.NODES : [p.to_dict() for p in peers]}
            return res

    def ping(self, uuid: int, ip: str, port: int):
        """ Ping peer and return respond time """
        info("peer", "ping", "sending ping")
        origin = Peer(self._uuid, self._ip, self._port)
        target = Peer(uuid, ip, port)
        target.ping(origin)

    def req_find_node(self, target_uuid: int):
        """ Initiate an iterative node lookup """
        info("peer", "req_find_node", f"sending find_node: {target_uuid}")
        origin = Peer(self._uuid, self._ip, self._port)
        shortlist = ShortList(target_uuid, self._bucket_size)

        # Add ourself to rejected nodes so we don't add ourselves to the shortlist
        shortlist.set_rejected(origin)

        # Add initial <alpaha> closest peers
        for peer in self._table.get_closest_nodes(origin, target_uuid, amount=self._alpha):
            shortlist.add(peer)

        while not shortlist.is_complete() and shortlist.has_uncontacted_peers():
            peers = shortlist.get_peers(self._alpha)

            for peer in peers:
                assert (origin.uuid, origin.ip, origin.port) != (peer.uuid, peer.ip, peer.port), "Don't add origin to shortlist"

                msg_req = FindNodeMsg(uuid=self._uuid, ip=self._ip, port=self._port)
                msg_req.target_uuid = target_uuid

                if (msg_res := send_request(peer.ip, peer.port, msg_req)):
                    assert type(msg_res) != MsgError, f"Find node received an error from: {peer}"
                    self._table.insert_peer(peer, origin)
                    shortlist.set_contacted(peer)

                    # add received peers to new_peers if not already contacted
                    new_peers = [Peer(x["uuid"], x["ip"], x["port"]) for x in msg_res.return_values[MsgKey.NODES]]
                    for p in new_peers:
                        shortlist.add(p)
                        p.parent = peer

                else:
                    # Request failed, remove peer from our routing table (if it's there)
                    self._table.remove_peer(peer, self._uuid)
                    shortlist.set_rejected(peer)

        shortlist.print_results()
        return shortlist.get_results()

    def req_find_value(self, k: str):
        """ Do an iterative search for nodes close to <k> to find k:v pair.
            Similar to req_find_node() but if a key is found, stop search immediately.
            If key is found, store key at closest node to <K> that doesn't have the key stored. """


    def req_store(self, k: str, v: str):
        origin = Peer(self._uuid, self._ip, self._port)

        target_uuid = self._store.get_hash(k)
        msg_req = StoreMsg(uuid=self._uuid, ip=self._ip, port=self._port)
        msg_req.key = target_uuid
        msg_req.value = v

        print(f"Finding key @ {target_uuid:016b}")
        print(msg_req)
        print(msg_req.to_bencoding())

        for peer in self.req_find_node(target_uuid):
            if (msg_res := send_request(peer.ip, peer.port, msg_req)):
                assert type(msg_res) != MsgError, f"store received an error from: {peer}"
                info("node", "req_store", f"stored {k}:{v} @ {peer}")
            else:
                self._table.remove_peer(peer, self._uuid)
                error("node", "req_store", f"peer didn't respond: {peer}")
            print(msg_res)


    def bootstrap(self, peers):
        """ Add bootstrap nodes to table and do a lookup for our own uuid in these new
            nodes to populate our and their routing table. """
        origin = Peer(self._uuid, self._ip, self._port)

        for peer in peers:
            info("node", "bootstrap", str(peer))
            if peer == origin:
                error("node", "bootstrap", "not bootstrapping from ourself!")
                continue
            self._table.insert_peer(peer, origin)

        self.req_find_node(self._uuid)


        print("end")

    def run(self):
        info("node", "run", f"starting listener")

        # This starts blocking server that listens for incoming connections
        self.listen()
