from typing import Optional
import random
import math
from dataclasses import dataclass
import time
import socket

from p2p.network.server import Server
from p2p.network.message import PingMsg, StoreMsg, FindNodeMsg, FindValueMsg, ResponseMsg, ErrorMsg
from p2p.network.message import MsgError, MsgKey
from p2p.network.bencode import BencDecodeError
from p2p.network.utils import send_request

from p2p.periodical import MaintenanceThread, TaskDeleteExpiredCache, TaskRepublishKeys, TaskRefreshBuckets

from p2p.store import Store
from p2p.routing import RouteTable
from p2p.crawler import ShortList, NodeCrawler, ValueCrawler
from p2p.peer import Peer

from core.utils import debug, info, error

"""
    Join:
        1. Insert bootstrap node in bucket
        2. Perform FIND_NODE RPC on our own UUID
        3. Refresh all buckets further away than it's closest neighbor (?)

"""

T_EXPIRE_CACHED_KEY_SEC = 60 * 60
T_EXPIRE_KEY_SEC = 60 * 60 * 24


class Node(Server):
    def __init__(self, ip: str, port: int, keyspace: int, bucket_size: int, alpha: int, uuid: Optional[int]=None) -> None:

        # Specify the callbacks that need to run to respond to incoming query messages
        callbacks = { "ping":      self.rpc_ping_callback,
                      "store":     self.rpc_store_callback,
                      "find_node": self.rpc_find_node_callback,
                      "find_key":  self.rpc_find_key_callback }

        Server.__init__(self, ip, port, callbacks)

        self._keyspace = keyspace                   # Size of network uuid/keys in bits
        self._bucket_size = bucket_size             # Amount of peers stored per bucket (K)
        self._uuid = uuid or self._create_uuid()

        if math.log2(self._uuid) > self._keyspace:
            raise ValueError("node", "init", f"uuid is not in keyspace")

        # Concurency parameter when looking up nodes.
        # Lookup max <alpha> nodes at a time
        self._alpha = alpha

        self._store = Store(self._keyspace)
        self._table = RouteTable(self._store, keyspace, bucket_size, self._uuid)

        self._originator_store = Store(self._keyspace)
        self._republish_store = Store(self._keyspace)
        self._cache_store = Store(self._keyspace)

    def __repr__(self):
        return f"NODE: {self._uuid:016b} {self._uuid}@{self._ip}:{self._port}"

    def _create_uuid(self):
        return random.randrange(0, (2**self._keyspace)-1)

    def rpc_ping_callback(self, msg: PingMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming PING query message """
        # TODO: Check if we have to save the peer in the routing table
        # echo -n "d1:t2:xx1:y1:q1:q4:ping1:ad2:idd4:uuidi666e2:ip9:127.0.0.14:porti666eeee" | ncat localhost 12345
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)
        origin = Peer(self._uuid, self._ip, self._port)
        self.welcome_peer(sender)
        self._table.insert_peer(sender, origin)
        return ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)

    def rpc_find_node_callback(self, msg: FindNodeMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming FIND_NODE query message """
        # echo -n d1:t2:xx1:y1:q1:q9:find_node1:ad2:idd4:uuidi666e2:ip9:127.0.0.14:porti666ee6:targetd4:uuidi98766e2:ip9:127.0.0.14:porti98766eeee | ncat localhost 12345
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)

        peers = self._table.get_closest_nodes(sender, msg.target_uuid)
        origin = Peer(self._uuid, self._ip, self._port)

        self.welcome_peer(sender)
        self._table.insert_peer(sender, origin)
        # TODO: write and read address={ip, port}

        #for i,p in enumerate(peers):
        #    print(f"[{i}] find_node_cb returning: {p}")

        res = ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)
        res.return_values = {MsgKey.NODES : [p.to_dict() for p in peers]}
        return res

    def rpc_store_callback(self, msg: StoreMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming STORE query message """
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)
        origin = Peer(self._uuid, self._ip, self._port)
        self._store.put_by_uuid(msg.key, msg.value)
        self.welcome_peer(sender)
        self._table.insert_peer(sender, origin)

        info("node", "store_cb", f"storing: {msg.key} : {msg.value}")
        print(self._store)

        # TODO: Respond with a message echoing the key, value
        res = ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)
        return res

    def rpc_find_key_callback(self, msg: FindValueMsg) -> ResponseMsg|ErrorMsg:
        """ Called by Server(), respond to incoming FIND_KEY query message.
            If key is found on this node, return key. Otherwise return the K closest
            nodes to key that we know of """
        sender = Peer(msg.sender_uuid, msg.sender_ip, msg.sender_port)
        origin = Peer(self._uuid, self._ip, self._port)
        self.welcome_peer(sender)
        self._table.insert_peer(sender, origin)
        res = ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)

        if value := self._store.get_by_uuid(msg.key):
            res.return_values = { MsgKey.VALUE : value }
            return res
        else:
            peers = self._table.get_closest_nodes(sender, msg.key)
            res.return_values = {MsgKey.NODES : [p.to_dict() for p in peers]}
            return res

    def call_ping(self, uuid: int, ip: str, port: int):
        """ Ping peer and return respond time """
        info("peer", "ping", "sending ping")
        origin = Peer(self._uuid, self._ip, self._port)
        target = Peer(uuid, ip, port)
        target.ping(origin)

    def call_find_node(self, target_uuid: int):
        origin = Peer(self._uuid, self._ip, self._port)
        boot_peers = self._table.get_closest_nodes(origin, target_uuid, amount=self._alpha)
        crawler = NodeCrawler(self._bucket_size, self._alpha, self._table, origin, boot_peers, target_uuid)
        peers = crawler.find()
        print(crawler.shortlist.print_results())
        return peers

    def call_find_value(self, k: str):
        """ Do an iterative search for nodes close to <k> to find k:v pair.
            Similar to call_find_node() but if a key is found, stop search immediately.
            If key is found, store key at closest node to <K> that doesn't have the key stored. """
        origin = Peer(self._uuid, self._ip, self._port)
        key_uuid = self._store.get_hash(k)
        boot_peers = self._table.get_closest_nodes(origin, key_uuid, amount=self._alpha)
        crawler = ValueCrawler(self._bucket_size, self._alpha, self._table, origin, boot_peers, key_uuid)

        if value := crawler.find():
            print(crawler.shortlist.get_results(limit=1))

            # Cache key at closest node that didn't have the k:v pair
            for p in crawler.shortlist.get_contacted_peers()[1:]:
                if self.call_store(value, key_str=k, target_peer=p):
                    break

        print(crawler.shortlist.print_results())
        return value

    def call_store(self, v: str, key_str: Optional[str]=None, key_uuid: Optional[int]=None, target_peer: Optional[Peer]=None):
        """ Find nodes close to <k> and ask them to store the k:v pair.
            If target_peer != None, don't search, just ask only this peer.
            Returns: True if at least one of them was stored succesfully. """

        if key_str:
            key_uuid = self._store.get_hash(key_str)
        elif key_uuid:
            ...
        else:
            raise ValueError("key_str | key_uuid is required")

        msg_req = StoreMsg(uuid=self._uuid, ip=self._ip, port=self._port)
        msg_req.key = key_uuid
        msg_req.value = v
        result = False

        print(f"Finding key @ {key_uuid:016b}")

        if target_peer:
            peers = [target_peer]
        else:
            peers = self.call_find_node(key_uuid)

        for peer in peers:
            if (msg_res := send_request(peer.ip, peer.port, msg_req)):
                assert type(msg_res) != MsgError, f"store received an error from: {peer}"
                info("node", "call_store", f"stored {key_uuid}:{v} @ {peer}")
                result = True
            else:
                self._table.remove_peer(peer, self._uuid)
                error("node", "call_store", f"peer didn't respond: {peer}")

        # We need to take responsibility for keeping this k:v alive so we're going
        # to resend it every n seconds
        self._originator_store.put_by_uuid(key_uuid, v)
        return result

    def welcome_peer(self, peer: Peer):
        """ We need to check if there's a k:v in store that matches the new_peer's range.
            This implies that this is a new peer that is closer to the key than we previously
            knew of. We need to notify this peer of this k:v. """
        # TODO: not tested yet!

        origin = Peer(self._uuid, self._ip, self._port)

        if not (next_peer := self._table.find_next_neighbour(peer, origin.uuid)):
            # TODO: do something here
            return

        for d in self._store.find_kv_in_range(peer.uuid, next_peer.uuid):
            self.call_store(d["value"], key_uuid=d["uuid"], target_peer=peer)

    def buckets_refresh(self):
        ...

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

        self.call_find_node(self._uuid)

    def run(self):
        # TODO: do bucket refresh by picking a random peer from bucket that has not been refreshed
        # for an hour and ping if still alive

        # Do some periodic tasks
        t = MaintenanceThread()
        t.add_task(TaskDeleteExpiredCache(self._store, "expired_cache", 5))
        t.add_task(TaskRepublishKeys(self._originator_store, self.call_store, "originator", 60*60*6))
        t.add_task(TaskRefreshBuckets(self._table, "refresh", 15))
        t.start()

        # This starts blocking server that listens for incoming connections
        info("node", "run", f"starting listener")
        self.listen()

        time.sleep(10)

        t.stop()
