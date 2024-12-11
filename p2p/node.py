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

from p2p.store import Store
from p2p.routing import RouteTable
from p2p.crawler import ShortList
from p2p.peer import Peer

from core.utils import debug, info, error


class Node(Server):
    def __init__(self, ip: str, port: int, keyspace: int, bucket_size: int, alpha: int, uuid: Optional[int]=None) -> None:

        # Specify the callbacks that need to run to respond to incoming query messages
        callbacks = { "ping":      self.res_ping_callback,
                      "store":     self.res_store_callback,
                      "find_node": self.res_find_node_callback,
                      "find_key":  self.res_find_key_callback }

        Server.__init__(self, ip, port, callbacks)

        self._keyspace = keyspace                   # Size of network uuid/keys in bits
        self._bucket_size = bucket_size             # Amount of peers stored per bucket (K)
        self._uuid = uuid or self._create_uuid()

        if math.log2(self._uuid) > self._keyspace:
            raise ValueError("node", "init", f"uuid is not in keyspace")

        # Concurency parameter when looking up nodes.
        # Lookup max <alpha> nodes at a time
        self._alpha = alpha

        self._table = RouteTable(keyspace, bucket_size, self._uuid)
        self._store = Store(self._keyspace)

    def __repr__(self):
        return f"NODE: {self._uuid:016b} {self._uuid}@{self._ip}:{self._port}"

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
        self._store.put_by_uuid(msg.key, msg.value)
        self._table.insert_peer(sender, origin)

        info("node", "store_cb", f"storing: {msg.key} : {msg.value}")
        print(self._store)

        # TODO: Respond with a message echoing the key, value
        res = ResponseMsg(transaction_id=msg.transaction_id, uuid=self._uuid, ip=self._ip, port=self._port)
        return res

    def res_find_key_callback(self, msg: FindValueMsg) -> ResponseMsg|ErrorMsg:
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

    def req_ping(self, uuid: int, ip: str, port: int):
        """ Ping peer and return respond time """
        info("peer", "ping", "sending ping")
        origin = Peer(self._uuid, self._ip, self._port)
        target = Peer(uuid, ip, port)
        target.ping(origin)

    def req_find_node_bak(self, target_uuid: int):
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

    def get_nodes_from_peer(self, peer: Peer, target_uuid: int):
        origin = Peer(self._uuid, self._ip, self._port)

        msg_req = FindNodeMsg(uuid=self._uuid, ip=self._ip, port=self._port)
        msg_req.target_uuid = target_uuid
        new_peers = []

        if (msg_res := send_request(peer.ip, peer.port, msg_req)):
            assert type(msg_res) != MsgError, f"Find node received an error from: {peer}"
            self._table.insert_peer(peer, origin)

            # add received peers to new_peers if not already contacted
            new_peers = [Peer(x["uuid"], x["ip"], x["port"]) for x in msg_res.return_values[MsgKey.NODES]]
            for p in new_peers:
                p.parent = peer

        else:
            # Request failed, remove peer from our routing table (if it's there)
            self._table.remove_peer(peer, self._uuid)

        return new_peers

    def req_find_node(self, target_uuid: int):
        """ Initiate an iterative node lookup """
        info("peer", "req_find_node", f"sending find_node: {target_uuid}")
        origin = Peer(self._uuid, self._ip, self._port)
        shortlist = ShortList(target_uuid, self._bucket_size)

        # Add ourself to rejected nodes so we don't add ourselves to the shortlist
        shortlist.set_rejected(origin)
        round = 1

        peers = []
        #print(self._table)

        # Add initial <alpaha> closest peers
        for peer in self._table.get_closest_nodes(origin, target_uuid, amount=self._alpha):
            shortlist.add(peer)

        #while shortlist.has_uncontacted_peers():
        while not shortlist.is_complete() and shortlist.has_uncontacted_peers():
            round += 1
            peers = shortlist.get_peers(self._alpha)

            for peer in peers:
                assert (origin.uuid, origin.ip, origin.port) != (peer.uuid, peer.ip, peer.port), "Don't add origin to shortlist"

                if new_peers := self.get_nodes_from_peer(peer, target_uuid):
                    shortlist.set_contacted(peer)
                    for p in new_peers:
                        shortlist.add(p)
                else:
                    shortlist.set_rejected(peer)

        print(f"Finished in {round} rounds")
        shortlist.print_results()
        #print(self._table)
        return shortlist.get_results()

    def req_find_value(self, k: str):
        """ Do an iterative search for nodes close to <k> to find k:v pair.
            Similar to req_find_node() but if a key is found, stop search immediately.
            If key is found, store key at closest node to <K> that doesn't have the key stored. """
        origin = Peer(self._uuid, self._ip, self._port)
        key_uuid = self._store.get_hash(k)
        shortlist = ShortList(key_uuid, self._bucket_size)

        # Add ourself to rejected nodes so we don't add ourselves to the shortlist
        shortlist.set_rejected(origin)

        info("node", "find_value", f"finding '{k} ({key_uuid:016b}")

        # Add initial <alpaha> closest peers
        for peer in self._table.get_closest_nodes(origin, key_uuid, amount=self._alpha):
            shortlist.add(peer)

        #while shortlist.has_uncontacted_peers():
        while not shortlist.is_complete() and shortlist.has_uncontacted_peers():
            peers = shortlist.get_peers(self._alpha)

            for peer in peers:
                assert (origin.uuid, origin.ip, origin.port) != (peer.uuid, peer.ip, peer.port), "Don't add origin to shortlist"

                msg_req = FindValueMsg(uuid=self._uuid, ip=self._ip, port=self._port)
                msg_req.key = key_uuid

                if (msg_res := send_request(peer.ip, peer.port, msg_req)):
                    assert type(msg_res) != MsgError, f"Find node received an error from: {peer}"
                    self._table.insert_peer(peer, origin)


                    # add received peers to new_peers if not already contacted
                    if value := msg_res.return_values.get(MsgKey.VALUE):
                        info("node", "find_value", f"Found key '{value}' @ {peer}")

                        # Cache key at closest node that didn't have the k:v pair
                        print(shortlist.print_results())
                        if p := shortlist.get_results(limit=1):
                            self.req_store(k, value, target_peer=p[0])

                        return value

                    shortlist.set_contacted(peer)


                    new_peers = [Peer(x["uuid"], x["ip"], x["port"]) for x in msg_res.return_values[MsgKey.NODES]]
                    for p in new_peers:
                        shortlist.add(p)
                        p.parent = peer

                else:
                    # Request failed, remove peer from our routing table (if it's there)
                    self._table.remove_peer(peer, self._uuid)
                    shortlist.set_rejected(peer)
        
        print(shortlist.print_results())
        error("node", "find_value", f"Failed to find key: {k} ({key_uuid})")


    def req_store(self, k: str, v: str, target_peer: Optional[Peer]=None):
        """ Find nodes close to <k> and ask them to store the k:v pair.
            If target_peer != None, don't search, just ask only this peer """
        origin = Peer(self._uuid, self._ip, self._port)

        target_uuid = self._store.get_hash(k)
        msg_req = StoreMsg(uuid=self._uuid, ip=self._ip, port=self._port)
        msg_req.key = target_uuid
        msg_req.value = v

        print(f"Finding key @ {target_uuid:016b}")

        if target_peer:
            peers = [target_peer]
        else:
            peers = self.req_find_node(target_uuid)

        for peer in peers:
            if (msg_res := send_request(peer.ip, peer.port, msg_req)):
                assert type(msg_res) != MsgError, f"store received an error from: {peer}"
                info("node", "req_store", f"stored {k}:{v} @ {peer}")
            else:
                self._table.remove_peer(peer, self._uuid)
                error("node", "req_store", f"peer didn't respond: {peer}")


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
        # TODO: do bucket refresh by picking a random peer from bucket that has not been refreshed
        # for an hour and ping if still alive

        # This starts blocking server that listens for incoming connections
        self.listen()
