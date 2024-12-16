from dataclasses import dataclass, field
from typing import Callable, Optional, Any

from p2p.network.message import PingMsg

from p2p.network.utils import send_request

from core.utils import id_to_str


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

    def to_str(self, keyspace: int):
        return id_to_str(self.uuid, self.ip, self.port, keyspace)

    def to_dict(self):
        return { "uuid" : self.uuid,
                 "ip" : self.ip,
                 "port" : self.port }

    def trace_back(self):
        """ Trace back peer to get a path to origin. This is used for debuggin only """
        trace = []
        peer = self
        i = 0
        while peer:
            trace.append(f"{peer.uuid:016b}")
            #trace.append(f"{peer.uuid:016b}")
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
