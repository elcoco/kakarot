from typing import Optional
import random
import math


class RouteTable():
    def __init__(self, table_size: int, uuid: int) -> None:
        self._table_size = table_size
        self._route_table = [None] * table_size

        # The uuid of this node.
        # Since the routing table represents the distance from this node we need to calculate the offset
        self._uuid = uuid

    def __repr__(self):
        out = []
        out.append(f"ROUTE_TABLE({2**self._table_size})")
        out.append("INDEX  UUID  NODE")
        for i, n in enumerate(self._route_table):
            uuid = (self._uuid + 2**i) % 2**self._table_size
            out.append(f"{i:>5}   {uuid:>03}  {n}")
        return "\n".join(out)

    def get_distance(self, uuid: int):
        """ Calculate distance of this node to UUID """
        assert(uuid != self._uuid)

        # Node is ahead of us in the ring
        if uuid > self._uuid:
            return uuid - self._uuid

        # Node is behind us in the ring
        else:
            return uuid + (2**self._table_size) - self._uuid

    def distance_to_pos(self, distance: int):
        """ Convert distance to index of node in routing table """
        return int(math.log2(distance))

    def get_nearest_node(self, uuid: int):
        """ Return known node that's nearest to requested node """

        # Search in a ring only goes forward starting from the nodes own UUID.
        # So closest node to 35 would be at position 2**5=32
        # Calculate in which bit the node is stored that is closest to the node we're looking for
        distance = self.get_distance(uuid)
        pos = self.distance_to_pos(distance)
        return self._route_table[pos]

    def set(self, node: "Node"):
        ...

    def get(self, uuid: int):
        ...




class Node():
    def __init__(self, ip: str, port: int, net_size: int, uuid: Optional[int]=None) -> None:

        # size of network in bits
        self._net_size = net_size

        self._ip = ip
        self._port = port

        if uuid == None:
            self._uuid = self._get_uuid()
        else:
            self._uuid = uuid

        self._route_table = RouteTable(self._net_size, self._uuid)
        print(self._route_table)

        print(self._route_table.get_nearest_node(1))

    def __repr__(self):
        return f"NODE: {self._ip}:{self._port} => {self._uuid}"

    def _get_uuid(self):
        return random.randrange(0, (2**self._net_size)-1)



