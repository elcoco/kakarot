from typing import Optional
import random
import math
import json
import requests
from dataclasses import dataclass
import time
from core.rest import HttpRequest, Rest, RestEndpoint, HttpMethod, HttpResponse


@dataclass
class Peer():
    ip: str
    port: int

    def __repr__(self):
        return f"PEER: {self.ip}:{self.port}"


class RouteTable():
    def __init__(self, table_size: int, uuid: int) -> None:
        self._table_size = table_size
        self._table: list[Optional[Peer]] = [None] * table_size

        # The uuid of this node.
        # Since the routing table represents the distance from this node we need to calculate the offset
        self._uuid = uuid

    def __repr__(self):
        out = [(f"ROUTE_TABLE ({2**self._table_size})")]

        uuids = ["UUID"]
        uuids += [str((self._uuid + 2**i) % 2**self._table_size) for i in range(0, self._table_size)]
        uuids_rjust = max(len(x) for x in uuids)
        uuids = [x.rjust(uuids_rjust) for x in uuids]


        ids = ["INDEX"]
        ids += [str(x) for x in range(self._table_size)]
        i_rjust = max(len(x) for x in ids)
        ids = [x.rjust(i_rjust) for x in ids]

        peers = ["PEERS"]
        peers += self._table

        for uuid, id, peer in zip(uuids, ids, peers):
            out.append(f"{id}  {uuid}  {peer}")

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

    def get_nearest_node(self, uuid: int) -> Optional[Peer]:
        """ Return known node that's nearest to requested node """

        # Search in a ring only goes forward starting from the nodes own UUID.
        # So closest node to 35 would be at position 2**5=32
        # Calculate in which bit the node is stored that is closest to the node we're looking for
        distance = self.get_distance(uuid)
        pos = self.distance_to_pos(distance)
        return self._table[pos]



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

        self._is_stopped = False

    def __repr__(self):
        return f"NODE: {self._ip}:{self._port} => {self._uuid}"

    def _get_uuid(self):
        return random.randrange(0, (2**self._net_size)-1)

    def stop(self):
        self._is_stopped = True

    def http_get(self, url: str, timeout: int=5):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(e)
            return
        except requests.ConnectionError as e:
            print(e)
            return
        except requests.Timeout as e:
            print(e)
            return

        return r.json()

    def search_peer(self, peer: Peer, uuid: int, timeout=3):
        """ Contact peer to ask for route to uuid """
        if js := self.http_get(f"{peer.ip}:{peer.port}/find_peer/{uuid}"):
            print(f"response: {peer}")
            print(json.dumps(js, indent=4))
        else:
            print(f"Failed to find peer: {peer}")

    def pong_cb(self, ip: str, port: int, req: HttpRequest):
        return HttpResponse({"msg": "pong!"})

    def run(self):

        rest = Rest(self._port)

        pong = RestEndpoint("/test/ping", HttpMethod.GET, self.pong_cb)
        rest.add_endpoint(pong)

        try:
            rest.run()
        except KeyboardInterrupt:
            rest.stop()
