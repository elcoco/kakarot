from typing import Optional

from core.utils import debug, info, error

from p2p.peer import Peer
from p2p.routing import RouteTable
from p2p.network.utils import send_request
from p2p.network.message import PingMsg, StoreMsg, FindNodeMsg, FindValueMsg, ResponseMsg, ErrorMsg
from p2p.network.message import MsgError, MsgKey


class ShortList():
    def __init__(self, target_uuid: int, limit: int):
        """
        source: https://xlattice.sourceforge.net/components/protocol/kademlia/specs.html#FIND_VALUE
        """
        # List of uncontacted + contacted peers
        self._nearest = []

        # Contacted peers
        self._contacted = []

        # Peers that did not respond end up here
        self._rejected = []

        self._target_uuid = target_uuid

        # the max size of list, probably K
        self._limit = limit

        self.closest_peer: Optional[Peer] = None

    def __repr__(self):
        out = []
        out.append(f"target: {self._target_uuid:016b}")
        out.append(f"Shortlist:")
        for i,p in enumerate(self.get_uncontacted_peers()):
            out.append(f"  {i:2}: {p}")
        out.append(f"contacted:")
        for i,p in enumerate(self.get_results()):
            out.append(f"  {i:2}: {p}")
        return "\n".join(out)

    def set_contacted(self, peer: Peer):
        self._contacted.append(peer)

    def set_rejected(self, peer: Peer):
        """ Peer did not respond, let's put it on a list so we can contact it again later if needed """
        try:
            self._nearest.remove(peer)
        except ValueError:
            pass
        self._rejected.append(peer)

    def is_contacted(self, peer: Peer):
        for p in self._contacted:
            if p.uuid == peer.uuid:
                return p

    def is_in(self, src_list: list[Peer], peer: Peer):
        """ Check if peer is in src_list. We cannot just use the "in" keyword because
            that would compare peers by reference instead by attributes. """
        for p in src_list:
            if p.uuid == peer.uuid and p.ip == peer.ip and p.port == peer.port:
                return peer

    def add(self, peer: Peer):
        """ Add peer and return wether this peer was closer to target_uuid than any previous peers """

        if self.is_in(self._nearest + self._rejected, peer):
            #error("shortlist", "add", f"skipping: {peer}")
            return

        # Keep list sorted by nearest
        for i, p in enumerate(self._nearest):
            if peer.get_distance(self._target_uuid) < p.get_distance(self._target_uuid):
                self._nearest.insert(i, peer)
                break

        self._nearest.append(peer)

        # update closest peer
        if not self.closest_peer:
            self.closest_peer = peer
            return True
        if peer.get_distance(self._target_uuid) < self.closest_peer.get_distance(self._target_uuid):
            self.closest_peer = peer
            return True

    def get_results(self, limit=None):
        """ Return a list of K peers (or less if we couldn't find more) sorted by closeness """
        return [p for p in self._nearest[:self._limit] if self.is_contacted(p)][:limit]

    def get_nearest_peers(self, limit: Optional[int]=None):
        """ Return the next set of uncontacted peers from the shortlist.
            If limit == None, return uncontacted peers, in top K peers ranked by closeness """
        return [p for p in self._nearest[:self._limit]][:limit]

    def get_uncontacted_peers(self, limit: Optional[int]=None):
        """ Return the next set of uncontacted peers from the shortlist.
            If limit == None, return uncontacted peers, in top K peers ranked by closeness """
        return [p for p in self._nearest[:self._limit] if not self.is_contacted(p)][:limit]

    def get_contacted_peers(self, limit: Optional[int]=None):
        """ Return the next set of uncontacted peers from the shortlist.
            If limit == None, return uncontacted peers, in top K peers ranked by closeness """
        return [p for p in self._nearest[:self._limit] if self.is_contacted(p)][:limit]

    def has_uncontacted_peers(self):
        """ Check if the top K peers have all been contacted """
        return self.get_uncontacted_peers()

    def print_results(self):
        print(f"FIND_PEER RESULTS ({self._target_uuid:016b}, {self._target_uuid})")
        for i,peer in enumerate(self.get_results()):
            print(f"{i:2} {' > '.join(peer.trace_back())}")


class CrawlerBaseClass():
    """ 
        Initiate an iterative node lookup
        The process:

        All these operations are done on the top K nodes (contacted and non-contacted), sorted by distance
        from target key/UUID called NEAREST_LST.

        while top K nearest nodes contain uncontacted nodes, do:

            1.  Call find_{node|key} to current ALPHA nodes from NEAREST_LST, adding results to NEAREST_LST
            1a. If in step 1 a newer node is found than previously known, go back to step 1
            1b. If in step 1 no newer node is found than previously known, call find_{node|key} to all remaining
                uncontacted nodes from top K nearest nodes in NEAREST_LST
            4. Repeat until all uncontacted nodes in top K nearest nodes from NEAREST_LST are queried
    """
    def __init__(self, k: int, alpha: int, table: RouteTable, origin: Peer, bootstrap_peers: list[Peer], target_uuid: int):
        self._k = k
        self._alpha = alpha
        self.target_uuid = target_uuid

        # We need access to the routing table so we can newly discovered peers to it
        self._table = table

        # Create shortlist object and add ourself to the rejected list so we don't insert
        # ourselves in the list
        self._origin = origin
        self.shortlist = ShortList(target_uuid, k)
        self.shortlist.set_rejected(origin)

        # Add initial nodes to the shortlist
        for peer in bootstrap_peers:
            self.shortlist.add(peer)

        # Keep track of amount iterations and connections for debugging purposes
        self._round = 1
        self._connections = 1

        # Keep track of closest Peers inbetween iterations so we can detect if newer peers have been found
        self._prev_closest: Optional[Peer] = None

    def handle_peers(self, peers: list[Peer]):
        """ Must be implemented when subclassed. """

    def _find(self):
        self._round += 1

        # If previous round didn't return any newer peers than already seen, query
        # all remaining uncontacted peers for this round
        if self._prev_closest == self.shortlist.closest_peer:
            peers = self.shortlist.get_uncontacted_peers()
        else:
            peers = self.shortlist.get_uncontacted_peers(self._alpha)

        self._prev_closest = self.shortlist.closest_peer
        return peers
            

class NodeCrawler(CrawlerBaseClass):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def request_nodes_from_peer(self, peer: Peer):
        msg_req = FindNodeMsg(uuid=self._origin.uuid, ip=self._origin.ip, port=self._origin.port)
        msg_req.target_uuid = self.target_uuid
        new_peers = []
        self._connections += 1

        if (msg_res := send_request(peer.ip, peer.port, msg_req)):
            assert type(msg_res) != MsgError, f"Find node received an error from: {peer}"
            self._table.insert_peer(peer, self._origin)
            self.shortlist.set_contacted(peer)

            # add received peers to new_peers if not already contacted
            new_peers = [Peer(x["uuid"], x["ip"], x["port"]) for x in msg_res.return_values[MsgKey.NODES]]
            for p in new_peers:
                p.parent = peer

        else:
            # Request failed, remove peer from our routing table (if it's there)
            self._table.remove_peer(peer, self._origin.uuid)
            self.shortlist.set_rejected(peer)

        return new_peers

    def handle_peers(self, peers: list[Peer]):
        for peer in peers:
            assert self._origin.uuid != peer.uuid, "Don't add ourself to shortlist"

            if new_peers := self.request_nodes_from_peer(peer):
                for p in new_peers:
                    self.shortlist.add(p)

    def find(self):

        while self.shortlist.has_uncontacted_peers():
            peers = self._find()
            self.handle_peers(peers)

        print(f"Finished in {self._round} rounds, connections: {self._connections}")
        return self.shortlist.get_results()


class ValueCrawler(CrawlerBaseClass):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def request_value_from_peer(self, peer: Peer) -> str|list[Peer]:
        msg_req = FindValueMsg(uuid=self._origin.uuid, ip=self._origin.ip, port=self._origin.port)
        msg_req.key = self.target_uuid
        self._connections += 1

        if (msg_res := send_request(peer.ip, peer.port, msg_req)):
            assert type(msg_res) != MsgError, f"Find node received an error from: {peer}"
            self._table.insert_peer(peer, self._origin)
            self.shortlist.set_contacted(peer)

            # Check if peer returned the value we were looking for or a list of closest peers
            if value := msg_res.return_values.get(MsgKey.VALUE):
                return value
            else:
                new_peers = [Peer(x["uuid"], x["ip"], x["port"]) for x in msg_res.return_values[MsgKey.NODES]]
                for p in new_peers:
                    p.parent = peer
                return new_peers

        else:
            # Request failed, remove peer from our routing table (if it's there)
            self._table.remove_peer(peer, self._origin.uuid)
            self.shortlist.set_rejected(peer)

        return []

    def handle_peers(self, peers: list[Peer]):
        for peer in peers:
            assert self._origin.uuid != peer.uuid, "Don't add ourself to shortlist"

            if result := self.request_value_from_peer(peer):

                # Check if method returned list with new peers or the value we were searching for
                if not isinstance(result, list):
                    info("node", "find_value", f"Found value: '{result}' @ {peer}")
                    return result

                for p in result:
                    self.shortlist.add(p)

    def find(self):
        while self.shortlist.has_uncontacted_peers():
            peers = self._find()
            if value := self.handle_peers(peers):
                print(f"Finished in {self._round} rounds, connections: {self._connections}")
                return value

        print(f"Failed to find value in {self._round} rounds, connections: {self._connections}")
