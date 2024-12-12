from typing import Optional

from p2p.peer import Peer
from p2p.routing import RouteTable
from p2p.network.utils import send_request
from p2p.network.message import PingMsg, StoreMsg, FindNodeMsg, FindValueMsg, ResponseMsg, ErrorMsg
from p2p.network.message import MsgError, MsgKey


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
        # List of uncontacted nodes
        self._list = []

        self._target_uuid = target_uuid

        # the max size of list, probably K
        self._limit = limit

        # Contacted peers
        self._contacted = []

        # Peers that did not respond end up here
        self._rejected = []

        self.closest_peer: Optional[Peer] = None

    def __repr__(self):
        out = []
        out.append(f"target: {self._target_uuid:016b}")
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
        """ Add peer and return wether this peer was closer to target_uuid than any previous peers """

        if self.is_in(self._list + self._contacted + self._rejected, peer):
            #error("shortlist", "add", f"skipping: {peer}")
            return

        self._list.append(peer)

        # update closest peer
        if not self.closest_peer:
            self.closest_peer = peer
            return True
        if peer.get_distance(self._target_uuid) < self.closest_peer.get_distance(self._target_uuid):
            self.closest_peer = peer
            return True

    def is_complete(self):
        return len(self._contacted) >= self._limit

    def get_results(self, limit=None):
        """ Return a list of K peers (or less if we couldn't find more) sorted by closeness """
        if not limit:
            limit = self._limit

        return sorted(self._contacted, key=lambda x: x.get_distance(self._target_uuid))[:limit]

    def get_uncontacted_peers(self, limit: Optional[int]=None):
        """ Return the next set of uncontacted peers from the shortlist.
            If limit == None, return all uncontacted peers """
        count = limit or self._limit
        return sorted(self._list, key=lambda x: x.get_distance(self._target_uuid))[:count]

    def print_results(self):
        print(f"FIND_PEER RESULTS ({self._target_uuid:016b}, {self._target_uuid})")
        for i,peer in enumerate(self.get_results()):
            print(f"{i:2} {' > '.join(peer.trace_back())}")


class CrawlerBaseClass():
    """ 
        Initiate an iterative node lookup
        The process:
        We keep a list (ShortList) of K nodes. These nodes are sorted by distance from the Key/UUID
        that we're looking for

        1. Calls find_{node|key} to current ALPHA nearest not already queried nodes,
           adding results to current nearest list of k nodes.
        2. If in step 1 a newer node is found than previously known, go back to step 1
        3. If in step 1 no newer node is found than previously known, call find_{node|key}
           to all K nearest nodes that we have not queried before.
           yet queried, 
        4. Repeat until nearest list has all been queried.
    """
    def __init__(self, k: int, alpha: int, table: RouteTable, origin: Peer, target_uuid: int):
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

        # Keep track of amount iterations and connections for debugging purposes
        self._round = 1
        self._connections = 1

        # Keep track of closest Peers inbetween iterations so we can detect if newer peers have been found
        self._prev_closest: Optional[Peer] = None

    def handle_peers(self, peers: list[Peer]):
        """ Must be implemented when subclassed. """

    def request_nodes_from_peer(self, peer: Peer):
        """ Must be implemented when subclassed. """

    def find(self, bootstrap_peers: Optional[list[Peer]]=None):

        # Add initial nodes to the shortlist
        if bootstrap_peers:
            for peer in bootstrap_peers:
                self.shortlist.add(peer)

        while self.shortlist.has_uncontacted_peers():
            self._round += 1

            # If previous round didn't return any newer peers than already seen, query
            # all uncontacted peers for this round
            if self._prev_closest == self.shortlist.closest_peer:
                peers = self.shortlist.get_uncontacted_peers()
            else:
                peers = self.shortlist.get_uncontacted_peers(self._alpha)

            self.handle_peers(peers)

            self._prev_closest = self.shortlist.closest_peer

        print(f"Finished in {self._round} rounds")
        return self.shortlist.get_results()
            

class NodeCrawler(CrawlerBaseClass):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def request_nodes_from_peer(self, peer: Peer):

        msg_req = FindNodeMsg(uuid=self._origin.uuid, ip=self._origin.ip, port=self._origin.port)
        msg_req.target_uuid = self.target_uuid
        new_peers = []

        if (msg_res := send_request(peer.ip, peer.port, msg_req)):
            assert type(msg_res) != MsgError, f"Find node received an error from: {peer}"
            self._table.insert_peer(peer, self._origin)

            # add received peers to new_peers if not already contacted
            new_peers = [Peer(x["uuid"], x["ip"], x["port"]) for x in msg_res.return_values[MsgKey.NODES]]
            for p in new_peers:
                p.parent = peer

        else:
            # Request failed, remove peer from our routing table (if it's there)
            self._table.remove_peer(peer, self._origin.uuid)

        return new_peers

    def handle_peers(self, peers: list[Peer]):
        for peer in peers:
            assert self._origin.uuid != peer.uuid, "Don't add ourself to shortlist"

            if new_peers := self.request_nodes_from_peer(peer):
                self.shortlist.set_contacted(peer)
                for p in new_peers:
                    self.shortlist.add(p)
            else:
                self.shortlist.set_rejected(peer)
















