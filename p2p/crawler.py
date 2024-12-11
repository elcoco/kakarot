from typing import Optional

from p2p.peer import Peer


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

        self._closest_peer: Optional[Peer] = None

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
        if not self._closest_peer:
            self._closest_peer = peer
            return True
        if peer.get_distance(self._target_uuid) < self._closest_peer.get_distance(self._target_uuid):
            self._closest_peer = peer
            return True

    def is_complete(self):
        return len(self._contacted) >= self._limit

    def get_results(self, limit=None):
        """ Return a list of K peers (or less if we couldn't find more) sorted by closeness """
        if not limit:
            limit = self._limit

        return sorted(self._contacted, key=lambda x: x.get_distance(self._target_uuid))[:limit]

    def get_peers(self, limit: int):
        """ Return the next set of peers from the shortlist """
        return sorted(self._list, key=lambda x: x.get_distance(self._target_uuid))[:limit]

    def print_results(self):
        print(f"FIND_PEER RESULTS ({self._target_uuid:016b}, {self._target_uuid})")
        for i,peer in enumerate(self.get_results()):
            print(f"{i:2} {' > '.join(peer.trace_back())}")
