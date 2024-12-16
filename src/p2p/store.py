import hashlib
from typing import Optional
import time


class StoreItem():
    def __init__(self, uuid: int, value: str, republish_interval: Optional[int]=None, ttl: Optional[int]=None) -> None:
        self.uuid = uuid
        self._value = value

        # We don't republish records that have been updated within the last <seconds>.
        # See optimizations in official spec (kademlia paper, section: 2.5)
        self._t_last_updated: float = time.time()
        self._republish_interval = republish_interval

        # Record time so we can delete record after <ttl> seconds
        self._t_created = time.time()
        self._ttl = ttl

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value: str):
        self._value = value
        self._t_last_updated = time.time()

    def needs_republish(self):
        if not self._republish_interval:
            return True
        return time.time() - self._t_last_updated > self._republish_interval

    def is_expired(self):
        if self._ttl:
            return time.time() - self._t_created > self._ttl

    def __repr__(self):
        return f"{self.uuid} : {self.value}"


class Store():
    def __init__(self, keyspace: int, republish_interval: int, ttl: int) -> None:
        self._keyspace = keyspace

        # here we store {hashed_key : value} pairs
        self._store: list[StoreItem] = []

        self._ttl = ttl
        self._republish_interval = republish_interval

    def __repr__(self):
        out = ["STORE:"]
        for i,item in enumerate(self._store):
            out.append(f"  {i}: {item}")
        return "\n".join(out)

    def _lookup(self, k: int) -> Optional[StoreItem]:
        for item in self._store:
            if item.uuid == k:
                return item

    def find_kv_in_range(self, start: int, end: int) -> list[StoreItem]:
        """ This method is called from RouteTable().insert_peer() to check if the new peer's range
            includes a k:v UUID from store. If so, we need to notify the peer of this k:v
        """
        return [item for item in self._store if start < item.uuid < end]

    def remove_item(self, item: StoreItem):
        self._store.remove(item)

    def get_items(self):
        return self._store

    def put(self, uuid: int, v: str):
        assert type(uuid) == int
        assert uuid >= 0 and uuid < 2**self._keyspace

        if item := self._lookup(uuid):
            item.value = v
        else:
            self._store.append(StoreItem(uuid, v, self._republish_interval, self._ttl))

    def get(self, uuid: int):
        assert type(uuid) == int
        assert uuid >= 0 and uuid < 2**self._keyspace
        if item := self._lookup(uuid):
            return item.value
