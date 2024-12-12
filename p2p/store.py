import hashlib
from typing import Optional
import time


class StoreItem():
    def __init__(self, uuid: int, value: str, ttl: Optional[int]=None) -> None:
        self.uuid = uuid
        self.value = value

        # Record time so we can delete record after <ttl> seconds
        self._t_created = time.time()
        self._ttl = ttl

    def is_expired(self):
        if self._ttl:
            return time.time() - self._t_created > self._ttl

    def __repr__(self):
        return f"{self.uuid} : {self.value}"


class Store():
    def __init__(self, keyspace: int) -> None:
        self._keyspace = keyspace

        # here we store {hashed_key : value} pairs
        self._store = []

    def __repr__(self):
        out = ["STORE:"]
        for i,item in enumerate(self._store):
            out.append(f"  {i}: {item}")
        return "\n".join(out)

    def _lookup(self, k: int) -> Optional[StoreItem]:
        for item in self._store:
            if item.uuid == k:
                return item

    def find_kv_in_range(self, start: int, end: int) -> list[dict]:
        """ This method is called from RouteTable().insert_peer() to check if the new peer's range
            includes a k:v UUID from store. If so, we need to notify the peer of this k:v
        """
        return [{"uuid" : item.uuid, "value" : item.value} for item in self._store if start < item.uuid < end]

    def remove_item(self, item: StoreItem):
        self._store.remove(item)

    def get_items(self):
        return self._store

    def get_hash(self, data: str):
        nbytes = int(self._keyspace/8)
        return int.from_bytes(hashlib.sha256(data.encode()).digest()[:nbytes])

    def put_by_uuid(self, k: int, v: str, ttl: Optional[int]=None):
        assert type(k) == int
        assert k >= 0 and k < 2**self._keyspace

        if item := self._lookup(k):
            item.value = v
        else:
            self._store.append(StoreItem(k, v, ttl))

    def put_by_str(self, k: str, v: str, ttl: Optional[int]=None):
        assert type(k) == str
        self.put_by_uuid(self.get_hash(k), v, ttl)

    def get_by_uuid(self, k: int):
        assert type(k) == int
        assert k >= 0 and k < 2**self._keyspace
        if item := self._lookup(k):
            return item.value

    def get_by_str(self, k: str):
        assert type(k) == str
        return self.get_by_uuid(self.get_hash(k))
