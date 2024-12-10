import hashlib
from typing import Optional


class StoreItem():
    def __init__(self, uuid: int, value: str, t_expire_sec: Optional[int]=None) -> None:
        self.uuid = uuid
        self.value = value
        self._t_expire_sec = t_expire_sec

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

    def get_hash(self, data: str):
        nbytes = int(self._keyspace/8)
        return int.from_bytes(hashlib.sha256(data.encode()).digest()[:nbytes])

    def put_by_uuid(self, k: int, v: str):
        assert type(k) == int
        assert k >= 0 and k < 2**self._keyspace

        if item := self._lookup(k):
            item.value = v
        else:
            self._store.append(StoreItem(k, v))

    def put_by_str(self, k: str, v: str):
        assert type(k) == str
        self.put_by_uuid(self.get_hash(k), v)

    def get_by_uuid(self, k: int):
        assert type(k) == int
        assert k >= 0 and k < 2**self._keyspace
        if item := self._lookup(k):
            return item.value

    def get_by_str(self, k: str):
        assert type(k) == str
        return self.get_by_uuid(self.get_hash(k))
