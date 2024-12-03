from dataclasses import dataclass, field
from typing import Callable, Optional, Any
from enum import Enum
import socket
import threading
import time
import select
import json
import random
import string

from core.utils import debug, info, error


class BencDecodeError(Exception): pass
class BencEncodeError(Exception): pass


class QueryType(Enum):
    PING = 0            # probe node to see if it is online
    STORE = 1           # instruct node to store key:value pair
    FIND_PEER = 2       # returns an (IP address, UDP port, Node ID) tuple for each of the k nodes closest to the target id
    FIND_VALUE = 3
    UNDEFINED = 4

class MsgType(Enum):
    QUERY = 0            # probe node to see if it is online
    RESPONSE = 1           # instruct node to store key:value pair
    ERROR = 2       # returns an (IP address, UDP port, Node ID) tuple for each of the k nodes closest to the target id
    UNDEFINED = 3

msg_type_by_str = { "q" : MsgType.QUERY,
                    "r" : MsgType.RESPONSE,
                    "e" : MsgType.ERROR }

msg_type_by_enum = { MsgType.QUERY:    "q",
                     MsgType.RESPONSE: "r",
                     MsgType.ERROR:    "e" }

query_type_by_str = { "ping" :       QueryType.PING,
                      "store" :      QueryType.STORE,
                      "find_peer" :  QueryType.FIND_PEER,
                      "find_value" : QueryType.FIND_VALUE }

query_type_by_enum = { QueryType.PING:       "ping",
                       QueryType.STORE:      "store",
                       QueryType.FIND_PEER:  "find_peer",
                       QueryType.FIND_VALUE: "find_value" }

@dataclass
class Msg():
    ip: Optional[str] = None
    port: Optional[int] = None
    data: dict = field(default_factory=dict)

    # Bittorrent RPC protocol: https://www.bittorrent.org/beps/bep_0005.html
    # The transaction id (2 bytes) is created by querying node and echoed by responding node
    # With this nonce we can assure that the response belongs to the query
    # {"t": "<transaction_id>"}
    # Bencode: 1:t2:<transaction_id>
    transaction_id: Optional[str] = None

    # Messages must be be one of:
    #   q = query
    #   r = response
    #   e = error
    # {"y": "q|r|e"}
    # Bencode: 1:y1:q
    msg_type: MsgType = MsgType.UNDEFINED

    query_type: QueryType = QueryType.UNDEFINED

    def set_msg_type(self, mtype: MsgType):
        self.data["y"] = mtype

    def _parse_int(self, data: str) -> tuple[int,int]:
        """ Bencoding int format: i<int>e """
        out = ""
        for i, c in enumerate(data):
            if i == 0 and c == "i":
                continue
            if i == 1 and c == "-":
                out += c
            elif c.isnumeric():
                out += c
            elif c == "e":
                info("msg", "parse_int", f"found: {out}")
                return int(out), i+1
            else:
                raise BencDecodeError(f"Failed to parse int, illegal char ({c}): {data}")
        raise BencDecodeError(f"Failed to parse int, no delimiter found: {data}")

    def _parse_byte_str(self, data: str) -> tuple[str,int]:
        """ Bencoding byte string format: <size>:<string> """
        size_str = ""
        i = 0

        for i, c in enumerate(data, 1):
            if c.isnumeric():
                size_str += c
            elif c == ":":
                break
            else:
                raise BencDecodeError(f"Failed to parse byte string size, illegal char ({c}): {data}")

        size = int(size_str)
        if len(data[i:]) < size:
            raise BencDecodeError(f"Failed to parse byte string, not enough data for size: {size}: {data}")

        total_size = i + size
        data = data[i:total_size]
        info("msg", "parse_str", f"found: [{size}] {data}")
        return data, total_size

    def _parse_dict(self, data: str) -> tuple[dict,int]:
        """ Bencoding dict format: d<key><value><key>...e """
        out = {}
        lst = []
        pos = 1

        if data[0] != "d":
            raise BencDecodeError(f"Failed to parse dict, malformed: {data}")

        # Recursively parse all items in list
        while pos < len(data):
            if data[pos] ==  "e":
                if len(lst) % 2 != 0:
                    raise BencDecodeError(f"Failed to parse dict, amount items must be even")

                for i in range(0, len(lst), 2):
                    if type(lst[i]) != str:
                        raise BencDecodeError(f"Failed to parse dict, key must be a string: {lst[i]}")

                    out[lst[i]] = lst[i+1]

                info("msg", "parse_dict", f"found: {out}")
                return out, pos+1

            res, size = self._parse(data[pos:])
            pos += size
            lst.append(res)

        raise BencDecodeError(f"Failed to parse dict, no delimiter found: {data}")

    def _parse_list(self, data: str) -> tuple[list,int]:
        """ Bencoding list format: l<item0><item1>...e """
        out = []
        pos = 1

        if data[0] != "l":
            raise BencDecodeError(f"Failed to parse list, malformed: {data}")

        # Recursively parse all items in list
        while pos < len(data):
            if data[pos] ==  "e":
                info("msg", "parse_list", f"found: {out}")
                return out, pos+1

            res, size = self._parse(data[pos:])
            pos += size
            out.append(res)

        raise BencDecodeError(f"Failed to parse list, no delimiter found: {data}")

    def _parse(self, data: str) -> tuple[Any, int]:
        """ Parse string, method is used for recursion """
        pos = 0
        match data[pos]:
            case "i":
                res, size = self._parse_int(data[pos:])
                return res, pos + size
            case c if c.isnumeric():
                res, size = self._parse_byte_str(data[pos:])
                return res, pos + size
            case "l":
                res, size = self._parse_list(data[pos:])
                return res, pos + size
            case "d":
                res, size = self._parse_dict(data[pos:])
                return res, pos + size
            case _:
                raise BencDecodeError(f"Failed to parse: {data[pos:]}")

    def _parse_query_type(self, qtype: str):
        match qtype:
            case "ping":
                return QueryType.PING
            case "store":
                return QueryType.STORE
            case "find_peer":
                return QueryType.FIND_PEER
            case "find_value":
                return QueryType.FIND_VALUE
            case _:
                raise BencDecodeError("Message has unknown query type")

    def _parse_msg_type(self, mtype: str):
        match mtype:
            case "q":
                return MsgType.QUERY
            case "r":
                return MsgType.RESPONSE
            case "e":
                return MsgType.ERROR
            case _:
                raise BencDecodeError("Message has unknown message type")

    def loads(self, data: str):
        """ Parse bencoded string into python native data structures """
        pos = 0

        while pos < len(data):
            ret, size = self._parse(data[pos:])
            pos += size

            assert(type(ret) == dict)
            self.data |= ret

        print(json.dumps(self.data, indent=4))

        # Parse some mandatory keys
        if "y" not in self.data.keys():
            raise BencDecodeError("Message has no message type specified")
        self.msg_type = msg_type_by_str[self.data["y"]]
        del self.data["y"]

        if "t" not in self.data.keys():
            raise BencDecodeError("Message has no message transaction id")
        self.transaction_id = self.data["t"]
        del self.data["t"]

        if "q" in self.data.keys():
            self.query_type = query_type_by_str[self.data["q"]]
            del self.data["q"]

        return self.data

    def _msg_type_to_str(self, mtype: MsgType):
        match mtype:
            case MsgType.QUERY:
                return "q"
            case MsgType.RESPONSE:
                return "r"
            case MsgType.ERROR:
                return "e"
            case _:
                raise BencDecodeError("Failed to encode msg, unknown message type")

    def _query_type_to_str(self, mtype: MsgType):
        match mtype:
            case QueryType.PING:
                return "ping"
            case QueryType.STORE:
                return "store"
            case QueryType.FIND_PEER:
                return "find_peer"
            case QueryType.FIND_VALUE:
                return "find_value"
            case _:
                raise BencDecodeError("Failed to encode msg, unknown message type")

    def dumps(self, data=None):
        """ Dump self.data as benc encoded string """

        out = ""

        if data == None:
            data = self.data

            transaction_id = self.transaction_id
            if transaction_id == None:
                transaction_id = "".join([random.choice(string.ascii_letters) for _ in range(2)])
            data["t"] = transaction_id

            if self.msg_type == MsgType.UNDEFINED:
                raise BencDecodeError("No message type specified")
            data["y"] = msg_type_by_enum[self.msg_type]

            if self.query_type != QueryType.UNDEFINED:
                data["q"] = query_type_by_enum[self.query_type]

            # TODO for some reason this is not encoded properly
            print(data)

        for k,v in data.items():
            out += f"{len(k)}:{k}"
            match v:
                case int():
                    out += f"i{v}e"
                case str():
                    out += f"{len(v)}:{v}"
                case list():
                    out += f"l{self.dumps(v)}e"
                case dict():
                    for k2,v2 in v.items():
                        out += f"d{len(k2)}:{k2}{self.dumps([v2])}"
                    out += "e"
                case _:
                    raise BencEncodeError(f"Failed to encode, unknown type: {type(item)}")

        return out




class ConnThread(threading.Thread):
    thread_id = 0

    def __init__(self, conn, ip: str, port: int):
        threading.Thread.__init__(self)
        self._conn = conn
        self._ip = ip
        self._port = port

        self._id = ConnThread.thread_id
        ConnThread.thread_id += 1

    def __repr__(self):
        return f"[{self._id}]{self._ip}:{self._port}"

    def send(self, res: Msg):
        self._conn.send(str(res).encode())
        
    def run(self):
        with self._conn:
            info("conn_thread", str(self), f"accepted")

            try:
                data = self._conn.recv(1024)
            except TimeoutError:
                error("conn_thread", str(self), f"timedout")
                self._conn.close()
                return

            if not data:
                error("conn_thread", str(self), f"no data")
                return

            msg = Msg()
            try:
                js = msg.loads(data.decode())
                print(js)
                print(msg.dumps())
            except BencDecodeError as e:
                error("conn_thread", str(self), f"{e}")

        info("conn_thread", "run", "disconnected")


class Api():
    def __init__(self, ip: str, port: int, timeout: int=5) -> None:
        self._port = port
        self._ip = ip
        self._timeout = timeout

        self._pool = []
        self._stopped = False

    def _get_ip(self):
        return socket.gethostbyname(socket.gethostname())

    def _check_pool(self) -> int:
        """ Remove stopped threads from pool and return amount of alive connections """
        alive = 0
        for t in self._pool.copy():
            if not t.is_alive():
                t.join()
                self._pool.remove(t)
            else:
                alive += 1
        return alive

    def stop(self):
        self._stopped = True

    def listen(self):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            info("api", "listen", f"listening on {self._ip}:{self._port}")

            #s.setblocking(False)
            s.bind((self._ip, self._port))
            s.listen(1)

            read_list = [s]

            while not self._stopped:

                # Blocks until socket state changes. This way we effectively get non blocking sockets
                readable, writable, errored = select.select(read_list, [], [])

                for s in readable:
                    alive = self._check_pool()

                    info("rest", "listen", f"waiting for new connection")

                    conn, addr = s.accept()
                    conn.settimeout(self._timeout)

                    t = ConnThread(conn, addr[0], addr[1])
                    t.start()

        info("rest", "listen", f"closing api")

        # close all client connections
        for t in self._pool:
            t.join()

        s.close()

