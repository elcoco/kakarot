from dataclasses import dataclass, field
from typing import Callable, Optional, Any
from enum import Enum
import socket
import threading
import time
import select
import json

from core.utils import debug, info, error


class BencDecodeError(Exception):
    pass

class BencEncodeError(Exception):
    pass

class MType(Enum):
    PING = 0
    UNDEFINED = 1


@dataclass
class BencStr():
    value: str


@dataclass
class BencInt():
    value: int


@dataclass
class Msg():
    ip: Optional[str] = None
    port: Optional[int] = None
    mtype: MType = MType.UNDEFINED
    data: list = field(default_factory=list)

    def __repr__(self):
        ...

    def _parse_int(self, data: str) -> tuple[int,int]:
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
        """ Parse string for list, syntax: l<item0><item1>e """
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

    def loads(self, data: str):
        """ Parse bencoded data into python native structures """
        pos = 0

        while pos < len(data):
            ret, size = self._parse(data[pos:])
            pos += size
            self.data.append(ret)

        print(json.dumps(self.data, indent=4))
        return self.data

    def dumps(self, data=None):
        """ Dump self.data as benc encoded string """

        string = ""

        if data == None:
            data = self.data

        for item in data:
            match item:
                case int():
                    string += f"i{item}e"
                case str():
                    string += f"{len(item)}:{item}"
                case list():
                    string += f"l{self.dumps(item)}e"
                case dict():
                    for k,v in item.items():
                        string += f"d{len(k)}:{k}{self.dumps([v])}"
                    string += "e"
                case _:
                    raise BencEncodeError(f"Failed to encode, unknown type: {type(item)}")

        return string






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
            js = msg.loads(data.decode())
            print(js)

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

