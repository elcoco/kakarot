from dataclasses import dataclass, field
from typing import Callable, Optional, Any
import socket
import threading
import select
import json
import random
import string

from p2p.api_parsers import Bencoder, BencDecodeError
from core.utils import debug, info, error

ERROR_GENERIC  = 201  # the rest of the errors
ERROR_SERVER   = 202  # internal server error
ERROR_PROTOCOL = 203  # malformed packet, invalid arguments or bad token
ERROR_METHOD   = 204  # unexpected method, eg: from api we only reply to query messages

class MsgError(Exception): pass


class MsgBaseClass(Bencoder):
    def __init__(self, transaction_id: Optional[str]=None):
        self._data: dict = {}

        if transaction_id == None:
            self.transaction_id = "".join([random.choice(string.ascii_letters+string.digits) for _ in range(2)])
        else:
            self.transaction_id = transaction_id

    def __repr__(self):
        return json.dumps(self._data, indent=4)

    def validate(self):
        """ Needs to be implemented when subclassed """

    @property
    def transaction_id(self):
        return self._data["t"]

    @transaction_id.setter
    def transaction_id(self, transaction_id: str):
        self._data["t"] = transaction_id

    @property
    def msg_type(self):
        return self._data["y"]

    @msg_type.setter
    def msg_type(self, mtype: str):
        self._data["y"] = mtype

    def is_query(self):
        return self._data.get("y") == "q"

    def is_response(self):
        return self._data.get("y") == "r"

    def is_error(self):
        return self._data.get("y") == "e"

    def from_bencoding(self, data: str):
        self._data = self.loads(data)
        self.validate()

    def from_dict(self, data: dict):
        self._data = data
        self.validate()

    def to_bencoding(self):
        self.validate()
        return self.dumps(self._data)


class ResponseMsg(MsgBaseClass):
    def __init__(self, *args, uuid: Optional[int]=None, ip: Optional[str]=None, port: Optional[int]=None, **kwargs):
        MsgBaseClass.__init__(self, *args, **kwargs)

        # sender info
        self.uuid = uuid
        self.ip = ip
        self.port = port

        if all([uuid, ip, port]):
            self.set_sender_node(uuid, ip, port)

        self.msg_type = "r"

    @property
    def id(self):
        return self._data["a"]["id"]

    def set_sender_node(self, uuid: int, ip: str, port: int):
        if not "r" in self._data.keys():
            self._data["r"] = {}
        self._data["r"]["id"] = { "uuid": uuid,
                                  "ip":   ip,
                                  "port": port }
    @property
    def return_values(self):
        return self._data["r"]

    @return_values.setter
    def return_values(self, data: dict):
        self._data["r"] |= data

    def validate(self):
        if not self._data.get("t"):
            raise MsgError(f"Failed to validate message, message has no transaction id")
        if not self._data.get("y"):
            raise MsgError(f"Failed to validate message, message has no type information")
        if not self._data["y"] == "r":
            raise MsgError(f"Failed to validate message, message type is not response")
        if not self._data.get("r"):
            raise MsgError(f"Failed to validate response message, message has no response type information")
        if not self._data["r"].get("id"):
            raise MsgError(f"Failed to validate response message, id key not found in arguments")


class QueryMsgBaseClass(MsgBaseClass):
    def __init__(self, *args, uuid: Optional[int]=None, ip: Optional[str]=None, port: Optional[int]=None, **kwargs):
        MsgBaseClass.__init__(self, *args, **kwargs)

        # sender info
        self.uuid = uuid
        self.ip = ip
        self.port = port

        if all([uuid, ip, port]):
            self.set_sender_node(uuid, ip, port)

        self.msg_type = "q"

    @property
    def query_type(self):
        return self._data["q"]

    @query_type.setter
    def query_type(self, qtype: str):
        self._data["q"] = qtype

    @property
    def id(self):
        return self._data["a"]["id"]

    def set_sender_node(self, uuid: int, ip: str, port: int):
        if not "a" in self._data.keys():
            self._data["a"] = {}
        self._data["a"]["id"] = { "uuid": uuid,
                                  "ip":   ip,
                                  "port": port }

    def validate(self):
        print("validating:", self._data)
        if not self._data.get("t"):
            raise MsgError(f"Failed to validate message, message has no transaction id")
        if not self._data.get("y"):
            raise MsgError(f"Failed to validate message, message has no type information")
        if not self._data["y"] == "q":
            raise MsgError(f"Failed to validate message, message type is not query")
        if not self._data.get("q"):
            raise MsgError(f"Failed to validate query message, message has no query type information")
        if not self._data.get("a"):
            raise MsgError(f"Failed to validate query message, message has no arguments")
        if not self._data["a"].get("id"):
            raise MsgError(f"Failed to validate query message, id dictionary not found in arguments")
        if not self._data["a"]["id"].get("uuid"):
            raise MsgError(f"Failed to validate query message, missing uuid in id")
        if not self._data["a"]["id"].get("ip"):
            raise MsgError(f"Failed to validate query message, missing ip in id")
        if not self._data["a"]["id"].get("port"):
            raise MsgError(f"Failed to validate query message, missing port in id")

        match self._data["q"]:
            case "ping":
                # ping has no extra arguments
                ...
            case "store":
                ...
            case "find_node":
                if not self._data["a"].get("target"):
                    raise MsgError(f"Failed to validate find_node query message, target node not found in arguments")
            case "find_key":
                ...
            case _:
                raise MsgError(f"Failed to validate message, unknown query type: {self._data['y']}")


class PingMsg(QueryMsgBaseClass):
    def __init__(self, *args, **kwargs):
        QueryMsgBaseClass.__init__(self, *args, **kwargs)
        self.query_type = "ping"


class StoreMsg(QueryMsgBaseClass):
    def __init__(self, *args, **kwargs):
        QueryMsgBaseClass.__init__(self, *args, **kwargs)
        self.query_type = "store"


class FindNodeMsg(QueryMsgBaseClass):
    def __init__(self, *args, **kwargs):
        QueryMsgBaseClass.__init__(self, *args, **kwargs)
        self.query_type = "find_node"

    @property
    def target_node(self):
        return self._data["a"].get("target")

    def set_target_node(self, uuid: int, ip: str, port: int):
        self._data["a"]["target"] = { "uuid": uuid,
                                      "ip":   ip,
                                      "port": port }


class FindKeyMsg(QueryMsgBaseClass):
    def __init__(self, *args, **kwargs):
        QueryMsgBaseClass.__init__(self, *args, **kwargs)
        self.query_type = "find_key"


class ErrorMsg(MsgBaseClass):
    def __init__(self, *args, code: Optional[int]=None, msg: Optional[str]=None, **kwargs):
        MsgBaseClass.__init__(self, *args, **kwargs)
        self._error_code = code
        self._data["e"] = [None, None]

        if code:
            self.error_code = code
        if msg:
            self.error_msg = msg

        self.msg_type = "e"

    @property
    def error_code(self):
        return self._data["e"][0]

    @error_code.setter
    def error_code(self, code: int):
        self._data["e"][0] = code

    @property
    def error_msg(self):
        return self._data["e"][1]

    @error_msg.setter
    def error_msg(self, msg: str):
        self._data["e"][1] = msg

    def validate(self):
        if not self._data.get("t"):
            raise MsgError(f"Failed to validate message, message has no transaction id")
        if not self._data.get("y"):
            raise MsgError(f"Failed to validate message, message has no type information")
        if not self._data["y"] == "e":
            raise MsgError(f"Failed to validate message, message type is not error")
        if not self._data.get("e"):
            raise MsgError(f"Failed to validate error message, message has no error information")

        try:
            int(self._data.get("e")[0])   # error code
            self._data.get("e")[1]        # error message
        except IndexError:
            raise MsgError(f"Failed to validate error message, message has malformed error")
        except ValueError:
            raise MsgError(f"Failed to validate error message, message has malformed error")


class ConnThread(threading.Thread):
    thread_id = 0

    def __init__(self, conn, ip: str, port: int, callbacks: dict[str,Callable]):
        threading.Thread.__init__(self)
        self._conn = conn
        self._ip = ip
        self._port = port

        self._id = ConnThread.thread_id
        ConnThread.thread_id += 1

        # These callbacks are used to respond to incoming messages
        self._callbacks = callbacks

    def __repr__(self):
        return f"[{self._id}]{self._ip}:{self._port}"

    def send(self, res):
        self._conn.send(str(res).encode())

    def run(self):
        """ Receive message and try to parse it into a query message.
            Try to find the right callback for the message.
            All message types that are not query messages are considered errors """

        with self._conn:
            info("conn_thread", str(self), f"accepted")

            try:
                data = self._conn.recv(1024)
            except TimeoutError:
                error("conn_thread", str(self), f"timedout")
                return

            if not data:
                error("conn_thread", str(self), f"no data")
                return

            try:
                parsed = Bencoder().loads(data.decode())
            except BencDecodeError as e:
                self.send(ErrorMsg(code=ERROR_PROTOCOL, msg=str(e)))
                error("conn_thread", str(self), f"{e}")
                return

            print(json.dumps(parsed, indent=4))

            if parsed.get("y") == None:
                self.send(ErrorMsg(code=ERROR_PROTOCOL, msg="missing msg type"))
                error("conn_thread", str(self), "Missing message type")
                return

            if (qtype := parsed.get("q")) == None:
                self.send(ErrorMsg(code=ERROR_PROTOCOL, msg="missing query type"))
                error("conn_thread", str(self), "Missing query type")
                return

            match qtype:
                case "ping":
                    msg = PingMsg()
                case "store":
                    msg = StoreMsg()
                case "find_node":
                    msg = FindNodeMsg()
                case "find_key":
                    msg = FindKeyMsg()
                case _:
                    self.send(ErrorMsg(code=ERROR_METHOD, msg="unknown query type"))
                    error("conn_thread", str(self), "Unknown query type")
                    return

            try:
                msg.from_dict(parsed)
            except MsgError as e:
                self.send(ErrorMsg(code=ERROR_PROTOCOL, msg=str(e)))
                error("conn_thread", str(self), str(e))
                return

            self.send(self._callbacks[qtype](msg))

        info("conn_thread", "run", "disconnected")


class Api():
    def __init__(self, ip: str, port: int, callbacks: dict[str,Callable], timeout: int=5) -> None:
        self._port = port
        self._ip = ip
        self._timeout = timeout

        # These callbacks are used to respond to incoming messages
        self._callbacks = callbacks

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

                    t = ConnThread(conn, addr[0], addr[1], self._callbacks)
                    t.start()

        info("rest", "listen", f"closing api")

        # close all client connections
        for t in self._pool:
            t.join()

        s.close()
