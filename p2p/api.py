import socket
import threading
import select
import json
from typing import Callable, Optional, Any

from p2p.api_parsers import Bencoder, BencDecodeError
from core.utils import debug, info, error

from p2p.message import MsgKey, ErrCode, MsgType, QueryType
from p2p.message import PingMsg, StoreMsg, FindNodeMsg, FindKeyMsg, ResponseMsg, ErrorMsg
from p2p.message import MsgError


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
        return f"[{self._id}] {self._ip}:{self._port}"

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
                self.send(ErrorMsg(code=ErrCode.PROTOCOL, msg=str(e)).to_bencoding())
                error("conn_thread", str(self), f"{e}")
                return

            print(json.dumps(parsed, indent=4))

            if parsed.get(MsgKey.MSG_TYPE) == None:
                self.send(ErrorMsg(code=ErrCode.PROTOCOL, msg="missing msg type").to_bencoding())
                error("conn_thread", str(self), "Missing message type")
                return

            if (qtype := parsed.get(MsgType.QUERY)) == None:
                self.send(ErrorMsg(code=ErrCode.PROTOCOL, msg="missing query type").to_bencoding())
                error("conn_thread", str(self), "Missing query type")
                return

            match qtype:
                case QueryType.PING:
                    msg = PingMsg()
                case QueryType.STORE:
                    msg = StoreMsg()
                case QueryType.FIND_NODE:
                    msg = FindNodeMsg()
                case QueryType.FIND_KEY:
                    msg = FindKeyMsg()
                case _:
                    self.send(ErrorMsg(code=ErrCode.METHOD, msg="unknown query type").to_bencoding())
                    error("conn_thread", str(self), "Unknown query type")
                    return

            try:
                msg.from_dict(parsed)
            except MsgError as e:
                self.send(ErrorMsg(code=ErrCode.PROTOCOL, msg=str(e)).to_bencoding())
                error("conn_thread", str(self), str(e))
                return

            # TODO: needs to handle errors
            # Runs appropriate callback and sends returned message back to requesting node
            self.send(self._callbacks[qtype](msg).to_bencoding())

        info("conn_thread", "run", "disconnected")


class Api(threading.Thread):
    """ Api is a thread that listens for connections. When a peer wants to connect to us it will
        start a new thread. This thread will parse the message. If it is a query message it will
        execute the appropriate callback to handle it. """
    def __init__(self, ip: str, port: int, callbacks: dict[str,Callable], timeout: int=5, max_threads: int=10) -> None:
        threading.Thread.__init__(self)
        self._port = port
        self._ip = ip
        self._timeout = timeout

        # These callbacks are used to respond to incoming messages
        self._callbacks = callbacks

        self._pool = []
        self._stopped = False

        self._max_threads = max_threads

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

    def _listen(self, timeout=1):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            info("api", "listen", f"listening on {self._ip}:{self._port}")

            s.bind((self._ip, self._port))
            s.listen(1)

            read_list = [s]

            while not self._stopped:

                # Blocks until socket state changes. Will timeout after n seconds.
                # This way we effectively get non blocking sockets
                readable, writable, errored = select.select(read_list, [], [], timeout)

                for s in readable:
                    if (alive := self._check_pool()) >= self._max_threads:
                        error("api", "listen", f"denying connection, too many active connections: {alive}")
                        break

                    conn, addr = s.accept()
                    conn.settimeout(self._timeout)

                    t = ConnThread(conn, addr[0], addr[1], self._callbacks)
                    t.start()

        info("api", "listen", f"closing api")

        # close all client connections
        for t in self._pool:
            t.join()

        s.close()

    def stop(self):
        self._stopped = True

    def run(self):
        self._listen()
