from dataclasses import dataclass, field
from typing import Callable, Optional
from enum import Enum
import socket
import threading
import time
import json

from core.utils import debug, info, error


class RestEndpointNotFoundError(Exception):
    pass


class HttpMethod(Enum):
    GET = 0
    POST = 1
    NOT_IMPLEMENTED = 2


@dataclass
class RestEndpoint():
    resource: str
    method: HttpMethod
    callback: Callable

    def __repr__(self):
        return f"{self.method}: {self.resource}"


class HttpRequest():
    def __init__(self, req_data):
        self._req_data = req_data
        self.headers = {}
        self._parse()

    def __repr__(self):
        """ Return string just for debugging """
        out = []
        out.append(f"method:   {self.method}")
        out.append(f"url:      {self.url}")
        out.append(f"protocol: {self.protocol}")
        out.append(f"headers:")

        for k,v in self.headers.items():
            out.append(f"  {k}: {v}")
        out.append(f"data:     {self.data}")

        return "\n".join(out)

    def _parse(self):
        """ Parse request into something usable """
        req, rest = self._req_data.split("\r\n", 1)

        method, self.url, self.protocol = req.split()

        if method == "GET":
            self.method = HttpMethod.GET
        elif method == "POST":
            self.method = HttpMethod.POST
        else:
            self.method = HttpMethod.NOT_IMPLEMENTED

        headers, self.data = rest.split("\r\n\r\n", 1)

        for header in headers.split("\r\n"):
            k, v = header.split(": ", 1)
            self.headers[k] = v


@dataclass
class HttpResponse():
    data: str|dict
    status_code: int = 200
    protocol: str = "HTTP/1.1"
    headers: dict = field(default_factory=dict)

    def __repr__(self):
        """ Return string that can be sent to requesting node """
        out = []
        out.append(f"{self.protocol} {self.status_code}\r\n")
        for k,v in self.headers.items():
            out.append(f"{k}: {v}\r\n")

        if type(self.data) == dict:
            out.append("Content-Type: application/json\r\n\r\n")
            out.append(f"{json.dumps(self.data)}\r\n")
        else:
            out.append("Content-Type: text/plain\r\n\r\n")
            out.append(f"{self.data}\r\n")

        return "".join(out)


class ConnThread(threading.Thread):
    thread_id = 0

    def __init__(self, conn, ip: str, port: int, endpoints: list[RestEndpoint]):
        threading.Thread.__init__(self)

        self._conn = conn
        self._ip = ip
        self._port = port
        self._endpoints = endpoints

        self._stopped = False
        self._id = ConnThread.thread_id
        ConnThread.thread_id += 1

    def is_stopped(self):
        return self._stopped

    def __repr__(self):
        return f"CONN_THREAD: {self._id} {self._ip}:{self._port}"

    def stop(self):
        self._stopped = True

    def find_endpoint(self, req: HttpRequest):
        for ep in self._endpoints:
            if ep.method == req.method and ep.resource == req.url:
                return ep

    def send(self, res: HttpResponse):
        self._conn.send(str(res).encode())

    def run(self):
        with self._conn:
            info("conn_thread", "run", f"accepted: {self}")

            try:
                data = self._conn.recv(1024)
            except TimeoutError:
                error("conn_thread", "run", f"connection timedout")
                return

            if not data:
                error("conn_thread", "run", f"no data")
                return

            req = HttpRequest(data.decode())
            print(req)

            if ep := self.find_endpoint(req):
                info("conn_thread", "run", f"found endpoint: {ep}")
                res = ep.callback(self._ip, self._port, req)
                self.send(res)
            else:
                self.send(HttpResponse({"error_msg":"Resource not found!"}, status_code=401))

        info("conn_thread", "run", "disconnected")


class Rest():
    def __init__(self, port: int) -> None:
        self._ip = self._get_ip()
        self._port = port
        self._endpoints = []

        self._stopped = False
        self._pool: list[ConnThread] = []

    def add_endpoint(self, endpoint: RestEndpoint):
        self._endpoints.append(endpoint)

    def _get_ip(self):
        return socket.gethostbyname(socket.gethostname())

    def _check_pool(self):
        """ Remove stopped threads from pool """
        for t in self._pool.copy():
            if t.is_stopped():
                t.join()
                self._pool.remove(t)

    def stop(self):
        self._stopped = True

        info("rest", "stop", f"waiting for threads to join")
        for t in self._pool:
            t.join()

    def run(self, timeout: int=5):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            info("rest", "run", f"starting RESTfull server on {self._ip}:{self._port}")

            s.bind(('', self._port))
            s.listen(1)

            while not self._stopped:
                self._check_pool()

                info("rest", "run", f"waiting for new connection")

                conn, addr = s.accept()
                conn.settimeout(timeout)

                t = ConnThread(conn, addr[0], addr[1], self._endpoints)
                self._pool.append(t)
                t.start()

        s.close()
