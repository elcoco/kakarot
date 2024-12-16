import time
import socket

from core.utils import debug, info, error
from p2p.network.message import MsgError
from p2p.network.message import ResponseMsg
from p2p.network.bencoding import BencDecodeError


MSG_LENGTH = 1024

def send_request(ip: str, port: int, msg_req, timeout=5):
    """ Send a request message and receive response """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

        t_start = time.time()

        try:
            s.connect((ip, port))
        except ConnectionRefusedError as e:
            error("peer", "send_recv", f"{str(e)}, {ip}:{port}")
            return

        s.settimeout(timeout)

        msg_bin = str(msg_req.to_bencoding()).encode()
        bytes_sent = 0

        while bytes_sent < len(msg_bin):
            try:
                n = s.send(msg_bin)
            except BrokenPipeError as e:
                error("peer", "send_recv", str(e))
                return

            if n == 0:
                error("peer", "send_recv", f"connection broken")
                return
            bytes_sent += n

        data = b""

        try:
            while (chunk := s.recv(MSG_LENGTH)):
                data += chunk
        except TimeoutError:
            error("peer", "send_recv", "Connection timedout")
            return
        except ConnectionResetError as e:
            error("peer", "send_recv", str(e))
            return

        try:
            msg_res = ResponseMsg()
            msg_res.from_bencoding(data.decode())
            msg_res.response_time = time.time() - t_start
        except MsgError as e:
            error("peer", "send_recv", str(e))
            return
        except BencDecodeError as e:
            error("peer", "send_recv", str(e))
            return

        if msg_res.transaction_id != msg_req.transaction_id:
            error("peer", "send_recv", f"communication error, transaction_id error: {msg_req.transaction_id} != {msg_res.transaction_id}")
            return

    return msg_res
