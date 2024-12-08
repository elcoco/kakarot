from dataclasses import dataclass, field
from typing import Callable, Optional, Any
from enum import Enum, StrEnum, IntEnum
import random
import string
import json

from core.utils import debug, info, error
from p2p.bencode import Bencoder


class MsgError(Exception): pass


class ErrCode(IntEnum):
    GENERIC   = 201  # the rest of the errors
    SERVER    = 202  # internal server error
    PROTOCOL  = 203  # malformed packet, invalid arguments or bad token
    METHOD    = 204  # unexpected method, eg: from server we only reply to query messages
    UNDEFINED = -1


class MsgKey(StrEnum):
    TRANSACTION_ID = "t"      # unique identifier, generated by querying node that matches query/response
    MSG_TYPE       = "y"      # v is specified in MsgType()
    QUERY_TYPE     = "q"      # v is specified in QueryType()
    QUERY_ARGS     = "a"      # query messages have required arguments with sender id
    RESPONSE_ARGS  = "r"      # response messages have required arguments with sender id
    ERROR_ARGS     = "e"      # error messages have required args with code and message
    SENDER_ID      = "id"     # v is specified in IdKey
    TARGET_ID      = "target" # v is specified in IdKey


class MsgType(StrEnum):
    QUERY    = "q"
    RESPONSE = "r"
    ERROR    = "e"


class QueryType(StrEnum):
    PING      = "ping"
    STORE     = "store"
    FIND_NODE = "find_node"
    FIND_KEY  = "find_key"


class MsgBaseClass(Bencoder):
    def __init__(self, transaction_id: Optional[str]=None):
        self._data: dict = {}

        # Set random 2 byte transaction id if not specified
        if transaction_id == None:
            self.transaction_id = "".join([random.choice(string.ascii_letters+string.digits) for _ in range(2)])
        else:
            self.transaction_id = transaction_id

    def __repr__(self):
        return json.dumps(self._data, indent=4)

    def validate(self):
        """ Needs to be implemented when subclassed """

    @property
    def msg_type(self):
        return self._data.get("y")

    @msg_type.setter
    def msg_type(self, mtype: str):
        self._data["y"] = mtype

    @property
    def transaction_id(self):
        return self._data[MsgKey.TRANSACTION_ID]

    @transaction_id.setter
    def transaction_id(self, transaction_id: str):
        self._data[MsgKey.TRANSACTION_ID] = transaction_id

    def is_query(self):
        return self._data.get(MsgKey.MSG_TYPE) == MsgType.QUERY

    def is_response(self):
        return self._data.get(MsgKey.MSG_TYPE) == MsgType.RESPONSE

    def is_error(self):
        return self._data.get(MsgKey.MSG_TYPE) == MsgType.ERROR

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
    def __init__(self, *args, uuid: Optional[int]=None, **kwargs):
        MsgBaseClass.__init__(self, *args, **kwargs)

        if uuid:
            self.sender_id = uuid

        # we can set time between request and response
        self._response_time: int = -1
        self.msg_type = MsgType.RESPONSE

    @property
    def sender_id(self):
        return self._data[MsgKey.RESPONSE_ARGS][MsgKey.SENDER_ID]

    @sender_id.setter
    def sender_id(self, id: int):
        if not MsgKey.RESPONSE_ARGS in self._data.keys():
            self._data[MsgKey.RESPONSE_ARGS] = {}
        self._data[MsgKey.RESPONSE_ARGS][MsgKey.SENDER_ID] = id

    @property
    def response_time(self):
        return self._exec_time

    @response_time.setter
    def response_time(self, exec_time: int):
        self._exec_time = exec_time

    @property
    def return_values(self):
        return self._data[MsgKey.RESPONSE_ARGS]

    @return_values.setter
    def return_values(self, data: dict):
        self._data[MsgKey.RESPONSE_ARGS] |= data

    def validate(self):
        if not self._data.get("t"):
            raise MsgError(f"Failed to validate message, message has no transaction id")
        if not self._data.get(MsgKey.MSG_TYPE):
            raise MsgError(f"Failed to validate message, message has no type information")
        if not self._data["y"] == MsgType.RESPONSE:
            raise MsgError(f"Failed to validate message, message type is not response")
        if not self._data.get(MsgKey.RESPONSE_ARGS):
            raise MsgError(f"Failed to validate response message, message has no response type information")
        if not self._data[MsgKey.RESPONSE_ARGS].get(MsgKey.SENDER_ID):
            raise MsgError(f"Failed to validate response message, id key not found in arguments")


class QueryMsgBaseClass(MsgBaseClass):
    def __init__(self, *args, uuid: Optional[int]=None, **kwargs):
        MsgBaseClass.__init__(self, *args, **kwargs)

        if uuid:
            self.sender_id = uuid

        self.msg_type = MsgType.QUERY

    @property
    def query_type(self):
        return self._data[MsgKey.QUERY_TYPE]

    @query_type.setter
    def query_type(self, qtype: str):
        self._data[MsgKey.QUERY_TYPE] = qtype

    @property
    def sender_id(self):
        return self._data[MsgKey.QUERY_ARGS][MsgKey.SENDER_ID]

    @sender_id.setter
    def sender_id(self, id: int):
        if not MsgKey.QUERY_ARGS in self._data.keys():
            self._data[MsgKey.QUERY_ARGS] = {}
        self._data[MsgKey.QUERY_ARGS][MsgKey.SENDER_ID] = id

    def validate(self):
        if not self._data.get(MsgKey.TRANSACTION_ID):
            raise MsgError(f"Failed to validate message, message has no transaction id")
        if not self._data.get(MsgKey.MSG_TYPE):
            raise MsgError(f"Failed to validate message, message has no type information")
        if not self._data[MsgKey.MSG_TYPE] == MsgType.QUERY:
            raise MsgError(f"Failed to validate message, message type is not query")
        if not self._data.get(MsgKey.QUERY_TYPE):
            raise MsgError(f"Failed to validate query message, message has no query type information")
        if not self._data.get(MsgKey.QUERY_ARGS):
            raise MsgError(f"Failed to validate query message, message has no arguments")
        if not self._data[MsgKey.QUERY_ARGS].get(MsgKey.SENDER_ID):
            raise MsgError(f"Failed to validate query message, id dictionary not found in arguments")

        match self._data[MsgKey.QUERY_TYPE]:
            case QueryType.PING:
                # ping has no extra arguments
                ...
            case QueryType.STORE:
                ...
            case QueryType.FIND_NODE:
                if not self._data[MsgKey.QUERY_ARGS].get(MsgKey.TARGET_ID):
                    raise MsgError(f"Failed to validate find_node query message, target node not found in arguments")
                print("TYPE:", type(self._data[MsgKey.QUERY_ARGS].get(MsgKey.TARGET_ID)))
                if type(self._data[MsgKey.QUERY_ARGS].get(MsgKey.TARGET_ID)) != int:
                    raise MsgError(f"Failed to validate find_node query message, target node must be integer")
            case QueryType.FIND_KEY:
                ...
            case _:
                raise MsgError(f"Failed to validate message, unknown query type: {self._data[MsgKey.QUERY_TYPE]}")


class PingMsg(QueryMsgBaseClass):
    def __init__(self, *args, **kwargs):
        QueryMsgBaseClass.__init__(self, *args, **kwargs)
        self.query_type = QueryType.PING


class StoreMsg(QueryMsgBaseClass):
    def __init__(self, *args, **kwargs):
        QueryMsgBaseClass.__init__(self, *args, **kwargs)
        self.query_type = QueryType.STORE


class FindNodeMsg(QueryMsgBaseClass):
    def __init__(self, *args, **kwargs):
        QueryMsgBaseClass.__init__(self, *args, **kwargs)
        self.query_type = QueryType.FIND_NODE

    @property
    def target_id(self):
        return self._data[MsgKey.QUERY_ARGS].get(MsgKey.TARGET_ID)

    @target_id.setter
    def target_id(self, uuid: int):
        self._data[MsgKey.QUERY_ARGS][MsgKey.TARGET_ID] = uuid


class FindKeyMsg(QueryMsgBaseClass):
    def __init__(self, *args, **kwargs):
        QueryMsgBaseClass.__init__(self, *args, **kwargs)
        self.query_type = QueryType.FIND_KEY


class ErrorMsg(MsgBaseClass):
    def __init__(self, *args, code: ErrCode=ErrCode.UNDEFINED, msg: str="", **kwargs):
        MsgBaseClass.__init__(self, *args, **kwargs)
        self._data[MsgKey.ERROR_ARGS] = [code, msg]
        self.msg_type = MsgType.ERROR

    @property
    def error_code(self):
        return self._data[MsgKey.ERROR_ARGS][0]

    @error_code.setter
    def error_code(self, code: ErrCode):
        self._data[MsgKey.ERROR_ARGS][0] = code

    @property
    def error_msg(self):
        return self._data[MsgKey.ERROR_ARGS][1]

    @error_msg.setter
    def error_msg(self, msg: str):
        self._data[MsgKey.ERROR_ARGS][1] = msg

    def validate(self):
        if not self._data.get(MsgKey.TRANSACTION_ID):
            raise MsgError(f"Failed to validate message, message has no transaction id")
        if not self._data.get(MsgKey.MSG_TYPE):
            raise MsgError(f"Failed to validate message, message has no type information")
        if not self._data[MsgKey.MSG_TYPE] == MsgType.ERROR:
            raise MsgError(f"Failed to validate message, message type is not error")
        if not self._data.get(MsgKey.ERROR_ARGS):
            raise MsgError(f"Failed to validate error message, message has no error information")

        try:
            int(self._data.get(MsgKey.ERROR_ARGS)[0])   # error code
            self._data.get(MsgKey.ERROR_ARGS)[1]        # error message
        except IndexError:
            raise MsgError(f"Failed to validate error message, message has malformed error")
        except ValueError:
            raise MsgError(f"Failed to validate error message, message has malformed error")
