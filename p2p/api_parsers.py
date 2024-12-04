from typing import Any

from core.utils import debug, info, error


class BencDecodeError(Exception): pass
class BencEncodeError(Exception): pass


class Bencoder():
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
                debug("msg", "parse_int", f"found: {out}")
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
        debug("msg", "parse_str", f"found: [{size}] {data}")
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

                debug("msg", "parse_dict", f"found: {out}")
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
                debug("msg", "parse_list", f"found: {out}")
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
        """ Parse bencoded string into python native data structures """
        if not isinstance(data, str):
            raise BencDecodeError("Failed to decode, data is not a string")
        parsed, _ = self._parse(data)
        return parsed

    def dumps(self, data) -> str:
        """ Dump self.data as benc encoded string """
        out = ""

        match data:
            case int():
                out += f"i{data}e"
            case str():
                out += f"{len(data)}:{data}"
            case list():
                out += "l"
                for item in data:
                    out += self.dumps(item)
                out += "e"
            case dict():
                out += "d"
                for k,v in data.items():
                    out += f"{len(k)}:{k}{self.dumps(v)}"
                out += "e"
            case _:
                raise BencEncodeError(f"Failed to encode, unknown type: {type(v)}")

        return out
