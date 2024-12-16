class SkapaException(Exception):
    pass

class UnreachableError(Exception):
    pass

class SSHException(SkapaException): pass
class SSHJSONDecodeError(SSHException): pass
class SSHConnectionError(SSHException): pass

