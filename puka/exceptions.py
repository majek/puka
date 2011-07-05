import socket
from . import spec_exceptions

class ConnectionBroken(socket.error): pass
class UnsupportedProtocol(socket.error): pass


def exception_from_frame(result):
    reply_code = result.get('reply_code', 0)
    if reply_code in spec_exceptions.ERRORS:
        return spec_exceptions.ERRORS[reply_code](result)
    return spec_exceptions.AMQPError(result)

def mark_frame(result, exception=None):
    result.is_error = True
    if exception is None:
        result.exception = exception_from_frame(result)
    else:
        result.exception = exception
    return result
