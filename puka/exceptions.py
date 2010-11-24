import socket
from . import spec_exceptions

class ConnectionError(socket.error): pass



def exception_from_frame(result):
    reply_code = result.get('reply_code', 0)
    if reply_code in spec_exceptions.ERRORS:
        return spec_exceptions.ERRORS[reply_code](result)
    return spec_exceptions.AMQPError(result)

def mark_frame(result):
    result.is_error = True
    result.exception = exception_from_frame(result)
    return result

def mark_frame_connection_error(result):
    result.is_error = True
    result.exception = ConnectionError()
    return result
