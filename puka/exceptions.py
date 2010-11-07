from . import spec_exceptions

def exception_from_frame(result):
    reply_code = result.get('reply_code', 0)
    if reply_code in spec_exceptions.ERRORS:
        return spec_exceptions.ERRORS[reply_code](result)
    return spec_exceptions.AMQPError(result)

def mark_frame(result):
    result.is_error = True
    result.exception = exception_from_frame(result)

