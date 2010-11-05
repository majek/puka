from . import spec


class PukaError(Exception): pass

class AMQPError(PukaError): pass

class NotFound(AMQPError): pass
class NoRoute(AMQPError): pass
class NoConsumers(AMQPError): pass


def exception_from_frame(result):
    if result['reply_code'] == 404:
        return NotFound(result)
    elif result['reply_code'] == 312:
        return NoRoute(result)
    elif result['reply_code'] == 313:
        return NoConsumers(result)
    return AMQPError(result)

def mark_frame(result):
    result.is_error = True
    result.exception = exception_from_frame(result)

