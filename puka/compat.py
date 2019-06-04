from future import utils as futils


def as_bytes(obj):
    """Return the string type as bytes, encoding as needed"""
    if not isinstance(obj, futils.binary_type):
        obj = obj.encode('utf-8')
    return obj


def join_as_bytes(args):
    """Join string args to a byte string, encoding to bytes as needed"""
    args = (as_bytes(o) for o in args)
    return b''.join(args)