from future import utils as futils


def as_str(obj):
    """Return a string type, decoding from bytes as needed"""
    if not futils.PY2 and hasattr(obj, 'decode'):
        obj = obj.decode('utf-8')
    return obj


def as_bytes(obj):
    """Return a bytes type, encoding from string as needed"""
    if not isinstance(obj, futils.binary_type):
        obj = obj.encode('utf-8')
    return obj


def join_as_bytes(args):
    """Join string args to a byte string, encoding to bytes as needed"""
    args = (as_bytes(o) for o in args)
    return b''.join(args)