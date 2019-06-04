from __future__ import absolute_import
from builtins import object

import os
from io import BytesIO

# Python 2.4 support: os lacks SEEK_END and friends
try:
    getattr(os, "SEEK_END")
except AttributeError:
    os.SEEK_SET, os.SEEK_CUR, os.SEEK_END = list(range(3))


class SimpleBuffer(object):
    """
    >>> b = SimpleBuffer()
    >>> b.write(b'abcdef')
    >>> b.read(3)
    'abc'
    >>> b.consume(3)
    >>> b.write(b'z')
    >>> b.read()
    'defz'
    >>> b.read()
    'defz'
    >>> b.read(0)
    ''
    >>> repr(b)
    "<SimpleBuffer of 4 bytes, 7 total size, 'defz'>"
    >>> str(b)
    "<SimpleBuffer of 4 bytes, 7 total size, 'defz'>"
    >>> len(b)
    4
    >>> bool(b)
    True
    >>> b.flush()
    >>> len(b)
    0
    >>> bool(b)
    False
    >>> b.read(1)
    ''
    >>> b.write(b'a'*524288)
    >>> b.flush() # run GC code
    """
    def __init__(self):
        self.buf = BytesIO()
        self.size = 0
        self.offset = 0

    def write(self, data):
        self.buf.write(data)
        self.size += len(data)

    def read(self, size=None):
        self.buf.seek(self.offset)

        if size is None:
            data = self.buf.read()
        else:
            data = self.buf.read(size)

        self.buf.seek(0, os.SEEK_END)
        return data

    def consume(self, size):
        self.offset += size
        self.size -= size
        # GC old StringIO instance and free memory used by it.
        if self.size == 0 and self.offset > 524288:
            self.buf.close()
            self.buf = BytesIO()
            self.offset = 0

    def flush(self):
        self.consume(self.size)

    def __bool__(self):
        return self.size > 0

    def __len__(self):
        return self.size

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return '<SimpleBuffer of %i bytes, %i total size, %r%s>' % \
                    (self.size, self.size + self.offset, self.read(16),
                     (self.size > 16) and '...' or '')
