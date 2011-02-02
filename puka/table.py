# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0
#
# The contents of this file are subject to the Mozilla Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
# the License for the specific language governing rights and
# limitations under the License.
#
# The Original Code is Pika.
#
# The Initial Developers of the Original Code are LShift Ltd, Cohesive
# Financial Technologies LLC, and Rabbit Technologies Ltd.  Portions
# created before 22-Nov-2008 00:00:00 GMT by LShift Ltd, Cohesive
# Financial Technologies LLC, or Rabbit Technologies Ltd are Copyright
# (C) 2007-2008 LShift Ltd, Cohesive Financial Technologies LLC, and
# Rabbit Technologies Ltd.
#
# Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
# Ltd. Portions created by Cohesive Financial Technologies LLC are
# Copyright (C) 2007-2009 Cohesive Financial Technologies
# LLC. Portions created by Rabbit Technologies Ltd are Copyright (C)
# 2007-2009 Rabbit Technologies Ltd.
#
# Portions created by Tony Garnock-Jones are Copyright (C) 2009-2010
# LShift Ltd and Tony Garnock-Jones.
#
# All Rights Reserved.
#
# Contributor(s): ______________________________________.
#
# Alternatively, the contents of this file may be used under the terms
# of the GNU General Public License Version 2 or later (the "GPL"), in
# which case the provisions of the GPL are applicable instead of those
# above. If you wish to allow use of your version of this file only
# under the terms of the GPL, and not to allow others to use your
# version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the
# notice and other provisions required by the GPL. If you do not
# delete the provisions above, a recipient may use your version of
# this file under the terms of any one of the MPL or the GPL.
#
# ***** END LICENSE BLOCK *****
#
# Code originally from Pika, adapted to Puka.
#

import struct
import decimal
import datetime
import calendar


def encode(table):
    r'''
    >>> encode(None)
    '\x00\x00\x00\x00'
    >>> encode({})
    '\x00\x00\x00\x00'
    >>> encode({'a':1, 'c':1, 'd':'x', 'e':{}})
    '\x00\x00\x00\x1d\x01aI\x00\x00\x00\x01\x01cI\x00\x00\x00\x01\x01eF\x00\x00\x00\x00\x01dS\x00\x00\x00\x01x'
    >>> encode({'a':decimal.Decimal('1.0')})
    '\x00\x00\x00\x08\x01aD\x00\x00\x00\x00\x01'
    >>> encode({'a':decimal.Decimal('5E-3')})
    '\x00\x00\x00\x08\x01aD\x03\x00\x00\x00\x05'
    >>> encode({'a':datetime.datetime(2010,12,31,23,58,59)})
    '\x00\x00\x00\x0b\x01aT\x00\x00\x00\x00M\x1enC'
    >>> encode({'test':decimal.Decimal('-0.01')})
    '\x00\x00\x00\x0b\x04testD\x02\xff\xff\xff\xff'
    >>> encode({'a':-1, 'b':[1,2,3,4,-1],'g':-1})
    '\x00\x00\x00.\x01aI\xff\xff\xff\xff\x01bA\x00\x00\x00\x19I\x00\x00\x00\x01I\x00\x00\x00\x02I\x00\x00\x00\x03I\x00\x00\x00\x04I\xff\xff\xff\xff\x01gI\xff\xff\xff\xff'
    >>> encode({'a': True, 'b':False})
    '\x00\x00\x00\x08\x01at\x01\x01bt\x00'
    '''
    pieces = []
    if table is None:
        table = {}
    length_index = len(pieces)
    pieces.append(None) # placeholder
    tablesize = 0
    for (key, value) in table.iteritems():
        pieces.append(struct.pack('B', len(key)))
        pieces.append(key)
        tablesize = tablesize + 1 + len(key)
        tablesize += encode_value(pieces, value)
    pieces[length_index] = struct.pack('>I', tablesize)
    return ''.join(pieces)

def encode_value(pieces, value):
    if isinstance(value, str):
        pieces.append(struct.pack('>cI', 'S', len(value)))
        pieces.append(value)
        return 5 + len(value)
    elif isinstance(value, bool):
        pieces.append(struct.pack('>cB', 't', int(value)))
        return 2
    elif isinstance(value, int):
        pieces.append(struct.pack('>ci', 'I', value))
        return 5
    elif isinstance(value, long):
        pieces.append(struct.pack('>cq', 'l', value))
        return 9
    elif isinstance(value, decimal.Decimal):
        value = value.normalize()
        if value._exp < 0:
            decimals = -value._exp
            raw = int(value * (decimal.Decimal(10) ** decimals))
            pieces.append(struct.pack('>cBi', 'D', decimals, raw))
        else:
            # per spec, the "decimals" octet is unsigned (!)
            pieces.append(struct.pack('>cBi', 'D', 0, int(value)))
        return 6
    elif isinstance(value, datetime.datetime):
        pieces.append(struct.pack('>cQ', 'T', calendar.timegm(
                    value.utctimetuple())))
        return 9
    elif isinstance(value, dict):
        pieces.append(struct.pack('>c', 'F'))
        piece = encode(value)
        pieces.append(piece)
        return 1 + len(piece)
    elif isinstance(value, list):
        p = []
        [encode_value(p, v) for v in value]
        piece = ''.join(p)
        pieces.append(struct.pack('>cI', 'A', len(piece)))
        pieces.append(piece)
        return 5 + len(piece)
    else:
        assert False, "Unsupported field kind during encoding %r/%r" % (
            key, value)

def decode(encoded, offset):
    '''
    >>> decode(encode(None), 0)
    ({}, 4)
    >>> decode(encode({}), 0)[0]
    {}
    >>> decode(encode({'a':1, 'c':1, 'd':'x', 'e':{}, 'f':-1}), 0)[0]
    {'a': 1, 'c': 1, 'e': {}, 'd': 'x', 'f': -1}

    # python 2.5 reports Decimal("1.01"), python 2.6 Decimal('1.01')
    >>> decode(encode({'a':decimal.Decimal('1.01')}), 0)[0] # doctest: +ELLIPSIS
    {'a': Decimal(...1.01...)}
    >>> decode(encode({'a':decimal.Decimal('5E-30')}), 0)[0] # doctest: +ELLIPSIS
    {'a': Decimal(...5E-30...)}
    >>> decode(encode({'a':datetime.datetime(2010,12,31,23,58,59)}), 0)[0]
    {'a': datetime.datetime(2010, 12, 31, 23, 58, 59)}
    >>> decode(encode({'test':decimal.Decimal('-1.234')}), 0)[0]
    {'test': Decimal('-1.234')}
    >>> decode(encode({'test':decimal.Decimal('1.234')}), 0)[0]
    {'test': Decimal('1.234')}
    >>> decode(encode({'test':decimal.Decimal('1000000')}), 0)[0]
    {'test': Decimal('1000000')}
    >>> decode(encode({'a':[1,2,3,'a',decimal.Decimal('-0.01')]}), 0)[0]
    {'a': [1, 2, 3, 'a', Decimal('-0.01')]}
    >>> decode(encode({'a': 0x7EADBEEFDEADBEEFL}), 0)[0]
    {'a': 9128161957192253167L}
    >>> decode(encode({'a': True, 'b': False}), 0)[0]
    {'a': True, 'b': False}
    '''
    result = {}
    tablesize = struct.unpack_from('>I', encoded, offset)[0]
    offset = offset + 4
    limit = offset + tablesize
    while offset < limit:
        keylen = struct.unpack_from('B', encoded, offset)[0]
        offset = offset + 1
        key = encoded[offset : offset + keylen]
        offset = offset + keylen
        result[key], offset = decode_value(encoded, offset)
    return (result, offset)

def decode_value(encoded, offset):
    kind = encoded[offset]
    offset = offset + 1
    if kind == 'S':
        length = struct.unpack_from('>I', encoded, offset)[0]
        offset = offset + 4
        value = encoded[offset : offset + length]
        offset = offset + length
    elif kind == 't':
        value = struct.unpack_from('>B', encoded, offset)[0]
        value = bool(value)
        offset = offset + 1
    elif kind == 'I':
        value = struct.unpack_from('>i', encoded, offset)[0]
        offset = offset + 4
    elif kind == 'l':
        value = struct.unpack_from('>q', encoded, offset)[0]
        value = long(value)
        offset = offset + 8
    elif kind == 'D':
        decimals = struct.unpack_from('B', encoded, offset)[0]
        offset = offset + 1
        raw = struct.unpack_from('>i', encoded, offset)[0]
        offset = offset + 4
        value = decimal.Decimal(raw) * (decimal.Decimal(10) ** -decimals)
    elif kind == 'T':
        value = datetime.datetime.utcfromtimestamp(
            struct.unpack_from('>Q', encoded, offset)[0])
        offset = offset + 8
    elif kind == 'F':
        (value, offset) = decode(encoded, offset)
    elif kind == 'A':
        length, = struct.unpack_from('>I', encoded, offset)
        offset = offset + 4
        offset_end = offset + length
        value = []
        while offset < offset_end:
            v, offset = decode_value(encoded, offset)
            value.append(v)
        assert offset == offset_end
    else:
        assert False, "Unsupported field kind %s during decoding" % (kind,)
    return value, offset
