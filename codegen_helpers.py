import itertools
import random
import re

from codegen import BANNED_FIELDS


def fl_iterate(items):
    items = list(items)
    assert len(items) > 0
    for j, item in enumerate(items):
        yield item, j == 0, j == len(items)-1



class Field(object):
    def __init__(self, fmt=None, size=None, name=None, decor_name=True):
        self.fmt = fmt
        self.size = size
        self.name = name
        self.decor_name = decor_name

    def dname(self, decor):
        if self.decor_name:
            return decor % self.name
        else:
            return self.name

    def do_print(self, prefix, decor):
        dname = self.dname(decor)
        self._do_print(prefix, dname)

class FieldStr(Field):
    def _do_print(self, prefix, dname):
        print prefix+"%s = data[offset : offset+str_len]" % dname
        print prefix+"offset += str_len"

class FieldTable(Field):
    def _do_print(self, prefix, dname):
        print prefix+"%s, offset = table.decode(data, offset)" % dname

def xdecode_bits(wrapper, name):
    wrapper.bits.append( name )
    if len(wrapper.bits) == 1:
        x = [Field('B', 1, 'bits', False)]
    return x

unpack_fixed_types = {
    'octet':     lambda w, n:[Field('B', 1, n)],
    'short':     lambda w, n:[Field('H', 2, n)],
    'long':      lambda w, n:[Field('I', 4, n)],
    'longlong':  lambda w, n:[Field('Q', 8, n)],
    'timestamp': lambda w, n:[Field('Q', 8, n)],
    'shortstr':  lambda w, n:[Field('B', 1, 'str_len', False), FieldStr(name=n)],
    'longstr':   lambda w, n:[Field('I', 4, 'str_len', False), FieldStr(name=n)],
    'table':     lambda w, n:[FieldTable(name=n)],
    'bit':      xdecode_bits,
}


class UnpackWrapper(object):
    fixed_types = unpack_fixed_types
    def __init__(self):
        self.fields = []
        self.bits = []

    def add(self, n, t):
        self.fields += self.fixed_types[t](self, n)

    def _groups(self):
        for for_struct, group in itertools.groupby(self.fields, lambda f: \
                                           True if f.fmt else random.random()):
            yield for_struct is True, list(group)

    def do_print(self, p, decor):
        for for_struct, fields in self._groups():
            if for_struct:
                for f, first, last in fl_iterate(fields):
                    print p+"%s%s%s%s" % (
                        '(' if first else ' ',
                        f.dname(decor),
                        ',' if first and last else '',
                        ')' if last else ',\n',
                    ),
                fmts = ''.join([f.fmt for f in fields])
                print "= struct.unpack_from('!%s', data, offset)" % (fmts,)
                if 'bits' in [f.dname(decor) for f in fields]:
                    self.do_print_bits(p, decor)
                print p+"offset += %s" % ('+'.join(
                        map(str, [f.size for f in fields])
                        ),)
            else:
                assert len(fields)==1
                fields[0].do_print(p, decor)

    def do_print_bits(self, prefix, decor):
        for b, name in enumerate(self.bits):
            print prefix+"%s = bool(bits & 0x%x)" % (decor % name, 1 << b)


fixed_types = {
    'octet': ('B', 1),
    'short': ('H', 2),
    'long': ('I', 4),
    'longlong': ('Q', 8),
    'timestamp': ('Q', 8),
}

class PackWrapper(object):
    def __init__(self):
        self.fields = []
        self.bits = []

    def add(self, n, t):
        nl = 'len(%s)' % n
        nr = '%s_raw' % n
        nrl = 'len(%s)' % nr
        if n in BANNED_FIELDS:
            default = BANNED_FIELDS[n]
            if t in fixed_types:
                self.fields += [
                    (fixed_types[t][0], fixed_types[t][1], str(default))
                    ]
                return
            elif t == 'shortstr':
                if not default:
                    self.fields += [
                        ('B', 1, '0'),
                        ]
                    return
                else:
                    self.fields += [
                        ('B', 1, str(len(default))),
                        (None, len(default), repr(default)),
                        ]
                    return
            elif t == 'bit':
                pass
            else:
                assert False, "not supported %s" % (t,)

        if t in fixed_types:
            self.fields += [
                (fixed_types[t][0], fixed_types[t][1], n)
                ]
        elif t == 'shortstr':
            self.fields += [
                ('B', 1, nl),
                (None, nl, n),
                ]
        elif t == 'longstr':
            self.fields += [
                ('I', 4, nl),
                (None, nl, n),
                ]
        elif t == 'table':
            self.fields += [
                (None, nrl, nr)
                ]
        elif t == 'bit':
            if not self.bits:
                self.fields += [
                    ('B', 1, self.encode_bits)
                    ]
            self.bits.append( n )
        else:
            raise Exception("bad type %s" % (t,))

    def encode_bits(self):
        acc = []
        for i, n in enumerate(self.bits):
            if n in BANNED_FIELDS:
                if BANNED_FIELDS[n]:
                    acc.append( str(BANNED_FIELDS[n]) )
            else:
                acc.append( '(%s and 0x%x or 0)' % (n, 1 << i) )
        if not acc:
            acc = '0'
        return ' | '.join( acc )

    def get_sizes(self):
        return zip(*self.fields)[1]

    def close(self):
        nfields = []
        for fmt, sz, name in self.fields:
            if callable(name):
                name = name()
            nfields.append( (fmt, sz, str(name)) )
        self.fields = nfields

    def group_count(self):
        return len(list(self.groups()))

    def groups(self):
        groups = itertools.groupby(self.fields, lambda (a,b,c): True \
                                       if a else random.random())
        for _, fields_group in groups:
            fmt, sizes, names = itertools.izip(*fields_group)
            if re.match("^[0-9]+$", ''.join(names)):
                for sz in sizes:
                    assert isinstance(sz, int), repr(sz)
                immediate = True
            else:
                immediate = False
            yield immediate, fmt, sizes, names

    def do_print(self, prefix, _):
        for immediate, fmt, sizes, names in self.groups():
            if immediate:
                s = ""
                for size, name in zip(sizes, names):
                    s+=  "%0*x" % (size*2, int(name))
                print prefix+'"%s",' % (''.join(["\\x%s" % p
                                                for p in re.findall('..', s)]),)
            else:
                if fmt[0] is not None:
                    print prefix+"struct.pack('!%s', %s)," % (''.join(fmt),
                                                              ', '.join(names))
                else:
                    assert len(fmt) == 1
                    print prefix+"%s," % (names[0],)

