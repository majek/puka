#!/usr/bin/env python

import os
import simplejson as json
import sys
import string

sys.path.append(os.path.join("..","rabbitmq-codegen"))
from amqp_codegen import *

import codegen_helpers

AMQP_ACCEPTED_BY_UPDATE_JSON="amqp-accepted-by-update.json"

BANNED_CLASSES=['access', 'tx']
BANNED_FIELDS= {
    'ticket': 0,
    'nowait': 0,
    'capabilities': '',
    'insist' : 0,
    'out_of_band': '',
    'known_hosts': '',
}

def pyize(*args):
    a = ' '.join(args).replace('-', '_').replace(' ', '_')
    if a == 'global': a+= '_'
    if a == 'type': a+= '_'
    return a

def Pyize(*args):
    return ''.join([a.title() for a in args]).replace('-', '').replace(' ', '')

def PYIZE(*args):
    return ' '.join(args).replace('-', '_').replace(' ', '_').upper()




def print_constants(spec):
    for c in spec.allClasses():
        for m in c.allMethods():
            print "%-32s= 0x%08X \t# %i,%i %i" % (
                m.u,
                m.method_id,
                m.klass.index, m.index, m.method_id
                )

    print
    for c in spec.allClasses():
        if c.fields:
            print "%-24s= 0x%04X" % (
                c.u,
                c.index,)
    print

def print_decode_methods_map(client_methods):
    print "METHODS = {"
    for m in client_methods:
        print "    %-32s%s," % (
            m.u + ':',
            m.decode,
            )
    print "}"
    print

def print_decode_properties_map(props_classes):
    print "PROPS = {"
    for c in props_classes:
        print "    %s: %s, \t# %d" % (
            c.u, c.decode, c.index)
    print "}"
    print


def print_decode_method(m):
    print "class %s(Frame):" % (m.frame,)
    print "    name = '%s'" % (pyize(m.klass.name + '.' + m.name),)
    print "    method_id = %s" % (pyize('method', m.klass.name, m.name).upper(),)
    if m.hasContent:
        print "    has_content = True"
        print "    class_id = %s" % (m.klass.u,)
    print ""

    print "def %s(data, offset):" % (m.decode,)
    print "    frame = %s()" % (m.frame,)

    fields = codegen_helpers.UnpackWrapper()
    for i, f in enumerate(m.arguments):
        fields.add(f.n, f.t)

    fields.do_print(' '*4, "frame['%s']")
    print "    return frame, offset"
    print


def print_decode_properties(c):
    print "def %s(data, offset):" % (c.decode,)
    print "    props = {}"
    print "    flags, = struct.unpack_from('!H', data, offset)"
    print "    offset += 2"
    print "    assert (flags & 0x01) == 0"
    for i, f in enumerate(c.fields):
        print "    if (flags & 0x%04x): # 1 << %i" % (1 << (15-i), 15-i)
        fields = codegen_helpers.UnpackWrapper()
        fields.add(f.n, f.t)
        fields.do_print(" "*8, "props['%s']")
    print "    return props, offset"
    print




def _default_params(m):
    for f in m.arguments:
        yield "%s=%r" % (f.n, f.defaultvalue)
    if m.hasContent:
        yield "user_headers={}"
        yield "payload=''"
        yield "frame_size=None"

def _method_params_list(m):
    for f in m.arguments:
        if not f.banned:
            yield f.n
    if m.hasContent:
        yield 'user_headers'
        yield 'body'
        yield 'frame_size'

def print_encode_method(m):
    print "# %s" % (' '.join(_default_params(m)),)
    print "def %s(%s):" % (m.encode, ', '.join(_method_params_list(m)),)
    for f in [f for f in m.arguments if not f.banned and f.t in ['table']]:
        print "    %s_raw = table.encode(%s)" % (f.n, f.n)

    if m.hasContent:
        print "    props, headers = split_headers(user_headers, %s_PROPS)" % (
            m.klass.name.upper(),)
        print "    if headers:"
        print "        props['headers'] = headers"

    fields = codegen_helpers.PackWrapper()
    fields.add(m.u, 'long')
    for f in m.arguments:
        fields.add(f.n, f.t)
    fields.close()

    if not m.hasContent:
        print "    return ( (0x01,"
        if fields.group_count() > 1:
            print "              ''.join(("
            fields.do_print(' '*16, '%s')
            print "              ))"
        else:
            fields.do_print(' '*16, '%s')
        print "           ), )"
    else:
        print "    return [ (0x01,"
        print "              ''.join(("
        fields.do_print(' '*16, '%s')
        print "              ))"
        print "           ),"
        print "           %s(len(body), props)," % (m.klass.encode,)
        print "        ] + encode_body(body, frame_size)"

def print_encode_properties(c):
    print "%s_PROPS = set(("% (c.name.upper(),)
    for f in c.fields:
        print '    "%s",' % (f.n,)
    print "    ))"
    print
    print "def %s(body_size, props):" % (c.encode,)
    print "    pieces = []"
    print "    flags = 0"
    for i, f in enumerate(c.fields):
        pn = pyize(f.name)
        print "    %s = props.get('%s')" % (pn, pn)
        print "    if %s is not None:" % (pn,)
        print "        flags |= 0x%04x # (1 << %i)" % ( 1 << (15-i), 15-i,)

        if f.t in ['table']:
            print "        %s_raw = table.encode(%s)" % (f.n, f.n)
        fields = codegen_helpers.PackWrapper()
        fields.add(f.n, f.t)
        fields.close()
        print ' '*8+'pieces.extend(['
        fields.do_print(' '*16, '%s')
        print ' '*8 + '])'

    print "    str_pieces = ''.join(pieces)"
    print "    return (0x02, ''.join(("
    print "        struct.pack('!HHQH',"
    print "                    %s, 0, body_size, flags)," % (c.u,)
    print "        str_pieces,"
    print "        ))"
    print "        )"


def GetAmqpSpec(spec_path, accepted_by_udate):
    spec = AmqpSpec(spec_path)

    for c in spec.allClasses():
        c.banned = bool(c.name in BANNED_CLASSES)
        c.u = PYIZE('CLASS', c.name)

    spec.classes = filter(lambda c:not c.banned, spec.classes)

    for c in spec.allClasses():
        for m in c.allMethods():
            m.u = PYIZE('METHOD', m.klass.name, m.name)
            m.method_id = m.klass.index << 16 | m.index
            m.decode = pyize('decode', m.klass.name, m.name)
            m.encode = pyize('encode', m.klass.name, m.name)
            m.frame = Pyize('frame', m.klass.name, m.name)

            try:
                m.accepted_by = accepted_by_udate[c.name][m.name]
            except KeyError:
                print >> sys.stderr, " [!] Method %s.%s unknown! Assuming " \
                    "['server', 'client']" % (c.name, m.name)
                m.accepted_by = ['server', 'client']

            for f in m.arguments:
                f.t = spec.resolveDomain(f.domain)
                f.n = pyize(f.name)
                f.banned = bool(f.name in BANNED_FIELDS)

    for c in spec.allClasses():
        if c.fields:
            c.decode = pyize('decode', c.name, 'properties')
            c.encode = pyize('encode', c.name, 'properties')
            for f in c.fields:
                f.t = spec.resolveDomain(f.domain)
                f.n = pyize(f.name)
    return spec

def main(spec_path):
    accepted_by_udate = json.loads(file(AMQP_ACCEPTED_BY_UPDATE_JSON).read())
    spec = GetAmqpSpec(spec_path, accepted_by_udate)
    print """# Autogenerated - do not edit
import struct
from . import table

"""
    print "PREAMBLE = 'AMQP\\x00\\x%02x\\x%02x\\x%02x'" % (
        spec.major, spec.minor, spec.revision)
    print
    print_constants(spec)
    print

    props_classes = [c for c in spec.allClasses() if c.fields]


    client_methods = [m for m in spec.allMethods() if 'client' in m.accepted_by]
    print
    print '''
class Frame(dict):
    has_content = False
    is_error = False

'''
    for m in client_methods:
        print_decode_method(m)
        print
    print
    print_decode_methods_map(client_methods)
    print
    for c in props_classes:
        print_decode_properties(c)
        print
    print_decode_properties_map(props_classes)
    print

    server_methods = [m for m in spec.allMethods() if 'server' in m.accepted_by]
    for m in server_methods:
        print_encode_method(m)
        print

    for c in props_classes:
        print_encode_properties(c)
        print

    print """
def split_headers(user_headers, properties_set):
    props = {}
    headers = {}
    for key, value in user_headers.iteritems():
        if key in properties_set:
            props[key] = value
        else:
            headers[key] = value
    return props, headers

def encode_body(body, frame_size):
    limit = frame_size - 8 - 1   # spec is broken...
    r = []
    while body:
        payload, body = body[:limit], body[limit:]
        r.append( (0x03, payload) )
    return r
"""

if __name__ == "__main__":
    do_main_dict({"framing": main})







