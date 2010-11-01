import select
import socket
import time
import errno
import struct
from urlparse import unquote, urlsplit

from . import simplebuffer
from . import channel
from . import machine
from . import spec


import logging
log = logging.getLogger(__name__)
FORMAT_CONS = '%(asctime)s %(name)-12s %(levelname)8s\t%(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT_CONS)


def parse_amqp_url(amqp_url):
    '''
    >>> parse_amqp_url('amqp:///')
    ('guest', 'guest', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://a:b@c:1/d')
    ('a', 'b', '/d', 'c', 1)
    >>> parse_amqp_url('amqp://g%20uest:g%20uest@host/vho%20st')
    ('g uest', 'g uest', '/vho st', 'host', 5672)
    >>> parse_amqp_url('http://asd')
    Traceback (most recent call last):
      ...
    AssertionError: Only amqp:// protocol supported.
    '''
    assert amqp_url.startswith('amqp://'), "Only amqp:// protocol supported."
    # urlsplit doesn't know how to parse query when scheme is amqp,
    # we need to pretend we're http'
    o = urlsplit('http://' + amqp_url[len('amqp://'):])
    username = unquote(o.username) if o.username else 'guest'
    password = unquote(o.password) if o.password else 'guest'

    vhost = unquote(o.path) if o.path else '/'
    host = o.hostname or 'localhost'
    port = o.port if o.port else 5672
    return (username, password, vhost, host, port)

def set_ridiculously_high_buffers(sd):
    '''
    Set large tcp/ip buffers kernel. Let's move the complexity
    to the operating system! That's a wonderful idea!
    '''
    for flag in [socket.SO_SNDBUF, socket.SO_RCVBUF]:
        for i in range(10):
            bef = sd.getsockopt(socket.SOL_SOCKET, flag)
            try:
                sd.setsockopt(socket.SOL_SOCKET, flag, bef*2)
            except socket.error:
                break
            aft = sd.getsockopt(socket.SOL_SOCKET, flag)
            if aft <= bef or aft >= 1024*1024:
                break


class Connection(object):
    frame_max = 131072

    def __init__(self, amqp_url):
        self.sd = socket.socket()
        self.sd.setblocking(False)
        set_ridiculously_high_buffers(self.sd)

        self.recv_buf = simplebuffer.SimpleBuffer()
        self.recv_need = 8
        self.send_buf = simplebuffer.SimpleBuffer()
        self.channels = {}
        self.free_channels = []
        self.tickets = {}
        self.ticket_number = 1
        self.ready_tickets = set()

        (self.username, self.password, self.vhost, self.host, self.port) = \
            parse_amqp_url(amqp_url)

    def fileno(self):
        return self.sd

    def _connect(self):
        try:
            self.sd.connect((self.host, self.port))
        except socket.error, e:
            if hasattr(e, 'errno') and (e.errno == errno.EINPROGRESS):
                pass
            else:
                raise
        r = self.send_buf.write('AMQP\x00\x00\x09\x01')

        self.channels[0] = channel.Channel(self, 0)
        return machine.connection_handshake(self)

    def on_read(self):
        try:
            r = self.sd.recv(1048576)
        except socket.error, e:
            if hasattr(e, 'errno') and (e.errno == EAGAIN):
                return
            else:
                raise

        if len(r) == 0:
            # TODO
            raise Exception('conn died')

        self.recv_buf.write(r)

        while len(self.recv_buf) >= self.recv_need:
            consumed, self.recv_need = \
                self._handle_read(self.recv_buf.read(), 0)
            if consumed:
                self.recv_buf.consume(consumed)

    def _handle_read(self, data, offset):
        if len(data)-offset < 8:
            return 0, 8
        frame_type, channel, payload_size = \
            struct.unpack_from('!BHI', data, offset)
        offset += 7
        if len(data) < 8+payload_size:
            return 0, 8+payload_size
        assert data[7+payload_size] == '\xCE'

        if frame_type == 0x01: # Method frame
            method_id, = struct.unpack_from('!I', data, offset)
            offset += 4
            frame, offset = spec.METHODS[method_id](data, offset)
            self.channels[channel].consume_method(frame)
        elif frame_type == 0x02: # header frame
            (class_id, body_size) = struct.unpack_from('!HxxQ', data, offset)
            offset += 12
            props, offset = spec.PROPS[class_id](data, offset)
            self.channels[channel].consume_props(body_size, props)
        elif frame_type == 0x03: # body frame
            body_chunk = str(data[offset : offset+payload_size])
            self.channels[channel].consume_body(body_chunk)
            offset += len(body_chunk)
        else:
            assert False, "Unknown frame type"

        assert data[offset] == '\xCE'
        offset += 1
        assert offset == 8+payload_size
        return offset, 8


    def _send_frames(self, raw_frames):
        p = bool(self.send_buf)
        for raw_frame in raw_frames:
            self.send_buf.write(raw_frame)
        if not p: # if buffer wasn't full before, try to write straightaway.
            self.on_write()

    def needs_write(self):
        return bool(self.send_buf)

    def on_write(self):
        try:
            r = self.sd.send(self.send_buf.read())
        except socket.error, e:
            if hasattr(exn, 'errno') and (exn.errno == EAGAIN):
                return
            else:
                raise
        self.send_buf.consume(r)


    def wait(self, *ticket_numbers):
        '''
        Wait for selected tickets. Exit after ticket runs a callback.
        '''
        fd = self.fileno()
        ticket_numbers = set(ticket_numbers)
        while True:
            while True:
                ready = ticket_numbers & self.ready_tickets
                if not ready:
                    break
                ticket_number = list(ready)[0]
                return self.tickets[ticket_number].run_callback()

            r, w, e = select.select([fd],
                                    [fd] if self.needs_write() else [],
                                    [fd])
            if r or e:
                self.on_read()
            if w:
                self.on_write()

    def wait_for_any(self):
        '''
        Wait for any ticket. Block forever.
        '''
        fd = self.fileno()
        while True:
            self.run_any_callbacks()
            r, w, e = select.select([fd],
                                    [fd] if self.needs_write() else [],
                                    [fd])
            if r or e:
                self.on_read()
            if w:
                self.on_write()

    def run_any_callbacks(self):
        '''
        Run any callbacks, any tickets, but do not block.
        '''
        while self.ready_tickets:
            ticket_number = list(self.ready_tickets)[0]
            self.tickets[ticket_number].run_callback()
