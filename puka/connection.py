import errno
import logging
import select
import socket
import struct
import urlparse

from . import channel
from . import machine
from . import simplebuffer
from . import spec
from . import ticket

log = logging.getLogger('puka')



class Connection(object):
    frame_max = 131072

    def __init__(self, amqp_url):
        self.channels = channel.ChannelCollection()
        self.tickets = ticket.TicketCollection(self)

        self.sd = socket.socket()
        self.sd.setblocking(False)
        set_ridiculously_high_buffers(self.sd)

        self._init_buffers()
        (self.username, self.password, self.vhost, self.host, self.port) = \
            parse_amqp_url(amqp_url)

    def _init_buffers(self):
        self.recv_buf = simplebuffer.SimpleBuffer()
        self.recv_need = 8
        self.send_buf = simplebuffer.SimpleBuffer()


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
        return machine.connection_handshake(self)


    def on_read(self):
        try:
            r = self.sd.recv(131072)
        except socket.error, e:
            if hasattr(e, 'errno') and (e.errno == errno.EAGAIN):
                return
            else:
                raise

        if len(r) == 0:
            # TODO
            raise Exception('conn died')

        self.recv_buf.write(r)

        if len(self.recv_buf) >= self.recv_need:
            data = self.recv_buf.read()
            offset = 0
            while len(data) - offset >= self.recv_need:
                offset, self.recv_need = \
                    self._handle_read(data, offset)
            self.recv_buf.consume(offset)

    def _handle_read(self, data, start_offset):
        offset = start_offset
        if len(data)-start_offset < 8:
            return start_offset, 8
        frame_type, channel, payload_size = \
            struct.unpack_from('!BHI', data, offset)
        offset += 7
        if len(data)-start_offset < 8+payload_size:
            return start_offset, 8+payload_size
        assert data[offset+payload_size] == '\xCE'

        if frame_type == 0x01: # Method frame
            method_id, = struct.unpack_from('!I', data, offset)
            offset += 4
            frame, offset = spec.METHODS[method_id](data, offset)
            self.channels.channels[channel].inbound_method(frame)
        elif frame_type == 0x02: # header frame
            (class_id, body_size) = struct.unpack_from('!HxxQ', data, offset)
            offset += 12
            props, offset = spec.PROPS[class_id](data, offset)
            self.channels.channels[channel].inbound_props(body_size, props)
        elif frame_type == 0x03: # body frame
            body_chunk = str(data[offset : offset+payload_size])
            self.channels.channels[channel].inbound_body(body_chunk)
            offset += len(body_chunk)
        else:
            assert False, "Unknown frame type"

        offset += 1 # '\xCE'
        assert offset == start_offset+8+payload_size
        return offset, 8


    def _send(self, data):
        p = bool(self.send_buf)
        self.send_buf.write(data)
        if not p: # buffer wasn't full before, try to write straightaway.
            self.on_write()

    def _send_frames(self, channel_number, frames):
        decorate = lambda (frame_type, payload): \
            ''.join((struct.pack('!BHI',
                                 frame_type,
                                 channel_number,
                                 len(payload)),
                     payload, '\xCE'))
        self._send( ''.join(map(decorate, frames)) )


    def needs_write(self):
        return bool(self.send_buf)

    def on_write(self):
        try:
            r = self.sd.send(self.send_buf.read())
        except socket.error, e:
            if hasattr(exn, 'errno') and (exn.errno == errno.EAGAIN):
                return
            else:
                raise
        self.send_buf.consume(r)


    def tune_frame_max(self, new_frame_max):
        new_frame_max = new_frame_max if new_frame_max != 0 else 2**19
        self.frame_max = min(self.frame_max, new_frame_max)
        return self.frame_max


    def wait(self, *ticket_numbers):
        '''
        Wait for selected tickets. Exit after ticket runs a callback.
        '''
        fd = self.fileno()
        ticket_numbers = set(ticket_numbers)
        while True:
            while True:
                ready = ticket_numbers & self.tickets.ready
                if not ready:
                    break
                ticket_number = list(ready)[0]
                return self.tickets.run_callback(ticket_number)

            r, w, e = select.select([fd],
                                    [fd] if self.needs_write() else [],
                                    [fd])
            if r or e:
                self.on_read()
            if w:
                self.on_write()

    def wait_for_any(self):
        return self.loop()

    def loop(self):
        '''
        Wait for any ticket. Block forever.
        '''
        fd = self.fileno()
        self._loop_break = False
        self.run_any_callbacks()
        while not self._loop_break:
            r, w, e = select.select([fd],
                                    [fd] if self.needs_write() else [],
                                    [fd])
            if r or e:
                self.on_read()
            if w:
                self.on_write()
            self.run_any_callbacks()

    def loop_break(self):
        self._loop_break = True

    def run_any_callbacks(self):
        '''
        Run any callbacks, any tickets, but do not block.
        '''
        while self.tickets.ready:
            ticket_number = list(self.tickets.ready)[0]
            self.tickets.run_callback(ticket_number, raise_errors=False)

    def shutdown(self):
        self.sd.shutdown(socket.SHUT_RDWR)
        self.sd.close()
        self.sd = None
        self._init_buffers()



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
    o = urlparse.urlsplit('http://' + amqp_url[len('amqp://'):])
    username = urlparse.unquote(o.username) if o.username else 'guest'
    password = urlparse.unquote(o.password) if o.password else 'guest'

    vhost = urlparse.unquote(o.path) if o.path else '/'
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
