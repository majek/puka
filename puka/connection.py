import errno
import logging
import select
import socket
import struct
import time
import urllib
from . import urlparse

from . import channel
from . import exceptions
from . import machine
from . import simplebuffer
from . import spec
from . import promise

log = logging.getLogger('puka')



class Connection(object):
    frame_max = 131072

    def __init__(self, amqp_url='amqp:///', pubacks=None):
        self.pubacks = pubacks

        self.channels = channel.ChannelCollection()
        self.promises = promise.PromiseCollection(self)

        (self.username, self.password, self.vhost, self.host, self.port) = \
            parse_amqp_url(str(amqp_url))

    def _init_buffers(self):
        self.recv_buf = simplebuffer.SimpleBuffer()
        self.recv_need = 8
        self.send_buf = simplebuffer.SimpleBuffer()


    def fileno(self):
        return self.sd.fileno()

    def socket(self):
        return self.sd

    def _connect(self):
        self._handle_read = self._handle_conn_read
        self._init_buffers()

        try:
            addrinfo = socket.getaddrinfo(self.host, self.port, socket.AF_INET6, socket.SOCK_STREAM)
        except socket.gaierror:
            addrinfo = socket.getaddrinfo(self.host, self.port, socket.AF_INET, socket.SOCK_STREAM)

        (family, socktype, proto, canonname, sockaddr) = addrinfo[0]
        self.sd = socket.socket(family, socktype, proto)
        self.sd.setblocking(False)
        set_ridiculously_high_buffers(self.sd)
        try:
            self.sd.connect(sockaddr)
        except socket.error, e:
            if e.errno not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                raise
        return machine.connection_handshake(self)

    def on_read(self):
        try:
            r = self.sd.recv(131072)
        except socket.error, e:
            if e.errno == errno.EAGAIN:
                return
            else:
                raise

        if len(r) == 0:
            # a = self.sd.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            self._shutdown(exceptions.mark_frame(spec.Frame(),
                                                 exceptions.ConnectionBroken()))

        self.recv_buf.write(r)

        if len(self.recv_buf) >= self.recv_need:
            data = self.recv_buf.read()
            offset = 0
            while len(data) - offset >= self.recv_need:
                offset, self.recv_need = \
                    self._handle_read(data, offset)
            self.recv_buf.consume(offset)

    def _handle_conn_read(self, data, offset):
        self._handle_read = self._handle_frame_read
        if data[offset:].startswith('AMQP'):
            a,b,c,d = struct.unpack('!BBBB', data[offset+4:offset+4+4])
            self._shutdown(exceptions.mark_frame(
                    spec.Frame(),
                    exceptions.UnsupportedProtocol("%s.%s.%s.%s" % (a,b,c,d))))
            return 0,0
        else:
            return self._handle_frame_read(data, offset)

    def _handle_frame_read(self, data, start_offset):
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
        # Do not try to write straightaway, better wait for more data.
        self.send_buf.write(data)

    def _send_frames(self, channel_number, frames):
        self._send( ''.join([''.join((struct.pack('!BHI',
                                                  frame_type,
                                                  channel_number,
                                                  len(payload)),
                                      payload, '\xCE')) \
                                 for frame_type, payload in frames]) )

    def needs_write(self):
        return bool(self.send_buf)

    def on_write(self):
        try:
            # On windows socket.send blows up if the buffer is too large.
            r = self.sd.send(self.send_buf.read(128*1024))
        except socket.error, e:
            if e.errno == errno.EAGAIN:
                return
            else:
                raise
        self.send_buf.consume(r)


    def _tune_frame_max(self, new_frame_max):
        new_frame_max = new_frame_max if new_frame_max != 0 else 2**19
        self.frame_max = min(self.frame_max, new_frame_max)
        return self.frame_max


    def wait(self, promise_numbers, timeout=None, raise_errors=True):
        '''
        Wait for selected promises. Exit after promise runs a callback.
        '''
        if timeout is not None:
            t1 = time.time() + timeout
        else:
            td = None

        if isinstance(promise_numbers, int):
            promise_numbers = [promise_numbers]
        promise_numbers = set(promise_numbers)

        # Try flushing the write buffer before entering the loop, we
        # may as well return soon, and the user has no way to figure
        # out if the write buffer was flushed or not - (ie: did the
        # wait run select() or not)
        #
        # This is problem is especially painful with regard to
        # async messages, like basic_ack. See #3.
        #
        # Additionally, during the first round trip on windows - when
        # the connection is being established, the socket may not yet
        # be in the connected state - swallow an error in that case.
        try:
            self.on_write()
        except socket.error, e:
            if e.errno != 10057:
                raise

        while True:
            while True:
                ready = promise_numbers & self.promises.ready
                if not ready:
                    break
                promise_number = ready.pop()
                return self.promises.run_callback(promise_number,
                                                  raise_errors=raise_errors)

            if timeout is not None:
                t0 = time.time()
                td = t1 - t0
                if td < 0:
                    break

            r, w, e = select.select([self],
                                    [self] if self.needs_write() else [],
                                    [self],
                                    td)
            if r or e:
                self.on_read()
            if w:
                self.on_write()
            if not r and not e and not w:
                # timeout
                return None

    def wait_for_any(self):
        return self.loop()

    def wait_for_all(self, promise_list, raise_errors=True):
        for promise in promise_list:
            self.wait(promise, raise_errors=raise_errors)

    def loop(self, timeout=None):
        '''
        Wait for any promise. Block forever.
        '''
        if timeout is not None:
            t1 = time.time() + timeout
        else:
            td = None
        self._loop_break = False

        while True:
            self.run_any_callbacks()

            if self._loop_break:
                break

            if timeout is not None:
                t0 = time.time()
                td = t1 - t0
                if td < 0:
                    break
            r, w, e = select.select([self],
                                    [self] if self.needs_write() else [],
                                    [self],
                                    td)
            if r or e:
                self.on_read()
            if w:
                self.on_write()

        # Try flushing the write buffer just after the loop. The user
        # has no way to figure out if the buffer was flushed or
        # not. (ie: if the loop() require waiting on for data or not).
        self.on_write()


    def loop_break(self):
        self._loop_break = True

    def run_any_callbacks(self):
        '''
        Run any callbacks, any promises, but do not block.
        '''
        while self.promises.ready:
            [self.promises.run_callback(promise, raise_errors=False) \
                 for promise in list(self.promises.ready)]


    def _shutdown(self, result):
        # Cancel all events.
        for promise in self.promises.all():
            # It's possible that a promise may be already `done` but still not
            # removed. For example due to `refcnt`. In that case don't run
            # callbacks.
            if promise.to_be_released is False:
                promise.done(result)

        # And kill the socket
        try:
            self.sd.shutdown(socket.SHUT_RDWR)
        except socket.error, e:
            if e.errno is not errno.ENOTCONN: raise
        self.sd.close()
        self.sd = None
        # Sending is illegal
        self.send_buf = None

    def _close(self):
        return machine.connection_close(self)

    def set_callback(self, promise_number, callback):
        promise = self.promises.by_number(promise_number)
        promise.user_callback = callback


def parse_amqp_url(amqp_url):
    '''
    >>> parse_amqp_url('amqp:///')
    ('guest', 'guest', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://a:b@c:1/d')
    ('a', 'b', 'd', 'c', 1)
    >>> parse_amqp_url('amqp://g%20uest:g%20uest@host/vho%20st')
    ('g uest', 'g uest', 'vho st', 'host', 5672)
    >>> parse_amqp_url('http://asd')
    Traceback (most recent call last):
      ...
    AssertionError: Only amqp:// protocol supported.
    >>> parse_amqp_url('amqp://host/%2f')
    ('guest', 'guest', '/', 'host', 5672)
    >>> parse_amqp_url('amqp://host/%2fabc')
    ('guest', 'guest', '/abc', 'host', 5672)
    >>> parse_amqp_url('amqp://host/')
    ('guest', 'guest', '/', 'host', 5672)
    >>> parse_amqp_url('amqp://host')
    ('guest', 'guest', '/', 'host', 5672)
    >>> parse_amqp_url('amqp://user:pass@host:10000/vhost')
    ('user', 'pass', 'vhost', 'host', 10000)
    >>> parse_amqp_url('amqp://user%61:%61pass@ho%61st:10000/v%2fhost')
    ('usera', 'apass', 'v/host', 'hoast', 10000)
    >>> parse_amqp_url('amqp://')
    ('guest', 'guest', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://:@/') # this is a violation, vhost should be=''
    ('', '', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://user@/')
    ('user', 'guest', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://user:@/')
    ('user', '', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://host')
    ('guest', 'guest', '/', 'host', 5672)
    >>> parse_amqp_url('amqp:///vhost')
    ('guest', 'guest', 'vhost', 'localhost', 5672)
    >>> parse_amqp_url('amqp://host/')
    ('guest', 'guest', '/', 'host', 5672)
    >>> parse_amqp_url('amqp://host/%2f%2f')
    ('guest', 'guest', '//', 'host', 5672)
    >>> parse_amqp_url('amqp://[::1]')
    ('guest', 'guest', '/', '::1', 5672)
    '''
    assert amqp_url.startswith('amqp://'), "Only amqp:// protocol supported."
    # urlsplit doesn't know how to parse query when scheme is amqp,
    # we need to pretend we're http'
    o = urlparse.urlsplit('http://' + amqp_url[len('amqp://'):])
    username = urllib.unquote(o.username) if o.username is not None else 'guest'
    password = urllib.unquote(o.password) if o.password is not None else 'guest'

    path = o.path[1:] if o.path.startswith('/') else o.path
    # We do not support empty vhost case. Empty vhost is treated as
    # '/'. This is mostly for backwards compatibility, and the fact
    # that empty vhost is not very useful.
    vhost = urllib.unquote(path) if path else '/'
    host = urllib.unquote(o.hostname) if o.hostname else 'localhost'
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
