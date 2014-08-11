import errno
import logging
import select
import socket
import ssl
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

    '''
    Constructor of Puka Connection object.

    amqp_url - a url-like address of an AMQP server
    pubacks  - should Puka try to use 'publisher acks' for implementing
               blocking 'publish'. In early days (before RabbitMQ 2.3),
               pubacks weren't available, so Puka had to emulate blocking
               publish using trickery - sending a confirmation message with
               'mandatory' flag and waiting for it being bounced by the
               broker. Possible values:
                   True  - always use pubacks
                   False - never use pubakcs, always emualte them
                   None (default) - auto-detect if pubacks are availalbe
    client_properties - A dictionary of properties to be sent to the
               server.
    heartbeat - basic support for AMQP-level heartbeats (in seconds)
    ssl_parameters - SSL parameters to be used for amqps: connection
               (instance of SslConnectionParameters)
    '''
    def __init__(self, amqp_url='amqp:///', pubacks=None,
                 client_properties=None, heartbeat=0,
                 ssl_parameters=None):
        self.pubacks = pubacks

        self.channels = channel.ChannelCollection()
        self.promises = promise.PromiseCollection(self)

        (self.username, self.password, self.vhost,
            self.host, self.port, self.ssl) = parse_amqp_url(str(amqp_url))

        self.client_properties = client_properties

        self.heartbeat = heartbeat
        self._ssl_parameters = ssl_parameters
        self._needs_ssl_handshake = False

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

        addrinfo = None
        if socket.has_ipv6:
            try:
                addrinfo = socket.getaddrinfo(
                    self.host, self.port, socket.AF_INET6, socket.SOCK_STREAM)
            except socket.gaierror:
                pass
        if not addrinfo:
            addrinfo = socket.getaddrinfo(
                self.host, self.port, socket.AF_INET, socket.SOCK_STREAM)

        (family, socktype, proto, canonname, sockaddr) = addrinfo[0]
        self.sd = socket.socket(family, socktype, proto)
        set_ridiculously_high_buffers(self.sd)
        set_close_exec(self.sd)
        try:
            self.sd.connect(sockaddr)
        except socket.error, e:
            if e.errno not in (errno.EINPROGRESS, errno.EWOULDBLOCK):
                raise

        self.sd.setblocking(False)
        if self.ssl:
            self.sd = self._wrap_socket(self.sd)
            self._needs_ssl_handshake = True

        return machine.connection_handshake(self)

    def _wrap_socket(self, sock):
        """Wrap the socket for connecting over SSL.
        :rtype: ssl.SSLSocket
        """
        keyfile = None if self._ssl_parameters is None else \
            self._ssl_parameters.keyfile

        certfile = None if self._ssl_parameters is None else \
            self._ssl_parameters.certfile

        ca_certs = None if self._ssl_parameters is None else \
            self._ssl_parameters.ca_certs

        #require_certificate
        cert_reqs = ssl.CERT_NONE
        if ca_certs:
            cert_reqs = ssl.CERT_REQUIRED if \
                self._ssl_parameters.require_certificate \
                else ssl.CERT_OPTIONAL

        return ssl.wrap_socket(sock,
                               do_handshake_on_connect=False,
                               keyfile=keyfile,
                               certfile=certfile,
                               cert_reqs=cert_reqs,
                               ca_certs=ca_certs)

    def _do_ssl_handshake(self, timeout=None):
        """Perform SSL handshaking
        """
        if not self._needs_ssl_handshake:
            return False
        if timeout is not None:
            t1 = time.time() + timeout
        else:
            td = None
            t1 = None

        while True:
            if timeout is not None:
                t0 = time.time()
                td = t1 - t0
                if td < 0:
                    break
            try:
                self.sd.do_handshake()
                self._needs_ssl_handshake = False
                break
            except ssl.SSLError, e:
                if e.args[0] == ssl.SSL_ERROR_WANT_READ:
                    select.select([self.sd], [], [])
                elif e.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                    select.select([], [self.sd], [])
                else:
                    raise

    def on_read(self):
        while True:
            try:
                r = self.sd.recv(Connection.frame_max)
                break
            except ssl.SSLError, e:
                if e.args[0] == ssl.SSL_ERROR_WANT_READ:
                    select.select([self.sd], [], [])
                    continue
                raise
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
            while len(data) - offset >= self.recv_need and self.sd is not None:
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
        elif frame_type == 0x08: # heartbeat frame
            # One corner of the spec doc says this will be 0x04, most
            # says 0x08 which seems to be what's been implemented by
            # RabbitMQ at least.
            #
            # Got heartbeat, respond with one.
            #
            # It seems likely this logic is slightly incorrect. We're
            # getting a heartbeat because we asked for one from the
            # server. At connection setup it probably asked us for one
            # as well with the same timeout.  We're using the server
            # heartbeat as a trigger instead of setting up a separate
            # heartbeat cycler.
            self._send_frames(channel_number=0, frames=[(0x08, '')])
        else:
            assert False, "Unknown frame type 0x%x" % frame_type

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
        if not self.send_buf:  # already shutdown or empty buffer?
            return
        while True:
            try:
                # On windows socket.send blows up if the buffer is too large.
                r = self.sd.send(self.send_buf.read(128*1024))
                break
            except ssl.SSLError, e:
                if e.args[0] == ssl.SSL_ERROR_WANT_WRITE:
                    select.select([], [self.sd], [])
                    continue
                raise
            except socket.error, e:
                if e.errno in (errno.EWOULDBLOCK, errno.ENOBUFS):
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

        self._do_ssl_handshake(timeout=timeout)

        # Make sure the buffer is flushed if possible before entering
        # the loop. We may return soon, and the user has no way to
        # figure out if the write buffer was flushed or not - (ie: did
        # the wait run select() or not)
        #
        # This is problem is especially painful with regard to
        # async messages, like basic_ack. See #3.
        r, w, e = select.select((self,),
                                (self,) if self.needs_write() else (),
                                (self,),
                                0)
        if r or e:
            self.on_read()
        if w:
            self.on_write()


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

        self._do_ssl_handshake(timeout=timeout)

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
    ('guest', 'guest', '/', 'localhost', 5672, False)
    >>> parse_amqp_url('amqp://a:b@c:1/d')
    ('a', 'b', 'd', 'c', 1, False)
    >>> parse_amqp_url('amqp://g%20uest:g%20uest@host/vho%20st')
    ('g uest', 'g uest', 'vho st', 'host', 5672, False)
    >>> parse_amqp_url('http://asd')
    Traceback (most recent call last):
      ...
    AssertionError: Only amqp:// and amqps:// protocols are supported.
    >>> parse_amqp_url('amqp://host/%2f')
    ('guest', 'guest', '/', 'host', 5672, False)
    >>> parse_amqp_url('amqp://host/%2fabc')
    ('guest', 'guest', '/abc', 'host', 5672, False)
    >>> parse_amqp_url('amqp://host/')
    ('guest', 'guest', '/', 'host', 5672, False)
    >>> parse_amqp_url('amqp://host')
    ('guest', 'guest', '/', 'host', 5672, False)
    >>> parse_amqp_url('amqp://user:pass@host:10000/vhost')
    ('user', 'pass', 'vhost', 'host', 10000, False)
    >>> parse_amqp_url('amqp://user%61:%61pass@ho%61st:10000/v%2fhost')
    ('usera', 'apass', 'v/host', 'hoast', 10000, False)
    >>> parse_amqp_url('amqp://')
    ('guest', 'guest', '/', 'localhost', 5672, False)
    >>> parse_amqp_url('amqp://:@/') # this is a violation, vhost should be=''
    ('', '', '/', 'localhost', 5672, False)
    >>> parse_amqp_url('amqp://user@/')
    ('user', 'guest', '/', 'localhost', 5672, False)
    >>> parse_amqp_url('amqp://user:@/')
    ('user', '', '/', 'localhost', 5672, False)
    >>> parse_amqp_url('amqp://host')
    ('guest', 'guest', '/', 'host', 5672, False)
    >>> parse_amqp_url('amqp:///vhost')
    ('guest', 'guest', 'vhost', 'localhost', 5672, False)
    >>> parse_amqp_url('amqp://host/')
    ('guest', 'guest', '/', 'host', 5672, False)
    >>> parse_amqp_url('amqp://host/%2f%2f')
    ('guest', 'guest', '//', 'host', 5672, False)
    >>> parse_amqp_url('amqp://[::1]')
    ('guest', 'guest', '/', '::1', 5672, False)
    >>> parse_amqp_url('amqps://user:pass@host:10000/vhost')
    ('user', 'pass', 'vhost', 'host', 10000, True)
    '''
    assert amqp_url.startswith('amqp://') or \
        amqp_url.startswith('amqps://'), \
        "Only amqp:// and amqps:// protocols are supported."
    # urlsplit doesn't know how to parse query when scheme is amqp,
    # we need to pretend we're http'
    o = urlparse.urlsplit('http' + amqp_url[len('amqp'):])
    username = urllib.unquote(o.username) if o.username is not None else 'guest'
    password = urllib.unquote(o.password) if o.password is not None else 'guest'

    path = o.path[1:] if o.path.startswith('/') else o.path
    # We do not support empty vhost case. Empty vhost is treated as
    # '/'. This is mostly for backwards compatibility, and the fact
    # that empty vhost is not very useful.
    vhost = urllib.unquote(path) if path else '/'
    host = urllib.unquote(o.hostname) if o.hostname else 'localhost'
    port = o.port if o.port else 5672
    ssl = o.scheme == 'https'
    return (username, password, vhost, host, port, ssl)

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

def set_close_exec(fd):
    '''
    exec functions (e.g. subprocess.Popen) by default force the child
    process to inherit all file handles which can result in stuck
    connections and unacknowledged messages. Setting FD_CLOEXEC forces
    the handles to be closed first.
    '''
    try:
        import fcntl
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
        fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
    except ImportError:
        pass


class SslConnectionParameters(object):
    def __init__(self):
        self._certfile = None
        self._keyfile = None
        self._ca_certs = None
        self._require_certificate = True

    @property
    def certfile(self):
        return self._certfile

    @certfile.setter
    def certfile(self, value):
        self._certfile = value

    @property
    def keyfile(self):
        return self._keyfile

    @keyfile.setter
    def keyfile(self, value):
        self._keyfile = value

    @property
    def ca_certs(self):
        return self._ca_certs

    @ca_certs.setter
    def ca_certs(self, value):
        self._ca_certs = value

    @property
    def require_certificate(self):
        return self._require_certificate

    @require_certificate.setter
    def require_certificate(self, value):
        self._require_certificate = value
