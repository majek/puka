import logging

from . import channel
from . import spec
from . import exceptions

log = logging.getLogger('puka')



class PromiseCollection(object):
    def __init__(self, conn):
        self.conn = conn
        self._promises = {}
        self.promise_number = 1
        self.ready = set()

    def new(self, on_channel, **kwargs):
        number = self.promise_number
        self.promise_number += 1
        promise = Promise(self.conn, number, on_channel, **kwargs)
        self._promises[number] = promise
        return promise

    def free(self, promise):
        del self._promises[promise.number]

    def mark_ready(self, promise):
        self.ready.add( promise.number )

    def unmark_ready(self, promise):
        self.ready.remove( promise.number )

    def run_callback(self, number, **kwargs):
        return self._promises[number].run_callback(**kwargs)

    def by_number(self, number):
        return self._promises[number]

    def all(self):
        return self._promises.values()


class Promise(object):
    to_be_released = False
    delay_release = None
    user_callback = None
    after_machine_callback = None
    refcnt = 0

    def __init__(self, conn, number, on_channel, reentrant=False,
                 no_channel=False):
        self.number = number
        self.conn = conn
        self.on_channel = on_channel
        self.reentrant = reentrant

        self.methods = {}
        self.callbacks = []

        if not no_channel:
            self.conn.channels.allocate(self, self._on_channel)
        else:
            self.channel = None
            self.after_machine_callback = self._on_channel

    def restore_error_handler(self):
        self.register(spec.METHOD_CHANNEL_CLOSE, self._on_channel_close)

    def _on_channel(self):
        if self.channel:
            self.channel.alive = True
        self.restore_error_handler()
        self.on_channel(self)

    def _on_channel_close(self, _t, result):
        # log.warn('channel %i died %r', self.channel.number, result)
        exceptions.mark_frame(result)
        self.send_frames(spec.encode_channel_close_ok())
        if self.channel:
            self.channel.alive = False
        self.done(result)

    def recv_method(self, result):
        # log.debug('#%i recv_method %r', self.number, result)
        # In this order, to allow callback to re-register to the same method.
        callback = self.methods[result.method_id]
        del self.methods[result.method_id]
        callback(self, result)

    def register(self, method_id, callback):
        self.methods[method_id] = callback

    def unregister(self, method_id):
        del self.methods[method_id]


    def send_frames(self, frames):
        self.conn._send_frames(self.channel.number, frames)


    def done(self, result, delay_release=None, no_callback=False):
        # log.debug('#%i done %r', self.number, result)
        assert self.to_be_released == False
        if not self.reentrant:
            assert len(self.callbacks) == 0
        if not no_callback:
            self.callbacks.append( (self.user_callback, result) )
        else:
            self.callbacks.append( (None, result) )
        self.conn.promises.mark_ready(self)
        self.to_be_released = True
        self.delay_release = delay_release
        self.methods.clear()
        self.restore_error_handler()

    def ping(self, result):
        assert self.to_be_released == False
        assert self.reentrant
        self.callbacks.append( (self.user_callback, result) )
        self.conn.promises.mark_ready(self)


    def run_callback(self, raise_errors=True):
        user_callback, result = self.callbacks.pop(0)
        if not self.callbacks:
            self.conn.promises.unmark_ready(self)
        if user_callback:
            user_callback(self.number, result)
            # We may be already freed now.
            # (consider basic_get + basic_ack inside callback)

        self.maybe_release()
        if raise_errors and result.is_error:
            raise result.exception
        return result


    def after_machine(self):
        if self.after_machine_callback:
            self.after_machine_callback()
            self.after_machine_callback = None

    def refcnt_inc(self):
        self.refcnt += 1

    def refcnt_dec(self):
        assert self.refcnt > 0
        self.refcnt -= 1
        if self.refcnt == 0:
            self.maybe_release()

    def refcnt_clear(self):
        self.refcnt = 0

    def maybe_release(self):
        # If not released yet, not used by callbacks, and not refcounted.
        if ( self.number is not None and not self.callbacks and
             self.to_be_released and self.refcnt == 0):
            self._release()

    def _release(self):
        # Release channel and unlink self.
        if self.delay_release is None:
            if self.channel:
                self.conn.channels.deallocate(self.channel)
            self.conn.promises.free(self)
        elif self.delay_release is Ellipsis:
            # Never free.
            pass
        else:
            # TODO:
            print "Unable to free channel %i (promise %i)" % \
                (self.channel.number, self.number)
        self.number = None
