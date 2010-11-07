import logging

from . import channel
from . import spec
from . import exceptions

log = logging.getLogger('puka')



class TicketCollection(object):
    def __init__(self, conn):
        self.conn = conn
        self._tickets = {}
        self.ticket_number = 1
        self.ready = set()

    def new(self, on_channel, **kwargs):
        number = self.ticket_number
        self.ticket_number += 1
        ticket = Ticket(self.conn, number, on_channel, **kwargs)
        self._tickets[number] = ticket
        return ticket

    def free(self, ticket):
        del self._tickets[ticket.number]

    def mark_ready(self, ticket):
        self.ready.add( ticket.number )

    def unmark_ready(self, ticket):
        self.ready.remove( ticket.number )

    def run_callback(self, number, **kwargs):
        return self._tickets[number].run_callback(**kwargs)

    def by_number(self, number):
        return self._tickets[number]

    def all(self):
        return self._tickets.values()


class Ticket(object):
    to_be_released = False
    delay_release = None
    user_callback = None
    user_data = None
    after_machine_callback = None
    refcnt = 0

    def __init__(self, conn, number, on_channel, reentrant=False):
        self.number = number
        self.conn = conn
        self.on_channel = on_channel
        self.reentrant = reentrant

        self.methods = {}
        self.callbacks = []

        self.conn.channels.allocate(self, self._on_channel)

    def restore_error_handler(self):
        self.register(spec.METHOD_CHANNEL_CLOSE, self._on_channel_close)

    def _on_channel(self):
        self.channel.alive = True
        self.restore_error_handler()
        self.on_channel(self)

    def _on_channel_close(self, _t, result):
        # log.warn('channel %i died %r', self.channel.number, result)
        exceptions.mark_frame(result)
        self.send_frames(spec.encode_channel_close_ok())
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


    def done(self, result, delay_release=None):
        assert self.to_be_released == False
        if not self.reentrant:
            assert len(self.callbacks) == 0
        self.callbacks.append( (self.user_callback, self.user_data, result) )
        self.to_be_released = True
        self.delay_release = delay_release
        self.conn.tickets.mark_ready(self)

    def ping(self, result):
        assert self.to_be_released == False
        assert self.reentrant
        self.callbacks.append( (self.user_callback, self.user_data, result) )
        self.conn.tickets.mark_ready(self)


    def run_callback(self, raise_errors=True):
        user_callback, user_data, result = self.callbacks.pop(0)
        if user_callback:
            user_callback(self.number, result, user_data)
            # At this point, after callback, self might be already freed.
        if not self.callbacks:
            self.conn.tickets.unmark_ready(self)

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
        self.refcnt -= 1
        if self.refcnt == 0:
            self.maybe_release()

    def maybe_release(self):
        # If not released yet, not used by callbacks, and not refcounted.
        if (self.channel and not self.callbacks and
            self.to_be_released and self.refcnt == 0):
            self._release()

    def _release(self):
        # Release channel and unlink self.
        if self.delay_release is None:
            self.conn.channels.deallocate(self.channel)
            self.conn.tickets.free(self)
        elif self.delay_release is Ellipsis:
            # Never free.
            pass
        else:
            # TODO:
            print "Unable to free channel %i (ticket %i)" % \
                (self.channel.number, self.number)

