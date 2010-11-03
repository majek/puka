from . import channel

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

    def run_callback(self, number):
        return self._tickets[number].run_callback()

    def by_number(self, number):
        return self._tickets[number]

class Ticket(object):
    to_be_released = False
    delay_release = None
    user_callback = None
    user_data = None
    after_machine_callback = None

    def __init__(self, conn, number, on_channel, reentrant=False):
        self.number = number
        self.conn = conn
        self.on_channel = on_channel
        self.reentrant = reentrant

        self.methods = {}
        self.callbacks = []

        self.conn.channels.allocate(self, self._on_channel)

    def _on_channel(self):
        self.on_channel(self)


    def recv_method(self, result):
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


    def run_callback(self):
        user_callback, user_data, result = self.callbacks.pop(0)
        if user_callback:
            user_callback(self, result, user_data)
        if not self.callbacks:
            self.conn.tickets.unmark_ready(self)

        if not self.callbacks and self.to_be_released:
            # Release channel and free ticket.
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
        return result


    def after_machine(self):
        if self.after_machine_callback:
            self.after_machine_callback()
            self.after_machine_callback = None
