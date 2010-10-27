from . import channel

class Ticket(object):
    def __init__(self, conn, frames=None, on_channel=None,
                 delay_channel_release=None, assign_channel=None, reentrant=False):
        self.conn = conn
        self.user_callback = None
        self.frames = frames
        self.on_channel = on_channel
        self.channel = assign_channel
        self.to_be_released = False
        self.reentrant = reentrant
        self.callbacks = []

        self.number = self.conn.ticket_number
        self.conn.ticket_number += 1
        self.conn.tickets[self.number] = self

        if not self.channel:
            if self.conn.free_channels:
                self.channel = self.conn.free_channels.pop()
                self.channel.ticket = self
                self._on_channel()
            else:
                self.channel = channel.Channel(self.conn)
                self.channel.ticket = self
                from . import machine # TODO
                machine.channel_open(self, self._on_channel)
        else:
            self.channel.ticket = self

    def _on_channel(self):
        self.on_channel(self)

    def register(self, method_id, callback, *user_args):
        self.channel.register(method_id, lambda result:callback(self, result, *user_args))

    def send_frames(self, frames):
        raw_frames = self.channel.decorate_frames(frames)
        self.conn._send_frames(raw_frames)

    def done(self, result):
        assert self.to_be_released == False
        if not self.reentrant:
            assert len(self.callbacks) == 0
        self.callbacks.append( (self.user_callback, result) )
        self.to_be_released = True
        # TODO: release channel? make sure its' not registered
        self.channel.ticket = None
        self.channel = None
        self.conn.ready_tickets.add( self.number )

    def ping(self, result):
        assert self.to_be_released == False
        assert self.reentrant
        self.callbacks.append( (self.user_callback, result) )
        self.conn.ready_tickets.add( self.number )

    def run_callback(self):
        callback, result = self.callbacks.pop()
        if callback:
            callback(self, result)
        if not self.callbacks:
            self.conn.ready_tickets.remove( self.number )

        if not self.callbacks and self.to_be_released:
            # TODO: release channel? make sure its' not registered
            del self.conn.tickets[self.number]

        return result

