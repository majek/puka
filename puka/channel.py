import array
import logging

from . import machine

log = logging.getLogger('puka')



class ChannelCollection(object):
    channel_max = 65535

    def __init__(self):
        self.channels = {}
        self.free_channels = []
        # Channel 0 is a special case.
        self.free_channel_numbers = [0]
        zero_channel = self.new()
        self.free_channels.append( zero_channel )

    def tune_channel_max(self, new_channel_max):
        new_channel_max = new_channel_max if new_channel_max != 0 else 65535
        self.channel_max = min(self.channel_max, new_channel_max)
        self.free_channel_numbers = array.array('H',
                                                xrange(self.channel_max, 0, -1))
        return self.channel_max

    def new(self):
        # TODO: handle out of channels case
        number = self.free_channel_numbers.pop()
        channel = Channel(number)
        self.channels[number] = channel
        return channel

    def allocate(self, primise, on_channel):
        if self.free_channels:
            channel = self.free_channels.pop()
            channel.primise = primise
            primise.channel = channel
            primise.after_machine_callback = on_channel
        else:
            channel = self.new()
            channel.primise = primise
            primise.channel = channel
            machine.channel_open(primise, on_channel)
        return channel

    def deallocate(self, channel):
        channel.primise.channel = channel.primise = None
        if channel.alive:
            self.free_channels.append( channel )
        else:
            del self.channels[channel.number]
            self.free_channel_numbers.append( channel.number )



class Channel(object):
    alive = False

    def __init__(self, number):
        self.number = number
        self.primise = None
        self._clear_inbound_state()

    def _clear_inbound_state(self):
        self.method_frame = self.props = None
        self.body_chunks = []
        self.body_len = self.body_size = 0


    def inbound_method(self, frame):
        if frame.has_content:
            self.method_frame = frame
        else:
            self._handle_inbound(frame)

    def inbound_props(self, body_size, props):
        self.body_size = body_size
        self.props = props
        if self.body_size == 0: # don't expect body frame
            self.inbound_body('')

    def inbound_body(self, body_chunk):
        self.body_chunks.append( body_chunk )
        self.body_len += len(body_chunk)
        if self.body_len == self.body_size:
            result = self.method_frame
            props = self.props

            result['body'] = ''.join(self.body_chunks)
            result['headers'] = props.get('headers', {})
            # Aint need a reference loop.
            if 'headers' in props:
                del props['headers']
            result['headers'].update( props )
            # Fix delivery_mode.
            result['headers']['persistent'] = \
                result['headers'].get('delivery_mode', 1) == 2

            self._clear_inbound_state()
            return self._handle_inbound(result)

    def _handle_inbound(self, result):
        self.primise.recv_method(result)

