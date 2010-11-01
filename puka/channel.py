import struct

class Channel(object):
    def __init__(self, conn, number=None):
        self.conn = conn
        if number is not None:
            self.number = number
        else:
            self.number = self.conn.free_channel_numbers.pop()
        self.conn.channels[self.number] = self

        self.methods = {}
        self.result = None
        self.props = None
        self.body_chunks = []
        self.body_len = 0
        self.body_size = 0

    def _full_frame(self, frame):
        # In this order, to allow handler to re-register to the same event.
        handler = self.methods[frame.method_id]
        del self.methods[frame.method_id]
        handler(frame)

    def consume_method(self, frame):
        if frame.has_content:
            self.result = frame
        else:
            self._full_frame(frame)

    def consume_props(self, body_size, props):
        self.body_size = body_size
        self.props = props

    def consume_body(self, body_chunk):
        self.body_chunks.append( body_chunk )
        self.body_len += len(body_chunk)
        if self.body_len == self.body_size:
            result = self.result
            props = self.props
            body = ''.join(self.body_chunks)
            self.result = self.props = None
            self.body_chunks = []
            self.body_len = self.body_size = 0

            if 'headers' in props:
                headers = props['headers']
                del props['headers']
            else:
                headers = {}
            result['headers'] = headers
            result['headers'].update( props )
            result['body'] = body

            return self._full_frame(result)


    def register(self, method_id, callback):
        self.methods[method_id] = callback

    def unregister(self, method_id):
        del self.methods[method_id]

    def decorate_frames(self, frames):
        format = lambda frame_type, payload:''.join((struct.pack('!BHI',
                                                                 frame_type,
                                                                 self.number,
                                                                 len(payload)),
                                                     payload, '\xCE'))
        return [format(t, f) for t, f in frames]
