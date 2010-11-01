from . import spec
from . import ticket

####
def connection_handshake(conn):
    t = ticket.Ticket(conn,
                      assign_channel = conn.channels[0],
                      delay_channel_release = Ellipsis)

    t.register(spec.METHOD_CONNECTION_START, _connection_start)
    return t

def _connection_start(t, result):
    assert 'PLAIN' in result['mechanisms'].split(), "Only PLAIN auth supported."
    response = '\0%s\0%s' % (t.conn.username, t.conn.password)
    frames = spec.encode_connection_start_ok({}, 'PLAIN', response, 'en_US')
    t.register(spec.METHOD_CONNECTION_TUNE, _connection_tune)
    t.send_frames(frames)
    t.cached_result = result

def _connection_tune(t, result):
    t.conn.frame_max = min(t.conn.frame_max, result['frame_max'])

    channel_max = result['channel_max']
    if channel_max == 0:
        channel_max = 65535
    t.conn.channel_max = channel_max
    # TODO put that on the connection.
    t.conn.free_channel_numbers = range(channel_max, 0, -1)

    f1 = spec.encode_connection_tune_ok(channel_max, t.conn.frame_max, 0)
    f2 = spec.encode_connection_open(t.conn.vhost)
    t.register(spec.METHOD_CONNECTION_OPEN_OK, _connection_open_ok)
    t.send_frames(list(f1) + list(f2))

def _connection_open_ok(t, result):
    t.done(t.cached_result)    # Never free the ticket and channel.



###
def channel_open(t, callback):
    f = spec.encode_channel_open('')
    t.register(spec.METHOD_CHANNEL_OPEN_OK,
               _channel_open_ok, callback)
    t.send_frames(f)

def _channel_open_ok(t, result, callback):
    callback()



####
def queue_declare(conn, queue='', auto_delete=False, arguments={}):
    frames = spec.encode_queue_declare(queue, False, not auto_delete,
                                       False, auto_delete, arguments)
    return ticket.Ticket(conn,
                         frames = frames,
                         on_channel = _queue_declare,
                         delay_channel_release = 5000 if auto_delete else None,
                         )

def _queue_declare(t):
    t.register(spec.METHOD_QUEUE_DECLARE_OK, _queue_declare_ok)
    t.send_frames(t.frames)

def _queue_declare_ok(t, result):
    t.done(result)


####
def basic_publish(conn, exchange, routing_key, user_headers={}, body=''):
    frames = spec.encode_basic_publish(exchange, routing_key, False, False,
                                       user_headers, body, conn.frame_max)
    return ticket.Ticket(conn,
                         frames = frames,
                         on_channel = _basic_publish,
                         )

def _basic_publish(t):
    f = spec.encode_channel_flow(True)
    t.register(spec.METHOD_CHANNEL_FLOW_OK, _basic_publish_channel_flow_ok)
    t.send_frames(list(t.frames) + list(f))

def _basic_publish_channel_flow_ok(t, result):
    # TODO: handle publish errors
    t.done({})


####
def basic_consume(conn, queue, prefetch_size=0, prefetch_count=0,
                  no_local=False, no_ack=False, exclusive=False,
                  arguments={}):
    frames_qos = spec.encode_basic_qos(prefetch_size, prefetch_count, False)
    t = ticket.Ticket(conn,
                      frames = frames_qos,
                      on_channel = _basic_qos,
                      reentrant = True,
                      )
    t.frames_consume = spec.encode_basic_consume(queue, '', no_local, no_ack,
                                                 exclusive, arguments)
    return t

def _basic_qos(t):
    t.register(spec.METHOD_BASIC_QOS_OK, _basic_qos_ok)
    t.send_frames(t.frames)

def _basic_qos_ok(t, result):
    t.register(spec.METHOD_BASIC_CONSUME_OK, _basic_consume_ok)
    t.send_frames(t.frames_consume)

def _basic_consume_ok(t, result):
    t.register(spec.METHOD_BASIC_DELIVER, _basic_deliver)

def _basic_deliver(t, msg_result):
    t.register(spec.METHOD_BASIC_DELIVER, _basic_deliver)
    msg_result.update({'ticket_number': t.number})
    t.ping( msg_result )


####
def basic_ack(conn, msg_result):
    ticket_number = msg_result['ticket_number']
    f = spec.encode_basic_ack(msg_result['delivery_tag'], False)
    t = conn.tickets[ticket_number]
    t.send_frames(f)
    return t


####
def basic_get(conn, queue, no_ack=False):
    frames = spec.encode_basic_get(queue, no_ack)
    return ticket.Ticket(conn,
                         frames = frames,
                         on_channel = _basic_get,
                         )

def _basic_get(t):
    t.register(spec.METHOD_BASIC_GET_OK, _basic_get_ok)
    t.register(spec.METHOD_BASIC_GET_EMPTY, _basic_get_empty)
    t.send_frames(t.frames)


def _basic_get_ok(t, result):
    t.unregister(spec.METHOD_BASIC_GET_EMPTY)
    # TODO: ref count acks
    t.done(result)

def _basic_get_empty(t, result):
    t.unregister(spec.METHOD_BASIC_GET_OK)
    t.done(result)


####
def queue_purge(conn, queue):
    frames = spec.encode_queue_purge(queue)
    return ticket.Ticket(conn,
                         frames = frames,
                         on_channel = _queue_purge,
                         )

def _queue_purge(t):
    t.register(spec.METHOD_QUEUE_PURGE_OK, _queue_purge_ok)
    t.send_frames(t.frames)

def _queue_purge_ok(t, result):
    t.done(result)


####
def queue_delete(conn, queue, if_unused=False, if_empty=False):
    frames = spec.encode_queue_delete(queue, if_unused, if_empty)
    return ticket.Ticket(conn,
                         frames = frames,
                         on_channel = _queue_delete,
                         )

def _queue_delete(t):
    t.register(spec.METHOD_QUEUE_DELETE_OK, _queue_delete_ok)
    t.send_frames(t.frames)

def _queue_delete_ok(t, result):
    t.done(result)
