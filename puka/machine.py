import logging

from . import exceptions
from . import spec

log = logging.getLogger('puka')


####
def connection_handshake(conn):
    conn._send(spec.PREAMBLE)
    return conn.tickets.new(_connection_handshake)

def _connection_handshake(t):
    assert t.channel.number == 0
    t.register(spec.METHOD_CONNECTION_START, _connection_start)

def _connection_start(t, result):
    # log.info("Connected to %r", result['server_properties'])
    assert 'PLAIN' in result['mechanisms'].split(), "Only PLAIN auth supported."
    response = '\0%s\0%s' % (t.conn.username, t.conn.password)
    frames = spec.encode_connection_start_ok({'product': 'Puka'}, 'PLAIN',
                                             response, 'en_US')
    t.register(spec.METHOD_CONNECTION_TUNE, _connection_tune)
    t.send_frames(frames)
    t.cached_result = result

def _connection_tune(t, result):
    frame_max = t.conn.tune_frame_max(result['frame_max'])
    channel_max = t.conn.channels.tune_channel_max(result['channel_max'])

    t.register(spec.METHOD_CONNECTION_OPEN_OK, _connection_open_ok)
    f1 = spec.encode_connection_tune_ok(channel_max, frame_max, 0)
    f2 = spec.encode_connection_open(t.conn.vhost)
    t.send_frames(list(f1) + list(f2))

def _connection_open_ok(t, result):
    # Never free the ticket and channel.
    t.done(t.cached_result, delay_release=Ellipsis)
    t.register(spec.METHOD_CONNECTION_CLOSE, _connection_close)

def _connection_close(t, result):
    exceptions.mark_frame(result)
    log.error('Connection killed: %r', result)


####
def channel_open(t, callback):
    t.register(spec.METHOD_CHANNEL_OPEN_OK, _channel_open_ok)
    t.x_callback = callback
    t.send_frames( spec.encode_channel_open('') )

def _channel_open_ok(t, result):
    t.x_callback()


####
def queue_declare(conn, queue='', auto_delete=False, exclusive=False,
                  arguments={}, suicidal=False):
    # Don't use AMQP auto-delete. Use RabbitMQ queue-leases instead:
    # http://www.rabbitmq.com/extensions.html#queue-leases
    # durable = not auto_delete
    if not suicidal:
        t = conn.tickets.new(_queue_declare_passive)
    else:
        t = conn.tickets.new(_queue_declare_suicidal)
    args = {}
    if auto_delete:
        args['x-expires'] = 5000
    args.update( arguments )
    t.x_frames_passive = spec.encode_queue_declare(queue, True, not auto_delete,
                                                   exclusive, False, args)
    t.x_frames = spec.encode_queue_declare(queue, False, not auto_delete,
                                           exclusive, False, args)
    return t

def _queue_declare_passive(t):
    t.register(spec.METHOD_CHANNEL_CLOSE, _queue_declare_passive_fail)
    t.register(spec.METHOD_QUEUE_DECLARE_OK, _queue_declare_passive_ok)
    t.send_frames(t.x_frames_passive)

def _queue_declare_passive_ok(t, result):
    # Queue exists.
    result['exists'] = True
    t.done(result)

def _queue_declare_passive_fail(t, result):
    # Doesn't exist. 1. recreate channel. 2.
    t.restore_error_handler()
    t.register(spec.METHOD_CHANNEL_OPEN_OK, _queue_declare_suicidal)
    t.send_frames(
        list(spec.encode_channel_close_ok()) +
        list(spec.encode_channel_open('')) )

def _queue_declare_suicidal(t, result=None):
    t.register(spec.METHOD_QUEUE_DECLARE_OK, _queue_declare_ok)
    t.send_frames(t.x_frames)

def _queue_declare_ok(t, result):
    t.done(result)


####
def basic_publish(conn, exchange, routing_key, mandatory=False, immediate=False,
                  user_headers={}, body=''):
    # After the publish there is a need to synchronize state. Currently,
    # we're using dummy channel.flow, in future that should be dropped
    # in favor for publisher-acks.
    # There are three return-paths:
    #   - channel_flow_ok (ok)
    #   - channel-close  (error)
    #   - basic_return (not delivered)
    t = conn.tickets.new(_basic_publish)
    t.x_frames = spec.encode_basic_publish(exchange, routing_key, mandatory,
                                           immediate, user_headers, body,
                                           conn.frame_max)
    t.x_result = spec.Frame()
    t.x_result['empty'] = True
    return t

def _basic_publish(t):
    t.register(spec.METHOD_CHANNEL_FLOW_OK, _basic_publish_channel_flow_ok)
    t.register(spec.METHOD_BASIC_RETURN, _basic_publish_return)
    t.send_frames(
        list(t.x_frames) +
        list(spec.encode_channel_flow(True)) )

def _basic_publish_channel_flow_ok(t, result):
    # Publish went fine or we're after basic_return.
    t.done(t.x_result)

def _basic_publish_return(t, result):
    exceptions.mark_frame(result)
    t.x_result = result


####
def basic_consume(conn, queue, prefetch_size=0, prefetch_count=0,
                  no_local=False, no_ack=False, exclusive=False,
                  arguments={}):
    t = conn.tickets.new(_basic_qos, reentrant = True)
    t.x_frames = spec.encode_basic_qos(prefetch_size, prefetch_count, False)
    t.frames_consume = spec.encode_basic_consume(queue, '', no_local, no_ack,
                                                 exclusive, arguments)
    return t

def _basic_qos(t):
    t.register(spec.METHOD_BASIC_QOS_OK, _basic_qos_ok)
    t.send_frames(t.x_frames)

def _basic_qos_ok(t, result):
    t.register(spec.METHOD_BASIC_CONSUME_OK, _basic_consume_ok)
    t.send_frames(t.frames_consume)

def _basic_consume_ok(t, result):
    t.register(spec.METHOD_BASIC_DELIVER, _basic_deliver)

def _basic_deliver(t, msg_result):
    t.register(spec.METHOD_BASIC_DELIVER, _basic_deliver)
    msg_result['ticket_number'] = t.number
    t.refcnt_inc()
    t.ping( msg_result )


####
def basic_ack(conn, msg_result):
    ticket_number = msg_result['ticket_number']
    t = conn.tickets.by_number(ticket_number)
    t.send_frames( spec.encode_basic_ack(msg_result['delivery_tag'], False) )
    t.refcnt_dec()
    return t


####
def basic_get(conn, queue, no_ack=False):
    t = conn.tickets.new(_basic_get)
    t.x_frames = spec.encode_basic_get(queue, no_ack)
    return t

def _basic_get(t):
    t.register(spec.METHOD_BASIC_GET_OK, _basic_get_ok)
    t.register(spec.METHOD_BASIC_GET_EMPTY, _basic_get_empty)
    t.send_frames(t.x_frames)

def _basic_get_ok(t, msg_result):
    msg_result['ticket_number'] = t.number
    t.refcnt_inc()
    t.done(msg_result)

def _basic_get_empty(t, result):
    t.done(result)


####
def queue_purge(conn, queue):
    t = conn.tickets.new(_queue_purge)
    t.x_frames = spec.encode_queue_purge(queue)
    return t

def _queue_purge(t):
    t.register(spec.METHOD_QUEUE_PURGE_OK, _queue_purge_ok)
    t.send_frames(t.x_frames)

def _queue_purge_ok(t, result):
    t.done(result)


####
def queue_delete(conn, queue, if_unused=False, if_empty=False):
    t = conn.tickets.new(_queue_delete)
    t.x_frames = spec.encode_queue_delete(queue, if_unused, if_empty)
    return t

def _queue_delete(t):
    t.register(spec.METHOD_QUEUE_DELETE_OK, _queue_delete_ok)
    t.send_frames(t.x_frames)

def _queue_delete_ok(t, result):
    t.done(result)


####
def exchange_declare(conn, exchange, type='direct', arguments={},
                     suicidal=False):
    # Exchanges don't support 'x-expires', so we support only durable exchanges.
    auto_delete = False
    if not suicidal:
        t = conn.tickets.new(_exchange_declare_passive)
    else:
        t = conn.tickets.new(_exchange_declare_suicidal)

    t.x_frames_passive = spec.encode_exchange_declare(exchange, type, True,
                                                      not auto_delete,
                                                      auto_delete, False,
                                                      arguments)
    t.x_frames = spec.encode_exchange_declare(exchange, type, False,
                                              not auto_delete,
                                              auto_delete, False, arguments)
    return t

def _exchange_declare_passive(t):
    t.register(spec.METHOD_CHANNEL_CLOSE, _exchange_declare_passive_fail)
    t.register(spec.METHOD_EXCHANGE_DECLARE_OK, _exchange_declare_passive_ok)
    t.send_frames(t.x_frames_passive)

def _exchange_declare_passive_ok(t, result):
    # Exchange exists.
    result['exists'] = True
    t.done(result)

def _exchange_declare_passive_fail(t, result):
    # Doesn't exist. 1. recreate channel. 2.
    t.restore_error_handler()
    t.register(spec.METHOD_CHANNEL_OPEN_OK, _exchange_declare_suicidal)
    t.send_frames(
        list(spec.encode_channel_close_ok()) +
        list(spec.encode_channel_open('')) )

def _exchange_declare_suicidal(t, result=None):
    t.register(spec.METHOD_EXCHANGE_DECLARE_OK, _exchange_declare_ok)
    t.send_frames(t.x_frames)

def _exchange_declare_ok(t, result):
    t.done(result)


####
def exchange_delete(conn, exchange, if_unused=False):
    t = conn.tickets.new(_exchange_delete)
    t.x_frames = spec.encode_exchange_delete(exchange, if_unused)
    return t

def _exchange_delete(t):
    t.register(spec.METHOD_EXCHANGE_DELETE_OK, _exchange_delete_ok)
    t.send_frames(t.x_frames)

def _exchange_delete_ok(t, result):
    t.done(result)





