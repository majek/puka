import logging

from . import exceptions
from . import spec

log = logging.getLogger('puka')


####
def connection_handshake(conn):
    # Bypass conn._send, we want the socket to be writable first.
    conn.send_buf.write(spec.PREAMBLE)
    t = conn.tickets.new(_connection_handshake, reentrant=True)
    conn.x_connection_ticket = t
    return t

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
    t.x_cached_result = result

def _connection_tune(t, result):
    frame_max = t.conn.tune_frame_max(result['frame_max'])
    channel_max = t.conn.channels.tune_channel_max(result['channel_max'])

    t.register(spec.METHOD_CONNECTION_OPEN_OK, _connection_open_ok)
    f1 = spec.encode_connection_tune_ok(channel_max, frame_max, 0)
    f2 = spec.encode_connection_open(t.conn.vhost)
    t.send_frames(list(f1) + list(f2))

def _connection_open_ok(ct, result):
    ct.register(spec.METHOD_CONNECTION_CLOSE, _connection_close)
    # Never free the ticket and channel.
    ct.ping(ct.x_cached_result)
    ct.conn.x_connection_ticket = ct
    publish_ticket(ct.conn)

def publish_ticket(conn):
    pt = conn.tickets.new(_pt_channel_open_ok)
    pt.x_async_id = 0
    pt.x_async_map = {}
    conn.x_publish_ticket = pt

def _pt_channel_open_ok(pt, _result=None):
    pt.register(spec.METHOD_CHANNEL_CLOSE, _pt_channel_close)
    pt.register(spec.METHOD_BASIC_RETURN, _pt_basic_return)

def fix_basic_publish_headers(headers):
    nheaders = {}
    nheaders.update(headers) # copy
    if nheaders.get('persistent', True):
        nheaders['delivery_mode'] = 2
    # That's not a good idea.
    assert 'headers' not in headers
    return nheaders

def basic_publish_async(conn, exchange, routing_key, mandatory=False,
                        immediate=False, headers={}, body=''):
    pt = conn.x_publish_ticket
    async_id = pt.x_async_id
    pt.x_async_id += 1

    nheaders = fix_basic_publish_headers(headers)
    assert 'x-puka-async-id' not in nheaders
    nheaders['x-puka-async-id'] = async_id
    eheaders = {'x-puka-async-id': async_id, 'x-puka-footer': True}

    # TODO: channel not needed.
    t = conn.tickets.new(_basic_publish_async)
    t.x_frames = (list(spec.encode_basic_publish(exchange, routing_key,
                                                 mandatory, immediate, nheaders,
                                                 body, conn.frame_max)) +
                  list(spec.encode_basic_publish("", "", False, True, eheaders,
                                                 "", conn.frame_max)))
    pt.x_async_map[async_id] = t
    return t

def _basic_publish_async(t):
    pt = t.conn.x_publish_ticket
    pt.send_frames(t.x_frames)

def _pt_basic_return(pt, result):
    pt.register(spec.METHOD_BASIC_RETURN, _pt_basic_return)
    async_id = result['headers']['x-puka-async-id']
    if 'x-puka-footer' in result['headers']:
        if async_id in pt.x_async_map:
            # Success
            t = pt.x_async_map[async_id]
            t.done(spec.Frame())
            del pt.x_async_map[async_id]
        else:
            # Failure was already handled before
            pass
    else: # Failure, user message returned.
        assert async_id in pt.x_async_map, "Oops. Ordering went realy wrong."
        t = pt.x_async_map[async_id]
        exceptions.mark_frame(result)
        t.done(result)
        del pt.x_async_map[async_id]

def _pt_channel_close(pt, result):
    # Start off with reestablishing the channel
    pt.register(spec.METHOD_CHANNEL_OPEN_OK, _pt_channel_open_ok)
    pt.send_frames( list(spec.encode_channel_close_ok()) +
                    list(spec.encode_channel_open('')))
    # All the publishes need to be marked as failed.
    exceptions.mark_frame(result)
    for async_id, t in pt.x_async_map.items():
        t.done(result)
        del pt.x_async_map[async_id]


def _connection_close(t, result):
    exceptions.mark_frame(result)
    t.ping(result)
    # Explode, kill everything.
    log.error('Connection killed with %r', result)
    t.conn._shutdown(result)

def connection_close(conn):
    t = conn.x_connection_ticket
    t.register(spec.METHOD_CONNECTION_CLOSE_OK, _connection_close_ok)
    t.send_frames(spec.encode_connection_close(200, '', 0, 0))
    return t

def _connection_close_ok(t, result):
    t.ping(result)
    t.conn._shutdown(result)


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
                  headers={}, body=''):
    # After the publish there is a need to synchronize state. Currently,
    # we're using dummy channel.flow, in future that should be dropped
    # in favor for publisher-acks.
    # There are three return-paths:
    #   - channel_flow_ok (ok)
    #   - channel-close  (error)
    #   - basic_return (not delivered)
    nheaders = fix_basic_publish_headers(headers)

    t = conn.tickets.new(_basic_publish)
    t.x_frames = spec.encode_basic_publish(exchange, routing_key, mandatory,
                                           immediate, nheaders, body,
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
    t = conn.tickets.new(_bc_basic_qos, reentrant=True)
    t.x_frames = spec.encode_basic_qos(prefetch_size, prefetch_count, False)
    t.frames_consume = spec.encode_basic_consume(queue, '', no_local, no_ack,
                                                 exclusive, arguments)
    t.x_no_ack = no_ack
    return t

def _bc_basic_qos(t):
    t.register(spec.METHOD_BASIC_QOS_OK, _bc_basic_qos_ok)
    t.send_frames(t.x_frames)

def _bc_basic_qos_ok(t, result):
    t.register(spec.METHOD_BASIC_CONSUME_OK, _bc_basic_consume_ok)
    t.send_frames(t.frames_consume)

def _bc_basic_consume_ok(t, consume_result):
    t.x_consumer_tag = consume_result['consumer_tag']
    t.register(spec.METHOD_BASIC_DELIVER, _bc_basic_deliver)

def _bc_basic_deliver(t, msg_result):
    t.register(spec.METHOD_BASIC_DELIVER, _bc_basic_deliver)
    msg_result['ticket_number'] = t.number
    if t.x_no_ack is False:
        t.refcnt_inc()
    t.ping(msg_result)

##
def basic_ack(conn, msg_result):
    t = conn.tickets.by_number(msg_result['ticket_number'])
    t.send_frames( spec.encode_basic_ack(msg_result['delivery_tag'], False) )
    assert t.x_no_ack is False
    t.refcnt_dec()
    return t

##
def basic_reject(conn, msg_result):
    t = conn.tickets.by_number(msg_result['ticket_number'])
    # For basic.reject requeue must be True.
    t.send_frames( spec.encode_basic_reject(msg_result['delivery_tag'], True) )
    assert t.x_no_ack is False
    t.refcnt_dec()
    return t

##
def basic_qos(conn, consume_ticket_number, prefetch_size=0, prefetch_count=0):
    # TODO: new channel not required
    # TODO: race?
    t = conn.tickets.new(_basic_qos)
    t.x_ct = conn.tickets.by_number(consume_ticket_number)
    t.x_frames = spec.encode_basic_qos(prefetch_size, prefetch_count, False)
    return t

def _basic_qos(t):
    ct = t.x_ct
    ct.register(spec.METHOD_BASIC_QOS_OK, _basic_qos_ok)
    ct.send_frames( t.x_frames )
    ct.x_qos_ticket = t

def _basic_qos_ok(ct, result):
    t = ct.x_qos_ticket
    t.done(result)

##
def basic_cancel(conn, consume_ticket_number):
    # TODO: new channel not required
    # TODO: race?
    t = conn.tickets.new(_basic_cancel)
    t.x_ct = conn.tickets.by_number(consume_ticket_number)
    t.x_frames = spec.encode_basic_cancel(t.x_ct.x_consumer_tag)
    return t

def _basic_cancel(t):
    t.x_ct.register(spec.METHOD_BASIC_CANCEL_OK, _basic_cancel_ok)
    t.x_ct.send_frames( t.x_frames )
    t.x_ct.refcnt_clear()
    t.x_ct.x_mt = t
    t.x_ct = None

def _basic_cancel_ok(ct, result):
    ct.x_mt.done(result)
    ct.x_mt = None
    ct.done(None, no_callback=True)


####
def basic_get(conn, queue, no_ack=False):
    t = conn.tickets.new(_basic_get)
    t.x_frames = spec.encode_basic_get(queue, no_ack)
    t.x_no_ack = no_ack
    return t

def _basic_get(t):
    t.register(spec.METHOD_BASIC_GET_OK, _basic_get_ok)
    t.register(spec.METHOD_BASIC_GET_EMPTY, _basic_get_empty)
    t.send_frames(t.x_frames)

def _basic_get_ok(t, msg_result):
    msg_result['ticket_number'] = t.number
    if t.x_no_ack is False:
        t.refcnt_inc()
    t.done(msg_result)

def _basic_get_empty(t, result):
    result['empty'] = True
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
def _generic_callback(t):
    t.register(t.x_method, _generic_callback_ok)
    t.send_frames(t.x_frames)

def _generic_callback_ok(t, result):
    t.done(result)

####
def exchange_delete(conn, exchange, if_unused=False):
    t = conn.tickets.new(_generic_callback)
    t.x_method = spec.METHOD_EXCHANGE_DELETE_OK
    t.x_frames = spec.encode_exchange_delete(exchange, if_unused)
    return t

def exchange_bind(conn, destination, source, routing_key='', arguments={}):
    t = conn.tickets.new(_generic_callback)
    t.x_method = spec.METHOD_EXCHANGE_BIND_OK
    t.x_frames = spec.encode_exchange_bind(destination, source, routing_key,
                                           arguments)
    return t

def exchange_unbind(conn, destination, source, routing_key='', arguments={}):
    t = conn.tickets.new(_generic_callback)
    t.x_method = spec.METHOD_EXCHANGE_UNBIND_OK
    t.x_frames = spec.encode_exchange_unbind(destination, source, routing_key,
                                             arguments)
    return t

def queue_delete(conn, queue, if_unused=False, if_empty=False):
    t = conn.tickets.new(_generic_callback)
    t.x_method = spec.METHOD_QUEUE_DELETE_OK
    t.x_frames = spec.encode_queue_delete(queue, if_unused, if_empty)
    return t

def queue_purge(conn, queue):
    t = conn.tickets.new(_generic_callback)
    t.x_method = spec.METHOD_QUEUE_PURGE_OK
    t.x_frames = spec.encode_queue_purge(queue)
    return t

def queue_bind(conn, queue, exchange, routing_key='', arguments={}):
    t = conn.tickets.new(_generic_callback)
    t.x_method = spec.METHOD_QUEUE_BIND_OK
    t.x_frames = spec.encode_queue_bind(queue, exchange, routing_key,
                                        arguments)
    return t

def queue_unbind(conn, queue, exchange, routing_key='', arguments={}):
    t = conn.tickets.new(_generic_callback)
    t.x_method = spec.METHOD_QUEUE_UNBIND_OK
    t.x_frames = spec.encode_queue_unbind(queue, exchange, routing_key,
                                          arguments)
    return t

