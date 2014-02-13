import copy
import logging

from . import exceptions
from . import spec
from . import ordereddict

log = logging.getLogger('puka')

def _nothing(t):
    pass

####
def connection_handshake(conn):
    # Bypass conn._send, we want the socket to be writable first.
    conn.send_buf.write(spec.PREAMBLE)
    t = conn.promises.new(_connection_handshake, reentrant=True)
    conn.x_connection_promise = t
    return t

def _connection_handshake(t):
    assert t.channel.number == 0
    t.register(spec.METHOD_CONNECTION_START, _connection_start)

def _connection_start(t, result):
    # log.info("Connected to %r", result['server_properties'])
    assert 'PLAIN' in result['mechanisms'].split(), "Only PLAIN auth supported."
    response = '\0%s\0%s' % (t.conn.username, t.conn.password)
    scapa = result['server_properties'].get('capabilities', {})
    ccapa = {}
    if scapa.get('consumer_cancel_notify'):
        ccapa['consumer_cancel_notify'] = True

    properties = {'product': 'Puka', 'capabilities': ccapa}
    if t.conn.client_properties is not None:
        properties.update(t.conn.client_properties)

    frames = spec.encode_connection_start_ok(properties,
                                    'PLAIN', response, 'en_US')
    t.register(spec.METHOD_CONNECTION_TUNE, _connection_tune)
    t.send_frames(frames)
    t.x_cached_result = result
    t.conn.x_server_props = result['server_properties']
    try:
        t.conn.x_server_version = \
            map(int, t.conn.x_server_props['version'].split('.'))
    except ValueError:
        t.conn.x_server_version = (Ellipsis,)
    if t.conn.pubacks is None:
        t.conn.x_pubacks = scapa.get('publisher_confirms', False)
    else:
        t.conn.x_pubacks = t.conn.pubacks


def _connection_tune(t, result):
    frame_max = t.conn._tune_frame_max(result['frame_max'])
    channel_max = t.conn.channels.tune_channel_max(result['channel_max'])

    t.register(spec.METHOD_CONNECTION_OPEN_OK, _connection_open_ok)
    f1 = spec.encode_connection_tune_ok(channel_max, frame_max, t.conn.heartbeat)
    f2 = spec.encode_connection_open(t.conn.vhost)
    t.send_frames(f1 + f2)

def _connection_open_ok(ct, result):
    ct.register(spec.METHOD_CONNECTION_CLOSE, _connection_close)
    # Never free the promise and channel.
    ct.ping(ct.x_cached_result)
    ct.conn.x_connection_promise = ct
    publish_promise(ct.conn)


def publish_promise(conn):
    if conn.x_pubacks:
        pt = conn.promises.new(_pt_channel_open_ok_puback)
    else:
        pt = conn.promises.new(_pt_channel_open_ok)
    pt.x_async_enabled = False
    pt.x_delivery_tag = 1
    pt.x_delivery_tag_shift = 0
    pt.x_async_inflight = ordereddict.OrderedDict()
    pt.x_async_next = []
    conn.x_publish_promise = pt

def _pt_channel_open_ok_puback(pt, _result=None):
    pt.send_frames( spec.encode_confirm_select() )
    pt.register(spec.METHOD_CONFIRM_SELECT_OK, _pt_channel_open_ok)
    pt.register(spec.METHOD_BASIC_ACK, _pt_basic_ack)

def _pt_channel_open_ok(pt, _result=None):
    pt.x_async_enabled = True
    pt.register(spec.METHOD_CHANNEL_CLOSE, _pt_channel_close)
    pt.register(spec.METHOD_BASIC_RETURN, _pt_basic_return)
    # Send remaining messages.
    _pt_async_flush(pt)

def fix_basic_publish_headers(headers):
    assert 'headers' not in headers # That's not a good idea.
    nheaders = {}
    nheaders.update(headers)
    return nheaders

def basic_publish(conn, exchange, routing_key='', mandatory=False,
                  headers={}, body=''):
    pt = conn.x_publish_promise
    delivery_tag = pt.x_delivery_tag
    pt.x_delivery_tag += 1

    nheaders = fix_basic_publish_headers(headers)
    assert 'x-puka-delivery-tag' not in nheaders
    nheaders['x-puka-delivery-tag'] = delivery_tag

    frames = spec.encode_basic_publish(exchange, routing_key, mandatory,
                                       False, nheaders, body,
                                       conn.frame_max)
    if not conn.x_pubacks:
        # Construct ack packet.
        eheaders = {'x-puka-delivery-tag': delivery_tag, 'x-puka-footer': True}
        frames = frames + \
                 spec.encode_basic_publish('', '', True, False, eheaders,
                                                '', conn.frame_max)
    t = conn.promises.new(_nothing, no_channel=True)
    pt.x_async_next.append( (delivery_tag, t, frames) )
    _pt_async_flush(pt)
    return t

def _pt_async_flush(pt):
    if pt.x_async_enabled:
        frames_acc = []
        for delivery_tag, t, frames in pt.x_async_next:
            pt.x_async_inflight[delivery_tag] = t
            frames_acc.extend( frames )
        pt.x_async_next = []
        pt.send_frames(frames_acc)

def _pt_basic_return(pt, result):
    pt.register(spec.METHOD_BASIC_RETURN, _pt_basic_return)
    delivery_tag = result['headers']['x-puka-delivery-tag']
    if delivery_tag in pt.x_async_inflight:
        t = pt.x_async_inflight.pop(delivery_tag)
        if 'x-puka-footer' in result['headers']: # ok
            t.done(spec.Frame())
        else: # return
            exceptions.mark_frame(result)
            t.done(result)

def _pt_basic_ack(pt, result):
    pt.register(spec.METHOD_BASIC_ACK, _pt_basic_ack)
    delivery_tag = result['delivery_tag'] + pt.x_delivery_tag_shift
    if delivery_tag in pt.x_async_inflight:
        if result['multiple'] == True:
            delivery_tags = []
            for key in pt.x_async_inflight.iterkeys():
                if key <= delivery_tag:
                    delivery_tags.append(key)
                else:
                    break
        else:
            delivery_tags = [delivery_tag]
        for delivery_tag in delivery_tags:
            t = pt.x_async_inflight.pop(delivery_tag)
            t.done(spec.Frame())


def _pt_channel_close(pt, result):
    pt.x_async_enabled = False
    pt.x_delivery_tag_shift = pt.x_delivery_tag
    # Start off with reestablishing the channel
    if pt.conn.x_pubacks:
        pt.x_delivery_tag_shift -= 1 # starting from 1.
        pt.register(spec.METHOD_CHANNEL_OPEN_OK, _pt_channel_open_ok_puback)
    else:
        pt.register(spec.METHOD_CHANNEL_OPEN_OK, _pt_channel_open_ok)
    pt.send_frames( spec.encode_channel_close_ok() +
                    spec.encode_channel_open(''))
    # All the publishes are marked as failed.
    exceptions.mark_frame(result)
    for t in pt.x_async_inflight.itervalues():
        t.done(result)
    pt.x_async_inflight.clear()


def _connection_close(t, result):
    exceptions.mark_frame(result)
    t.ping(result)
    # Explode, kill everything.
    log.error('Connection killed with %r', result)
    t.conn._shutdown(result)

def connection_close(conn):
    t = conn.x_connection_promise
    t.register(spec.METHOD_CONNECTION_CLOSE_OK, _connection_close_ok)
    t.send_frames(spec.encode_connection_close(200, '', 0, 0))
    return t

def _connection_close_ok(t, result):
    # Ping this promise with success.
    t.ping(copy.copy(result))
    # Cancel all our promises with failure.
    exceptions.mark_frame(result)
    t.conn._shutdown(result)


####
def channel_open(t, callback):
    t.register(spec.METHOD_CHANNEL_OPEN_OK, _channel_open_ok)
    t.x_callback = callback
    t.send_frames( spec.encode_channel_open('') )

def _channel_open_ok(t, result):
    t.x_callback()


####
def queue_declare(conn, queue='', durable=False, exclusive=False,
                  auto_delete=False, passive=False, arguments={}):
    t = conn.promises.new(_queue_declare)
    t.x_frames = spec.encode_queue_declare(queue, passive, durable, exclusive,
                                           auto_delete, arguments)
    return t

def _queue_declare(t, result=None):
    t.register(spec.METHOD_QUEUE_DECLARE_OK, _queue_declare_ok)
    t.send_frames(t.x_frames)

def _queue_declare_ok(t, result):
    t.done(result)


####
def basic_consume(conn, queue, prefetch_count=0, no_local=False, no_ack=False,
                  exclusive=False, arguments={}):
    q = {'queue': queue,
         'no_local': no_local,
         'exclusive': exclusive,
         'arguments': arguments,
         }
    return basic_consume_multi(conn, [q], prefetch_count, no_ack)

####
def basic_consume_multi(conn, queues, prefetch_count=0, no_ack=False):
    t = conn.promises.new(_bcm_basic_qos, reentrant=True)
    t.x_frames = spec.encode_basic_qos(0, prefetch_count, False)
    t.x_consumes = []
    for i, item in enumerate(queues):
        if isinstance(item, str):
            queue = item
            no_local = exclusive = False
            arguments = {}
            consumer_tag = '%s.%s.%s' % (t.number, i, '')
        else:
            queue = item['queue']
            no_local = item.get('no_local', False)
            exclusive = item.get('exclusive', False)
            arguments = item.get('arguments', {})
            consumer_tag = '%s.%s.%s' % (t.number, i, item.get('consumer_tag', ''))
        t.x_consumes.append( (queue, spec.encode_basic_consume(
                    queue, consumer_tag, no_local, no_ack, exclusive, arguments)) )
    t.x_no_ack = no_ack
    t.x_consumer_tag = {}
    t.register(spec.METHOD_BASIC_DELIVER, _bcm_basic_deliver)
    t.register(spec.METHOD_BASIC_CANCEL, _bcm_basic_cancel)
    return t

def _bcm_basic_qos(t):
    t.register(spec.METHOD_BASIC_QOS_OK, _bcm_basic_qos_ok)
    t.send_frames(t.x_frames)

def _bcm_basic_qos_ok(t, result):
    _bcm_send_basic_consume(t)

def _bcm_send_basic_consume(t):
    t.register(spec.METHOD_BASIC_CONSUME_OK, _bcm_basic_consume_ok)
    t.x_queue, frames = t.x_consumes.pop()
    t.send_frames(frames)

def _bcm_basic_consume_ok(t, consume_result):
    t.x_consumer_tag[t.x_queue] = consume_result['consumer_tag']
    if t.x_consumes:
        _bcm_send_basic_consume(t)

def _bcm_basic_deliver(t, msg_result):
    t.register(spec.METHOD_BASIC_DELIVER, _bcm_basic_deliver)
    msg_result['promise_number'] = t.number
    if t.x_no_ack is False:
        t.refcnt_inc()
    t.ping(msg_result)

def _bcm_basic_cancel(ct, result):
    ct.register(spec.METHOD_BASIC_CANCEL, _generic_callback_nop)
    ct.x_ct = ct
    _basic_cancel(ct)

##
def basic_ack(conn, msg_result):
    t = conn.promises.by_number(msg_result['promise_number'])
    t.send_frames( spec.encode_basic_ack(msg_result['delivery_tag'], False) )
    assert t.x_no_ack is False
    t.refcnt_dec()
    return t

##
def basic_reject(conn, msg_result, requeue=True):
    t = conn.promises.by_number(msg_result['promise_number'])
    t.send_frames(spec.encode_basic_reject(msg_result['delivery_tag'], requeue))
    assert t.x_no_ack is False
    t.refcnt_dec()
    return t

##
def basic_qos(conn, consume_promise, prefetch_count=0):
    # TODO: race?
    t = conn.promises.new(_basic_qos, no_channel=True)
    t.x_ct = conn.promises.by_number(consume_promise)
    t.x_frames = spec.encode_basic_qos(0, prefetch_count, False)
    return t

def _basic_qos(t):
    ct = t.x_ct
    ct.register(spec.METHOD_BASIC_QOS_OK, _basic_qos_ok)
    ct.send_frames( t.x_frames )
    ct.x_qos_promise = t

def _basic_qos_ok(ct, result):
    t = ct.x_qos_promise
    t.done(result)

##
def basic_cancel(conn, consume_promise):
    # TODO: race?
    t = conn.promises.new(_basic_cancel, no_channel=True)
    t.x_ct = conn.promises.by_number(consume_promise)
    return t

def _basic_cancel(t):
    t.x_ct.x_mt = t
    _basic_cancel_one(t.x_ct)

def _basic_cancel_one(ct):
    consumer_tag = ct.x_consumer_tag.pop(ct.x_consumer_tag.keys()[0])
    ct.register(spec.METHOD_BASIC_CANCEL_OK, _basic_cancel_ok)
    ct.send_frames( spec.encode_basic_cancel(consumer_tag) )

def _basic_cancel_ok(ct, result):
    if ct.x_consumer_tag:
        _basic_cancel_one(ct)
    else:
        ct.x_mt.done(result)
        if ct != ct.x_mt:
            ct.done(None, no_callback=True)
        ct.x_mt = None
        ct.refcnt_clear()

####
def basic_get(conn, queue, no_ack=False):
    t = conn.promises.new(_basic_get)
    t.x_frames = spec.encode_basic_get(queue, no_ack)
    t.x_no_ack = no_ack
    return t

def _basic_get(t):
    t.register(spec.METHOD_BASIC_GET_OK, _basic_get_ok)
    t.register(spec.METHOD_BASIC_GET_EMPTY, _basic_get_empty)
    t.send_frames(t.x_frames)

def _basic_get_ok(t, msg_result):
    msg_result['promise_number'] = t.number
    if t.x_no_ack is False:
        t.refcnt_inc()
    t.done(msg_result)

def _basic_get_empty(t, result):
    result['empty'] = True
    t.done(result)


####
def exchange_declare(conn, exchange, type='direct', durable=False,
                     auto_delete=False, arguments={}):
    t = conn.promises.new(_exchange_declare)

    t.x_frames = spec.encode_exchange_declare(exchange, type, False, durable,
                                              auto_delete, False, arguments)
    return t

def _exchange_declare(t, result=None):
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

def _generic_callback_nop(t, result):
    pass

####
def exchange_delete(conn, exchange, if_unused=False):
    t = conn.promises.new(_generic_callback)
    t.x_method = spec.METHOD_EXCHANGE_DELETE_OK
    t.x_frames = spec.encode_exchange_delete(exchange, if_unused)
    return t

def exchange_bind(conn, destination, source, routing_key='', arguments={}):
    t = conn.promises.new(_generic_callback)
    t.x_method = spec.METHOD_EXCHANGE_BIND_OK
    t.x_frames = spec.encode_exchange_bind(destination, source, routing_key,
                                           arguments)
    return t

def exchange_unbind(conn, destination, source, routing_key='', arguments={}):
    t = conn.promises.new(_generic_callback)
    t.x_method = spec.METHOD_EXCHANGE_UNBIND_OK
    t.x_frames = spec.encode_exchange_unbind(destination, source, routing_key,
                                             arguments)
    return t

def queue_delete(conn, queue, if_unused=False, if_empty=False):
    t = conn.promises.new(_generic_callback)
    t.x_method = spec.METHOD_QUEUE_DELETE_OK
    t.x_frames = spec.encode_queue_delete(queue, if_unused, if_empty)
    return t

def queue_purge(conn, queue):
    t = conn.promises.new(_generic_callback)
    t.x_method = spec.METHOD_QUEUE_PURGE_OK
    t.x_frames = spec.encode_queue_purge(queue)
    return t

def queue_bind(conn, queue, exchange, routing_key='', arguments={}):
    t = conn.promises.new(_generic_callback)
    t.x_method = spec.METHOD_QUEUE_BIND_OK
    t.x_frames = spec.encode_queue_bind(queue, exchange, routing_key,
                                        arguments)
    return t

def queue_unbind(conn, queue, exchange, routing_key='', arguments={}):
    t = conn.promises.new(_generic_callback)
    t.x_method = spec.METHOD_QUEUE_UNBIND_OK
    t.x_frames = spec.encode_queue_unbind(queue, exchange, routing_key,
                                          arguments)
    return t

