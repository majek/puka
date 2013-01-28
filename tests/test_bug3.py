#! /usr/bin/env py.test

# https://github.com/majek/puka/issues/3
from __future__ import with_statement
import puka, pytest


@pytest.fixture
def client(request, amqp_url):
    """a connected puka.Client instance"""
    client = puka.Client(amqp_url)
    promise = client.connect()
    client.wait(promise)
    return client


def _epilogue(qname, expected, amqp_url):
    client = puka.Client(amqp_url)
    promise = client.connect()
    client.wait(promise)
    promise = client.queue_declare(queue=qname)
    q = client.wait(promise)
    assert q['message_count'] == expected

    promise = client.queue_delete(queue=qname)
    client.wait(promise)


def test_bug3_wait(client, makeid, amqp_url):
    qname = makeid()
    promise = client.queue_declare(queue=qname)
    client.wait(promise)

    for i in range(3):
        promise = client.basic_publish(exchange='',
                                       routing_key=qname,
                                       body='x')
        client.wait(promise)

    consume_promise = client.basic_consume(qname)
    for i in range(3):
        msg = client.wait(consume_promise)
        client.basic_ack(msg)
        assert msg['body'] == 'x'

    client.socket().close()
    _epilogue(qname, 1, amqp_url)


def test_bug3_loop(client, makeid, amqp_url):
    qname = makeid()
    promise = client.queue_declare(queue=qname)
    client.wait(promise)

    for i in range(3):
        promise = client.basic_publish(exchange='',
                                       routing_key=qname,
                                       body='x')
        client.wait(promise)

    i = [0]

    def cb(_, msg):
        client.basic_ack(msg)
        assert msg['body'] == 'x'
        i[0] += 1
        if i[0] == 3:
            client.loop_break()
    consume_promise = client.basic_consume(qname, callback=cb)
    client.loop()

    client.socket().close()
    _epilogue(qname, 0, amqp_url)
