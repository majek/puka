#! /usr/bin/env py.test

from __future__ import with_statement
import pytest


@pytest.fixture
def names(request, client, makeid):
    names = [makeid() for i in range(3)]
    for name in names:
        promise = client.queue_declare(queue=name)
        client.wait(promise)
        promise = client.basic_publish(exchange='', routing_key=name,
                                       body='a')
        client.wait(promise)

    def cleanup():
        for x in [client.queue_delete(queue=name) for name in names]:
            client.wait(x)
    request.addfinalizer(cleanup)
    return names


def test_cancel_single(client, qname1):
    promise = client.basic_publish(exchange='', routing_key=qname1,
                                   body='a')
    client.wait(promise)

    consume_promise = client.basic_consume(queue=qname1, prefetch_count=1)
    result = client.wait(consume_promise)
    assert result['body'] == 'a'

    promise = client.basic_cancel(consume_promise)
    result = client.wait(promise)
    assert 'consumer_tag' in result

    # TODO: better error
    # It's illegal to wait on consume_promise after cancel.
    #with self.assertRaises(Exception):
    #    client.wait(consume_promise)


def test_cancel_multi(client, names):
    consume_promise = client.basic_consume_multi(queues=names,
                                                 no_ack=True)
    for i in range(len(names)):
        result = client.wait(consume_promise)
        assert result['body'] == 'a'

    promise = client.basic_cancel(consume_promise)
    result = client.wait(promise)
    assert 'consumer_tag' in result

    # TODO: better error
    #with self.assertRaises(Exception):
    #    client.wait(consume_promise)


def test_cancel_single_notification(client, qname1):
    promise = client.basic_publish(exchange='', routing_key=qname1,
                                   body='a')
    client.wait(promise)

    consume_promise = client.basic_consume(queue=qname1, prefetch_count=1)
    result = client.wait(consume_promise)
    assert result['body'] == 'a'

    promise = client.queue_delete(qname1)

    result = client.wait(consume_promise)
    assert result.name == 'basic.cancel_ok'

    # Make sure the consumer died:
    promise = client.queue_declare(queue=qname1)
    result = client.wait(promise)
    assert result['consumer_count'] == 0


def test_cancel_multi_notification(client, names):
    consume_promise = client.basic_consume_multi(queues=names,
                                                 no_ack=True)
    for i in range(len(names)):
        result = client.wait(consume_promise)
        assert result['body'] == 'a'

    promise = client.queue_delete(names[0])

    result = client.wait(consume_promise)
    assert result.name == 'basic.cancel_ok'

    # Make sure the consumer died:
    for name in names:
        promise = client.queue_declare(queue=name)
        result = client.wait(promise)
        assert result['consumer_count'] == 0


def test_cancel_multi_notification_concurrent(client, names):
    consume_promise = client.basic_consume_multi(queues=names,
                                                 no_ack=True)
    for i in range(len(names)):
        result = client.wait(consume_promise)
        assert result['body'] == 'a'

    client.queue_delete(names[0])
    client.queue_delete(names[2])

    result = client.wait(consume_promise)
    assert result.name == 'basic.cancel_ok'

    # Make sure the consumer died:
    for name in names:
        promise = client.queue_declare(queue=name)
        result = client.wait(promise)
        assert result['consumer_count'] == 0
