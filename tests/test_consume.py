from __future__ import with_statement

import puka, pytest


def test_shared_qos(client, qname1, qname2):
    promise = client.basic_publish(exchange='', routing_key=qname1,
                                  body='a')
    client.wait(promise)
    promise = client.basic_publish(exchange='', routing_key=qname2,
                                  body='b')
    client.wait(promise)

    consume_promise = client.basic_consume_multi([qname1, qname2],
                                                prefetch_count=1)
    result = client.wait(consume_promise, timeout=0.1)
    r1 = result['body']
    assert r1 in ['a', 'b']

    result = client.wait(consume_promise, timeout=0.1)
    assert result is None

    promise = client.basic_qos(consume_promise, prefetch_count=2)
    result = client.wait(promise)

    result = client.wait(consume_promise, timeout=0.1)
    r2 = result['body']
    assert sorted([r1, r2]) == ['a', 'b']

    promise = client.basic_cancel(consume_promise)
    client.wait(promise)


def test_access_refused(client, makeid):
    name = makeid("exclusive-q")
    promise = client.queue_declare(queue=name, exclusive=True)
    client.wait(promise)

    promise = client.queue_declare(queue=name)
    with pytest.raises(puka.ResourceLocked):
        client.wait(promise)

    # Testing exclusive basic_consume.
    promise = client.basic_consume(queue=name, exclusive=True)
    client.wait(promise, timeout=0.001)

    # Do something syncrhonus.
    promise = client.queue_declare(exclusive=True)
    client.wait(promise)

    promise = client.basic_consume(queue=name)
    with pytest.raises(puka.AccessRefused):
        client.wait(promise)

    promise = client.queue_delete(queue=name)
    client.wait(promise)
