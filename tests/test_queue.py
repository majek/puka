from __future__ import with_statement

import puka
import pytest


def test_queue_declare(client, makeid):
    qname = makeid("this-queue-should-be-autodeleted")
    promise = client.queue_declare(queue=qname, auto_delete=True)
    client.wait(promise)
    # The queue intentionally left hanging. Should be autoremoved.
    # Yes, no assertion here, we don't want to wait for 5 seconds.


def test_queue_redeclare(client, makeid):
    qname = makeid()

    promise = client.queue_declare(queue=qname, auto_delete=False)
    r = client.wait(promise)

    promise = client.queue_declare(queue=qname, auto_delete=False)
    r = client.wait(promise)

    promise = client.queue_declare(queue=qname, auto_delete=True)
    with pytest.raises(puka.PreconditionFailed):
        client.wait(promise)

    promise = client.queue_delete(queue=qname)
    client.wait(promise)

def test_queue_redeclare_args(client, makeid):
    qname = makeid()
    promise = client.queue_declare(queue=qname, arguments={})
    r = client.wait(promise)

    promise = client.queue_declare(queue=qname, arguments={'x-expires':101})
    with pytest.raises(puka.PreconditionFailed):
        client.wait(promise)

    promise = client.queue_delete(queue=qname)
    client.wait(promise)


def test_queue_delete_not_found(client):

    promise = client.queue_delete(queue='not_existing_queue')

    with pytest.raises(puka.NotFound):
        client.wait(promise)


def test_queue_bind(client, makeid):
    qname = makeid()

    t = client.queue_declare(queue=qname)
    client.wait(t)

    t = client.exchange_declare(exchange=qname, type='direct')
    client.wait(t)

    t = client.basic_publish(exchange=qname, routing_key=qname, body='a')
    client.wait(t)

    t = client.queue_bind(exchange=qname, queue=qname, routing_key=qname)
    client.wait(t)

    t = client.basic_publish(exchange=qname, routing_key=qname, body='b')
    client.wait(t)

    t = client.queue_unbind(exchange=qname, queue=qname, routing_key=qname)
    client.wait(t)

    t = client.basic_publish(exchange=qname, routing_key=qname, body='c')
    client.wait(t)

    t = client.basic_get(queue=qname)
    r = client.wait(t)
    assert r['body'] == 'b'
    assert r['message_count'] == 0

    t = client.queue_delete(queue=qname)
    client.wait(t)

    t = client.exchange_delete(exchange=qname)
    client.wait(t)

