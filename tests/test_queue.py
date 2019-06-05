from __future__ import with_statement

import os
import puka
import random
import unittest

import base

class TestQueue(base.TestCase):

    @base.connect
    def test_queue_declare(self, client):
        qname = 'test%s-this-queue-should-be-autodeleted' % (random.random(),)
        promise = client.queue_declare(queue=qname, auto_delete=True)
        client.wait(promise)
        # The queue intentionally left hanging. Should be autoremoved.
        # Yes, no assertion here, we don't want to wait for 5 seconds.

        self.cleanup_promise(client.queue_delete, queue=qname)

    @base.connect
    def test_queue_redeclare(self, client):
        qname = 'test%s' % (random.random(),)

        promise = client.queue_declare(queue=qname, auto_delete=False)
        r = client.wait(promise)
        self.cleanup_promise(client.queue_delete, queue=qname)

        promise = client.queue_declare(queue=qname, auto_delete=False)
        r = client.wait(promise)

        promise = client.queue_declare(queue=qname, auto_delete=True)
        with self.assertRaises(puka.PreconditionFailed):
            client.wait(promise)

    @base.connect
    def test_queue_redeclare_args(self, client):
        qname = 'test%s' % (random.random(),)

        promise = client.queue_declare(queue=qname, arguments={})
        r = client.wait(promise)
        self.cleanup_promise(client.queue_delete, queue=qname)

        promise = client.queue_declare(queue=qname, arguments={'x-expires':101})
        with self.assertRaises(puka.PreconditionFailed):
            client.wait(promise)

    @base.connect
    def test_queue_delete_not_found(self, client):
        promise = client.queue_delete(queue='not_existing_queue')

        with self.assertRaises(puka.NotFound):
            client.wait(promise)

    @base.connect
    def test_queue_bind(self, client):
        qname = 'test%s' % (random.random(),)

        t = client.queue_declare(queue=qname)
        client.wait(t)
        self.cleanup_promise(client.queue_delete, queue=qname)

        t = client.exchange_declare(exchange=qname, type='direct')
        client.wait(t)
        self.cleanup_promise(client.exchange_delete, exchange=qname)

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
        self.assertEqual(r['body'], 'b')
        self.assertEqual(r['message_count'], 0)

        t = client.queue_delete(queue=qname)
        client.wait(t)

        t = client.exchange_delete(exchange=qname)
        client.wait(t)

