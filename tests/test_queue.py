from __future__ import with_statement

import os
import puka
import random
import unittest_backport as unittest


AMQP_URL=os.getenv('AMQP_URL')

class TestQueue(unittest.TestCase):
    def test_queue_declare(self):
        qname = 'test%s-this-queue-should-be-autodeleted' % (random.random(),)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.queue_declare(queue=qname, auto_delete=True)
        client.wait(ticket)
        # The queue intentionally left hanging. Should be autoremoved.
        # Yes, no assertion here, we don't want to wait for 5 seconds.

    def test_queue_redeclare(self):
        qname = 'test%s' % (random.random(),)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.queue_declare(queue=qname, auto_delete=False)
        r = client.wait(ticket)
        self.assertTrue('exists' not in r)

        ticket = client.queue_declare(queue=qname, auto_delete=False)
        r = client.wait(ticket)
        self.assertTrue(r['exists'])

        # Puka can't verify if the queue has the same properties.
        ticket = client.queue_declare(queue=qname, auto_delete=True)
        r = client.wait(ticket)
        self.assertTrue(r['exists'])

        # Unless you can sacrifice the connection...
        ticket = client.queue_declare(queue=qname, auto_delete=False,
                                      suicidal=True)
        client.wait(ticket)

        ticket = client.queue_declare(queue=qname, auto_delete=True,
                                      suicidal=True)
        with self.assertRaises(puka.NotAllowed):
            client.wait(ticket)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)


        ticket = client.queue_delete(queue=qname)
        client.wait(ticket)

    def test_queue_delete_not_found(self):
        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.queue_delete(queue='not_existing_queue')

        with self.assertRaises(puka.NotFound):
            client.wait(ticket)


    def test_queue_bind(self):
        qname = 'test%s' % (random.random(),)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

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
        self.assertEquals(r['body'], 'b')
        self.assertEquals(r['message_count'], 0)

        t = client.queue_delete(queue=qname)
        client.wait(t)

        t = client.exchange_delete(exchange=qname)
        client.wait(t)

