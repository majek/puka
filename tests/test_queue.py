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
        with self.assertRaises(puka.exceptions.NotAllowed):
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

        with self.assertRaises(puka.exceptions.NotFound):
            client.wait(ticket)


