import os
import unittest_backport as unittest
import puka
import random


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
        client.wait(ticket)

        ticket = client.queue_declare(queue=qname, auto_delete=False)
        client.wait(ticket)

        # TODO: fix Rabbit and/or AMQP
        #with self.assertRaises(KeyError):
        #    ticket = client.queue_declare(queue=qname, auto_delete=True)
        #    client.wait(ticket)

        ticket = client.queue_delete(queue=qname)
        client.wait(ticket)


