import os
import unittest
import puka
import random


AMQP_URL=os.getenv('AMQP_URL')

class TestBasic(unittest.TestCase):
    def test_simple_roundtrip(self):
        qname = 'test%s' % (random.random(),)
        msg = '%s' % (random.random(),)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.queue_declare(queue=qname, auto_delete=True)
        client.wait(ticket)

        ticket = client.basic_publish(exchange='', routing_key=qname,
                                      body=msg)
        client.wait(ticket)

        consume_ticket = client.basic_consume(queue=qname)
        result = client.wait(consume_ticket)
        client.basic_ack(result)
        self.assertEqual(result['body'], msg)

        ticket = client.queue_delete(queue=qname)
        client.wait(ticket)


    def test_purge(self):
        qname = 'test%s' % (random.random(),)
        msg = '%s' % (random.random(),)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.queue_declare(queue=qname, auto_delete=True)
        client.wait(ticket)

        ticket = client.basic_publish(exchange='', routing_key=qname,
                                      body=msg)
        client.wait(ticket)

        ticket = client.queue_purge(queue=qname)
        r = client.wait(ticket)
        self.assertEqual(r['message_count'], 1)

        ticket = client.queue_purge(queue=qname)
        r = client.wait(ticket)
        self.assertEqual(r['message_count'], 0)

        ticket = client.queue_delete(queue=qname)
        client.wait(ticket)


