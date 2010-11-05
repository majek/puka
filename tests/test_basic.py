import os
import puka
import random
import unittest_backport as unittest


AMQP_URL=os.getenv('AMQP_URL')

class TestBasic(unittest.TestCase):
    def test_simple_roundtrip(self):
        qname = 'test%s' % (random.random(),)
        msg = '%s' % (random.random(),)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.queue_declare(queue=qname)
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

        ticket = client.queue_declare(queue=qname)
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


    def test_basic_get_ack(self):
        qname = 'test%s' % (random.random(),)
        msg = '%s' % (random.random(),)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.queue_declare(queue=qname)
        client.wait(ticket)

        for i in range(4):
            ticket = client.basic_publish(exchange='', routing_key=qname,
                                          body=msg+str(i))
            client.wait(ticket)

        msgs = []
        for i in range(4):
            ticket = client.basic_get(queue=qname)
            result = client.wait(ticket)
            self.assertEqual(result['body'], msg+str(i))
            self.assertEqual(result['redelivered'], False)
            msgs.append( result )

        ticket = client.basic_get(queue=qname)
        result = client.wait(ticket)
        self.assertEqual('body' in result, False)

        self.assertEqual(len(client.channels.free_channels), 1)
        self.assertEqual(client.channels.free_channel_numbers[-1], 6)
        for msg in msgs:
            client.basic_ack(msg)
        self.assertEqual(len(client.channels.free_channels), 5)
        self.assertEqual(client.channels.free_channel_numbers[-1], 6)

        ticket = client.queue_delete(queue=qname)
        client.wait(ticket)


    def test_basic_publish_bad_exchange(self):
        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        for i in range(2):
            ticket = client.basic_publish(exchange='invalid_exchange',
                                          routing_key='xxx', body='')

            self.assertEqual(len(client.channels.free_channels), 0)
            self.assertEqual(client.channels.free_channel_numbers[-1], 2)

            with self.assertRaises(puka.exceptions.NotFound) as cm:
                client.wait(ticket)

            (r,) = cm.exception # unpack args of exception
            self.assertTrue(r.is_error)
            self.assertEqual(r['reply_code'], 404)

            self.assertEqual(len(client.channels.free_channels), 0)
            self.assertEqual(client.channels.free_channel_numbers[-1], 1)


    def test_basic_return(self):
        qname = 'test%s' % (random.random(),)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.basic_publish(exchange='', routing_key=qname,
                                      mandatory=True, body='')
        with self.assertRaises(puka.exceptions.NoRoute):
            client.wait(ticket)

        ticket = client.queue_declare(queue=qname)
        client.wait(ticket)

        ticket = client.basic_publish(exchange='', routing_key=qname,
                                      mandatory=True, body='')
        client.wait(ticket) # no error

        ticket = client.basic_publish(exchange='', routing_key=qname,
                                      mandatory=True, immediate=True, body='')
        with self.assertRaises(puka.exceptions.NoConsumers):
            client.wait(ticket)




