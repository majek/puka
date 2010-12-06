from __future__ import with_statement

import os
import puka
import random
import socket

import base


AMQP_URL=os.getenv('AMQP_URL')

class TestPublishAsync(base.TestCase):
    def test_simple_roundtrip(self):
        client = puka.Client(self.amqp_url)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.queue_declare(queue=self.name)
        client.wait(ticket)

        ticket = client.basic_publish(exchange='', routing_key=self.name,
                                      body=self.msg)
        client.wait(ticket)

        consume_ticket = client.basic_consume(queue=self.name, no_ack=True)
        result = client.wait(consume_ticket)
        self.assertEqual(result['body'], self.msg)

        ticket = client.queue_delete(queue=self.name)
        client.wait(ticket)

    def test_big_failure(self):
        client = puka.Client(self.amqp_url)
        ticket = client.connect()
        client.wait(ticket)

        # synchronize publish channel - give time for chanel-open
        ticket = client.queue_declare(queue=self.name)
        client.wait(ticket)

        ticket1 = client.basic_publish(exchange='', routing_key='',
                                       body=self.msg)
        ticket2 = client.basic_publish(exchange='wrong_exchange',
                                       routing_key='',
                                       body=self.msg)
        ticket3 = client.basic_publish(exchange='', routing_key='',
                                       body=self.msg)
        client.wait(ticket1)
        with self.assertRaises(puka.NotFound):
            client.wait(ticket2)
        with self.assertRaises(puka.NotFound):
            client.wait(ticket3)

        ticket = client.basic_publish(exchange='', routing_key='',
                                      body=self.msg)
        client.wait(ticket)

        ticket = client.queue_delete(queue=self.name)
        client.wait(ticket)

    def test_return(self):
        client = puka.Client(self.amqp_url)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.basic_publish(exchange='', routing_key='',
                                      body=self.msg, immediate=True)
        with self.assertRaises(puka.NoConsumers):
            client.wait(ticket)

    def test_batched_acks(self):
        client = puka.Client(self.amqp_url)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.queue_declare(queue=self.name)
        client.wait(ticket)

        tickets = [client.basic_publish(exchange='', routing_key=self.name,
                                        body=self.msg)
                   for i in range(10)]
        responses = [client.wait(ticket) for ticket in tickets]
        # Some of the responses are identical for few tickets.
        self.assertTrue(len(set([id(r) for r in responses])) < 10)

        ticket = client.queue_delete(queue=self.name)
        client.wait(ticket)

    def test_batched_return(self):
        client = puka.Client(self.amqp_url)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.queue_declare(queue=self.name)
        client.wait(ticket)

        tickets = [client.basic_publish(exchange='',
                                        routing_key=(self.name if i != 6 \
                                                         else 'badname'),
                                        mandatory=True, body=self.msg)
                   for i in range(10)]
        responses = [client.wait(ticket, raise_errors=False) \
                         for ticket in tickets]
        # Some of the responses are identical for few tickets.
        self.assertTrue(len(set([id(r) for r in responses])) < 10)
        self.assertTrue(responses[5] is responses[7])
        self.assertEqual(responses[6]['reply_code'], 312)

        ticket = client.queue_delete(queue=self.name)
        client.wait(ticket)

    def test_return(self):
        client = puka.Client(self.amqp_url)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.queue_declare(queue=self.name)
        client.wait(ticket)

        ticket = client.basic_publish(exchange='', routing_key='badname',
                                      immediate=True, body=self.msg)
        try:
            client.wait(ticket)
        except puka.NoConsumers, (response,):
            pass

        self.assertEqual(response['reply_code'], 313)

        ticket = client.queue_delete(queue=self.name)
        client.wait(ticket)


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
