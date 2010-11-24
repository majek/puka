from __future__ import with_statement

import os
import puka
import random

import base


class TestExchange(base.TestCase):
    def test_exchange_redeclare(self):
        client = puka.Client(self.amqp_url)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.exchange_declare(exchange=self.name)
        r = client.wait(ticket)

        ticket = client.exchange_declare(exchange=self.name, type='fanout')
        with self.assertRaises(puka.PreconditionFailed):
            client.wait(ticket)

        ticket = client.exchange_delete(exchange=self.name)
        client.wait(ticket)

    def test_exchange_delete_not_found(self):
        client = puka.Client(self.amqp_url)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.exchange_delete(exchange='not_existing_exchange')

        with self.assertRaises(puka.NotFound):
            client.wait(ticket)

    def test_bind(self):
        client = puka.Client(self.amqp_url)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.exchange_declare(exchange=self.name1, type='fanout')
        client.wait(ticket)

        ticket = client.exchange_declare(exchange=self.name2, type='fanout')
        client.wait(ticket)

        ticket = client.queue_declare()
        qname = client.wait(ticket)['queue']

        ticket = client.queue_bind(queue=qname, exchange=self.name2)
        client.wait(ticket)

        ticket = client.basic_publish(exchange=self.name1, routing_key='',
                                      body='a')
        client.wait(ticket)

        ticket = client.exchange_bind(source=self.name1, destination=self.name2)
        client.wait(ticket)

        ticket = client.basic_publish(exchange=self.name1, routing_key='',
                                      body='b')
        client.wait(ticket)

        ticket = client.exchange_unbind(source=self.name1,
                                        destination=self.name2)
        client.wait(ticket)

        ticket = client.basic_publish(exchange=self.name1, routing_key='',
                                      body='c')
        client.wait(ticket)

        ticket = client.basic_get(queue=qname, no_ack=True)
        r = client.wait(ticket)
        self.assertEquals(r['body'], 'b')

        ticket = client.basic_get(queue=qname)
        r = client.wait(ticket)
        self.assertTrue('empty' in r)

        ticket = client.exchange_delete(exchange=self.name1)
        client.wait(ticket)
        ticket = client.exchange_delete(exchange=self.name2)
        client.wait(ticket)
        ticket = client.queue_delete(queue=qname)
        client.wait(ticket)

        ticket = client.close()
        client.wait(ticket)


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
