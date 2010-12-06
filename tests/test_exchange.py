from __future__ import with_statement

import os
import puka
import random

import base


class TestExchange(base.TestCase):
    def test_exchange_redeclare(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.exchange_declare(exchange=self.name)
        r = client.wait(primise)

        primise = client.exchange_declare(exchange=self.name, type='fanout')
        with self.assertRaises(puka.PreconditionFailed):
            client.wait(primise)

        primise = client.exchange_delete(exchange=self.name)
        client.wait(primise)

    def test_exchange_delete_not_found(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.exchange_delete(exchange='not_existing_exchange')

        with self.assertRaises(puka.NotFound):
            client.wait(primise)

    def test_bind(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.exchange_declare(exchange=self.name1, type='fanout')
        client.wait(primise)

        primise = client.exchange_declare(exchange=self.name2, type='fanout')
        client.wait(primise)

        primise = client.queue_declare()
        qname = client.wait(primise)['queue']

        primise = client.queue_bind(queue=qname, exchange=self.name2)
        client.wait(primise)

        primise = client.basic_publish(exchange=self.name1, routing_key='',
                                      body='a')
        client.wait(primise)

        primise = client.exchange_bind(source=self.name1, destination=self.name2)
        client.wait(primise)

        primise = client.basic_publish(exchange=self.name1, routing_key='',
                                      body='b')
        client.wait(primise)

        primise = client.exchange_unbind(source=self.name1,
                                        destination=self.name2)
        client.wait(primise)

        primise = client.basic_publish(exchange=self.name1, routing_key='',
                                      body='c')
        client.wait(primise)

        primise = client.basic_get(queue=qname, no_ack=True)
        r = client.wait(primise)
        self.assertEquals(r['body'], 'b')

        primise = client.basic_get(queue=qname)
        r = client.wait(primise)
        self.assertTrue('empty' in r)

        primise = client.exchange_delete(exchange=self.name1)
        client.wait(primise)
        primise = client.exchange_delete(exchange=self.name2)
        client.wait(primise)
        primise = client.queue_delete(queue=qname)
        client.wait(primise)

        primise = client.close()
        client.wait(primise)


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
