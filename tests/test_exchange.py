from __future__ import with_statement

import os
import unittest

import puka
import random

import base


class TestExchange(base.TestCase):

    @base.connect
    def test_exchange_redeclare(self, client):
        promise = client.exchange_declare(exchange=self.name)
        r = client.wait(promise)
        self.cleanup_promise(client.exchange_delete, exchange=self.name)

        promise = client.exchange_declare(exchange=self.name, type='fanout')
        with self.assertRaises(puka.PreconditionFailed):
            client.wait(promise)

        promise = client.exchange_delete(exchange=self.name)
        client.wait(promise)

    def test_exchange_delete_not_found(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.exchange_delete(exchange='not_existing_exchange')

        with self.assertRaises(puka.NotFound):
            client.wait(promise)

    @base.connect
    def test_bind(self, client):
        promise = client.exchange_declare(exchange=self.name1, type='fanout')
        client.wait(promise)
        self.cleanup_promise(client.exchange_delete, exchange=self.name1)

        promise = client.exchange_declare(exchange=self.name2, type='fanout')
        client.wait(promise)
        self.cleanup_promise(client.exchange_delete, exchange=self.name2)

        promise = client.queue_declare()
        qname = client.wait(promise)['queue']
        self.cleanup_promise(client.queue_declare, name=qname)

        promise = client.queue_bind(queue=qname, exchange=self.name2)
        client.wait(promise)

        promise = client.basic_publish(exchange=self.name1, routing_key='',
                                      body='a')
        client.wait(promise)

        promise = client.exchange_bind(source=self.name1, destination=self.name2)
        client.wait(promise)

        promise = client.basic_publish(exchange=self.name1, routing_key='',
                                      body='b')
        client.wait(promise)

        promise = client.exchange_unbind(source=self.name1,
                                        destination=self.name2)
        client.wait(promise)

        promise = client.basic_publish(exchange=self.name1, routing_key='',
                                      body='c')
        client.wait(promise)

        promise = client.basic_get(queue=qname, no_ack=True)
        r = client.wait(promise)
        self.assertEqual(r['body'], 'b')

        promise = client.basic_get(queue=qname)
        r = client.wait(promise)
        self.assertTrue('empty' in r)

        promise = client.exchange_delete(exchange=self.name1)
        client.wait(promise)
        promise = client.exchange_delete(exchange=self.name2)
        client.wait(promise)
        promise = client.queue_delete(queue=qname)
        client.wait(promise)


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
