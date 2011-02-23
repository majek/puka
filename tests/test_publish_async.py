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
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                      body=self.msg)
        client.wait(promise)

        consume_promise = client.basic_consume(queue=self.name, no_ack=True)
        result = client.wait(consume_promise)
        self.assertEqual(result['body'], self.msg)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)

    def test_big_failure(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        # synchronize publish channel - give time for chanel-open
        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise1 = client.basic_publish(exchange='', routing_key='',
                                       body=self.msg)
        promise2 = client.basic_publish(exchange='wrong_exchange',
                                       routing_key='',
                                       body=self.msg)
        promise3 = client.basic_publish(exchange='', routing_key='',
                                       body=self.msg)
        client.wait(promise1)
        with self.assertRaises(puka.NotFound):
            client.wait(promise2)
        with self.assertRaises(puka.NotFound):
            client.wait(promise3)

        # validate if it still works
        promise = client.basic_publish(exchange='', routing_key='',
                                      body=self.msg)
        client.wait(promise)

        # and fail again.
        promise = client.basic_publish(exchange='wrong_exchange',
                                       routing_key='',
                                       body=self.msg)
        with self.assertRaises(puka.NotFound):
            client.wait(promise)

        # and validate again
        promise = client.basic_publish(exchange='', routing_key='',
                                      body=self.msg)
        client.wait(promise)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)

    def test_return(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key='',
                                      body=self.msg, immediate=True)
        with self.assertRaises(puka.NoConsumers):
            client.wait(promise)


    def test_return_2(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key='badname',
                                      immediate=True, body=self.msg)
        try:
            client.wait(promise)
        except puka.NoConsumers, (response,):
            pass

        self.assertEqual(response['reply_code'], 313)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
