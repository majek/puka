from __future__ import with_statement

import os
import puka
import random
import socket

import base


AMQP_URL=os.getenv('AMQP_URL', 'amqp:///')

class TestPublishAsync(base.TestCase):
    pubacks = None
    def test_simple_roundtrip(self):
        client = puka.Client(self.amqp_url, pubacks=self.pubacks)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body=self.msg)
        client.wait(promise)

        consume_promise = client.basic_consume(queue=self.name, no_ack=False)

        msg = client.wait(consume_promise)
        self.assertEqual(msg['body'], self.msg)

        client.basic_ack(msg)

        result = client.wait(consume_promise, timeout=0.1)
        self.assertEqual(result, None)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)

    def test_big_failure(self):
        client = puka.Client(self.amqp_url, pubacks=self.pubacks)
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
        client = puka.Client(self.amqp_url, pubacks=self.pubacks)
        promise = client.connect()
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key='',
                                       body=self.msg, immediate=True)
        with self.assertRaises(puka.NoConsumers):
            client.wait(promise)


    def test_return_2(self):
        client = puka.Client(self.amqp_url, pubacks=self.pubacks)
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


    def test_simple_basic_get(self):
        client = puka.Client(self.amqp_url, pubacks=self.pubacks)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body=self.msg)
        client.wait(promise)

        promise = client.basic_get(queue=self.name)
        result = client.wait(promise)
        self.assertEqual(result['body'], self.msg)
        client.basic_ack(result)

        promise = client.basic_get(queue=self.name)
        result = client.wait(promise)
        self.assertTrue(result['empty'])

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


    def test_bug21(self):
        # Following the testcase: https://github.com/majek/puka/issues/21
        client = puka.Client(self.amqp_url, pubacks=self.pubacks)
        promise = client.connect()
        client.wait(promise)

        promises = []
        for i in range(0, 42):
            promise = client.basic_publish('', 'test_key', 'test_body')
            self.assertTrue(len(client.send_buf) > 0)
            promises.append(promise)

        #client.wait(promises)



class TestPublishAsyncPubacksTrue(TestPublishAsync):
    pubacks = True

class TestPublishAsyncPubacksFalse(TestPublishAsync):
    pubacks = False

class TestPublishAckDetection(base.TestCase):
    # Assuming reasonably recent RabbitMQ server (which does pubacks).
    def test_pubacks(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        r = client.wait(promise)
        self.assertEqual(client.pubacks, None)
        self.assertTrue(r['server_properties']['capabilities']\
                            ['publisher_confirms'])
        self.assertTrue(client.x_pubacks)


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
