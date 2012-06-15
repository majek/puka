from __future__ import with_statement

import os
import puka

import base


class TestBasic(base.TestCase):
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


    def test_purge(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body=self.msg)
        client.wait(promise)

        promise = client.queue_purge(queue=self.name)
        r = client.wait(promise)
        self.assertEqual(r['message_count'], 1)

        promise = client.queue_purge(queue=self.name)
        r = client.wait(promise)
        self.assertEqual(r['message_count'], 0)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


    def test_basic_get_ack(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        for i in range(4):
            promise = client.basic_publish(exchange='', routing_key=self.name,
                                           body=self.msg+str(i))
            client.wait(promise)

        msgs = []
        for i in range(4):
            promise = client.basic_get(queue=self.name)
            result = client.wait(promise)
            self.assertEqual(result['body'], self.msg+str(i))
            self.assertEqual(result['redelivered'], False)
            msgs.append( result )

        promise = client.basic_get(queue=self.name)
        result = client.wait(promise)
        self.assertEqual('body' in result, False)

        self.assertEqual(len(client.channels.free_channels), 1)
        self.assertEqual(client.channels.free_channel_numbers[-1], 7)
        for msg in msgs:
            client.basic_ack(msg)
        self.assertEqual(len(client.channels.free_channels), 5)
        self.assertEqual(client.channels.free_channel_numbers[-1], 7)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


    def test_basic_publish_bad_exchange(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        for i in range(2):
            promise = client.basic_publish(exchange='invalid_exchange',
                                           routing_key='xxx', body='')

            self.assertEqual(len(client.channels.free_channels), 0)
            self.assertEqual(client.channels.free_channel_numbers[-1], 2)

            with self.assertRaises(puka.NotFound) as cm:
                client.wait(promise)

            (r,) = cm.exception # unpack args of exception
            self.assertTrue(r.is_error)
            self.assertEqual(r['reply_code'], 404)

            self.assertEqual(len(client.channels.free_channels), 0)
            self.assertEqual(client.channels.free_channel_numbers[-1], 2)


    def test_basic_return(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       mandatory=True, body='')
        with self.assertRaises(puka.NoRoute):
            client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       mandatory=True, body='')
        client.wait(promise) # no error

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       immediate=True, body='')
        with self.assertRaises(puka.NoConsumers):
            r = client.wait(promise)
            print r

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


    def test_persistent(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body=self.msg) # persistence=default
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body=self.msg,
                                       headers={'delivery_mode':2})
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body=self.msg,
                                       headers={'delivery_mode':1})
        client.wait(promise)

        promise = client.basic_get(queue=self.name, no_ack=True)
        result = client.wait(promise)
        self.assertTrue('delivery_mode' not in result['headers'])

        promise = client.basic_get(queue=self.name, no_ack=True)
        result = client.wait(promise)
        self.assertTrue('delivery_mode' in result['headers'])
        self.assertEquals(result['headers']['delivery_mode'], 2)

        promise = client.basic_get(queue=self.name, no_ack=True)
        result = client.wait(promise)
        self.assertTrue('delivery_mode' in result['headers'])
        self.assertEquals(result['headers']['delivery_mode'], 1)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


    def test_basic_reject(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body='a')
        client.wait(promise)

        t = client.basic_get(queue=self.name)
        r = client.wait(t)
        self.assertEqual(r['body'], 'a')
        self.assertTrue(not r['redelivered'])
        client.basic_reject(r)

        t = client.basic_get(queue=self.name)
        r = client.wait(t)
        self.assertEqual(r['body'], 'a')
        self.assertTrue(r['redelivered'])

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


    def test_basic_reject_no_requeue(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body='a')
        client.wait(promise)

        t = client.basic_get(queue=self.name)
        r = client.wait(t)
        self.assertEqual(r['body'], 'a')
        self.assertTrue(not r['redelivered'])
        client.basic_reject(r, requeue=False)

        t = client.basic_get(queue=self.name)
        r = client.wait(t)
        self.assertTrue(r['empty'])
        self.assertFalse('redelivered' in r)
        self.assertFalse('body' in r)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


    def test_basic_reject_dead_letter_exchange(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.exchange_declare(exchange=self.name1, type='fanout')
        client.wait(promise)

        promise = client.queue_declare(
            queue=self.name, arguments={'x-dead-letter-exchange': self.name1})
        client.wait(promise)

        promise = client.queue_declare(exclusive=True)
        dlxqname = client.wait(promise)['queue']

        promise = client.queue_bind(queue=dlxqname, exchange=self.name1)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body='a')
        client.wait(promise)

        t = client.basic_get(queue=self.name)
        r = client.wait(t)
        self.assertEqual(r['body'], 'a')
        self.assertTrue(not r['redelivered'])
        client.basic_reject(r, requeue=False)

        t = client.basic_get(queue=self.name)
        r = client.wait(t)
        self.assertTrue(r['empty'])
        self.assertFalse('redelivered' in r)
        self.assertFalse('body' in r)

        t = client.basic_get(queue=dlxqname)
        r = client.wait(t)
        self.assertEqual(r['body'], 'a')
        self.assertEqual(r['headers']['x-death'][0]['reason'], 'rejected')
        self.assertTrue(not r['redelivered'])

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)

        promise = client.exchange_delete(exchange=self.name1)
        client.wait(promise)


    def test_properties(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        t = client.queue_declare(queue=self.name)
        client.wait(t)

        headers = {
            "content_type": 'a',
            "content_encoding": 'b',
            #"headers":
            "delivery_mode": 2,
            "priority": 1,
            "correlation_id": 'd',
            "reply_to": 'e',
            "expiration": 'f',
            "message_id": 'g',
            "timestamp": 1,
            "type_": 'h',
            "user_id": 'guest',  # that one needs to match real user
            "app_id": 'j',
            "cluster_id": 'k',
            "custom": 'l',
            "blah2": [True, 1, -1, 64000L, None, float(12e10),
                      -64000L, [1,2,3,4, {"a":"b", "c":[]}]],
            }

        t = client.basic_publish(exchange='', routing_key=self.name,
                                 body='a', headers=headers.copy())
        client.wait(t)

        t = client.basic_get(queue=self.name, no_ack=True)
        r = client.wait(t)
        self.assertEqual(r['body'], 'a')
        recv_headers = r['headers']
        del recv_headers['x-puka-delivery-tag']

        self.assertEqual(repr(headers), repr(recv_headers))

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


    def test_basic_ack_fail(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body='a')
        client.wait(promise)

        promise = client.basic_consume(queue=self.name)
        result = client.wait(promise)

        with self.assertRaises(puka.PreconditionFailed):
            r2 = result.copy()
            r2['delivery_tag'] = 999
            client.basic_ack(r2)
            client.wait(promise)

        promise = client.basic_consume(queue=self.name)
        result = client.wait(promise)
        client.basic_ack(result)

        with self.assertRaises(AssertionError):
            client.basic_ack(result)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


    def test_basic_cancel(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        for i in range(2):
            promise = client.basic_publish(exchange='', routing_key=self.name,
                                           body='a')
            client.wait(promise)

        consume_promise = client.basic_consume(queue=self.name)
        msg1 = client.wait(consume_promise)
        self.assertEqual(msg1['body'], 'a')
        client.basic_ack(msg1)

        promise = client.basic_cancel(consume_promise)
        result = client.wait(promise)
        self.assertTrue('consumer_tag' in result)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body='b')
        client.wait(promise)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


    def test_close(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body=self.msg)
        client.wait(promise)

        consume_promise = client.basic_consume(queue=self.name)
        msg_result = client.wait(consume_promise)

        promise = client.queue_delete(self.name)
        client.wait(promise)

        promise = client.close()
        client.wait(promise)


    def test_basic_consume_fail(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        consume_promise = client.basic_consume(queue='bad_q_name')
        with self.assertRaises(puka.NotFound):
            msg_result = client.wait(consume_promise)

        promise = client.close()
        client.wait(promise)

    def test_broken_ack_on_close(self):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)

        promise = client.queue_declare()
        qname = client.wait(promise)['queue']

        promise = client.basic_publish(exchange='', routing_key=qname, body='a')
        client.wait(promise)

        promise = client.basic_get(queue=qname)
        r = client.wait(promise)
        self.assertEquals(r['body'], 'a')

        promise = client.queue_delete(queue=qname)
        client.wait(promise)

        promise = client.close()
        client.wait(promise)

    @base.connect
    def test_basic_qos(self, client):
        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body='a')
        client.wait(promise)
        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body='b')
        client.wait(promise)
        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body='c')
        client.wait(promise)

        consume_promise = client.basic_consume(queue=self.name, prefetch_count=1)
        result = client.wait(consume_promise, timeout=0.1)
        self.assertEqual(result['body'], 'a')

        result = client.wait(consume_promise, timeout=0.1)
        self.assertEqual(result, None)

        promise = client.basic_qos(consume_promise, prefetch_count=2)
        result = client.wait(promise)

        result = client.wait(consume_promise, timeout=0.1)
        self.assertEqual(result['body'], 'b')

        result = client.wait(consume_promise, timeout=0.1)
        self.assertEqual(result, None)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)



if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
