from __future__ import with_statement

import os
import puka

import base


class TestBasic(base.TestCase):
    def test_simple_roundtrip(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.queue_declare(queue=self.name)
        client.wait(primise)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body=self.msg)
        client.wait(primise)

        consume_primise = client.basic_consume(queue=self.name, no_ack=True)
        result = client.wait(consume_primise)
        self.assertEqual(result['body'], self.msg)

        primise = client.queue_delete(queue=self.name)
        client.wait(primise)


    def test_purge(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.queue_declare(queue=self.name)
        client.wait(primise)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body=self.msg)
        client.wait(primise)

        primise = client.queue_purge(queue=self.name)
        r = client.wait(primise)
        self.assertEqual(r['message_count'], 1)

        primise = client.queue_purge(queue=self.name)
        r = client.wait(primise)
        self.assertEqual(r['message_count'], 0)

        primise = client.queue_delete(queue=self.name)
        client.wait(primise)


    def test_basic_get_ack(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.queue_declare(queue=self.name)
        client.wait(primise)

        for i in range(4):
            primise = client.basic_publish(exchange='', routing_key=self.name,
                                          body=self.msg+str(i))
            client.wait(primise)

        msgs = []
        for i in range(4):
            primise = client.basic_get(queue=self.name)
            result = client.wait(primise)
            self.assertEqual(result['body'], self.msg+str(i))
            self.assertEqual(result['redelivered'], False)
            msgs.append( result )

        primise = client.basic_get(queue=self.name)
        result = client.wait(primise)
        self.assertEqual('body' in result, False)

        self.assertEqual(len(client.channels.free_channels), 1)
        self.assertEqual(client.channels.free_channel_numbers[-1], 7)
        for msg in msgs:
            client.basic_ack(msg)
        self.assertEqual(len(client.channels.free_channels), 5)
        self.assertEqual(client.channels.free_channel_numbers[-1], 7)

        primise = client.queue_delete(queue=self.name)
        client.wait(primise)


    def test_basic_publish_bad_exchange(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        for i in range(2):
            primise = client.basic_publish(exchange='invalid_exchange',
                                          routing_key='xxx', body='')

            self.assertEqual(len(client.channels.free_channels), 0)
            self.assertEqual(client.channels.free_channel_numbers[-1], 2)

            with self.assertRaises(puka.NotFound) as cm:
                client.wait(primise)

            (r,) = cm.exception # unpack args of exception
            self.assertTrue(r.is_error)
            self.assertEqual(r['reply_code'], 404)

            self.assertEqual(len(client.channels.free_channels), 0)
            self.assertEqual(client.channels.free_channel_numbers[-1], 2)


    def test_basic_return(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      mandatory=True, body='')
        with self.assertRaises(puka.NoRoute):
            client.wait(primise)

        primise = client.queue_declare(queue=self.name)
        client.wait(primise)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      mandatory=True, body='')
        client.wait(primise) # no error

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      immediate=True, body='')
        with self.assertRaises(puka.NoConsumers):
            client.wait(primise)

        primise = client.queue_delete(queue=self.name)
        client.wait(primise)


    def test_persistent(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.queue_declare(queue=self.name)
        client.wait(primise)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body=self.msg) # persistence=default
        client.wait(primise)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body=self.msg,
                                      headers={'persistent':True})
        client.wait(primise)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body=self.msg,
                                      headers={'persistent':False})
        client.wait(primise)

        primise = client.basic_get(queue=self.name, no_ack=True)
        result = client.wait(primise)
        self.assertTrue(result['headers']['persistent'])
        self.assertEquals(result['headers']['delivery_mode'], 2)

        primise = client.basic_get(queue=self.name, no_ack=True)
        result = client.wait(primise)
        self.assertTrue(result['headers']['persistent'])
        self.assertEquals(result['headers']['delivery_mode'], 2)

        primise = client.basic_get(queue=self.name, no_ack=True)
        result = client.wait(primise)
        self.assertTrue(not result['headers']['persistent'])
        self.assertTrue('delivery_mode' not in result['headers'])

        primise = client.queue_delete(queue=self.name)
        client.wait(primise)


    def test_basic_reject(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.queue_declare(queue=self.name)
        client.wait(primise)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body='a')
        client.wait(primise)

        t = client.basic_get(queue=self.name)
        r = client.wait(t)
        self.assertEqual(r['body'], 'a')
        self.assertTrue(not r['redelivered'])
        client.basic_reject(r)

        t = client.basic_get(queue=self.name)
        r = client.wait(t)
        self.assertEqual(r['body'], 'a')
        self.assertTrue(r['redelivered'])

        primise = client.queue_delete(queue=self.name)
        client.wait(primise)


    def test_properties(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        t = client.queue_declare(queue=self.name)
        client.wait(t)

        headers = {
            "content_type": 'a',
            "content_encoding": 'b',
            #"headers": ,
            #"delivery_mode": ,
            "priority": 1,
            "correlation_id": 'd',
            "reply_to": 'e',
            "expiration": 'f',
            "message_id": 'g',
            "timestamp": 1,
            "type_": 'h',
            "user_id": 'i',
            "app_id": 'j',
            "cluster_id": 'k',
            "custom": 'l',
            }

        t = client.basic_publish(exchange='', routing_key=self.name,
                                 body='a', headers=headers.copy())
        client.wait(t)

        t = client.basic_get(queue=self.name, no_ack=True)
        r = client.wait(t)
        self.assertEqual(r['body'], 'a')
        recv_headers = r['headers']
        del recv_headers['delivery_mode']
        del recv_headers['persistent']
        del recv_headers['x-puka-async-id']

        self.assertEqual(headers, recv_headers)

        primise = client.queue_delete(queue=self.name)
        client.wait(primise)


    def test_basic_ack_fail(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.queue_declare(queue=self.name)
        client.wait(primise)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body='a')
        client.wait(primise)

        primise = client.basic_consume(queue=self.name)
        result = client.wait(primise)

        with self.assertRaises(puka.PreconditionFailed):
            r2 = result.copy()
            r2['delivery_tag'] = 999
            client.basic_ack(r2)
            client.wait(primise)

        primise = client.basic_consume(queue=self.name)
        result = client.wait(primise)
        client.basic_ack(result)

        with self.assertRaises(AssertionError):
            client.basic_ack(result)

        primise = client.queue_delete(queue=self.name)
        client.wait(primise)


    def test_basic_cancel(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.queue_declare(queue=self.name)
        client.wait(primise)

        for i in range(2):
            primise = client.basic_publish(exchange='', routing_key=self.name,
                                          body='a')
            client.wait(primise)

        consume_primise = client.basic_consume(queue=self.name)
        msg1 = client.wait(consume_primise)
        self.assertEqual(msg1['body'], 'a')
        client.basic_ack(msg1)

        primise = client.basic_cancel(consume_primise)
        result = client.wait(primise)
        self.assertTrue('consumer_tag' in result)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body='b')
        client.wait(primise)

        primise = client.queue_delete(queue=self.name)
        client.wait(primise)


    def test_close(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.queue_declare(queue=self.name)
        client.wait(primise)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body=self.msg)
        client.wait(primise)

        consume_primise = client.basic_consume(queue=self.name)
        msg_result = client.wait(consume_primise)

        primise = client.queue_delete(self.name)
        client.wait(primise)

        primise = client.close()
        client.wait(primise)


    def test_basic_consume_fail(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        consume_primise = client.basic_consume(queue='bad_q_name')
        with self.assertRaises(puka.NotFound):
            msg_result = client.wait(consume_primise)

        primise = client.close()
        client.wait(primise)

    def test_broken_ack_on_close(self):
        client = puka.Client(self.amqp_url)
        primise = client.connect()
        client.wait(primise)

        primise = client.queue_declare()
        qname = client.wait(primise)['queue']

        primise = client.basic_publish(exchange='', routing_key=qname, body='a')
        client.wait(primise)

        primise = client.basic_get(queue=qname)
        r = client.wait(primise)
        self.assertEquals(r['body'], 'a')

        primise = client.queue_delete(queue=qname)
        client.wait(primise)

        primise = client.close()
        client.wait(primise)

    @base.connect
    def test_basic_qos(self, client):
        primise = client.queue_declare(queue=self.name)
        client.wait(primise)

        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body='a')
        client.wait(primise)
        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body='b')
        client.wait(primise)
        primise = client.basic_publish(exchange='', routing_key=self.name,
                                      body='c')
        client.wait(primise)

        consume_primise = client.basic_consume(queue=self.name, prefetch_count=1)
        result = client.wait(consume_primise, timeout=0.1)
        self.assertEqual(result['body'], 'a')

        result = client.wait(consume_primise, timeout=0.1)
        self.assertEqual(result, None)

        primise = client.basic_qos(consume_primise, prefetch_count=2)
        result = client.wait(primise)

        result = client.wait(consume_primise, timeout=0.1)
        self.assertEqual(result['body'], 'b')

        result = client.wait(consume_primise, timeout=0.1)
        self.assertEqual(result, None)

        primise = client.queue_delete(queue=self.name)
        client.wait(primise)



if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
