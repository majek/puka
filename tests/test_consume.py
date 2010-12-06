from __future__ import with_statement

import os
import puka

import base


class TestBasicConsumeMulti(base.TestCase):
    @base.connect
    def test_shared_qos(self, client):
        primise = client.queue_declare(queue=self.name1)
        client.wait(primise)

        primise = client.queue_declare(queue=self.name2)
        client.wait(primise)


        primise = client.basic_publish(exchange='', routing_key=self.name1,
                                      body='a')
        client.wait(primise)
        primise = client.basic_publish(exchange='', routing_key=self.name2,
                                      body='b')
        client.wait(primise)


        consume_primise = client.basic_consume_multi([self.name1, self.name2],
                                                    prefetch_count=1)
        result = client.wait(consume_primise, timeout=0.1)
        r1 = result['body']
        self.assertTrue(r1 in ['a', 'b'])

        result = client.wait(consume_primise, timeout=0.1)
        self.assertEqual(result, None)

        primise = client.basic_qos(consume_primise, prefetch_count=2)
        result = client.wait(primise)

        result = client.wait(consume_primise, timeout=0.1)
        r2 = result['body']
        self.assertEqual(sorted([r1, r2]), ['a', 'b'])


        primise = client.basic_cancel(consume_primise)
        client.wait(primise)

        primise = client.queue_delete(queue=self.name1)
        client.wait(primise)

        primise = client.queue_delete(queue=self.name2)
        client.wait(primise)


    @base.connect
    def test_access_refused(self, client):
        primise = client.queue_declare(queue=self.name, exclusive=True)
        client.wait(primise)

        primise = client.queue_declare(queue=self.name)
        with self.assertRaises(puka.ResourceLocked):
            client.wait(primise)

        primise = client.basic_consume(queue=self.name, exclusive=True)
        client.wait(primise, timeout=0.01)

        primise = client.basic_consume(queue=self.name)
        with self.assertRaises(puka.AccessRefused):
            client.wait(primise)

        primise = client.queue_delete(queue=self.name)
        client.wait(primise)


