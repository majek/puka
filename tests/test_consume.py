from __future__ import with_statement

import os
import puka

import base


class TestBasicConsumeMulti(base.TestCase):
    @base.connect
    def test_shared_qos(self, client):
        promise = client.queue_declare(queue=self.name1)
        client.wait(promise)

        promise = client.queue_declare(queue=self.name2)
        client.wait(promise)


        promise = client.basic_publish(exchange='', routing_key=self.name1,
                                      body='a')
        client.wait(promise)
        promise = client.basic_publish(exchange='', routing_key=self.name2,
                                      body='b')
        client.wait(promise)


        consume_promise = client.basic_consume_multi([self.name1, self.name2],
                                                    prefetch_count=1)
        result = client.wait(consume_promise, timeout=0.1)
        r1 = result['body']
        self.assertTrue(r1 in ['a', 'b'])

        result = client.wait(consume_promise, timeout=0.1)
        self.assertEqual(result, None)

        promise = client.basic_qos(consume_promise, prefetch_count=2)
        result = client.wait(promise)

        result = client.wait(consume_promise, timeout=0.1)
        r2 = result['body']
        self.assertEqual(sorted([r1, r2]), ['a', 'b'])


        promise = client.basic_cancel(consume_promise)
        client.wait(promise)

        promise = client.queue_delete(queue=self.name1)
        client.wait(promise)

        promise = client.queue_delete(queue=self.name2)
        client.wait(promise)


    @base.connect
    def test_access_refused(self, client):
        promise = client.queue_declare(queue=self.name, exclusive=True)
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        with self.assertRaises(puka.ResourceLocked):
            client.wait(promise)

        # Testing exclusive basic_consume.
        promise = client.basic_consume(queue=self.name, exclusive=True)
        client.wait(promise, timeout=0.001)

        # Do something syncrhonus.
        promise = client.queue_declare(exclusive=True)
        client.wait(promise)

        promise = client.basic_consume(queue=self.name)
        with self.assertRaises(puka.AccessRefused):
            client.wait(promise)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())

