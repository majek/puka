from __future__ import with_statement

import os
import puka

import base


class TestBasicConsumeMulti(base.TestCase):

    @base.connect
    def test_shared_qos(self, client):
        promise = client.queue_declare(queue=self.name1)
        self.cleanup_promise(client.queue_delete, queue=self.name1)
        client.wait(promise)

        promise = client.queue_declare(queue=self.name2)
        self.cleanup_promise(client.queue_delete, queue=self.name2)
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

    @base.connect
    def test_access_refused(self, client):
        promise = client.queue_declare(queue=self.name, exclusive=True)
        self.cleanup_promise(client.queue_delete, queue=self.name)
        client.wait(promise)

        promise = client.queue_declare(queue=self.name)
        with self.assertRaises(puka.ResourceLocked):
            client.wait(promise)

        # Testing exclusive basic_consume.
        promise = client.basic_consume(queue=self.name, exclusive=True)
        client.wait(promise, timeout=0.001)

        # Do something synchronus.
        promise = client.queue_declare(exclusive=True)
        client.wait(promise)

        promise = client.basic_consume(queue=self.name)
        with self.assertRaises(puka.AccessRefused):
            client.wait(promise)


    @base.connect
    def test_consumer_tag(self, client):
        # In Puka it's impossible to get `consumer_tag` from
        # basic_consume result. And usually it doesn't matter. But
        # when calling `basic_consume_multi` that starts to be an
        # issue: how to distinguish one queue from another? In #38 we
        # enabled manually specifying consumer tags.

        p1 = client.queue_declare(queue=self.name1)
        p2 = client.queue_declare(queue=self.name2)
        self.cleanup_promise(client.queue_delete, queue=self.name1)
        self.cleanup_promise(client.queue_delete, queue=self.name2)
        client.wait_for_all([p1, p2])

        # Single consumer, unspecified tag
        promise = client.basic_publish(exchange='', routing_key=self.name1,
                                       body=self.msg)
        client.wait(promise)

        # basic_get doesn't return consumer_tag
        consume_promise = client.basic_consume(queue=self.name1)
        result = client.wait(consume_promise)
        self.assertEqual(result['body'], self.msg)
        self.assertEqual(result['consumer_tag'], '%s.0.' % consume_promise)
        client.basic_ack(result)
        promise = client.basic_cancel(consume_promise)
        result = client.wait(promise)

        # Consume multi
        p1 = client.basic_publish(exchange='', routing_key=self.name1,
                                  body=self.msg1)
        p2 = client.basic_publish(exchange='', routing_key=self.name2,
                                  body=self.msg2)
        client.wait_for_all([p1, p2])

        consume_promise = client.basic_consume_multi([
                self.name1,
                {'queue': self.name2,
                 'consumer_tag': 'whooa!'}])

        for _ in range(2):
            result = client.wait(consume_promise)
            if result['body'] == self.msg1:
                self.assertEqual(result['body'], self.msg1)
                self.assertEqual(result['consumer_tag'],
                                 '%s.0.' % consume_promise)
            else:
                self.assertEqual(result['body'], self.msg2)
                self.assertEqual(result['consumer_tag'],
                                 '%s.1.whooa!' % consume_promise)
            client.basic_ack(result)

    @base.connect
    def test_consumer_tag_repeated(self, client):
        # In theory consumer_tags are unique. But our users may not
        # know about it. Test puka's behaviour in that case

        p1 = client.queue_declare(queue=self.name1)
        p2 = client.queue_declare(queue=self.name2)
        client.wait_for_all([p1, p2])

        try:
            promise = client.basic_publish(exchange='', routing_key=self.name1,
                                           body=self.msg)
            client.wait(promise)

            consume_promise = client.basic_consume_multi([
                    {'queue': self.name1,
                     'consumer_tag': 'repeated'},
                    {'queue': self.name1,
                     'consumer_tag': 'repeated'},
                    {'queue': self.name2,
                     'consumer_tag': 'repeated'}])

            result = client.wait(consume_promise)
            self.assertEqual(result['body'], self.msg)
            ct = result['consumer_tag'].split('.')
            self.assertEqual(ct[0], '%s' % consume_promise)
            self.assertTrue(ct[1] in ('0', '1', '2'))
            self.assertEqual(ct[2], 'repeated')

        finally:
            p1 = client.queue_delete(queue=self.name1)
            p2 = client.queue_delete(queue=self.name2)
            client.wait_for_all([p1, p2])


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())

