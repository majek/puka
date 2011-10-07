from __future__ import with_statement

import os
import puka

import base


class TestCancel(base.TestCase):
    @base.connect
    def test_cancel_single(self, client):
        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body='a')
        client.wait(promise)

        consume_promise = client.basic_consume(queue=self.name, prefetch_count=1)
        result = client.wait(consume_promise)
        self.assertEqual(result['body'], 'a')

        promise = client.basic_cancel(consume_promise)
        result = client.wait(promise)
        self.assertTrue('consumer_tag' in result)

        # TODO: better error
        # It's illegal to wait on consume_promise after cancel.
        #with self.assertRaises(Exception):
        #    client.wait(consume_promise)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)


    @base.connect
    def test_cancel_multi(self, client):
        names = [self.name, self.name1, self.name2]
        for name in names:
            promise = client.queue_declare(queue=name)
            client.wait(promise)
            promise = client.basic_publish(exchange='', routing_key=name,
                                           body='a')
            client.wait(promise)


        consume_promise = client.basic_consume_multi(queues=names,
                                                     no_ack=True)
        for i in range(len(names)):
            result = client.wait(consume_promise)
            self.assertEqual(result['body'], 'a')

        promise = client.basic_cancel(consume_promise)
        result = client.wait(promise)
        self.assertTrue('consumer_tag' in result)

        # TODO: better error
        #with self.assertRaises(Exception):
        #    client.wait(consume_promise)

        promise = client.queue_delete(queue=self.name)
        client.wait(promise)

    @base.connect
    def test_cancel_single_notification(self, client):
        promise = client.queue_declare(queue=self.name)
        client.wait(promise)

        promise = client.basic_publish(exchange='', routing_key=self.name,
                                       body='a')
        client.wait(promise)

        consume_promise = client.basic_consume(queue=self.name, prefetch_count=1)
        result = client.wait(consume_promise)
        self.assertEqual(result['body'], 'a')

        promise = client.queue_delete(self.name)

        result = client.wait(consume_promise)
        self.assertEqual(result.name, 'basic.cancel_ok')

        # Make sure the consumer died:
        promise = client.queue_declare(queue=self.name)
        result = client.wait(promise)
        self.assertEqual(result['consumer_count'], 0)


    @base.connect
    def test_cancel_multi_notification(self, client):
        names = [self.name, self.name1, self.name2]
        for name in names:
            promise = client.queue_declare(queue=name)
            client.wait(promise)
            promise = client.basic_publish(exchange='', routing_key=name,
                                           body='a')
            client.wait(promise)

        consume_promise = client.basic_consume_multi(queues=names,
                                                     no_ack=True)
        for i in range(len(names)):
            result = client.wait(consume_promise)
            self.assertEqual(result['body'], 'a')

        promise = client.queue_delete(names[0])

        result = client.wait(consume_promise)
        self.assertEqual(result.name, 'basic.cancel_ok')

        # Make sure the consumer died:
        for name in names:
            promise = client.queue_declare(queue=name)
            result = client.wait(promise)
            self.assertEqual(result['consumer_count'], 0)

    @base.connect
    def test_cancel_multi_notification_concurrent(self, client):
        names = [self.name, self.name1, self.name2]
        for name in names:
            promise = client.queue_declare(queue=name)
            client.wait(promise)
            promise = client.basic_publish(exchange='', routing_key=name,
                                           body='a')
            client.wait(promise)

        consume_promise = client.basic_consume_multi(queues=names,
                                                     no_ack=True)
        for i in range(len(names)):
            result = client.wait(consume_promise)
            self.assertEqual(result['body'], 'a')

        client.queue_delete(names[0])
        client.queue_delete(names[2])

        result = client.wait(consume_promise)
        self.assertEqual(result.name, 'basic.cancel_ok')

        # Make sure the consumer died:
        for name in names:
            promise = client.queue_declare(queue=name)
            result = client.wait(promise)
            self.assertEqual(result['consumer_count'], 0)



if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
