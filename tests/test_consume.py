from __future__ import with_statement

import os
import puka

import base


class TestBasicConsumeMulti(base.TestCase):
    @base.connect
    def test_shared_qos(self, client):
        ticket = client.queue_declare(queue=self.name1)
        client.wait(ticket)

        ticket = client.queue_declare(queue=self.name2)
        client.wait(ticket)


        ticket = client.basic_publish(exchange='', routing_key=self.name1,
                                      body='a')
        client.wait(ticket)
        ticket = client.basic_publish(exchange='', routing_key=self.name2,
                                      body='b')
        client.wait(ticket)


        consume_ticket = client.basic_consume_multi([self.name1, self.name2],
                                                    prefetch_count=1)
        result = client.wait(consume_ticket, timeout=0.1)
        r1 = result['body']
        self.assertTrue(r1 in ['a', 'b'])

        result = client.wait(consume_ticket, timeout=0.1)
        self.assertEqual(result, None)

        ticket = client.basic_qos(consume_ticket, prefetch_count=2)
        result = client.wait(ticket)

        result = client.wait(consume_ticket, timeout=0.1)
        r2 = result['body']
        self.assertEqual(sorted([r1, r2]), ['a', 'b'])


        ticket = client.basic_cancel(consume_ticket)
        client.wait(ticket)

        ticket = client.queue_delete(queue=self.name1)
        client.wait(ticket)

        ticket = client.queue_delete(queue=self.name2)
        client.wait(ticket)


    @base.connect
    def test_access_refused(self, client):
        ticket = client.queue_declare(queue=self.name, exclusive=True)
        client.wait(ticket)

        ticket = client.queue_declare(queue=self.name)
        with self.assertRaises(puka.ResourceLocked):
            client.wait(ticket)

        ticket = client.basic_consume(queue=self.name, exclusive=True)
        client.wait(ticket, timeout=0.01)

        ticket = client.basic_consume(queue=self.name)
        with self.assertRaises(puka.AccessRefused):
            client.wait(ticket)

        ticket = client.queue_delete(queue=self.name)
        client.wait(ticket)


