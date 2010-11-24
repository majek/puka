import os
import puka
import random

import base


class TestBasic(base.TestCase):
    def test_simple_roundtrip(self):
        client = puka.Client(self.amqp_url)

        def on_connect(t, result):
            client.queue_declare(queue=self.name,
                                 callback=on_queue_declare)

        def on_queue_declare(t, result):
            client.basic_publish(exchange='', routing_key=self.name,
                                 body=self.msg,
                                 callback=on_basic_publish)

        def on_basic_publish(t, result):
            client.basic_get(queue=self.name,
                             callback=on_basic_get)

        def on_basic_get(t, result):
            self.assertEqual(result['body'], self.msg)
            client.basic_ack(result)
            client.queue_delete(queue=self.name,
                                callback=on_queue_delete)

        def on_queue_delete(t, result):
            client.loop_break()

        client.connect(callback=on_connect)
        client.loop()

        ticket = client.close()
        client.wait(ticket)


    def test_close(self):
        def on_connection(ticket, result):
            client.queue_declare(queue=self.name, callback=on_queue_declare)

        def on_queue_declare(ticket, result):
            client.basic_publish(exchange='', routing_key=self.name,
                                 body="Hello world!",
                                 callback=on_basic_publish)

        def on_basic_publish(ticket, result):
            client.queue_delete(queue=self.name,
                                callback=on_queue_delete)

        def on_queue_delete(ticket, result):
            client.loop_break()

        client = puka.Client(self.amqp_url)
        client.connect(callback=on_connection)
        client.loop()

        ticket = client.close()
        client.wait(ticket)


    def test_consume_close(self):
        def on_connection(ticket, result):
            client.queue_declare(queue=self.name, auto_delete=True,
                                 callback=on_queue_declare)

        def on_queue_declare(ticket, result):
            client.basic_consume(queue=self.name, callback=on_basic_consume)
            client.loop_break()

        def on_basic_consume(ticket, result):
            self.assertTrue(result.is_error)

        client = puka.Client(self.amqp_url)
        client.connect(callback=on_connection)
        client.loop()

        ticket = client.close()
        client.wait(ticket)

        client.run_any_callbacks()

if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())

