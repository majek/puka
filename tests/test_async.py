import os
import puka
import random

import base


class TestBasic(base.TestCase):
    def test_simple_roundtrip(self):
        client = puka.Client(self.amqp_url)

        def on_connect(t, result, ud):
            client.queue_declare(queue=self.name,
                                 callback=on_queue_declare)

        def on_queue_declare(t, result, ud):
            client.basic_publish(exchange='', routing_key=self.name,
                                 body=self.msg,
                                 callback=on_basic_publish)

        def on_basic_publish(t, result, ud):
            client.basic_get(queue=self.name,
                             callback=on_basic_get)

        def on_basic_get(t, result, ud):
            self.assertEqual(result['body'], self.msg)
            client.basic_ack(result)
            client.queue_delete(queue=self.name,
                                callback=on_queue_delete)

        def on_queue_delete(t, result, ud):
            client.loop_break()

        client.connect(callback=on_connect)
        client.loop()

if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())
