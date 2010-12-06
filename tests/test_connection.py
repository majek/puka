from __future__ import with_statement

import os
import puka
import socket

import base


class TestConnection(base.TestCase):
    def test_broken_url(self):
        client = puka.Client('amqp://does.not.resolve/')
        with self.assertRaises(socket.gaierror):
            primise = client.connect()

    def test_connection_refused(self):
        client = puka.Client('amqp://127.0.0.1:9999/')
        with self.assertRaises(socket.error):
            # Can raise in connect or on wait
            primise = client.connect()
            client.wait(primise)

    # The following tests take 3 seconds each, due to Rabbit.
    def test_wrong_user(self):
        client = puka.Client('amqp://xxx:bbb@127.0.0.1/')
        primise = client.connect()
        with self.assertRaises(puka.ConnectionBroken):
            client.wait(primise)

    # def test_wrong_vhost(self):
    #     client = puka.Client('amqp:///xxxx')
    #     primise = client.connect()
    #     with self.assertRaises(puka.ConnectionBroken):
    #         client.wait(primise)


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())

