from __future__ import with_statement

import os
import puka
import socket

import base


class TestConnection(base.TestCase):
    def test_broken_url(self):
        client = puka.Client('amqp://does.not.resolve/')
        with self.assertRaises(socket.gaierror):
            ticket = client.connect()

    def test_connection_refused(self):
        with self.assertRaises(socket.error):
            client = puka.Client('amqp://127.0.0.1:9999/')
            ticket = client.connect()
            client.wait(ticket)

    def test_connection_refused(self):
        with self.assertRaises(socket.error):
            client = puka.Client('amqp://127.0.0.1:9999/')
            ticket = client.connect()
            client.wait(ticket)


if __name__ == '__main__':
    import tests
    tests.run_unittests(globals())

