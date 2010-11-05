import os
import puka
import random
import unittest_backport as unittest


AMQP_URL=os.getenv('AMQP_URL')

class TestExchange(unittest.TestCase):
    def test_exchange_redeclare(self):
        ename = 'test%s' % (random.random(),)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.exchange_declare(exchange=ename)
        r = client.wait(ticket)
        self.assertTrue('exists' not in r)

        ticket = client.exchange_declare(exchange=ename)
        r = client.wait(ticket)
        self.assertTrue(r['exists'])

        # Puka can't verify if the exchange has the same properties.
        ticket = client.exchange_declare(exchange=ename, type='fanout')
        r = client.wait(ticket)
        self.assertTrue(r['exists'])

        # Unless you can sacrifice the connection...
        ticket = client.exchange_declare(exchange=ename, suicidal=True)
        client.wait(ticket)

        ticket = client.exchange_declare(exchange=ename, type='fanout',
                                         suicidal=True)
        with self.assertRaises(puka.exceptions.NotAllowed):
            client.wait(ticket)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.exchange_delete(exchange=ename)
        client.wait(ticket)

    def test_exchange_delete_not_found(self):
        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        ticket = client.exchange_delete(exchange='not_existing_exchange')

        with self.assertRaises(puka.exceptions.NotFound):
            client.wait(ticket)

