import os
import unittest
import puka
import random
import time

AMQP_URL=os.getenv('AMQP_URL')

class TestLimits(unittest.TestCase):
    def test_parallel_queue_declare(self):
        qname = 'test%s' % (random.random(),)
        msg = '%s' % (random.random(),)

        client = puka.Client(AMQP_URL)
        ticket = client.connect()
        client.wait(ticket)

        tickets = [client.queue_declare(queue=qname+'.%s' % (i,),
                                        auto_delete=True)
                   for i in xrange(1000)]

        for ticket in tickets:
            client.wait(ticket)

