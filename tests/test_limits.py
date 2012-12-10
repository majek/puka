import os
import unittest
import puka
import random
import time

AMQP_URL=os.getenv('AMQP_URL', 'amqp:///')

class TestLimits(unittest.TestCase):
    def test_parallel_queue_declare(self):
        qname = 'test%s' % (random.random(),)
        msg = '%s' % (random.random(),)

        client = puka.Client(AMQP_URL)
        promise = client.connect()
        client.wait(promise)

        queues = [qname+'.%s' % (i,) for i in xrange(100)]
        promises = [client.queue_declare(queue=q) for q in queues]

        for promise in promises:
            client.wait(promise)

        promises = [client.queue_delete(queue=q) for q in queues]
        for promise in promises:
            client.wait(promise)

