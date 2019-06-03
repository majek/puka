import os
import unittest
import puka
import random
import time

import base

class TestLimits(base.TestCase):

    @base.connect
    def test_parallel_queue_declare(self, client):
        qname = 'test%s' % (random.random(),)
        msg = '%s' % (random.random(),)

        queues = [qname+'.%s' % (i,) for i in xrange(100)]
        promises = [client.queue_declare(queue=q) for q in queues]

        for promise in promises:
            client.wait(promise)

        promises = [client.queue_delete(queue=q) for q in queues]
        for promise in promises:
            client.wait(promise)
