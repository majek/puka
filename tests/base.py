import functools
import os
import puka
import random
import unittest_backport as unittest


class TestCase(unittest.TestCase):
    def setUp(self):
        self.name = 'test%s' % (random.random(),)
        self.name1 = 'test%s' % (random.random(),)
        self.name2 = 'test%s' % (random.random(),)
        self.msg = '%s' % (random.random(),)
        self.amqp_url = os.getenv('AMQP_URL', 'amqp:///')

    def tearDown(self):
        pass


def connect(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)
        r = method(self, client, *args, **kwargs)
        promise = client.close()
        client.wait(promise)
        return r
    return wrapper
