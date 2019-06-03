import functools
import logging
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
        self.msg1 = '%s' % (random.random(),)
        self.msg2 = '%s' % (random.random(),)
        self.amqp_url = os.getenv('AMQP_URL', 'amqp:///')
        self.client = None
        self._promise_cleanup = []

    def tearDown(self):
        self.run_cleanup_promises()

    def cleanup_promise(self, fn, *args, **kwargs):
        self._promise_cleanup.append((fn, args, kwargs))

    def run_cleanup_promises(self, client=None):
        if not client:
            client = self.client
        if not client:
            return

        promises, self._promise_cleanup = self._promise_cleanup, []
        for cb, args, kwargs in reversed(promises):
            try:
                client.wait(cb(*args, **kwargs))
            except Exception as e:
                logging.error("failed to run client cleanup callback %r: %s", cb, e)

def connect(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        self.client = client = puka.Client(self.amqp_url)
        promise = client.connect()
        client.wait(promise)
        r = None
        try:
            r = method(self, client, *args, **kwargs)
        finally:
            self.run_cleanup_promises()
        promise = client.close()
        client.wait(promise)
        self.client = None
        return r
    return wrapper
