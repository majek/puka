#!/usr/bin/env python

import sys
sys.path.append("..")

import puka
import threading
import time


counter = 0
counter_t0 = time.time()

headers={'persistent': False}


class Worker(object):
    def __init__(self, client, q, inc):
        self.client = client
        self.q = q
        self.inc = inc
        self.body='\x00'
        self.ct = 100
        self.prefetch = 3
        self.client.queue_declare(queue=self.q, callback=self.cbk1)

    def cbk1(self, t, result):
        self.client.queue_purge(queue=self.q, callback=self.cbk_fill)

    def cbk_fill(self, t, result):
        if self.ct > 0:
            self.ct -= 1
            self.client.basic_publish(exchange='', routing_key=self.q,
                                      body=self.body, headers=headers,
                                      callback=self.cbk_fill)
        else:
            self.cbk3()

    def cbk3(self):
        self.client.basic_consume(queue=self.q, prefetch_count=self.prefetch,
                             callback=self.cbk_consume)

    def cbk_consume(self, t, msg):
        self.client.basic_publish(exchange='', routing_key=self.q,
                                  body=self.body, headers=headers,
                             callback= \
                                 lambda t, result:self.cbk_ack(t, result, msg))

    def cbk_ack(self, t, result, msg):
        self.client.basic_ack(msg)
        self.inc()


def main():
    client = puka.Client("amqp://localhost/")
    primise = client.connect()
    client.wait(primise)

    def inc():
        global counter
        # got message.
        counter += 1

    for q in ['q%02i' % i for i in range(51)]:
        Worker(client, q, inc)

    def print_counter():
        global counter, counter_t0
        counter_t1 = time.time()
        td = counter_t1 - counter_t0
        print "send: %i " % (counter/td,)
        counter = 0
        counter_t0 = counter_t1
        threading.Timer(1.0, print_counter).start()

    threading.Timer(1.0, print_counter).start()
    print ' [*] loop'
    client.wait_for_any()


if __name__ == '__main__':
    main()
