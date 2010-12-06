#!/usr/bin/env python

import sys
sys.path.append("..")

import puka
import threading
import time


counter = 0
counter_t0 = time.time()

def main():
    client = puka.Client("amqp://localhost/")
    promise = client.connect()
    client.wait(promise)

    body='\x00'


    def cbk0(q):
        client.queue_declare(queue=q, callback=cbk1, user_data=q)

    def cbk1(t, result, q):
        client.queue_purge(queue=q, callback=cbk_fill, user_data=(q, 10))

    def cbk_fill(t, result, ud):
        q, ct = ud
        if ct > 0:
            client.basic_publish(exchange='', routing_key=q, body=body,
                                 user_headers={'delivery_mode':2},
                                 callback=cbk_fill, user_data=(q, ct-1))
        else:
            cbk3(q)

    def cbk3(q):
        client.basic_get(queue=q, no_ack=True,
                         callback=cbk_get, user_data=q)

    def cbk_get(t, msg, q):
        client.basic_publish(exchange='', routing_key=q, body=body,
                             user_headers={'delivery_mode':2},
                             callback=cbk_ack, user_data=q)
    def cbk_ack(t, result, q):
        global counter
        # got message.
        counter += 1
        cbk3(q)


    for q in ['q%02i' % i for i in range(8)]:
        cbk0(q)

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
