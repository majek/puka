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
    ticket = client.connect()
    client.wait(ticket)

    body='\x00'


    def cbk0(q):
        client.queue_declare(queue=q, callback=cbk1, user_data=q)

    def cbk1(t, result, q):
        client.queue_purge(queue=q, callback=cbk2, user_data=q)

    def cbk2(t, result, q):
        client.basic_publish(exchange='', routing_key=q, body=body,
                             user_headers={'delivery_mode':2},
                             callback=cbk3, user_data=q)

    def cbk3(t, result, q):
        client.basic_get(queue=q, no_ack=True,
                         callback=cbk4, user_data=q)

    def cbk4(t, result, q):
        global counter
        # got message.
        counter += 1
        cbk2(None, None, q)


    for q in ['q%02i' % i for i in range(64)]:
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
