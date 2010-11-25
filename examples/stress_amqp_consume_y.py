#!/usr/bin/env python

import sys
sys.path.append("..")

import puka
import time


counter = 0
counter_t0 = time.time()

headers={'persistent': False}


AMQP_URL="amqp://localhost/"
QUEUE_CNT=2
BURST_SIZE=12
QUEUE_SIZE=1000
BODY_SIZE=1
PREFETCH_CNT=1


TICKETS = {}

def blah(method):
    def wrapper(client, *args, **kwargs):
        gen = method(client, *args, **kwargs)
        ticket = gen.next()
        client.set_callback(ticket, lambda t, result: \
                                callback_wrapper(client, gen, t, result))
        TICKETS[gen] = ticket
    return wrapper

def callback_wrapper(client, gen, t, result):
    ticket = gen.send(result)
    client.set_callback(ticket, lambda t, result: \
                            callback_wrapper(client, gen, t, result))
    TICKETS[gen] = ticket


@blah
def worker(client, q, msg_cnt, body, prefetch_cnt, inc):
    result = (yield client.queue_declare(queue=q))
    fill = max(msg_cnt - result['message_count'], 0)

    while fill > 0:
        fill -= BURST_SIZE
        for i in xrange(BURST_SIZE):
            ticket = client.basic_publish(exchange='', routing_key=q,
                                          body=body, headers=headers)
        yield ticket # Wait only for one in burst (the last one).
        inc(BURST_SIZE)

    consume_ticket = client.basic_consume(queue=q, prefetch_count=prefetch_cnt)
    while True:
        msg = (yield consume_ticket)
        client.basic_publish(exchange='', routing_key=q,
                             body=body, headers=headers)
        client.basic_ack(msg)
        inc()


def main():
    client = puka.Client(AMQP_URL)
    ticket = client.connect()
    client.wait(ticket)

    def inc(value=1):
        global counter
        counter += value

    for q in ['q%04i' % i for i in range(QUEUE_CNT)]:
        worker(client, q, QUEUE_SIZE, 'a' * BODY_SIZE, PREFETCH_CNT, inc)


    print ' [*] loop'
    while True:
        t0 = time.time()
        t1 = t0 + 1.0
        while True:
            td = t1 - time.time()
            if td > 0:
                client.wait(TICKETS.values(), timeout=td)
            else:
                break
        global counter
        td = time.time() - t0
        print "send: %i " % (counter/td,)
        counter = 0



if __name__ == '__main__':
    main()
