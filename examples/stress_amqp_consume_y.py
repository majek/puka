#!/usr/bin/env python

import sys
sys.path.append("..")

import puka
import time


counter = 0
counter_t0 = time.time()

headers={'persistent': False}


#AMQP_URL="amqp://a:a@localhost/a"
AMQP_URL="amqp://localhost/"
QUEUE_CNT=20
BURST_SIZE=120
QUEUE_SIZE=17000
BODY_SIZE=1
PREFETCH_CNT=1


PRIMISES = {}

def blah(method):
    def wrapper(client, *args, **kwargs):
        gen = method(client, *args, **kwargs)
        promise = gen.next()
        client.set_callback(promise, lambda t, result: \
                                callback_wrapper(client, gen, t, result))
        PRIMISES[gen] = promise
    return wrapper

def callback_wrapper(client, gen, t, result):
    promise = gen.send(result)
    client.set_callback(promise, lambda t, result: \
                            callback_wrapper(client, gen, t, result))
    PRIMISES[gen] = promise


@blah
def worker(client, q, msg_cnt, body, prefetch_cnt, inc, avg):
    result = (yield client.queue_declare(queue=q))
    fill = max(msg_cnt - result['message_count'], 0)

    while fill > 0:
        fill -= BURST_SIZE
        for i in xrange(BURST_SIZE):
            promise = client.basic_publish(exchange='', routing_key=q,
                                          body=body, headers=headers)
        yield promise # Wait only for one in burst (the last one).
        inc(BURST_SIZE)

    consume_promise = client.basic_consume(queue=q, prefetch_count=prefetch_cnt)
    while True:
        msg = (yield consume_promise)
        t0 = time.time()
        yield client.basic_publish(exchange='', routing_key=q,
                                   body=body, headers=headers)
        td = time.time() - t0
        avg(td)
        client.basic_ack(msg)
        inc()

average = average_count = 0.0

def main():
    client = puka.Client(AMQP_URL)
    promise = client.connect()
    client.wait(promise)

    def inc(value=1):
        global counter
        counter += value

    def avg(td):
        global average, average_count
        average += td
        average_count += 1

    for q in ['q%04i' % i for i in range(QUEUE_CNT)]:
        worker(client, q, QUEUE_SIZE, 'a' * BODY_SIZE, PREFETCH_CNT, inc, avg)


    print ' [*] loop'
    while True:
        t0 = time.time()
        t1 = t0 + 1.0
        while True:
            td = t1 - time.time()
            if td > 0:
                client.wait(PRIMISES.values(), timeout=td)
            else:
                break
        global counter, average, average_count
        td = time.time() - t0
        average_count = max(average_count, 1.0)
        print "send: %i  avg: %.3fms " % (counter/td, (average/average_count)*1000.0)
        counter = average = average_count = 0



if __name__ == '__main__':
    main()
