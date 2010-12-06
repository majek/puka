#!/usr/bin/env python

import sys
sys.path.append("..")

import puka
import time
import collections

counter = 0
counter_t0 = time.time()

headers={'persistent': False}


#AMQP_URL="amqp://a:a@localhost/a"
AMQP_URL="amqp://localhost/"
QUEUE_CNT=1
BURST_SIZE=120
QUEUE_SIZE=17000
BODY_SIZE=1
PREFETCH_CNT=1


def blah(method):
    waiting_for = [None]
    promises = collections.defaultdict(list)
    def callback_wrapper(client, gen, t, result):
        promises[t].append(result)
        while waiting_for[0] in promises:
            result = promises[waiting_for[0]].pop(0)
            if not promises[waiting_for[0]]:
                del promises[waiting_for[0]]
            waiting_for[0] = gen.send(result)
        client.set_callback(waiting_for[0], lambda t, result: \
                            callback_wrapper(client, gen, t, result))

    def wrapper(client, *args, **kwargs):
        gen = method(client, *args, **kwargs)
        waiting_for[0] = gen.next()
        client.set_callback(waiting_for[0], lambda t, result: \
                                callback_wrapper(client, gen, t, result))
    return wrapper



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
        client.basic_publish(exchange='', routing_key=q,
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


    global counter, average, average_count

    print ' [*] loop'
    while True:
        t0 = time.time()
        client.loop(timeout=1.0)
        td = time.time() - t0
        average_count = max(average_count, 1.0)
        print "send: %i  avg: %.3fms " % (counter/td, (average/average_count)*1000.0)
        counter = average = average_count = 0



if __name__ == '__main__':
    main()
