#!/usr/bin/env python

import sys
sys.path.append("..")

import puka


client = puka.Client("amqp://localhost/")
promise = client.connect()
client.wait(promise)

promise = client.queue_declare(queue='test')
client.wait(promise)

print "  [*] Waiting for a message. Press CTRL+C to quit."

consume_promise = client.basic_consume(queue='test')
result = client.wait(consume_promise)
print " [x] Received message %r" % (result,)

client.basic_ack(result)

client.basic_cancel(consume_promise)
client.wait(consume_promise)

promise = client.close()
client.wait(promise)
