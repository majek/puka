#!/usr/bin/env python

import sys
sys.path.append("..")

import puka


client = puka.Client("amqp://localhost/")
primise = client.connect()
client.wait(primise)

primise = client.queue_declare(queue='test')
client.wait(primise)

print "  [*] Waiting for a message. Press CTRL+C to quit."

consume_primise = client.basic_consume(queue='test')
result = client.wait(consume_primise)
print " [x] Received message %r" % (result,)

client.basic_ack(result)

client.basic_cancel(consume_primise)
client.wait(consume_primise)

primise = client.close()
client.wait(primise)
