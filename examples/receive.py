#!/usr/bin/env python

import sys
sys.path.append("..")

import puka


client = puka.Client("amqp://localhost/")
ticket = client.connect()
client.wait(ticket)

ticket = client.queue_declare(queue='test')
client.wait(ticket)

print "  [*] Waiting for messages. Press CTRL+C to quit."

consume_ticket = client.basic_consume(queue='test')
while True:
    result = client.wait(consume_ticket)
    print " [x] Received message %r" % (result,)

    client.basic_ack(result)


