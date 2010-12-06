#!/usr/bin/env python

import sys
sys.path.append("..")


import puka

client = puka.Client("amqp://localhost/")

promise = client.connect()
client.wait(promise)

promise = client.queue_declare(queue='test')
client.wait(promise)

promise = client.basic_publish(exchange='', routing_key='test',
                              body="Hello world!")
client.wait(promise)

print " [*] Message sent"

promise = client.close()
client.wait(promise)

