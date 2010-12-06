#!/usr/bin/env python

import sys
sys.path.append("..")


import puka

client = puka.Client("amqp://localhost/")

primise = client.connect()
client.wait(primise)

primise = client.queue_declare(queue='test')
client.wait(primise)

primise = client.basic_publish(exchange='', routing_key='test',
                              body="Hello world!")
client.wait(primise)

print " [*] Message sent"

primise = client.close()
client.wait(primise)

