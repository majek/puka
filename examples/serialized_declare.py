#!/usr/bin/env python

import sys
sys.path.append("..")

import logging
FORMAT_CONS = '%(asctime)s %(name)-12s %(levelname)8s\t%(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT_CONS)



import puka

client = puka.Client("amqp://localhost/")

ticket = client.connect()
client.wait(ticket)

for i in range(1000):
    ticket = client.queue_declare(queue='a%04i' % i)
    client.wait(ticket)

for i in range(1000):
    ticket = client.queue_delete(queue='a%04i' % i)
    client.wait(ticket)

ticket = client.close()
client.wait(ticket)
