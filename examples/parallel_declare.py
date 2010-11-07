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

tickets = [client.queue_declare(queue='a%04i' % i) for i in range(1000)]
for ticket in tickets:
    client.wait(ticket)

tickets = [client.queue_delete(queue='a%04i' % i) for i in range(1000)]
for ticket in tickets:
    client.wait(ticket)

ticket = client.close()
client.wait(ticket)
