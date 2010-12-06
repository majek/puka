#!/usr/bin/env python

import sys
sys.path.append("..")

import logging
FORMAT_CONS = '%(asctime)s %(name)-12s %(levelname)8s\t%(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT_CONS)



import puka

client = puka.Client("amqp://localhost/")

promise = client.connect()
client.wait(promise)

promises = [client.queue_declare(queue='a%04i' % i) for i in range(1000)]
for promise in promises:
    client.wait(promise)

promises = [client.queue_delete(queue='a%04i' % i) for i in range(1000)]
for promise in promises:
    client.wait(promise)

promise = client.close()
client.wait(promise)
