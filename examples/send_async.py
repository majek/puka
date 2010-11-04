#!/usr/bin/env python

import sys
sys.path.append("..")

import logging
FORMAT_CONS = '%(asctime)s %(name)-12s %(levelname)8s\t%(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT_CONS)



import puka

def on_connection(ticket, result, user_data):
    client.queue_declare(queue='test', callback=on_queue_declare)

def on_queue_declare(ticket, result, user_data):
    client.basic_publish(exchange='', routing_key='test',
                         body="Hello world!",
                         callback=on_basic_publish)

def on_basic_publish(ticket, result, user_data):
    print " [*] Message sent"
    sys.exit(0)

client = puka.Client("amqp://localhost/")
client.connect(callback=on_connection)
client.wait_for_any()
