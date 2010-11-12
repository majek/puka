#!/usr/bin/env python

import sys
sys.path.append("..")


import puka

def on_connection(ticket, result):
    client.queue_declare(queue='test', callback=on_queue_declare)

def on_queue_declare(ticket, result):
    client.basic_publish(exchange='', routing_key='test',
                         body="Hello world!",
                         callback=on_basic_publish)

def on_basic_publish(ticket, result):
    print " [*] Message sent"
    client.loop_break()

client = puka.Client("amqp://localhost/")
client.connect(callback=on_connection)
client.loop()

ticket = client.close()
client.wait(ticket)
