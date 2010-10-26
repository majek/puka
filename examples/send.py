#!/usr/bin/env python

import sys
sys.path.append("..")

import puka


client = puka.Puka("amqp://localhost/")
ticket = client.connect()
client.wait(ticket)

ticket = client.queue_declare(queue='test')
client.wait(ticket)

ticket = client.basic_publish(exchange='', routing_key='test',
                              body="Hello world!")
client.wait(ticket)

print " [*] Message sent"

