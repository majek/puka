#!/usr/bin/env python
import sys
sys.path.append("..")
import puka

client = puka.Client("amqp://localhost/")
promise = client.connect()
client.wait(promise)


promise = client.exchange_declare(exchange='headers_logs', type='headers')
client.wait(promise)

argv = sys.argv[1:-1] if len(sys.argv) > 2 else ['anonymous:info']
headers = dict(arg.split(':', 2) for arg in argv)

message = sys.argv[-1] if len(sys.argv) > 1 else 'Hello World!'
promise = client.basic_publish(exchange='headers_logs', routing_key='',
                               body=message,
                               headers=headers)
client.wait(promise)

print " [x] Sent %r %r" % (headers, message)
client.close()
