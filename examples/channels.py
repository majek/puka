#!/usr/bin/env python

import sys
sys.path.append("..")

import puka
import time

headers={'persistent': False}

AMQP_URL="amqp://localhost/"
CONNECTIONS=500

class C(object):
    def __init__(self, x):
        self.x = x
        self.client = puka.Client(AMQP_URL)
        self.client.connect(callback=self.on_connect)

    def on_connect(self, t, result):
        self.x.to_connect -= 1
        if self.x.to_connect == 0:
            self.x.on_connect()

    def spawn_channels(self, channels):
        self.channels = channels
        for i in range(self.channels):
            self.client.queue_declare("a", callback=self.on_queue_declare)

    def on_queue_declare(self, t, result):
        self.channels -= 1
        if self.channels == 0:
            print "channels created"

class X(object):
    def __init__(self):
        self.cs = []
        self.clients = []
        self.to_connect = CONNECTIONS
        for i in xrange(CONNECTIONS):
            c = C(self)
            self.cs.append( c )
            self.clients.append( c.client )

    def on_connect(self):
        print "connections established"
        for c in self.cs:
            c.spawn_channels(200)

def main():
    x = X()
    while True:
        puka.loop(x.clients)


if __name__ == '__main__':
    main()
