Puka - the opinionated RabbitMQ client
======================================

Puka is yet-another Python client library for RabbitMQ. But as opposed
to similar libraries, it does not try to expose a generic AMQP
API. Instead, it takes an opinionated view on how the user should
interact with RabbitMQ.


Puka is simple
--------------

Puka exposes a simple, easy to understand API. Take a look at the
`publisher` example:

    import puka

    client = puka.Puka("amqp://localhost/")

    ticket = client.connect()
    client.wait(ticket)

    ticket = client.queue_declare(queue='test')
    client.wait(ticket)

    ticket = client.basic_publish(exchange='', routing_key='test',
                                  body='Hello world!')
    client.wait(ticket)


Puka is asynchronous
--------------------

Puka by all means is asynchronous. Although, as you can see in example
above, it can behave synchronously. That's especially useful for
simple tasks when you don't want to introduce complex callbacks.

Here's the same code written in an asynchronous way:

    import puka
    import sys

    def run():
        client = puka.Puka("amqp://localhost/")

        client.connect(callback=on_connection)

        def on_connection(result):
            client.queue_declare(queue='test', callback=on_queue_declare)

        def on_queue_declare(result):
            client.basic_publish(exchange='', routing_key='test',
                                 body="Hello world!",
                                 callback=on_basic_publish)

        def on_basic_publish(result):
            sys.exit(0)

        client.wait_forever()

    run()


Puka never blocks
-----------------

In the pure asynchronous programming style Puka never blocks your
program waiting for network. On the other hand it is your
responsibility to notify when new data is available on the network
socket. To allow that Puka allows you to access the raw socket
descriptor. With that in hand you can construct your own event
loop. Here's an the event loop that may replace `wait_forever` from
previous example:

     fd = client.fileno()
     while True:
        while client.handle_data():
            pass

        r, w, e = select.select([fd],[fd] if client.needs_write() else [], [fd])
        if r or e:
            client.on_read()
        if w:
            client.on_write()


Puka is fast
------------

Puka is asynchronous and has no trouble in handling many requests at a
time. This can be exploited to achieve a degree of parallelism. For
example, this snippet creates 1000 queues in parallel:

    tickets = [puka.queue_declare(queue='a%04i' % i) for i in range(1000)]
    puka.wait_for_many(tickets)


Puka is also created to be a flat library, with only a few necessary
indirection layers underneath. That does effect in a pretty low CPU
usage.


Puka is sane
------------

Puka does expose only a sane subset of AMQP, as judged by the
author. The major differences between Puka and raw AMQP are:

 - Channels and transactions aren't exposed at all.
 - Properties and headers are exposed as a one thing.
 - Deleting queues 'on broker restart' is not allowed. Queues can be
   only `auto_delete` or `persistent`.
 - Mystic 'delivery-mode' property is renamed to `persistent` and by default
   all outgoing messages are marked as `persistent`.
 - Everything is made to be synchronous, even 'basic_publish' and 'basic_ack'.
 - Heartbeats aren't supported. Use tcp keepalives or just do 'queue_declare'
   every n seconds.


Puka is experimental
--------------------

Puka is a side project, written mostly to prove if it is possible to
create a reasonable API on top of the AMQP protocol. It is not
supported by anyone and might be abandoned at any time.

