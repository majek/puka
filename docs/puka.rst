:mod:`puka` --- An opinionated RabbitMQ client
==============================================

.. module:: puka
   :synopsis: An opinionated RabbitMQ client.

The :mod:`puka` module implements a client for AMQP 0-9-1
protocol. It's tuned to work with RabbitMQ broker, but should work
fine with other message brokers that support this protocol.

AMQP model defines:

::

  Exchange ---binding---> Queue


The :mod:`puka` module defines one class:

.. class:: Client(amqp_url='amqp:///')

   Constructor of ...


Exceptions
----------

The :mod:`puka` module defines the following exceptions, as defined by
AMQP protocol.

.. exception:: NoRoute
.. exception:: NoConsumers
.. exception:: AccessRefused
.. exception:: NotFound
.. exception:: ResourceLocked
.. exception:: PreconditionFailed

Connection errors:

.. exception:: ConnectionBroken


Client Objects
--------------

Client object (:class:`Client`) provides the public methiods described
below.


Connection interface
++++++++++++++++++++

.. method:: Client.fileno()
.. method:: Client.socket()

.. method:: Client.on_read()
.. method:: Client.on_write()
.. method:: Client.needs_write()
.. method:: Client.run_any_callbacks()

.. method:: Client.wait(ticket)

.. method:: Client.loop()
.. method:: Client.loop_break()


Ticket interface
++++++++++++++++

Functions below return a 'ticket'.

Methods for connection establishment:

.. method:: Client.connect()

Establishes an asynchronous connection with the server. You're
forbidden to do any other action before this step has finished.

.. method:: Client.close()

Immediately closes the connection. All buffered data will be lost. All
tickets will be closed with an error.


AMQP methods used to manage exchanges:

.. method:: Client.exchange_declare(exchange, type='direct', arguments={})
.. method:: Client.exchange_delete(exchange, if_unused=False)
.. method:: Client.exchange_bind(destination, source, routing_key="", arguments={})
http://www.rabbitmq.com/extensions.html#exchange-bindings

.. method:: Client.exchange_unbind(destination, source, routing_key="", arguments={})

AMQP methods used to manage queues:

.. method:: Client.queue_declare(queue="", auto_delete=False, exclusive=False, arguments={})
.. method:: Client.queue_delete(queue, if_unused=False, if_empty=False)
.. method:: Client.queue_purge(queue)
.. method:: Client.queue_bind(queue, exchange, routing_key="", arguments={})
.. method:: Client.queue_unbind(queue, exchange, routing_key="", arguments={})


AMQP basic methods:

.. method:: Client.basic_publish(exchange, routing_key, mandatory=False, immediate=False, headers={}, body="")

.. method:: Client.basic_get(queue, no_ack=False)

.. method:: Client.basic_consume(queue, prefetch_size=0, prefetch_count=0, no_local=False, no_ack=False, exclusive=False, arguments={})

.. method:: Client.basic_consume_multi(queues, prefetch_size=0, prefetch_count=0, no_ack=False)

.. method:: Client.basic_qos(consume_ticket, prefetch_size=0, prefetch_count=0)
.. method:: Client.basic_cancel(consume_ticket)

.. method:: Client.basic_ack(msg_result)
.. method:: Client.basic_reject(msg_result)


Basic Examples
--------------

