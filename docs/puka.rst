:mod:`puka` --- An opinionated RabbitMQ client
==============================================

.. module:: puka
   :synopsis: An opinionated RabbitMQ client.

The :mod:`puka` module implements a client for AMQP 0-9-1
protocol. It's tuned to work with RabbitMQ broker, but should work
fine with other message brokers that support this protocol.

AMQP protocol defines a model which consists of Exchanges, Bindings and Queues:

::

  Exchange ---binding---> Queue

For more information see:

 - `RabbitMQ tutorials <http://www.rabbitmq.com/getstarted.html>`_.
 - AMQP 0-9-1 `Specification <http://www.amqp.org/confluence/download/attachments/720900/amqp0-9-1.pdf?version=1&modificationDate=1227526523000>`_ and `Definitions <http://www.amqp.org/confluence/download/attachments/720900/amqp-xml-doc0-9-1.pdf?version=1&modificationDate=1227526523000>`_


The :mod:`puka` module defines a single class:

.. class:: Client(amqp_url='amqp:///', pubacks=None, client_properties=None)

   Consturctor for :class:`Client` class. `amqp_url` is an url-like
   address of the RabbitMQ server. Default points to
   `amqp://guest:guest@localhost:5672/`. `pubacks` tells if the client
   should take advantage of the publiser-acks feature on the server -
   autodetect by default.  `client_properties` is a dict of properties
   to add to the connection properties sent to the server.

Exceptions
----------

The :mod:`puka` module exposes the following exceptions, as defined by
AMQP protocol.

.. exception:: NoRoute
.. exception:: NoConsumers
.. exception:: AccessRefused
.. exception:: NotFound
.. exception:: ResourceLocked
.. exception:: PreconditionFailed

Additionally :mod:`puka` defines a connection error:

.. exception:: ConnectionBroken


Client Objects
--------------

Client object (:class:`Client`) provides the public methods described
below. They can be grouped in three distinct subcategories.

Connection interface
++++++++++++++++++++

The following methods are responsible for networking and socket
handling.

.. method:: Client.fileno()

   Return a socket's file descriptor (a small integer). This is useful
   with :func:`select.select`.

.. method:: Client.socket()

   Return a :func:`socket.socket` object.

.. method:: Client.on_read()

   Inform the :class:`Client` object that the socket is now in readable state.

.. method:: Client.on_write()

   Inform the :class:`Client` object that the socket is now in writable state.

.. method:: Client.needs_write()

   Return `true` if the send buffer is full, and needs to be written to
   the socket.

.. method:: Client.run_any_callbacks()

   Run any outstanding user callbacks for any of the `promises`.

.. method:: Client.wait(promise, timeout=None, raise_errors=True)

   Wait up to `timeout` seconds for an event on given `promise`. If the event
   was received before `timeout`, run the callback for the `promise` and
   return AMQP `response`.

.. method:: Client.loop(timeout=None)

   Enter an event loop, keep on handling network and executing user
   callbacks for up to `timeout` seconds.

.. method:: Client.loop_break()

   Cause the event loop to break on next iteration.


Promise interface
+++++++++++++++++

Functions below return a `promise`. It's a small number, that identifies
an asynchronous request. You can wait for a `promise` to be done and
receive a `response` for it.

Connection handling methods:
,,,,,,,,,,,,,,,,,,,,,,,,,,,,

.. method:: Client.connect()

   Establishes an asynchronous connection with the server. You're
   forbidden to do any other action before this step is finished.

.. method:: Client.close()

   Immediately closes the connection. All buffered data will be lost. All
   outstanding promises will be closed with an error.


AMQP methods used to manage exchanges:
,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,

.. method:: Client.exchange_declare(exchange, type='direct', durable=False, auto_delete=False, arguments={})
.. method:: Client.exchange_delete(exchange, if_unused=False)
.. method:: Client.exchange_bind(destination, source, routing_key="", arguments={})

   For details see the documentation of `exchange bindings <http://www.rabbitmq.com/extensions.html#exchange-bindings>`_ RabbitMQ feature.

.. method:: Client.exchange_unbind(destination, source, routing_key="", arguments={})


AMQP methods used to manage queues:
,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,

.. method:: Client.queue_declare(queue="", durable=False, exclusive=False, auto_delete=False, arguments={})
.. method:: Client.queue_delete(queue, if_unused=False, if_empty=False)
.. method:: Client.queue_purge(queue)
.. method:: Client.queue_bind(queue, exchange, routing_key="", arguments={})
.. method:: Client.queue_unbind(queue, exchange, routing_key="", arguments={})


AMQP methods used to handle messages:
,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,

.. method:: Client.basic_publish(exchange, routing_key, mandatory=False, immediate=False, headers={}, body="")

.. method:: Client.basic_get(queue, no_ack=False)

.. method:: Client.basic_consume(queue, prefetch_count=0, no_local=False, no_ack=False, exclusive=False, arguments={})

   Return a `consume_promise`.

.. method:: Client.basic_consume_multi(queues, prefetch_count=0, no_ack=False)

   Return a `consume_promise`.

.. method:: Client.basic_qos(consume_promise, prefetch_count=0)

   Given a `consume_promise` returned by :meth:`basic_consume` or
   :meth:`basic_consume_multi` changes the `prefetch_count` for that
   consumes.

.. method:: Client.basic_cancel(consume_promise)

   Given a `consume_promise` returned by :meth:`basic_consume` or
   :meth:`basic_consume_multi` cancels the consume. You can `wait` on
   the returned promise.

.. method:: Client.basic_ack(msg_result)

   Given a result of `basic_consume` or `basic_consume_multi` promise
   (ie: a message) acknowledges it. It's an asynchronous method.

.. method:: Client.basic_reject(msg_result)

   Given a result of `basic_consume` or `basic_consume_multi` promise
   (ie: a message) rejects it. It's an asynchronous method.


Basic Example
-------------

Synchronously send a message:

::

  import puka

  client = puka.Client("amqp://localhost/")
  promise = client.connect()
  client.wait(promise)

  promise = client.queue_declare(queue='test')
  client.wait(promise)

  promise = client.basic_publish(exchange='', routing_key='test',
                                body="Hello world!")
  client.wait(promise)

  promise = client.close()
  client.wait(promise)


Synchronously receive three messages:

::

  import puka

  client = puka.Client("amqp://localhost/")
  promise = client.connect()
  client.wait(promise)

  promise = client.queue_declare(queue='test')
  client.wait(promise)

  consume_promise = client.basic_consume(queue='test')
  for i in range(3):
      result = client.wait(consume_promise)
      print " [x] Received message %r" % (result,)

      client.basic_ack(result)

  promise = client.basic_cancel(consume_promise)
  client.wait(promise)

  promise = client.close()
  client.wait(promise)


Asynchronously send a message:

::

  import puka

  def on_connection(promise, result):
      client.queue_declare(queue='test', callback=on_queue_declare)

  def on_queue_declare(promise, result):
      client.basic_publish(exchange='', routing_key='test',
                           body="Hello world!",
                           callback=on_basic_publish)

  def on_basic_publish(promise, result):
      print " [*] Message sent"
      client.loop_break()

  client = puka.Client("amqp://localhost/")
  client.connect(callback=on_connection)
  client.loop()

  promise = client.close()
  client.wait(promise)
