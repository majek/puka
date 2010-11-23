:mod:`puka` --- An opinionated RabbitMQ client
==============================================

.. module:: puka
   :synopsis: An opinionated RabbitMQ client.

The :mod:`puka` module implements a client for AMQP 0-9-1
protocol. It's tuned to work with RabbitMQ broker, but should work
fine with other message brokers that support this protocol.

AMQP defines:

::

  Exchange ---binding---> Queue


The :mod:`puka` module defines one class:

.. class:: Client(amqp_url='amqp:///')

   Constructor of ...


Exceptions
----------

The :mod:`puka` module defines the following exceptions, as defined by
AMQP protocol.

.. exception:: ContentTooLarge
.. exception:: NoRoute
.. exception:: NoConsumers
.. exception:: AccessRefused
.. exception:: NotFound
.. exception:: ResourceLocked
.. exception:: PreconditionFailed
.. exception:: ConnectionForced
.. exception:: InvalidPath
.. exception:: FrameError
.. exception:: AMQPSyntaxError
.. exception:: CommandInvalid
.. exception:: ChannelError
.. exception:: UnexpectedFrame
.. exception:: ResourceError
.. exception:: NotAllowed
.. exception:: AMQPNotImplemented
.. exception:: InternalError


Client Objects
--------------

Client object (:class:`Client`) provides the public methiods described below.


Connection interface
++++++++++++++++++++

.. method:: Client.connect()
.. method:: Client.close()
.. method:: Client.fileno()
.. method:: Client.socket()
.. method:: Client.on_read()
.. method:: Client.on_write()
.. method:: Client.needs_write()
.. method:: Client.wait()
.. method:: Client.run_any_callbacks()
.. method:: Client.loop()
.. method:: Client.loop_break()


AMQP interface
++++++++++++++


.. method:: Client.exchange_declare()
.. method:: Client.exchange_delete()
.. method:: Client.exchange_bind()
.. method:: Client.exchange_unbind()
.. method:: Client.queue_declare()
.. method:: Client.queue_purge()
.. method:: Client.queue_delete()
.. method:: Client.queue_bind()
.. method:: Client.queue_unbind()
.. method:: Client.basic_publish()
.. method:: Client.basic_get()

.. method:: Client.basic_consume()
.. method:: Client.basic_qos()
.. method:: Client.basic_ack()
.. method:: Client.basic_reject()
.. method:: Client.basic_cancel()


Basic Examples
--------------

