from __future__ import absolute_import
from future.utils import with_metaclass

import functools

from . import connection
from . import machine

class meta_attach_methods(type):
    def __new__(cls, name, bases, nmspc):
        decorator, list_of_methods = nmspc['attach_methods']
        for method in list_of_methods:
            nmspc[method.__name__] = decorator(method)
        return super(meta_attach_methods, cls).__new__(cls, name, bases, nmspc)


def machine_decorator(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        callback = kwargs.get('callback')
        if callback is not None:
            del kwargs['callback']
        p = method(*args, **kwargs)
        p.user_callback = callback
        p.after_machine()
        return p.number
    return wrapper


class Client(with_metaclass(meta_attach_methods, connection.Connection)):
    attach_methods = (machine_decorator, [
        machine.queue_declare,
        machine.queue_purge,
        machine.queue_delete,
        machine.basic_publish,
        machine.basic_consume,
        machine.basic_consume_multi,
        machine.basic_cancel,
        machine.basic_qos,
        machine.basic_get,
        machine.exchange_declare,
        machine.exchange_delete,
        machine.exchange_bind,
        machine.exchange_unbind,
        machine.queue_bind,
        machine.queue_unbind,
        ])

    @machine_decorator
    def connect(self):
        return self._connect()

    @machine_decorator
    def close(self):
        return self._close()

    def basic_ack(self, *args, **kwargs):
        machine.basic_ack(self, *args, **kwargs)

    def basic_reject(self, *args, **kwargs):
        machine.basic_reject(self, *args, **kwargs)
