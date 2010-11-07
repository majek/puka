from . import connection
from . import machine

def meta_attach_methods(name, bases, cls):
    decorator, list_of_methods = cls['attach_methods']
    for method in list_of_methods:
        cls[method.__name__] = decorator(method)
    return type(name, bases, cls)


def machine_decorator(method):
    def wrapper(*args, **kwargs):
        callback = kwargs.get('callback')
        if callback is not None:
            del kwargs['callback']
        user_data = kwargs.get('user_data')
        if user_data is not None:
            del kwargs['user_data']
        t = method(*args, **kwargs)
        t.user_callback = callback
        t.user_data = user_data
        t.after_machine()
        return t.number
    return wrapper


class Client(connection.Connection):
    __metaclass__ = meta_attach_methods
    attach_methods = (machine_decorator, [
        machine.queue_declare,
        machine.queue_purge,
        machine.queue_delete,
        machine.basic_publish,
        machine.basic_consume,
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

    def basic_cancel(self, *args, **kwargs):
        machine.basic_cancel(self, *args, **kwargs)
