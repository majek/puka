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
        if callback:
            del kwargs['callback']
        t = method(*args, **kwargs)
        t.callback = callback
        return t.number
    return wrapper


class Client(connection.Connection):
    __metaclass__ = meta_attach_methods
    attach_methods = (machine_decorator, [
        machine.queue_declare,
        machine.basic_publish,
        machine.basic_consume,
        ])

    @machine_decorator
    def connect(self):
        return self._connect()

    def basic_ack(self, msg_result):
        machine.basic_ack(self, msg_result)
