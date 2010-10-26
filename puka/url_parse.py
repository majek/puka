from urlparse import unquote, urlsplit

def parse_amqp_url(amqp_url):
    '''
    >>> parse_amqp_url('amqp:///')
    ('guest', 'guest', '/', 'localhost', 5672)
    >>> parse_amqp_url('amqp://a:b@c:1/d')
    ('a', 'b', '/d', 'c', 1)
    >>> parse_amqp_url('amqp://g%20uest:g%20uest@host/vho%20st')
    ('g uest', 'g uest', '/vho st', 'host', 5672)
    >>> parse_amqp_url('http://asd')
    Traceback (most recent call last):
      ...
    AssertionError: Only amqp:// protocol supported.
    '''
    assert amqp_url.startswith('amqp://'), "Only amqp:// protocol supported."
    # urlsplit doesn't know how to parse query when scheme is amqp,
    # we need to pretend we're http'
    o = urlsplit('http://' + amqp_url[len('amqp://'):])
    username = unquote(o.username) if o.username else 'guest'
    password = unquote(o.password) if o.password else 'guest'

    vhost = unquote(o.path) if o.path else '/'
    host = o.hostname or 'localhost'
    port = o.port if o.port else 5672
    return (username, password, vhost, host, port)



