import os, random, puka, pytest, py, string


@pytest.fixture
def makeid(request):
    """create test specific names"""
    name = request._pyfuncitem.name
    name = py.std.re.sub("[\W]", "_", name)

    def makeid(s=""):
        return '%s_%s__%s' % (name, s, "".join([random.choice(string.lowercase) for x in range(6)]))
    return makeid


@pytest.fixture
def amqp_url():
    """the URL to a running rabbitmq instance"""
    return os.getenv('AMQP_URL', 'amqp:///')


@pytest.fixture
def client(request, amqp_url):
    """a connected puka.Client instance"""
    client = puka.Client(amqp_url)
    promise = client.connect()
    client.wait(promise)

    def cleanup():
        promise = client.close()
        client.wait(promise)

    request.addfinalizer(cleanup)

    return client


def _qname(request, client, name):
    promise = client.queue_declare(queue=name)
    client.wait(promise)

    def cleanup():
        promise = client.queue_delete(queue=name)
        client.wait(promise)

    request.addfinalizer(cleanup)

    return name


@pytest.fixture
def qname1(request, client, makeid):
    """create a new queue, return it's name"""
    return _qname(request, client, makeid("qname1"))


@pytest.fixture
def qname2(request, client, makeid):
    """create a new queue, return it's name"""
    return _qname(request, client, makeid("qname2"))


@pytest.fixture
def msg(request, makeid):
    """a message to sent"""
    return makeid("message")
