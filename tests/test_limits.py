#! /usr/bin/env py.test


def test_parallel_queue_declare(client, makeid, msg):
    qname = makeid()

    queues = [qname + '.%s' % (i,) for i in xrange(100)]
    promises = [client.queue_declare(queue=q) for q in queues]

    for promise in promises:
        client.wait(promise)

    promises = [client.queue_delete(queue=q) for q in queues]
    for promise in promises:
        client.wait(promise)
