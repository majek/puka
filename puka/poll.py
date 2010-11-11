import select

def loop(clients):
    while True:
        for c in clients:
            c.run_any_callbacks()

        rfds = clients
        wfds = [c for c in clients if c.needs_write()]
        r, w, e = select.select(rfds, wfds, rfds)

        for c in r:
            c.on_read()
        for c in w:
            c.on_write()
