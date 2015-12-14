#!/usr/bin/python
import time
import select
import socket


class Proxy:

    buffer_size = 4096 * 2
    delay = 0.0001

    def __init__(self, host_port, to_host_port):
        self.forward_to = to_host_port
        self.input_list = []
        self.channel = {}

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(host_port)
        self.server.listen(200)

    def main_loop(self):
        self.input_list.append(self.server)

        while time.sleep(self.delay) or True:
            for self.s in select.select(self.input_list, [], [])[0]:
                if self.s == self.server:
                    self.on_accept()
                    break

                self.data = self.s.recv(self.buffer_size)
                if len(self.data) == 0:
                    self.on_close()
                    break
                else:
                    self.on_recv()

    def on_accept(self):
        forward = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientsock, clientaddr = self.server.accept()
        try:
            forward.connect(self.forward_to)

            self.input_list.append(clientsock)
            self.input_list.append(forward)
            self.channel[clientsock] = forward
            self.channel[forward] = clientsock
        except Exception:
            clientsock.close()

    def on_close(self):
        out = self.channel[self.s]

        self.input_list.remove(self.s)
        self.input_list.remove(self.channel[self.s])

        self.channel[out].close()
        self.channel[self.s].close()

        del self.channel[out]
        del self.channel[self.s]

    def on_recv(self):
        if self.allow_data():
            print repr(self.data)
            self.channel[self.s].send(self.data)

    def allow_data(self):
        return not ("admin.$cmd" in self.data and "shutdown" in self.data)


if __name__ == '__main__':
    import os, sys

    if len(sys.argv) < 3 or ":" not in sys.argv[2]:
        program = os.path.basename(sys.argv[0])
        print "usage: {} <port> <to_host>:<to_port>".format(program)
        sys.exit(1)

    try:
        to_host, to_port = sys.argv[2].split(":")
        Proxy(('', int(sys.argv[1])), (to_host, int(to_port))).main_loop()
    except KeyboardInterrupt:
        sys.exit(1)
