#!/usr/bin/env python
"""Proxy an MJPEG stream (perhaps other things) to a number of clients
intelligently, disconnecting when there's nobody watching.

Copyright 2010 Russ Garrett
You may use this work without restrictions, as long as this notice is included.
The work is provided "as is" without warranty of any kind, neither express nor implied.
"""

import eventlet, sys, logging
from eventlet.green import socket
from eventlet.semaphore import Semaphore

logging.basicConfig(level=logging.INFO)

class MJPEGProxy:
    def __init__(self, listen_address, connect_address):
        self.log = logging.getLogger('MJPEGProxy')
        self.connect_address = connect_address
        self.listen_address = listen_address
        self.clients = []
        self.connection = None
        self.header = None
        self.sem = Semaphore(1)

    def run(self):
        self.log.info("Starting")
        eventlet.spawn_n(self.proxy)
        self.listen()

    def listen(self):
        server = eventlet.listen(self.listen_address)
        while True:
            connection, address = server.accept()
            self.add_client(connection, address)

    def proxy(self):
        while True:
            eventlet.sleep(0) # sem.release(); sem.acquire() doesn't yield?!
            self.sem.acquire()
            if len(self.clients) == 0:
                if self.connection:
                    self.disconnect()

                self.sem.release()
                eventlet.sleep(0.1)
                continue

            self.sem.release()

            data = self.connection.recv(1024)

            if (len(data) == 0):
                self.log.info("No data recieved from source, forcing reconnect.");
                self.disconnect()
                data = self.connect()

            for client in self.clients:
                try:
                    client.send(data)
                except socket.error, err:
                    self.clients.remove(client)
                    self.log.info("Client %s disconnected: %s [clients: %s]", client, err, len(self.clients))

    def add_client(self, client, address):
        self.sem.acquire()
        try:
            data = ''
            if self.connection is None:
                data = self.connect()
            client.send(self.header + "\r\n\r\n" + data)
            self.clients.append(client)
            self.log.info("Client %s connected [clients: %s]", address, len(self.clients))
        except:
            self.log.info("Failed to connect client %s [clients: %s]", address, len(self.clients))
            client.close()
        finally:
            self.sem.release()

    def disconnect(self):
        self.log.info("Disconnecting from source")
        self.connection.close()
        self.connection = None
        self.header = None

    def connect(self):
        self.log.info("Connecting to source %s", self.connect_address)
        data = ''

        try:
            self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connection.connect(self.connect_address)
            data = self.connection.recv(1024)
            header, data = data.split("\r\n\r\n")[0:2]
            self.header = header
            self.log.info("Connection successful, header recieved")

        except:
            self.log.info("Failed connecting to source %s", self.connect_address)

            for client in self.clients:
                self.log.info("Client %s disconnected due to source failure", client)
                client.close()
                self.clients.remove(client)

        return data

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Usage: mjpegproxy.py listen_host:listen_port connect_host:connect_port"
        sys.exit(1)
    listen = sys.argv[1].split(':')
    connect = sys.argv[2].split(':')
    p = MJPEGProxy((listen[0], int(listen[1])), (connect[0], int(connect[1])))
    p.run()
