#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-02-26

"""
Run server: python test_rpc.py

Run client: python test_rpc.py client
"""

import sys

from zope.interface import implements

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor

from gcommon.rpc import RpcManager, RpcClientDecorator

from gen_py.demo import ttypes
from gen_py.demo import Doorman

rabbit_server = '192.168.1.11'
rabbit_port = 5672

rabbit_vhost= '/'
rabbit_user = 'guest'
rabbit_pass = 'guest'
rabbit_spec = './amqp0-8.stripped.rabbitmq.xml'


SERVER_EXCHANGE = 'rpc-service'
CLIENT_EXCHANGE = 'rpc-response'

SERVER_QUEUE = 'server.001'
SERVER_KEY = 't'


class DoormanHandler(object):
    implements(Doorman.Iface)

    def hello(self, username):
        return "Hello, %s" % username

    def helloEx(self, username):
        return "Hello, %s %s" % (username.lastName, username.firstName)


@inlineCallbacks
def start_server():
    rpc = RpcManager()

    yield rpc.connect_to_broker(rabbit_server, rabbit_port, rabbit_vhost,
                                rabbit_user, rabbit_pass, rabbit_spec)

    server = yield rpc.create_rpc_server(SERVER_EXCHANGE, SERVER_QUEUE)
    yield rpc.bind_new_key(server, SERVER_KEY)

    handler = DoormanHandler()
    yield rpc.start_server(server, Doorman.Processor, handler, CLIENT_EXCHANGE)


@inlineCallbacks
def start_client():
    class MyDecorator(RpcClientDecorator):
        def calc_routing_key(self, func_name, *args):
            return 't'

    rpc = RpcManager(MyDecorator)
    yield rpc.connect_to_broker(rabbit_server, rabbit_port, rabbit_vhost,
                                rabbit_user, rabbit_pass, rabbit_spec)

    client = yield rpc.create_rpc_client(Doorman.Client, 'default-routing-key',
                                         SERVER_EXCHANGE, CLIENT_EXCHANGE)

    result = yield client.hello('RPC')
    print result

    username = ttypes.Username('Guo Peng', 'Li')
    result = yield client.helloEx(username)
    print result

    reactor.stop()

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == 'client':
        reactor.callLater(0, start_client)
    else:
        reactor.callLater(0, start_server)

    reactor.run()
