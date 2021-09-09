#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 05 Mar 2012
# author: "Guo Peng Li" <liguopeng@foxmail.com>


import logging
import math

from twisted.internet import reactor

logger = logging.getLogger('connpool')


class MessageBase:
    _Prefix = 'Msg-ID'

    def __init__(self, msg_id=''):
        self.msg_id = msg_id
        self.failures = 0

    def __str__(self):
        return '<%s: %s>' % (self._Prefix, self.msg_id)


class MessageQueue:
    """ Manage connection pool to a server and all requests to it."""
    DropOld = 0
    RejectNew = 1

    def __init__(self, name, max_requests, policy_on_full):
        self._name = name
        self._requests = []

        self._max_requests = max_requests
        self._policy = policy_on_full

        # A queue's consumer is an object which can take request from the
        # queue and process the request.
        self._consumer = None
        self._consumer_idle = True

    def set_queue_consumer(self, consumer):
        """ Define who's gonna consume products """
        self._consumer = consumer
        consumer.set_queue_producer(self)

    def enqueue(self, req):
        """ Incoming new request """
        if len(self._requests) > self._max_requests:
            # too much pending requests...
            if self._policy == self.DropOld:
                old_req = self.dequeue()
                logger.error("Queue is full, drop the oldest request: %s" % str(old_req))
            else:
                # Queue full, reject new request
                logger.error("Queue is full, reject current request: %s" % str(req))
                return False

        logger.info("insert req into server queue")

        if type(req) is list:
            self._requests.extend(req)
        else:
            self._requests.append(req)

        self.check_idle_and_consume()

    def _start_consume(self):
        """ Schedule consuming after new requests coming """
        self._consumer.consume()
        self._consumer_idle = True

    def fetch_head(self, count):
        """ Fetch a bunch of requests from the head of list """
        if len(self._requests) <= count:
            products = self._requests * 1
            self._requests = []
        else:
            products = self._requests[:count]
            self._requests = self._requests[count:]

        return products

    def insert_front(self, reqs):
        """ Some requests should placed in the front of the queue, i.e. retry requests """
        self._requests[0:0] = reqs

        self.check_idle_and_consume()

    def check_idle_and_consume(self):
        """ Find an idle consumer and consume the products """
        if self._consumer and self._consumer_idle:
            self._consumer_idle = False
            reactor.callLater(0, self._start_consume)

    def dequeue(self):
        """ Dequeue as the name says """
        if self._requests:
            return self._requests.pop(0)

        return None

    def size(self):
        """ Get queue size """
        return len(self._requests)

    def close(self):
        """ Not implemented """
        pass


class ConnectionPool:
    """Connection poll to a singe server.

    Connection pool consumes "requests" from a outgoing message queue (all
    requests in this queue must be targeted for the same server).
    """

    def __init__(self, client_factory, context_factory, config):

        self.server_name = config.get('postman.ios_push_server.name')
        self.server_port = config.get_int('postman.ios_push_server.port')

        self._idle_connections = []
        self._connections = []
        self._queue_producer = None

        # The number of connections which is still not connected to server (in
        # connecting status).
        self._new_connections = 0
        self._max_connections = config.get_int('postman.connection.max_connections')
        self._max_request_in_one_fetch = config.get_int('postman.connection.max_request_in_one_fetch')
        self._connection_retry_timeout = config.get_int('postman.connection.retry_timeout')

        self._max_failed_connections = 20
        self._count_failed_connections = 0
        self._block_times = 0
        self._block_new_connections = False

        self.Client_Factory = client_factory(self, self._connection_retry_timeout)
        self.SSL_Context = context_factory()

    def set_queue_producer(self, producer):
        self._queue_producer = producer
        self.Client_Factory._queue = producer

    def consume(self):
        logger.debug("start consume requests from server queue [%s]" % self.server_name)
        self._create_connections()
        self._do_consume()

    def _do_consume(self):
        """Try get a new request and an idle connection to process the request."""

        logger.info("try sending more requests. requests in queue: %d" % self._queue_producer.size())

        while self._idle_connections and self._queue_producer.size():
            reqs = self._queue_producer.fetch_head(self._max_request_in_one_fetch)
            conn = self._idle_connections.pop()

            conn.process(reqs)

    def _reconnect(self):
        self._block_new_connections = False
        self._count_failed_connections = 0

        self._create_connections()

    def _create_connections(self):
        """Check if more connections are required and create them."""

        if self._block_new_connections:
            return
        elif self._max_failed_connections < self._count_failed_connections:
            self._block_times += 1
            self._block_new_connections = True

            if self._block_times > 20:
                logger.monitor.critical('cannot connect to server: %s, blocked times: %d' %
                                        (self.server_name, self._block_times))

            wait_time = pow(1.1, self._block_times)
            wait_time = min(wait_time, 24 * 3600)
            reactor.callLater(wait_time, self._reconnect)
            return

        queue_size = self._queue_producer.size()

        logger.debug("check if more connection needed: messages: %d, connections: %d, "
                     "idle connections: %d, incoming new connections: %d"
                     % (queue_size, len(self._connections), len(self._idle_connections), self._new_connections))

        # n_conn indicates how many established and building connections the pool have
        n_conn = len(self._connections) + self._new_connections

        # n_max indicates the max number of connections the pool SHOULD accept
        # SHOULD indicates following rules(by priority):
        # 0. Connections mustn't exceed max allowed connection count
        # 1. Connections should be able to process the requests
        # Thus, (queue_size/self._max_request_in_one_fetch) connections will just capable to process ALL requests
        # But it can NEVER exceed self._max_connections
        # math.ceil ensures any pending requests could be processed
        n_max = min(int(math.ceil(float(queue_size)/self._max_request_in_one_fetch)) + n_conn, self._max_connections)

        # n_new indicates how many new connections will be made in this estimation
        n_new = max(n_max - n_conn, 0)

        if n_new + n_conn == 0 and queue_size > 0:
            # If there WILL be NO connection in the pool
            n_new = 1

        logger.debug("will create %d new connections" % n_new)

        for i in range(n_new):
            reactor.connectSSL(self.server_name, self.server_port, self.Client_Factory, self.SSL_Context)
            self._new_connections += 1

    def on_connection_lost(self, protocol):
        """A connection in the pool is disconnected somehow."""

        logger.info("connection to server disconnected: %s" % str(protocol))

        # Remove any references of the connection in the pool
        self._connections.remove(protocol)

        if protocol in self._idle_connections:
            self._idle_connections.remove(protocol)
        else:
            protocol.set_idle()

        self._create_connections()

    def new_connection_failed(self):
        """Failed to created a new connection to server."""
        self._count_failed_connections += 1

        logger.info('failed to create a new connection to %s, pending connections: %d.'
                         % (self.server_name, self._new_connections - 1) )

        self._new_connections -= 1
        self._create_connections()

    def new_connection_made(self, protocol):
        """A new connection is just connected to the server."""
        self._count_failed_connections = 0
        self._block_times = 0
        self._block_new_connections = False

        self._new_connections -= 1

        self._idle_connections.append(protocol)
        self._connections.append(protocol)

        self._do_consume()

    def on_request_processed(self, conn):
        """A request is just processed by conn, return it to connection pool."""

        logger.info('request has been processed! %s on %s' % (str(conn.current_request()), str(conn)))

        conn.set_idle()
        self._idle_connections.append(conn)
        self._do_consume()

    def on_retry_timeout(self, conn):
        self.on_request_processed(conn)

    def close(self):
        pass


# Test Codes
if __name__ == "__main__":
    print 'Done'
