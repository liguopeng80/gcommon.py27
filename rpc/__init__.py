#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-02-26

from contextlib import contextmanager
from thrift.transport import TTransport

import txamqp.spec
from txamqp.client import TwistedDelegate
from txamqp.contrib.thrift.protocol import ThriftAMQClient
from txamqp.contrib.thrift.transport import TwistedAMQPTransport
from twisted.internet.defer import inlineCallbacks, TimeoutError
from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet.protocol import ClientCreator
from thrift.protocol.TBinaryProtocol import TBinaryProtocol, TBinaryProtocolFactory

from gcommon.cluster.cluster_manager import ClusterManager
from gcommon.utils.async import enable_timeout
from gcommon.utils.counters import Sequence
from slimproto.error_define.ttypes import slim_errors

import logging
logger = logging.getLogger('rpc')


class RpcException(RuntimeError):
    """未知 RPC 错误"""


class RpcServerException(RpcException):
    """RPC 服务器异常。"""
    error_code = slim_errors.gen_server_internal


class RpcServerSuspended(RpcServerException):
    """服务不可用"""
    error_code = slim_errors.rpc_server_suspended


class RpcClientBadRouting(RpcServerException):
    """客户端发送了错误的请求到服务器（应该发送至其它服务器）"""
    error_code = slim_errors.rpc_client_bad_routing


def thrift_monkey_patch():
    """对 Thrift 框架打补丁，将 unicode 字符串转换为 utf-8."""
    old_write_string = TBinaryProtocol.writeString

    def _new_write_string(self, s):
        if isinstance(s, unicode):
            s = s.encode('utf-8')

        return old_write_string(self, s)

    TBinaryProtocol.writeString = _new_write_string

    old_read_string = TBinaryProtocol.readString

    def _new_read_string(self):
        value = old_read_string(self)
        return value.decode('utf-8')

    TBinaryProtocol.readString = _new_read_string


# monkey patch
thrift_monkey_patch()


class RpcEndpoint(object):
    """RPC 通信的一个端点（服务器或者客户端）。"""
    def __init__(self, exchange_name, queue_name, channel):
        self.exchange_name = exchange_name
        self.queue_name = queue_name

        self.channel = channel


class RpcRequestDict(dict):
    TIMEOUT = 30

    def __setitem__(self, key, value):
        """请求对象都是 deferred 对象。为请求增加超时功能。"""
        assert isinstance(value, defer.Deferred)
        logger.debug('rpc-request start: rseqid = %s', key)

        enable_timeout(value, self.TIMEOUT)
        value.addBoth(self._log_result, key)
        value.addErrback(self.pop_timeout_deferred, key)

        dict.__setitem__(self, key, value)

    @staticmethod
    def _log_result(result, key):
        logger.debug('rpc-request finished: rseqid = %s, result type = %s', key, type(result))
        return result

    def pop_timeout_deferred(self, result, key):
        if isinstance(result.value, TimeoutError):
            self.pop(key)
        return result


class SlimAmqConnection(ThriftAMQClient):
    def __init__(self, *args, **kwargs):
        ThriftAMQClient.__init__(self, *args, **kwargs)
        self._channel_seq = Sequence()

    @inlineCallbacks
    def create_server_endpoint(self, server_exchange, service_queue, server_exchange_type='direct'):
        """为服务提供者 (RPC service provider) 创建 channel 和 endpoint 对象。"""
        channel_id = self._channel_seq.next_value()
        channel = yield self.channel(channel_id)

        yield channel.channel_open()
        yield channel.exchange_declare(exchange=server_exchange, type=server_exchange_type)
        yield channel.queue_declare(queue=service_queue, auto_delete=True)

        endpoint = RpcEndpoint(server_exchange, service_queue, channel)
        defer.returnValue(endpoint)

    @inlineCallbacks
    def start_server(self, endpoint, rpc_processor, service_handler, client_exchange):
        """服务器开始处理消息队列。"""
        processor = rpc_processor(service_handler)
        factory = TBinaryProtocolFactory()

        reply = yield endpoint.channel.basic_consume(queue=endpoint.queue_name)
        queue = yield self.queue(reply.consumer_tag)

        d = queue.get()
        d.addCallback(self.parseServerMessage, endpoint.channel, client_exchange,
                      queue, processor, iprot_factory=factory, oprot_factory=factory)
        d.addErrback(self.catchClosedServerQueue)
        d.addErrback(self.handleServerQueueError)

    @defer.inlineCallbacks
    def start_client(self, client_exchange, server_exchange, routing_key, client_class, client_queue=None,
                     client_exchange_type='direct', server_exchange_type='direct'):
        """为服务调用者 (RPC service consumer) 创建 channel 和 endpoint 对象。"""
        channel_id = self._channel_seq.next_value()
        channel = yield self.channel(channel_id)

        yield channel.channel_open()
        yield channel.exchange_declare(exchange=server_exchange, type=server_exchange_type)
        yield channel.exchange_declare(exchange=client_exchange, type=client_exchange_type)

        if client_queue is None:
            """使用随机名称 —— 通常客户端不需要预设的 queue name."""
            reply = yield channel.queue_declare(exclusive=True, auto_delete=True)
            client_queue = reply.queue

            yield channel.queue_bind(queue=client_queue, exchange=client_exchange, routing_key=client_queue)
        else:
            """Queue 的创建和 bindings 由该函数的调用者维护。本函数仅从 queue 中接收消息。"""

        endpoint = RpcEndpoint(client_exchange, client_queue, channel)
        # defer.returnValue(endpoint)

        """启动 RPC 客户端（启动接收响应的队列）。"""
        reply = yield endpoint.channel.basic_consume(queue=endpoint.queue_name)

        thrift_client_name = client_class.__name__ + routing_key

        amqp_transport = TwistedAMQPTransport(
            endpoint.channel, server_exchange, routing_key, clientName=thrift_client_name,
            replyTo=endpoint.queue_name, replyToField=self.replyToField)

        factory = TBinaryProtocolFactory()
        thrift_client = client_class(amqp_transport, factory)
        self._enableTimeoutOnRpcRequests(thrift_client)

        # RPC 响应队列，接收 RPC 服务器的响应消息
        queue = yield self.queue(reply.consumer_tag)
        self._receiveNextFrame(endpoint.channel, queue, thrift_client, factory)

        # 无法送达的请求将进入 basic return queue (ThriftTwistedDelegate).
        basic_return_queue = yield self.thriftBasicReturnQueue(thrift_client_name)
        self._receiveUnrouteableMessage(endpoint.channel, basic_return_queue, thrift_client, factory)

        defer.returnValue(thrift_client)

    def _receiveNextFrame(self, channel, queue, thrift_client, iprot_factory):
        """
        从队列中接收下一个消息并处理。
        """
        d = queue.get()
        d.addCallback(self.safeParseClientMessage, channel, queue, thrift_client, iprot_factory)
        d.addErrback(self.catchClosedClientQueue)
        d.addErrback(self.handleClientQueueError)

    def _receiveUnrouteableMessage(self, channel, queue, thrift_client, iprot_factory):
        """
        接收并处理消息路由错误（忽略）。
        """
        d = queue.get()
        d.addCallback(self.parseClientUnrouteableMessage, channel, queue, thrift_client, iprot_factory)
        d.addErrback(self.catchClosedClientQueue)
        d.addErrback(self.handleClientQueueError)

    def _enableTimeoutOnRpcRequests(self, thrift_client):
        """
        对 RPC client 对象增加超时处理功能。
        """
        thrift_client._reqs = RpcRequestDict()

    def safeParseClientMessage(self, msg, channel, queue, thrift_client, iprot_factory):
        """
        解析并处理收到的 RPC 响应。忽略不能识别的消息。
        """
        try:
            self._processClientMessage(iprot_factory, msg, thrift_client)
        except Exception, e:
            logger.critical("fatal RPC client error: %s, %s", type(e), str(e))
            pass

        # 从队列中删除该消息
        channel.basic_ack(msg.delivery_tag, True)

        # 开始接收下一个消息
        self._receiveNextFrame(channel, queue, thrift_client, iprot_factory)

    def _processClientMessage(self, iprot_factory, msg, thrift_client):
        """处理在 RPC 响应队列中收到的消息。可能抛出各种异常。"""
        tr = TTransport.TMemoryBuffer(msg.content.body)

        if iprot_factory is None:
            iprot = self.factory.iprot_factory.getProtocol(tr)
        else:
            iprot = iprot_factory.getProtocol(tr)

        (fname, mtype, rseqid) = iprot.readMessageBegin()

        if rseqid in thrift_client._reqs:
            logger.debug(
                'rpc-reply: fname = %r, rseqid = %s, mtype = %r, routing key = %r, client = %s',
                fname, rseqid, mtype, msg.routing_key, thrift_client.__implemented__.__name__
            )

            method = getattr(thrift_client, 'recv_' + fname)
            method(iprot, mtype, rseqid)
        else:
            # 可以输出 msg.content.body 打印完整的消息数据包
            logger.warning(
                'bad-rpc-reply: fname = %r, rseqid = %s, mtype = %r, routing key = %r, client = %s',
                fname, rseqid, mtype, msg.routing_key, thrift_client.__implemented__.__name__
            )

    def handleClientQueueError(self, failure):
        # todo: 处理 queue 错误
        pass

    def handleClosedClientQueue(self, failure):
        # todo: 处理连接错误
        pass


class RpcManager(object):
    _RPC_PROTOCOL = SlimAmqConnection

    def __init__(self, decorator=None):
        self._broker_conn = None

        decorator = decorator or RpcClientDecorator
        self._decorator = decorator()

    @inlineCallbacks
    def connect_to_broker(self, host, port, vhost, username, password, spec_file):
        """连接至 RabbitMQ"""
        spec = txamqp.spec.load(spec_file)

        delegate = TwistedDelegate()

        conn = yield ClientCreator(reactor, self._RPC_PROTOCOL, delegate, vhost, spec).connectTCP(host, port)
        yield conn.authenticate(username, password)

        self._broker_conn = conn

    @inlineCallbacks
    def create_rpc_server(self, server_exchange, server_queue=""):
        if not server_queue:
            server_queue = self.get_server_queue_name()

        endpoint = yield self._broker_conn.create_server_endpoint(server_exchange, server_queue)
        defer.returnValue(endpoint)

    @inlineCallbacks
    def start_server(self, endpoint, rpc_processor, service_handler, client_exchange):
        yield self._broker_conn.start_server(endpoint, rpc_processor, service_handler, client_exchange)

    @staticmethod
    def get_server_queue_name():
        return ''

    @inlineCallbacks
    def create_rpc_client(self, rpc_client_class, routing_key, server_exchange, client_exchange,
                          server_exchange_type='direct', client_exchange_type='direct'):
        """创建 RPC Client 对象（以及对应的队列等资源）"""
        # 修改 client 类
        self._decorator.decorate(rpc_client_class)

        # 创建 thrift RPC client 实例
        thrift_client = yield self._broker_conn.start_client(
            client_exchange, server_exchange, routing_key, rpc_client_class,
            client_exchange_type=client_exchange_type,
            server_exchange_type=server_exchange_type
        )

        defer.returnValue(thrift_client)

    @inlineCallbacks
    def bind_new_key(self, endpoint, key, exchange=''):
        """在 endpoint 的接收队列上绑定一个新的 key."""
        if not exchange:
            exchange = endpoint.exchange_name

        yield endpoint.channel.queue_bind(
            queue=endpoint.queue_name,
            exchange=exchange,
            routing_key=key
        )

    @inlineCallbacks
    def unbind_key(self, endpoint, key, exchange=''):
        """在 endpoint 的接收队列上取消一个 key 的绑定."""
        if not exchange:
            exchange = endpoint.exchange_name

        yield endpoint.channel.queue_unbind(
            queue=endpoint.queue_name,
            exchange=exchange,
            routing_key=key
        )


class RpcClientDecorator(object):
    """对 thrift 生成的 RPC Client 对象进行封装。

    用来在客户端调用时动态指定合适的 routing key。"""
    _DECORATED_MARK = "_slim_decorated"

    def decorate(self, rpc_client_class):
        """对 RPC Client 的 send_xxx 方法进行替换。"""
        if getattr(rpc_client_class, self._DECORATED_MARK, None):
            # has been decorated
            return

        interface_methods = rpc_client_class.__implemented__.declared[0]._InterfaceClass__attrs
        name_prefix = 'send_'

        for name, value in rpc_client_class.__dict__.iteritems():
            if name in interface_methods:
                # 用户自定义方法
                continue

            if name.startswith(name_prefix):
                if name[len(name_prefix):] in interface_methods:
                    # rpc_client_class['name'] = self.rpc_method_decorator(value)
                    setattr(rpc_client_class, name, self.rpc_method_decorator(value))

        setattr(rpc_client_class, self._DECORATED_MARK, True)

    def verify_routing_key(self, func_name, *args):
        """根据当前的服务集群状态，以及当前服务器自身的 UID，验证进入服务器的请求是否合法。"""
        correct_server = self.calc_routing_key(func_name, *args)

        return correct_server == ClusterManager.server.unique_server_name

    @contextmanager
    def routing_key_scope(self, client_instance, new_routing_key):
        """在调用函数前替换 routing key, 在调用后恢复旧 routing key."""
        old_key = client_instance._transport.routingKey
        try:
            client_instance._transport.routingKey = new_routing_key
            yield
        finally:
            client_instance._transport.routingKey = old_key

    def rpc_method_decorator(self, rpc_method):
        def __rpc_wrapper(client_obj, *args, **kws):
            new_key = self.calc_routing_key(rpc_method.__name__, *args)
            new_key = self.hash_routing_key(new_key)

            with self.routing_key_scope(client_obj, new_key) as _:
                return rpc_method(client_obj, *args, **kws)

        return __rpc_wrapper

    def calc_routing_key(self, func_name, *args):
        raise NotImplementedError('for sub class')

    @staticmethod
    def hash_routing_key(raw_key):
        """比如提供一致性哈希等算法对负载进行分配。"""
        return raw_key


