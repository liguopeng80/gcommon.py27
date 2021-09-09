#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-05-19

"""对失败的操作进行重试

TODO: 为 RPC 调用增加超时检测 - 当 rabbitmq 或者 rpc server 出现问题时，客户端不能长久等待。
"""

import time
import logging

from twisted.internet.defer import inlineCallbacks, maybeDeferred, returnValue
from gcommon.rpc import RpcServerException
from gcommon.utils.counters import Sequence
from gcommon.utils.timer import AsyncTimer

logger = logging.getLogger('rpc')


def patch_thrift_client(retrying, client):
    """为 thrift client 的类成员函数（RPC 接口函数）增加 retry 功能。

    client -> Thrift Client class (not object).
    """
    client_interface = client.__implemented__.declared[0]

    for name in client_interface._InterfaceClass_attrs:
        value = getattr(client, name, None)
        if callable(value):
            new_value = retrying.rpc_retry(value)
            setattr(client, name, new_value)


class TwistedRetrying(object):
    """重试异步操作"""
    RETRY_INTERVAL = 1.1
    STOP_MAX_RETRY_TIMES = 4
    STOP_MAX_DELAY = 10

    _sequence = Sequence()

    def __init__(self, identifier='', retry_interval=0, max_retry_times=0, max_delay=0):
        self.retry_interval = retry_interval or self.RETRY_INTERVAL
        self.max_retry_times = max_retry_times or self.STOP_MAX_RETRY_TIMES
        self.max_delay = max_delay or self.STOP_MAX_DELAY

        self._id = self._sequence.next_value()
        if identifier:
            self._id = "%06d.%s" % (self._id, identifier)
        else:
            self._id = "%06d" % self._id

    def rpc_retry(self, func):
        """Decorator"""
        @inlineCallbacks
        def __wrap(client_obj, *args, **kwargs):
            result = yield self.call(client_obj, func, *args, **kwargs)
            returnValue(result)

        __wrap.__name__ = func.__name__
        return __wrap

    @inlineCallbacks
    def call(self, client_obj, func, *args, **kwargs):
        """带有重试功能的 RPC 调用

        client_obj -> RPC client 实例
        func -> 未绑定的 RPC client 成员函数
        """
        member_func = func.__get__(client_obj)
        result = yield self.call_member_func(member_func, *args, **kwargs)
        returnValue(result)

    @inlineCallbacks
    def call_member_func(self, func, *args, **kwargs):
        """带有重试功能的 RPC 调用"""
        start_time = int(round(time.time() * 1000))
        attempt_number = 0

        while True:
            attempt_number += 1
            try:
                logger.debug('[%s] try rpc request: %s, %s, args: %s', attempt_number, self._id, func.__name__, args)
                result = yield maybeDeferred(func, *args, **kwargs)
            except RpcServerException, e:
                logger.warn('[%s] server error on rpc request: %s, error: %s', self._id, func.__name__, e)
                yield AsyncTimer.start(self.retry_interval)

                # 判断是否还可以继续重试
                now_time = int(round(time.time() * 1000))
                if (now_time - start_time > self.max_delay) or (attempt_number > self.max_retry_times):
                    raise
                else:
                    continue

            except Exception, e:
                logger.warn('[%s] unexpected error on rpc request: %s, error: %s', self._id, func.__name__, e)
                raise

            else:
                logger.debug('[%s] rpc request finished with result: %s', self._id, func.__name__, result)
                returnValue(result)


