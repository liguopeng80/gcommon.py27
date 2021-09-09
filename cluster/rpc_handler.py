#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-05-21

import logging
import time
import traceback

from copy import deepcopy
from twisted.internet.defer import inlineCallbacks, returnValue, maybeDeferred, TimeoutError
from gcommon.app.slim_error_define import SlimErrorCodes

from gcommon.rpc import RpcServerSuspended, RpcClientBadRouting
from gcommon.utils import tm
from gcommon.utils.monitor import monitor
from gcommon.utils import security

from slimproto.error_define.ttypes import slim_errors
from slimproto.base.ttypes import InvalidOperation


class RpcServiceHandler(object):
    Routing_Key_Manager = None

    def __init__(self, app_server):
        self.server = app_server

        self.routing_key_manager = self.Routing_Key_Manager
        self.routing_key = app_server.get_routine_key()

        self.logger = logging.getLogger('rpc')

    def server_failover_enabled(self):
        return self.server.is_failover_enabled()

    def must_be_running(self):
        if not self.server.is_running():
            raise RpcServerSuspended()

    def ensure_correct_routing(self, api_name, *rpc_args):
        routing_key = self.routing_key_manager.calc_routing_key(api_name, *rpc_args)
        if routing_key != self.routing_key:
            raise RpcClientBadRouting()

    @staticmethod
    def rpc_service_control(func):
        """对 RPC API 进行预处理：

        1. 在服务挂起（或者因错误而不能正常提供服务时）拒绝客户端请求
        2. 检测 RPC 请求是否应该由当前服务器处理（错误路由常发生于服务器切换之间的短暂间隔）
        """
        @monitor(func.__name__)
        @inlineCallbacks
        def __inner(*args):
            obj = getattr(func, 'im_self', None)
            if obj:
                service_handler = obj
                rpc_args = args
                bound_func = func
            else:
                service_handler = args[0]
                rpc_args = args[1:]
                bound_func = func.__get__(service_handler)

            if service_handler.server_failover_enabled():
                # 服务器状态检查
                service_handler.must_be_running()

                # 客户端路由检查
                service_handler.ensure_correct_routing(func.__name__, *rpc_args)

            # 处理请求
            result = yield service_handler.call_with_access_log(bound_func, *rpc_args)
            returnValue(result)

        __inner.__name__ = func.__name__
        return __inner

    @inlineCallbacks
    def call_with_access_log(self, func, *rpc_args):
        self.logger.debug('%s - %s - start', func.__name__, security.erase_security_info_in_rpcargs(deepcopy(rpc_args)))

        when_started = time.time()

        def __error_handler(f):

            try:
                f.raiseException()
            except InvalidOperation, e:
                raise
            except Exception, e:
                self.logger.error('%s - error - exception: %s, stack: \n%s', func.__name__,
                                  f.getErrorMessage(), ''.join(traceback.format_tb(f.getTracebackObject())))
                self.logger.access('%s - %s - error - %sms - %s', func.__name__,
                                   security.erase_security_info_in_rpc_result(rpc_args),
                                   tm.past_millisecond(when_started), e)
                raise self._to_thrift_exception(e)

        d = maybeDeferred(func, *rpc_args)
        d.addErrback(__error_handler)

        result = yield d
        self.logger.access('%s - %s - ok - %sms - %s', func.__name__,
                           security.erase_security_info_in_rpcargs(rpc_args),
                           tm.past_millisecond(when_started),
                           security.erase_security_info_in_rpc_result(deepcopy(result)))

        returnValue(result)

    @staticmethod
    def _to_thrift_exception(e):
        if isinstance(e, TimeoutError):
            return InvalidOperation(SlimErrorCodes.rpc_server_timeout, 'rpc_server_timeout')

        return InvalidOperation(what=slim_errors.gen_server_internal,
                                why=slim_errors._VALUES_TO_NAMES[slim_errors.gen_server_internal])
