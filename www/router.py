#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 2015-08-18

import re
import logging
import traceback

import sys

import time
from twisted.internet.defer import inlineCallbacks, maybeDeferred
from exceptions import NotImplementedError
from gcommon.app import const
from gcommon.utils import tm
from gcommon.utils.jsonobj import JsonObject
from gcommon.app.slim_errors import SlimError, SlimExcept
from gcommon.www.http_utils import set_options_methods


logger = logging.getLogger('Router')


def url_route(url_pattern, **args):
    """Decorator: 修饰 URL 的处理函数，构造 URL MAP."""
    def __inner(func):
        setattr(func, const.ROUTER_PARAM_PATH, url_pattern)
        setattr(func, const.ROUTER_PARAM_OPT, args)
        return func

    return __inner


class SlimNavigator(object):
    def __init__(self, service_handler):
        # TODO: routing table should be stored in trees other than list
        self.routes = []

        for item_name in dir(service_handler):
            item = getattr(service_handler, item_name)
            uri_path = getattr(item, const.ROUTER_PARAM_PATH, None)
            if uri_path:
                router_opts = getattr(item, const.ROUTER_PARAM_OPT)
                methods = set(router_opts.pop('methods', []))

                method = router_opts.pop('method', None)
                if method:
                    methods = methods.add(method)

                self.add_url_rules(uri_path, item, methods)

    @staticmethod
    def build_route_pattern(route):
        """ compile url pattern into regex """
        route_regex = re.sub(r'(<\w+>)', r'(?P\1[-_:\.\w]+)', route)
        return re.compile("^{}$".format(route_regex))

    def add_url_rules(self, route_str, view_func, methods):
        """ add url pattern into routing table
            a table content contains
                - url pattern
                - view function
                - accepted methods
                    -If methods not defined, acceptable methods will be set to GET by default
        """
        if not methods:
            methods = set(["GET"])

        methods.add("OPTIONS")
        route_pattern = self.build_route_pattern(route_str)

        self.routes.append((route_pattern, view_func, methods))

    def get_route_match(self, path, method):
        """ Get correspond view function from routing table """
        for route_pat, view_func, methods in self.routes:
            if method not in methods:
                # requested method must in accepted methods
                continue
            match = route_pat.match(path)
            if match:
                # get appropriate view function and arguments
                return match.groupdict(), view_func, methods

        # No matched path
        return None

    @inlineCallbacks
    def serve(self, path, request, method):
        """ serve a request from specific path """
        logger.debug('process %s request start - from %s:%s: %s', method, request.client.host, request.client.port, path)
        request.loaded_content = request.content.read()
        when_started = time.time()

        route_match = self.get_route_match(path, method)
        if route_match:
            if method == 'OPTIONS':
                set_options_methods(request, allowed_methods=route_match[-1])
                result = SlimError.ok

                rtn = JsonObject()
                rtn.result = result.code
                rtn.result_desc = result.desc
                request.write(rtn.dumps())
                logger.access('processed %s request from %s:%s: %s - result: %s',
                              method, request.client.host, request.client.port, path, rtn)
                request.finish()
                return
            elif method == 'POST':
                set_options_methods(request, post=True)
            elif method == 'GET':
                set_options_methods(request, get=True)
            elif method == 'PUT':
                set_options_methods(request, put=True)
            elif method == 'DEL':
                set_options_methods(request, delete=True)

            # 获取 URL 中匹配的模式参数和处理函数
            params, view_func, _ = route_match

            # 将请求和方法也加入到处理函数
            params[const.ROUTER_VIEW_FUNC_KWS_REQUEST] = request
            params[const.ROUTER_VIEW_FUNC_KWS_METHOD] = method
            # todo: remove this line
            params['kwargs'] = params

            # 提取处理函数需要的参数（去除 self 参数）
            arg_names = getattr(view_func, const.ROUTER_PARAM_VIEW_FUNC_PARAMS)
            view_func_params = {}
            for param_name in arg_names:
                if param_name in params:
                    view_func_params[param_name] = params[param_name]
                else:
                    # 从 URL 参数和 post 参数中获取
                    view_func_params[param_name] = ""

            def __error_handler(f):

                try:
                    f.raiseException()
                except Exception, e:
                    logger.error('%s - error - exception: %s, stack: \n%s', view_func.__name__,
                                 f.getErrorMessage(), ''.join(traceback.format_tb(f.getTracebackObject())))

                    result = JsonObject()
                    if isinstance(e, SlimExcept):
                        result.result = e.cmd_error
                        result.result_desc = e.message
                    else:
                        result.result = SlimError.gen_server_internal.code
                        result.result_desc = SlimError.gen_server_internal.desc

                    return result

            d = maybeDeferred(view_func, **view_func_params)
            d.addErrback(__error_handler)
            result = yield d

        else:
            result = JsonObject()
            result.result = SlimError.server_not_implemented.code
            result.result_desc = SlimError.server_not_implemented.desc

        request.setHeader('Content-Type', 'application/json')
        request.write(result.dumps())

        logger.access('processed %s request from %s:%s: %sms - %s - %s - %s',
                      method, request.client.host, request.client.port,
                      tm.past_millisecond(when_started),
                      path, request.loaded_content, result)
        request.finish()

if __name__ == "__main__":
    # Test Code
    class Request(object):
        def setHeader(self, *args, **kwargs):
            pass

    class Test(object):
        @url_route('/api/hello')
        def hello(self, ):
            return "Hello World!"

        @url_route('/api/hello/<username>')
        def hello_user(self, username):
            return "HELLO {}".format(username)

        @url_route('/api/hello/<username>/team/<teamname>')
        def hello_join(self, username, teamname):
            return "Hello %s, welcome to %s" % (username, teamname)

        @url_route('/api/hello/noparam', method='GET')
        def hello_noparam(self, noparm):
            return "Hello %s." % (noparm or 'noparam')

        @url_route('/api/hello/request/<request_id>')
        def hello_with_request(self, request, request_id):
            return "Hello %s in %s." % (request_id, request)

    app = SlimNavigator(Test())
    request = Request()

    print app.serve('/api/hello', request, 'GET')
    print app.serve('/api/hello/current', request, 'GET')
    print app.serve('/api/hello/current/team/slim', request, 'GET')

    print app.serve('/api/hello/noparam', request, 'GET')
    print app.serve('/api/hello/request/guli', request, 'GET')
