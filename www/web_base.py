#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-08-19

import logging
import hashlib
import binascii

from exceptions import NotImplementedError

from twisted.web import resource
from twisted.internet.defer import Deferred
from twisted.web.server import NOT_DONE_YET
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from gcommon.data.app.security_cache import SecurityCache
from gcommon.app import const
from gcommon.app.slim_errors import *
from gcommon.rpc import RpcManager
from gcommon.rpc.postman_client import PostmanRoutingKey
from gcommon.utils.jsonobj import JsonObject
from gcommon.utils.rpcparams import get_all_rpc_params, get_rpc_param
from gcommon.utils.async import pass_through_cb
from gcommon.www.router import SlimNavigator

from slimproto.postman import Postman


post_client = None

logger = logging.getLogger('webbase')


def router(url_pattern, **args):
    def __inner(func):
        setattr(func, const.ROUTER_PARAM_PATH, url_pattern)
        setattr(func, const.ROUTER_PARAM_OPT, args)

        # Save original param list in function for wrapping
        arg_names = func.func_code.co_varnames[1:func.func_code.co_argcount]
        setattr(func, const.ROUTER_PARAM_VIEW_FUNC_PARAMS, arg_names)
        return func
    return __inner


class SlimWebBase(resource.Resource):
    """ Base providing Slim web service
        Base Web Service class, All service api should be here
        Usage:
            all business handlers should be decorated with @router(uri, accepted_methods)
            accepted methods is 'GET' by default, leading handler param must be *request*

            e.g.    @router('/hello/world')
                    def hello_world(self, request):
                        pass

                    @router('/hello/world', methods=['GET', 'POST])
                    def hello_world(self, request):
                        pass

            mutable parts in URI should be bracketed with '<' and '>', variable names with
            in the brackets is used as handler params

            e.g.    @router('/api/team/<team_id>/invitations')
                    def _process_team_invitations(self, request, team_id)
                        pass

            add extra accepted methods is NOT supported by now, so handlers dealing with same
            RESTful API should check the request's method
    """
    isLeaf = False
    _security_cache = SecurityCache()

    def __init__(self, config):
        resource.Resource.__init__(self)

        self._cfg = config
        self._app = SlimNavigator(self)

    def getChild(self, path, request):
        return self

    def render(self, request):
        path = request.path
        method = request.method

        # Retrieve real ip address
        # TODO: X-Forwarded-For Check
        if request.requestHeaders._rawHeaders.has_key('x-real-ip'):
            real_ip = str(request.requestHeaders._rawHeaders['x-real-ip'][0])
        else:
            real_ip = str(request.client.host)

        logger.info('process request from %s:%s: %s', real_ip, request.client.port, path)

        # Security check
        # TODO: Refactor, Apply different security policy for each interfaces
        # result = None
        # if self._security_cache.is_ip_banned(real_ip):
        #     result = SlimWebBase._return_result(SlimError.gen_ip_banned)
        # else:
        #     if self._security_cache.add_ip_counter(real_ip):
        #         pass
        #     else:
        #         result = SlimWebBase._return_result(SlimError.gen_exceed_request_limit)
        # if result:
        #     request.setHeader('Content-Type', 'application/json')
        #     return result.dumps()

        # Process request
        try:
            d = Deferred()
            reactor.callLater(0, d.callback, None)
            d.addCallback(pass_through_cb(self._app.serve, path, request, method))
            return NOT_DONE_YET
        except NotImplementedError:
            result = SlimWebBase._return_result(SlimError.gen_bad_request)
        except SlimExcept, e:
            result = SlimWebBase._return_result(e.cmd_error, error_message=e.message)

        logger.access('%s - result:%s',path, result.result)

        request.setHeader('Content-Type', 'application/json')
        return result.dumps()

    @inlineCallbacks
    def init_post_client(self, cfg):
        rpc_params = get_all_rpc_params(cfg, 'postman')

        service_key = get_rpc_param(cfg, 'server_key', 'postman')
        PostmanRoutingKey.init_key(service_key)
        rpc = RpcManager(PostmanRoutingKey)

        yield rpc.connect_to_broker(rpc_params['server'], rpc_params['port'], rpc_params['vhost'],
                                    rpc_params['user'], rpc_params['pwd'], rpc_params['spec'])

        self.post_client = yield rpc.create_rpc_client(Postman.Client, service_key,
                                                       rpc_params.server_exchange, rpc_params.client_exchange)

    @staticmethod
    def parse_request_body(request):
        # body = request.content.read()
        req_message = JsonObject.loads(request.loaded_content)

        logger.debug('Request args - %s', req_message.dumps())
        return req_message

    @staticmethod
    def _parse_paging_params(request):
        """ Get paging params from request, aka, page & size, only capable in method GET """
        # Process param page
        if const.WEB_API_PARAM_LAST in request.args.keys():
            page = int(request.args[const.WEB_API_PARAM_LAST][0])
        else:
            page = const.WEB_PAGING_DEFAULT_ID

        # Process param size
        if const.WEB_API_PARAM_SIZE in request.args.keys():
            size = int(request.args[const.WEB_API_PARAM_SIZE][0])
        else:
            size = const.WEB_PAGING_MAX_SIZE

        return page, size

    @staticmethod
    def _parse_login_params(req_message):
        email = req_message.user.email
        password = req_message.user.password

        terminal_type = req_message.terminal_type
        device = req_message.device
        return email, password, terminal_type, device

    @staticmethod
    def _verify_terminal_parameters(terminal_type, device):
        if terminal_type not in ('device', 'browser'):
            return False

        if not (device and device.device_id and device.os and device.version and device.model):
            return False

    @staticmethod
    def _return_result(slim_error, **kws):
        r = JsonObject()
        r.result = slim_error.code
        r.result_desc = slim_error.desc

        for key, value in kws.iteritems():
            r[key] = value

        return r

    @staticmethod
    def _hash_strings(*args):
        content = ''.join(args)
        sha512 = hashlib.sha512(content)
        return binascii.hexlify(sha512.digest())

    @staticmethod
    def _get_token_string(token_id, token_text):
        return '%d-%s' % (token_id, token_text)
