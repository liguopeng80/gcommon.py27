#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-05-04

import optparse
import traceback
import os
import sys
import logging

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, maybeDeferred

from gcommon import cluster
from gcommon.logger import log_util
from gcommon.logger import server_logger
from gcommon.cluster.cluster_manager import ClusterManager
from gcommon.cluster.server_init import create_zookeeper_service
from gcommon.cluster.zkmanager import SlimZookeeperManager, SlimHashLockManager
from gcommon.utils.rpcparams import get_rpc_param, get_rpc_routing_key
from gcommon.utils import env
from gcommon.utils import proc
from gcommon.utils.cfgfile import DefaultConfigParser
from gcommon.proto import init_thrift_protocol_stack

init_thrift_protocol_stack()

logger = logging.getLogger('server')

CONFIG_FILE_NAME = 'default.conf'

PROJECT_LOG_DIR = '../../../log/'
PROJECT_CONFIG_DIR = '../../../etc/'

ENV_CONFIG_DIR = 'SLIM_CONFIG_DIR'
ENV_LOG_DIR = 'SLIM_LOG_DIR'


def get_log_folder(options):
    """返回当前服务器的 log 目录。如果目录不存在则创建之。"""
    if options.log_folder:
        log_base = options.log_folder
    else:
        log_base = env.get_env(ENV_LOG_DIR)
        if not log_base:
            log_base = env.get_relative_folder(__file__, PROJECT_LOG_DIR)

    log_folder = os.path.join(log_base, options.service, '%s' % options.instance)
    # create if the log folder is not existed
    if not os.path.isdir(log_folder):
        os.makedirs(log_folder)

    return log_folder


def get_config_file(options):
    """配置文件，优先顺序（配置参数，环境变量，工程目录）"""
    if options.config_file:
        return options.config_file

    config_dir = env.get_env(ENV_CONFIG_DIR)
    if config_dir:
        return os.path.join(config_dir, CONFIG_FILE_NAME)

    project_cfg_dir = env.get_relative_folder(__file__, PROJECT_CONFIG_DIR)
    return os.path.join(project_cfg_dir, CONFIG_FILE_NAME)


def parse_command_line(service_name, parser, all_args):
    """解析命令行参数。"""
    # set usage
    usage_text = """Start %(service)s server.
    %(app)s [-c override_config_file] [-i instance] [-l log_folder] [-sid service_id]"""

    usage_param = {
        'app': all_args[0],
        'service': service_name,
    }

    print usage_param
    parser.set_usage(usage_text % usage_param)

    # add arguments
    parser.add_option('-c', '--config-file', dest='config_file',
                      action='store', default='', help='server config file')

    parser.add_option('-s', '--service', dest='service',
                      action='store', default='', help='service name')

    parser.add_option('-i', '--instance', dest='instance',
                      action='store', default=0, help='instance sequence')

    parser.add_option('-l', '--log-folder', dest='log_folder',
                      action='store', default='', help='log folder')

    parser.add_option('--sid', dest='service_id',
                      action='store', default='', help='service ID')

    parser.add_option('-d', '--debug', dest='debug',
                      action='store_true', default=False, help='enable debug')

    # parse command
    all_args = all_args[1:]
    return parser.parse_args(all_args)


class SlimServer(object):
    STATUS_CONTROLLER_CLASS = None
    controller = None

    SERVICE_NAME = 'undefined'
    INSTANCE = 0
    VERSION = 'undefined'

    DEFAULT_CONFIG = {}

    def init_server(self):
        """初始化服务器"""
        pass

    @inlineCallbacks
    def start_server(self):
        """启动服务器"""
        raise NotImplementedError('for sub-class')

    def _get_service_specific_confg(self):
        """服务器特定的配置参数"""
        return None

    def __init__(self):
        self.options = None
        self.args = None

        self.config_file = ''
        self.log_dir = ''

        self.cfg = DefaultConfigParser(self.DEFAULT_CONFIG)

        # 解析命令行
        parser = optparse.OptionParser()

        options, args = parse_command_line(self.SERVICE_NAME, parser, sys.argv)
        self.options, self.args = options, args

        self.verify_command_line(parser)

        # 初始化 logger
        self.init_logger()

        # 加载配置项
        self.load_server_config()

        self.full_server_name = proc.get_process_id(self.SERVICE_NAME, int(self.options.instance))
        self.unique_server_name = proc.get_process_unique_id(self.SERVICE_NAME, int(self.options.instance))

    def _init_controller(self):
        if self._is_zookeeper_enabled_in_cfg() and self.STATUS_CONTROLLER_CLASS:
            # todo: load init status from config file
            # todo: client failover and server failover
            self.controller = self.STATUS_CONTROLLER_CLASS(self)
            self.controller.subscribe(self._on_server_status_changed)

            cluster.Failover_Enabled = True
        else:
            self.STATUS_CONTROLLER_CLASS = None
            cluster.Failover_Enabled = False

    def _is_zookeeper_enabled(self):
        """应用服务器支持 zookeeper"""
        return self._is_zookeeper_enabled_in_cfg() and self._is_zookeeper_enabled_on_server()

    def _is_zookeeper_enabled_on_server(self):
        """应用服务器支持 zookeeper"""
        return self.STATUS_CONTROLLER_CLASS is not None

    def _is_zookeeper_enabled_in_cfg(self):
        """部署环境支持 zookeeper"""
        return self.cfg.get_bool('zookeeper.enabled')

    def is_failover_enabled(self):
        return self._is_zookeeper_enabled()

    def is_my_resource(self, key):
        if not self._is_zookeeper_enabled():
            # for local debug
            return True
        else:
            return self._hl_manager.is_my_resource(key)

    def is_running(self):
        """服务是否正在运行"""
        if self._is_zookeeper_enabled():
            return self.controller.is_running()
        else:
            # 没有状态控制类的服务总是处于运行状态
            return True

    def _on_server_status_changed(self, _controller):
        """服务器状态改变（停止/运行）"""
        # raise NotImplementedError('for sub-class')
        pass

    def verify_command_line(self, parser):
        # if self.args:
        #     parser.error('No arguments needed.')
        if self.options.service:
            if self.options.service != self.SERVICE_NAME:
                parser.error('bad service name. expected: %s, got: %s.'
                             % (self.SERVICE_NAME, self.options.service))
        else:
            self.options.service = self.SERVICE_NAME

        if not self.options.instance:
            self.options.instance = self.INSTANCE

        pass

    def load_server_config(self):
        self.config_file = get_config_file(self.options)
        params = self.get_config_params()

        if self.config_file:
            self.cfg.read(self.config_file, params)

    def init_logger(self):
        log_folder = get_log_folder(self.options)
        # TODO: stdio_handler should be False in production environment
        server_logger.init_logger(log_folder, add_stdio_handler=True)

    def get_config_params(self):
        cfg_root = env.get_folder(self.config_file)
        service_config = self._get_service_specific_confg()

        params = {
            'SERVICE': self.options.service,
            'INSTANCE': self.options.instance,
            'CFGROOT': cfg_root,
        }

        if service_config:
            params.update(service_config)

        return params

    def main(self):
        # 如果需要，启动 controller
        self._init_controller()
        ClusterManager.reg_app_server(self)

        # 打印服务器启动信息
        log_util.log_server_started(logger, self.SERVICE_NAME, self.VERSION)

        reactor.callLater(0, self._service_main)
        if self.controller:
            reactor.callLater(0, self.controller.start)

        reactor.run()

    @inlineCallbacks
    def _service_main(self):
        def __error_back(failure):
            stack = ''.join(traceback.format_tb(failure.getTracebackObject()))
            logger.error('failure: \n%s', stack)

            return failure

        try:
            d = maybeDeferred(self._service_main_with_exception)
            d.addErrback(__error_back)
            yield d
        except Exception, e:
            logger.error('server exception: %s', e)
            reactor.stop()

    def _init_zookeeper_client(self):
        # init zookeeper client
        self.zk_service = create_zookeeper_service()
        zk_manager = SlimZookeeperManager(self.controller, self.zk_service)
        zk_manager.start()

        # todo: remove testing code
        test_hash_lock = False
        if test_hash_lock:
            from zkhashlock import HashLockObserver
            self.__class__ = type(self.__class__.__name__, (self.__class__, HashLockObserver), {})
            self.start_hash_lock(self)

    def start_hash_lock(self, observer):
        """
        :type observer: HashLockObserver
        """
        assert self._is_zookeeper_enabled()

        self._hl_manager = SlimHashLockManager(self.controller, self.zk_service)
        self._hl_manager.set_observer(observer)
        self._hl_manager.start(default_service=False)

    @inlineCallbacks
    def _service_main_with_exception(self):
        if self._is_zookeeper_enabled():
            self._init_zookeeper_client()

        yield maybeDeferred(self.init_server)
        yield maybeDeferred(self.start_server)

        logger.debug('--------- STARTED ---------')

    def get_routine_key(self):
        if cluster.Failover_Enabled:
            key = get_rpc_routing_key(self.cfg, self.unique_server_name)
        else:
            key = get_rpc_param(self.cfg, 'server_key', self.SERVICE_NAME)

        return key

