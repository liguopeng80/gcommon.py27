# -*- coding: utf-8 -*- 
# created: 2021-04-16
# creator: liguopeng@liguopeng.net

"""Moved from SlimServer (server_base.py)"""

import optparse
import os
import sys

from gcommon.logger import glogger
from gcommon.utils import genv
from gcommon.utils.gjsonobj import JsonObject
from gcommon.utils.gyaml import YamlConfigParser


DEFAULT_CONFIG_FILE = 'default.yaml'
DEFAULT_SECRET_CONFIG_FILE = 'secret.default.yaml'

PROJECT_ROOT = '../../../'
PROJECT_LOG_DIR = '../../../log/'
PROJECT_CONFIG_DIR = '../../../deploy/'
PROJECT_SECRET_CONFIG_DIR = '../../../deploy/'

ENV_PROJECT_ROOT = 'G_PROJECT_ROOT'
ENV_CONFIG_FILE = 'G_COMMON_CONFIG_FILE'
ENV_CONFIG_DIR = 'G_COMMON_CONFIG_DIR'
ENV_SECRET_CONFIG_FILE = 'G_COMMON_SECRET_CONFIG_FILE'
ENV_SECRET_CONFIG_DIR = 'G_COMMON_SECRET_CONFIG_DIR'
ENV_LOG_DIR = 'G_COMMON_LOG_DIR'


def parse_command_line(service_name, parser, all_args, parse_service_options=None):
    """解析命令行参数。"""
    # set usage
    usage_text = """Start %(service)s server.
    %(app)s [-c override_config_file] [-i instance] [-l log_folder] [--sid service_id]"""

    usage_param = {
        'app': all_args[0],
        'service': service_name,
    }

    print(usage_param)
    parser.set_usage(usage_text % usage_param)

    # add arguments
    parser.add_option('-c', '--config-file', dest='config_file',
                      action='store', default='', help='server config file')

    parser.add_option('--secret-config', dest='secret_config_file',
                      action='store', default='', help='server secret config file')

    parser.add_option('-s', '--service', dest='service',
                      action='store', default='', help='service name')

    parser.add_option('-i', '--instance', dest='instance',
                      action='store', default=0, help='instance sequence')

    parser.add_option('--log-folder', dest='log_folder',
                      action='store', default='', help='log folder')

    parser.add_option('-l', '--log-base', dest='log_base',
                      action='store', default='', help='log base')

    parser.add_option('--log-line-no', dest='log_line_no',
                      action='store_true', default=False, help='log file name and line no')

    parser.add_option('--sid', dest='service_id',
                      action='store', default='', help='service ID')

    parser.add_option('-d', '--debug', dest='debug',
                      action='store_true', default=False, help='enable debug')

    parser.add_option('--multi-thread', dest='multi_thread',
                      action='store_true', default=False, help='is multi thread service')

    if parse_service_options:
        parse_service_options(parser)

    # parse command
    all_args = all_args[1:]
    return parser.parse_args(all_args)


def get_log_folder(options, default_config):
    """返回当前服务器的 log 目录。如果目录不存在则创建之。优先顺序（配置参数，环境变量，工程目录）"""
    if options.log_folder:
        return options.log_folder

    if options.log_base:
        # 使用命令行参数
        log_base = options.log_base
    else:
        # 使用环境变量
        log_base = genv.get_env(ENV_LOG_DIR)

    if not log_base:
        if default_config.log_folder:
            # 从缺省配置加载
            return default_config.log_folder
        elif default_config.log_base:
            # 从缺省配置加载
            log_base = default_config.log_base
        else:
            # 完全没有配置
            log_base = genv.get_relative_folder(__file__, PROJECT_LOG_DIR)

    # log/
    # log/my-service/
    # log/my-service/1/
    if not options.service:
        log_folder = log_base
    elif not options.instance:
        log_folder = os.path.join(log_base, options.service)
    else:
        log_folder = os.path.join(log_base, options.service, '%s' % options.instance)

    # create if the log folder is not existed
    if not os.path.isdir(log_folder):
        os.makedirs(log_folder)

    return log_folder


def get_config_file(options, default_config):
    """配置文件，优先顺序（配置参数，环境变量，工程目录）"""
    # 命令行参数
    if options.config_file:
        return options.config_file

    # 环境变量
    config_file = genv.get_env(ENV_CONFIG_FILE)
    if config_file:
        return config_file

    config_dir = genv.get_env(ENV_CONFIG_DIR)
    if config_dir:
        return os.path.join(config_dir, DEFAULT_CONFIG_FILE)

    # 程序指定配置
    if default_config.config_file:
        return default_config.config_file

    if default_config.config_dir:
        return os.path.join(default_config.config_dir, DEFAULT_CONFIG_FILE)

    # 默认配置
    project_cfg_dir = genv.get_relative_folder(__file__, PROJECT_CONFIG_DIR)
    return os.path.join(project_cfg_dir, DEFAULT_CONFIG_FILE)


def get_project_root():
    project_root = genv.get_env(ENV_PROJECT_ROOT)
    if not project_root:
        project_root = genv.get_relative_folder(__file__, PROJECT_ROOT)

    return project_root


def get_secret_config_file(options, default_config):
    """配置文件，优先顺序（配置参数，环境变量，工程目录）"""
    # 命令行参数
    if options.secret_config_file:
        return options.secret_config_file

    # 环境变量
    config_file = genv.get_env(ENV_SECRET_CONFIG_FILE)
    if config_file:
        return config_file

    config_dir = genv.get_env(ENV_SECRET_CONFIG_FILE)
    if config_dir:
        return os.path.join(config_dir, DEFAULT_SECRET_CONFIG_FILE)

    # 程序指定配置
    if default_config.secret_config_file:
        return default_config.secret_config_file

    if default_config.secret_config_dir:
        return os.path.join(default_config.secret_config_dir, DEFAULT_SECRET_CONFIG_FILE)

    # 默认配置
    project_cfg_dir = genv.get_relative_folder(__file__, PROJECT_SECRET_CONFIG_DIR)
    return os.path.join(project_cfg_dir, DEFAULT_SECRET_CONFIG_FILE)


def init_main(service_name="", default_config=None, thread_logger=False, parse_service_options=None):
    """加载进程的基本配置，并初始化日志等设置"""
    if not service_name:
        full_service_name = sys.argv[0]
        _path, filename = os.path.split(full_service_name)
        service_name, _ext = os.path.splitext(filename)

    default_config = JsonObject(default_config or {})
    if not default_config.service:
        default_config.service = service_name

    # 解析命令行参数
    parser = optparse.OptionParser()
    options, args = parse_command_line(service_name, parser, sys.argv, parse_service_options=parse_service_options)

    # 初始化日志服务
    log_folder = get_log_folder(options, default_config)
    glogger.init_logger(log_folder, thread_logger=thread_logger or options.multi_thread)

    # 加载进程配置（default_config 同样用作配置参数）
    config_file = get_config_file(options, default_config)
    config = YamlConfigParser(default_config)
    config.read(config_file, default_config)

    secret_config_file = get_secret_config_file(options, default_config)
    if os.path.exists(secret_config_file):
        config.load_module("secret", secret_config_file)

    config.args = args
    config.options = options
    config.project_root = get_project_root()

    return config


if __name__ == '__main__':
    import logging
    default_demo_config = {"config_file": "test/demo_data.yaml"}
    demo_config = init_main(service_name="demo", default_config=default_demo_config)

    logger = logging.getLogger("test")
    logger.debug(demo_config)

    pass


