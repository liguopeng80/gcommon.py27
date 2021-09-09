#!/usr/bin/python
# -*- coding: utf-8 -*-

from gcommon.utils import env


def init_thrift_protocol_stack():
    """将 thrift 协议的生成文件加入 python path."""
    protocol_src_folder = env.get_relative_folder(__file__, './gen-py')
    env.insert_python_source_folder(protocol_src_folder)

