#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import platform


def is_windows():
    uname = platform.uname()
    return uname[0].lower() == 'windows'


def get_env(name):
    """查看环境变量"""
    return os.environ.get(name, '')


def set_env(name, new_value):
    """设置环境变量"""
    os.environ[name] = new_value


def get_relative_folder(base_path, relative_path):
    """根据 base 目录，以及目标目录相对该目录的相对路径，得到目标目录的绝对路径。"""
    fullname = os.path.abspath(base_path)
    
    if os.path.isdir(fullname):
        folder = fullname
    else:
        folder, _filename = os.path.split(fullname)
    
    abs_folder = os.path.abspath(os.path.join(folder, relative_path))
    
    return abs_folder


def insert_python_source_folder(newt_src_folder):
    """增加新的 python source folder."""
    sys.path.insert(0, newt_src_folder)


def get_folder(fullname):
    """返回路径所对应的目录（去除文件名）。"""
    if os.path.isdir(fullname):
        folder = fullname
    else:
        folder, _filename = os.path.split(fullname)
    
    return os.path.abspath(folder)

