#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-05-14

"""异常和错误处理"""
import sys
import traceback


def format_exception_stack():
    stack = traceback.format_exc()
    return stack


def format_exception(exception=None):
    if exception:
        stack = traceback.format_exception(type(exception), exception, exception.__traceback__)
    else:
        stack = traceback.format_exception(sys.last_type, sys.last_value, sys.last_traceback)

    return ''.join(stack)
