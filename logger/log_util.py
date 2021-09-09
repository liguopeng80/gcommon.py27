#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-03-16
import time
import traceback

from gcommon.utils import gtime


def log_server_started(logger, service_name, version):
    logger.info('-' * 72)
    logger.info('- %s STARTED, version: %s' % (service_name, version))
    logger.info('-' * 72)


def log_function_call(logger, detail=False):
    def _func_logger_decorator(func):
        def _func_logger(*args, **kws):
            if detail:
                logger.debug("fn-called: %s, %s",
                             func.__name__, func.__code__)
            else:
                if args:
                    logger.debug("fn-called: %s, %s", func.__name__, args)
                else:
                    logger.debug("fn-called: %s", func.__name__)
            return func(*args, **kws)

        return _func_logger

    return _func_logger_decorator


def log_cls_function_call(logger, detail=False):
    def _func_logger_decorator(func):
        def _func_logger(self, *args, **kws):
            if detail:
                logger.debug("fn-called: %s - %s, %s",
                             func.__name__, self, func.__code__)
            else:
                if args:
                    str_args = ", ".join([str(arg) for arg in args])
                    logger.debug("fn-called: %s - %s, %s", func.__name__, self, str_args)
                else:
                    logger.debug("fn-called: %s - %s", func.__name__, self)
            return func(self, *args, **kws)
        
        return _func_logger
    
    return _func_logger_decorator


def log_obj_function_call(func):
    def _func_logger(self, *args, **kws):
        if args:
            str_args = ", ".join([str(arg) for arg in args])
            self.logger.debug("fn-called: %s - %s, %s", func.__name__, self, str_args)
        else:
            self.logger.debug("fn-called: %s - %s", func.__name__, self)

        return func(self, *args, **kws)

    return _func_logger


def log_callback(logger, name=None):
    def _func_logger_decorator(func):
        def _func_logger(*args, **kws):
            func_name = name or func.__name__
            start = time.time()

            try:
                result = func(*args, **kws)
            except:
                end = time.time()
                stack = traceback.format_exc()
                logger.error("%s called, time used: %.3f - exception: %s", func_name, end - start, stack)
                raise
            else:
                end = time.time()
                logger.info("%s called, time used: %.3f", func_name, end - start)
                return result

        return _func_logger
    return _func_logger_decorator
