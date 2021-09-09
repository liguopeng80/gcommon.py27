#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-03-11

import logging
import threading


class ClassLogger(object):
    def __init__(self, logger):
        if isinstance(logger, str):
            self.logger = logging.getLogger(logger)
        else:
            self.logger = logger

    def multi_thread_func_logger(self, func):
        def __logger_helper(obj, *args, **kws):
            self.logger.debug('[%x] - %s - >>> %s.%s - args: %s, kws: %s - %s',
                              threading.get_ident(),
                              obj.get_logger_identifier(),
                              obj.__class__.__name__,
                              func.__name__,
                              args,
                              kws,
                              obj.get_logger_status(),
                              )

            result = func(obj, *args, **kws)

            self.logger.debug('[%x] - %s - <<< %s.%s - result: %s - %s',
                              threading.get_ident(),
                              obj.get_logger_identifier(),
                              obj.__class__.__name__,
                              func.__name__,
                              result,
                              obj.get_logger_status(),
                              )

            return result

        return __logger_helper

    def sync_func_logger(self, func):
        def __logger_helper(obj, *args, **kws):
            self.logger.debug('%s >>> %s.%s - args: %s, kws: %s - %s',
                              obj.get_logger_identifier(),
                              obj.__class__.__name__,
                              func.__name__,
                              args,
                              kws,
                              obj.get_logger_status(),
                              )

            result = func(obj, *args, **kws)

            self.logger.debug('%s <<< %s.%s - result: %s - %s',
                              obj.get_logger_identifier(),
                              obj.__class__.__name__,
                              func.__name__,
                              result,
                              obj.get_logger_status(),
                              )

            return result

        return __logger_helper

    def critical(self, *args, **kwargs):
        self.logger.critical(*args, **kwargs)

    def error(self, *args, **kwargs):
        self.logger.error(*args, **kwargs)

    def exception(self, *args, **kwargs):
        self.logger.exception(*args, **kwargs)

    def warning(self, *args, **kwargs):
        self.logger.warning(*args, **kwargs)

    def info(self, *args, **kwargs):
        self.logger.info(*args, **kwargs)

    def debug(self, *args, **kwargs):
        self.logger.debug(*args, **kwargs)

    def log(self, *args, **kwargs):
        self.logger.log(*args, **kwargs)


def test():
    from gcommon.logger import init_logger
    init_logger(stdio_handler=True, file_handler=False)

    logger = ClassLogger('test_class')
    logger.critical("haha: %s", 1+1)
    logger.error("haha: %s", 1+2)
    logger.exception("haha: %s", 1+3)
    logger.warning("haha: %s", 1+4)
    logger.info("haha: %s", 1+5)
    logger.debug("haha: %s", 1+6)

    import logging
    logger.log(logging.CRITICAL, "haha: %s", 1+7)


if __name__ == '__main__':
    test()
