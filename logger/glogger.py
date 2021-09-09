#!/usr/bin/python
# -*- coding: utf-8 -*- 

"""Provide logging module."""

import os
import sys
import logging

from logging.handlers import TimedRotatingFileHandler


VERBOSE = logging.DEBUG - 5
ACCESS = logging.INFO + 5

Default_Log_Root = '.'

logger = logging.getLogger()


LOG_TIME = "%(asctime)-15s"
LOG_LINE_NO = "{%(pathname)s:%(lineno)d}"
LOG_THREAD = "[%(thread)08d]"
LOG_MESSAGE = "%(levelname)-3s %(name)-8s %(message)s"

LOG_FORMAT = '%(asctime)-15s %(levelname)-3s %(name)-8s %(message)s'
THREAD_LOG_FORMAT = '%(asctime)-15s [%(thread)08d] %(levelname)-3s %(name)-8s %(message)s'


class StdIORedirector:
    """Redirect std-out/std-err to log file."""

    STD_IN = 0
    STD_OUT = 1
    STD_ERR = 2

    """ write error messages on stderr in error.log"""

    def __init__(self, level):
        self._level = level
        self._buffer = ''

    def write(self, msg):
        self._buffer = self._buffer + msg
        pos = self._buffer.rfind('\n')

        if pos == -1:
            return

        msg = self._buffer[:pos]
        self._buffer = self._buffer[pos + 1:]

        if self._level == self.STD_OUT:
            logger.debug(msg)
        elif self._level == self.STD_ERR:
            logger.error(msg)
            
    def flush(self):
        pass


class RotatingFileHandler(TimedRotatingFileHandler):
    """Rotate log file by both time and size."""

    def __init__(self, filename, max_bytes=0, when='d', interval=1, backup_count=0, encoding=None, delay=False,
                 utc=False):
        """Create a new time-size rotating file handler. 
        
        when        - 'd' or 'D' means 'days'
        backupCount - max log files
        utc         - if using UTC time
        """

        TimedRotatingFileHandler.__init__(self, filename, when, interval, backup_count, encoding, delay, utc)
        self.maxBytes = max_bytes

    def shouldRollover(self, record):
        if self.stream is None:
            self.stream = self._open()

        if self.maxBytes > 0:
            msg = "%s\n" % self.format(record)
            self.stream.seek(0, 2)

            if self.stream.tell() + len(msg) >= self.maxBytes:
                return True

        return TimedRotatingFileHandler.shouldRollover(self, record)


class LevelFilter(logging.Filter):
    def __init__(self, level, name=''):
        logging.Filter.__init__(self, name)
        self.level = level

    def filter(self, record):
        if record.levelno != self.level:
            return 0

        return 1


def create_file_handler(filename, formatter, level, backup_count, max_bytes):
    handler = RotatingFileHandler(filename, when='midnight', interval=1,
                                  backup_count=backup_count, max_bytes=max_bytes, encoding='utf-8')
    handler.setFormatter(formatter)
    handler.setLevel(level)

    return handler


def access_logging_func(self, msg, *args, **kwargs):
    self.log(ACCESS, msg, *args, **kwargs)


def verbose_logging_func(self, msg, *args, **kwargs):
    self.log(VERBOSE, msg, *args, **kwargs)


def init_stdio_logger():
    init_logger(stdio_handler=True, file_handler=False)


def init_logger(log_folder='', redirect_stdio=False, stdio_handler=True,
                file_handler=True, thread_logger=False, detail=False):
    # Create a new handler for "root logger" on console (stdout):
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s %(name)-5s %(levelname)-5s %(message)s')

    # from config import config
    # log_folder = config.get_str('d.logging.accesslog_dir')

    if not log_folder:
        log_folder = Default_Log_Root

    if stdio_handler:
        redirect_stdio = False

    logging.addLevelName(VERBOSE, 'VEB')
    logging.addLevelName(logging.DEBUG, 'DBG')
    logging.addLevelName(logging.INFO, 'INF')
    logging.addLevelName(ACCESS, 'ACC')
    logging.addLevelName(logging.WARNING, 'WAR')
    logging.addLevelName(logging.ERROR, 'ERR')
    logging.addLevelName(logging.CRITICAL, 'CRT')

    # monkey patch
    logging.Logger.access = access_logging_func
    logging.Logger.verbose = verbose_logging_func

    debug_log = os.path.join(log_folder, 'debug.log')
    access_log = os.path.join(log_folder, 'access.log')
    monitor_log = os.path.join(log_folder, 'monitor.log')

    # print 'Debug log file:   ', debug_log
    # print 'Access log file:  ', access_log
    # print 'Monitor log file: ', monitor_log

    logger.setLevel(logging.DEBUG)

    formatter = _get_log_formatter(threading=thread_logger, detail=detail)
    formatter = logging.Formatter(formatter)

    if file_handler:
        # interval = 1
        backup_count = 5000

        # debug log
        debug_handle = create_file_handler(debug_log, formatter, logging.DEBUG, backup_count, 1280 * 1024 * 1024)
        logger.addHandler(debug_handle)

        # access log
        access_handle = create_file_handler(access_log, formatter, ACCESS, backup_count, 512 * 1024 * 1024)

        access_filter = LevelFilter(ACCESS, 'access_filter')
        access_handle.addFilter(access_filter)

        logger.addHandler(access_handle)

        # monitor log
        monitor_handler = create_file_handler(monitor_log, formatter, logging.CRITICAL, backup_count, 128 * 1024 * 1024)
        logger.addHandler(monitor_handler)

        if redirect_stdio:
            sys.stderr = StdIORedirector(StdIORedirector.STD_ERR)
            sys.stdout = StdIORedirector(StdIORedirector.STD_OUT)
        elif stdio_handler:
            pass

    if stdio_handler:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)


def init_basic_config(level=logging.DEBUG, detail=False):
    formatter = _get_log_formatter(detail=detail)
    logging.basicConfig(level=level, format=formatter)
    return logging.getLogger()


def init_threading_config(level=logging.DEBUG, detail=False):
    formatter = _get_log_formatter(detail=detail)
    logging.basicConfig(level=level, format=formatter)
    return logging.getLogger()


def _get_log_formatter(threading=False, detail=False):
    formatter = LOG_TIME

    if detail:
        formatter += " " + LOG_LINE_NO

    if threading:
        formatter += " " + LOG_THREAD

    return formatter + " " + LOG_MESSAGE


# Test Codes
if __name__ == "__main__":
    init_logger()

    foo = logger.getChild('foo')
    foo.info('what do you want?')
    foo.error('really a bad day')
    foo.critical('fatal error')
    foo.access('wahaha...')

    bar = logger.getChild('bar')
    bar.info('not a problem')
    bar.log(ACCESS, 'not a problem')

    print('Done')

