# -*- coding: utf-8 -*-
#
# author: Guopeng Li
# created: 27 Aug 2008

"""Util to create random data (string, int, etc)."""

import random
import string
import threading
import uuid
from datetime import datetime

from gcommon.utils import gtime


def rand_string(length, case_sensitive=False):
    if case_sensitive:
        data = string.ascii_letters + string.digits
    else:
        data = string.ascii_uppercase + string.digits

    return ''.join(random.SystemRandom().choice(data) for _ in range(length))


def rand_numbers(length):
    data = string.digits
    return ''.join(random.SystemRandom().choice(data) for _ in range(length))


def uuid_string():
    return str(uuid.uuid4()).replace('-', '')


def uid_with_timestamp():
    time_str = gtime.date_str_for_id()
    uid = "%s-%s-%s" % (time_str, rand_string(8), rand_numbers(4))
    return uid


class RandomSequence(object):
    """随机 ID 生成器，每个线程独立生成序号"""
    def __init__(self, instance="", max_value=999999):
        self._value= 0
        self._max_value = max_value

        if instance:
            self._instance = instance
        else:
            thread_id = threading.get_ident()
            thread_id = "%08d" % thread_id
            self._instance = thread_id[-6:]

    def _next_value(self):
        self._value = self._value + 1
        if self._value > self._max_value:
            self._value = 1

        return self._value

    def next(self):
        dt = datetime.now()

        time_str = dt.strftime("%Y%m%d-%H%M%S")
        next_value = self._next_value()

        return "%s-%s%s-%04d" % (time_str, self._instance, rand_numbers(6), next_value)
