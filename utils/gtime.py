#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-01-14

"""Utils for date and time.

约定：任何不带时区的日期对象，均采用本地时间（而不是 UTC 时间）。

核心冷知识：

1. time.time() 返回的是 UTC 时间。

2. datetime.now() 返回的日期是不带时区的。

3. datetime.now(tzlocal.get_localzone()) 能得到带时区的本地时间，时区类型是 DstTzInfo.

4. datetime.now().astimezone() 也能得到带本地时区的本地时间，但时区类型是 datetime.timezone.

5. 在 time.mktime 只接收本地时间，但输出 UTC 时间。
"""

import calendar
import time
from datetime import datetime, timedelta
import platform
from dateutil import parser
import pytz
# import tzlocal


def today():
    """根据【本地时间】返回当天的日期"""
    return datetime.now().date()


class Timestamp(object):
    @staticmethod
    def seconds():
        """UTC 时间，10 字节时间戳"""
        return int(time.time())

    @staticmethod
    def milliseconds():
        """UTC 时间，13 字节时间戳"""
        return int(time.time() * 1000)

    @staticmethod
    def parse(value):
        """解析时间戳，支持带时区字符串、不带时区字符串。

        value = Timestamp.seconds()
        value = Timestamp.milliseconds()

        value = "2021-08-25 12:12:12"
        value = "2021-08-25T12:12:12Z"
        value = "2021-08-25T12:12:12+00:00"
        """
        dt = parser.isoparse(value)
        if dt.tzinfo is not None:
            """转换成本地时间，然后删除时区"""
            dt = dt.astimezone().replace(tzinfo=None)

        return dt


def local_timestamp(in_milliseconds=False):
    """函数名不准确。但是本函数被大量使用，无可挽回...
    以后尽量使用 timestamp

    time.time 返回的实际上是【utc 时间】。
    """
    if in_milliseconds:
        return int(time.time() * 1000)
    else:
        return int(time.time())


def is_naive_datetime(dt):
    """判断 datetime 对象是否带有时区。

    如果没有时区，在任何时候该对象都应该被当作本地时区对待。
    """
    return dt.tzinfo is None


def date_to_timestamp(dt):
    """Return: POSIX-timestamp. (in seconds)

    注意：time.mktime 只接收本地时间，输出的却是 UTC 时间。
    """
    if not is_naive_datetime(dt):
        # 确保时间是本地时间
        dt = dt.astimezone()

    posix_timestamp = time.mktime(dt.timetuple())
    return int(posix_timestamp)


def timestamp_to_date(ts):
    """时间戳转换成本地时间（不含时区）。"""
    dt = datetime.fromtimestamp(ts)

    return dt


def past_millisecond(time_started):
    """How many milliseconds has past...

    time_started: posix timestamp.
    """
    return int((time.time() - time_started) * 1000)


def max_timestamp():
    """返回时戳能表示的最大日期"""
    if '32bit' in platform.architecture():
        max_year = 2019
    else:
        max_year = 2999

    max_dt = datetime(max_year, 12, 31, 23, 59, 59, 999999)
    return date_to_timestamp(max_dt)


def past_seconds(time_started):
    """从参数表示的时间开始，已经流逝的时间。

    如果 time_started 比当前时间晚，则返回 0，不返回负值。

    time_started: posix timestamp.
    """
    diff = time.time() - time_started
    if diff < 0:
        return 0

    return diff


def dt_past_seconds(dt):
    """从参数表示的时间值开始，已经流逝的时间"""
    if is_naive_datetime(dt):
        diff = datetime.now() - dt
    else:
        diff = datetime.now().astimezone() - dt

    return diff.total_seconds()


def has_expired(expiration_time):
    """指定的时间是否已过期"""
    if expiration_time.tzinfo is None:
        return datetime.now() > expiration_time
    else:
        return datetime.now().astimezone() > expiration_time


class TimeHelper(object):
    """计算 context 的执行时间"""
    start = 0

    def __init__(self, header=""):
        self.header = header

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, exc_type, exc_value, traceback):
        time_delta = time.time() - self.start

        if self.header:
            print(self.header, " - time used:", time_delta)
        else:
            print("time used:", time_delta)

        if not exc_type:
            return False
        else:
            return True


def days_before(days, dt=None):
    """返回几天前的日期。

    默认返回不带时区的本地时间。
    """
    if not dt:
        dt = datetime.now()

    return dt - timedelta(days=days)


def date_str(dt=None):
    """返回 2021-08-25 格式的字符串。

    默认采用本地当前时间。
    """
    if not dt:
        dt = datetime.now()

    return "%04d-%02d-%02d" % (dt.year, dt.month, dt.day)


def utc_time_str(dt=None):
    """返回 UCT 标准时间，按照 Java 的方式格式化：2011-02-18T08:30:30Z"""
    date_format = "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ"

    if not dt:
        dt = datetime.utcnow()

    millisecond = int(dt.microsecond / 1000)

    date_str = date_format % (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, millisecond)

    return date_str


def local_time_str(dt=None):
    """返回 UCT 标准时间，按照 Java 的方式格式化：2011-02-18T08:30:30Z"""
    date_format = "%04d-%02d-%02dT%02d:%02d:%02d.%03d"

    if not dt:
        dt = datetime.now()

    millisecond = int(dt.microsecond / 1000)

    date_str = date_format % (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, millisecond)

    return date_str


class DateUtil(object):
    TIME_DAY_START = " 00:00:00"
    TIME_DAY_END = " 23:59:59"

    @staticmethod
    def first_day_of_month(dt):
        return datetime(dt.year, dt.month, 1)

    @staticmethod
    def last_day_of_month(dt):
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        return datetime(dt.year, dt.month, day=last_day) + timedelta(days=1)

    @staticmethod
    def first_day_of_next_month(dt):
        return DateUtil.last_day_of_month(dt) + timedelta(days=1)

    @staticmethod
    def first_day_of_last_month(dt):
        last_day = DateUtil.last_day_of_month(dt)
        return DateUtil.first_day_of_month(last_day)

    @staticmethod
    def last_day_of_last_month(dt):
        return DateUtil.first_day_of_month(dt) - timedelta(days=1)

    @staticmethod
    def last_monday(dt):
        return DateUtil.this_monday(dt) - timedelta(days=7)

    @staticmethod
    def last_sunday(dt):
        return DateUtil.this_sunday(dt) - timedelta(days=7)

    @staticmethod
    def this_monday(dt):
        monday = dt - timedelta(days=dt.weekday())
        return monday.replace(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def this_sunday(dt):
        sunday = dt + timedelta(days=7 - dt.weekday())
        return sunday.replace(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def next_monday(dt):
        return DateUtil.this_monday(dt) + timedelta(days=7)

    @staticmethod
    def next_sunday(dt):
        return DateUtil.this_sunday(dt) + timedelta(days=7)

    @staticmethod
    def middle_night(dt):
        """to 23:59:59"""
        return dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    @staticmethod
    def one_day_before(dt = None):
        if not dt:
            dt = datetime.now()

        return dt - timedelta(days=1)


class DateDelta(object):
    @staticmethod
    def seconds_later(seconds):
        """数秒以后"""
        return datetime.now() + timedelta(seconds=seconds)

    @staticmethod
    def seconds_before(seconds):
        """数秒之前"""
        return datetime.now() - timedelta(seconds=seconds)

    @staticmethod
    def seconds_left(dt):
        """todo: 剩余多少秒？？有 bug 啊"""
        now = datetime.now()
        if now <= dt:
            return 0

        diff = now - dt
        return diff.total_seconds()

    @staticmethod
    def years_later(dt=None, years=1):
        """多年以后"""
        if not dt:
            dt = datetime.now()

        return dt + timedelta(days=years*365)

    @staticmethod
    def years_before(dt=None, years=1):
        """多年以前"""
        if not dt:
            dt = datetime.now()

        return dt - timedelta(days=years*365)


def date_str_for_id(dt = None):
    if not dt:
        dt = datetime.now()

    return dt.strftime("%Y%m%d-%H%M%S")


def date_str_by_minute(dt = None):
    if not dt:
        dt = datetime.now()

    return dt.strftime("%Y-%m-%d-%H-%M")


# Test Codes
if __name__ == "__main__":
    max_timestamp = max_timestamp()
    print('Done: %s' % max_timestamp)

    print(utc_time_str())

