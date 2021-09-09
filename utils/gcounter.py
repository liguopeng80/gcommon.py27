#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-08-19

"""计数器、计时器、序号发生器等。"""
import time
from contextlib import contextmanager


class _Register(object):
    """具有名称的对象的全局注册表"""
    _items = None

    @classmethod
    def register(cls, name, item):
        cls._initialize()
        assert cls._items.get(name, None) is None
        cls._items[name] = item

    @classmethod
    def get(cls, name):
        cls._initialize()

        item = cls._items.get(name, None)
        if not item:
            item = cls(name)

        return item

    @classmethod
    def all(cls):
        cls._initialize()
        return cls._items

    @classmethod
    def _initialize(cls):
        if cls._items is None:
            cls._items = {}


class Sequence(object):
    """序列号生成器"""
    def __init__(self, start=0, step=1):
        self.value = start
        self.step = step

    def next_value(self):
        self.value += self.step
        return self.value

    def __str__(self):
        return str(self.value)


class SimpleCounter(object):
    """简单计数器。"""

    def __init__(self):
        self._value = 0

    def inc(self, count=1):
        self._value += count

    def dec(self, count=1):
        self._value -= count

    @property
    def value(self):
        return self._value

    def __str__(self):
        return str(self._value)


class Counter(SimpleCounter, _Register):
    """有名称的计数器将被存入全局注册表。"""
    def __init__(self, name=None):
        SimpleCounter.__init__(self)
        if name:
            self.register(name, self)

    def inc(self, count=1):
        self._value += count

    def dec(self, count=1):
        self._value -= count

    @property
    def value(self):
        return self._value

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return str(self._value)


class Gauge(object):
    @staticmethod
    @contextmanager
    def create(name, value=1):
        """某种状态的当前活跃数量。"""
        counter = Counter.get(name)
        try:
            counter.inc(value)
            yield counter
        finally:
            counter.dec(value)
        pass


class Timer(_Register):
    """统计调用时间"""
    @staticmethod
    @contextmanager
    def create(name):
        """某种状态的当前活跃数量。"""
        timer = Timer.get(name)
        try:
            start = time.time()
            yield
        finally:
            time_past = time.time() - start
            time_past = int(time_past * 1000)
            timer.inc(time_past)
        pass

    def __init__(self, name):
        self.name = name
        self.count = 0
        self.total_time = 0

        self.register(name, self)

    def inc(self, time_past):
        """增加一次执行次数，并同时增加时间"""
        self.count += 1
        self.total_time += time_past

    def clear(self):
        """计算平均时间并将计时器清零"""
        if not self.count:
            return 0
        average = self.total_time / self.count
        self.count = 0
        self.total_time = 0

        return average


def demo():
    connections = Counter("total_connections")
    connections.inc(100)

    with Gauge.create('active_connections', 1):
        # {'total_connections': 100, 'active_connections': 1}
        print(Counter.all())

    # {'total_connections': 100, 'active_connections': 0}
    print(Counter.all())

    for i in range(10):
        with Timer.create('user_login'):
            time.sleep(0.01)

    for name, timer in Timer.all().items():
        # user_login 9
        print(name, timer.clear())


if __name__ == '__main__':
    demo()
    print('Done')
