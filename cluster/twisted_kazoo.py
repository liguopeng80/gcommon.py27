#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-04-23

from twisted.internet import reactor


def twisted_callback(func):
    """decorator: 将回调发送回 twisted 线程（不在当前线程执行）"""
    def __func(*args, **kwargs):
        reactor.callFromThread(func, *args, **kwargs)

    __func.__name__ = func.__name__
    return __func


def twisted_call(func, *args, **kwargs):
    """从 twisted 线程之外，调用需要在 twisted reactor 循环内执行的函数。

    优点：可以避免锁操作。否则必须对并行访问的资源加锁。

    缺点：调用请求会在 reactor 上排队，当前线程 (twisted reactor 之外) 会被阻塞，
    直到调用请求被执行完毕。
    """
    return reactor.callFromThread(func, *args, **kwargs)

