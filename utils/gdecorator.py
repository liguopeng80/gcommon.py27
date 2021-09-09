#!/usr/bin/python
# -*- coding: utf-8 -*-


class Decorator(object):
    func = None

    def __init__(self, func):
        self.__func = func

    def __getattribute__(self, item):
        if item == "func":
            return super(Decorator, self).__getattribute__(item)

        return self.func.__getattribute__(item)

    def __setattr__(self, key, value):
        if key == "func":
            return super(Decorator, self).__setattr__(key, value)

        return self.func.__setattr__(key)


class DummyDecorator(Decorator):
    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

