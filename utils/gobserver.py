#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-05-07


class SimpleObservableSubject(object):
    """简单 subject 对象"""
    def __init__(self):
        self._observers = set()

    def subscribe(self, obv):
        self._observers.add(obv)

    def unsubscribe(self, obv):
        self._observers.discard(obv)

    def notify_observers(self):
        for obv in self._observers:
            obv(self)

