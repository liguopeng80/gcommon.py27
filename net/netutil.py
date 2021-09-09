#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-05-12


class _ConnectionStatus(object):
    """通用网络客户端连接状态（无论服务器是基于有连接/无连接/长连接/短链接）"""
    Initialized = 0
    Connected = 1

    Connecting = 11
    Reconnecting = 12

    Suspended = 13

    Conn_Failed = 21
    Closing = 22
    Closed = 23

    def __init__(self, value):
        self._value = value

    def is_connecting(self):
        return self._value in (self.Connecting, self.Reconnecting)

    def is_connected(self):
        return self._value == self.Connected

    def is_closed(self):
        return self._value in (self.Conn_Failed, self.Closed, self.Initialized)

    def is_closing(self):
        return self._value == self.Closing

    def is_suspended(self):
        return self._value == self.Suspended

    def __str__(self):
        return str(self._value)


class ConnectionStatus(object):
    """所有支持的连接状态"""
    Initialized = _ConnectionStatus(_ConnectionStatus.Initialized)
    Connected = _ConnectionStatus(_ConnectionStatus.Connected)
    Closed = _ConnectionStatus(_ConnectionStatus.Closed)

    Connecting = _ConnectionStatus(_ConnectionStatus.Connecting)
    Reconnecting = _ConnectionStatus(_ConnectionStatus.Reconnecting)
    Suspended = _ConnectionStatus(_ConnectionStatus.Suspended)

    Conn_Failed = _ConnectionStatus(_ConnectionStatus.Conn_Failed)
    Closing = _ConnectionStatus(_ConnectionStatus.Closing)


