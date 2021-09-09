#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 2015-04-07

from gcommon.rpc import RpcClientDecorator


class PostmanRoutingKey(RpcClientDecorator):
    """随机分配，不需要特殊规则"""
    @classmethod
    def init_key(cls, key):
        cls._key = key

    def calc_routing_key(self, func_name, *args):
        return self._key
