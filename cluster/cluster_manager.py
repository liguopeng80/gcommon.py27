#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-05-06

"""应用服务器集群"""

import hash_ring
import logging
from gcommon.cluster.twisted_kazoo import twisted_callback

logger = logging.getLogger('server')


class ClusterManager(object):
    """"维护当前服务器进程所提供的服务实例对象，以及所依赖的外部服务节点。"""
    server = None
    controller = None

    @staticmethod
    def reg_app_server(server):
        ClusterManager.server = server
        ClusterManager.controller = server.controller


class NodeManager(object):
    """应用服务器集群节点管理"""
    _managers = {}

    @staticmethod
    def add_node_manager(service_name):
        manager = NodeManager(service_name)
        NodeManager._managers[service_name] = manager

        return manager

    @staticmethod
    def get_node_manager(service_name):
        manager = NodeManager._managers.get(service_name)
        if not manager:
            manager = NodeManager._managers(service_name)

        return manager

    def __init__(self, service_name):
        self.SERVICE_NAME = service_name

        self.server_nodes = set()
        self.server_ring = hash_ring.HashRing(self.server_nodes)

        self.watched = False

    def is_watched(self):
        return self.watched

    def set_watched(self, watched=True):
        self.watched = watched

    @twisted_callback
    def set_server_nodes(self, nodes):
        """服务节点变更"""
        logger.info('service node changed - %s - nodes: %s', self.SERVICE_NAME, nodes)
        if not nodes:
            logger.critical('All service nodes DOWN - %s', self.SERVICE_NAME)

        self.server_nodes = set(nodes)
        self.server_ring = hash_ring.HashRing(self.server_nodes)

    def add_server_nodes(self, **nodes):
        """增加一个或者多个节点"""
        self.server_nodes.update(set(nodes))
        self.server_ring = hash_ring.HashRing(self.server_nodes)

    def del_server_nodes(self, **nodes):
        """删除一个或者多个节点"""
        self.server_nodes -= nodes
        self.server_ring = hash_ring.HashRing(self.server_nodes)

    def get_server(self, key):
        """获取给定 Key 所对应的服务器节点"""
        if not isinstance(key, str):
            key = str(key)

        return self.server_ring.get_node(key)

