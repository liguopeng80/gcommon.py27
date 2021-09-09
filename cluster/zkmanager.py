#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-05-13

import logging
from gcommon import cluster
from gcommon.cluster import server_init


logger = logging.getLogger('zookeeper')


class ZkServiceManager(object):
    """Zookeeper 客户端，负责注册/删除/监控当前服务器运行所依赖的其它服务节点"""
    def __init__(self, service_controller, zk_service):
        self._under_working = False
        self._service_controller = service_controller

        self._zk_service = zk_service
        self._zk_client = zk_service.kazoo_client

    def start(self, default_service=True):
        """初始化，注册回调关注服务状态"""
        logger.info('server register started')
        self._service_controller.subscribe(self._on_app_server_status_changed)

        if default_service:
            self._zk_service.subscribe(self._on_zk_service_status_changed)

    def _on_zk_service_status_changed(self, service):
        """Zookeeper 服务状态改变"""
        assert service == self._zk_service

        if service.is_good():
            # zookeeper 客户端连接成功
            self._on_zk_online()

            logger.debug('creating server alive node on zookeeper')
            server_init.zk_create_alive_node(self._zk_client)
            logger.info('server alive node on zookeeper created')

    def _on_app_server_status_changed(self, controller):
        """应用服务器的状态改变"""
        logger.info('server status changed to %s', controller.status)
        assert controller == self._service_controller

        if controller.status.is_starting():
            self._under_working = True
            self._do_more_starting_actions()

        elif controller.status.is_running():
            # 服务器已经进入工作状态
            pass
        elif self._under_working:
            self._under_working = False
            self._do_more_cleanup_actions()

    def _on_zk_online(self):
        pass

    def _do_more_starting_actions(self):
        """更多的初始化动作，由需要的派生类实现"""
        pass

    def _do_more_cleanup_actions(self):
        """更多的清理动作，由需要的派生类实现"""
        pass


class SlimZookeeperManager(ZkServiceManager):
    def _do_more_starting_actions(self):
        # 外部依赖均已就绪，注册 zookeeper 节点
        logger.debug('creating working node on zookeeper')
        server_init.zk_create_working_node(self._zk_client)
        logger.debug('working node on zookeeper created')

        map(self._watch_external_services, cluster.All_Node_Managers)

    def _do_more_cleanup_actions(self):
        # 服务离开工作状态，删除服务节点
        logger.debug('deleting working node on zookeeper')
        server_init.zk_delete_working_node(self._zk_client)
        logger.info('working node on zookeeper deleted')

    def _watch_external_services(self, node_manager):
        """对某个服务器的工作节点进行监听"""
        if node_manager.is_watched():
            return

        path = server_init.get_parent_path_to_working_service(node_manager.SERVICE_NAME)

        self._zk_service.zk.ensure_path(path)
        self._zk_service.zk.ChildrenWatch(path, node_manager.set_server_nodes)

        node_manager.set_watched(True)


class SlimHashLockManager(ZkServiceManager):
    _lock = None
    _observer = None

    def is_my_resource(self, key):
        return self._lock.is_my_resource(key)

    def set_observer(self, observer):
        self._observer = observer

    def _do_more_starting_actions(self):
        from gcommon.cluster.zkhashlock import SlimHashLock

        lock_path = server_init.get_parent_node_to_hash_lock()
        service_uid = cluster.ClusterManager.server.unique_server_name

        self._lock = SlimHashLock(self._zk_service.zk, lock_path, service_uid, self._observer)

    def _do_more_cleanup_actions(self):
        if self._lock:
            self._lock.cleanup()

        self._lock = None
