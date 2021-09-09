#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-05-05

"""管理 Zookeeper 服务"""
import logging

from twisted.internet import reactor

from gcommon.cluster.svc_base import ExternalService
from gcommon.cluster.zookeeper import ZookeeperObserver, ZookeeperClientManager
from gcommon.net.netutil import ConnectionStatus

logger = logging.getLogger('zookeeper')


class ZookeeperService(ExternalService, ZookeeperObserver):
    """Zookeeper 服务管理器，负责监控（和尝试恢复） zookeeper 服务器的连接状态。"""
    Service_Name = 'zookeeper'

    # 尝试重连的时间间隔
    RECONNECTION_INTERVAL = 3

    def __init__(self, hosts, reconn_interval=0):
        ExternalService.__init__(self, self.Service_Name, crucial=True)
        ZookeeperObserver.__init__(self)

        manager = ZookeeperClientManager(self, hosts)
        self.set_client_manager(manager)

        self.reconn_interval = reconn_interval or self.RECONNECTION_INTERVAL

    @property
    def kazoo_client(self):
        return self.client_manager.kazoo_client

    def start(self):
        self._try_reconnection(wait=False)

    def stop(self):
        self.client_manager.stop()
        self.client_manager.wait()

    def _on_conn_opened(self):
        """连接打开或者恢复 - in Twisted thread"""
        # 通知 service controller, zookeeper 服务已近就绪
        if self.is_bad():
            self.enable_service()
        else:
            # 连接从 suspended 状态恢复，什么都不用干
            pass

    def _on_conn_lost(self):
        """会话断开 - in Twisted thread"""
        # 通知 service controller, zookeeper 服务已经失效
        self.disable_service()

        # 启动重连机制，尝试恢复 zookeeper 服务
        self._try_reconnection()

    def _on_conn_suspended(self):
        """连接断开，会话挂起 - in Twisted thread"""
        # Zookeeper 客户端会自动尝试恢复，等待
        assert self.is_good()

    def _on_conn_failed(self):
        """初始连接失败，无法建立会话 - in Twisted thread"""
        assert self.is_bad()

        # 通知 service controller, zookeeper 服务已经失效。
        # 对第一次连接来说，这是一个不必要的调用
        self.disable_service()

        # 启动重连
        self._try_reconnection()

    def _try_reconnection(self, wait=True):
        seconds = wait and self.reconn_interval or 0
        logger.debug('reconnecting after %s seconds - current connection status: %s',
                     seconds, self.conn_status)

        if self.conn_status.is_connecting():
            # 正在尝试重连
            logger.debug('a reconnecting has been scheduled, skip')
            return

        self.conn_status = ConnectionStatus.Reconnecting

        logger.debug('a reconnecting will be executed after %s seconds', seconds)
        reactor.callLater(seconds, self._do_reconnect)

    def _do_reconnect(self):
        """尝试重新连接 zookeeper 服务器"""
        logger.debug('try reconnect to zookeeper')
        self.client_manager.start()


